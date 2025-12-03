import express from "express";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions/index.js";
import { Api } from "telegram";
import { promises as fs } from "fs";
import path from "path";

/* ------------ config / helpers ------------ */
const DATA_DIR = "/app/data";
const SESS_DIR = path.join(DATA_DIR, "sessions");
const BASE_DELAY = parseInt(process.env.SAFE_BASE_DELAY_MS || "1200", 10);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const jitter = (b) => b + Math.floor(Math.random() * 350);

async function safeInvoke(fn) {
  await sleep(jitter(BASE_DELAY));
  try { return { ok: true, value: await fn() }; }
  catch (e) {
    const m = String(e?.message || e).match(/FLOOD_WAIT_(\d+)/i);
    if (m) return { ok:false, flood:true, wait_seconds: parseInt(m[1],10)||0, error:e };
    return { ok:false, error:e, message: String(e?.message||e) };
  }
}

function ok(res, data = null, meta = null) { res.status(200).json({ success: true, data, meta, error: null }); }
function err(res, http = 500, code = "INTERNAL", message = "Internal error", details = null) { res.status(http).json({ success: false, data: null, meta: null, error: { code, message, details } }); }

function ensureAuth(req, res) {
  const token = req.headers.authorization?.replace("Bearer ", "");
  if (process.env.ADMIN_TOKEN && token !== process.env.ADMIN_TOKEN) {
    err(res, 401, "UNAUTHORIZED", "Missing or invalid ADMIN_TOKEN");
    return false;
  }
  return true;
}

async function readSessionStringByName(session_name) {
  const candidates = [
    path.join(SESS_DIR, `${session_name}.json`),
    path.join(SESS_DIR, `${session_name}.session`),
    path.join(SESS_DIR, `${session_name}.sessionstring`),
    path.join(SESS_DIR, session_name)
  ];
  for (const p of candidates) {
    try {
      if (p.endsWith(".json")) {
        const raw = JSON.parse(await fs.readFile(p, "utf8"));
        if (raw?.session_string) return String(raw.session_string).trim();
      } else {
        const s = (await fs.readFile(p, "utf8")).trim();
        if (s) return s;
      }
    } catch {}
  }
  throw new Error("SessionString not found for session_name=" + session_name);
}

async function getClient({ session_name, session_string, api_id, api_hash }) {
  const sessionStr = session_string || (session_name ? await readSessionStringByName(session_name) : null);
  if (!sessionStr) throw new Error("BAD_REQUEST: session_name or session_string is required");
  const client = new TelegramClient(new StringSession(sessionStr), Number(api_id), String(api_hash), { connectionRetries: 1 });
  const conn = await safeInvoke(() => client.connect());
  if (!conn.ok) {
    if (conn.flood) {
      const ex = new Error("FLOOD_WAIT");
      ex.__http = 429;
      ex.__payload = { success:false, data:null, meta:null, error:{ code:"FLOOD_WAIT", message:"FLOOD_WAIT", details:{ wait_seconds: conn.wait_seconds } } };
      throw ex;
    }
    throw conn.error;
  }
  return client;
}

/* ---------- peers & utils ---------- */
function makePeerVariants({ linked_chat_id, linked_chat_access_hash }) {
  const variants = [];
  if (Number.isFinite(Number(linked_chat_id))) {
    // variant A: channel/supergroup
    variants.push({
      kind: "channel",
      peer: new Api.InputPeerChannel({
        channelId: BigInt(linked_chat_id),
        accessHash: linked_chat_access_hash != null ? BigInt(linked_chat_access_hash) : undefined
      })
    });
    // variant B: legacy/basic chat
    variants.push({
      kind: "chat",
      peer: new Api.InputPeerChat({ chatId: Number(linked_chat_id) })
    });
  }
  return variants;
}

async function joinIfNeeded(client, peer){
  try { await safeInvoke(() => client.invoke(new Api.channels.JoinChannel({ channel: peer }))); }
  catch(e){
    const msg = String(e?.message||"");
    if (/(USER_ALREADY_PARTICIPANT|INVITE_HASH_INVALID|CHANNEL_PRIVATE|CHANNELS_TOO_MUCH)/.test(msg)) return;
    const m = msg.match(/FLOOD_WAIT_(\d+)/); if (m) { const er=new Error("FLOOD_WAIT"); er.code="FLOOD_WAIT"; er.wait_seconds=parseInt(m[1],10)||0; throw er; }
  }
}

async function readMembersCount(client, ent){
  try {
    if (!ent) return null;
    if (ent.className === "Chat" || ent.className === "ChatForbidden") {
      const f = await safeInvoke(() => client.invoke(new Api.messages.GetFullChat({ chatId: ent.id })));
      if (f.ok) return f.value?.fullChat?.participantsCount ?? null;
      return null;
    }
    const f = await safeInvoke(() => client.invoke(new Api.channels.GetFullChannel({ channel: ent })));
    if (f.ok) return f.value?.fullChat?.participantsCount ?? f.value?.fullChat?.subscribersCount ?? null;
  } catch {}
  return null;
}

/* ------------ linked resolve (как было) ------------ */
async function resolveLinkedChat(client, channelEntity){
  // 1) full -> linkedChatId
  const full = await safeInvoke(() => client.invoke(new Api.channels.GetFullChannel({ channel: channelEntity })));
  if (full.flood) return { flood:true, wait_seconds: full.wait_seconds };
  if (full.ok) {
    const linkedId = full.value?.fullChat?.linkedChatId ?? null;
    if (linkedId && (full.value?.chats?.length || full.value?.users?.length)) {
      const pool = [...(full.value.chats||[]), ...(full.value.users||[])];
      const chat = pool.find(c => Number(c?.id ?? -1) === Number(linkedId)) || null;
      if (chat) {
        const members = await readMembersCount(client, chat);
        return {
          method: "full",
          entity: chat,
          title: chat?.title || null,
          username: chat?.username ? `@${chat.username}` : null,
          id: Number(chat?.id) || null,
          access_hash: (typeof chat?.accessHash !== "undefined") ? Number(chat.accessHash) : null,
          members
        };
      }
    }
  }
  // 2) discussion
  const h = await safeInvoke(() => client.invoke(new Api.messages.GetHistory({ peer: channelEntity, limit: 5 })));
  if (h.flood) return { flood:true, wait_seconds: h.wait_seconds };
  if (h.ok) {
    for (const m of (h.value?.messages || [])) {
      const dm = await safeInvoke(() => client.invoke(new Api.messages.GetDiscussionMessage({ peer: channelEntity, msgId: m.id })));
      if (dm.flood) return { flood:true, wait_seconds: dm.wait_seconds };
      if (dm.ok && dm.value?.chats?.length) {
        const chat = dm.value.chats[0];
        const members = await readMembersCount(client, chat);
        return {
          method: "discussion",
          entity: chat,
          title: chat?.title || null,
          username: chat?.username ? `@${chat.username}` : null,
          id: Number(chat?.id) || null,
          access_hash: (typeof chat?.accessHash !== "undefined") ? Number(chat.accessHash) : null,
          members
        };
      }
    }
  }
  // 3) history markers
  const hh = await safeInvoke(() => client.invoke(new Api.messages.GetHistory({ peer: channelEntity, limit: 10 })));
  if (hh.flood) return { flood:true, wait_seconds: hh.wait_seconds };
  if (hh.ok) {
    const msgs = hh.value?.messages || [];
    const has = msgs.some(m => Boolean(m?.replies) || Boolean(m?.replyTo) || (m?.replyToMsgId != null));
    if (has) return { method: "history", entity: null };
  }
  return { method: "none", entity: null };
}

/* ------------ router ------------ */
export default function registerParsing(app) {
  const router = express.Router();

  // POST /v1/parsing/linked/resolve
  router.post("/linked/resolve", async (req, res) => {
    if (!ensureAuth(req, res)) return;
    const t0 = Date.now();
    const { session_name, session_string, api_id, api_hash, username } = req.body || {};
    if (!api_id || !api_hash || !username) return err(res, 400, "BAD_REQUEST", "api_id, api_hash, username are required");

    let client;
    try {
      client = await getClient({ session_name, session_string, api_id, api_hash });
      const ent = await safeInvoke(() => client.getEntity(username));
      if (!ent.ok) throw ent.error;
      const channel = ent.value;

      await joinIfNeeded(client, channel).catch(e => { if (e?.code === "FLOOD_WAIT") throw e; });

      const r = await resolveLinkedChat(client, channel);
      if (r.flood) return err(res, 429, "FLOOD_WAIT", "Too many requests", { wait_seconds: r.wait_seconds });

      if (r.entity) {
        return ok(res, {
          parse_state: "parsable",
          linked_chat_username: r.username,
          linked_chat_title: r.title,
          linked_chat_members: r.members ?? null,
          linked_chat_id: r.id ?? null,
          linked_chat_access_hash: r.access_hash ?? null
        }, { method: r.method, took_ms: Date.now() - t0 });
      }

      if (r.method === "history") {
        return ok(res, {
          parse_state: "parsable",
          linked_chat_username: null,
          linked_chat_title: null,
          linked_chat_members: null,
          linked_chat_id: null,
          linked_chat_access_hash: null
        }, { method: "history", took_ms: Date.now() - t0 });
      }

      return ok(res, {
        parse_state: "NonParsing",
        linked_chat_username: null,
        linked_chat_title: null,
        linked_chat_members: null,
        linked_chat_id: null,
        linked_chat_access_hash: null
      }, { method: "none", took_ms: Date.now() - t0 });

    } catch (e) {
      if (e?.__payload) return res.status(e.__http||500).json(e.__payload);
      const msg = String(e?.message||e);
      const code =
        msg.includes("AUTH_KEY_UNREGISTERED") ? "UNAUTHORIZED" :
        msg.includes("SESSION_PASSWORD_NEEDED") ? "TWO_FACTOR_REQ" :
        msg.includes("CHANNEL_PRIVATE") ? "NOT_FOUND" :
        "INTERNAL";
      return err(res, code==="UNAUTHORIZED"?401:code==="NOT_FOUND"?404:500, code, msg);
    } finally { try { await client?.disconnect(); } catch {} }
  });

  // POST /v1/parsing/members/sample — двойной пробой peer (channel/chat)
  router.post("/members/sample", async (req, res) => {
    if (!ensureAuth(req, res)) return;
    const t0 = Date.now();
    const { session_name, session_string, api_id, api_hash, username, linked_chat_id, linked_chat_access_hash, limits, window_days } = req.body || {};
    if (!api_id || !api_hash) return err(res, 400, "BAD_REQUEST", "api_id and api_hash are required");

    const limHistory = Math.max(1, Math.min(1000, Number(limits?.history ?? 200)));
    const limParticipants = Math.max(1, Math.min(200, Number(limits?.participants ?? 100)));
    const wndDays = Math.max(1, Math.min(365, Number(window_days ?? 30)));
    const oldestTs = Date.now() - wndDays * 24 * 3600 * 1000;

    let client;
    try {
      client = await getClient({ session_name, session_string, api_id, api_hash });

      // Определяем варианты peer
      let variants = [];
      if (Number.isFinite(Number(linked_chat_id))) {
        variants = makePeerVariants({ linked_chat_id, linked_chat_access_hash });
      } else if (username) {
        const ent = await safeInvoke(() => client.getEntity(username));
        if (!ent.ok) throw ent.error;
        variants = [{ kind: "resolved", peer: ent.value }];
        // try to map channel -> linked discussion chat
        try {
          const r = await resolveLinkedChat(client, ent.value);
          if (r && r.entity) { variants.unshift({ kind: "linked", peer: r.entity }); methodTrace.push("linked:from_channel"); }
        } catch (_e) {}
      } else {
        return err(res, 400, "BAD_REQUEST", "Provide username or linked_chat_id");
      }

      let used = null;
      let groupMeta = { title: null, username: null, members_count: null, online_count: null };
      let sample = [];
      let sampled_msgs = 0;
      let methodTrace = [];

      // Перебираем варианты peer до успеха
      for (const v of variants) {
        // Пробуем агрегаты
        const fullCh = await safeInvoke(() => client.invoke(new Api.channels.GetFullChannel({ channel: v.peer })));
        if (fullCh.ok) {
          groupMeta.title = fullCh.value?.chats?.[0]?.title || groupMeta.title;
          groupMeta.username = fullCh.value?.chats?.[0]?.username ? `@${fullCh.value.chats[0].username}` : groupMeta.username;
          groupMeta.members_count = fullCh.value?.fullChat?.participantsCount ?? fullCh.value?.fullChat?.subscribersCount ?? groupMeta.members_count;
          groupMeta.online_count = fullCh.value?.fullChat?.onlineCount ?? groupMeta.online_count;
          used = { kind: v.kind, peer: v.peer };
          methodTrace.push(`${v.kind}:fullChannel`);
        } else {
          // если не канал — пробуем как чат
          if (Number.isFinite(Number(linked_chat_id))) {
            const fc = await safeInvoke(() => client.invoke(new Api.messages.GetFullChat({ chatId: Number(linked_chat_id) })));
            if (fc.ok) {
              groupMeta.title = fc.value?.chats?.[0]?.title || groupMeta.title;
              groupMeta.members_count = fc.value?.fullChat?.participantsCount ?? groupMeta.members_count;
              used = { kind: "chat", peer: v.kind === "chat" ? v.peer : new Api.InputPeerChat({ chatId: Number(linked_chat_id) }) };
              methodTrace.push("chat:fullChat");
            }
          }
        }

        if (!used) continue;

        // На всякий случай — вступаем (для открытых)
        await joinIfNeeded(client, used.peer).catch(() => {});
        methodTrace.push("joinIfNeeded");

        // 1) Пробуем participants
        const part = await safeInvoke(() => client.invoke(new Api.channels.GetParticipants({
          channel: used.peer, filter: new Api.ChannelParticipantsRecent(), offset: 0, limit: Math.min(200, limParticipants), hash: 0
        })));
        if (part.ok && Array.isArray(part.value?.users) && part.value.users.length) {
          sample.push(...part.value.users.map(u => ({ user_id: String(u.id), username: u?.username ? `@${u.username}` : null, is_bot: !!u?.bot })));
          methodTrace.push(`${used.kind}:participants`);
        }

        // 2) Добираем из history в окно
        let offsetId = 0;
        const seen = new Set(sample.map(x => x.user_id));
        for (;;) {
          const h = await safeInvoke(() => client.invoke(new Api.messages.GetHistory({ peer: used.peer, limit: Math.min(200, limHistory), offsetId })));
          if (!h.ok) { methodTrace.push(`${used.kind}:history_fail`); break; }
          const msgs = h.value?.messages || [];
          if (!msgs.length) break;
          sampled_msgs += msgs.length;

          let reachedWindow = false;
          for (const m of msgs) {
            const sec = Number(m?.date || 0);
            const ms = sec ? sec * 1000 : null;
            if (ms && ms < oldestTs) { reachedWindow = true; break; }
            const uid = m?.fromId?.userId ?? m?.peerId?.userId ?? null;
            if (uid != null) {
              const k = String(uid);
              if (!seen.has(k)) { seen.add(k); sample.push({ user_id: k, username: null, is_bot: false }); }
            }
          }
          if (reachedWindow) break;
          offsetId = msgs[msgs.length - 1]?.id || 0;
          if (!offsetId) break;
          if (Date.now() - t0 > 60_000) { methodTrace.push("cutoff60s"); break; }
          await sleep(jitter(BASE_DELAY));
        }

        // если уже что-то нашли — выходим из цикла вариантов
        if (sample.length || sampled_msgs) break;
      }

      const unique_users = new Set(sample.map(x => x.user_id)).size;
      return ok(res, { group: groupMeta, sample }, { sampled_msgs, unique_users, window_days: wndDays, method: methodTrace.join(" -> "), took_ms: Date.now() - t0 });

    } catch (e) {
      if (e?.__payload) return res.status(e.__http||500).json(e.__payload);
      const msg = String(e?.message || e);
      const code =
        msg.includes("CHAT_ADMIN_REQUIRED") ? "UNPROCESSABLE" :
        msg.includes("CHANNEL_PRIVATE") ? "NOT_FOUND" :
        msg.includes("AUTH_KEY_UNREGISTERED") ? "UNAUTHORIZED" :
        "INTERNAL";
      return err(res, code==="NOT_FOUND"?404:code==="UNAUTHORIZED"?401:500, code, msg);
    } finally { try { await client?.disconnect(); } catch {} }
  });

  app.use("/v1/parsing", router);
}
