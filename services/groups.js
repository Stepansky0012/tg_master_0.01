import express from "express";
import { promises as fs } from "fs";
import path from "path";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions/index.js";
import { Api } from "telegram";

/* ----------------- debug logger ----------------- */
const DBG = String(process.env.DEBUG_GROUPS || "");
const dlog = (...a) => { if (DBG && DBG !== "0" && DBG !== "false") console.log("[groups][debug]", ...a); };

/* ----------------- helpers: reply & auth ----------------- */
function ok(res, data = null, meta = null) { res.status(200).json({ success: true, data, meta, error: null }); }
function err(res, http = 500, code = "INTERNAL", message = "Internal error", details = null) { res.status(http).json({ success: false, data: null, meta: null, error: { code, message, details } }); }
function isAuth(req) { const h = req.headers["authorization"] || ""; const token = h.startsWith("Bearer ") ? h.slice(7) : h; return process.env.ADMIN_TOKEN ? token === process.env.ADMIN_TOKEN : true; }
function guard(req, res, next) { if (!isAuth(req)) return err(res, 401, "UNAUTHORIZED", "Missing or invalid ADMIN_TOKEN"); next(); }

/* ----------------- input normalizer for n8n ----------------- */
function parsingGroupsNormalizer(req, _res, next) {
  try {
    const b = req.body || {};
    const aliases = ["ParsingGroups","Parsing_groups","Parsing_grups","usernames","username"];
    let raw; for (const k of aliases) if (b[k] != null) { raw = b[k]; break; }

    if (Array.isArray(raw) && raw.length && typeof raw[0] === "object") {
      const RU_KEY = "Парсинг групп"; const acc = [];
      for (const o of raw) if (o && typeof o === "object" && Array.isArray(o[RU_KEY])) acc.push(...o[RU_KEY]);
      b.usernames = acc;
    }

    const src = b.usernames ?? raw;
    if (typeof src === "string") {
      const s = src.trim();
      if (s.startsWith("[" ) && s.endsWith("]")) { try { b.usernames = JSON.parse(s); } catch {} }
      if (!Array.isArray(b.usernames)) b.usernames = s.split(/[\s,]+/g).filter(Boolean);
    } else if (Array.isArray(src)) { b.usernames = src; }
    else if (typeof b.username === "string") { b.usernames = [b.username]; }

    if (Array.isArray(b.usernames)) {
      const seen = new Set();
      b.usernames = b.usernames
        .map(x => typeof x === "string" ? x.trim() : "")
        .filter(Boolean)
        .map(x => x.startsWith("@") ? x : "@" + x.replace(/^@+/, ""))
        .map(x => x.toLowerCase())
        .filter(u => (seen.has(u) ? false : (seen.add(u), true)));
    }
    req.body = b;
  } catch {}
  next();
}

/* ----------------- Telegram helpers ----------------- */
function baseTypeFromEntity(e) {
  if (!e) return null;
  if (e.className === "Channel" || e.className === "ChannelForbidden") {
    if (e.megagroup || e.gigagroup) return "supergroup";
    if (e.broadcast) return "channel";
    return "group";
  }
  if (e.className === "Chat" || e.className === "ChatForbidden") return "group";
  return null;
}
const sleep = (ms)=>new Promise(r=>setTimeout(r,ms));
const jitter=(b)=>b+Math.floor(Math.random()*120);
async function safeInvoke(client, query, {delayMs=Number(process.env.SAFE_BASE_DELAY_MS||950)}={}) {
  try {
    const res = await client.invoke(query);
    await sleep(jitter(delayMs));
    return res;
  } catch (e) {
    const msg = String(e?.message||"");
    const m = msg.match(/FLOOD_WAIT_(\d+)/i);
    if (m) { const secs = parseInt(m[1],10)||0; const er = new Error("FLOOD_WAIT"); er.code="FLOOD_WAIT"; er.wait_seconds=secs; throw er; }
    throw e;
  }
}

/* join channel if needed (public), ignore benign errors */
async function joinIfNeeded(client, channelEnt){
  try {
    dlog("JoinChannel try:", channelEnt?.username || channelEnt?.id);
    await safeInvoke(client, new Api.channels.JoinChannel({ channel: channelEnt }));
    dlog("JoinChannel ok");
  } catch(e){
    const msg = String(e?.message||"");
    dlog("JoinChannel err:", msg);
    if (/(USER_ALREADY_PARTICIPANT|INVITE_HASH_INVALID|CHANNEL_PRIVATE|CHANNELS_TOO_MUCH)/.test(msg)) return;
    if (/FLOOD_WAIT_(\d+)/.test(msg)) { const secs = parseInt(msg.match(/FLOOD_WAIT_(\d+)/)[1],10)||0; const er=new Error("FLOOD_WAIT"); er.code="FLOOD_WAIT"; er.wait_seconds=secs; throw er; }
  }
}

async function readMembersCount(client, ent){
  if (!ent) return null;
  if (ent.className === "Chat" || ent.className === "ChatForbidden") {
    try { const f = await safeInvoke(client, new Api.messages.GetFullChat({ chatId: ent.id })); return f?.fullChat?.participantsCount ?? null; } catch { return null; }
  }
  if (ent.className === "Channel" || ent.className === "ChannelForbidden") {
    try { const f = await safeInvoke(client, new Api.channels.GetFullChannel({ channel: ent })); return f?.fullChat?.participantsCount ?? f?.fullChat?.subscribersCount ?? null; } catch { return null; }
  }
  return null;
}

/* улучшенная: через full.chats, а если нет — через DiscussionMessage */
async function getLinkedChatInfo(client, channelEntity) {
  try {
    dlog("GetFullChannel for", channelEntity?.username || channelEntity?.id);
    const full = await safeInvoke(client, new Api.channels.GetFullChannel({ channel: channelEntity }));
    const linkedId = full?.fullChat?.linkedChatId;
    dlog("GetFullChannel linkedChatId:", linkedId || null);
    if (!linkedId) return null;

    // 1) Пробуем достать сущность напрямую из full.chats
    try {
      const pool = [...(full?.chats || []), ...(full?.users || [])];
      const chat = pool.find(c => (c?.id === linkedId) || (c?.chat && c.chat.id === linkedId)) || null;
      if (chat) {
        dlog("linked chat found in full.chats:", chat?.id, chat?.username || null);
        const members = await readMembersCount(client, chat);
        return {
          linked_chat_username: chat.username ? `@${chat.username}` : null,
          linked_chat_title: chat.title || null,
          linked_chat_members: members,
          linked_chat_id: Number(chat.id) || null,
          linked_chat_access_hash: (typeof chat.accessHash !== undefined ? Number(chat.accessHash) : null)
        };
      }
    } catch (e) {
      dlog("lookup in full.chats failed:", String(e?.message||e));
    }

    // 2) Если full.chats не дал сущность — достаём через DiscussionMessage (берём любой пост канала)
    try {
      dlog("fallback via GetDiscussionMessage scan");
      const h = await safeInvoke(client, new Api.messages.GetHistory({ peer: channelEntity, limit: 5 }));
      const msgs = [...(h.messages || [])];
      for (const m of msgs) {
        const mid = m?.id; if (!mid) continue;
        try {
          const dm = await safeInvoke(client, new Api.messages.GetDiscussionMessage({ peer: channelEntity, msgId: mid }));
          const chat = (dm.chats || [])[0];
          if (!chat) continue;
          dlog("discussion_message gave chat:", chat?.id, chat?.username || null);
          const members = await readMembersCount(client, chat);
          return {
            linked_chat_username: chat.username ? `@${chat.username}` : null,
            linked_chat_title: chat.title || null,
            linked_chat_members: members,
          linked_chat_id: Number(chat.id) || null,
          linked_chat_access_hash: (typeof chat.accessHash !== undefined ? Number(chat.accessHash) : null)
          };
        } catch (e) {
          dlog("GetDiscussionMessage miss for msg", mid, "->", String(e?.message||e));
        }
      }
    } catch (e) {
      dlog("history pre-scan error:", String(e?.message||e));
    }

    // 3) В крайнем случае — считаем parsable без деталей (знаем, что linked есть)
    dlog("linkedChatId known but no entity fetched -> parsable with nulls");
    return { linked_chat_username: null, linked_chat_title: null, linked_chat_members: null,
          linked_chat_id: null,
          linked_chat_access_hash: null
        };
  } catch (e) { dlog("GetFullChannel error:", String(e?.message||e)); return null; }
}

/* fallback: по истории сообщений каналов (маркер обсуждений) */
async function hasDiscussionByHistory(client, channelEntity) {
  try {
    dlog("GetHistory for", channelEntity?.username || channelEntity?.id);
    const h = await safeInvoke(client, new Api.messages.GetHistory({ peer: channelEntity, limit: 10 }));
    const msgs = [...(h.messages || [])];
    for (const m of msgs) {
      const r = m?.replies;
      if (r && (Number.isFinite(r.replies) || r.comments)) { dlog("history indicates discussion via replies/comments"); return true; }
      if (m?.replyTo) { dlog("history indicates discussion via replyTo"); return true; }
    }
    dlog("history shows no discussion markers");
    return false;
  } catch (e) { dlog("GetHistory error:", String(e?.message||e)); return false; }
}

/* resolve */
async function resolveByUsername(client, uname) {
  const u = uname.replace(/^@/, "");
  try {
    dlog("ResolveUsername:", u);
    const r = await safeInvoke(client, new Api.contacts.ResolveUsername({ username: u }));
    const ent = r.channels?.[0] || r.chats?.[0] || null;
    dlog("ResolveUsername ->", ent?.className || null, ent?.id || null, ent?.username || null);
    return ent;
  } catch (e) {
    dlog("ResolveUsername error, fallback getEntity:", String(e?.message||e));
    try { const ent = await client.getEntity(uname); dlog("getEntity ->", ent?.className || null, ent?.id || null); return ent; } catch (ee) { dlog("getEntity error:", String(ee?.message||ee)); return null; }
  }
}

/* ----------------- session connect ----------------- */
async function connectByBody(req) {
  const { api_id, api_hash, session_name, session_string } = req.body || {};
  const apiId = Number(api_id || process.env.TG_API_ID || "");
  const apiHash = String(api_hash || process.env.TG_API_HASH || "");
  if (!Number.isFinite(apiId) || !apiHash) throw new Error("TG_API_ID and TG_API_HASH required (body or env)");

  let str = session_string || "";
  if (!str && session_name) {
    const DATA_DIR = "/app/data"; const SESS_DIR = path.join(DATA_DIR, "sessions");
    try {
      const p = path.join(SESS_DIR, `${session_name}.json`);
      const raw = JSON.parse(await fs.readFile(p, "utf8"));
      str = raw?.session_string || "";
    } catch {}
  }

  dlog("connect with api_id:", apiId, "session_string:", str ? "present" : "empty", "session_name:", session_name || null);
  const client = new TelegramClient(new StringSession(str||""), apiId, apiHash, { connectionRetries: 3 });
  await client.connect();
  return client;
}

/* ----------------- core classification ----------------- */
async function classifyOne(client, uname) {
  dlog("=== classify start:", uname, "===");
  const entity = await resolveByUsername(client, uname);
  if (!entity) { dlog("no entity -> NonParsing"); return { username: uname, parse_state: "NonParsing" }; }
  const t = baseTypeFromEntity(entity);
  dlog("entity baseType:", t, "class:", entity.className, "id:", entity.id, "username:", entity.username || null);

  // groups/supergroups — parsable сами по себе
  if (t === "group" || t === "supergroup") {
    const members = await readMembersCount(client, entity);
    dlog("group/supergroup -> parsable; members:", members);
    return {
      username: uname,
      parse_state: "parsable",
      linked_chat_username: entity.username ? `@${entity.username}` : null,
      linked_chat_title: entity.title || null,
      linked_chat_members: members,
          linked_chat_id: Number(chat.id) || null,
          linked_chat_access_hash: (typeof chat.accessHash !== undefined ? Number(chat.accessHash) : null)
    };
  }

  // channel — join -> GetFullChannel(+full.chats) -> (если надо) DiscussionMessage
  if (t === "channel") {
    await joinIfNeeded(client, entity);

    const info1 = await getLinkedChatInfo(client, entity);
    if (info1) { dlog("channel linked -> parsable", info1); return { username: uname, parse_state: "parsable", ...info1 }; }

    const hasDiscuss = await hasDiscussionByHistory(client, entity);
    if (hasDiscuss) {
      dlog("history shows discussions -> parsable (no linked id)");
      return { username: uname, parse_state: "parsable", linked_chat_username: null, linked_chat_title: null, linked_chat_members: null,
          linked_chat_id: null,
          linked_chat_access_hash: null
        };
    }

    dlog("channel -> NonParsing (no linked, no discussion)");
    return { username: uname, parse_state: "NonParsing" };
  }

  dlog("unknown type -> NonParsing");
  return { username: uname, parse_state: "NonParsing" };
}

/* ----------------- register routes ----------------- */
export default function registerGroups(app) {
  app.use("/v1/groups/summary", parsingGroupsNormalizer);
  app.use("/v1/groups/summary/*", parsingGroupsNormalizer);

  app.post(["/v1/groups/summary", "/v1/groups/summary/*"], guard, async (req, res) => {
    const { usernames } = req.body || {};
    if (!Array.isArray(usernames) || usernames.length === 0) return err(res, 400, "BAD_REQUEST", "usernames is required and must be a non-empty array");

    let client;
    try { client = await connectByBody(req); }
    catch (e) { return err(res, 400, "BAD_REQUEST", String(e?.message || e)); }

    try {
      const items = [];
      for (const uname of usernames) {
        try {
          items.push(await classifyOne(client, uname));
        } catch (e) {
          if (e?.code === "FLOOD_WAIT") return err(res, 429, "FLOOD_WAIT", "Rate limited by Telegram", { wait_seconds: e.wait_seconds });
          items.push({ username: uname, parse_state: "NonParsing" });
        }
      }
      return ok(res, { items }, { count: items.length });
    } catch (e) {
      return err(res, 500, "INTERNAL", String(e?.message || e));
    } finally {
      try { await client.disconnect(); } catch {}
    }
  });
}

/* ----------------- optional standalone ----------------- */
if (process.env.STANDALONE_GROUPS === "1") {
  const app = express();
  app.use(express.json({ limit: "512kb" }));
  app.use(express.urlencoded({ extended: true }));
  const register = (await import(new URL(import.meta.url).pathname)).default;
  register(app);
  const PORT = Number(process.env.PORT || 8081);
  app.listen(PORT, () => console.log(`groups module (standalone) on :${PORT}`));
}
