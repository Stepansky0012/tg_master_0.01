import express from "express";
import { promises as fs } from "fs";
import path from "path";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions/index.js";
import { Api } from "telegram";

const app = express();
app.use(express.json({ limit: "512kb" }));
app.use(express.urlencoded({ extended: true }));

const DATA_DIR = "/app/data";
const SESS_DIR = path.join(DATA_DIR, "sessions");

// ---------- helpers ----------
function ok(res, data = null, meta = null) { res.status(200).json({ success: true, data, meta, error: null }); }
function err(res, http = 500, code = "INTERNAL", message = "Internal error", details = null) { res.status(http).json({ success: false, data: null, meta: null, error: { code, message, details } }); }
function isAuth(req) { const h = req.headers["authorization"] || ""; const token = h.startsWith("Bearer ") ? h.slice(7) : h; return process.env.ADMIN_TOKEN ? token === process.env.ADMIN_TOKEN : true; }
function guard(req, res, next) { if (!isAuth(req)) return err(res, 401, "UNAUTHORIZED", "Missing or invalid ADMIN_TOKEN"); next(); }

// --- debug входа (без секретов) ---
app.use(['/v1/groups/summary','/v1/groups/summary/*'], (req,_res,next)=>{
  try {
    const b = req.body || {};
    const keys = Object.keys(b);
    const types = Object.fromEntries(keys.map(k => [k, Array.isArray(b[k]) ? 'array' : typeof b[k]]));
    console.debug(`[groups/summary][debug] ct=${req.headers['content-type']||''} bodyType=${typeof b} keys=${keys.join(',')} types=${JSON.stringify(types)} len.usernames=${Array.isArray(b.usernames)?b.usernames.length:'n/a'}`);
  } catch {}
  next();
});

// --- n8n ParsingGroups normalizer ---
function parsingGroupsNormalizer(req, _res, next) {
  try {
    const b = req.body || {};
    const aliases = ['ParsingGroups','Parsing_groups','Parsing_grups','usernames'];
    let raw;
    for (const k of aliases) { if (b[k] != null) { raw = b[k]; break; } }

    // [{ "Парсинг групп": ["@a", ...] }]
    if (Array.isArray(raw) && raw.length && typeof raw[0] === 'object') {
      const RU_KEY = 'Парсинг групп';
      const acc = [];
      for (const o of raw) { if (o && typeof o === 'object' && Array.isArray(o[RU_KEY])) acc.push(...o[RU_KEY]); }
      b.usernames = acc;
    }

    const src = b.usernames ?? raw;

    if (typeof src === 'string') {
      const s = src.trim();
      if (s.startsWith('[') && s.endsWith(']')) { try { b.usernames = JSON.parse(s); } catch {} }
      if (!Array.isArray(b.usernames)) { b.usernames = s.split(/[\s,]+/g).filter(Boolean); }
    } else if (Array.isArray(src)) { b.usernames = src; }

    if (Array.isArray(b.usernames)) {
      const seen = new Set();
      b.usernames = b.usernames
        .map(x => typeof x === 'string' ? x.trim() : '')
        .filter(Boolean)
        .map(x => x.startsWith('@') ? x : '@' + x.replace(/^@+/, ''))
        .map(x => x.toLowerCase())
        .filter(u => { if (seen.has(u)) return false; seen.add(u); return true; });
    }
    req.body = b;
  } catch {}
  next();
}
app.use('/v1/groups/summary', parsingGroupsNormalizer);
app.use('/v1/groups/summary/*', parsingGroupsNormalizer);

// ---------- extractors ----------
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
function extractMembersCount(full) {
  const fc = full?.fullChat;
  const cands = [fc?.participantsCount, fc?.subscribersCount].filter(v => Number.isFinite(v) && v >= 0);
  return cands.length ? cands[0] : null;
}
function extractOnlineCount(full, baseType) {
  const v = full?.fullChat?.onlineCount;
  if ((baseType === "supergroup" || baseType === "group") && Number.isFinite(v) && v >= 0) return v;
  return null;
}
function extractLinkedChatUsername(full) {
  try {
    const fc = full?.fullChat;
    const linkedId = fc?.linkedChatId;
    if (!linkedId) return null;
    const list = [...(full?.chats || []), ...(full?.users || [])];
    const found = list.find(x => (x?.id === linkedId) || (x?.chat && x.chat.id === linkedId));
    const u = found?.username || found?.userName;
    return u ? (u.startsWith("@") ? u : "@" + u) : null;
  } catch { return null; }
}

// ---------- routes ----------
app.get("/v1/health", (_req, res) => ok(res, { status: "ok", service: "tg-groups" }));

app.post(["/v1/groups/summary", "/v1/groups/summary/*"], guard, async (req, res) => {
  const t0 = Date.now();
  const { session_name, usernames } = req.body || {};
  if (!session_name) return err(res, 400, "BAD_REQUEST", "session_name is required");
  if (!Array.isArray(usernames) || usernames.length === 0) {
    return err(res, 400, "BAD_REQUEST", "usernames is required and must be a non-empty array");
  }

  // env для Telegram
  const apiId = Number(process.env.TG_API_ID || "");
  const apiHash = String(process.env.TG_API_HASH || "");
  if (!Number.isFinite(apiId) || !apiHash) {
    return err(res, 500, "INTERNAL", "TG_API_ID and TG_API_HASH env variables are required");
  }

  // Проверка наличия файла сессии
  let sessionString;
  try {
    const p = path.join(SESS_DIR, `${session_name}.json`);
    const raw = JSON.parse(await fs.readFile(p, "utf8"));
    sessionString = raw?.session_string;
    if (!sessionString) return err(res, 404, "NOT_FOUND", `session_name "${session_name}" file is invalid or missing session_string`);
  } catch {
    return err(res, 404, "NOT_FOUND", `session_name "${session_name}" not found`);
  }

  const client = new TelegramClient(new StringSession(sessionString), apiId, apiHash, { connectionRetries: 3 });
  try {
    await client.connect();
  } catch (e) {
    const msg = String(e?.message || e);
    if (msg.includes("FLOOD_WAIT")) return err(res, 429, "FLOOD_WAIT", msg);
    return err(res, 502, "TELEGRAM_DOWN", msg);
  }

  const items = [];
  let okCount = 0, failCount = 0;

  for (const uname of usernames) {
    try {
      const entity = await client.getEntity(uname);
      if (!entity) {
        items.push({ username: uname, type: null, title: null, members_count: null, status: "not_found", linked_chat_username: null, online_count: null });
        failCount++; continue;
      }
      const baseType = baseTypeFromEntity(entity);

      let full = null;
      if (entity.className === "Channel" || entity.className === "ChannelForbidden") {
        try {
          full = await client.invoke(new Api.channels.GetFullChannel({ channel: entity }));
        } catch {
          try { full = await client.invoke(new Api.messages.GetFullChat({ chatId: entity.id })); } catch {}
        }
      } else if (entity.className === "Chat" || entity.className === "ChatForbidden") {
        try { full = await client.invoke(new Api.messages.GetFullChat({ chatId: entity.id })); } catch {}
      }

      const title = entity.title || entity.firstName || entity.username || uname;
      const members_count = extractMembersCount(full);
      const online_count = extractOnlineCount(full, baseType);
      const linked_chat_username = extractLinkedChatUsername(full);
      const status = entity.username ? "public" : "private";

      items.push({
        username: uname,
        type: baseType,
        title: title || null,
        members_count: Number.isFinite(members_count) ? members_count : null,
        status,
        linked_chat_username,
        online_count: Number.isFinite(online_count) ? online_count : null
      });
      okCount++;
    } catch (e) {
      const msg = String(e?.message || e);
      if (msg.includes("FLOOD_WAIT")) {
        const secs = Number((msg.match(/FLOOD_WAIT_(\d+)/) || [])[1] || 0);
        items.push({ username: uname, type: null, title: null, members_count: null, status: "error", linked_chat_username: null, online_count: null, error: `FLOOD_WAIT_${secs}` });
        failCount++; continue;
      }
      if (msg.includes("USERNAME_INVALID") || msg.includes("USERNAME_NOT_OCCUPIED") || msg.includes("CHANNEL_PRIVATE")) {
        items.push({ username: uname, type: null, title: null, members_count: null, status: "not_found", linked_chat_username: null, online_count: null });
        failCount++; continue;
      }
      items.push({ username: uname, type: null, title: null, members_count: null, status: "error", linked_chat_username: null, online_count: null });
      failCount++;
    }
  }

  try { await client.disconnect(); } catch {}

  const meta = { processed: usernames.length, ok: okCount, failed: failCount, took_ms: Date.now() - t0 };
  return ok(res, { items }, meta);
});

const PORT = Number(process.env.PORT || 8081);
app.listen(PORT, () => console.log(`tg-groups listening on :${PORT}`));
