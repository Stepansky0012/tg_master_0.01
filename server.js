import registerParsing from "./services/parsing.js";
import registerGroups from "./services/groups.js";
import registerChecker from "./checker.js";
import registerConverter from "./converter.js";
import registerAccountManager from "./account_manager.js";
import express from "express";
import { promises as fs } from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions/index.js";
import { Api } from "telegram";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DATA_DIR = "/app/data";
const SESS_DIR = path.join(DATA_DIR, "sessions");

const app = express();
app.use(express.json({ limit: "512kb" }));

// === Модули ===
registerAccountManager(app);   // /v1/auth/* (с SRP-2FA)
registerChecker(app);          // /v1/checker/*
registerConverter(app);        // /v1/converter/*

// === helpers ===
async function ensureDirs() { await fs.mkdir(SESS_DIR, { recursive: true }); }
function ok(res, data = null, meta = null) { res.status(200).json({ success: true, data, meta, error: null }); }
function err(res, http = 500, code = "INTERNAL", message = "Internal error", details = null) { res.status(http).json({ success: false, data: null, meta: null, error: { code, message, details } }); }
function isAuth(req) { const h = req.headers["authorization"] || ""; const token = h.startsWith("Bearer ") ? h.slice(7) : h; return process.env.ADMIN_TOKEN ? token === process.env.ADMIN_TOKEN : true; }
function guard(req, res, next) { if (!isAuth(req)) return err(res, 401, "UNAUTHORIZED", "Missing or invalid ADMIN_TOKEN"); next(); }

// === health ===
app.get("/v1/health", (_req, res) => ok(res, { status: "ok" }));

const PORT = Number(process.env.PORT || 8080);
await ensureDirs();

// === Совместимость: /v1/groups/summary_legacy ===
app.post("/v1/groups/summary_legacy", guard, async (req, res) => {
  const t0 = Date.now();
  try {
    const { session_name, api_id, api_hash } = req.body || {};
    if (!api_id || !api_hash) return err(res, 400, "BAD_REQUEST", "api_id and api_hash are required");

    // usernames / Parsing_groups нормализация
    let usernames = [];
    if (Array.isArray(req.body?.usernames)) usernames = req.body.usernames;
    else if (req.body?.Parsing_groups || req.body?.Parsing_grups) {
      const pg = (req.body.Parsing_groups ?? req.body.Parsing_grups);
      if (Array.isArray(pg)) usernames = pg;
      else if (typeof pg === "string") usernames = pg.split(/[\s,\n\r\t]+/).filter(Boolean);
    }
    usernames = usernames.map(u => (u||"").toString().trim()).filter(Boolean).map(u => u.startsWith("@") ? u : ("@"+u));
    if (!usernames.length) return err(res, 400, "BAD_REQUEST", "usernames (or Parsing_groups) is required");

    await ensureDirs();

    // session_string
    let sessString = null;
    try {
      if (session_name) {
        const raw = JSON.parse(await fs.readFile(path.join(SESS_DIR, session_name + ".json"), "utf8"));
        sessString = raw.session_string;
      } else {
        const files = (await fs.readdir(SESS_DIR)).filter(f => f.endsWith(".json"));
        if (files.length === 1) sessString = JSON.parse(await fs.readFile(path.join(SESS_DIR, files[0]), "utf8")).session_string;
      }
    } catch {}

    if (!sessString) return err(res, 400, "BAD_REQUEST", "session_name is required (or keep exactly one saved session)");

    const client = new TelegramClient(new StringSession(sessString), Number(api_id), String(api_hash), { connectionRetries: 2 });
    await client.connect();

    const items = [];
    let okCount = 0, failCount = 0;

    for (const uname of usernames) {
      const out = { username: uname, type: null, title: null, members_count: null, status: "not_found", linked_chat_username: null, online_count: null };
      try {
        const entity = await client.getEntity(uname).catch(() => null);
        if (!entity) { items.push(out); failCount++; continue; }

        // тип
        let type = "group";
        if (entity?.gigagroup || entity?.megagroup) type = "supergroup";
        if (entity?.broadcast || entity?.__className?.includes("Channel")) type = "channel";
        out.type = type; out.title = entity.title || entity.firstName || entity.username || uname;

        // подробности
        let linkedChatUsername = null; let membersCount = null; let status = "public";
        try {
          const input = await client.getInputEntity(entity);
          const full = await client.invoke(new Api.channels.GetFullChannel({ channel: input }));
          const fc = full?.fullChat;
          membersCount = (fc?.participantsCount ?? fc?.subscribersCount ?? null);
          if (typeof fc?.onlineCount === "number") out.online_count = fc.onlineCount;
          if (full?.chats && fc?.linkedChatId) {
            const linked = full.chats.find(c => String(c.id) === String(fc.linkedChatId));
            linkedChatUsername = linked?.username ? ("@"+linked.username) : null;
          }
        } catch {
          try {
            const chatId = entity?.id;
            if (chatId) {
              const full2 = await client.invoke(new Api.messages.GetFullChat({ chatId }));
              const fc2 = full2?.fullChat;
              membersCount = (fc2?.participantsCount ?? null);
            }
          } catch {}
        }

        out.members_count = (typeof membersCount === "number" ? membersCount : null);
        out.linked_chat_username = linkedChatUsername;
        out.status = status;

        items.push(out); okCount++;
      } catch { items.push(out); failCount++; }
    }

    await client.disconnect();
    ok(res, { items }, { processed: items.length, ok: okCount, failed: failCount, took_ms: Date.now() - t0 });
  } catch (e) {
    const msg = String((e && e.message) || e);
    if (msg.includes("FLOOD_WAIT")) return err(res, 429, "FLOOD_WAIT", msg);
    return err(res, 500, "INTERNAL", msg);
  }
});

// Регистрируем расширенные сервисы
registerParsing(app);

// Запуск
app.listen(PORT, () => console.log("tg-master auth listening on :" + PORT));

// === n8n normalizer для /v1/groups/summary ===
app.use("/v1/groups/summary", (req, _res, next) => {
  try {
    const b = req.body || {};
    const aliases = ["ParsingGroups","Parsing_groups","Parsing_grups","usernames"];
    let raw;
    for (const k of aliases) if (b[k] != null) { raw = b[k]; break; }

    if (Array.isArray(raw) && raw.length && typeof raw[0] === "object") {
      const RU_KEY = "Парсинг групп"; const acc = [];
      for (const obj of raw) if (obj && typeof obj === "object" && Array.isArray(obj[RU_KEY])) acc.push(...obj[RU_KEY]);
      b.usernames = acc; req.body = b;
    }

    const listSrc = b.usernames ?? raw;
    if (typeof listSrc === "string") {
      const s = listSrc.trim();
      if (s.startsWith("[") && s.endsWith("]")) { try { b.usernames = JSON.parse(s); } catch {} }
      if (!Array.isArray(b.usernames)) b.usernames = s.split(/[,\s]+/g).filter(Boolean);
    } else if (Array.isArray(listSrc)) {
      b.usernames = listSrc;
    }

    if (Array.isArray(b.usernames)) {
      b.usernames = b.usernames
        .map(x => typeof x === "string" ? x.trim() : "")
        .filter(Boolean)
        .map(x => x.startsWith("@") ? x : "@" + x.replace(/^@+/, ""))
        .map(x => x.toLowerCase());
    }

    req.body = b;
  } catch {}
  next();
});

// /v1/groups/summary — современный модуль
registerGroups(app);

// Фильтр TIMEOUT из GramJS
const __origCE = console.error.bind(console);
console.error = (...args) => {
  try {
    const msg = args.map(x => (x && x.stack) ? x.stack : String(x)).join(" ");
    if (msg.includes("telegram/client/updates.js") && msg.includes("TIMEOUT")) return;
  } catch {}
  return __origCE(...args);
};
