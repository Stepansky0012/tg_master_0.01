import express from "express";
import { promises as fs } from "fs";
import path from "path";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions/index.js";
import { Api } from "telegram";
import { customAlphabet } from "nanoid";
import { computeCheck } from "telegram/Password.js"; // SRP helper

const nano = customAlphabet("abcdefghijklmnopqrstuvwxyz0123456789", 10);

const DATA_DIR = "/app/data";
const SESS_DIR = path.join(DATA_DIR, "sessions");

async function ensureDirs() { await fs.mkdir(SESS_DIR, { recursive: true }); }
function ok(res, data = null, meta = null) { res.status(200).json({ success: true, data, meta, error: null }); }
function err(res, http = 500, code = "INTERNAL", message = "Internal error", details = null) { res.status(http).json({ success: false, data: null, meta: null, error: { code, message, details } }); }
function isAuth(req) { const h = req.headers["authorization"] || ""; const token = h.startsWith("Bearer ") ? h.slice(7) : h; return process.env.ADMIN_TOKEN ? token === process.env.ADMIN_TOKEN : true; }
function guard(req, res, next) { if (!isAuth(req)) return err(res, 401, "UNAUTHORIZED", "Missing or invalid ADMIN_TOKEN"); next(); }
function validName(name) { return /^[a-zA-Z0-9._-]{1,64}$/.test(String(name||"")); }

// challenges for login flow
const challenges = new Map();
const CH_TTL_MS = 10 * 60 * 1000;
setInterval(() => {
  const now = Date.now();
  for (const [id, ch] of challenges) if (now - ch.createdAt > CH_TTL_MS) challenges.delete(id);
}, 60 * 1000);

export default function registerAccountManager(app) {
  const router = express.Router();
  router.use(express.json({ limit: "512kb" }));

  // --- list sessions
  router.get("/session", async (_req, res) => {
    try {
      await ensureDirs();
      const files = await fs.readdir(SESS_DIR);
      const items = [];
      for (const f of files) {
        if (!f.endsWith(".json")) continue;
        const raw = JSON.parse(await fs.readFile(path.join(SESS_DIR, f), "utf8"));
        items.push({ name: raw.name, created_at: raw.created_at });
      }
      ok(res, { items });
    } catch (e) { err(res, 500, "INTERNAL", String(e)); }
  });

  // --- save ready SessionString
  router.post("/session", guard, async (req, res) => {
    try {
      const { session_string, session_name } = req.body || {};
      if (!session_string) return err(res, 400, "BAD_REQUEST", "session_string is required");
      const name = session_name && validName(session_name) ? session_name : `sess-${nano()}`;
      await ensureDirs();
      const p = path.join(SESS_DIR, name + ".json");
      await fs.writeFile(p, JSON.stringify({ name, session_string, created_at: new Date().toISOString() }, null, 2));
      ok(res, { session_name: name });
    } catch (e) { err(res, 500, "INTERNAL", String(e)); }
  });

  // --- delete session
  router.delete("/session", guard, async (req, res) => {
    try {
      const { session_name } = req.body || {};
      if (!session_name) return err(res, 400, "BAD_REQUEST", "session_name is required");
      const p = path.join(SESS_DIR, session_name + ".json");
      await fs.unlink(p).catch(() => { throw new Error("NOT_FOUND"); });
      ok(res, { deleted: session_name });
    } catch (e) {
      const msg = String(e?.message || e);
      if (msg === "NOT_FOUND") return err(res, 404, "NOT_FOUND", "session not found");
      return err(res, 500, "INTERNAL", msg);
    }
  });

  // --- start login (send code)
  router.post("/login/start", guard, async (req, res) => {
    try {
      const { phone, api_id, api_hash } = req.body || {};
      if (!phone || !api_id || !api_hash) return err(res, 400, "BAD_REQUEST", "phone, api_id, api_hash are required");
      const client = new TelegramClient(new StringSession(""), Number(api_id), String(api_hash), { connectionRetries: 5 });
      await client.connect();
      const r = await client.invoke(new Api.auth.SendCode({
        phoneNumber: String(phone),
        apiId: Number(api_id),
        apiHash: String(api_hash),
        settings: new Api.CodeSettings({})
      }));
      const tmpSession = client.session.save();
      await client.disconnect();

      const challenge_id = nano();
      challenges.set(challenge_id, {
        createdAt: Date.now(),
        phone: String(phone),
        api_id: Number(api_id),
        api_hash: String(api_hash),
        phoneCodeHash: r.phoneCodeHash,
        tmpSession
      });

      ok(res, { challenge_id });
    } catch (e) { err(res, 500, "INTERNAL", String(e)); }
  });

  // --- verify code (may require 2FA)
  router.post("/login/verify_code", guard, async (req, res) => {
    try {
      const { challenge_id, code, session_name } = req.body || {};
      if (!challenge_id || !code) return err(res, 400, "BAD_REQUEST", "challenge_id and code are required");
      const ch = challenges.get(challenge_id);
      if (!ch) return err(res, 404, "NOT_FOUND", "challenge not found or expired");

      const client = new TelegramClient(new StringSession(ch.tmpSession), ch.api_id, ch.api_hash, { connectionRetries: 5 });
      await client.connect();
      try {
        await client.invoke(new Api.auth.SignIn({
          phoneNumber: ch.phone,
          phoneCodeHash: ch.phoneCodeHash,
          phoneCode: String(code)
        }));
      } catch (e) {
        const msg = String(e?.message || e);
        if (msg.includes("SESSION_PASSWORD_NEEDED")) {
          return err(res, 403, "TWO_FACTOR_REQ", "2FA password required", { challenge_id });
        }
        throw e;
      }

      const sessionString = client.session.save();
      await ensureDirs();
      const name = session_name && validName(session_name) ? session_name : `sess-${nano()}`;
      await fs.writeFile(path.join(SESS_DIR, name + ".json"), JSON.stringify({ name, session_string: sessionString, created_at: new Date().toISOString() }, null, 2));
      challenges.delete(challenge_id);
      ok(res, { session_name: name, session_string: sessionString });
    } catch (e) { err(res, 500, "INTERNAL", String(e)); }
  });

  // --- verify 2FA password (SRP)
  router.post("/login/verify_password", guard, async (req, res) => {
    try {
      const { challenge_id, password, session_name } = req.body || {};
      if (!challenge_id || !password) return err(res, 400, "BAD_REQUEST", "challenge_id and password are required");
      const ch = challenges.get(challenge_id);
      if (!ch) return err(res, 404, "NOT_FOUND", "challenge not found or expired");

      const client = new TelegramClient(new StringSession(ch.tmpSession), ch.api_id, ch.api_hash, { connectionRetries: 5 });
      await client.connect();

      // SRP flow
      const pwd = await client.invoke(new Api.account.GetPassword());
      const { srpId, A, M1 } = await computeCheck(pwd, String(password));
      await client.invoke(new Api.auth.CheckPassword({ password: new Api.InputCheckPasswordSRP({ srpId, A, M1 }) }));

      const sessionString = client.session.save();
      await ensureDirs();
      const name = session_name && validName(session_name) ? session_name : `sess-${nano()}`;
      await fs.writeFile(path.join(SESS_DIR, name + ".json"), JSON.stringify({ name, session_string: sessionString, created_at: new Date().toISOString() }, null, 2));
      challenges.delete(challenge_id);
      ok(res, { session_name: name, session_string: sessionString });
    } catch (e) {
      const msg = String(e?.message || e);
      if (msg.includes("SRP_ID_INVALID") || msg.includes("PASSWORD_HASH_INVALID")) {
        return err(res, 400, "BAD_REQUEST", "Invalid 2FA password");
      }
      err(res, 500, "INTERNAL", msg);
    }
  });

  // --- check one session
  router.post("/session/check", guard, async (req, res) => {
    try {
      const { session_name, api_id, api_hash } = req.body || {};
      if (!session_name || !api_id || !api_hash) return err(res, 400, "BAD_REQUEST", "session_name, api_id, api_hash are required");
      await ensureDirs();
      const raw = JSON.parse(await fs.readFile(path.join(SESS_DIR, session_name + ".json"), "utf8"));
      const client = new TelegramClient(new StringSession(raw.session_string), Number(api_id), String(api_hash), { connectionRetries: 1 });
      await client.connect();
      const me = await client.getMe();
      await client.disconnect();
      ok(res, { session_name, ok: true, user_id: me.id, username: me.username || null });
    } catch (e) {
      const msg = String(e?.message || e);
      if (msg.includes("AUTH_KEY_UNREGISTERED")) return err(res, 401, "UNAUTHORIZED", "Session is invalid or revoked");
      if (msg.includes("FLOOD_WAIT")) return err(res, 429, "FLOOD_WAIT", msg);
      return err(res, 500, "INTERNAL", msg);
    }
  });

  // --- summarize all sessions
  router.post("/session/summary", guard, async (req, res) => {
    try {
      const { api_id, api_hash } = req.body || {};
      if (!api_id || !api_hash) return err(res, 400, "BAD_REQUEST", "api_id and api_hash are required");
      await ensureDirs();
      const files = (await fs.readdir(SESS_DIR)).filter(f => f.endsWith(".json"));
      const items = [];
      for (const f of files) {
        try {
          const raw = JSON.parse(await fs.readFile(path.join(SESS_DIR, f), "utf8"));
          let status = "invalid", user_id = null, username = null;
          try {
            const client = new TelegramClient(new StringSession(raw.session_string), Number(api_id), String(api_hash), { connectionRetries: 1 });
            await client.connect();
            const me = await client.getMe();
            await client.disconnect();
            status = "ok"; user_id = me.id; username = me.username || null;
          } catch (e) {
            const msg = String(e?.message || e);
            if (msg.includes("AUTH_KEY_UNREGISTERED")) status = "revoked"; else status = "error";
          }
          items.push({ name: raw.name, created_at: raw.created_at, status, user_id, username });
        } catch {
          items.push({ name: f.replace(/\.json$/, ""), created_at: null, status: "error", user_id: null, username: null });
        }
      }
      ok(res, { items });
    } catch (e) { err(res, 500, "INTERNAL", String(e)); }
  });

  app.use("/v1/auth", router);
}
