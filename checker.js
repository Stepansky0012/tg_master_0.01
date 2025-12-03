import express from 'express';
import { TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions/index.js';
import { Api } from 'telegram';

/** --- simple auth --- */
function isAuth(req) {
  const h = req.headers['authorization'] || '';
  const token = h.startsWith('Bearer ') ? h.slice(7) : h;
  return process.env.ADMIN_TOKEN ? token === process.env.ADMIN_TOKEN : true;
}
function guard(req, res, next) {
  if (!isAuth(req)) return res.status(401).json({ success:false, data:null, meta:null, error:{ code:'UNAUTHORIZED', message:'Missing or invalid ADMIN_TOKEN', details:null }});
  next();
}

/** --- client factory --- */
async function makeClient({ sessionName, apiId, apiHash }) {
  if (!sessionName) throw new Error('session_name required');
  const fs = await import('node:fs/promises');
  const p1 = `/app/data/sessions/${sessionName}.json`;
  const p2 = `/srv/tg-master-data/sessions/${sessionName}.json`;
  let raw;
  try { raw = await fs.readFile(p1, 'utf8'); } catch { raw = await fs.readFile(p2, 'utf8'); }
  const { session_string } = JSON.parse(raw);
  if (!session_string) throw new Error('session_string missing');
  const client = new TelegramClient(new StringSession(session_string), Number(apiId), String(apiHash), {
    connectionRetries: 3,
    deviceModel: 'tg-master-checker',
    appVersion: '0.9',
    systemVersion: 'Linux'
  });
  if (!client.connected) await client.connect();
  return client;
}

/** --- anti-flood limiter (per session) --- */
const RL = new Map();
// env tuning
const BASE_MIN_INTERVAL = Math.max(200, Number(process.env.CHECKER_MIN_INTERVAL_MS ?? 1100)); // 1.1s по умолчанию
const MAX_MIN_INTERVAL  = Math.max(BASE_MIN_INTERVAL, Number(process.env.CHECKER_MAX_INTERVAL_MS ?? 30000)); // до 30с
const MAX_PER_MIN       = Math.max(1, Number(process.env.CHECKER_MAX_PER_MIN ?? 45)); // «мягкий» кап за минуту

function getBucket(session) {
  const b = RL.get(session) ?? { nextAt: 0, minInterval: BASE_MIN_INTERVAL, wins: 0, winStarted: 0 };
  const now = Date.now();
  // окно 60s
  if (!b.winStarted || now - b.winStarted >= 60000) { b.winStarted = now; b.wins = 0; }
  RL.set(session, b);
  return b;
}
async function acquirePermit(session) {
  const b = getBucket(session);
  const now = Date.now();
  // если перебор за минуту — ждём до конца окна
  if (b.wins >= MAX_PER_MIN) {
    const sleep = b.winStarted + 60000 - now;
    if (sleep > 0) await new Promise(r => setTimeout(r, sleep));
    // reset окна
    b.winStarted = Date.now();
    b.wins = 0;
  }
  const wait = Math.max(0, b.nextAt - Date.now());
  if (wait > 0) await new Promise(r => setTimeout(r, wait));
  // бронируем следующее окно
  const at = Date.now();
  b.nextAt = at + b.minInterval;
  b.wins += 1;
}
function backoffOnFlood(session, waitSeconds) {
  const b = getBucket(session);
  // увеличиваем минимальный интервал: в 2 раза, но не выше MAX_MIN_INTERVAL
  const proposed = Math.min(MAX_MIN_INTERVAL, Math.max(b.minInterval * 2, BASE_MIN_INTERVAL));
  b.minInterval = proposed;
  // дополнительно ставим «жёсткую» паузу до now + waitSeconds*1000
  const penalty = Date.now() + (Number(waitSeconds || 0) * 1000);
  b.nextAt = Math.max(b.nextAt, penalty);
  RL.set(session, b);
}
function softenAfterSuccess(session) {
  const b = getBucket(session);
  // плавно снижаем интервал (на 10%), но не ниже базового
  b.minInterval = Math.max(BASE_MIN_INTERVAL, Math.floor(b.minInterval * 0.9));
  RL.set(session, b);
}

/** --- normalization --- */
function normStatus(s) {
  if (!s) return null;
  const c = s.className;
  if (c === 'UserStatusOnline') return { state: 'online', expires: s.expires };
  if (c === 'UserStatusOffline') return { state: 'offline', was_online: s.wasOnline };
  if (c === 'UserStatusRecently') return { state: 'recently' };
  if (c === 'UserStatusLastWeek') return { state: 'last_week' };
  if (c === 'UserStatusLastMonth') return { state: 'last_month' };
  if (c === 'UserStatusEmpty') return { state: 'unknown' };
  return { state: 'unknown' };
}

function normalizeUser(u, full) {
  return {
    id: u?.id ? String(u.id) : null,
    access_hash: u?.accessHash ? String(u.accessHash) : null,
    username: u?.username ? `@${u.username}` : null,
    first_name: u?.firstName ?? null,
    last_name: u?.lastName ?? null,
    phone: u?.phone ?? null,
    is_premium: u?.premium ?? false,
    bot: u?.bot ?? false,
    scam: u?.scam ?? false,
    fake: u?.fake ?? false,
    verified: u?.verified ?? false,
    restricted: u?.restricted ?? false,
    status: normStatus(u?.status),
    language: u?.langCode ?? null,
    bio: full?.fullUser?.about ?? null,
    common_chats_count: full?.fullUser?.commonChatsCount ?? null,
  };
}

/** --- resolvers --- */
async function resolveByUsername(client, username) {
  const handle = String(username).replace(/^@/, '');
  const entity = await client.getEntity(handle);
  const full = await client.invoke(new Api.users.GetFullUser({ id: entity }));
  return normalizeUser(entity, full);
}
async function resolveByUserId(client, user_id, access_hash) {
  let entity = null;
  if (access_hash) {
    entity = new Api.InputUser({ userId: BigInt(user_id), accessHash: BigInt(access_hash) });
  } else {
    entity = await client.getEntity(BigInt(user_id)).catch(() => null);
    if (!entity) throw new Error('USER_NOT_FOUND_OR_HASH_REQUIRED');
  }
  const full = await client.invoke(new Api.users.GetFullUser({ id: entity }));
  let userObj = null;
  if (entity.className === 'User') userObj = entity;
  else {
    const arr = await client.invoke(new Api.users.GetUsers({ id: [entity] }));
    userObj = arr && arr[0] ? arr[0] : null;
  }
  return normalizeUser(userObj, full);
}
async function resolveByPhone(client, phone) {
  const contacts = [ new Api.InputPhoneContact({ clientId: BigInt(Date.now()), phone: String(phone), firstName: 'x', lastName: 'x' }) ];
  const imp = await client.invoke(new Api.contacts.ImportContacts({ contacts }));
  const u = (imp.users && imp.users[0]) ? imp.users[0] : null;
  if (!u) return null;
  const full = await client.invoke(new Api.users.GetFullUser({ id: u }));
  try { await client.invoke(new Api.contacts.DeleteByPhones({ phones: [String(phone)] })); } catch {}
  return normalizeUser(u, full);
}
async function resolveOne(client, payload) {
  const { username, user_id, access_hash, phone } = payload || {};
  if (username) return resolveByUsername(client, username);
  if (user_id) return resolveByUserId(client, user_id, access_hash);
  if (phone) {
    const res = await resolveByPhone(client, phone);
    if (!res) throw new Error('NOT_FOUND_BY_PHONE');
    return res;
  }
  throw new Error('BAD_REQUEST');
}

/** --- helpers --- */
function parseFlood(msg) {
  const m = String(msg || '').match(/wait of\s+(\d+)\s+seconds/i);
  return m ? parseInt(m[1], 10) : null;
}

/** --- router --- */
export default function registerChecker(app) {
  const router = express.Router();
  router.use(express.json({ limit: '512kb' }));
  router.use(guard);

  // POST /v1/checker/resolve
  router.post('/resolve', async (req, res) => {
    const t0 = Date.now();
    const body = req.body || {};
    const session = body.session_name;
    try {
      const { session_name, api_id, api_hash, username, user_id, access_hash, phone } = body;
      if (!session_name) return res.status(400).json({ success:false, data:null, meta:null, error:{ code:'BAD_REQUEST', message:'session_name is required', details:null }});
      const id = api_id || process.env.TG_API_ID || process.env.API_ID || process.env.TELEGRAM_API_ID;
      const hash = api_hash || process.env.TG_API_HASH || process.env.API_HASH || process.env.TELEGRAM_API_HASH;
      if (!id || !hash) return res.status(400).json({ success:false, data:null, meta:null, error:{ code:'BAD_REQUEST', message:'api_id and api_hash are required', details:null }});

      // анти-флуд перед вызовом
      await acquirePermit(session_name);

      const client = await makeClient({ sessionName: session_name, apiId: id, apiHash: hash });
      const item = await resolveOne(client, { username, user_id, access_hash, phone });
      softenAfterSuccess(session_name);
      return res.json({ success:true, data:{ item }, meta:{ took_ms: Date.now()-t0, rate_limit:{ min_interval_ms: getBucket(session_name).minInterval } }, error:null });
    } catch (e) {
      const msg = String(e?.message || e);
      const waitSec = parseFlood(msg);
      const isFlood = (/FLOOD_WAIT/i.test(msg) || waitSec !== null);
      if (isFlood) backoffOnFlood(session, waitSec);
      const code = isFlood ? 'FLOOD_WAIT'
                 : /NOT_FOUND_BY_PHONE/.test(msg) ? 'NOT_FOUND'
                 : /USER_NOT_FOUND_OR_HASH_REQUIRED/.test(msg) ? 'BAD_REQUEST'
                 : /BAD_REQUEST/.test(msg) ? 'BAD_REQUEST'
                 : 'INTERNAL';
      const http = code === 'FLOOD_WAIT' ? 429 : code === 'BAD_REQUEST' ? 400 : code === 'NOT_FOUND' ? 404 : 500;
      return res.status(http).json({ success:false, data:null, meta:{ rate_limit:{ min_interval_ms: getBucket(session||body.session_name||'').minInterval } }, error:{ code, message: msg, details: (waitSec!==null ? { wait_seconds: waitSec } : null) }});
    }
  });

  // POST /v1/checker/batch — троттлинг + ранний выход при FLOOD_WAIT
  router.post('/batch', async (req, res) => {
    const t0 = Date.now();
    const body = req.body || {};
    const session = body.session_name;
    try {
      const { session_name, api_id, api_hash, items, batch_size = 100, pause_ms = 300 } = body;
      if (!session_name) return res.status(400).json({ success:false, data:null, meta:null, error:{ code:'BAD_REQUEST', message:'session_name is required', details:null }});
      if (!Array.isArray(items) || items.length === 0) return res.status(400).json({ success:false, data:null, meta:null, error:{ code:'BAD_REQUEST', message:'items[] required', details:null }});
      const id = api_id || process.env.TG_API_ID || process.env.API_ID || process.env.TELEGRAM_API_ID;
      const hash = api_hash || process.env.TG_API_HASH || process.env.API_HASH || process.env.TELEGRAM_API_HASH;
      const client = await makeClient({ sessionName: session_name, apiId: id, apiHash: hash });

      const out = [];
      let ok = 0, failed = 0;
      let floodWait = null;
      const bs = Math.max(1, Math.min(1000, Number(batch_size)));
      const pause = Math.max(0, Number(pause_ms));

      outer: for (let i = 0; i < items.length; i += bs) {
        const chunk = items.slice(i, i + bs);
        for (const it of chunk) {
          try {
            // анти-флуд
            await acquirePermit(session_name);
            const r = await resolveOne(client, it);
            softenAfterSuccess(session_name);
            out.push({ success:true, item: r, error:null });
            ok++;
          } catch (e) {
            const msg = String(e?.message || e);
            const waitSec = parseFlood(msg);
            const isFlood = (/FLOOD_WAIT/i.test(msg) || waitSec !== null);
            const code = isFlood ? 'FLOOD_WAIT'
                       : /NOT_FOUND_BY_PHONE/.test(msg) ? 'NOT_FOUND'
                       : /USER_NOT_FOUND_OR_HASH_REQUIRED/.test(msg) ? 'BAD_REQUEST'
                       : 'INTERNAL';
            out.push({ success:false, item:null, error:{ code, message: msg, details: (waitSec!==null ? { wait_seconds: waitSec } : null) }});
            failed++;
            if (isFlood) { backoffOnFlood(session_name, waitSec); floodWait = waitSec ?? 60; break outer; }
          }
        }
        if (i + bs < items.length && pause > 0) await new Promise(r => setTimeout(r, pause));
      }

      return res.json({
        success: true,
        data: { items: out },
        meta: {
          total: items.length,
          ok, failed,
          took_ms: Date.now()-t0,
          flood_wait: floodWait ? { wait_seconds: floodWait } : null,
          rate_limit: { min_interval_ms: getBucket(session_name).minInterval, max_per_min: MAX_PER_MIN }
        },
        error: null
      });
    } catch (e) {
      const msg = String(e?.message || e);
      const waitSec = parseFlood(msg);
      const isFlood = (/FLOOD_WAIT/i.test(msg) || waitSec !== null);
      if (isFlood) backoffOnFlood(session, waitSec);
      const code = isFlood ? 'FLOOD_WAIT' : 'INTERNAL';
      const http = code === 'FLOOD_WAIT' ? 429 : 500;
      return res.status(http).json({ success:false, data:null, meta:{ rate_limit:{ min_interval_ms: getBucket(session||body.session_name||'').minInterval, max_per_min: MAX_PER_MIN } }, error:{ code, message: msg, details: (waitSec!==null ? { wait_seconds: waitSec } : null) }});
    }
  });

  app.use('/v1/checker', router);
}
