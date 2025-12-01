import express from 'express';
import { TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions/index.js';
import { Api } from 'telegram';

// ===== helpers =====
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
    deviceModel: 'tg-master-converter',
    appVersion: '0.9',
    systemVersion: 'Linux'
  });
  if (!client.connected) await client.connect();
  return client;
}

function toType(chat) {
  if (!chat) return null;
  if (chat.className === 'Channel' && chat.megagroup) return 'supergroup';
  if (chat.className === 'Channel' && !chat.megagroup) return 'channel';
  if (chat.className === 'Chat' && chat.megagroup) return 'supergroup';
  if (chat.className === 'Chat') return 'group';
  return 'unknown';
}

async function toFull(client, chat) {
  try {
    if (chat.className === 'Channel') {
      const input = new Api.InputChannel({ channelId: chat.id, accessHash: chat.accessHash });
      return await client.invoke(new Api.channels.GetFullChannel({ channel: input }));
    }
    if (chat.className === 'Chat') {
      return await client.invoke(new Api.messages.GetFullChat({ chatId: chat.id }));
    }
  } catch (_e) {}
  return null;
}

function normalize(chat, full) {
  const base = {
    id: chat?.id ?? null,
    access_hash: chat?.accessHash ?? null,
    type: toType(chat),
    title: chat?.title ?? null,
    username: chat?.username ? `@${chat.username}` : null,
    is_verified: chat?.verified ?? null,
    is_scam: chat?.scam ?? null,
    is_fake: chat?.fake ?? null,
  };
  if (full) {
    const about = full.fullChat?.about ?? null;
    const participantsCount = full.fullChat?.participantsCount ?? null;
    const onlineCount = full.fullChat?.onlineCount ?? null;
    const linked = full.chats?.find(c => c.id === full.fullChat?.linkedChatId);
    base.about = about;
    base.members_count = participantsCount;
    base.online_count = onlineCount ?? null;
    base.linked_chat_username = linked?.username ? `@${linked.username}` : null;
  }
  return base;
}

async function resolveEntity(client, { username, title, invite }) {
  if (username) {
    const handle = username.replace(/^@/, '');
    return client.getEntity(handle);
  }
  if (invite) {
    const hash = (invite.match(/(?:joinchat\/|\+)([A-Za-z0-9_-]+)/) || [])[1];
    if (!hash) throw new Error('Invalid invite link');
    const info = await client.invoke(new Api.messages.CheckChatInvite({ hash }));
    if (info.className === 'ChatInviteAlready') return info.chat;
    throw new Error('Join via invite required');
  }
  if (title) {
    const res = await client.invoke(new Api.contacts.Search({ q: title, limit: 50 }));
    const chats = [...(res.chats ?? [])];
    let match = chats.find(c => (c.title || '').toLowerCase() === title.toLowerCase());
    if (!match) match = chats.find(c => (c.title || '').toLowerCase().includes(title.toLowerCase()));
    if (!match) return null;
    return match;
  }
  throw new Error('Provide one of: username | title | invite');
}

// ===== router =====
export default function registerConverter(app) {
  const router = express.Router();

  // POST /v1/converter/resolve
  router.post('/resolve', async (req, res) => {
    const t0 = Date.now();
    try {
      const { session_name, api_id, api_hash, username, title, invite } = req.body || {};
      if (!session_name) return res.status(400).json({ success: false, data: null, meta: null, error: { code: 'BAD_REQUEST', message: 'session_name required', details: null } });
      const id = api_id || process.env.TG_API_ID || process.env.API_ID || process.env.TELEGRAM_API_ID;
      const hash = api_hash || process.env.TG_API_HASH || process.env.API_HASH || process.env.TELEGRAM_API_HASH;
      if (!id || !hash) return res.status(400).json({ success: false, data: null, meta: null, error: { code: 'BAD_REQUEST', message: 'api_id and api_hash are required', details: null } });
      const client = await makeClient({ sessionName: session_name, apiId: id, apiHash: hash });

      const entity = await resolveEntity(client, { username, title, invite });
      if (!entity) return res.status(404).json({ success: false, data: null, meta: null, error: { code: 'NOT_FOUND', message: 'chat not found', details: null } });
      const full = await toFull(client, entity);
      const item = normalize(entity, full);

      return res.json({ success: true, data: { item }, meta: { took_ms: Date.now() - t0 }, error: null });
    } catch (e) {
      const message = String(e?.message || e);
      const code = /FLOOD_WAIT/.test(message) ? 'FLOOD_WAIT' : 'INTERNAL';
      const status = code === 'FLOOD_WAIT' ? 429 : 500;
      return res.status(status).json({ success: false, data: null, meta: null, error: { code, message, details: null } });
    }
  });

  // POST /v1/converter/members
  router.post('/members', async (req, res) => {
    const t0 = Date.now();
    try {
      const { session_name, api_id, api_hash, username, title, invite, limit = 100, page_size = 200, offset = 0, pause_ms = 500 } = req.body || {};
      if (!session_name) return res.status(400).json({ success: false, data: null, meta: null, error: { code: 'BAD_REQUEST', message: 'session_name required', details: null } });
      const id = api_id || process.env.TG_API_ID || process.env.API_ID || process.env.TELEGRAM_API_ID;
      const hash = api_hash || process.env.TG_API_HASH || process.env.API_HASH || process.env.TELEGRAM_API_HASH;
      if (!id || !hash) return res.status(400).json({ success: false, data: null, meta: null, error: { code: 'BAD_REQUEST', message: 'api_id and api_hash are required', details: null } });
      const client = await makeClient({ sessionName: session_name, apiId: id, apiHash: hash });

      const entity = await resolveEntity(client, { username, title, invite });
      if (!entity) return res.status(404).json({ success: false, data: null, meta: null, error: { code: 'NOT_FOUND', message: 'chat not found', details: null } });
      if (!(entity.className === 'Channel' && entity.megagroup)) {
        return res.json({ success: true, data: { items: [] }, meta: { returned: 0, took_ms: Date.now() - t0 }, error: null });
      }

      const step = Math.min(Number(page_size) || 200, 200);
      const wantAll = String(limit).toUpperCase() === 'ALL';
      const maxTotal = wantAll ? Number.MAX_SAFE_INTEGER : Math.max(Number(limit) || 0, 0);

      let items = [];
      let off = Number(offset) || 0;

      while (items.length < maxTotal) {
        const q = new Api.channels.GetParticipants({
          channel: new Api.InputChannel({ channelId: entity.id, accessHash: entity.accessHash }),
          filter: new Api.ChannelParticipantsRecent(),
          offset: off,
          limit: step,
          hash: 0
        });
        const resp = await client.invoke(q);

        const usersMap = new Map();
        for (const u of resp.users || []) usersMap.set(String(u.id), u);

        const batch = (resp.participants || []).map(p => {
          const u = usersMap.get(String(p.userId));
          return {
            user_id: String(p.userId),
            username: u?.username ? `@${u.username}` : null,
            first_name: u?.firstName ?? null,
            last_name: u?.lastName ?? null,
            bot: u?.bot ?? false
          };
        });

        if (batch.length === 0) break;

        if (wantAll) {
          items.push(...batch);
        } else {
          const room = maxTotal - items.length;
          items.push(...batch.slice(0, room));
        }

        off += batch.length;
        if (batch.length < step) break; // достигли конца
        // анти-флуд пауза
        await new Promise(r => setTimeout(r, Math.max(Number(pause_ms) || 0, 0)));
      }

      return res.json({
        success: true,
        data: { items },
        meta: { returned: items.length, next_offset: off, took_ms: Date.now() - t0 },
        error: null
      });
    } catch (e) {
      const message = String(e?.message || e);
      const code = /FLOOD_WAIT/.test(message) ? 'FLOOD_WAIT' : 'INTERNAL';
      const status = code === 'FLOOD_WAIT' ? 429 : 500;
      return res.status(status).json({ success: false, data: null, meta: null, error: { code, message, details: null } });
    }
  });

  app.use('/v1/converter', router);
}
