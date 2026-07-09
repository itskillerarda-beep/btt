'use strict';
/**
 * bot.js — Mineflayer MC relay core (multi-bot)
 *
 * Supports up to MAX_BOTS separate bot profiles (name + username) all
 * connecting to the same configured server. Only the "primary" (relay) bot
 * forwards chat/join-leave/system messages to the Discord webhook, runs chat
 * automations, and sends messages typed via the dashboard or Discord — this
 * avoids duplicate webhook posts when several bots are online at once.
 * The other bots just stay connected (AFK).
 */

const mineflayer = require('mineflayer');
const axios      = require('axios');
const net        = require('net');

const MAX_BOTS = 5;

// ── Shared server config ──────────────────────────────────────────────────────
// NOTE: webhookUrl was previously hardcoded here and is a credential — it now
// defaults to empty. Re-enter it in the dashboard's Webhook panel and save.

const config = {
  host:       'man.serveminecraft.net',
  port:       25565,
  webhookUrl: '',
  version:    'auto'
};

// ── Bot profiles (identities that can connect to the server) ─────────────────

const profiles = [
  { id: 'bot1', name: 'Bot 1', username: 'ShadowZ2' }
];
let primaryId = 'bot1'; // the relay bot — forwards chat, runs automations, sends outgoing chat

// Runtime state per bot id
const instances = new Map();
function ensureState(id) {
  if (!instances.has(id)) {
    instances.set(id, {
      bot: null, status: 'disconnected', manualStop: true,
      autoJoinTimer: null, packetCount: 0
    });
  }
  return instances.get(id);
}
ensureState('bot1');

function findProfile(id) { return profiles.find(p => p.id === id); }

// ── Global relay settings ─────────────────────────────────────────────────────

let autoJoin       = false;
let blockedPhrases = [];
let nameAliases    = {};
let useDisplayName = false;
let automations    = [];
const logs         = [];

let lastPing = {
  online: false, motd: '', players: [],
  playerCount: 0, maxPlayers: 0, latency: null,
  version: '', checkedAt: null
};

// ── Logging ───────────────────────────────────────────────────────────────────

function log(msg, type = 'info', label = null) {
  const full = label ? `[${label}] ${msg}` : msg;
  logs.unshift({ msg: full, type, time: new Date().toISOString() });
  if (logs.length > 200) logs.pop();
  console.log(`[${type.toUpperCase()}] ${full}`);
}

function clearLogs() {
  logs.length = 0;
  logs.push({ msg: 'Logs cleared', type: 'info', time: new Date().toISOString() });
}

// ── Webhook queue (shared — single flush loop for all bots) ──────────────────
//
// Design goals:
//   • Exactly ONE flush loop running at all times (flushLoop flag)
//   • Messages sent one-by-one with MIN_GAP_MS spacing between them
//   • On 429: pause the whole loop for retry_after + small buffer
//   • On permanent 4xx: drop the message, move on
//   • On network/5xx: exponential backoff per-item, drop after MAX_RETRIES
//   • Queue capped at MAX_QUEUE to prevent unbounded memory growth

const MIN_GAP_MS   = 1500;   // minimum ms between successful sends
const MAX_RETRIES  = 5;
const BASE_BACKOFF = 2000;   // ms, doubles each retry
const MAX_QUEUE    = 100;

const webhookQueue = [];
let   flushRunning = false;   // single-loop guard
let   flushTimer   = null;    // handle for the scheduled next tick

function _cancelFlushTimer() {
  if (flushTimer !== null) {
    clearTimeout(flushTimer);
    flushTimer = null;
  }
}

function _scheduleFlush(delayMs = 0) {
  _cancelFlushTimer();
  flushTimer = setTimeout(_flushTick, delayMs);
}

async function _flushTick() {
  flushTimer    = null;
  flushRunning  = true;

  if (!config.webhookUrl || webhookQueue.length === 0) {
    flushRunning = false;
    return;
  }

  const item = webhookQueue[0];

  try {
    await axios.post(config.webhookUrl, item.payload, { timeout: 10000 });

    // ── Success ──────────────────────────────────────────────────
    webhookQueue.shift();
    flushRunning = false;

    if (webhookQueue.length > 0) {
      _scheduleFlush(MIN_GAP_MS);
    }

  } catch (err) {
    const status = err.response?.status;

    // ── 429 Rate limited ─────────────────────────────────────────
    if (status === 429) {
      const retryAfterSec = err.response?.data?.retry_after ?? 2;
      const waitMs        = Math.ceil(retryAfterSec * 1000) + 500; // +500ms buffer
      log(`Webhook rate-limited — pausing queue for ${(waitMs / 1000).toFixed(1)}s (${webhookQueue.length} item(s) pending)`, 'warn');
      flushRunning = false;
      _scheduleFlush(waitMs);
      return;
    }

    // ── Permanent 4xx (bad webhook, deleted, etc.) ────────────────
    if (status >= 400 && status < 500) {
      log(`Webhook permanent error ${status} — dropping message`, 'error');
      webhookQueue.shift();
      flushRunning = false;
      if (webhookQueue.length > 0) _scheduleFlush(MIN_GAP_MS);
      return;
    }

    // ── Network / 5xx — retry with backoff ───────────────────────
    item.attempts = (item.attempts || 0) + 1;

    if (item.attempts >= MAX_RETRIES) {
      log(`Webhook failed after ${MAX_RETRIES} attempts — dropping message`, 'error');
      webhookQueue.shift();
      flushRunning = false;
      if (webhookQueue.length > 0) _scheduleFlush(MIN_GAP_MS);
      return;
    }

    const backoffMs = Math.min(BASE_BACKOFF * 2 ** (item.attempts - 1), 30000);
    log(`Webhook error ${status || 'network'} (attempt ${item.attempts}/${MAX_RETRIES}) — retrying in ${(backoffMs / 1000).toFixed(1)}s`, 'warn');
    flushRunning = false;
    _scheduleFlush(backoffMs);
  }
}

/**
 * Add a payload to the queue. Starts the flush loop if it isn't running.
 */
function queueWebhook(payload) {
  if (!config.webhookUrl) return;

  // Drop oldest if queue is full
  if (webhookQueue.length >= MAX_QUEUE) {
    log(`Webhook queue full (${MAX_QUEUE}) — dropping oldest message`, 'warn');
    webhookQueue.shift();
  }

  webhookQueue.push({ payload, attempts: 0 });

  // Only kick off the loop if it isn't already scheduled or running
  if (!flushRunning && flushTimer === null) {
    _scheduleFlush(0);
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function isBlocked(text) {
  return text ? blockedPhrases.some(p => p.trim() && text.toLowerCase().includes(p.trim().toLowerCase())) : false;
}

function isFakePlayer(name) {
  return !name || name.startsWith('|') || /slot_\d+/i.test(name);
}

function isOwnBot(name) {
  return profiles.some(p => p.username.toLowerCase() === (name || '').toLowerCase());
}

function teamColor(team) {
  if (!team) return 0x95a5a6;
  const t = team.toLowerCase();
  if (t.includes('red'))                          return 0xe74c3c;
  if (t.includes('blue'))                         return 0x3498db;
  if (t.includes('green'))                        return 0x2ecc71;
  if (t.includes('yellow'))                       return 0xf1c40f;
  if (t.includes('gold'))                         return 0xf39c12;
  if (t.includes('purple'))                       return 0x9b59b6;
  if (t.includes('aqua'))                         return 0x1abc9c;
  if (t.includes('white'))                        return 0xecf0f1;
  if (t.includes('gray') || t.includes('grey'))   return 0x95a5a6;
  if (t.includes('black'))                        return 0x2c3e50;
  if (t.includes('admin') || t.includes('owner')) return 0xe74c3c;
  if (t.includes('mod'))                          return 0x3498db;
  if (t.includes('vip'))                          return 0xf1c40f;
  let hash = 0;
  for (const c of team) hash = c.charCodeAt(0) + ((hash << 5) - hash);
  return Math.abs(hash) % 0xFFFFFF;
}

// ── Chat automations (only ever run by the primary/relay bot) ────────────────

function runAutomations(text, id) {
  const st = instances.get(id);
  if (!st || !st.bot || st.status !== 'connected' || !automations.length || !text) return;
  const bot   = st.bot;
  const label = findProfile(id)?.name;

  for (const rule of automations) {
    if (!rule.trigger || !rule.response) continue;
    const h = text.toLowerCase(), n = rule.trigger.toLowerCase();
    const match = rule.matchType === 'exact'      ? h === n
                : rule.matchType === 'startswith' ? h.startsWith(n)
                :                                   h.includes(n);
    if (match) {
      log(`[AUTO] Trigger: "${rule.trigger}" → "${rule.response}"`, 'info', label);
      setTimeout(() => {
        const cur = instances.get(id);
        if (cur && cur.bot === bot && cur.status === 'connected') {
          bot.chat(rule.response);
          log(`[AUTO] Sent: ${rule.response}`, 'success', label);
        }
      }, rule.delay ?? 600);
    }
  }
}

// ── Webhook senders (only invoked for the primary/relay bot's events) ────────

function sendChatMessage(mcUsername, chatText, team) {
  if (isBlocked(chatText)) { log(`[BLOCKED] ${chatText}`, 'warn'); return; }
  const hasTeam = !!team?.trim();
  const label   = hasTeam ? `[${team}] ${mcUsername}` : mcUsername;
  queueWebhook({
    username:   label,
    avatar_url: `https://mc-heads.net/avatar/${encodeURIComponent(mcUsername)}/64`,
    embeds: [{
      description: `**${label}**: ${chatText}`,
      color:       teamColor(hasTeam ? team : null),
      footer:      { text: config.host },
      timestamp:   new Date().toISOString()
    }]
  });
}

function sendJoinLeave(mcUsername, joined) {
  queueWebhook({
    username:   mcUsername,
    avatar_url: `https://mc-heads.net/avatar/${encodeURIComponent(mcUsername)}/64`,
    embeds: [{
      description: `${joined ? '📥' : '📤'} **${mcUsername}** ${joined ? 'joined' : 'left'} the game`,
      color:       joined ? 0x2ecc71 : 0xe74c3c,
      footer:      { text: config.host },
      timestamp:   new Date().toISOString()
    }]
  });
}

function sendSystemMessage(text) {
  if (isBlocked(text)) { log(`[BLOCKED] ${text}`, 'warn'); return; }
  queueWebhook({
    username:   config.host || 'Server',
    avatar_url: 'https://mc-heads.net/avatar/MHF_Herobrine/64',
    embeds: [{
      description: `📢 ${text}`,
      color:       0x778ca3,
      footer:      { text: config.host },
      timestamp:   new Date().toISOString()
    }]
  });
}

// ── MC Status Ping ────────────────────────────────────────────────────────────

function writeVarInt(arr, val) {
  val = val >>> 0;
  do {
    let byte = val & 0x7F;
    val >>>= 7;
    if (val !== 0) byte |= 0x80;
    arr.push(byte);
  } while (val !== 0);
}

function readVarInt(buf, offset) {
  let result = 0, shift = 0;
  while (offset < buf.length) {
    const byte = buf[offset++];
    result |= (byte & 0x7F) << shift;
    shift += 7;
    if (!(byte & 0x80)) return [result, offset];
    if (shift >= 35) throw new Error('VarInt overflow');
  }
  throw new Error('VarInt incomplete');
}

function encodePacket(data) {
  const lenArr = [];
  writeVarInt(lenArr, data.length);
  return Buffer.concat([Buffer.from(lenArr), data]);
}

function stripColors(str) {
  if (!str) return '';
  return str.replace(/§[0-9a-fk-or]/gi, '').replace(/\u00A7[0-9a-fk-or]/gi, '').trim();
}

function extractMotd(desc) {
  if (!desc) return '';
  if (typeof desc === 'string') return stripColors(desc);
  const parts = [];
  if (desc.text)  parts.push(desc.text);
  if (desc.extra) for (const e of desc.extra) parts.push(typeof e === 'string' ? e : (e.text || ''));
  return stripColors(parts.join(''));
}

function pingServer(host, port) {
  host = host || config.host;
  port = Number(port || config.port) || 25565;

  return new Promise(resolve => {
    if (!host) return resolve({ online: false });

    const socket = new net.Socket();
    let done     = false;
    let rxBuf    = Buffer.alloc(0);
    const t0     = Date.now();

    const finish = res => {
      if (done) return;
      done = true;
      socket.destroy();
      resolve(res);
    };

    socket.setTimeout(10000);
    socket.once('timeout', () => { log(`Ping timeout for ${host}:${port}`, 'warn'); finish({ online: false }); });
    socket.once('error',   err => { log(`Ping error for ${host}:${port} — ${err.message}`, 'warn'); finish({ online: false }); });

    socket.once('connect', () => {
      try {
        const hostBuf = Buffer.from(host, 'utf8');
        const hsData  = [];
        hsData.push(0x00);
        writeVarInt(hsData, 0);
        writeVarInt(hsData, hostBuf.length);
        for (const b of hostBuf) hsData.push(b);
        hsData.push((port >> 8) & 0xFF);
        hsData.push(port & 0xFF);
        hsData.push(0x01);
        socket.write(Buffer.concat([
          encodePacket(Buffer.from(hsData)),
          encodePacket(Buffer.from([0x00]))
        ]));
      } catch (e) { log(`Ping build error — ${e.message}`, 'warn'); finish({ online: false }); }
    });

    socket.on('data', chunk => {
      rxBuf = Buffer.concat([rxBuf, chunk]);
      try {
        let off = 0;
        if (rxBuf.length < 1) return;
        const [pktLen, after1] = readVarInt(rxBuf, off); off = after1;
        if (rxBuf.length < off + pktLen) return;
        const [pktId, after2]   = readVarInt(rxBuf, off);
        if (pktId !== 0x00) { log(`Ping unexpected packet ID 0x${pktId.toString(16)}`, 'warn'); return finish({ online: false }); }
        const [jsonLen, after3] = readVarInt(rxBuf, after2);
        if (rxBuf.length < after3 + jsonLen) return;
        const json = JSON.parse(rxBuf.slice(after3, after3 + jsonLen).toString('utf8'));
        finish({
          online:      true,
          motd:        extractMotd(json.description),
          players:     (json.players?.sample || []).map(p => ({ name: p.name, id: p.id })),
          playerCount: json.players?.online  ?? 0,
          maxPlayers:  json.players?.max     ?? 0,
          latency:     Date.now() - t0,
          version:     json.version?.name   || '',
          protocol:    json.version?.protocol ?? null,
          checkedAt:   new Date().toISOString()
        });
      } catch (e) {
        if (e.message !== 'VarInt incomplete') { log(`Ping parse error — ${e.message}`, 'warn'); finish({ online: false }); }
      }
    });

    socket.connect(port, host);
  });
}

// ── Protocol → mineflayer version string map ──────────────────────────────────

const PROTOCOL_TO_VERSION = {
  769: '1.21.4',
  768: '1.21.2',
  767: '1.21',
  766: '1.20.6',
  765: '1.20.4',
  764: '1.20.2',
  763: '1.20.1',
  762: '1.19.4',
  761: '1.19.3',
  760: '1.19.2',
  759: '1.19',
  758: '1.18.2',
  757: '1.18.1',
  756: '1.17.1',
  755: '1.17',
  754: '1.16.5',
  753: '1.16.3',
  751: '1.16.2',
  736: '1.16.1',
  578: '1.15.2',
  575: '1.15.1',
  573: '1.15',
  498: '1.14.4',
  490: '1.14.3',
  485: '1.14.2',
  480: '1.14.1',
  477: '1.14',
  404: '1.13.2',
  401: '1.13.1',
  393: '1.13',
  340: '1.12.2',
  338: '1.12.1',
  335: '1.12',
  315: '1.11',
  110: '1.9.4',
  47:  '1.8.9'
};

function protocolToVersion(protocol) {
  if (!protocol) return null;
  if (PROTOCOL_TO_VERSION[protocol]) return PROTOCOL_TO_VERSION[protocol];
  const keys = Object.keys(PROTOCOL_TO_VERSION).map(Number).sort((a, b) => b - a);
  for (const k of keys) {
    if (protocol >= k) return PROTOCOL_TO_VERSION[k];
  }
  return null;
}

async function probeServer() {
  if (!config.host) return false;
  try {
    const result = await pingServer(config.host, config.port);
    lastPing = result.online
      ? { ...result, checkedAt: new Date().toISOString() }
      : { ...lastPing, online: false, checkedAt: new Date().toISOString() };
    log(`Probe result: ${result.online ? 'ONLINE' : 'OFFLINE'} (${config.host}:${config.port})${result.online ? ` ver=${result.version} protocol=${result.protocol}` : ''}`, result.online ? 'info' : 'warn');
    return result.online;
  } catch (e) {
    log(`probeServer error — ${e.message}`, 'error');
    return false;
  }
}

// ── Auto-join scheduler (per bot instance) ───────────────────────────────────

function clearAutoJoinTimer(id) {
  const st = ensureState(id);
  if (st.autoJoinTimer) { clearTimeout(st.autoJoinTimer); st.autoJoinTimer = null; }
}

function scheduleAutoJoin(id, delayMs = 15000) {
  clearAutoJoinTimer(id);
  const st = ensureState(id);
  if (!autoJoin || st.manualStop) return;
  const label = findProfile(id)?.name || id;
  log(`Auto-join: next probe in ${delayMs / 1000}s…`, 'warn', label);
  st.autoJoinTimer = setTimeout(async () => {
    if (!autoJoin || st.manualStop) return;
    if (st.bot) { log('Auto-join: bot already exists — skipping', 'info', label); return; }
    const online = await probeServer();
    if (online) {
      log('Auto-join: server online — connecting…', 'success', label);
      createBotInstance(id);
    } else {
      log('Auto-join: server offline — retrying in 15s…', 'warn', label);
      scheduleAutoJoin(id, 15000);
    }
  }, delayMs);
}

// ── Text collector ────────────────────────────────────────────────────────────

function collectText(node) {
  if (!node) return '';
  if (typeof node === 'string') return node;
  let out = '';
  if (node.text) out += node.text;
  if (Array.isArray(node.extra)) for (const child of node.extra) out += collectText(child);
  return out;
}

// ── Chat parser ───────────────────────────────────────────────────────────────

function parseServerChat(json) {
  try {
    const raw = typeof json === 'string' ? JSON.parse(json) : json;
    if (!raw) return null;
    const outerExtra = raw.extra?.[0]?.extra;
    if (!Array.isArray(outerExtra) || outerExtra.length < 3) return null;
    const usernameNode = outerExtra[0]?.extra?.[0]?.extra?.[0]?.hoverEvent?.contents?.extra?.[0]?.extra?.[0]?.extra?.[0];
    const username     = usernameNode?.text?.trim();
    if (!username) return null;
    const nameBlock = outerExtra[1]?.extra?.[0]?.extra?.[0]?.extra?.[0]?.extra?.[0];
    const rank      = nameBlock?.text?.trim() || null;
    const chatRaw   = collectText(outerExtra[2]).trim();
    const chatText  = chatRaw.replace(/^:\s*/, '');
    if (!chatText) return null;
    return { rank, username, chatText };
  } catch (e) {
    log(`[PARSE ERROR] ${e.message}`, 'warn');
    return null;
  }
}

// ── Resolve version to use ────────────────────────────────────────────────────

async function resolveVersion() {
  if (config.version && config.version !== 'auto') {
    log(`Using pinned version: ${config.version}`, 'info');
    return config.version;
  }

  log('Version set to auto — pinging server to detect…', 'info');
  const ping = await pingServer(config.host, config.port);

  if (!ping.online) {
    log('Auto-detect ping failed — falling back to 1.20.4', 'warn');
    return '1.20.4';
  }

  log(`Server reports: version="${ping.version}" protocol=${ping.protocol}`, 'info');

  const mapped = protocolToVersion(ping.protocol);
  if (mapped) {
    log(`Auto-detected version: ${mapped} (protocol ${ping.protocol})`, 'success');
    return mapped;
  }

  const match = ping.version.match(/1\.\d+(?:\.\d+)?/);
  if (match) {
    log(`Auto-detected version from string: ${match[0]}`, 'success');
    return match[0];
  }

  log(`Could not map protocol ${ping.protocol} — falling back to false (mineflayer auto)`, 'warn');
  return false;
}

// ── Bot lifecycle (per instance) ──────────────────────────────────────────────

function createBotInstance(id) {
  const profile = findProfile(id);
  if (!profile) { log(`createBot: unknown bot id ${id}`, 'error'); return; }
  if (!config.host) { log('Missing server host — set it in Server Config', 'error', profile.name); return; }

  const st = ensureState(id);
  st.manualStop = false;

  if (st.bot) { log('createBot called but bot instance already exists — skipping', 'warn', profile.name); return; }

  st.status = 'connecting';
  log(`Connecting to ${config.host}:${config.port} as ${profile.username}…`, 'info', profile.name);

  const preCheck   = new net.Socket();
  let preCheckDone = false;

  preCheck.setTimeout(8000);

  preCheck.once('connect', () => {
    preCheckDone = true;
    preCheck.destroy();
    log(`Pre-flight TCP OK — port ${config.port} reachable`, 'info', profile.name);
    resolveVersion().then(ver => _spawnBotInstance(id, ver)).catch(e => {
      log(`resolveVersion error — ${e.message}`, 'error', profile.name);
      _spawnBotInstance(id, false);
    });
  });

  preCheck.once('timeout', () => {
    if (preCheckDone) return;
    preCheckDone = true;
    preCheck.destroy();
    log(`Pre-flight TCP TIMEOUT — ${config.host}:${config.port} unreachable`, 'error', profile.name);
    st.status = 'disconnected';
    if (autoJoin && !st.manualStop) scheduleAutoJoin(id, 15000);
  });

  preCheck.once('error', err => {
    if (preCheckDone) return;
    preCheckDone = true;
    preCheck.destroy();
    log(`Pre-flight TCP ERROR — ${err.code || err.message}`, 'error', profile.name);
    st.status = 'disconnected';
    if (autoJoin && !st.manualStop) scheduleAutoJoin(id, 15000);
  });

  preCheck.connect(Number(config.port), config.host);
}

function _spawnBotInstance(id, resolvedVersion) {
  const profile = findProfile(id);
  const st      = ensureState(id);
  const label   = profile.name;

  log(`Spawning mineflayer bot — host=${config.host} port=${config.port} user=${profile.username} resolvedVersion=${resolvedVersion}…`, 'info', label);

  const botOptions = {
    host:           config.host,
    port:           Number(config.port),
    username:       profile.username,
    auth:           'offline',
    connectTimeout: 30000,
    keepAlive:      true,
    hideErrors:     false
  };

  if (resolvedVersion) {
    botOptions.version = resolvedVersion;
  }

  const bot = mineflayer.createBot(botOptions);
  st.bot         = bot;
  st.packetCount = 0;

  setImmediate(() => {
    if (!st.bot || !st.bot._client) {
      log('[DEBUG] bot._client not available after setImmediate', 'error', label);
      return;
    }

    const client    = st.bot._client;
    const rawSocket = client.socket ?? client.stream ?? null;

    if (rawSocket) {
      rawSocket.on('close', hadError => {
        log(`[SOCKET] TCP closed — hadError=${hadError}`, hadError ? 'error' : 'warn', label);
      });
      rawSocket.on('error', err => {
        log(`[SOCKET] TCP error — ${err.code || err.message}`, 'error', label);
      });
    } else {
      log('[DEBUG] No raw socket found on bot._client', 'warn', label);
    }

    client.on('packet', (data, meta) => {
      st.packetCount++;
      if (st.packetCount <= 30) {
        const dataStr = JSON.stringify(data);
        const preview = dataStr.length > 300 ? dataStr.slice(0, 300) + '…' : dataStr;
        log(`[PKT #${st.packetCount}] state=${client.state} name=${meta.name} data=${preview}`, 'info', label);
      }
    });

    client.on('state', (newState, oldState) => {
      log(`[STATE] ${oldState} → ${newState}`, 'info', label);
    });

    client.on('disconnect', packet => {
      let reason = '';
      try {
        const r = packet?.reason;
        reason = typeof r === 'string' ? collectText(JSON.parse(r)) : collectText(r);
      } catch { reason = String(packet?.reason ?? '(no reason)'); }
      log(`[LOGIN DISCONNECT] "${reason}"`, 'error', label);
    });

    client.on('kick_disconnect', packet => {
      let reason = '';
      try {
        const r = packet?.reason;
        reason = typeof r === 'string' ? collectText(JSON.parse(r)) : collectText(r);
      } catch { reason = String(packet?.reason ?? '(no reason)'); }
      log(`[PLAY KICK] "${reason}"`, 'error', label);
    });
  });

  bot.once('login', () => {
    st.status = 'connected';
    clearAutoJoinTimer(id);
    log(`✓ Logged in as ${bot.username} | server version: ${bot.version}`, 'success', label);
  });

  bot.once('spawn', () => {
    log(`✓ Bot spawned — gameMode=${bot.game?.gameMode} dimension=${bot.game?.dimension}`, 'success', label);
  });

  bot.on('resourcePack', (url, hash, forced) => {
    log(`[RESOURCE PACK] forced=${forced} — accepting`, 'info', label);
    bot.acceptResourcePack();
  });

  bot.on('playerJoined', player => {
    if (player.username === bot.username || isFakePlayer(player.username)) return;
    log(`[JOIN] ${player.username}`, 'success', label);
    if (id === primaryId) sendJoinLeave(player.username, true);
  });

  bot.on('playerLeft', player => {
    if (player.username === bot.username || isFakePlayer(player.username)) return;
    log(`[LEAVE] ${player.username}`, 'warn', label);
    if (id === primaryId) sendJoinLeave(player.username, false);
  });

  bot.on('message', (message, position) => {
    if (!['system', 'chat'].includes(position)) return;
    const plain = message.toString().trim();
    if (!plain) return;
    if (id !== primaryId) return; // only the relay bot forwards chat/webhook events

    const json   = message.json ?? message.unsigned?.json ?? null;
    const parsed = parseServerChat(json);
    if (parsed) {
      const { rank, username, chatText } = parsed;
      log(`[CHAT] ${rank ? `[${rank}] ` : ''}${username}: ${chatText}`, 'success', label);
      sendChatMessage(username, chatText, rank || null);
      runAutomations(chatText, id);
    } else {
      if (plain.endsWith('joined the game') || plain.endsWith('left the game')) return;
      log(`[SYS] ${plain}`, 'info', label);
      sendSystemMessage(plain);
      runAutomations(plain, id);
    }
  });

  let fired = false;

  const onDisconnect = (evLabel, detail, type = 'warn') => {
    if (fired) return;
    fired = true;
    st.status = 'disconnected';
    log(`[DISCONNECT] ${evLabel}: ${detail}`, type, label);
    log(`[DISCONNECT] packets received: ${st.packetCount} | state: ${st.bot?._client?.state ?? 'unknown'}`, 'info', label);
    st.bot = null;
    if (autoJoin && !st.manualStop) {
      log('Auto-join: retrying in 10s…', 'warn', label);
      scheduleAutoJoin(id, 10000);
    }
  };

  bot.once('kicked', r => {
    const reason = typeof r === 'string' ? r : JSON.stringify(r);
    log(`[KICKED RAW] ${reason}`, 'error', label);
    onDisconnect('Kicked', reason, 'error');
  });

  bot.once('error', err => {
    log(`[ERROR RAW] code=${err.code} msg=${err.message}`, 'error', label);
    onDisconnect('Error', err.message, 'error');
  });

  bot.once('end', reason => {
    const detail = reason ? `reason=${reason}` : 'connection closed (no reason given)';
    log(`[END RAW] ${detail}`, 'warn', label);
    onDisconnect('Disconnected', detail);
  });
}

function destroyBotInstance(id) {
  const st      = ensureState(id);
  const profile = findProfile(id);
  st.manualStop = true;
  clearAutoJoinTimer(id);
  if (st.bot) { try { st.bot.quit('Stopping relay bot'); } catch {} st.bot = null; }
  st.status = 'disconnected';
  log('Bot stopped', 'info', profile?.name);
}

// ── Bot profile management (create/remove/select relay bot) ──────────────────

function listBots() {
  return profiles.map(p => {
    const st = ensureState(p.id);
    return { id: p.id, name: p.name, username: p.username, status: st.status, isPrimary: p.id === primaryId };
  });
}

function addBot(name, username) {
  username = (username || '').trim();
  name     = (name || '').trim() || username;
  if (!username) throw new Error('Username is required');
  if (profiles.length >= MAX_BOTS) throw new Error(`Maximum of ${MAX_BOTS} bots reached`);
  if (profiles.some(p => p.username.toLowerCase() === username.toLowerCase())) {
    throw new Error('A bot with that username already exists');
  }

  const id = 'bot_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  const profile = { id, name, username };
  profiles.push(profile);
  ensureState(id);
  if (!primaryId) primaryId = id;
  log(`Bot profile created: ${name} (${username})`);
  return profile;
}

function removeBot(id) {
  const idx = profiles.findIndex(p => p.id === id);
  if (idx === -1) throw new Error('Bot not found');

  destroyBotInstance(id);
  instances.delete(id);
  const [removed] = profiles.splice(idx, 1);

  if (primaryId === id) {
    primaryId = profiles.length ? profiles[0].id : null;
    if (primaryId) log(`Relay bot reassigned to: ${findProfile(primaryId).name}`);
  }
  log(`Bot profile removed: ${removed.name}`);
}

function setPrimary(id) {
  const profile = findProfile(id);
  if (!profile) throw new Error('Bot not found');
  primaryId = id;
  log(`Relay bot set to: ${profile.name}`);
}

function getPrimaryId() { return primaryId; }

function getOverallStatus() {
  let anyConnected = false, anyConnecting = false;
  for (const st of instances.values()) {
    if (st.status === 'connected')  anyConnected  = true;
    if (st.status === 'connecting') anyConnecting = true;
  }
  if (anyConnected)  return 'connected';
  if (anyConnecting) return 'connecting';
  return 'disconnected';
}

// ── Chat API ──────────────────────────────────────────────────────────────────

function pickSenderBot() {
  if (primaryId) {
    const st = instances.get(primaryId);
    if (st && st.status === 'connected' && st.bot) return st.bot;
  }
  for (const st of instances.values()) {
    if (st.status === 'connected' && st.bot) return st.bot;
  }
  return null;
}

function sendChat(message) {
  const bot = pickSenderBot();
  if (!bot) { log('No connected bot available to send chat', 'error'); return false; }
  bot.chat(message);
  log(`[SENT] ${message}`, 'success');
  return true;
}

function sendDiscordChat(discordKey, text) {
  const bot = pickSenderBot();
  if (!bot) return false;
  const safe = text.replace(/^[\\/\\.]+/, '').trim();
  if (!safe) return false;
  const displayName = nameAliases[discordKey] || discordKey;
  const prefix      = `(${displayName}): `;
  const body        = safe.slice(0, 256 - prefix.length);
  bot.chat(`${prefix}${body}`);
  log(`[DC→MC] ${prefix}${body}`, 'success');
  return true;
}

function getOnlinePlayers() {
  for (const st of instances.values()) {
    if (st.status === 'connected' && st.bot) {
      return Object.values(st.bot.players)
        .map(p => p.username)
        .filter(n => n && !isFakePlayer(n) && !isOwnBot(n));
    }
  }
  return null;
}

function getLastPing() { return lastPing; }

// ── Settings setters ──────────────────────────────────────────────────────────

function setAutoJoin(enabled) {
  autoJoin = !!enabled;
  if (!autoJoin) {
    for (const id of instances.keys()) clearAutoJoinTimer(id);
  }
  log(`Auto-join ${autoJoin ? 'enabled' : 'disabled'}`);
}

function setBlockedPhrases(phrases)  { blockedPhrases = Array.isArray(phrases) ? phrases : []; }
function setNameAliases(aliases)     { nameAliases    = (aliases && typeof aliases === 'object') ? aliases : {}; }
function setUseDisplayName(val)      { useDisplayName = !!val; log(`Name key: ${useDisplayName ? 'display name' : 'username'}`); }
function setAutomations(rules) {
  automations = Array.isArray(rules) ? rules.filter(r => r.trigger && r.response) : [];
  log(`Automations updated: ${automations.length} rule(s)`);
}

// ── Exports ───────────────────────────────────────────────────────────────────

module.exports = {
  config,
  MAX_BOTS,

  // profiles
  listBots, addBot, removeBot, setPrimary, getPrimaryId,

  // lifecycle
  connectBot: createBotInstance,
  disconnectBot: destroyBotInstance,

  // status
  getOverallStatus,
  pingServer,
  getLastPing,

  // chat
  sendChat, sendDiscordChat, getOnlinePlayers,

  // settings
  setAutoJoin, setBlockedPhrases, setNameAliases, setUseDisplayName, setAutomations,
  clearLogs,

  getLogs:           () => logs,
  getAutoJoin:       () => autoJoin,
  getUseDisplayName: () => useDisplayName,
  getNameAliases:    () => nameAliases,
  getAutomations:    () => automations,
  getBlockedPhrases: () => blockedPhrases
};
