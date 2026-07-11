'use strict';
/**
 * bot.js — Mineflayer MC relay core (multi-bot)
 *
 * Supports up to MAX_BOTS separate bot profiles (name + username) all
 * connecting to the same configured server. Two independent roles:
 *
 *   • Relay bot (relayId)     — forwards MC chat/join-leave/system messages
 *                               to the Discord webhook, runs automations.
 *   • Speaker bot (speakerId) — sends messages typed via the dashboard or
 *                               Discord (/say, t!) into MC chat.
 *
 * These can point at the same bot or two different bots. Any other bots are
 * just AFK — connected, but silent.
 *
 * State (server config, bot profiles, role assignments, relay settings, and
 * which bots should be connected) is persisted to disk so a process restart
 * (crash, redeploy, host restart) automatically restores everything and
 * reconnects the bots that were online — instead of coming back empty.
 */

const mineflayer = require('mineflayer');
const axios      = require('axios');
const net        = require('net');
const fs         = require('fs');
const path       = require('path');

const MAX_BOTS = 5;

// ── Persistence ────────────────────────────────────────────────────────────
// Note: on hosts with an ephemeral filesystem (no attached volume), this file
// survives in-process restarts/crashes but not a full container rebuild. If
// your host supports a persistent volume, mount it over the `data/` folder.

const DATA_DIR    = path.join(__dirname, 'data');
const STATE_FILE  = path.join(DATA_DIR, 'state.json');

function loadState() {
  try {
    if (!fs.existsSync(STATE_FILE)) return null;
    return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
  } catch (e) {
    console.error(`[STATE] Failed to load ${STATE_FILE}: ${e.message}`);
    return null;
  }
}

let saveScheduled = false;
function saveState() {
  // Debounce: multiple mutations in the same tick collapse into one write.
  if (saveScheduled) return;
  saveScheduled = true;
  setImmediate(() => {
    saveScheduled = false;
    try {
      if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
      const snapshot = {
        config, profiles, relayId, speakerId,
        autoJoin, blockedPhrases, nameAliases, useDisplayName, automations,
        // ids of bots that should auto-reconnect on boot / after a crash
        wantConnected: profiles.filter(p => !ensureState(p.id).manualStop).map(p => p.id)
      };
      fs.writeFileSync(STATE_FILE, JSON.stringify(snapshot, null, 2));
    } catch (e) {
      console.error(`[STATE] Failed to save: ${e.message}`);
    }
  });
}

const saved = loadState();

// ── Shared server config ──────────────────────────────────────────────────────
// Defaults fall back to env vars (MC_HOST, MC_PORT, MC_VERSION, WEBHOOK_URL) if
// nothing was ever saved to disk yet.

const config = Object.assign({
  host:       process.env.MC_HOST     || 'man.serveminecraft.net',
  port:       Number(process.env.MC_PORT) || 25565,
  webhookUrl: process.env.WEBHOOK_URL || '',
  version:    process.env.MC_VERSION  || 'auto'
}, saved?.config || {});

// ── Bot profiles (identities that can connect to the server) ─────────────────

let profiles = (Array.isArray(saved?.profiles) && saved.profiles.length)
  ? saved.profiles
  : [{ id: 'bot1', name: 'Bot 1', username: 'ShadowZ2' }];

let relayId   = saved?.relayId   ?? profiles[0]?.id ?? null;
let speakerId = saved?.speakerId ?? profiles[0]?.id ?? null;

// Runtime state per bot id (never persisted directly — rebuilt on boot)
const instances = new Map();
function ensureState(id) {
  if (!instances.has(id)) {
    instances.set(id, {
      bot: null, status: 'disconnected', manualStop: true,
      autoJoinTimer: null, packetCount: 0, failCount: 0
    });
  }
  return instances.get(id);
}
for (const p of profiles) ensureState(p.id);

function findProfile(id) { return profiles.find(p => p.id === id); }

// ── Global relay settings ─────────────────────────────────────────────────────

let autoJoin       = saved?.autoJoin ?? false;
let blockedPhrases = Array.isArray(saved?.blockedPhrases) ? saved.blockedPhrases : [];
let nameAliases    = (saved?.nameAliases && typeof saved.nameAliases === 'object') ? saved.nameAliases : {};
let useDisplayName = !!saved?.useDisplayName;
let automations    = Array.isArray(saved?.automations) ? saved.automations : [];
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
//   • Exactly ONE flush loop running at all times (flushRunning flag)
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

// ── Chat automations (run independently on every connected bot — e.g. so a ──
// private "/login <password>" prompt gets answered by whichever bot actually
// received it, not just the relay bot) ────────────────────────────────────────

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

// ── Webhook senders (only invoked for the relay bot's events) ────────────────

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
    socket.once('timeout', () => { finish({ online: false }); });
    socket.once('error',   () => { finish({ online: false }); });

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
      } catch (e) { finish({ online: false }); }
    });

    socket.on('data', chunk => {
      rxBuf = Buffer.concat([rxBuf, chunk]);
      try {
        let off = 0;
        if (rxBuf.length < 1) return;
        const [pktLen, after1] = readVarInt(rxBuf, off); off = after1;
        if (rxBuf.length < off + pktLen) return;
        const [pktId, after2]   = readVarInt(rxBuf, off);
        if (pktId !== 0x00) return finish({ online: false });
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
        if (e.message !== 'VarInt incomplete') finish({ online: false });
      }
    });

    socket.connect(port, host);
  });
}

// ── Protocol → mineflayer version string map ──────────────────────────────────

const PROTOCOL_TO_VERSION = {
  769: '1.21.4', 768: '1.21.2', 767: '1.21', 766: '1.20.6', 765: '1.20.4',
  764: '1.20.2', 763: '1.20.1', 762: '1.19.4', 761: '1.19.3', 760: '1.19.2',
  759: '1.19',   758: '1.18.2', 757: '1.18.1', 756: '1.17.1', 755: '1.17',
  754: '1.16.5', 753: '1.16.3', 751: '1.16.2', 736: '1.16.1', 578: '1.15.2',
  575: '1.15.1', 573: '1.15',   498: '1.14.4', 490: '1.14.3', 485: '1.14.2',
  480: '1.14.1', 477: '1.14',   404: '1.13.2', 401: '1.13.1', 393: '1.13',
  340: '1.12.2', 338: '1.12.1', 335: '1.12',   315: '1.11',   110: '1.9.4',
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
    return result.online;
  } catch (e) {
    return false;
  }
}

// ── Auto-join scheduler (per bot instance, with gradual backoff) ─────────────
//
// Retries forever while autoJoin is on and the bot hasn't been manually
// stopped — this is what makes a long server outage (full restart, crash,
// maintenance) resolve itself once the server comes back, with no manual
// intervention needed. Backoff grows 15s → ~22s → ~34s → ... capped at 60s the
// longer the outage lasts, then resets to 15s the moment the bot logs back in.

const AUTO_JOIN_MIN_DELAY = 15000;
const AUTO_JOIN_MAX_DELAY = 60000;

function clearAutoJoinTimer(id) {
  const st = ensureState(id);
  if (st.autoJoinTimer) { clearTimeout(st.autoJoinTimer); st.autoJoinTimer = null; }
}

function scheduleAutoJoin(id, delayMs = AUTO_JOIN_MIN_DELAY) {
  clearAutoJoinTimer(id);
  const st = ensureState(id);
  if (!autoJoin || st.manualStop) return;
  const label = findProfile(id)?.name || id;
  log(`Auto-join: next probe in ${Math.round(delayMs / 1000)}s…`, 'warn', label);
  st.autoJoinTimer = setTimeout(async () => {
    if (!autoJoin || st.manualStop) return;
    if (st.bot) return;
    const online = await probeServer();
    if (online) {
      log('Auto-join: server online — connecting…', 'success', label);
      createBotInstance(id);
    } else {
      st.failCount = (st.failCount || 0) + 1;
      const nextDelay = Math.min(AUTO_JOIN_MIN_DELAY * Math.pow(1.5, st.failCount), AUTO_JOIN_MAX_DELAY);
      log(`Auto-join: server offline — retrying in ${Math.round(nextDelay / 1000)}s… (attempt ${st.failCount})`, 'warn', label);
      scheduleAutoJoin(id, nextDelay);
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
    return null;
  }
}

// ── Resolve version to use ────────────────────────────────────────────────────

async function resolveVersion() {
  if (config.version && config.version !== 'auto') {
    return config.version;
  }

  const ping = await pingServer(config.host, config.port);

  if (!ping.online) {
    return '1.20.4';
  }

  const mapped = protocolToVersion(ping.protocol);
  if (mapped) return mapped;

  const match = ping.version?.match(/1\.\d+(?:\.\d+)?/);
  if (match) return match[0];

  return false;
}

// ── Bot lifecycle (per instance) ──────────────────────────────────────────────

function createBotInstance(id) {
  const profile = findProfile(id);
  if (!profile) { log(`createBot: unknown bot id ${id}`, 'error'); return; }
  if (!config.host) { log('Missing server host — set it in Server Config', 'error', profile.name); return; }

  const st = ensureState(id);
  st.manualStop = false;
  saveState();

  if (st.bot) { log('createBot called but bot instance already exists — skipping', 'warn', profile.name); return; }

  st.status = 'connecting';
  log(`Connecting to ${config.host}:${config.port} as ${profile.username}…`, 'info', profile.name);

  const preCheck   = new net.Socket();
  let preCheckDone = false;

  preCheck.setTimeout(8000);

  preCheck.once('connect', () => {
    preCheckDone = true;
    preCheck.destroy();
    if (st.manualStop) { st.status = 'disconnected'; return; } // Stop was pressed mid-connect
    resolveVersion().then(ver => {
      if (st.manualStop) { st.status = 'disconnected'; return; }
      _spawnBotInstance(id, ver);
    }).catch(e => {
      log(`resolveVersion error — ${e.message}`, 'error', profile.name);
      if (!st.manualStop) _spawnBotInstance(id, false);
    });
  });

  preCheck.once('timeout', () => {
    if (preCheckDone) return;
    preCheckDone = true;
    preCheck.destroy();
    log(`Pre-flight TCP TIMEOUT — ${config.host}:${config.port} unreachable`, 'error', profile.name);
    st.status = 'disconnected';
    if (autoJoin && !st.manualStop) scheduleAutoJoin(id);
  });

  preCheck.once('error', err => {
    if (preCheckDone) return;
    preCheckDone = true;
    preCheck.destroy();
    log(`Pre-flight TCP ERROR — ${err.code || err.message}`, 'error', profile.name);
    st.status = 'disconnected';
    if (autoJoin && !st.manualStop) scheduleAutoJoin(id);
  });

  preCheck.connect(Number(config.port), config.host);
}

function _spawnBotInstance(id, resolvedVersion) {
  const profile = findProfile(id);
  const st      = ensureState(id);
  if (!profile) return; // bot was removed while resolving version
  const label   = profile.name;

  log(`Spawning mineflayer bot — host=${config.host} port=${config.port} user=${profile.username} resolvedVersion=${resolvedVersion}…`, 'info', label);

  const botOptions = {
    host:           config.host,
    port:           Number(config.port),
    username:       profile.username,
    auth:           'offline',
    connectTimeout: 30000,
    keepAlive:      true,
    hideErrors:     true
  };

  if (resolvedVersion) {
    botOptions.version = resolvedVersion;
  }

  let bot;
  try {
    bot = mineflayer.createBot(botOptions);
  } catch (e) {
    log(`[SPAWN ERROR] ${e.message}`, 'error', label);
    st.status = 'disconnected';
    if (autoJoin && !st.manualStop) scheduleAutoJoin(id);
    return;
  }

  st.bot         = bot;
  st.packetCount = 0;

  // Defensive: guarantee an 'error' listener exists on the raw socket so a
  // stray socket error can never bubble up as an uncaught exception and
  // crash the whole process.
  setImmediate(() => {
    if (!st.bot || !st.bot._client) return;
    const client    = st.bot._client;
    const rawSocket = client.socket ?? client.stream ?? null;
    if (rawSocket && typeof rawSocket.on === 'function') {
      rawSocket.on('error', () => { /* handled via bot 'error'/'end' events below */ });
    }
  });

  bot.once('login', () => {
    st.status    = 'connected';
    st.failCount = 0;
    clearAutoJoinTimer(id);
    saveState();
    log(`✓ Logged in as ${bot.username} | server version: ${bot.version}`, 'success', label);
  });

  bot.once('spawn', () => {
    log(`✓ Bot spawned — gameMode=${bot.game?.gameMode} dimension=${bot.game?.dimension}`, 'success', label);
  });

  bot.on('resourcePack', (url, hash, forced) => {
    bot.acceptResourcePack();
  });

  bot.on('playerJoined', player => {
    if (player.username === bot.username || isFakePlayer(player.username)) return;
    log(`[JOIN] ${player.username}`, 'success', label);
    if (id === relayId) sendJoinLeave(player.username, true);
  });

  bot.on('playerLeft', player => {
    if (player.username === bot.username || isFakePlayer(player.username)) return;
    log(`[LEAVE] ${player.username}`, 'warn', label);
    if (id === relayId) sendJoinLeave(player.username, false);
  });

  bot.on('message', (message, position) => {
    if (!['system', 'chat'].includes(position)) return;

    const plain = message.toString().trim();
    if (!plain) return;

    const json   = message.json ?? message.unsigned?.json ?? null;
    const parsed = parseServerChat(json);

    if (parsed) {
      const { rank, username, chatText } = parsed;
      log(`[CHAT] ${rank ? `[${rank}] ` : ''}${username}: ${chatText}`, 'success', label);
      if (id === relayId) sendChatMessage(username, chatText, rank || null);
      runAutomations(chatText, id); // every connected bot checks its own automations
    } else {
      if (plain.endsWith('joined the game') || plain.endsWith('left the game')) return;
      log(`[SYS] ${plain}`, 'info', label);
      if (id === relayId) sendSystemMessage(plain);
      runAutomations(plain, id); // e.g. a private "/login" prompt only this bot received
    }
  });

  let fired = false;

  const onDisconnect = (evLabel, detail, type = 'warn') => {
    if (fired) return;
    fired = true;
    st.status = 'disconnected';
    log(`[DISCONNECT] ${evLabel}: ${detail}`, type, label);
    st.bot = null;
    if (autoJoin && !st.manualStop) {
      scheduleAutoJoin(id, AUTO_JOIN_MIN_DELAY);
    }
  };

  bot.once('kicked', r => {
    const reason = typeof r === 'string' ? r : JSON.stringify(r);
    onDisconnect('Kicked', reason, 'error');
  });

  bot.once('error', err => {
    onDisconnect('Error', err?.message || String(err), 'error');
  });

  bot.once('end', reason => {
    const detail = reason ? `reason=${reason}` : 'connection closed (no reason given)';
    onDisconnect('Disconnected', detail);
  });
}

function destroyBotInstance(id) {
  const st      = ensureState(id);
  const profile = findProfile(id);
  st.manualStop = true;
  st.failCount  = 0;
  clearAutoJoinTimer(id);
  if (st.bot) { try { st.bot.quit('Stopping relay bot'); } catch {} st.bot = null; }
  st.status = 'disconnected';
  saveState();
  log('Bot stopped', 'info', profile?.name);
}

// ── Bot profile management (create/remove/select roles) ──────────────────────

function listBots() {
  return profiles.map(p => {
    const st = ensureState(p.id);
    return {
      id: p.id, name: p.name, username: p.username, status: st.status,
      isRelay: p.id === relayId, isSpeaker: p.id === speakerId
    };
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
  if (!relayId)   relayId   = id;
  if (!speakerId) speakerId = id;
  saveState();
  log(`Bot profile created: ${name} (${username})`);
  return profile;
}

function removeBot(id) {
  const idx = profiles.findIndex(p => p.id === id);
  if (idx === -1) throw new Error('Bot not found');

  destroyBotInstance(id);
  instances.delete(id);
  const [removed] = profiles.splice(idx, 1);

  if (relayId === id)   relayId   = profiles[0]?.id ?? null;
  if (speakerId === id) speakerId = profiles[0]?.id ?? null;

  saveState();
  log(`Bot profile removed: ${removed.name}`);
}

function setRelay(id) {
  const profile = findProfile(id);
  if (!profile) throw new Error('Bot not found');
  relayId = id;
  saveState();
  log(`Relay bot (MC → Discord) set to: ${profile.name}`);
}

function setSpeaker(id) {
  const profile = findProfile(id);
  if (!profile) throw new Error('Bot not found');
  speakerId = id;
  saveState();
  log(`Speaker bot (Discord → MC) set to: ${profile.name}`);
}

function getRelayId()   { return relayId; }
function getSpeakerId() { return speakerId; }

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

function pickSpeakerBot() {
  if (speakerId) {
    const st = instances.get(speakerId);
    if (st && st.status === 'connected' && st.bot) return st.bot;
  }
  // Fallback: any connected bot, so dashboard chat still works if the
  // designated speaker happens to be offline.
  for (const st of instances.values()) {
    if (st.status === 'connected' && st.bot) return st.bot;
  }
  return null;
}

function sendChat(message) {
  const bot = pickSpeakerBot();
  if (!bot) { log('No connected bot available to send chat', 'error'); return false; }
  bot.chat(message);
  log(`[SENT] ${message}`, 'success');
  return true;
}

function sendDiscordChat(discordKey, text) {
  const bot = pickSpeakerBot();
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
  saveState();
  log(`Auto-join ${autoJoin ? 'enabled' : 'disabled'}`);
}

function setBlockedPhrases(phrases)  { blockedPhrases = Array.isArray(phrases) ? phrases : []; saveState(); }
function setNameAliases(aliases)     { nameAliases    = (aliases && typeof aliases === 'object') ? aliases : {}; saveState(); }
function setUseDisplayName(val)      { useDisplayName = !!val; saveState(); log(`Name key: ${useDisplayName ? 'display name' : 'username'}`); }
function setAutomations(rules) {
  automations = Array.isArray(rules) ? rules.filter(r => r.trigger && r.response) : [];
  saveState();
  log(`Automations updated: ${automations.length} rule(s)`);
}

function setServerConfig({ host, port, webhookUrl, version }) {
  if (host !== undefined)       config.host       = host.trim();
  if (port !== undefined)       config.port       = Number(port) || 25565;
  if (webhookUrl !== undefined) config.webhookUrl = webhookUrl || '';
  if (version !== undefined)    config.version    = version || 'auto';
  saveState();
}

// ── Boot: resume bots that were connected before the last restart ────────────

function resumeConnections() {
  const ids = Array.isArray(saved?.wantConnected) ? saved.wantConnected : [];
  if (!ids.length) return;
  log(`Resuming ${ids.length} bot(s) that were connected before restart…`);
  for (const id of ids) {
    if (findProfile(id)) createBotInstance(id);
  }
}
// Small delay to let the process fully settle (env vars, listeners, etc.)
// before hitting the network.
setTimeout(resumeConnections, 3000);

// ── Exports ───────────────────────────────────────────────────────────────────

module.exports = {
  config,
  MAX_BOTS,

  // profiles
  listBots, addBot, removeBot,
  setRelay, setSpeaker, getRelayId, getSpeakerId,

  // lifecycle
  connectBot: createBotInstance,
  disconnectBot: destroyBotInstance,

  // status
  getOverallStatus,
  pingServer,
  getLastPing,
  setServerConfig,

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
