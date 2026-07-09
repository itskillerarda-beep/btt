'use strict';
/**
 * discord.js — Discord bot for MC relay
 *
 * Commands (in configured channel, by users with the allowed role):
 *   /say <text>   — sends "(name): text" to Minecraft via the relay bot
 *   t!<text>      — shorthand for the same
 *   !status       — replies with MC server status + relay bot info + player count
 *   !players      — replies with the full online player list
 *
 * Security:
 *   • Only users with the configured role ID can use any command
 *   • Leading / \ . stripped from message text before sending to MC
 *   • Bot messages are ignored
 *   • Only the configured channel is watched
 *
 * NOTE: the bot token/channel/role used to be hardcoded here — that's a leaked
 * credential. They now default from environment variables and can otherwise be
 * set from the dashboard (which is how they get into dcState at runtime).
 */

const { Client, GatewayIntentBits, EmbedBuilder } = require('discord.js');
const botManager = require('./bot');

// ── State ─────────────────────────────────────────────────────────────────────

let client       = null;
let loginTimeout = null;
let retryCount   = 0;
const MAX_RETRIES = 5;

const dcState = {
  token:     process.env.DISCORD_BOT_TOKEN  || '',
  channelId: process.env.DISCORD_CHANNEL_ID || '',
  roleId:    process.env.DISCORD_ROLE_ID    || ''
};

const UNSAFE_START = /^[\\/\\.]+/;

// ── Helpers ───────────────────────────────────────────────────────────────────

function hasRole(member) {
  if (!dcState.roleId) return true;
  return member.roles.cache.has(dcState.roleId);
}

function sanitise(text) {
  return text.replace(UNSAFE_START, '').trim();
}

function cleanupClient() {
  if (loginTimeout) {
    clearTimeout(loginTimeout);
    loginTimeout = null;
  }
  if (client) {
    try { client.destroy(); } catch (e) {
      console.error(`[DISCORD] Error destroying client: ${e.message}`);
    }
    client = null;
  }
}

// ── Command handlers ──────────────────────────────────────────────────────────

function handleStatus(msg) {
  const overall   = botManager.getOverallStatus();
  const bots      = botManager.listBots();
  const connected = bots.filter(b => b.status === 'connected').length;
  const players   = botManager.getOnlinePlayers();
  const online    = overall === 'connected';
  const playerCnt = players ? players.length : 0;
  const primary   = bots.find(b => b.isPrimary);

  const embed = new EmbedBuilder()
    .setTitle('🎮 MC Server Status')
    .setColor(online ? 0x2ecc71 : 0xe74c3c)
    .addFields(
      { name: 'Server',    value: botManager.config.host || 'Not set', inline: true },
      { name: 'Status',    value: online ? '🟢 Online' : '🔴 Offline', inline: true },
      { name: 'Players',   value: online ? String(playerCnt) : '—', inline: true },
      { name: 'Bots',      value: `${connected}/${bots.length} connected`, inline: true },
      { name: 'Relay bot', value: primary ? `${primary.name} (${primary.status})` : 'None set', inline: true }
    )
    .setTimestamp();

  msg.reply({ embeds: [embed] }).catch(e => {
    console.error(`[DISCORD] Failed to send status embed: ${e.message}`);
  });
}

function handlePlayers(msg) {
  const players = botManager.getOnlinePlayers();
  const overall  = botManager.getOverallStatus();

  if (overall !== 'connected') {
    return msg.reply('❌ No relay bot is connected.').catch(e => {
      console.error(`[DISCORD] Failed to send reply: ${e.message}`);
    });
  }

  const embed = new EmbedBuilder()
    .setTitle('👥 Online Players')
    .setColor(0x3498db)
    .setFooter({ text: botManager.config.host })
    .setTimestamp();

  if (!players || players.length === 0) {
    embed.setDescription('No players online right now.');
  } else {
    embed.setDescription(players.map(p => `• \`${p}\``).join('\n'));
    embed.addFields({ name: 'Total', value: String(players.length), inline: true });
  }

  msg.reply({ embeds: [embed] }).catch(e => {
    console.error(`[DISCORD] Failed to send players embed: ${e.message}`);
  });
}

function handleChat(msg, text) {
  const key  = botManager.getUseDisplayName()
    ? (msg.member?.displayName || msg.author.username)
    : msg.author.username;

  const safe = sanitise(text);
  if (!safe) return;

  const sent = botManager.sendDiscordChat(key, safe);
  if (sent) {
    msg.react('✅').catch(() => {});
  } else {
    msg.react('❌').catch(() => {});
  }
}

// ── Bot lifecycle ─────────────────────────────────────────────────────────────

function startDiscordBot(token, channelId, roleId) {
  if (!token || !channelId) {
    console.error('[DISCORD] Cannot start — token or channelId missing');
    return;
  }

  cleanupClient();
  retryCount = 0;

  dcState.token     = token;
  dcState.channelId = channelId;
  dcState.roleId    = roleId || '';

  attemptLogin();
}

function attemptLogin() {
  if (!dcState.token || !dcState.channelId) return;

  console.log(`[DISCORD] Attempting login... (attempt ${retryCount + 1}/${MAX_RETRIES})`);

  client = new Client({
    intents: [
      GatewayIntentBits.Guilds,
      GatewayIntentBits.GuildMessages,
      GatewayIntentBits.MessageContent
    ],
    // Force REST over HTTPS only — helps on restrictive hosts
    rest: {
      timeout: 15000
    }
  });

  // ── Debug events ────────────────────────────────────────────────────────────

  client.on('debug', info => {
    // Only log meaningful debug lines, not heartbeat spam
    if (
      info.includes('Identified') ||
      info.includes('Ready') ||
      info.includes('gateway') ||
      info.includes('connect') ||
      info.includes('session') ||
      info.includes('heartbeat') && info.includes('lost') ||
      info.includes('reconnect') ||
      info.includes('resume') ||
      info.includes('error') ||
      info.includes('fail')
    ) {
      console.log(`[DISCORD DEBUG] ${info}`);
    }
  });

  client.on('warn', info => {
    console.warn(`[DISCORD WARN] ${info}`);
  });

  // ── Shard/WebSocket error ────────────────────────────────────────────────────

  client.on('shardError', err => {
    console.error(`[DISCORD] WebSocket/Shard error: ${err.message}`);
    console.error(`[DISCORD] Stack: ${err.stack}`);
  });

  // ── Disconnect / reconnect events ────────────────────────────────────────────

  client.on('shardDisconnect', (event, shardId) => {
    console.warn(`[DISCORD] Shard ${shardId} disconnected — code: ${event.code}, reason: ${event.reason || 'none'}`);
  });

  client.on('shardReconnecting', shardId => {
    console.log(`[DISCORD] Shard ${shardId} reconnecting...`);
  });

  client.on('shardResume', (shardId, replayedEvents) => {
    console.log(`[DISCORD] Shard ${shardId} resumed — replayed ${replayedEvents} events`);
  });

  // ── Ready ────────────────────────────────────────────────────────────────────

  client.once('ready', () => {
    retryCount = 0;
    if (loginTimeout) { clearTimeout(loginTimeout); loginTimeout = null; }
    console.log(`[DISCORD] ✅ Logged in as ${client.user.tag}`);
    console.log(`[DISCORD] Watching channel: ${dcState.channelId}`);
    console.log(`[DISCORD] Role restriction: ${dcState.roleId || 'none'}`);
  });

  // ── Messages ─────────────────────────────────────────────────────────────────

  client.on('messageCreate', msg => {
    if (msg.author.bot) return;

    console.log(`[DISCORD] msg in ${msg.channelId} (watch: ${dcState.channelId}) from ${msg.author.username}: "${msg.content}"`);

    if (msg.channelId !== dcState.channelId) return;

    if (dcState.roleId && !hasRole(msg.member)) {
      console.log(`[DISCORD] Blocked ${msg.author.username} — missing role ${dcState.roleId}`);
      return;
    }

    const content = msg.content.trim();

    if (!content) {
      console.warn('[DISCORD] WARNING: msg.content is empty — check Message Content Intent is enabled at discord.com/developers');
      return;
    }

    if (content === '!status')  return handleStatus(msg);
    if (content === '!players') return handlePlayers(msg);

    if (content.toLowerCase().startsWith('/say ')) {
      const text = content.slice(5).trim();
      if (text) return handleChat(msg, text);
      return;
    }

    if (content.toLowerCase().startsWith('t!')) {
      const text = content.slice(2).trim();
      if (text) return handleChat(msg, text);
      return;
    }
  });

  // ── Error ─────────────────────────────────────────────────────────────────────

  client.on('error', err => {
    console.error(`[DISCORD] Client error: ${err.message}`);
    console.error(`[DISCORD] Stack: ${err.stack}`);
  });

  // ── Login timeout — if Discord doesn't respond in 30s, retry ─────────────────

  loginTimeout = setTimeout(() => {
    console.error('[DISCORD] Login timed out after 30s — no response from Discord gateway');
    handleRetry();
  }, 30000);

  // ── Attempt login ─────────────────────────────────────────────────────────────

  client.login(dcState.token)
    .then(() => {
      console.log('[DISCORD] Login promise resolved — waiting for ready event...');
    })
    .catch(err => {
      console.error(`[DISCORD] Login failed: ${err.message}`);
      console.error(`[DISCORD] Error code: ${err.code || 'none'}`);
      console.error(`[DISCORD] Stack: ${err.stack}`);

      if (loginTimeout) { clearTimeout(loginTimeout); loginTimeout = null; }

      // Token invalid — no point retrying
      if (
        err.message?.includes('TOKEN_INVALID') ||
        err.message?.includes('Improper token') ||
        err.code === 'TOKEN_INVALID'
      ) {
        console.error('[DISCORD] ❌ Token is invalid — please regenerate your bot token at discord.com/developers');
        return;
      }

      handleRetry();
    });
}

function handleRetry() {
  try { client.destroy(); } catch {}
  client = null;

  retryCount++;
  if (retryCount >= MAX_RETRIES) {
    console.error(`[DISCORD] ❌ Failed to connect after ${MAX_RETRIES} attempts — giving up`);
    console.error('[DISCORD] Possible causes:');
    console.error('[DISCORD]   1. Wispbyte is blocking Discord WebSocket (gateway.discord.gg)');
    console.error('[DISCORD]   2. Token is invalid — regenerate at discord.com/developers');
    console.error('[DISCORD]   3. Bot is disabled or deleted in Discord Developer Portal');
    console.error('[DISCORD]   4. Network/DNS issues on the host');
    return;
  }

  const delay = Math.min(5000 * retryCount, 30000);
  console.log(`[DISCORD] Retrying in ${delay / 1000}s... (${retryCount}/${MAX_RETRIES})`);
  loginTimeout = setTimeout(attemptLogin, delay);
}

function stopDiscordBot() {
  cleanupClient();
  retryCount = 0;
  console.log('[DISCORD] Bot stopped');
}

function getDiscordStatus() {
  if (!client) return 'offline';
  return client.isReady() ? 'online' : 'connecting';
}

// ── Exports ───────────────────────────────────────────────────────────────────

module.exports = { startDiscordBot, stopDiscordBot, getDiscordStatus, dcState };
