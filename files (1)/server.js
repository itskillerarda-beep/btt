'use strict';

const express    = require('express');
const path       = require('path');
const botManager = require('./bot');
const discord    = require('./discord');

const app  = express();
const PORT = process.env.PORT || 9956;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ── Status / Logs ─────────────────────────────────────────────────────────────

app.get('/api/status', (req, res) => {
  res.json({
    status:  botManager.getOverallStatus(),
    bots:    botManager.listBots(),
    maxBots: botManager.MAX_BOTS,
    config: {
      host:       botManager.config.host,
      port:       botManager.config.port,
      version:    botManager.config.version,
      webhookUrl: botManager.config.webhookUrl
    },
    discord: {
      status:    discord.getDiscordStatus(),
      channelId: discord.dcState.channelId || '',
      roleId:    discord.dcState.roleId    || '',
      hasToken:  !!discord.dcState.token
    },
    nameSettings: {
      useDisplayName: botManager.getUseDisplayName(),
      aliases:        botManager.getNameAliases()
    },
    automations: botManager.getAutomations(),
    autoJoin:    botManager.getAutoJoin(),
    ping:        botManager.getLastPing()
  });
});

app.get('/api/logs', (req, res) => {
  res.json({ logs: botManager.getLogs() });
});

app.post('/api/logs/clear', (req, res) => {
  botManager.clearLogs();
  res.json({ ok: true });
});

app.get('/api/ping', async (req, res) => {
  const host = req.query.host || botManager.config.host;
  const port = req.query.port || botManager.config.port;
  try {
    const result = await botManager.pingServer(host, port);
    res.json(result);
  } catch (e) {
    res.json({ online: false, error: e.message });
  }
});

// ── Server config (shared host/port/version/webhook — applies to all bots) ──

app.post('/api/server-config', (req, res) => {
  const { host, port, webhookUrl, version } = req.body;
  if (!host) return res.status(400).json({ error: 'host required' });

  botManager.config.host       = host.trim();
  botManager.config.port       = Number(port) || 25565;
  botManager.config.webhookUrl = webhookUrl || '';
  botManager.config.version    = version || 'auto';

  res.json({ ok: true });
});

// ── Bot profiles (create/remove/connect/disconnect/select relay bot) ────────

app.post('/api/bots', (req, res) => {
  const { name, username } = req.body;
  try {
    const bot = botManager.addBot(name, username);
    res.json({ ok: true, bot });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.delete('/api/bots/:id', (req, res) => {
  try {
    botManager.removeBot(req.params.id);
    res.json({ ok: true });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.post('/api/bots/:id/connect', (req, res) => {
  if (!botManager.config.host) return res.status(400).json({ error: 'Set the server host first' });
  botManager.connectBot(req.params.id);
  res.json({ ok: true });
});

app.post('/api/bots/:id/disconnect', (req, res) => {
  botManager.disconnectBot(req.params.id);
  res.json({ ok: true });
});

app.post('/api/bots/:id/primary', (req, res) => {
  try {
    botManager.setPrimary(req.params.id);
    res.json({ ok: true });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// ── Chat / settings ───────────────────────────────────────────────────────────

app.post('/api/chat', (req, res) => {
  const { message } = req.body;
  if (!message) return res.status(400).json({ error: 'message required' });
  res.json({ ok: botManager.sendChat(message) });
});

app.post('/api/autojoin', (req, res) => {
  const { enabled } = req.body;
  botManager.setAutoJoin(!!enabled);
  res.json({ ok: true, autoJoin: botManager.getAutoJoin() });
});

app.post('/api/blocked', (req, res) => {
  const { phrases } = req.body;
  if (!Array.isArray(phrases)) return res.status(400).json({ error: 'phrases must be an array' });
  botManager.setBlockedPhrases(phrases);
  res.json({ ok: true });
});

app.post('/api/names', (req, res) => {
  const { aliases, useDisplayName } = req.body;
  if (aliases !== undefined)        botManager.setNameAliases(aliases);
  if (useDisplayName !== undefined) botManager.setUseDisplayName(useDisplayName);
  res.json({ ok: true });
});

// ── Chat Automations ──────────────────────────────────────────────────────────

app.get('/api/automations', (req, res) => {
  res.json({ automations: botManager.getAutomations() });
});

app.post('/api/automations', (req, res) => {
  const { automations } = req.body;
  if (!Array.isArray(automations)) return res.status(400).json({ error: 'automations must be an array' });
  botManager.setAutomations(automations);
  res.json({ ok: true, count: botManager.getAutomations().length });
});

// ── Discord Bot ───────────────────────────────────────────────────────────────

app.post('/api/discord/start', (req, res) => {
  const { token, channelId, roleId } = req.body;
  if (!token || !channelId) return res.status(400).json({ error: 'token and channelId required' });
  discord.startDiscordBot(token, channelId, roleId);
  res.json({ ok: true });
});

app.post('/api/discord/stop', (req, res) => {
  discord.stopDiscordBot();
  res.json({ ok: true });
});

// ── Start ─────────────────────────────────────────────────────────────────────

app.listen(PORT, () => console.log(`Dashboard running on port ${PORT}`));
