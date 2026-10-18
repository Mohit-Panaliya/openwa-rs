'use strict';

/**
 * openwa-rs Node.js sidecar.
 *
 * Implements docs/SIDECAR_PROTOCOL.md (v1): newline-delimited JSON-RPC 2.0 over
 * stdin/stdout, backed by @whiskeysockets/baileys.
 *
 * stdout carries ONLY protocol lines. Every diagnostic goes to stderr.
 */

const readline = require('readline');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const pino = require('pino');

const VERSION = '0.1.0';
const DATA_DIR = process.env.DATA_DIR
  ? path.resolve(process.env.DATA_DIR)
  : path.join(__dirname, 'data');
const LOG_LEVEL = process.env.LOG_LEVEL || 'silent';

const MEDIA_TYPES = ['image', 'video', 'audio', 'document'];
const MEDIA_KINDS = new Set(['image', 'video', 'audio', 'document', 'sticker']);

const RECONNECT_BASE_MS = 3000;
const RECONNECT_MAX_MS = 30000;

/** @type {Map<string, Session>} */
const sessions = new Map();

let shuttingDown = false;
let baileysPromise = null;

/* ------------------------------------------------------------------------- */
/* stdout / stderr discipline                                                */
/* ------------------------------------------------------------------------- */

function stderr(...args) {  process.stderr.write(
    args
      .map((a) => (typeof a === 'string' ? a : a && a.stack ? a.stack : safeStringify(a)))
      .join(' ')
      + '\n',
  );
}

function safeStringify(value) {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

// Defensive: no dependency (or accidental console call) may pollute stdout,
// which is reserved for protocol lines.
for (const method of ['log', 'info', 'debug', 'warn', 'trace']) {
  console[method] = (...args) => stderr('[console.' + method + ']', ...args);
}

function writeLine(obj) {
  process.stdout.write(JSON.stringify(obj) + '\n');
}

function respond(id, result) {
  writeLine({ jsonrpc: '2.0', id, result });
}

function respondError(id, message) {
  writeLine({ jsonrpc: '2.0', id, error: { message: String(message) } });
}

// While a request is being handled, events are queued so that the request's
// response is always flushed before the events it triggered (lets the gateway
// correlate a fresh sessionId before seeing its status event).
let eventQueue = null;

function emit(type, payload) {
  const line = { jsonrpc: '2.0', method: 'event', params: { type, payload } };
  if (eventQueue) {
    eventQueue.push(line);
    return;
  }
  writeLine(line);
}

function flushEvents() {
  const queue = eventQueue;
  eventQueue = null;
  if (queue) for (const line of queue) writeLine(line);
}

function emitStatus(session, status, reason) {
  const payload = { sessionId: session.sessionId, status };
  if (reason !== undefined && reason !== null) payload.reason = String(reason);
  emit('session.status', payload);
}

/** Updates the session status and emits `session.status`, skipping no-op repeats. */
function setStatus(session, status, reason) {
  const changed = session.status !== status;
  session.status = status;
  if (!changed && reason === undefined) return;
  emitStatus(session, status, reason);
}

/* ------------------------------------------------------------------------- */
/* Baileys loading (lazy; the package is ESM-only)                            */
/* ------------------------------------------------------------------------- */

async function loadBaileys() {
  if (!baileysPromise) {
    baileysPromise = (async () => {
      const mod = await import('@whiskeysockets/baileys');
      const makeWASocket = mod.makeWASocket || mod.default;
      const { useMultiFileAuthState, DisconnectReason } = mod;
      if (typeof makeWASocket !== 'function' || typeof useMultiFileAuthState !== 'function') {
        throw new Error('unexpected @whiskeysockets/baileys API surface');
      }
      let version = 'unknown';
      try {
        version = require('@whiskeysockets/baileys/package.json').version;
      } catch {
        /* ignore */
      }
      stderr('[sidecar] baileys', version, 'loaded');
      return { makeWASocket, useMultiFileAuthState, DisconnectReason, getContentType: mod.getContentType };
    })().catch((err) => {
      baileysPromise = null;
      throw err;
    });
  }
  return baileysPromise;
}

/* ------------------------------------------------------------------------- */
/* Session helpers                                                           */
/* ------------------------------------------------------------------------- */

function createSession(name) {
  const sessionId = crypto.randomUUID();
  const session = {
    sessionId,
    name: name || sessionId,
    status: 'created',
    qr: null,
    sock: null,
    saveCreds: null,
    dataDir: path.join(DATA_DIR, sessionId),
    autoReconnect: false,
    stopped: false,
    pendingStart: null,
    reconnectTimer: null,
    reconnectAttempts: 0,
  };
  sessions.set(sessionId, session);
  return session;
}

function getSession(params) {
  const sessionId = params && params.sessionId;
  const session = typeof sessionId === 'string' ? sessions.get(sessionId) : undefined;
  if (!session) throw new Error('session not found');
  return session;
}

function requireConnected(session) {
  if (session.status !== 'connected' || !session.sock) {
    throw new Error('session is not connected');
  }
  return session.sock;
}

function sessionInfo(session) {
  return { sessionId: session.sessionId, name: session.name, status: session.status };
}

function clearReconnect(session) {
  if (session.reconnectTimer) {
    clearTimeout(session.reconnectTimer);
    session.reconnectTimer = null;
  }
}

function unwrapContent(message) {
  let content = message;
  for (let i = 0; i < 5 && content; i++) {
    const inner =
      (content.ephemeralMessage && content.ephemeralMessage.message) ||
      (content.viewOnceMessage && content.viewOnceMessage.message) ||
      (content.viewOnceMessageV2 && content.viewOnceMessageV2.message) ||
      (content.viewOnceMessageV2Extension && content.viewOnceMessageV2Extension.message) ||
      (content.documentWithCaptionMessage && content.documentWithCaptionMessage.message);
    if (!inner) break;
    content = inner;
  }
  return content;
}

function toTimestamp(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  if (typeof value === 'string') {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }
  if (typeof value.toNumber === 'function') {
    try {
      const n = value.toNumber();
      return Number.isFinite(n) ? n : null;
    } catch {
      /* fall through */
    }
  }
  if (typeof value.low === 'number') return value.low;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

/* ------------------------------------------------------------------------- */
/* Socket lifecycle                                                          */
/* ------------------------------------------------------------------------- */

async function startSocket(session) {
  const baileys = await loadBaileys();
  const { makeWASocket, useMultiFileAuthState, DisconnectReason, getContentType } = baileys;

  fs.mkdirSync(session.dataDir, { recursive: true });

  const { state, saveCreds } = await useMultiFileAuthState(session.dataDir);
  session.saveCreds = saveCreds;

  const sock = makeWASocket({
    auth: state,
    // diagnostics only: pino writes to stderr, stdout is protocol-only
    logger: pino({ level: LOG_LEVEL }, process.stderr),
  });
  session.sock = sock;

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', (update) => {
    if (!update) return;
    if (session.sock !== sock) return; // stale socket (stopped/replaced)

    if (update.qr) {
      session.qr = update.qr;
      session.status = 'qr_ready';
      emitStatus(session, 'qr_ready');
      emit('session.qr', { sessionId: session.sessionId, qr: update.qr });
    }

    if (update.connection === 'connecting') {
      setStatus(session, 'connecting');
    }

    if (update.connection === 'open') {
      session.qr = null;
      session.reconnectAttempts = 0;
      setStatus(session, 'connected');
      stderr('[sidecar] session', session.sessionId, 'connected');
    }

    if (update.connection === 'close') {
      const error = update.lastDisconnect && update.lastDisconnect.error;
      const statusCode =
        (error && error.output && error.output.statusCode) ||
        (error && typeof error.statusCode === 'number' ? error.statusCode : undefined);
      const reason = disconnectReasonText(error, statusCode);

      session.sock = null;
      session.qr = null;

      if (statusCode === DisconnectReason.loggedOut) {
        session.autoReconnect = false;
        clearReconnect(session);
        setStatus(session, 'logged_out', reason);
        stderr('[sidecar] session', session.sessionId, 'logged out');
        return;
      }

      setStatus(session, 'disconnected', reason);

      if (!session.autoReconnect) return;
      scheduleReconnect(session);
    }
  });

  sock.ev.on('messages.upsert', ({ messages, type }) => {
    if (type !== 'notify' || !Array.isArray(messages)) return;
    if (session.sock !== sock) return; // stale socket (stopped/replaced)
    for (const msg of messages) {
      try {
        if (!msg || !msg.key) continue;
        const content = unwrapContent(msg.message);
        const kind = (getContentType && content && getContentType(content)) || 'unknown';
        const text =
          (content && content.conversation) ||
          (content && content.extendedTextMessage && content.extendedTextMessage.text) ||
          '';
        emit('message.received', {
          sessionId: session.sessionId,
          messageId: msg.key.id || null,
          chatId: msg.key.remoteJid || null,
          sender: msg.key.participant || msg.key.remoteJid || null,
          fromMe: !!msg.key.fromMe,
          timestamp: toTimestamp(msg.messageTimestamp) ?? Math.floor(Date.now() / 1000),
          text: text || '',
          hasMedia: MEDIA_KINDS.has(kind),
          kind,
        });
      } catch (err) {
        stderr('[sidecar] messages.upsert handler failed:', err);
      }
    }
  });

  if (session.stopped) {
    // stop()/logout() raced with this start: honor the stop.
    await stopSocket(session);
  }

  return session;
}

function disconnectReasonText(error, statusCode) {
  if (error) {
    const payload = error.output && error.output.payload;
    if (payload && payload.message) return payload.message;
    if (error.message) return error.message;
  }
  return statusCode !== undefined && statusCode !== null ? String(statusCode) : undefined;
}

function scheduleReconnect(session) {
  if (shuttingDown || session.stopped || !session.autoReconnect) return;
  if (session.reconnectTimer) return;

  session.reconnectAttempts += 1;
  const delay = Math.min(RECONNECT_MAX_MS, RECONNECT_BASE_MS * Math.min(session.reconnectAttempts, 10));
  stderr('[sidecar] session', session.sessionId, 'reconnecting in', delay, 'ms');

  session.reconnectTimer = setTimeout(() => {
    session.reconnectTimer = null;
    if (shuttingDown || session.stopped || !session.autoReconnect) return;
    startSocket(session).catch((err) => {
      stderr('[sidecar] session', session.sessionId, 'reconnect failed:', err && err.message);
      setStatus(session, 'error', err && err.message);
      scheduleReconnect(session);
    });
  }, delay);
}

async function stopSocket(session) {
  clearReconnect(session);
  session.autoReconnect = false;
  session.stopped = true;
  const sock = session.sock;
  session.sock = null;
  if (sock) {
    try {
      await sock.end(undefined);
    } catch (err) {
      stderr('[sidecar] end() failed:', err && err.message);
    }
  }
}

/* ------------------------------------------------------------------------- */
/* Methods                                                                   */
/* ------------------------------------------------------------------------- */

async function methodStart(params) {
  const session = getSession(params);

  if (session.sock && ['connecting', 'qr_ready', 'connected'].includes(session.status)) {
    return { sessionId: session.sessionId, status: session.status };
  }

  if (session.pendingStart) {
    await session.pendingStart.catch(() => {});
    return { sessionId: session.sessionId, status: session.status };
  }

  session.autoReconnect = true;
  session.stopped = false;
  session.reconnectAttempts = 0;
  clearReconnect(session);
  session.qr = null;

  session.pendingStart = startSocket(session);
  try {
    await session.pendingStart;
  } catch (err) {
    session.autoReconnect = false;
    setStatus(session, 'error', err && err.message);
    throw err;
  } finally {
    session.pendingStart = null;
  }

  setStatus(session, 'connecting');
  return { sessionId: session.sessionId, status: 'connecting' };
}

async function methodStop(params) {
  const session = getSession(params);
  await stopSocket(session);
  setStatus(session, 'disconnected');
  return { sessionId: session.sessionId, status: 'disconnected' };
}

async function methodLogout(params) {
  const session = getSession(params);
  clearReconnect(session);
  session.autoReconnect = false;
  session.stopped = true;

  const sock = session.sock;
  if (sock && typeof sock.logout === 'function') {
    try {
      await sock.logout();
    } catch (err) {
      stderr('[sidecar] logout() failed:', err && err.message);
    }
  } else if (sock && typeof sock.end === 'function') {
    try {
      await sock.end(undefined);
    } catch (err) {
      stderr('[sidecar] end() failed:', err && err.message);
    }
  }

  session.sock = null;
  session.qr = null;
  setStatus(session, 'logged_out');
  return { sessionId: session.sessionId, status: 'logged_out' };
}

async function methodDelete(params) {
  const session = getSession(params);
  await stopSocket(session);
  sessions.delete(session.sessionId);
  fs.rmSync(session.dataDir, { recursive: true, force: true });
  stderr('[sidecar] session', session.sessionId, 'deleted');
  return { deleted: true };
}

async function methodSendText(params) {
  const session = getSession(params);
  const sock = requireConnected(session);

  const chatId = params.chatId;
  const text = params.text;
  if (typeof chatId !== 'string' || !chatId) throw new Error('chatId is required');
  if (typeof text !== 'string') throw new Error('text is required');

  const result = await sock.sendMessage(chatId, { text });
  return { messageId: (result && result.key && result.key.id) || null, chatId };
}

async function methodSendMedia(params) {
  const session = getSession(params);
  const sock = requireConnected(session);

  const chatId = params.chatId;
  if (typeof chatId !== 'string' || !chatId) throw new Error('chatId is required');

  const mediaType = params.mediaType;
  if (!MEDIA_TYPES.includes(mediaType)) {
    throw new Error('invalid mediaType: ' + mediaType);
  }

  let payload;
  if (typeof params.base64 === 'string' && params.base64.length > 0) {
    payload = Buffer.from(params.base64, 'base64');
  } else if (typeof params.url === 'string' && params.url.length > 0) {
    payload = { url: params.url };
  } else {
    throw new Error('media payload required: base64 or url');
  }

  const content = { [mediaType]: payload };
  if (params.mimetype) content.mimetype = params.mimetype;
  if (params.caption) content.caption = params.caption;
  if (params.filename) {
    // Baileys names this field `fileName` (documents only).
    content.fileName = params.filename;
  }

  const result = await sock.sendMessage(chatId, content);
  return { messageId: (result && result.key && result.key.id) || null, chatId };
}

const METHODS = {
  ping: async () => ({ pong: true, version: VERSION }),

  'session.create': async (params) => {
    const name = typeof params.name === 'string' && params.name.trim() ? params.name.trim() : undefined;
    const session = createSession(name);
    emitStatus(session, 'created');
    return sessionInfo(session);
  },

  'session.list': async () => Array.from(sessions.values(), sessionInfo),

  'session.get': async (params) => sessionInfo(getSession(params)),

  'session.start': methodStart,
  'session.stop': methodStop,
  'session.logout': methodLogout,
  'session.delete': methodDelete,

  'session.qr': async (params) => {
    const session = getSession(params);
    return { qr: session.qr || null };
  },

  'message.sendText': methodSendText,
  'message.sendMedia': methodSendMedia,
};

/* ------------------------------------------------------------------------- */
/* Dispatch                                                                  */
/* ------------------------------------------------------------------------- */

async function handleRequest(request) {
  const id = request && Object.prototype.hasOwnProperty.call(request, 'id') ? request.id : null;
  const method = request && request.method;
  const params = (request && request.params) || {};

  if (!method || typeof method !== 'string') {
    respondError(id, 'unknown method: ' + String(method));
    return;
  }

  const handler = Object.prototype.hasOwnProperty.call(METHODS, method) ? METHODS[method] : undefined;
  if (!handler) {
    respondError(id, 'unknown method: ' + method);
    return;
  }

  eventQueue = [];
  try {
    const result = await handler(params);
    respond(id, result);
  } catch (err) {
    const message = err && err.message ? err.message : String(err);
    stderr('[sidecar]', method, 'failed:', message);
    respondError(id, message);
  } finally {
    flushEvents();
  }
}

function handleLine(line) {
  const trimmed = line.trim();
  if (!trimmed) return;

  let request;
  try {
    request = JSON.parse(trimmed);
  } catch (err) {
    stderr('[sidecar] ignoring unparsable line:', trimmed.slice(0, 200));
    return;
  }

  handleRequest(request).catch((err) => {
    stderr('[sidecar] dispatch failed:', err);
  });
}

/* ------------------------------------------------------------------------- */
/* Shutdown                                                                  */
/* ------------------------------------------------------------------------- */

async function shutdown(code) {
  if (shuttingDown) return;
  shuttingDown = true;

  for (const session of sessions.values()) {
    clearReconnect(session);
    session.autoReconnect = false;
    const sock = session.sock;
    session.sock = null;
    if (sock) {
      try {
        await sock.end(undefined);
      } catch (err) {
        stderr('[sidecar] shutdown end() failed:', err && err.message);
      }
    }
  }

  stderr('[sidecar] exiting');
  process.exit(code === undefined ? 0 : code);
}

function parentAlive() {
  const ppid = process.ppid;
  if (!ppid || ppid <= 0) return false;
  try {
    process.kill(ppid, 0);
    return true;
  } catch (err) {
    // EPERM means the process exists but we cannot signal it.
    return !!(err && err.code === 'EPERM');
  }
}

function main() {
  stderr('[sidecar] starting; DATA_DIR=' + DATA_DIR + ' LOG_LEVEL=' + LOG_LEVEL);
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  } catch (err) {
    stderr('[sidecar] could not create DATA_DIR:', err && err.message);
  }

  const rl = readline.createInterface({ input: process.stdin, terminal: false });
  rl.on('line', handleLine);
  rl.on('close', () => {
    // Keep the event loop alive unless the parent gateway is actually gone.
    stderr('[sidecar] stdin closed; waiting for parent to exit');
    const timer = setInterval(() => {
      if (shuttingDown) {
        clearInterval(timer);
        return;
      }
      if (!parentAlive()) {
        clearInterval(timer);
        shutdown(0);
      }
    }, 1000);
  });

  process.on('SIGTERM', () => shutdown(0));
  process.on('SIGINT', () => shutdown(0));

  process.on('uncaughtException', (err) => {
    stderr('[sidecar] uncaught exception:', err);
  });
  process.on('unhandledRejection', (err) => {
    stderr('[sidecar] unhandled rejection:', err);
  });
}

main();
