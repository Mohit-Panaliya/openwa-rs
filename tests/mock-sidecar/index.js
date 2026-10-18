#!/usr/bin/env node
'use strict';

/*
 * Deterministic mock WhatsApp engine implementing docs/SIDECAR_PROTOCOL.md.
 *
 * Purpose: exercise the Rust gateway end to end (HTTP -> stdio JSON-RPC ->
 * events -> webhooks/SSE) in CI and smoke tests without touching WhatsApp's
 * network. Behaviour is fixed so assertions can be exact:
 *
 *   session.start       -> status qr_ready, emits session.status + session.qr
 *                          (qr = "mock-qr-data"), then session.status connected
 *   session.qr          -> { qr: "mock-qr-data" } once started, else null
 *   message.sendText    -> { messageId: "mock-msg-<n>", chatId } + message.received
 *   message.sendMedia   -> { messageId: "mock-media-<n>", chatId } + message.received
 *
 * No npm dependencies: pure Node stdlib, so it runs anywhere.
 */

const readline = require('readline');
const crypto = require('crypto');

const VERSION = '0.1.0';
const QR_DATA = 'mock-qr-data';

let counter = 0;
const sessions = new Map();

function nextId(prefix) {
  counter += 1;
  return `${prefix}-${counter}`;
}

function write(line) {
  process.stdout.write(JSON.stringify(line) + '\n');
}

function reply(id, result) {
  write({ jsonrpc: '2.0', id, result });
}

function replyError(id, message) {
  write({ jsonrpc: '2.0', id, error: { message } });
}

function emit(type, payload) {
  write({ jsonrpc: '2.0', method: 'event', params: { type, payload } });
}

function mustGet(sessionId) {
  const session = sessions.get(sessionId);
  if (!session) {
    throw new Error('session not found');
  }
  return session;
}

function requireConnected(session) {
  if (session.status !== 'connected') {
    throw new Error('session is not connected');
  }
}

const methods = {
  ping() {
    return { pong: true, version: VERSION };
  },

  'session.create'(params) {
    const sessionId = crypto.randomUUID();
    const name = (params.name || '').trim() || sessionId;
    const session = { sessionId, name, status: 'created', qr: null, timers: [] };
    sessions.set(sessionId, session);
    emit('session.status', { sessionId, status: 'created' });
    return { sessionId, name, status: session.status };
  },

  'session.list'() {
    return [...sessions.values()].map(({ sessionId, name, status }) => ({
      sessionId,
      name,
      status,
    }));
  },

  'session.get'(params) {
    const session = mustGet(params.sessionId);
    return { sessionId: session.sessionId, name: session.name, status: session.status };
  },

  'session.start'(params) {
    const session = mustGet(params.sessionId);
    if (session.status === 'connected') {
      return { sessionId: session.sessionId, status: session.status };
    }

    session.status = 'qr_ready';
    session.qr = QR_DATA;
    emit('session.status', { sessionId: session.sessionId, status: 'qr_ready' });
    emit('session.qr', { sessionId: session.sessionId, qr: QR_DATA });

    session.timers.push(
      setTimeout(() => {
        if (sessions.has(session.sessionId)) {
          session.status = 'connected';
          session.qr = null;
          emit('session.status', { sessionId: session.sessionId, status: 'connected' });
        }
      }, 300),
    );

    return { sessionId: session.sessionId, status: 'qr_ready' };
  },

  'session.stop'(params) {
    const session = mustGet(params.sessionId);
    session.timers.forEach(clearTimeout);
    session.timers = [];
    session.status = 'disconnected';
    session.qr = null;
    emit('session.status', { sessionId: session.sessionId, status: 'disconnected' });
    return { sessionId: session.sessionId, status: session.status };
  },

  'session.logout'(params) {
    const session = mustGet(params.sessionId);
    session.timers.forEach(clearTimeout);
    session.timers = [];
    session.status = 'logged_out';
    session.qr = null;
    emit('session.status', { sessionId: session.sessionId, status: 'logged_out' });
    return { sessionId: session.sessionId, status: session.status };
  },

  'session.delete'(params) {
    const session = mustGet(params.sessionId);
    session.timers.forEach(clearTimeout);
    sessions.delete(params.sessionId);
    return { deleted: true };
  },

  'session.qr'(params) {
    const session = mustGet(params.sessionId);
    return { qr: session.qr };
  },

  'message.sendText'(params) {
    const session = mustGet(params.sessionId);
    requireConnected(session);
    const messageId = nextId('mock-msg');
    emit('message.received', {
      sessionId: session.sessionId,
      messageId,
      chatId: params.chatId,
      sender: 'me',
      fromMe: true,
      timestamp: Math.floor(Date.now() / 1000),
      text: params.text,
      hasMedia: false,
      kind: 'text',
    });
    return { messageId, chatId: params.chatId };
  },

  'message.sendMedia'(params) {
    const session = mustGet(params.sessionId);
    requireConnected(session);
    if (!params.base64 && !params.url) {
      throw new Error('either base64 or url is required');
    }
    const messageId = nextId('mock-media');
    emit('message.received', {
      sessionId: session.sessionId,
      messageId,
      chatId: params.chatId,
      sender: 'me',
      fromMe: true,
      timestamp: Math.floor(Date.now() / 1000),
      text: params.caption || null,
      hasMedia: true,
      kind: params.mediaType || 'document',
    });
    return { messageId, chatId: params.chatId };
  },
};

const rl = readline.createInterface({ input: process.stdin, terminal: false });

rl.on('line', (line) => {
  const trimmed = line.trim();
  if (!trimmed) {
    return;
  }

  let request;
  try {
    request = JSON.parse(trimmed);
  } catch {
    process.stderr.write(`mock-sidecar: non-JSON input ignored: ${trimmed}\n`);
    return;
  }

  const { id, method, params = {} } = request;
  const handler = methods[method];
  if (!handler) {
    replyError(id, `unknown method: ${method}`);
    return;
  }

  try {
    reply(id, handler(params || {}));
  } catch (error) {
    replyError(id, error.message || String(error));
  }
});

function shutdown() {
  for (const session of sessions.values()) {
    session.timers.forEach(clearTimeout);
  }
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
process.stdin.on('close', shutdown);
