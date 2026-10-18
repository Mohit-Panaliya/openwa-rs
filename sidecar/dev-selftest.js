'use strict';

/**
 * Temporary verification harness for the openwa-rs sidecar.
 *
 * Spawns `index.js` as a child process and exercises the network-free part of
 * the protocol (docs/SIDECAR_PROTOCOL.md). `session.start` is intentionally
 * never called: it would hit WhatsApp's servers.
 *
 *   node dev-selftest.js
 */

const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const os = require('os');

const INDEX = path.join(__dirname, 'index.js');
const DATA_DIR = path.join(os.tmpdir(), 'openwa-sidecar-selftest-' + process.pid);

const results = [];
let stdoutLines = [];
let responses = new Map();
let events = [];
let nextId = 0;
let child;
let rawStdout = '';

function check(name, ok, detail) {
  results.push({ name, ok: !!ok, detail });
  console.log((ok ? '  PASS  ' : '  FAIL  ') + name + (ok || detail === undefined ? '' : '  -> ' + detail));
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function waitFor(predicate, timeoutMs, label) {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const tick = () => {
      let value;
      try {
        value = predicate();
      } catch (err) {
        return reject(err);
      }
      if (value) return resolve(value);
      if (Date.now() - started > timeoutMs) return reject(new Error('timeout waiting for ' + label));
      setTimeout(tick, 20);
    };
    tick();
  });
}

function rpc(method, params) {
  const id = ++nextId;
  child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params: params || {} }) + '\n');
  return id;
}

async function call(method, params) {
  const id = rpc(method, params);
  const line = await waitFor(() => responses.get(id), 15000, method + ' response');
  return line;
}

async function step(name, fn) {
  try {
    await fn();
  } catch (err) {
    check(name, false, err && err.message ? err.message : String(err));
  }
}

function start() {
  if (fs.existsSync(DATA_DIR)) fs.rmSync(DATA_DIR, { recursive: true, force: true });
  fs.mkdirSync(DATA_DIR, { recursive: true });

  child = spawn(process.execPath, [INDEX], {
    cwd: __dirname,
    env: Object.assign({}, process.env, { DATA_DIR, LOG_LEVEL: 'error' }),
    stdio: ['pipe', 'pipe', 'pipe'],
  });

  child.stdout.setEncoding('utf8');
  child.stdout.on('data', (chunk) => {
    rawStdout += chunk;
    let idx;
    while ((idx = rawStdout.indexOf('\n')) !== -1) {
      const line = rawStdout.slice(0, idx);
      rawStdout = rawStdout.slice(idx + 1);
      if (!line.trim()) continue;
      stdoutLines.push(line);
      let parsed;
      try {
        parsed = JSON.parse(line);
      } catch {
        continue; // recorded as a protocol violation below
      }
      if (parsed && parsed.method === 'event') events.push(parsed);
      else if (parsed && Object.prototype.hasOwnProperty.call(parsed, 'id')) responses.set(parsed.id, parsed);
    }
  });

  child.stderr.setEncoding('utf8');
  child.stderr.on('data', (chunk) => process.stderr.write('[child] ' + chunk));
}

async function run() {
  start();

  let created = null;

  await step('ping -> {pong:true, version:"0.1.0"}', async () => {
    const res = await call('ping', {});
    check('ping returns pong:true', res.result && res.result.pong === true, JSON.stringify(res));
    check('ping returns version 0.1.0', res.result && res.result.version === '0.1.0', JSON.stringify(res.result));
  });

  await step('session.create -> {sessionId, name, status:"created"} + session.status event', async () => {
    const res = await call('session.create', { name: 'selftest' });
    created = res.result;
    check('create result.status === "created"', created && created.status === 'created', JSON.stringify(created));
    check('create result.sessionId is a non-empty string', typeof created.sessionId === 'string' && created.sessionId.length >= 8);
    check('create result.name === "selftest"', created.name === 'selftest', JSON.stringify(created));

    const ev = await waitFor(
      () => events.find((e) => e.params.type === 'session.status' && e.params.payload.sessionId === created.sessionId),
      5000,
      'session.status event',
    );
    check('create emits session.status with status "created"', ev.params.payload.status === 'created', JSON.stringify(ev));

    const evQr = events.find((e) => e.params.type === 'session.qr');
    check('create emits no session.qr event', !evQr);
  });

  await step('session.create without name', async () => {
    const res = await call('session.create', {});
    check('anonymous create returns a string name', res.result && typeof res.result.name === 'string' && res.result.name.length > 0, JSON.stringify(res.result));
    await call('session.delete', { sessionId: res.result.sessionId });
  });

  await step('session.list -> [{sessionId, name, status}]', async () => {
    const res = await call('session.list', {});
    check('list returns an array', Array.isArray(res.result), JSON.stringify(res));
    const entry = Array.isArray(res.result) ? res.result.find((s) => s.sessionId === created.sessionId) : undefined;
    check('list contains the created session', !!entry, JSON.stringify(res.result));
    check('list entry is {sessionId, name, status}', !!entry && entry.name === 'selftest' && entry.status === 'created', JSON.stringify(entry));
  });

  await step('session.get -> {sessionId, name, status}', async () => {
    const res = await call('session.get', { sessionId: created.sessionId });
    check('get returns the created session', res.result && res.result.sessionId === created.sessionId && res.result.name === 'selftest' && res.result.status === 'created', JSON.stringify(res));
  });

  await step('session.get unknown -> error "session not found"', async () => {
    const res = await call('session.get', { sessionId: 'does-not-exist' });
    check('unknown session id errors', !res.result && res.error && res.error.message === 'session not found', JSON.stringify(res));
  });

  await step('session.get missing sessionId -> error "session not found"', async () => {
    const res = await call('session.get', {});
    check('missing sessionId errors', !res.result && res.error && res.error.message === 'session not found', JSON.stringify(res));
  });

  await step('session.qr -> {qr: null} for a created (not started) session', async () => {
    const res = await call('session.qr', { sessionId: created.sessionId });
    check('qr result has null qr', res.result && Object.prototype.hasOwnProperty.call(res.result, 'qr') && res.result.qr === null, JSON.stringify(res));
  });

  await step('session.start unknown id -> error "session not found" (no network)', async () => {
    const res = await call('session.start', { sessionId: 'nope' });
    check('start on unknown session errors', !res.result && res.error && res.error.message === 'session not found', JSON.stringify(res));
  });

  await step('message.sendText on unconnected session -> error "session is not connected"', async () => {
    const res = await call('message.sendText', { sessionId: created.sessionId, chatId: '123@s.whatsapp.net', text: 'hi' });
    check('sendText on non-connected session errors', !res.result && res.error && res.error.message === 'session is not connected', JSON.stringify(res));
  });

  await step('message.sendMedia on unconnected session -> error "session is not connected"', async () => {
    const res = await call('message.sendMedia', { sessionId: created.sessionId, chatId: '123@s.whatsapp.net', mediaType: 'image', mimetype: 'image/png', base64: 'aGk=' });
    check('sendMedia on non-connected session errors', !res.result && res.error && res.error.message === 'session is not connected', JSON.stringify(res));
  });

  await step('session.stop -> {sessionId, status}', async () => {
    const res = await call('session.stop', { sessionId: created.sessionId });
    check('stop returns disconnected', res.result && res.result.sessionId === created.sessionId && res.result.status === 'disconnected', JSON.stringify(res));
  });

  await step('unknown method -> error "unknown method: nope"', async () => {
    const res = await call('nope', {});
    check('unknown method errors', !res.result && res.error && res.error.message === 'unknown method: nope', JSON.stringify(res));
  });

  await step('session.delete -> {deleted:true}', async () => {
    const res = await call('session.delete', { sessionId: created.sessionId });
    check('delete returns deleted:true', res.result && res.result.deleted === true, JSON.stringify(res));
    const list = await call('session.list', {});
    check('deleted session is gone from session.list', Array.isArray(list.result) && !list.result.some((s) => s.sessionId === created.sessionId), JSON.stringify(list.result));
  });

  await step('stdout carries only protocol lines', async () => {
    let bad = null;
    for (const line of stdoutLines) {
      let parsed;
      try {
        parsed = JSON.parse(line);
      } catch {
        bad = line;
        break;
      }
      if (!parsed || parsed.jsonrpc !== '2.0' || (!('id' in parsed) && parsed.method !== 'event')) {
        bad = line;
        break;
      }
    }
    check('every stdout line is a JSON-RPC 2.0 response or event', !bad, bad === null ? undefined : JSON.stringify(bad).slice(0, 200));
  });
}

(async () => {
  try {
    await run();
  } catch (err) {
    check('selftest harness', false, err && err.message ? err.message : String(err));
  }

  // Windows can't deliver a catchable SIGTERM to a child, so we only assert that
  // the sidecar actually terminates when asked.
  let exited = null;
  if (child && child.exitCode === null && child.signalCode === null) {
    const done = new Promise((resolve) => child.once('exit', (code, signal) => resolve({ code, signal })));
    child.kill();
    exited = await Promise.race([done, sleep(5000).then(() => null)]);
  } else if (child) {
    exited = { code: child.exitCode, signal: child.signalCode };
  }
  check('sidecar child process terminates on kill', !!exited, 'timed out after 5s');

  if (child && (child.exitCode === null && child.signalCode === null)) child.kill('SIGKILL');
  if (fs.existsSync(DATA_DIR)) fs.rmSync(DATA_DIR, { recursive: true, force: true });

  const failed = results.filter((r) => !r.ok);
  console.log('');
  console.log(
    (failed.length === 0 ? 'PASS' : 'FAIL') +
      ' - ' + (results.length - failed.length) + '/' + results.length + ' checks passed' +
      '  (' + stdoutLines.length + ' stdout lines, ' + events.length + ' events)',
  );
  if (failed.length) {
    console.log('failed checks: ' + failed.map((f) => f.name).join('; '));
  }
  process.exit(failed.length === 0 ? 0 : 1);
})();
