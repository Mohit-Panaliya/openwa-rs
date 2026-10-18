# openwa-rs Node.js sidecar

Node.js sidecar process for the Rust gateway. It speaks **newline-delimited
JSON-RPC 2.0 over stdin/stdout** exactly as specified in
[`../docs/SIDECAR_PROTOCOL.md`](../docs/SIDECAR_PROTOCOL.md) (v1, locked
contract) and drives WhatsApp Web sessions through
[`@whiskeysockets/baileys`](https://github.com/WhiskeySockets/Baileys).

```
node sidecar/index.js
```

The gateway is expected to spawn this process with piped `stdin`/`stdout` and an
inherited `stderr`:

```rust
Command::new("node")
    .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/../sidecar/index.js"))
    .env("DATA_DIR", data_dir)
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .stderr(Stdio::inherit())
```

## Install

```
npm --prefix sidecar install
```

Runtime dependencies: `@whiskeysockets/baileys` and `pino`. Everything else is
Node's standard library.

## Environment

| var | default | meaning |
| --- | --- | --- |
| `DATA_DIR` | `<sidecar>/data` (i.e. `./sidecar/data` when spawned from the repo root) | per-session Baileys auth state |
| `LOG_LEVEL` | `silent` | pino level for diagnostics; **stderr only** |

## stdout discipline

`stdout` carries **only** protocol lines:

* responses: `{"jsonrpc":"2.0","id":<id>,"result":{...}}` or
  `{"jsonrpc":"2.0","id":<id>,"error":{"message":"..."}}`
* events: `{"jsonrpc":"2.0","method":"event","params":{"type":"...","payload":{...}}}`

Everything else (Baileys logs, pino output, internal errors) goes to `stderr`.
`console.log`/`info`/`debug`/`warn`/`trace` are redirected to `stderr` at startup
so a stray dependency log can never corrupt the protocol stream.

A request's response is always written **before** the events that the request
triggered (e.g. the `session.status` event for a fresh `session.create` arrives
after the `session.create` response), so the gateway can register the new
`sessionId` before it sees events referring to it.

## Methods

`ping`, `session.create`, `session.list`, `session.get`, `session.start`,
`session.stop`, `session.logout`, `session.delete`, `session.qr`,
`message.sendText`, `message.sendMedia` — params/result shapes are exactly the
ones tabulated in the protocol doc. Unknown methods answer
`error.message = "unknown method: <name>"`; unknown or missing `sessionId`
answers `error.message = "session not found"`. Every request is wrapped in
try/catch and always produces exactly one response with the same `id`.

Statuses follow the protocol: `created`, `connecting`, `qr_ready`, `connected`,
`disconnected`, `logged_out`, `error`.

## Behaviour notes

* `session.create` is network-free: it only mints a UUID and stores the session
  in memory with status `created` (nothing is written to disk).
* `session.start` loads/creates auth state from `DATA_DIR/<sessionId>` with
  `useMultiFileAuthState`, then calls
  `makeWASocket({ auth, logger: pino({ level: LOG_LEVEL }, process.stderr) })`.
  The removed `printQRInTerminal` option is **not** passed; QRs are delivered
  through the `session.qr` event and the `session.qr` method instead.
* Handlers wired on the socket: `creds.update` → `saveCreds`,
  `connection.update` (emits `session.status` / `session.qr`; `connected` on
  `connection === 'open'`), `messages.upsert` → `message.received` for
  `type === 'notify'` only.
* Auto-reconnect: on `connection === 'close'` the session moves to
  `disconnected` and is restarted after a short backoff (3s, doubling up to
  30s) unless the disconnect reason is `DisconnectReason.loggedOut` or the
  session was explicitly stopped/deleted. Events from a replaced/stopped
  socket are ignored.
* `message.sendText` / `message.sendMedia` require status `connected`, otherwise
  they fail with `session is not connected`. Media is built as
  `{ <mediaType>: Buffer|{url}, mimetype?, caption?, fileName? }`; note the
  protocol's `filename` param maps to Baileys' `fileName` field.
* `session.logout` calls `sock.logout()` when available, sets `logged_out` and
  disables auto-reconnect. `session.delete` stops the session, drops it from
  memory and removes `DATA_DIR/<sessionId>`.
* `SIGTERM`/`SIGINT` end every socket and exit `0`. When stdin reaches EOF the
  process keeps running (as the protocol requires) and polls the parent PID,
  exiting on its own once the parent is gone. (On Windows a parent using
  `child.kill()` terminates the child immediately, so it never sees a signal.)
* `message.received` payload: `{sessionId, messageId, chatId, sender, fromMe,
  timestamp, text, hasMedia, kind}`. `text` comes from `conversation` or
  `extendedTextMessage.text`; `kind` is the Baileys content type
  (`text`, `image`, `video`, `audio`, `document`, `sticker`, …) and `hasMedia`
  is true for the media kinds. `timestamp` is Unix **seconds** (a number, never
  `null`). `sender` is `key.participant ?? key.remoteJid`. One event is emitted
  per message in each `notify` upsert.

## Verification

`dev-selftest.js` is a small temporary harness: it spawns `index.js` as a child
process and asserts the network-free part of the protocol (`ping`,
`session.create`, `session.list`, `session.get`, `session.qr`, plus error cases
and stdout purity). It never calls `session.start`, which would contact
WhatsApp.

```
node sidecar/dev-selftest.js      # or: npm --prefix sidecar run selftest
```

`session.start` itself was verified manually against the real API (socket
created, QR received, `qr_ready` + `session.qr` emitted, clean stop) — see the
session notes in the change that introduced this file. `message.sendText`,
`message.sendMedia` and `message.received` require a paired account and were not
exercised end-to-end.

Requires Node.js 22.12+ (or 20.19+) because Baileys 7 is ESM-only and is loaded
via `await import()`.
