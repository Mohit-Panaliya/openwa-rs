# Sidecar protocol (v1) — locked contract

Transport: **newline-delimited JSON-RPC 2.0 over the child process's stdio.**
The Rust gateway spawns `node <SIDECAR_DIR>/index.js` with piped stdin/stdout.
`stderr` is diagnostics only and is inherited by the gateway (never protocol data).
`stdout` carries **only** protocol lines — no console.log, no logger output.

## Request (gateway → sidecar)

```json
{"jsonrpc":"2.0","id":12,"method":"session.start","params":{"sessionId":"..."}}
```

## Response (sidecar → gateway)

```json
{"jsonrpc":"2.0","id":12,"result":{"sessionId":"...","status":"connecting"}}
{"jsonrpc":"2.0","id":12,"error":{"message":"session not found"}}
```

Rules:
- `id` is an increasing integer chosen by the gateway. Every request gets exactly one response with the same `id`.
- Errors use `error.message` (string). `result` is absent on error.
- Unknown method → `error.message = "unknown method: <name>"`.

## Event (sidecar → gateway, unsolicited)

```json
{"jsonrpc":"2.0","method":"event","params":{"type":"session.status","payload":{...}}}
```

Event types:

| type | payload |
| --- | --- |
| `session.status` | `{sessionId, status, reason?}` |
| `session.qr` | `{sessionId, qr}` |
| `message.received` | `{sessionId, messageId, chatId, sender, fromMe, timestamp, text, hasMedia, kind}` |

## Methods

| method | params | result |
| --- | --- | --- |
| `ping` | `{}` | `{pong:true, version:"0.1.0"}` |
| `session.create` | `{name?}` | `{sessionId, name, status}` |
| `session.list` | `{}` | `[{sessionId, name, status}]` |
| `session.get` | `{sessionId}` | `{sessionId, name, status}` |
| `session.start` | `{sessionId}` | `{sessionId, status}` |
| `session.stop` | `{sessionId}` | `{sessionId, status}` |
| `session.logout` | `{sessionId}` | `{sessionId, status}` |
| `session.delete` | `{sessionId}` | `{deleted:true}` |
| `session.qr` | `{sessionId}` | `{qr: string \| null}` |
| `message.sendText` | `{sessionId, chatId, text}` | `{messageId, chatId}` |
| `message.sendMedia` | `{sessionId, chatId, mediaType, mimetype, base64?, url?, filename?, caption?}` | `{messageId, chatId}` |

`mediaType` ∈ `image | video | audio | document`.

## Session status values

`created` · `connecting` · `qr_ready` · `connected` · `disconnected` · `logged_out` · `error`

## Sidecar environment

| var | default | meaning |
| --- | --- | --- |
| `DATA_DIR` | `./sidecar/data` | per-session auth state + media scratch |
| `LOG_LEVEL` | `silent` | diagnostics to stderr only |

## Node sidecar requirements

- Read stdin line-by-line (`readline`, `terminal: false`); parse each non-empty line as a request.
- Keep the event loop alive; do not exit on stdin close unless the parent is gone.
- All logging goes to `process.stderr`.
- `session.create` must not touch the network.
- `session.start` loads/creates auth state from `DATA_DIR/<sessionId>` and starts a Baileys socket.
- Auto-reconnect on `close` unless the disconnect reason is `loggedOut` or the session was explicitly stopped.
- On `SIGTERM`/`SIGINT`: close all sockets, then exit.
