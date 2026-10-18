# Phase 1 — Rust gateway + Node/Baileys sidecar

Goal: OpenWA-compatible HTTP surface in Rust (low RSS), with the WhatsApp engine as a
swappable sidecar process. Phase 2 replaces the sidecar with a Rust engine
(`whatsapp-rust` / `whatsmeow`) to become a single binary.

## Run

```powershell
# 1) sidecar deps (once)
npm --prefix sidecar install

# 2) gateway
cargo run -p openwa-gateway
# http://localhost:2785/api/health
```

First boot prints the generated admin API key once and stores it at `data/.api-key`.

## Environment (gateway)

| var | default | meaning |
| --- | --- | --- |
| `PORT` | `2785` | HTTP port (same default as OpenWA) |
| `HOST` | `0.0.0.0` | bind address |
| `API_KEY` | — | admin key; otherwise read/created at `data/.api-key` |
| `SIDECAR_DIR` | `./sidecar` | directory containing `index.js` |
| `NODE_BIN` | `node` | node executable |
| `REQUEST_TIMEOUT_MS` | `15000` | sidecar RPC timeout |
| `RUST_LOG` | `info` | tracing filter |

## HTTP API

Auth: `X-API-Key: <key>` header **or** `?apiKey=<key>` query parameter (the query form
exists for `EventSource`/SSE clients, which cannot set headers). `/api/health` is public.

Errors: `{"error":"message"}` with `400` (bad request), `401` (unauthorized),
`404` (unknown session/resource), `502` (sidecar error), `504` (sidecar timeout).

| method | path | body | result |
| --- | --- | --- | --- |
| GET | `/api/health` | — | `{status:"ok", sidecar:"up"\|"down"}` |
| GET | `/api/sessions` | — | `{sessions:[{sessionId,name,status}]}` |
| POST | `/api/sessions` | `{name}` | `{sessionId,name,status}` |
| GET | `/api/sessions/{sessionId}` | — | `{sessionId,name,status}` |
| POST | `/api/sessions/{sessionId}/start` | — | `{status}` |
| POST | `/api/sessions/{sessionId}/stop` | — | `{status}` |
| POST | `/api/sessions/{sessionId}/logout` | — | `{status}` |
| DELETE | `/api/sessions/{sessionId}` | — | `{deleted:true}` |
| GET | `/api/sessions/{sessionId}/qr` | — | `{qr:string\|null}` |
| POST | `/api/sessions/{sessionId}/messages/send-text` | `{chatId,text}` | `{messageId,chatId}` |
| POST | `/api/sessions/{sessionId}/messages/send-media` | `{chatId,mediaType,mimetype,base64?\|url?,filename?,caption?}` | `{messageId,chatId}` |
| GET | `/api/events` | — | SSE stream of `{type,payload}` |
| POST | `/api/sessions/{sessionId}/webhooks` | `{url,events:[...],secret?}` | `{id,url,events}` |
| GET | `/api/sessions/{sessionId}/webhooks` | — | `{webhooks:[{id,url,events}]}` |
| DELETE | `/api/sessions/{sessionId}/webhooks/{webhookId}` | — | `{deleted:true}` |

### Examples

```bash
curl http://localhost:2785/api/health

curl -X POST http://localhost:2785/api/sessions \
  -H "X-API-Key: $KEY" -H "Content-Type: application/json" -d '{"name":"my-bot"}'

curl -X POST http://localhost:2785/api/sessions/$SID/messages/send-text \
  -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"chatId":"628123456789@c.us","text":"Hello from openwa-rs"}'
```

## Webhooks

In-memory registry per session (Phase 1.5: persist + retries/backoff + delivery log).
On a matching event the gateway POSTs the payload as JSON with:

- `X-OpenWA-Event: <type>`
- `X-OpenWA-Signature: sha256=<hex hmac-sha256 of the raw body with the webhook secret>`

An empty `events` array subscribes to every event. Dispatch is fire-and-forget with a
5s timeout; failures are logged, not retried.

## Layout

```
openwa-rs/
├── crates/gateway/     # Rust: axum HTTP API, auth, webhooks, SSE
├── sidecar/            # Node: Baileys bridge, implements docs/SIDECAR_PROTOCOL.md
├── tests/mock-sidecar/ # Node: protocol-complete fake engine for smoke tests (no network)
├── scripts/            # smoke test + dev helpers
└── docs/               # SIDECAR_PROTOCOL.md, PHASE1.md
```

## Roadmap

- **Phase 1 (done):** Rust gateway + Baileys sidecar, OpenWA-compatible core REST.
  Phase 1.5 (persistence, scoped keys, message/media store, webhook retries,
  dashboard) is documented in `docs/PHASE1_5.md`.
- **Phase 2 (next):** swap the sidecar for a Rust engine
  (`crates/engine` implementing the same contract), then run it in-process to
  collapse to one static binary and drop Node entirely.
