# openwa-rs

OpenWA-compatible WhatsApp HTTP gateway, rewritten around a Rust core.

The familiar REST surface (same routes, same `:2785` default port, same
`X-API-Key` auth) now runs on a Rust axum gateway with SQLite persistence, scoped
API keys, a webhook delivery log with retries, and a built-in dashboard. The
WhatsApp engine stays behind a locked process boundary
(`docs/SIDECAR_PROTOCOL.md`), so it can be swapped without touching the API:
Node/Baileys today, a Rust engine for the single-binary build next.

## Why

The original OpenWA runs `whatsapp-web.js`, which drives a full headless Chromium
per session — roughly 300-500 MB RSS per session, so a handful of sessions
saturates a small VPS. The same protocol is reachable over a plain WebSocket with
Baileys at roughly 30-80 MB per session. openwa-rs splits the problem so the
HTTP/auth/webhook/store layer is small and stable Rust, while the WhatsApp engine
lives behind a locked contract that can be replaced (Phase 2) without touching
the API.

## Architecture

```
HTTP client (SDK / curl / dashboard)
        |
        |  HTTP + X-API-Key  (port 2785)
        v
+--------------------------------------------------+
|  Rust axum gateway                               |
|  auth + roles/scoping, session registry,         |
|  SQLite store (keys, sessions, messages, media,  |
|  webhooks, deliveries), webhook retries,         |
|  SSE /api/events, dashboard at /                 |
+--------------------------------------------------+
        |
        |  newline-delimited JSON-RPC 2.0 over stdio
        v
+--------------------------------------------------+
|  Engine process (swappable)                      |
|  Node/Baileys today; crates/engine-sidecar later |
|  one socket per session                          |
+--------------------------------------------------+
        |
        |  WhatsApp Web protocol (websocket)
        v
   WhatsApp servers
```

Requests flow down; responses and unsolicited events (QR, incoming messages) flow
back up the same stdio pipe. `stderr` is diagnostics only.

## Quick start

```powershell
# 1) sidecar dependencies (once)
npm --prefix sidecar install

# 2) build and run the gateway (defaults to http://localhost:2785)
cargo run -p openwa-gateway

# first boot prints the generated admin API key once and stores it at data/.api-key
Get-Content data/.api-key
Invoke-RestMethod http://localhost:2785/api/health

# dashboard (asks for that API key once, keeps it in localStorage)
Start-Process http://localhost:2785/
```

Configuration is by environment variable; see `.env.example` for the full list.
Everything has a working default, so the two commands above are enough for a local
run. State lives under `DATA_DIR` (default `data/`): `openwa.db`, `media/`, and
the engine's per-session auth state.

On Windows without Visual Studio, see `docs/WINDOWS-TOOLCHAIN.md` first: this
project builds on the GNU host plus a MinGW-w64 binutils distribution.

### Verification

```powershell
# end-to-end, no WhatsApp account needed: mock engine, store, keys, webhooks, dashboard
pwsh scripts/smoke-test.ps1

# real Baileys engine: boots the socket, waits for a live pairing QR, pairs nothing
pwsh scripts/verify-real-sidecar.ps1

# Rust unit tests + the stdio protocol contract test against the mock engine
cargo test -p openwa-gateway

# memory: idle vs. N live sockets, per process
pwsh scripts/measure-memory.ps1 -Sessions 3
```

## HTTP API

Auth: `X-API-Key: <key>` header **or** `?apiKey=<key>` query parameter (the query
form exists for `EventSource`/SSE clients and `<img>` tags, which cannot set
headers). `/api/health` and the dashboard at `/` are public.

Errors: `{"error":"message"}` with `400` (bad request), `401` (unauthorized),
`403` (role or scope refusal), `404` (unknown resource), `502` (engine error),
`504` (engine timeout).

### Core

| method | path | body | result |
| --- | --- | --- | --- |
| GET | `/api/health` | — | `{status:"ok", sidecar:"up"\|"down", engine:"stdio"}` |
| GET | `/api/sessions` | — | `{sessions:[{sessionId,name,status}]}` |
| POST | `/api/sessions` | `{name}` | `{sessionId,name,status}` |
| GET | `/api/sessions/{sessionId}` | — | `{sessionId,name,status}` |
| POST | `/api/sessions/{sessionId}/start` | — | `{status}` |
| POST | `/api/sessions/{sessionId}/stop` | — | `{status}` |
| POST | `/api/sessions/{sessionId}/logout` | — | `{status}` |
| DELETE | `/api/sessions/{sessionId}` | — | `{deleted:true}` |
| GET | `/api/sessions/{sessionId}/qr` | — | `{qr:string\|null}` |
| POST | `/api/sessions/{sessionId}/messages/send-text` | `{chatId,text}` | `{messageId,chatId,message}` |
| POST | `/api/sessions/{sessionId}/messages/send-media` | `{chatId,mediaType,mimetype,base64?\|url?,filename?,caption?}` | `{messageId,chatId,mediaId,message}` |
| GET | `/api/sessions/{sessionId}/webhooks` | — | `{webhooks:[{id,url,events}]}` |
| POST | `/api/sessions/{sessionId}/webhooks` | `{url,events:[...],secret?}` | `{id,url,events}` |
| DELETE | `/api/sessions/{sessionId}/webhooks/{webhookId}` | — | `{deleted:true}` |
| GET | `/api/events` | — | SSE stream of `{type,payload}` |

### Persistence, keys and the dashboard

| method | path | role | body → result |
| --- | --- | --- | --- |
| GET | `/api/sessions/{sessionId}/messages` | viewer | `?chatId=&limit=&before=&search=` → `{messages:[...]}` |
| GET | `/api/media/{mediaId}` | viewer | the stored bytes with their content type |
| GET | `/api/sessions/{sessionId}/qr.png` | viewer | the pairing QR as SVG (`/qr.svg` is an alias) |
| GET | `/api/webhooks/deliveries` | operator | `?sessionId=&limit=` → `{deliveries:[...]}` |
| POST | `/api/webhooks/deliveries/{id}/retry` | operator | `{retried:true}` |
| GET | `/api/keys` | admin | `{keys:[{id,name,role,keyPrefix,allowedSessions,allowedChats}]}` |
| POST | `/api/keys` | admin | `{name,role,allowedSessions?,allowedChats?}` → `{id,apiKey,...}` |
| DELETE | `/api/keys/{id}` | admin | `{deleted:true}` (refuses the last admin) |
| POST | `/api/keys/{id}/rotate` | admin | `{id,apiKey}` |
| GET | `/api/stats` | admin | `{sessions,messages,media,webhooks,pendingDeliveries,keys}` |
| GET | `/` | public | the dashboard (`dashboard/`) |

Roles: `viewer` reads (sessions, QR, messages, media, webhooks), `operator` also
sends messages and manages sessions/webhooks, `admin` adds key management and
stats. A non-admin key with `allowedSessions` sees only those sessions; with
`allowedChats` it may only address those chats (and `GET .../messages` then
requires an explicit `chatId`). Keys are stored as sha256 hashes — the plaintext
is returned once, on create and on rotate. On first boot the gateway registers
the `API_KEY` / `data/.api-key` key as the bootstrap admin.

### Storage

`rusqlite` (bundled SQLite) at `$DATA_DIR/openwa.db`: `api_keys`, `sessions`,
`messages`, `media`, `webhooks`, `webhook_deliveries`. Messages are persisted from
the engine event stream *and* from the send handlers; a unique
`(session_id, engine_message_id)` index keeps those two paths from duplicating a
message. Inline (base64) media is written to `$DATA_DIR/media` and served from
`/api/media/{id}`.

### Webhooks

Subscriptions live in SQLite. Every dispatch is recorded in
`webhook_deliveries`; failures retry with backoff (1s, 5s, 25s) and a background
worker finishes retries interrupted by a restart. `POST .../retry` re-sends one
recorded delivery by hand. Deliveries are JSON POSTs carrying
`X-OpenWA-Event: <type>` and
`X-OpenWA-Signature: sha256=<hmac-sha256 of the raw body with the webhook secret>`.

## Memory

Measured on Windows with a release build (`pwsh scripts/measure-memory.ps1`), one
Rust gateway process plus one Node sidecar process holding live WhatsApp
multi-device sockets:

| state | Rust gateway | Node sidecar | total |
| --- | --- | --- | --- |
| idle, 0 sessions | 8.4 MB | 44.5 MB | 52.9 MB |
| 3 sessions socket-ready | 8.6 MB | 98.9 MB | 107.5 MB |

The gateway is essentially flat (~1.8 MB private) even with SQLite, scoped keys,
the message store and webhook retries in place; the sidecar costs ~18 MB of
marginal memory per live session. Blended that is ~36 MB per session, against
OpenWA's ~300-500 MB per session with `whatsapp-web.js`.

| engine | RSS per session | notes |
| --- | --- | --- |
| Chromium (`whatsapp-web.js`, OpenWA today) | ~300-500 MB | one headless Chrome renderer per session |
| Node + Baileys sidecar (Phase 1, measured) | ~45 MB base + ~19 MB/session | websocket only, no browser |
| Rust engine (Phase 2 target) | target ~10-30 MB | single static binary, no Node runtime |

The Phase 1 win is the removal of Chromium. Phase 2 removes the Node runtime
itself.

## Layout

```
openwa-rs/
├── crates/gateway/        # Rust: axum HTTP API, auth/roles, SQLite store, webhooks, SSE
├── crates/engine/         # Rust: in-process engine implementing docs/SIDECAR_PROTOCOL.md (Phase 2)
├── crates/engine-sidecar/ # Rust: stdio front end for crates/engine (Phase 2)
├── dashboard/             # Vanilla HTML/CSS/JS dashboard, served at / (no build step)
├── sidecar/               # Node: Baileys bridge, implements docs/SIDECAR_PROTOCOL.md
├── tests/mock-sidecar/    # Node: protocol-complete fake engine for smoke tests (no network)
├── scripts/               # smoke-test, verify-real-sidecar, measure-memory
├── docs/                  # SIDECAR_PROTOCOL.md, PHASE1.md, PHASE1_5.md, WINDOWS-TOOLCHAIN.md
├── .github/workflows/     # CI: fmt, clippy, node syntax checks, smoke test
├── Dockerfile             # multi-stage: Rust builder + Debian runtime with Node 22
└── docker-compose.yml
```

## Roadmap

- **Phase 1 — done:** Rust gateway + Baileys sidecar, OpenWA-compatible core REST.
- **Phase 1.5 — done:** SQLite persistence, API-key roles/session/chat scoping,
  message store + `GET .../messages`, local media storage + `/api/media/{id}`,
  webhook retries with a delivery log and manual retry, QR image rendering,
  built-in dashboard.
- **Phase 2 — in progress:** swap the sidecar for the Rust engine
  (`docs/SIDECAR_PROTOCOL.md` implemented in `crates/engine`), then run it
  in-process (`ENGINE_MODE=embedded`) to ship as one binary and drop Node.

## Warning

This is an **unofficial** WhatsApp client. It is not affiliated with, endorsed
by, or supported by WhatsApp or Meta, and automating a personal account can get
that account banned. Follow OpenWA's own guidance:

- use a dedicated phone number, never your primary account;
- do not cold-blast: unsolicited bulk messaging is the fastest route to a ban;
- keep rate limits and reply only to people who expect to hear from you.

Use at your own risk.
