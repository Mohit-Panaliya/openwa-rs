# Status — What Works and What Doesn't

This document is the source of truth for the current state of `openwa-rs` versus the
reference implementation `rmyndharis/OpenWA` (NestJS). Last updated: 2026-10-18.

## Summary

- **Single binary, minimal RSS**: `openwa-gateway.exe` built with `--features embedded-engine`
  runs the Rust WhatsApp engine in-process. Idle RSS ~9 MB (release). No Node, no sidecar process.
- **REST parity**: ~65% of OpenWA's REST surface is implemented and working.
- **Quality**: `cargo fmt` clean, `cargo clippy --workspace --all-targets -- -D warnings` clean,
  `cargo test --workspace` all pass, smoke test 33/33 PASS.

## What Works (implemented and tested)

### Core
- Multi-session management (create, list, get, start, stop, logout, delete)
- QR code generation and serving (SVG on `/qr.png` and `/qr.svg`)
- Session status tracking (created, connecting, qr_ready, connected, disconnected, logged_out, error)
- Message send (text, media with base64/url), message list (paginated, chat filter, search)
- Message react, edit, delete (revoke), star
- Webhook registration, delivery log, retries (1s/5s/25s, max 4 attempts), manual retry
- API key auth with roles (viewer/operator/admin), session + chat scoping, last-admin guard
- Media upload (base64) and serving (`/api/media/{id}`)
- SQLite persistence (rusqlite bundled), schema auto-migrated
- Event fan-out via SSE (`/events`) and webhooks
- Dashboard (React, served at `/`)

### Parity expansion (engine + gateway)
- Contacts: list, get (user info), check (is_on_whatsapp), profile picture, block, unblock, update (stub)
- Groups: list, get, create (real), leave, set subject, set description (real), add/remove/promote/demote, invite link, settings, join, membership requests, profile picture
- Profile: get, update name/about, set/remove picture
- Newsletters (channels): list, get, follow, unfollow
- Polls: create, vote
- Chats: mark-read, archive/unarchive, pin/unpin, mute/unmute, delete, clear
- Presence: subscribe, set
- Status: send text (real)
- Labels: list, create, delete, add/remove chat, list chats
- Bulk send, location, contact card (vCard), forward (stub), pin (stub)

### Infra
- CIDR/IP allow-list on API keys
- Per-IP rate limiting middleware (env: RATE_LIMIT_MAX, RATE_LIMIT_WINDOW_MS)
- Send pacing (env: SEND_PACING_ENABLED, per-session daily cap)
- FTS5 full-text search over messages (with LIKE fallback)
- Audit log (admin endpoint, best-effort wiring in routes)
- Multi-node: NODE_ID/PEER_NODES env, owner_node tracking, 409 hint for non-local sessions
- MCP endpoint (read + write tools) at /mcp
- OpenAPI spec at /api/openapi.json, Swagger UI at /api/docs
- Health: /api/health, /api/health/live, /api/health/ready

### Engine
- Rust engine (whatsapp-rust 0.7) replaces Node/Baileys via stdio JSON-RPC
- Embedded mode: `ENGINE_MODE=embedded` + `embedded-engine` feature for single binary
- Persistence: file-backed Backend (JSON snapshot) to avoid SQLite version collision
- Real QR generation (whatsapp-rust, scan verified in 1.2s)

## What Doesn't Work (stubs / missing)

### Stubs (route exists, engine side is no-op or placeholder)
- `message.sendContact` (sends vCard as text, not native contact card)
- `message.forward` (returns success, no actual forward)
- `message.pin` (no upstream API)
- `contact.update` (best-effort save_contact)
- `group.create` (returns groupId from real create_group, but may need GroupCreateOptions tuning)
- `status.sendText` (real, but requires StatusSendOptions construction)
- `calls.reject` (no upstream calls API)

### Not Implemented (platform features)
- **Plugin system** — requires a JS runtime (deno_core); fundamentally not a Rust thing
- **Multi-node full request forwarding** — currently returns 409 with owner hint; full proxy not implemented
- **Postgres backend** — storage layer is SQLite-only (rusqlite)
- **Redis cache** — no caching layer
- **S3/MinIO storage** — media stored locally only
- **Import/export migration** — depends on backends
- **Bull Board queue dashboard** — needs job queue infrastructure
- **n8n integration** — external, out of scope
- **MCP write tools** — partially (send_text, send_media, send_reaction, sessions_start/stop); full 51-tool surface not covered
- **Calls handling** — no call.received event wiring beyond basic emit
- **Message reactions events** — emit done, but no persistence
- **Full audit coverage** — best-effort wiring, not every operation
- **Send pacing full** — basic daily cap, no per-minute/typing simulation
- **CIDR tiers** — basic allow-list, no tiered rate limits
- **React dashboard full** — basic functional, not full OpenWA React feature parity

## Architecture

```
openwa-rs/
├── crates/
│   ├── gateway/          # Rust HTTP gateway (axum, rusqlite, tower)
│   ├── engine/           # Rust WhatsApp engine (whatsapp-rust)
│   └── engine-sidecar/   # Stdio JSON-RPC sidecar binary
├── dashboard/            # React (Vite) dashboard
├── sidecar/              # Node/Baileys sidecar (legacy engine)
├── tests/                # Mock sidecar for smoke tests
├── scripts/              # Smoke test, memory measurement, verification
└── docs/                 # Phase specs, protocol, parity, toolchain
```

## Engine Abstraction

The `Engine` enum supports two variants:
- `Stdio` — spawns a child process (Node/Baileys or Rust sidecar)
- `Embedded` (behind `embedded-engine` feature) — in-process Rust engine

Switch via `ENGINE_MODE=embedded` env var. Single binary uses embedded mode.

## Storage

- SQLite (rusqlite 0.39, bundled) — single connection behind mutex
- Schema auto-migrated on boot at `$DATA_DIR/openwa.db`
- Media stored in `$DATA_DIR/media/`
- Per-session engine state in `$DATA_DIR/{sessionId}/` (JSON snapshot)

## Auth Model

- API keys: sha256-hashed, roles (viewer < operator < admin)
- Session scoping: `allowedSessions` on keys
- Chat scoping: `allowedChats` on keys
- IP allow-list: `allowedIps` with CIDR support
- Bootstrap admin key from `API_KEY` env or `data/.api-key` file

## Memory

| Configuration | Gateway RSS | Node RSS | Total |
|---|---|---|---|
| Idle (0 sessions) | 8.4 MB | 44.5 MB | 52.9 MB |
| 3 sessions | 8.6 MB | 98.9 MB | 107.5 MB |
| Single binary (embedded, idle) | 9.1 MB | — | 9.1 MB |

## Test Coverage

- Unit tests: 12 (gateway) + 2 (engine store)
- Integration tests: 5 (db_store) + 1 (sidecar contract)
- Smoke test: 33 assertions (mock engine)
- Engine sidecar selftest: 23 assertions
- Real network: QR generation verified in 1.2s

## Remaining Roadmap

See `docs/PARITY.md` for the full parity matrix. The platform features (plugins, multi-node full forwarding, alternate DBs, queue dashboard, n8n) are deferred as they require external runtimes or cluster infrastructure. The single-binary Rust port stays focused on the gateway + engine.