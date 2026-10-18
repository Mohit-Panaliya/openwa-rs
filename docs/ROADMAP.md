# Roadmap — Whole Plan

This document captures the full plan for `openwa-rs`, executed in phases. Each phase is
independently verifiable.

## Phase 1 — Rust Gateway + Node Sidecar (COMPLETE)

**Goal**: Replace the NestJS gateway with a Rust axum gateway while keeping the
Node/Baileys engine as a sidecar behind a locked JSON-RPC contract.

**Deliverables**:
- Rust axum gateway (port 2785, OpenWA-compatible REST)
- Node/Baileys sidecar (stdio JSON-RPC per `docs/SIDECAR_PROTOCOL.md`)
- Mock engine for offline testing
- Docker image + compose, CI workflow, PowerShell scripts
- Memory measurement: 52.9 MB idle (gateway 8.4 + node 44.5)

**Verification**: 13/13 smoke, real QR verification, memory measurement.

## Phase 1.5 — Persistence, Scoping, Store, Dashboard (COMPLETE)

**Goal**: Add SQLite persistence, API key scoping, webhook delivery log, media store,
and a web dashboard.

**Deliverables**:
- rusqlite 0.39 (bundled) store with auto-migration
- DB-backed API keys (sha256-hashed), roles, session + chat scoping
- Webhook delivery log with retries (1s/5s/25s, max 4 attempts)
- Media store + `/api/media/{id}`
- Dashboard (vanilla HTML/CSS/JS, served at `/`)
- Smoke test extended 13 → 33 assertions
- Memory re-measured: 8.6 MB gateway / 98.9 MB node / 107.5 MB total (3 sessions)

**Verification**: 33/33 smoke, clippy clean, 18 tests pass.

## Phase 2 — Rust Engine (COMPLETE)

**Goal**: Replace the Node/Baileys sidecar with a pure-Rust engine (whatsapp-rust 0.7)
so the gateway ships as a single binary.

**Deliverables**:
- `crates/engine` — Rust WhatsApp engine implementing `docs/SIDECAR_PROTOCOL.md`
- `crates/engine-sidecar` — stdio JSON-RPC sidecar binary
- File-backed Backend (JSON snapshot) to avoid SQLite version collision
- Embedded mode: `ENGINE_MODE=embedded` + `embedded-engine` feature
- Selftest: 23/23 protocol checks, real QR in 1.2s

**Verification**: `cargo build -p openwa-engine` 0.44s, selftest 23/23, real network QR test pass.

## Phase 3 — Parity Expansion (IN PROGRESS)

**Goal**: Expand REST surface toward OpenWA parity (~65% achieved).

**Completed**:
- Contacts, groups, profile, labels, chats, presence, status, newsletters, polls
- Bulk send, location, contact card, forward (stub), pin (stub)
- CIDR/IP allow-list, rate limiting, send pacing, FTS5 search, audit log
- Multi-node ownership tracking
- MCP endpoint (read + write tools), OpenAPI spec, Swagger UI
- React dashboard (Vite + React)

**Remaining** (platform features):
- Plugin system, multi-node full forwarding, Postgres/Redis/S3, import/export
- Queue dashboard, n8n integration, calls handling, full audit coverage

## Phase 4 — Future Scope (DEFERRED)

These are platform features, not REST routes, and several require a JS runtime:

1. **Plugin system** — sandboxed JS workers (Chatwoot, Typebot, n8n). Requires deno_core.
2. **Multi-node full forwarding** — cluster membership, request proxying, sticky sessions.
3. **Postgres backend** — swap rusqlite for tokio-postgres/sqlx (conflicts with engine's sqlite).
4. **Redis cache** — optional caching layer.
5. **S3/MinIO storage** — swap local media store for cloud storage.
6. **Import/export migration** — depends on backends.
7. **Bull Board queue dashboard** — needs job queue infrastructure.
8. **n8n integration** — external workflow automation.
9. **Swagger UI polish** — interactive docs (currently basic).
10. **Calls handling** — call.received events, reject (stubbed).
11. **Message reactions events** — emit done, no persistence.
12. **Full audit coverage** — best-effort wiring, not every operation.

## Single Binary Strategy

The primary deliverable is a single Rust binary with minimal RSS:
- `cargo build -p openwa-gateway --features embedded-engine --release`
- Runs the Rust engine in-process (no Node, no sidecar)
- Idle RSS: ~9 MB (vs 52.9 MB for gateway + Node)
- `ENGINE_MODE=embedded` selects embedded mode

## Testing Strategy

1. **Unit tests** — auth, routing, DB store (rusqlite)
2. **Integration tests** — sidecar contract (mock engine speaks protocol)
3. **Smoke test** — 33 assertions against mock engine (offline)
4. **Engine selftest** — 23 protocol assertions against Rust sidecar
5. **Real network** — QR generation verified against WhatsApp (1.2s)
6. **Memory measurement** — RSS before/after sessions

## Deployment

- **Docker**: multi-stage build, copies dashboard/dist, sets DASHBOARD_DIR
- **Compose**: production (SQLite) + optional profiles (postgres, redis, minio)
- **CI**: GitHub Actions (fmt, clippy, build, test) for gateway + engine
- **Env vars**: see `.env.example` for full configuration

## Non-Goals

- Reimplementing the OpenWA plugin system (requires JS runtime)
- Full multi-node cluster (requires cluster membership protocol)
- Alternate database backends (Postgres, Redis) — SQLite is sufficient for single-node
- Full OpenWA React dashboard parity — basic functional dashboard provided
- n8n integration — external, use OpenWA for this