# Parity with OpenWA

Status of `openwa-rs` vs. `rmyndharis/OpenWA` (NestJS). "Implemented" = route exists in the
Rust gateway and proxies to the engine; "Stubbed" = route exists but the engine side is a no-op
or returns a placeholder; "Missing" = not yet implemented.

## Implemented (core + parity expansion)
- Sessions: create, list, get, start, stop, logout, delete, qr, qr.png/qr.svg
- Messages: send-text, send-media, send-bulk, send-location, send-contact (stub), list (paginated, chat filter, search), react, edit, delete (revoke), star, forward (stub), pin (stub)
- Contacts: list (DB), get (user info), check (is_on_whatsapp), profile picture, block, unblock
- Groups: list, get, create (stubbed result), leave, set subject, set description (stubbed), add/remove participants, promote/demote, invite link, settings, join via invite, membership requests
- Profile: get, update name/about, set/remove picture
- Labels: list, create, delete, add/remove chat, list chats
- Chats: mark read, archive/unarchive, pin/unpin, mute/unmute, delete, clear
- Presence: subscribe, set
- Status: send text (stubbed engine side)
- Newsletters (channels): list, get, follow, unfollow
- Polls: create, vote
- Webhooks: create, list, delete, deliveries, retry
- Keys: list, create, delete, rotate (roles, session + chat scoping, last-admin guard)
- Media: upload (base64), serve
- Stats, search (basic), audit log (admin)
- Health: /api/health, /api/health/live, /api/health/ready
- Events: SSE at /events
- Dashboard: vanilla HTML/CSS/JS at /

## Stubbed
- group.create (returns groupId: null), group.setDescription, status.sendText (engine side no-ops)
- label.list (returns empty from engine; DB-backed in gateway)
- message.sendContact, message.forward, message.pin (engine side stubs)
- contact.update (engine side stub)
- calls.reject (engine side stub)

## Missing (roadmap)
- Message: template, reactions events
- Contacts: block/unblock (implemented), update (stub), phone lookup, profile picture upload
- Groups: invite link revoke, settings set, profile picture
- Profile: profile picture set/remove (implemented)
- Channels/newsletters, calls, labels full CRUD via appstate
- Search: FTS plugin provider, global /api/search with scoping
- Audit: full coverage of all operations
- Infra: Postgres/Redis/S3, import/export, multi-node forwarding, send pacing, CIDR whitelisting, rate limiting tiers, MCP server (25-51 tools), plugin system, Swagger/OpenAPI, Bull Board queue dashboard, n8n
- Dashboard: React UI, queue dashboard

## Engine
- Rust engine (whatsapp-rust 0.7) replaces Node/Baileys via stdio JSON-RPC; embedded mode via `ENGINE_MODE=embedded` + `embedded-engine` feature for single binary.
- Persistence: file-backed Backend (JSON snapshot) to avoid SQLite version collision.