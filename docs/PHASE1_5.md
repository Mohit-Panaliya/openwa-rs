# Phase 1.5 — persistence, scoped keys, message store, media, dashboard

Everything below is additive to `docs/PHASE1.md`. The sidecar protocol is
unchanged. Storage is SQLite through `rusqlite` (bundled SQLite; no build-time
`DATABASE_URL` needed) at `DB_PATH` (default `$DATA_DIR/openwa.db`), created and
migrated on boot.

`rusqlite` rather than an async driver is deliberate: it pins `libsqlite3-sys` to
the same line the Phase 2 engine crate uses, so the workspace links exactly one
native SQLite and can be built into a single binary.

## Schema

```sql
api_keys(id TEXT PK, name TEXT, role TEXT, key_hash TEXT UNIQUE, key_prefix TEXT,
         allowed_sessions TEXT, allowed_chats TEXT, created_at INTEGER, last_used_at INTEGER)
sessions(id TEXT PK, name TEXT, status TEXT, created_at INTEGER, updated_at INTEGER)
messages(id INTEGER PK AUTOINCREMENT, session_id TEXT, chat_id TEXT, sender TEXT,
         from_me INTEGER, kind TEXT, text TEXT, has_media INTEGER, media_id TEXT,
         timestamp INTEGER, created_at INTEGER)
media(id TEXT PK, session_id TEXT, chat_id TEXT, media_type TEXT, mimetype TEXT,
      filename TEXT, bytes INTEGER, path TEXT, created_at INTEGER)
webhooks(id TEXT PK, session_id TEXT, url TEXT, events TEXT, secret TEXT, created_at INTEGER)
webhook_deliveries(id INTEGER PK AUTOINCREMENT, webhook_id TEXT, session_id TEXT,
                   event_type TEXT, endpoint TEXT, payload TEXT, status_code INTEGER,
                   error TEXT, attempts INTEGER, delivered INTEGER,
                   created_at INTEGER, updated_at INTEGER)
```

`allowed_sessions` / `allowed_chats` / `events` are JSON arrays. API keys are stored
as sha256 hashes; the plaintext is returned once on create/rotate.

Indexes: `messages(session_id, chat_id, id)`, `webhook_deliveries(session_id, id)`.

## Auth model

`X-API-Key` header or `?apiKey=` query resolves to a key row: `role` +
`allowed_sessions` + `allowed_chats`.

| role | can |
| --- | --- |
| `viewer` | read sessions, QR, messages, media |
| `operator` | viewer + start/stop/logout/delete session, send text/media, manage webhooks |
| `admin` | everything, plus keys and `GET /api/stats`; never scoped |

Scoping (applies to workspace keys, not admin):
- `allowed_sessions` non-empty and the target session is not listed → `403`.
- list routes return only the allowed rows.
- `allowed_chats` non-empty → `chatId`-bearing routes require a listed chat (`403`
  otherwise), including `GET .../messages` (where `chatId` becomes required).

Bootstrap: on first boot the gateway takes `API_KEY` (if set) or the generated
`data/.api-key` and ensures an `admin` row with that hash exists.

Status codes: `401` missing/invalid key, `403` role or scope refusal,
`400` bad request, `404` not found, `502` engine error, `504` engine timeout.

## Endpoints added in 1.5

| method | path | role | body → result |
| --- | --- | --- | --- |
| GET | `/api/keys` | admin | → `{keys:[{id,name,role,keyPrefix,allowedSessions,allowedChats,createdAt,lastUsedAt}]}` |
| POST | `/api/keys` | admin | `{name,role,allowedSessions?,allowedChats?}` → `{id,name,role,apiKey,allowedSessions,allowedChats}` |
| DELETE | `/api/keys/{id}` | admin | → `{deleted:true}` (refuses the last admin, `400`) |
| POST | `/api/keys/{id}/rotate` | admin | → `{id,apiKey}` |
| GET | `/api/stats` | admin | → `{sessions,messages,media,webhooks,pendingDeliveries}` |
| GET | `/api/sessions/{id}/messages` | viewer | `?chatId=&limit=&before=&search=` → `{messages:[...]}` |
| POST | `/api/sessions/{id}/messages/send-text` | operator | unchanged, result now `{messageId,chatId,message:{...}}` |
| POST | `/api/sessions/{id}/messages/send-media` | operator | unchanged, result gains `mediaId` |
| GET | `/api/media/{mediaId}` | viewer | → the raw bytes with the stored `content-type` |
| GET | `/api/sessions/{id}/qr.png` | viewer | → the pairing QR as SVG (`content-type: image/svg+xml`; `/qr.svg` is an alias) |
| GET | `/api/webhooks/deliveries` | operator | `?sessionId=&limit=` → `{deliveries:[{id,webhookId,sessionId,eventType,endpoint,statusCode,error,attempts,delivered,createdAt}]}` |
| POST | `/api/webhooks/deliveries/{id}/retry` | operator | → `{retried:true}` |
| GET | `/` | public | dashboard (`dashboard/index.html`), assets under `/assets/*` |

`message` shape (both the store and `messages` responses):

```json
{"id":12,"sessionId":"...","chatId":"...","sender":"...","fromMe":false,
 "kind":"text","text":"...","hasMedia":false,"mediaId":null,"timestamp":1712345678}
```

## Webhook delivery

Delivery is attempted immediately; failures retry 3 more times with backoff
(1s, 5s, 25s), then are left `delivered=0`. Every attempt writes/updates a
`webhook_deliveries` row (payload stored for replay). `POST .../retry` re-runs a
row regardless of state. Signing is unchanged (`X-OpenWA-Event`,
`X-OpenWA-Signature: sha256=...`).

## Engine selection (Phase 2 hook)

`ENGINE_CMD` (default `node sidecar/index.js`) is the command the gateway spawns
for the stdio engine. Setting it to the Rust engine binary
(`crates/engine-sidecar`) runs the same protocol with no Node runtime, and
`ENGINE_MODE=embedded` runs the Rust engine in-process for a single binary.

## Environment added in 1.5

| var | default | meaning |
| --- | --- | --- |
| `DATA_DIR` | `data` | root for the database, media and engine session state |
| `DB_PATH` | `$DATA_DIR/openwa.db` | SQLite database file |
| `MEDIA_DIR` | `$DATA_DIR/media` | stored media blobs |
| `DASHBOARD_DIR` | `dashboard` | static dashboard directory served at `/` |
| `ENGINE_CMD` | `node <SIDECAR_DIR>/index.js` | engine process to spawn |

## Dashboard contract

Vanilla HTML/CSS/JS, no build step, served from `dashboard/`. API key is entered
once and kept in `localStorage`; every request sends `X-API-Key`. Uses
`GET /api/sessions`, `POST /api/sessions`, start/stop/delete, `GET .../qr.png`
(as an `<img>` with `?apiKey=`), `GET .../messages`, send text, webhooks CRUD +
deliveries, `GET /api/stats`, and `EventSource /api/events?apiKey=`.
