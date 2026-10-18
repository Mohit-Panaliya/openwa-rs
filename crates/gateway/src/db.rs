//! SQLite persistence.
//!
//! `rusqlite` (bundled SQLite) on a single connection behind a mutex. Statements
//! are sub-millisecond local-file queries and are run inline; the gateway's
//! latency is dominated by the engine and the network, not by SQLite.
//!
//! rusqlite is deliberate: it pins `libsqlite3-sys` to the same major line as the
//! Phase 2 engine crate, so the whole workspace links exactly one native SQLite
//! (an async driver such as sqlx would introduce a second, conflicting copy).

use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::types::Value;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension, Row};
use serde::Serialize;

use crate::util::now_ms;

const SCHEMA: &str = "\
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS api_keys (
    id TEXT PRIMARY KEY, name TEXT NOT NULL, role TEXT NOT NULL,
    key_hash TEXT NOT NULL UNIQUE, key_prefix TEXT NOT NULL,
    allowed_sessions TEXT NOT NULL DEFAULT '[]',
    allowed_chats TEXT NOT NULL DEFAULT '[]',
    allowed_ips TEXT NOT NULL DEFAULT '[]',
    created_at INTEGER NOT NULL, last_used_at INTEGER);
CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL,
    created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, owner_node TEXT);
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL,
    chat_id TEXT NOT NULL, engine_message_id TEXT, sender TEXT,
    from_me INTEGER NOT NULL DEFAULT 0,
    kind TEXT NOT NULL DEFAULT 'text', text TEXT,
    has_media INTEGER NOT NULL DEFAULT 0, media_id TEXT,
    timestamp INTEGER NOT NULL, created_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS idx_messages_chat ON messages(session_id, chat_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_engine ON messages(session_id, engine_message_id)
    WHERE engine_message_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS media (
    id TEXT PRIMARY KEY, session_id TEXT NOT NULL, chat_id TEXT,
    media_type TEXT NOT NULL, mimetype TEXT, filename TEXT,
    bytes INTEGER NOT NULL, path TEXT NOT NULL, created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL,
    contact_id TEXT NOT NULL, name TEXT, phone TEXT, is_business INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
    UNIQUE(session_id, contact_id));
CREATE TABLE IF NOT EXISTS groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL,
    group_id TEXT NOT NULL, subject TEXT, owner TEXT, size INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
    UNIQUE(session_id, group_id));
CREATE TABLE IF NOT EXISTS labels (
    id TEXT PRIMARY KEY, session_id TEXT NOT NULL, name TEXT NOT NULL,
    color INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS label_chats (
    label_id TEXT NOT NULL, chat_id TEXT NOT NULL, session_id TEXT NOT NULL,
    PRIMARY KEY(label_id, chat_id));
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT, action TEXT NOT NULL, actor TEXT,
    resource TEXT, details TEXT, created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS webhooks (
    id TEXT PRIMARY KEY, session_id TEXT NOT NULL, url TEXT NOT NULL,
    events TEXT NOT NULL DEFAULT '[]', secret TEXT, created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT, webhook_id TEXT NOT NULL,
    session_id TEXT NOT NULL, event_type TEXT NOT NULL, endpoint TEXT NOT NULL,
    payload TEXT NOT NULL, status_code INTEGER, error TEXT,
    attempts INTEGER NOT NULL DEFAULT 0, delivered INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS idx_deliveries_session ON webhook_deliveries(session_id, id);
CREATE TABLE IF NOT EXISTS session_config (
    session_id TEXT PRIMARY KEY, auto_reject_calls INTEGER NOT NULL DEFAULT 0,
    max_reconnect_attempts INTEGER NOT NULL DEFAULT 0,
    reconnect_base_delay INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS send_pacing (
    session_id TEXT PRIMARY KEY, daily_count INTEGER NOT NULL DEFAULT 0,
    last_reset INTEGER NOT NULL DEFAULT 0);
";

#[derive(Clone)]
pub struct Db {
    conn: Arc<Mutex<Connection>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyRecord {
    pub id: String,
    pub name: String,
    pub role: String,
    pub key_prefix: String,
    pub allowed_sessions: Vec<String>,
    pub allowed_chats: Vec<String>,
    pub allowed_ips: Vec<String>,
    pub created_at: i64,
    pub last_used_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionConfig {
    pub session_id: String,
    pub auto_reject_calls: bool,
    pub max_reconnect_attempts: i64,
    pub reconnect_base_delay: i64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SendPacing {
    pub session_id: String,
    pub daily_count: i64,
    pub last_reset: i64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionRecord {
    pub id: String,
    pub name: String,
    pub status: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageRecord {
    pub id: i64,
    pub session_id: String,
    pub chat_id: String,
    pub sender: Option<String>,
    pub from_me: bool,
    pub kind: String,
    pub text: Option<String>,
    pub has_media: bool,
    pub media_id: Option<String>,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaRecord {
    pub id: String,
    pub session_id: String,
    pub chat_id: Option<String>,
    pub media_type: String,
    pub mimetype: Option<String>,
    pub filename: Option<String>,
    pub bytes: i64,
    pub path: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebhookRecord {
    pub id: String,
    pub session_id: String,
    pub url: String,
    pub events: Vec<String>,
    #[serde(skip_serializing)]
    pub secret: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryRecord {
    pub id: i64,
    pub webhook_id: String,
    pub session_id: String,
    pub event_type: String,
    pub endpoint: String,
    pub status_code: Option<i64>,
    pub error: Option<String>,
    pub attempts: i64,
    pub delivered: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone)]
pub struct NewDelivery {
    pub webhook_id: String,
    pub session_id: String,
    pub event_type: String,
    pub endpoint: String,
    pub payload: String,
}

/// A message to persist. `engine_message_id` deduplicates events against sends.
#[derive(Debug, Clone)]
pub struct NewMessage<'a> {
    pub session_id: &'a str,
    pub chat_id: &'a str,
    pub engine_message_id: Option<&'a str>,
    pub sender: Option<&'a str>,
    pub from_me: bool,
    pub kind: &'a str,
    pub text: Option<&'a str>,
    pub has_media: bool,
    pub media_id: Option<&'a str>,
    pub timestamp: i64,
}

fn json_string(list: &[String]) -> String {
    serde_json::to_string(list).unwrap_or_else(|_| "[]".to_string())
}

fn json_list(raw: Option<String>) -> Vec<String> {
    raw.and_then(|value| serde_json::from_str::<Vec<String>>(&value).ok())
        .unwrap_or_default()
}

fn key_from_row(row: &Row<'_>) -> rusqlite::Result<KeyRecord> {
    Ok(KeyRecord {
        id: row.get("id")?,
        name: row.get("name")?,
        role: row.get("role")?,
        key_prefix: row.get("key_prefix")?,
        allowed_sessions: json_list(row.get("allowed_sessions")?),
        allowed_chats: json_list(row.get("allowed_chats")?),
        allowed_ips: json_list(row.get("allowed_ips")?),
        created_at: row.get("created_at")?,
        last_used_at: row.get("last_used_at")?,
    })
}

fn message_from_row(row: &Row<'_>) -> rusqlite::Result<MessageRecord> {
    Ok(MessageRecord {
        id: row.get("id")?,
        session_id: row.get("session_id")?,
        chat_id: row.get("chat_id")?,
        sender: row.get("sender")?,
        from_me: row.get::<_, i64>("from_me")? != 0,
        kind: row.get("kind")?,
        text: row.get("text")?,
        has_media: row.get::<_, i64>("has_media")? != 0,
        media_id: row.get("media_id")?,
        timestamp: row.get("timestamp")?,
    })
}

fn webhook_from_row(row: &Row<'_>) -> rusqlite::Result<WebhookRecord> {
    Ok(WebhookRecord {
        id: row.get("id")?,
        session_id: row.get("session_id")?,
        url: row.get("url")?,
        events: json_list(row.get("events")?),
        secret: row.get("secret")?,
        created_at: row.get("created_at")?,
    })
}

fn delivery_from_row(row: &Row<'_>) -> rusqlite::Result<DeliveryRecord> {
    Ok(DeliveryRecord {
        id: row.get("id")?,
        webhook_id: row.get("webhook_id")?,
        session_id: row.get("session_id")?,
        event_type: row.get("event_type")?,
        endpoint: row.get("endpoint")?,
        status_code: row.get("status_code")?,
        error: row.get("error")?,
        attempts: row.get("attempts")?,
        delivered: row.get::<_, i64>("delivered")? != 0,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
    })
}

const MESSAGE_COLUMNS: &str =
    "id, session_id, chat_id, sender, from_me, kind, text, has_media, media_id, timestamp";

impl Db {
    pub async fn connect(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(path)?;
        connection.execute_batch("PRAGMA busy_timeout = 5000;")?;
        let db = Self {
            conn: Arc::new(Mutex::new(connection)),
        };
        db.migrate()?;
        Ok(db)
    }

    fn conn(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().expect("database mutex poisoned")
    }

    fn migrate(&self) -> anyhow::Result<()> {
        self.conn().execute_batch(SCHEMA)?;
        // Best-effort column additions for older databases.
        for statement in [
            "ALTER TABLE messages ADD COLUMN starred INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE messages ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE api_keys ADD COLUMN allowed_ips TEXT NOT NULL DEFAULT '[]'",
            "ALTER TABLE sessions ADD COLUMN owner_node TEXT",
        ] {
            let _ = self.conn().execute(statement, []);
        }
        // FTS5 virtual table + triggers (best-effort: ignore if FTS5 unavailable).
        let fts_statements = [
            "CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(text, content='messages', content_rowid='id')",
            "CREATE TRIGGER IF NOT EXISTS messages_fts_ai AFTER INSERT ON messages BEGIN INSERT INTO messages_fts(rowid, text) VALUES (new.id, new.text); END;",
            "CREATE TRIGGER IF NOT EXISTS messages_fts_ad AFTER DELETE ON messages BEGIN INSERT INTO messages_fts(messages_fts, rowid, text) VALUES('delete', old.id, old.text); END;",
            "CREATE TRIGGER IF NOT EXISTS messages_fts_au AFTER UPDATE ON messages BEGIN INSERT INTO messages_fts(messages_fts, rowid, text) VALUES('delete', old.id, old.text); INSERT INTO messages_fts(rowid, text) VALUES (new.id, new.text); END;",
        ];
        for statement in fts_statements {
            let _ = self.conn().execute_batch(statement);
        }
        Ok(())
    }

    // ---- api keys -------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    pub async fn insert_key(
        &self,
        id: &str,
        name: &str,
        role: &str,
        key_hash: &str,
        prefix: &str,
        allowed_sessions: &[String],
        allowed_chats: &[String],
        allowed_ips: &[String],
    ) -> Result<KeyRecord, rusqlite::Error> {
        let now = now_ms();
        self.conn().execute(
            "INSERT INTO api_keys
             (id, name, role, key_hash, key_prefix, allowed_sessions, allowed_chats, allowed_ips, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                id,
                name,
                role,
                key_hash,
                prefix,
                json_string(allowed_sessions),
                json_string(allowed_chats),
                json_string(allowed_ips),
                now
            ],
        )?;

        Ok(KeyRecord {
            id: id.to_string(),
            name: name.to_string(),
            role: role.to_string(),
            key_prefix: prefix.to_string(),
            allowed_sessions: allowed_sessions.to_vec(),
            allowed_chats: allowed_chats.to_vec(),
            allowed_ips: allowed_ips.to_vec(),
            created_at: now,
            last_used_at: None,
        })
    }

    pub async fn list_keys(&self) -> Result<Vec<KeyRecord>, rusqlite::Error> {
        let guard = self.conn();
        let mut statement = guard.prepare(
            "SELECT id, name, role, key_prefix, allowed_sessions, allowed_chats, allowed_ips, created_at, last_used_at
             FROM api_keys ORDER BY created_at",
        )?;
        let keys = statement
            .query_map([], key_from_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(keys)
    }

    pub async fn find_key_by_hash(
        &self,
        key_hash: &str,
    ) -> Result<Option<KeyRecord>, rusqlite::Error> {
        self.conn()
            .query_row(
                "SELECT id, name, role, key_prefix, allowed_sessions, allowed_chats, allowed_ips, created_at, last_used_at
                 FROM api_keys WHERE key_hash = ?1",
                params![key_hash],
                key_from_row,
            )
            .optional()
    }

    pub async fn touch_key(&self, id: &str) -> Result<(), rusqlite::Error> {
        self.conn().execute(
            "UPDATE api_keys SET last_used_at = ?1 WHERE id = ?2",
            params![now_ms(), id],
        )?;
        Ok(())
    }

    pub async fn rotate_key(
        &self,
        id: &str,
        key_hash: &str,
        prefix: &str,
    ) -> Result<bool, rusqlite::Error> {
        let changed = self.conn().execute(
            "UPDATE api_keys SET key_hash = ?1, key_prefix = ?2 WHERE id = ?3",
            params![key_hash, prefix, id],
        )?;
        Ok(changed > 0)
    }

    pub async fn count_keys_by_role(&self, role: &str) -> Result<i64, rusqlite::Error> {
        self.conn().query_row(
            "SELECT COUNT(*) FROM api_keys WHERE role = ?1",
            params![role],
            |row| row.get(0),
        )
    }

    pub async fn delete_key(&self, id: &str) -> Result<bool, rusqlite::Error> {
        let changed = self
            .conn()
            .execute("DELETE FROM api_keys WHERE id = ?1", params![id])?;
        Ok(changed > 0)
    }

    // ---- sessions -------------------------------------------------------

    pub async fn upsert_session(
        &self,
        id: &str,
        name: &str,
        status: &str,
        owner_node: Option<&str>,
    ) -> Result<(), rusqlite::Error> {
        let owner_param: Option<String> = owner_node.map(str::to_string);
        let owner_param = owner_param.filter(|value| !value.trim().is_empty());
        let default_owner = crate::cluster::ClusterConfig::default_node_id();
        let now = now_ms();
        self.conn().execute(
            "INSERT INTO sessions (id, name, status, created_at, updated_at, owner_node)
             VALUES (?1, ?2, ?3, ?4, ?5, COALESCE(?6, ?7))
             ON CONFLICT(id) DO UPDATE SET
                status = excluded.status,
                name = CASE WHEN excluded.name = '' THEN sessions.name ELSE excluded.name END,
                updated_at = excluded.updated_at,
                owner_node = COALESCE(CASE WHEN ?6 IS NOT NULL THEN ?6 END, sessions.owner_node, ?7)",
            params![id, name, status, now, now, owner_param, default_owner],
        )?;
        Ok(())
    }

    pub async fn get_session_owner(
        &self,
        session_id: &str,
    ) -> Result<Option<String>, rusqlite::Error> {
        self.conn()
            .query_row(
                "SELECT owner_node FROM sessions WHERE id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .optional()
    }

    pub async fn set_session_owner(
        &self,
        session_id: &str,
        node: &str,
    ) -> Result<(), rusqlite::Error> {
        self.conn().execute(
            "UPDATE sessions SET owner_node = ?1, updated_at = ?2 WHERE id = ?3",
            params![node, now_ms(), session_id],
        )?;
        Ok(())
    }

    pub async fn set_session_status(&self, id: &str, status: &str) -> Result<(), rusqlite::Error> {
        self.conn().execute(
            "UPDATE sessions SET status = ?1, updated_at = ?2 WHERE id = ?3",
            params![status, now_ms(), id],
        )?;
        Ok(())
    }

    pub async fn list_sessions(&self) -> Result<Vec<SessionRecord>, rusqlite::Error> {
        let guard = self.conn();
        let mut statement = guard.prepare(
            "SELECT id, name, status, created_at, updated_at FROM sessions ORDER BY created_at",
        )?;
        let sessions = statement
            .query_map([], |row| {
                Ok(SessionRecord {
                    id: row.get("id")?,
                    name: row.get("name")?,
                    status: row.get("status")?,
                    created_at: row.get("created_at")?,
                    updated_at: row.get("updated_at")?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(sessions)
    }

    pub async fn delete_session(&self, id: &str) -> Result<(), rusqlite::Error> {
        self.conn()
            .execute("DELETE FROM sessions WHERE id = ?1", params![id])?;
        Ok(())
    }

    pub async fn count_sessions(&self) -> Result<i64, rusqlite::Error> {
        self.conn()
            .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
    }

    // ---- messages -------------------------------------------------------

    /// Insert a message unless the same engine message id was already stored.
    ///
    /// Both the event stream and the send handlers persist messages, so the
    /// unique `(session_id, engine_message_id)` index deduplicates them.
    pub async fn save_message(&self, message: &NewMessage<'_>) -> Result<i64, rusqlite::Error> {
        let guard = self.conn();
        let changed = guard.execute(
            "INSERT OR IGNORE INTO messages
             (session_id, chat_id, engine_message_id, sender, from_me, kind, text, has_media, media_id, timestamp, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                message.session_id,
                message.chat_id,
                message.engine_message_id,
                message.sender,
                i64::from(message.from_me),
                message.kind,
                message.text,
                i64::from(message.has_media),
                message.media_id,
                message.timestamp,
                now_ms()
            ],
        )?;

        if changed > 0 {
            return Ok(guard.last_insert_rowid());
        }

        // Already stored: return the existing row.
        if let Some(engine_id) = message.engine_message_id {
            let existing = guard
                .query_row(
                    "SELECT id FROM messages WHERE session_id = ?1 AND engine_message_id = ?2",
                    params![message.session_id, engine_id],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?;
            if let Some(id) = existing {
                return Ok(id);
            }
        }

        guard.query_row(
            "SELECT id FROM messages WHERE session_id = ?1 AND chat_id = ?2 ORDER BY id DESC LIMIT 1",
            params![message.session_id, message.chat_id],
            |row| row.get(0),
        )
    }

    pub async fn get_message(&self, id: i64) -> Result<Option<MessageRecord>, rusqlite::Error> {
        self.conn()
            .query_row(
                &format!("SELECT {MESSAGE_COLUMNS} FROM messages WHERE id = ?1"),
                params![id],
                message_from_row,
            )
            .optional()
    }

    pub async fn list_messages(
        &self,
        session_id: &str,
        chat_id: Option<&str>,
        limit: i64,
        before: Option<i64>,
        search: Option<&str>,
    ) -> Result<Vec<MessageRecord>, rusqlite::Error> {
        let mut sql = format!("SELECT {MESSAGE_COLUMNS} FROM messages WHERE session_id = ?1");
        let mut values: Vec<Value> = vec![Value::Text(session_id.to_string())];

        if let Some(chat) = chat_id {
            sql.push_str(&format!(" AND chat_id = ?{}", values.len() + 1));
            values.push(Value::Text(chat.to_string()));
        }
        if let Some(before_id) = before {
            sql.push_str(&format!(" AND id < ?{}", values.len() + 1));
            values.push(Value::Integer(before_id));
        }
        if let Some(term) = search {
            let like = format!("%{term}%");
            let first = values.len() + 1;
            sql.push_str(&format!(
                " AND (text LIKE ?{first} OR sender LIKE ?{} OR chat_id LIKE ?{})",
                first + 1,
                first + 2
            ));
            values.push(Value::Text(like.clone()));
            values.push(Value::Text(like.clone()));
            values.push(Value::Text(like));
        }
        sql.push_str(&format!(" ORDER BY id DESC LIMIT ?{}", values.len() + 1));
        values.push(Value::Integer(limit.clamp(1, 500)));

        let guard = self.conn();
        let mut statement = guard.prepare(&sql)?;
        let mut messages = statement
            .query_map(params_from_iter(values.iter()), message_from_row)?
            .collect::<Result<Vec<_>, _>>()?;
        messages.reverse();
        Ok(messages)
    }

    pub async fn count_messages(&self) -> Result<i64, rusqlite::Error> {
        self.conn()
            .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
    }

    // ---- media ----------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    pub async fn insert_media(
        &self,
        id: &str,
        session_id: &str,
        chat_id: Option<&str>,
        media_type: &str,
        mimetype: Option<&str>,
        filename: Option<&str>,
        bytes: i64,
        path: &str,
    ) -> Result<MediaRecord, rusqlite::Error> {
        let now = now_ms();
        self.conn().execute(
            "INSERT INTO media
             (id, session_id, chat_id, media_type, mimetype, filename, bytes, path, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![id, session_id, chat_id, media_type, mimetype, filename, bytes, path, now],
        )?;

        Ok(MediaRecord {
            id: id.to_string(),
            session_id: session_id.to_string(),
            chat_id: chat_id.map(str::to_string),
            media_type: media_type.to_string(),
            mimetype: mimetype.map(str::to_string),
            filename: filename.map(str::to_string),
            bytes,
            path: path.to_string(),
            created_at: now,
        })
    }

    pub async fn get_media(&self, id: &str) -> Result<Option<MediaRecord>, rusqlite::Error> {
        self.conn()
            .query_row(
                "SELECT id, session_id, chat_id, media_type, mimetype, filename, bytes, path, created_at
                 FROM media WHERE id = ?1",
                params![id],
                |row| {
                    Ok(MediaRecord {
                        id: row.get("id")?,
                        session_id: row.get("session_id")?,
                        chat_id: row.get("chat_id")?,
                        media_type: row.get("media_type")?,
                        mimetype: row.get("mimetype")?,
                        filename: row.get("filename")?,
                        bytes: row.get("bytes")?,
                        path: row.get("path")?,
                        created_at: row.get("created_at")?,
                    })
                },
            )
            .optional()
    }

    pub async fn count_media(&self) -> Result<i64, rusqlite::Error> {
        self.conn()
            .query_row("SELECT COUNT(*) FROM media", [], |row| row.get(0))
    }

    // ---- webhooks -------------------------------------------------------

    pub async fn insert_webhook(
        &self,
        id: &str,
        session_id: &str,
        url: &str,
        events: &[String],
        secret: Option<&str>,
    ) -> Result<WebhookRecord, rusqlite::Error> {
        let now = now_ms();
        let events_json = json_string(events);
        self.conn().execute(
            "INSERT INTO webhooks (id, session_id, url, events, secret, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![id, session_id, url, events_json, secret, now],
        )?;

        Ok(WebhookRecord {
            id: id.to_string(),
            session_id: session_id.to_string(),
            url: url.to_string(),
            events: events.to_vec(),
            secret: secret.map(str::to_string),
            created_at: now,
        })
    }

    pub async fn list_webhooks(
        &self,
        session_id: &str,
    ) -> Result<Vec<WebhookRecord>, rusqlite::Error> {
        let guard = self.conn();
        let mut statement = guard.prepare(
            "SELECT id, session_id, url, events, secret, created_at
             FROM webhooks WHERE session_id = ?1 ORDER BY created_at",
        )?;
        let webhooks = statement
            .query_map(params![session_id], webhook_from_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(webhooks)
    }

    /// Every webhook subscription, used by the event fan-out.
    pub async fn all_webhooks(&self) -> Result<Vec<WebhookRecord>, rusqlite::Error> {
        let guard = self.conn();
        let mut statement = guard.prepare(
            "SELECT id, session_id, url, events, secret, created_at FROM webhooks ORDER BY created_at",
        )?;
        let webhooks = statement
            .query_map([], webhook_from_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(webhooks)
    }

    pub async fn get_webhook(&self, id: &str) -> Result<Option<WebhookRecord>, rusqlite::Error> {
        self.conn()
            .query_row(
                "SELECT id, session_id, url, events, secret, created_at FROM webhooks WHERE id = ?1",
                params![id],
                webhook_from_row,
            )
            .optional()
    }

    pub async fn delete_webhook(
        &self,
        session_id: &str,
        id: &str,
    ) -> Result<bool, rusqlite::Error> {
        let changed = self.conn().execute(
            "DELETE FROM webhooks WHERE id = ?1 AND session_id = ?2",
            params![id, session_id],
        )?;
        Ok(changed > 0)
    }

    pub async fn delete_session_webhooks(&self, session_id: &str) -> Result<(), rusqlite::Error> {
        self.conn().execute(
            "DELETE FROM webhooks WHERE session_id = ?1",
            params![session_id],
        )?;
        Ok(())
    }

    pub async fn count_webhooks(&self) -> Result<i64, rusqlite::Error> {
        self.conn()
            .query_row("SELECT COUNT(*) FROM webhooks", [], |row| row.get(0))
    }

    // ---- webhook deliveries --------------------------------------------

    pub async fn insert_delivery(&self, delivery: &NewDelivery) -> Result<i64, rusqlite::Error> {
        let now = now_ms();
        let guard = self.conn();
        guard.execute(
            "INSERT INTO webhook_deliveries
             (webhook_id, session_id, event_type, endpoint, payload, attempts, delivered, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 0, 0, ?6, ?7)",
            params![
                delivery.webhook_id,
                delivery.session_id,
                delivery.event_type,
                delivery.endpoint,
                delivery.payload,
                now,
                now
            ],
        )?;
        Ok(guard.last_insert_rowid())
    }

    pub async fn record_delivery_attempt(
        &self,
        id: i64,
        status_code: Option<i64>,
        error: Option<&str>,
        delivered: bool,
    ) -> Result<(), rusqlite::Error> {
        self.conn().execute(
            "UPDATE webhook_deliveries
             SET attempts = attempts + 1, status_code = ?1, error = ?2, delivered = ?3, updated_at = ?4
             WHERE id = ?5",
            params![status_code, error, i64::from(delivered), now_ms(), id],
        )?;
        Ok(())
    }

    pub async fn list_deliveries(
        &self,
        session_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<DeliveryRecord>, rusqlite::Error> {
        let limit = limit.clamp(1, 500);
        let guard = self.conn();
        let columns = "SELECT id, webhook_id, session_id, event_type, endpoint, status_code, error, attempts, delivered, created_at, updated_at
                       FROM webhook_deliveries";
        match session_id {
            Some(session) => {
                let mut statement = guard.prepare(&format!(
                    "{columns} WHERE session_id = ?1 ORDER BY id DESC LIMIT ?2"
                ))?;
                let rows = statement
                    .query_map(params![session, limit], delivery_from_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            }
            None => {
                let mut statement =
                    guard.prepare(&format!("{columns} ORDER BY id DESC LIMIT ?1"))?;
                let rows = statement
                    .query_map(params![limit], delivery_from_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            }
        }
    }

    pub async fn get_delivery(&self, id: i64) -> Result<Option<DeliveryRecord>, rusqlite::Error> {
        self.conn()
            .query_row(
                "SELECT id, webhook_id, session_id, event_type, endpoint, status_code, error, attempts, delivered, created_at, updated_at
                 FROM webhook_deliveries WHERE id = ?1",
                params![id],
                delivery_from_row,
            )
            .optional()
    }

    pub async fn delivery_payload(&self, id: i64) -> Result<Option<String>, rusqlite::Error> {
        self.conn()
            .query_row(
                "SELECT payload FROM webhook_deliveries WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()
    }

    pub async fn count_pending_deliveries(&self) -> Result<i64, rusqlite::Error> {
        self.conn().query_row(
            "SELECT COUNT(*) FROM webhook_deliveries WHERE delivered = 0",
            [],
            |row| row.get(0),
        )
    }

    /// Deliveries that still deserve a retry, oldest first.
    pub async fn retryable_deliveries(
        &self,
        max_attempts: i64,
    ) -> Result<Vec<i64>, rusqlite::Error> {
        let guard = self.conn();
        let mut statement = guard.prepare(
            "SELECT id FROM webhook_deliveries WHERE delivered = 0 AND attempts < ?1 ORDER BY id LIMIT 50",
        )?;
        let ids = statement
            .query_map(params![max_attempts], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ids)
    }

    // ---- contacts -------------------------------------------------------

    pub async fn upsert_contact(
        &self,
        session_id: &str,
        contact_id: &str,
        name: Option<&str>,
        phone: Option<&str>,
        is_business: bool,
    ) -> Result<(), rusqlite::Error> {
        let now = now_ms();
        self.conn().execute(
            "INSERT INTO contacts (session_id, contact_id, name, phone, is_business, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(session_id, contact_id) DO UPDATE SET
                name = COALESCE(excluded.name, contacts.name),
                phone = COALESCE(excluded.phone, contacts.phone),
                is_business = excluded.is_business,
                updated_at = excluded.updated_at",
            params![session_id, contact_id, name, phone, i64::from(is_business), now, now],
        )?;
        Ok(())
    }

    pub async fn list_contacts(
        &self,
        session_id: &str,
    ) -> Result<Vec<serde_json::Value>, rusqlite::Error> {
        let guard = self.conn();
        let mut statement = guard.prepare(
            "SELECT contact_id, name, phone, is_business, updated_at FROM contacts WHERE session_id = ?1 ORDER BY contact_id",
        )?;
        let rows = statement
            .query_map(params![session_id], |row| {
                Ok(serde_json::json!({
                    "contactId": row.get::<_, String>("contact_id")?,
                    "name": row.get::<_, Option<String>>("name")?,
                    "phone": row.get::<_, Option<String>>("phone")?,
                    "isBusiness": row.get::<_, i64>("is_business")? != 0,
                    "updatedAt": row.get::<_, i64>("updated_at")?,
                }))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    // ---- groups ---------------------------------------------------------

    pub async fn upsert_group(
        &self,
        session_id: &str,
        group_id: &str,
        subject: Option<&str>,
        owner: Option<&str>,
        size: i64,
    ) -> Result<(), rusqlite::Error> {
        let now = now_ms();
        self.conn().execute(
            "INSERT INTO groups (session_id, group_id, subject, owner, size, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(session_id, group_id) DO UPDATE SET
                subject = COALESCE(excluded.subject, groups.subject),
                owner = COALESCE(excluded.owner, groups.owner),
                size = excluded.size,
                updated_at = excluded.updated_at",
            params![session_id, group_id, subject, owner, size, now, now],
        )?;
        Ok(())
    }

    pub async fn list_groups(
        &self,
        session_id: &str,
    ) -> Result<Vec<serde_json::Value>, rusqlite::Error> {
        let guard = self.conn();
        let mut statement = guard.prepare(
            "SELECT group_id, subject, owner, size, updated_at FROM groups WHERE session_id = ?1 ORDER BY group_id",
        )?;
        let rows = statement
            .query_map(params![session_id], |row| {
                Ok(serde_json::json!({
                    "groupId": row.get::<_, String>("group_id")?,
                    "subject": row.get::<_, Option<String>>("subject")?,
                    "owner": row.get::<_, Option<String>>("owner")?,
                    "size": row.get::<_, i64>("size")?,
                    "updatedAt": row.get::<_, i64>("updated_at")?,
                }))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    // ---- labels ---------------------------------------------------------

    pub async fn insert_label(
        &self,
        id: &str,
        session_id: &str,
        name: &str,
        color: i64,
    ) -> Result<(), rusqlite::Error> {
        let now = now_ms();
        self.conn().execute(
            "INSERT OR REPLACE INTO labels (id, session_id, name, color, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![id, session_id, name, color, now],
        )?;
        Ok(())
    }

    pub async fn list_labels(
        &self,
        session_id: &str,
    ) -> Result<Vec<serde_json::Value>, rusqlite::Error> {
        let guard = self.conn();
        let mut statement = guard.prepare(
            "SELECT id, name, color, created_at FROM labels WHERE session_id = ?1 ORDER BY created_at",
        )?;
        let rows = statement
            .query_map(params![session_id], |row| {
                Ok(serde_json::json!({
                    "id": row.get::<_, String>("id")?,
                    "name": row.get::<_, String>("name")?,
                    "color": row.get::<_, i64>("color")?,
                    "createdAt": row.get::<_, i64>("created_at")?,
                }))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub async fn delete_label_db(
        &self,
        session_id: &str,
        id: &str,
    ) -> Result<bool, rusqlite::Error> {
        let changed = self.conn().execute(
            "DELETE FROM labels WHERE id = ?1 AND session_id = ?2",
            params![id, session_id],
        )?;
        let _ = self.conn().execute(
            "DELETE FROM label_chats WHERE label_id = ?1 AND session_id = ?2",
            params![id, session_id],
        );
        Ok(changed > 0)
    }

    pub async fn add_label_chat(
        &self,
        session_id: &str,
        label_id: &str,
        chat_id: &str,
    ) -> Result<(), rusqlite::Error> {
        self.conn().execute(
            "INSERT OR IGNORE INTO label_chats (label_id, chat_id, session_id) VALUES (?1, ?2, ?3)",
            params![label_id, chat_id, session_id],
        )?;
        Ok(())
    }

    pub async fn remove_label_chat(
        &self,
        session_id: &str,
        label_id: &str,
        chat_id: &str,
    ) -> Result<(), rusqlite::Error> {
        self.conn().execute(
            "DELETE FROM label_chats WHERE label_id = ?1 AND chat_id = ?2 AND session_id = ?3",
            params![label_id, chat_id, session_id],
        )?;
        Ok(())
    }

    pub async fn list_label_chats(
        &self,
        session_id: &str,
        label_id: &str,
    ) -> Result<Vec<String>, rusqlite::Error> {
        let guard = self.conn();
        let mut statement = guard
            .prepare("SELECT chat_id FROM label_chats WHERE session_id = ?1 AND label_id = ?2")?;
        let rows = statement
            .query_map(params![session_id, label_id], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;
        Ok(rows)
    }

    // ---- audit ----------------------------------------------------------

    pub async fn insert_audit(
        &self,
        action: &str,
        actor: Option<&str>,
        resource: Option<&str>,
        details: Option<&str>,
    ) -> Result<(), rusqlite::Error> {
        let now = now_ms();
        self.conn().execute(
            "INSERT INTO audit_log (action, actor, resource, details, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![action, actor, resource, details, now],
        )?;
        Ok(())
    }

    pub async fn list_audit(&self, limit: i64) -> Result<Vec<serde_json::Value>, rusqlite::Error> {
        let limit = limit.clamp(1, 500);
        let guard = self.conn();
        let mut statement = guard.prepare(
            "SELECT id, action, actor, resource, details, created_at FROM audit_log ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = statement
            .query_map(params![limit], |row| {
                Ok(serde_json::json!({
                    "id": row.get::<_, i64>("id")?,
                    "action": row.get::<_, String>("action")?,
                    "actor": row.get::<_, Option<String>>("actor")?,
                    "resource": row.get::<_, Option<String>>("resource")?,
                    "details": row.get::<_, Option<String>>("details")?,
                    "createdAt": row.get::<_, i64>("created_at")?,
                }))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    // ---- messages starred/pinned ----------------------------------------

    pub async fn set_message_starred(
        &self,
        session_id: &str,
        engine_message_id: &str,
        starred: bool,
    ) -> Result<(), rusqlite::Error> {
        self.conn().execute(
            "UPDATE messages SET starred = ?1 WHERE session_id = ?2 AND engine_message_id = ?3",
            params![i64::from(starred), session_id, engine_message_id],
        )?;
        Ok(())
    }

    // ---- FTS5 search ----------------------------------------------------

    pub async fn search_messages_fts(
        &self,
        session_id: &str,
        term: &str,
        limit: i64,
    ) -> Result<Vec<MessageRecord>, rusqlite::Error> {
        let limit = limit.clamp(1, 500);
        let term = term.trim();
        if term.is_empty() {
            return Ok(Vec::new());
        }
        let fts_term = format!("\"{}\"", term.replace('"', ""));
        let guard = self.conn();
        let sql = format!(
            "SELECT {MESSAGE_COLUMNS} FROM messages
             WHERE session_id = ?1 AND id IN (
                SELECT rowid FROM messages_fts WHERE messages_fts MATCH ?2
             )
             ORDER BY id DESC LIMIT ?3"
        );
        let mut statement = guard.prepare(&sql)?;
        let mut rows = statement
            .query_map(params![session_id, fts_term, limit], message_from_row)?
            .collect::<Result<Vec<_>, _>>()?;
        rows.reverse();
        Ok(rows)
    }

    pub async fn search_fts(
        &self,
        session_id: &str,
        term: &str,
        limit: i64,
    ) -> Result<Vec<MessageRecord>, rusqlite::Error> {
        self.search_messages_fts(session_id, term, limit).await
    }

    pub async fn fts_search(
        &self,
        session_id: &str,
        term: &str,
        limit: i64,
    ) -> Result<Vec<MessageRecord>, rusqlite::Error> {
        self.search_messages_fts(session_id, term, limit).await
    }

    // ---- session config --------------------------------------------------

    pub async fn upsert_session_config(
        &self,
        session_id: &str,
        auto_reject_calls: bool,
        max_reconnect_attempts: i64,
        reconnect_base_delay: i64,
    ) -> Result<(), rusqlite::Error> {
        self.conn().execute(
            "INSERT INTO session_config (session_id, auto_reject_calls, max_reconnect_attempts, reconnect_base_delay)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(session_id) DO UPDATE SET
                auto_reject_calls = excluded.auto_reject_calls,
                max_reconnect_attempts = excluded.max_reconnect_attempts,
                reconnect_base_delay = excluded.reconnect_base_delay",
            params![session_id, i64::from(auto_reject_calls), max_reconnect_attempts, reconnect_base_delay],
        )?;
        Ok(())
    }

    pub async fn get_session_config(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionConfig>, rusqlite::Error> {
        self.conn()
            .query_row(
                "SELECT session_id, auto_reject_calls, max_reconnect_attempts, reconnect_base_delay
                 FROM session_config WHERE session_id = ?1",
                params![session_id],
                |row| {
                    Ok(SessionConfig {
                        session_id: row.get("session_id")?,
                        auto_reject_calls: row.get::<_, i64>("auto_reject_calls")? != 0,
                        max_reconnect_attempts: row.get("max_reconnect_attempts")?,
                        reconnect_base_delay: row.get("reconnect_base_delay")?,
                    })
                },
            )
            .optional()
    }

    // Aliases
    pub async fn set_session_config(
        &self,
        session_id: &str,
        auto_reject_calls: bool,
        max_reconnect_attempts: i64,
        reconnect_base_delay: i64,
    ) -> Result<(), rusqlite::Error> {
        self.upsert_session_config(
            session_id,
            auto_reject_calls,
            max_reconnect_attempts,
            reconnect_base_delay,
        )
        .await
    }

    pub async fn save_session_config(
        &self,
        session_id: &str,
        auto_reject_calls: bool,
        max_reconnect_attempts: i64,
        reconnect_base_delay: i64,
    ) -> Result<(), rusqlite::Error> {
        self.upsert_session_config(
            session_id,
            auto_reject_calls,
            max_reconnect_attempts,
            reconnect_base_delay,
        )
        .await
    }

    pub async fn session_config(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionConfig>, rusqlite::Error> {
        self.get_session_config(session_id).await
    }

    pub async fn load_session_config(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionConfig>, rusqlite::Error> {
        self.get_session_config(session_id).await
    }

    // ---- send pacing -----------------------------------------------------

    pub async fn check_send_pacing(
        &self,
        session_id: &str,
        cap: i64,
    ) -> Result<bool, rusqlite::Error> {
        let today = crate::util::now_ms() / 86_400_000;
        let guard = self.conn();
        let row: Option<(i64, i64)> = guard
            .query_row(
                "SELECT daily_count, last_reset FROM send_pacing WHERE session_id = ?1",
                params![session_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        match row {
            None => Ok(true),
            Some((count, last_reset)) => {
                if last_reset != today {
                    Ok(true)
                } else {
                    Ok(count < cap)
                }
            }
        }
    }

    pub async fn increment_send_pacing(&self, session_id: &str) -> Result<i64, rusqlite::Error> {
        let today = crate::util::now_ms() / 86_400_000;
        self.conn().execute(
            "INSERT INTO send_pacing (session_id, daily_count, last_reset)
             VALUES (?1, 1, ?2)
             ON CONFLICT(session_id) DO UPDATE SET
                daily_count = CASE WHEN last_reset = ?2 THEN daily_count + 1 ELSE 1 END,
                last_reset = ?2",
            params![session_id, today],
        )?;
        let count = self
            .conn()
            .query_row(
                "SELECT daily_count FROM send_pacing WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .unwrap_or(1);
        Ok(count)
    }

    pub async fn get_send_pacing(
        &self,
        session_id: &str,
    ) -> Result<Option<SendPacing>, rusqlite::Error> {
        self.conn()
            .query_row(
                "SELECT session_id, daily_count, last_reset FROM send_pacing WHERE session_id = ?1",
                params![session_id],
                |row| {
                    Ok(SendPacing {
                        session_id: row.get("session_id")?,
                        daily_count: row.get("daily_count")?,
                        last_reset: row.get("last_reset")?,
                    })
                },
            )
            .optional()
    }

    // Aliases
    pub async fn check_pacing(&self, session_id: &str, cap: i64) -> Result<bool, rusqlite::Error> {
        self.check_send_pacing(session_id, cap).await
    }

    pub async fn increment_pacing(&self, session_id: &str) -> Result<i64, rusqlite::Error> {
        self.increment_send_pacing(session_id).await
    }

    pub async fn pacing_check(&self, session_id: &str, cap: i64) -> Result<bool, rusqlite::Error> {
        self.check_send_pacing(session_id, cap).await
    }

    pub async fn pacing_increment(&self, session_id: &str) -> Result<i64, rusqlite::Error> {
        self.increment_send_pacing(session_id).await
    }

    pub async fn send_pacing_check(
        &self,
        session_id: &str,
        cap: i64,
    ) -> Result<bool, rusqlite::Error> {
        self.check_send_pacing(session_id, cap).await
    }

    pub async fn send_pacing_increment(&self, session_id: &str) -> Result<i64, rusqlite::Error> {
        self.increment_send_pacing(session_id).await
    }

    pub async fn reset_send_pacing(&self, session_id: &str) -> Result<(), rusqlite::Error> {
        self.conn().execute(
            "DELETE FROM send_pacing WHERE session_id = ?1",
            params![session_id],
        )?;
        Ok(())
    }
}
