//! Persistence integration tests: real SQLite file, no network, no HTTP.
//!
//! Covers the behaviours the smoke test can only observe indirectly: message
//! deduplication, chat/limit/search filtering, key rotation, and the webhook
//! delivery log that the retry worker depends on.

use std::path::PathBuf;

use openwa_gateway::db::{Db, NewDelivery, NewMessage};
use openwa_gateway::util::{random_hex, sha256_hex};

/// Fresh database in the temp directory; the file is removed on drop.
struct TempDb {
    db: Db,
    path: PathBuf,
}

impl TempDb {
    async fn new() -> Self {
        let path = std::env::temp_dir().join(format!("openwa-test-{}.db", random_hex(8)));
        let db = Db::connect(&path).await.expect("database opens");
        Self { db, path }
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.path.display()));
        }
    }
}

fn message<'a>(
    session: &'a str,
    chat: &'a str,
    engine_id: Option<&'a str>,
    text: &'a str,
) -> NewMessage<'a> {
    NewMessage {
        session_id: session,
        chat_id: chat,
        engine_message_id: engine_id,
        sender: Some("me"),
        from_me: true,
        kind: "text",
        text: Some(text),
        has_media: false,
        media_id: None,
        timestamp: 1_700_000_000,
    }
}

#[tokio::test]
async fn messages_deduplicate_on_engine_id() {
    let temp = TempDb::new().await;
    let db = &temp.db;

    let first = db
        .save_message(&message("s1", "a@c.us", Some("m1"), "hello"))
        .await
        .expect("insert");
    // The event stream and the send handler both persist the same message.
    let second = db
        .save_message(&message("s1", "a@c.us", Some("m1"), "hello"))
        .await
        .expect("insert again");

    assert_eq!(first, second, "the same engine message must be stored once");
    assert_eq!(db.count_messages().await.unwrap(), 1);

    // A different engine id in the same chat is a new message.
    db.save_message(&message("s1", "a@c.us", Some("m2"), "again"))
        .await
        .unwrap();
    assert_eq!(db.count_messages().await.unwrap(), 2);

    // Messages without an engine id (events that carry none) still store.
    db.save_message(&message("s1", "a@c.us", None, "loose"))
        .await
        .unwrap();
    assert_eq!(db.count_messages().await.unwrap(), 3);

    let stored = db.get_message(first).await.unwrap().expect("row exists");
    assert!(stored.from_me);
    assert_eq!(stored.text.as_deref(), Some("hello"));
    assert_eq!(stored.chat_id, "a@c.us");
}

#[tokio::test]
async fn messages_filter_by_chat_and_search() {
    let temp = TempDb::new().await;
    let db = &temp.db;

    db.save_message(&message("s1", "a@c.us", Some("m1"), "alpha"))
        .await
        .unwrap();
    db.save_message(&message("s1", "b@c.us", Some("m2"), "beta"))
        .await
        .unwrap();
    db.save_message(&message("s2", "a@c.us", Some("m3"), "gamma"))
        .await
        .unwrap();

    let a_chat = db
        .list_messages("s1", Some("a@c.us"), 50, None, None)
        .await
        .unwrap();
    assert_eq!(a_chat.len(), 1);
    assert_eq!(a_chat[0].text.as_deref(), Some("alpha"));

    let all_s1 = db.list_messages("s1", None, 50, None, None).await.unwrap();
    assert_eq!(all_s1.len(), 2);
    assert_eq!(
        all_s1
            .iter()
            .map(|m| m.text.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec!["alpha", "beta"],
        "results come back oldest first"
    );

    let searched = db
        .list_messages("s1", None, 50, None, Some("bet"))
        .await
        .unwrap();
    assert_eq!(searched.len(), 1);
    assert_eq!(searched[0].text.as_deref(), Some("beta"));

    // `before` pages backwards from an id.
    let newest = db.list_messages("s1", None, 50, None, None).await.unwrap();
    let before_id = newest[1].id;
    let page = db
        .list_messages("s1", None, 50, Some(before_id), None)
        .await
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].text.as_deref(), Some("alpha"));

    // `limit` is clamped to a sane range rather than erroring.
    assert_eq!(
        db.list_messages("s1", None, -5, None, None)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn api_keys_round_trip_and_rotate() {
    let temp = TempDb::new().await;
    let db = &temp.db;

    let key = random_hex(24);
    let record = db
        .insert_key(
            "k1",
            "bootstrap",
            "admin",
            &sha256_hex(&key),
            "abcd1234",
            &[],
            &[],
            &[],
        )
        .await
        .unwrap();
    assert_eq!(record.role, "admin");
    assert!(record.last_used_at.is_none());

    let found = db
        .find_key_by_hash(&sha256_hex(&key))
        .await
        .unwrap()
        .expect("key resolves");
    assert_eq!(found.id, "k1");
    assert!(
        db.find_key_by_hash(&sha256_hex("wrong-key"))
            .await
            .unwrap()
            .is_none(),
        "an unknown key must not resolve"
    );

    db.touch_key("k1").await.unwrap();
    assert!(db.list_keys().await.unwrap()[0].last_used_at.is_some());
    assert_eq!(db.count_keys_by_role("admin").await.unwrap(), 1);
    assert_eq!(db.count_keys_by_role("viewer").await.unwrap(), 0);

    // Rotation replaces the hash, so the old secret stops resolving.
    let rotated = random_hex(24);
    assert!(db
        .rotate_key("k1", &sha256_hex(&rotated), "beefcafe")
        .await
        .unwrap());
    assert!(db
        .find_key_by_hash(&sha256_hex(&key))
        .await
        .unwrap()
        .is_none());
    assert!(db
        .find_key_by_hash(&sha256_hex(&rotated))
        .await
        .unwrap()
        .is_some());
    assert!(!db.rotate_key("missing", "x", "y").await.unwrap());

    // Scopes survive a round trip through the JSON columns.
    db.insert_key(
        "k2",
        "scoped",
        "operator",
        &sha256_hex("secret2"),
        "ffff0000",
        &["s1".to_string(), "s2".to_string()],
        &["a@c.us".to_string()],
        &[],
    )
    .await
    .unwrap();
    let scoped = db
        .find_key_by_hash(&sha256_hex("secret2"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        scoped.allowed_sessions,
        vec!["s1".to_string(), "s2".to_string()]
    );
    assert_eq!(scoped.allowed_chats, vec!["a@c.us".to_string()]);

    assert!(db.delete_key("k2").await.unwrap());
    assert!(!db.delete_key("k2").await.unwrap());
}

#[tokio::test]
async fn sessions_upsert_keeps_its_name() {
    let temp = TempDb::new().await;
    let db = &temp.db;

    db.upsert_session("s1", "bot", "created", None)
        .await
        .unwrap();
    // Status events arrive without a name; the stored name must survive.
    db.upsert_session("s1", "", "connected", None)
        .await
        .unwrap();

    let sessions = db.list_sessions().await.unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].name, "bot");
    assert_eq!(sessions[0].status, "connected");

    db.delete_session("s1").await.unwrap();
    assert_eq!(db.count_sessions().await.unwrap(), 0);
}

#[tokio::test]
async fn webhook_delivery_log_drives_retries() {
    let temp = TempDb::new().await;
    let db = &temp.db;

    let webhook = db
        .insert_webhook(
            "w1",
            "s1",
            "https://example.test/hook",
            &["message.received".to_string()],
            Some("s3cret"),
        )
        .await
        .unwrap();
    assert_eq!(webhook.events, vec!["message.received".to_string()]);
    // The secret is readable server-side (for signing) but never serialised out.
    assert_eq!(
        db.get_webhook("w1")
            .await
            .unwrap()
            .unwrap()
            .secret
            .as_deref(),
        Some("s3cret")
    );
    let wire = serde_json::to_string(&db.list_webhooks("s1").await.unwrap()).unwrap();
    assert!(!wire.contains("s3cret"), "secret leaked to clients: {wire}");

    let delivery = NewDelivery {
        webhook_id: "w1".to_string(),
        session_id: "s1".to_string(),
        event_type: "message.received".to_string(),
        endpoint: "https://example.test/hook".to_string(),
        payload: "{\"type\":\"message.received\"}".to_string(),
    };
    let id = db.insert_delivery(&delivery).await.unwrap();
    assert_eq!(db.count_pending_deliveries().await.unwrap(), 1);
    assert_eq!(db.retryable_deliveries(4).await.unwrap(), vec![id]);
    assert_eq!(
        db.delivery_payload(id).await.unwrap().as_deref(),
        Some("{\"type\":\"message.received\"}")
    );

    // A failed attempt stays retryable and records the reason.
    db.record_delivery_attempt(id, None, Some("connection refused"), false)
        .await
        .unwrap();
    let failed = db.get_delivery(id).await.unwrap().unwrap();
    assert_eq!(failed.attempts, 1);
    assert!(!failed.delivered);
    assert_eq!(failed.error.as_deref(), Some("connection refused"));
    assert_eq!(db.retryable_deliveries(4).await.unwrap(), vec![id]);

    // Retries stop once the attempts are exhausted.
    assert!(db.retryable_deliveries(1).await.unwrap().is_empty());

    // A successful attempt clears it from the pending set.
    db.record_delivery_attempt(id, Some(200), None, true)
        .await
        .unwrap();
    let delivered = db.get_delivery(id).await.unwrap().unwrap();
    assert_eq!(delivered.status_code, Some(200));
    assert!(delivered.delivered);
    assert_eq!(db.count_pending_deliveries().await.unwrap(), 0);

    assert_eq!(db.list_deliveries(Some("s1"), 50).await.unwrap().len(), 1);
    assert!(db
        .list_deliveries(Some("other"), 50)
        .await
        .unwrap()
        .is_empty());

    assert!(db.delete_webhook("s1", "w1").await.unwrap());
    assert!(!db.delete_webhook("s1", "w1").await.unwrap());
}
