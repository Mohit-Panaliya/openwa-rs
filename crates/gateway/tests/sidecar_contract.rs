//! Integration test: drives the deterministic mock engine over the real
//! stdio transport, asserting the contract in docs/SIDECAR_PROTOCOL.md.
//!
//! Requires Node.js on PATH. The mock has no npm dependencies.

use std::path::PathBuf;
use std::time::Duration;

use serde_json::json;

use openwa_gateway::sidecar::{Sidecar, SidecarError};

fn mock_sidecar_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/mock-sidecar")
}

#[tokio::test]
async fn mock_sidecar_speaks_the_protocol() {
    let dir = mock_sidecar_dir();
    assert!(
        dir.join("index.js").exists(),
        "mock sidecar missing at {}",
        dir.display()
    );

    let sidecar = Sidecar::spawn("node", &dir, 10_000)
        .await
        .expect("spawn mock sidecar");

    // ping
    let pong = sidecar.ping().await.expect("ping");
    assert_eq!(pong["pong"], json!(true));

    // session.create
    let created = sidecar
        .request("session.create", json!({ "name": "integration" }))
        .await
        .expect("session.create");
    let session_id = created["sessionId"]
        .as_str()
        .expect("sessionId")
        .to_string();
    assert_eq!(created["status"], json!("created"));
    assert_eq!(created["name"], json!("integration"));

    // session.list contains it
    let list = sidecar
        .request("session.list", json!({}))
        .await
        .expect("session.list");
    assert!(list
        .as_array()
        .expect("array")
        .iter()
        .any(|session| session["sessionId"] == json!(session_id)));

    // session.get
    let fetched = sidecar
        .session("session.get", &session_id)
        .await
        .expect("session.get");
    assert_eq!(fetched["sessionId"], json!(session_id));

    // qr is null before start
    let qr = sidecar
        .session("session.qr", &session_id)
        .await
        .expect("session.qr");
    assert!(qr["qr"].is_null());

    // unknown session surfaces a remote error the gateway maps to 404
    match sidecar.session("session.get", "nope").await {
        Err(SidecarError::Remote(message)) => {
            assert!(message.contains("not found"), "unexpected error: {message}");
        }
        other => panic!("expected a remote error, got {other:?}"),
    }

    // session.start -> qr_ready, then qr payload, then a connected event
    let mut events = sidecar.subscribe();
    let started = sidecar
        .session("session.start", &session_id)
        .await
        .expect("session.start");
    assert_eq!(started["status"], json!("qr_ready"));

    let qr = sidecar
        .session("session.qr", &session_id)
        .await
        .expect("session.qr");
    assert_eq!(qr["qr"], json!("mock-qr-data"));

    let mut saw_qr_event = false;
    let mut saw_connected = false;
    for _ in 0..12 {
        match tokio::time::timeout(Duration::from_secs(2), events.recv()).await {
            Ok(Ok(event)) => match event["type"].as_str() {
                Some("session.qr") => saw_qr_event = true,
                Some("session.status") if event["payload"]["status"] == json!("connected") => {
                    saw_connected = true;
                    break;
                }
                _ => {}
            },
            _ => break,
        }
    }
    assert!(saw_qr_event, "expected a session.qr event");
    assert!(saw_connected, "expected a connected session.status event");

    // message.sendText
    let sent = sidecar
        .request(
            "message.sendText",
            json!({ "sessionId": session_id, "chatId": "123@c.us", "text": "hi" }),
        )
        .await
        .expect("sendText");
    assert!(sent["messageId"]
        .as_str()
        .expect("messageId")
        .starts_with("mock-msg"));

    // message.sendMedia validation comes from the engine
    match sidecar
        .request(
            "message.sendMedia",
            json!({ "sessionId": session_id, "chatId": "123@c.us", "mediaType": "image" }),
        )
        .await
    {
        Err(SidecarError::Remote(message)) => {
            assert!(
                message.contains("base64 or url"),
                "unexpected error: {message}"
            );
        }
        other => panic!("expected a validation error, got {other:?}"),
    }

    // session.delete
    let deleted = sidecar
        .session("session.delete", &session_id)
        .await
        .expect("session.delete");
    assert_eq!(deleted["deleted"], json!(true));
}
