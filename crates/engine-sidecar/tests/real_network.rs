//! Real-network check: `session.start` reaches `qr_ready` and emits a non-empty
//! `session.qr` within 60 seconds, then `session.stop` and `session.delete`
//! succeed.
//!
//! This reproduces the flow of `scripts/verify-real-sidecar.ps1` (which drives
//! the Node sidecar) against the Rust binary. It needs outbound network access,
//! so it is `#[ignore]`d. Run it with:
//!
//! ```text
//! cargo test -p openwa-engine-sidecar --test real_network -- --ignored --nocapture
//! ```

mod common;

use std::time::{Duration, Instant};

use common::{event_payload, event_type, temp_data_dir, Harness};
use serde_json::{json, Value};

const QR_TIMEOUT: Duration = Duration::from_secs(60);

#[tokio::test]
#[ignore = "requires outbound network access to WhatsApp"]
async fn start_reaches_qr_ready() {
    let data_dir = temp_data_dir("real-network");
    let mut harness = Harness::start(&data_dir);

    let ping = harness.call(1, "ping", json!({})).await;
    assert_eq!(
        ping.get("result").and_then(|result| result.get("pong")),
        Some(&json!(true)),
        "ping failed: {ping}"
    );

    let created = harness
        .call(2, "session.create", json!({ "name": "real-network" }))
        .await;
    let session_id = created
        .get("result")
        .and_then(|result| result.get("sessionId"))
        .and_then(Value::as_str)
        .expect("session.create returned no sessionId")
        .to_string();
    println!("session id: {session_id}");

    let started_at = Instant::now();
    let started = harness
        .call(3, "session.start", json!({ "sessionId": session_id }))
        .await;
    assert_eq!(
        started
            .get("result")
            .and_then(|result| result.get("status"))
            .and_then(Value::as_str),
        Some("connecting"),
        "session.start failed: {started}"
    );

    let session_id_for_event = session_id.clone();
    let qr_event = harness
        .wait_for_event(
            move |event| {
                event_type(event) == "session.qr"
                    && event_payload(event)
                        .get("sessionId")
                        .and_then(Value::as_str)
                        == Some(session_id_for_event.as_str())
            },
            QR_TIMEOUT,
        )
        .await
        .expect("timed out waiting for a session.qr event");

    let qr = event_payload(&qr_event)
        .get("qr")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    println!(
        "got session.qr after {:.1}s ({} bytes)",
        started_at.elapsed().as_secs_f64(),
        qr.len()
    );
    assert!(!qr.is_empty(), "session.qr payload was empty");

    let session_id_for_status = session_id.clone();
    let qr_status = harness
        .wait_for_event(
            move |event| {
                event_type(event) == "session.status"
                    && event_payload(event)
                        .get("sessionId")
                        .and_then(Value::as_str)
                        == Some(session_id_for_status.as_str())
                    && event_payload(event).get("status").and_then(Value::as_str)
                        == Some("qr_ready")
            },
            Duration::from_secs(5),
        )
        .await;
    assert!(
        qr_status.is_some(),
        "no session.status qr_ready event was emitted"
    );

    let fetched = harness
        .call(4, "session.qr", json!({ "sessionId": session_id }))
        .await;
    assert_eq!(
        fetched
            .get("result")
            .and_then(|result| result.get("qr"))
            .and_then(Value::as_str),
        Some(qr.as_str()),
        "session.qr did not return the advertised code: {fetched}"
    );

    let stopped = harness
        .call(5, "session.stop", json!({ "sessionId": session_id }))
        .await;
    assert_eq!(
        stopped
            .get("result")
            .and_then(|result| result.get("status"))
            .and_then(Value::as_str),
        Some("disconnected"),
        "session.stop failed: {stopped}"
    );

    let deleted = harness
        .call(6, "session.delete", json!({ "sessionId": session_id }))
        .await;
    assert_eq!(
        deleted
            .get("result")
            .and_then(|result| result.get("deleted")),
        Some(&json!(true)),
        "session.delete failed: {deleted}"
    );

    harness.stop().await;
    let _ = std::fs::remove_dir_all(&data_dir);
}
