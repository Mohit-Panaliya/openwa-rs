//! Offline protocol selftest — a Rust port of `sidecar/dev-selftest.js`.
//!
//! Exercises the network-free part of `docs/SIDECAR_PROTOCOL.md` against the
//! real binary. `session.start` is only called with an unknown id, so no
//! WhatsApp connection is ever made.

mod common;

use std::time::Duration;

use common::{event_payload, event_type, temp_data_dir, Checks, Harness};
use serde_json::{json, Value};

#[tokio::test]
async fn offline_protocol_selftest() {
    let data_dir = temp_data_dir("selftest");
    let mut harness = Harness::start(&data_dir);
    let mut checks = Checks::default();
    let mut id = 0i64;
    let mut next = || {
        id += 1;
        id
    };

    // ping
    let res = harness.call(next(), "ping", json!({})).await;
    checks.check(
        "ping returns pong:true",
        res.get("result").and_then(|r| r.get("pong")) == Some(&json!(true)),
        res.to_string(),
    );
    checks.check(
        "ping returns version 0.1.0",
        res.get("result")
            .and_then(|r| r.get("version"))
            .and_then(Value::as_str)
            == Some("0.1.0"),
        res.get("result")
            .cloned()
            .unwrap_or(Value::Null)
            .to_string(),
    );

    // session.create + session.status event
    let res = harness
        .call(next(), "session.create", json!({ "name": "selftest" }))
        .await;
    let created = res.get("result").cloned().unwrap_or(Value::Null);
    let session_id = created
        .get("sessionId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    checks.check(
        "create result.status === \"created\"",
        created.get("status").and_then(Value::as_str) == Some("created"),
        created.to_string(),
    );
    checks.check(
        "create result.sessionId is a non-empty string",
        session_id.len() >= 8,
        created.to_string(),
    );
    checks.check(
        "create result.name === \"selftest\"",
        created.get("name").and_then(Value::as_str) == Some("selftest"),
        created.to_string(),
    );

    let session_id_for_event = session_id.clone();
    let event = harness
        .wait_for_event(
            move |event| {
                event_type(event) == "session.status"
                    && event_payload(event)
                        .get("sessionId")
                        .and_then(Value::as_str)
                        == Some(session_id_for_event.as_str())
            },
            Duration::from_secs(5),
        )
        .await;
    checks.check(
        "create emits session.status with status \"created\"",
        event
            .as_ref()
            .map(event_payload)
            .and_then(|payload| payload.get("status").cloned())
            == Some(json!("created")),
        event.map(|e| e.to_string()).unwrap_or_default(),
    );
    let qr_event = harness
        .events()
        .into_iter()
        .find(|event| event_type(event) == "session.qr");
    checks.check(
        "create emits no session.qr event",
        qr_event.is_none(),
        qr_event.map(|e| e.to_string()).unwrap_or_default(),
    );

    // session.create without name
    let res = harness.call(next(), "session.create", json!({})).await;
    let anonymous = res.get("result").cloned().unwrap_or(Value::Null);
    checks.check(
        "anonymous create returns a string name",
        anonymous
            .get("name")
            .and_then(Value::as_str)
            .is_some_and(|name| !name.is_empty()),
        anonymous.to_string(),
    );
    let anonymous_id = anonymous
        .get("sessionId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    harness
        .call(
            next(),
            "session.delete",
            json!({ "sessionId": anonymous_id }),
        )
        .await;

    // session.list
    let res = harness.call(next(), "session.list", json!({})).await;
    let list = res.get("result").and_then(Value::as_array).cloned();
    checks.check("list returns an array", list.is_some(), res.to_string());
    let entry = list
        .as_ref()
        .and_then(|list| {
            list.iter().find(|session| {
                session.get("sessionId").and_then(Value::as_str) == Some(session_id.as_str())
            })
        })
        .cloned();
    checks.check(
        "list contains the created session",
        entry.is_some(),
        res.get("result")
            .cloned()
            .unwrap_or(Value::Null)
            .to_string(),
    );
    checks.check(
        "list entry is {sessionId, name, status}",
        entry.as_ref().is_some_and(|entry| {
            entry.get("name").and_then(Value::as_str) == Some("selftest")
                && entry.get("status").and_then(Value::as_str) == Some("created")
        }),
        entry.map(|e| e.to_string()).unwrap_or_default(),
    );

    // session.get
    let res = harness
        .call(next(), "session.get", json!({ "sessionId": session_id }))
        .await;
    checks.check(
        "get returns the created session",
        res.get("result").is_some_and(|result| {
            result.get("sessionId").and_then(Value::as_str) == Some(session_id.as_str())
                && result.get("name").and_then(Value::as_str) == Some("selftest")
                && result.get("status").and_then(Value::as_str) == Some("created")
        }),
        res.to_string(),
    );

    // unknown / missing session id
    let res = harness
        .call(
            next(),
            "session.get",
            json!({ "sessionId": "does-not-exist" }),
        )
        .await;
    checks.check(
        "unknown session id errors",
        error_message(&res).as_deref() == Some("session not found"),
        res.to_string(),
    );
    let res = harness.call(next(), "session.get", json!({})).await;
    checks.check(
        "missing sessionId errors",
        error_message(&res).as_deref() == Some("session not found"),
        res.to_string(),
    );

    // session.qr for a created session
    let res = harness
        .call(next(), "session.qr", json!({ "sessionId": session_id }))
        .await;
    checks.check(
        "qr result has null qr",
        res.get("result")
            .and_then(|result| result.get("qr"))
            .is_some_and(Value::is_null),
        res.to_string(),
    );

    // session.start on an unknown id
    let res = harness
        .call(next(), "session.start", json!({ "sessionId": "nope" }))
        .await;
    checks.check(
        "start on unknown session errors",
        error_message(&res).as_deref() == Some("session not found"),
        res.to_string(),
    );

    // sends on an unconnected session
    let res = harness
        .call(
            next(),
            "message.sendText",
            json!({ "sessionId": session_id, "chatId": "123@s.whatsapp.net", "text": "hi" }),
        )
        .await;
    checks.check(
        "sendText on non-connected session errors",
        error_message(&res).as_deref() == Some("session is not connected"),
        res.to_string(),
    );
    let res = harness
        .call(
            next(),
            "message.sendMedia",
            json!({
                "sessionId": session_id,
                "chatId": "123@s.whatsapp.net",
                "mediaType": "image",
                "mimetype": "image/png",
                "base64": "aGk=",
            }),
        )
        .await;
    checks.check(
        "sendMedia on non-connected session errors",
        error_message(&res).as_deref() == Some("session is not connected"),
        res.to_string(),
    );

    // session.stop
    let res = harness
        .call(next(), "session.stop", json!({ "sessionId": session_id }))
        .await;
    checks.check(
        "stop returns disconnected",
        res.get("result").is_some_and(|result| {
            result.get("sessionId").and_then(Value::as_str) == Some(session_id.as_str())
                && result.get("status").and_then(Value::as_str) == Some("disconnected")
        }),
        res.to_string(),
    );

    // unknown method
    let res = harness.call(next(), "nope", json!({})).await;
    checks.check(
        "unknown method errors",
        error_message(&res).as_deref() == Some("unknown method: nope"),
        res.to_string(),
    );

    // session.delete
    let res = harness
        .call(next(), "session.delete", json!({ "sessionId": session_id }))
        .await;
    checks.check(
        "delete returns deleted:true",
        res.get("result").and_then(|result| result.get("deleted")) == Some(&json!(true)),
        res.to_string(),
    );
    let res = harness.call(next(), "session.list", json!({})).await;
    checks.check(
        "deleted session is gone from session.list",
        res.get("result")
            .and_then(Value::as_array)
            .is_some_and(|list| {
                !list.iter().any(|session| {
                    session.get("sessionId").and_then(Value::as_str) == Some(session_id.as_str())
                })
            }),
        res.get("result")
            .cloned()
            .unwrap_or(Value::Null)
            .to_string(),
    );

    // stdout discipline
    let lines = harness.stdout_lines();
    let bad = lines
        .iter()
        .find(|line| match serde_json::from_str::<Value>(line) {
            Ok(parsed) => {
                parsed.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
                    || (parsed.get("id").is_none()
                        && parsed.get("method").and_then(Value::as_str) != Some("event"))
            }
            Err(_) => true,
        });
    checks.check(
        "every stdout line is a JSON-RPC 2.0 response or event",
        bad.is_none(),
        bad.cloned().unwrap_or_default(),
    );

    harness.stop().await;
    let _ = std::fs::remove_dir_all(&data_dir);

    checks.finish();
}

fn error_message(response: &Value) -> Option<String> {
    response
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .map(str::to_string)
}
