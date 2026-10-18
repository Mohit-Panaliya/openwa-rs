//! Webhook delivery: HMAC-signed POSTs, a durable delivery log, and retries.
//!
//! Subscriptions live in SQLite (`docs/PHASE1_5.md`). Every dispatch records a
//! `webhook_deliveries` row, tries to deliver immediately, and retries failures
//! with backoff (1s, 5s, 25s). A background worker finishes retries that were
//! interrupted by a restart.

use std::time::Duration;

use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;
use tracing::{debug, warn};

use crate::db::{Db, DeliveryRecord, NewDelivery, WebhookRecord};
use crate::state::AppState;

type HmacSha256 = Hmac<Sha256>;

/// Initial attempt plus three retries.
pub const MAX_ATTEMPTS: i64 = 4;
const BACKOFF_MS: [u64; 3] = [1_000, 5_000, 25_000];

/// Does a subscription want this event type? Empty `events` means "everything".
pub fn matches(events: &[String], event_type: &str) -> bool {
    events.is_empty() || events.iter().any(|event| event == event_type)
}

/// Record the delivery and run the attempt chain in this task.
pub async fn record_and_deliver(
    db: Db,
    client: reqwest::Client,
    webhook: WebhookRecord,
    event_type: String,
    envelope: Value,
) {
    let body = envelope.to_string();
    let delivery = NewDelivery {
        webhook_id: webhook.id.clone(),
        session_id: webhook.session_id.clone(),
        event_type: event_type.clone(),
        endpoint: webhook.url.clone(),
        payload: body.clone(),
    };

    let id = match db.insert_delivery(&delivery).await {
        Ok(id) => id,
        Err(error) => {
            warn!(%error, webhook = %webhook.id, "could not record webhook delivery");
            return;
        }
    };

    attempt_chain(
        db,
        client,
        id,
        webhook.url,
        webhook.secret,
        event_type,
        body,
        0,
    )
    .await;
}

/// Re-send one recorded delivery once, regardless of earlier outcome.
pub async fn retry_once(
    db: Db,
    client: reqwest::Client,
    delivery: DeliveryRecord,
    webhook: WebhookRecord,
    payload: String,
) {
    // `MAX_ATTEMPTS - 1` means this attempt is the last one the chain allows, so
    // an explicit retry is a single shot.
    attempt_chain(
        db,
        client,
        delivery.id,
        webhook.url.clone(),
        webhook.secret.clone(),
        delivery.event_type.clone(),
        payload,
        MAX_ATTEMPTS - 1,
    )
    .await;
}

/// Retry loop: every 20s pick up failures that still have attempts left.
pub async fn retry_worker(state: AppState) {
    let mut ticker = tokio::time::interval(Duration::from_secs(20));
    ticker.tick().await; // the first tick fires immediately; skip it
    loop {
        ticker.tick().await;
        let ids = match state.db.retryable_deliveries(MAX_ATTEMPTS).await {
            Ok(ids) => ids,
            Err(error) => {
                warn!(%error, "could not list retryable webhook deliveries");
                continue;
            }
        };

        for id in ids {
            let Some(delivery) = state.db.get_delivery(id).await.ok().flatten() else {
                continue;
            };
            let Some(payload) = state.db.delivery_payload(id).await.ok().flatten() else {
                continue;
            };
            let Some(webhook) = state
                .db
                .get_webhook(&delivery.webhook_id)
                .await
                .ok()
                .flatten()
            else {
                continue;
            };

            tokio::spawn(attempt_chain(
                state.db.clone(),
                state.http.clone(),
                id,
                webhook.url,
                webhook.secret,
                delivery.event_type,
                payload,
                delivery.attempts,
            ));
        }
    }
}

/// Try to deliver `body`, recording each attempt, until success or exhaustion.
#[allow(clippy::too_many_arguments)]
async fn attempt_chain(
    db: Db,
    client: reqwest::Client,
    delivery_id: i64,
    url: String,
    secret: Option<String>,
    event_type: String,
    body: String,
    start_attempt: i64,
) {
    let mut attempt = start_attempt;
    loop {
        let outcome = send(&client, &url, secret.as_deref(), &event_type, &body).await;
        attempt += 1;

        match outcome {
            Ok(status) if status.is_success() => {
                let _ = db
                    .record_delivery_attempt(delivery_id, Some(status.as_u16() as i64), None, true)
                    .await;
                debug!(delivery = delivery_id, %url, "webhook delivered");
                return;
            }
            Ok(status) => {
                let _ = db
                    .record_delivery_attempt(
                        delivery_id,
                        Some(status.as_u16() as i64),
                        Some("non-2xx response"),
                        false,
                    )
                    .await;
                warn!(delivery = delivery_id, %url, %status, "webhook rejected");
            }
            Err(error) => {
                let _ = db
                    .record_delivery_attempt(delivery_id, None, Some(&error), false)
                    .await;
                warn!(delivery = delivery_id, %url, %error, "webhook delivery failed");
            }
        }

        if attempt >= MAX_ATTEMPTS {
            warn!(delivery = delivery_id, %url, "giving up on webhook delivery");
            return;
        }

        let index = (attempt - 1).clamp(0, BACKOFF_MS.len() as i64 - 1) as usize;
        tokio::time::sleep(Duration::from_millis(BACKOFF_MS[index])).await;
    }
}

/// One signed POST. Returns the status code or a transport error string.
async fn send(
    client: &reqwest::Client,
    url: &str,
    secret: Option<&str>,
    event_type: &str,
    body: &str,
) -> Result<reqwest::StatusCode, String> {
    let mut request = client
        .post(url)
        .timeout(Duration::from_secs(5))
        .header("content-type", "application/json")
        .header("x-openwa-event", event_type)
        .body(body.to_string());

    if let Some(secret) = secret {
        request = request.header(
            "x-openwa-signature",
            format!("sha256={}", hmac_hex(secret, body)),
        );
    }

    match request.send().await {
        Ok(response) => Ok(response.status()),
        Err(error) => Err(error.to_string()),
    }
}

pub fn hmac_hex(secret: &str, body: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("hmac accepts any key length");
    mac.update(body.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Build the payload sent to webhooks and the SSE stream.
pub fn event_envelope(event_type: &str, payload: &Value) -> Value {
    json!({ "type": event_type, "payload": payload })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_signature_is_stable_hex() {
        let signature = hmac_hex("s3cret", "{\"type\":\"message.received\"}");
        assert_eq!(signature.len(), 64);
        assert!(signature.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(
            signature,
            hmac_hex("other", "{\"type\":\"message.received\"}")
        );
        // Same secret and body always produce the same signature.
        assert_eq!(
            signature,
            hmac_hex("s3cret", "{\"type\":\"message.received\"}")
        );
    }

    #[test]
    fn empty_subscription_matches_everything() {
        assert!(matches(&[], "message.received"));
        assert!(matches(&["session.qr".to_string()], "session.qr"));
        assert!(!matches(&["session.qr".to_string()], "message.received"));
    }

    #[test]
    fn envelope_shape() {
        let envelope = event_envelope("session.qr", &json!({"sessionId": "s1", "qr": "x"}));
        assert_eq!(envelope["type"], "session.qr");
        assert_eq!(envelope["payload"]["sessionId"], "s1");
    }

    #[test]
    fn backoff_is_capped() {
        assert_eq!(BACKOFF_MS.len() as i64 + 1, MAX_ATTEMPTS);
    }
}
