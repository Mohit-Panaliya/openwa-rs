//! Webhook subscription routes plus the delivery log and manual retry.

use axum::extract::{Path, Query, State};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::{Identity, Role};
use crate::error::ApiError;
use crate::state::AppState;
use crate::util::random_hex;

#[derive(Deserialize)]
pub struct CreateWebhook {
    url: String,
    #[serde(default)]
    events: Vec<String>,
    #[serde(default)]
    secret: Option<String>,
}

pub async fn create(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<CreateWebhook>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;

    if !body.url.starts_with("http://") && !body.url.starts_with("https://") {
        return Err(ApiError::bad_request(
            "url must start with http:// or https://",
        ));
    }

    let secret = body.secret.filter(|value| !value.is_empty());
    let webhook = state
        .db
        .insert_webhook(
            &random_hex(8),
            &session_id,
            &body.url,
            &body.events,
            secret.as_deref(),
        )
        .await?;

    Ok(Json(serde_json::to_value(webhook).unwrap_or(Value::Null)))
}

pub async fn list(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Viewer)?;
    identity.ensure_session(&session_id)?;
    let webhooks = state.db.list_webhooks(&session_id).await?;
    Ok(Json(json!({ "webhooks": webhooks })))
}

pub async fn delete(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, webhook_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;

    if state.db.delete_webhook(&session_id, &webhook_id).await? {
        Ok(Json(json!({ "deleted": true })))
    } else {
        Err(ApiError::not_found("webhook not found"))
    }
}

#[derive(Deserialize)]
pub struct DeliveriesQuery {
    #[serde(rename = "sessionId")]
    session_id: Option<String>,
    limit: Option<i64>,
}

pub async fn deliveries(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Query(query): Query<DeliveriesQuery>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;

    if let Some(session_id) = query.session_id.as_deref() {
        identity.ensure_session(session_id)?;
    }

    let rows = state
        .db
        .list_deliveries(query.session_id.as_deref(), query.limit.unwrap_or(50))
        .await?;

    // Workspace keys only see deliveries for sessions they may touch.
    let visible: Vec<Value> = rows
        .into_iter()
        .filter(|row| identity.allows_session(&row.session_id))
        .map(|row| serde_json::to_value(row).unwrap_or(Value::Null))
        .collect();

    Ok(Json(json!({ "deliveries": visible })))
}

pub async fn retry(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(delivery_id): Path<i64>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;

    let delivery = state
        .db
        .get_delivery(delivery_id)
        .await?
        .ok_or_else(|| ApiError::not_found("delivery not found"))?;
    identity.ensure_session(&delivery.session_id)?;

    let webhook = state
        .db
        .get_webhook(&delivery.webhook_id)
        .await?
        .ok_or_else(|| ApiError::not_found("the webhook for this delivery no longer exists"))?;
    let payload = state
        .db
        .delivery_payload(delivery_id)
        .await?
        .unwrap_or_else(|| "{}".to_string());

    tokio::spawn(crate::webhooks::retry_once(
        state.db.clone(),
        state.http.clone(),
        delivery.clone(),
        webhook,
        payload,
    ));

    Ok(Json(json!({ "retried": true, "id": delivery_id })))
}
