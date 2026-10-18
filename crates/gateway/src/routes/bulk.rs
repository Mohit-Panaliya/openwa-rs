//! Bulk message routes.

use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::{Identity, Role};
use crate::error::ApiError;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct BulkMessage {
    #[serde(rename = "chatId")]
    pub chat_id: String,
    pub text: String,
}

#[derive(Deserialize)]
pub struct BulkBody {
    pub messages: Vec<BulkMessage>,
}

pub async fn send_bulk(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<BulkBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    if body.messages.is_empty() {
        return Err(ApiError::bad_request("messages is required"));
    }
    let messages: Vec<Value> = body
        .messages
        .iter()
        .map(|m| json!({ "chatId": m.chat_id, "text": m.text }))
        .collect();
    let result = state
        .engine
        .request(
            "message.sendBulk",
            json!({ "sessionId": session_id, "messages": messages }),
        )
        .await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("message.send_bulk", Some(&actor), None, None)
            .await;
    });

    Ok(Json(result))
}
