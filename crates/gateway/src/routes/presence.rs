//! Presence routes.

use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::{Identity, Role};
use crate::error::ApiError;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct SubscribeBody {
    pub chat_id: String,
}

pub async fn subscribe(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<SubscribeBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    identity.ensure_chat(&body.chat_id)?;
    state
        .engine
        .request(
            "presence.subscribe",
            json!({ "sessionId": session_id, "chatId": body.chat_id }),
        )
        .await?;
    Ok(Json(json!({ "subscribed": true })))
}

#[derive(Deserialize)]
pub struct SetPresence {
    #[serde(default = "default_state")]
    pub state: String,
}

fn default_state() -> String {
    "available".to_string()
}

pub async fn set(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<SetPresence>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    state
        .engine
        .request(
            "presence.set",
            json!({ "sessionId": session_id, "state": body.state }),
        )
        .await?;
    Ok(Json(json!({ "state": body.state })))
}
