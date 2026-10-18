//! Location message routes.

use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::{Identity, Role};
use crate::error::ApiError;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct SendLocationBody {
    #[serde(rename = "chatId")]
    pub chat_id: String,
    pub latitude: f64,
    pub longitude: f64,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub address: Option<String>,
}

pub async fn send_location(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<SendLocationBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let chat_id = body.chat_id.trim().to_string();
    if chat_id.is_empty() {
        return Err(ApiError::bad_request("chatId is required"));
    }
    identity.ensure_chat(&chat_id)?;
    let result = state
        .engine
        .request(
            "message.sendLocation",
            json!({
                "sessionId": session_id,
                "chatId": chat_id,
                "latitude": body.latitude,
                "longitude": body.longitude,
                "name": body.name,
                "address": body.address,
            }),
        )
        .await?;
    Ok(Json(result))
}
