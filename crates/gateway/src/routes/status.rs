//! Status routes.

use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::{Identity, Role};
use crate::error::ApiError;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct SendStatus {
    pub text: String,
}

pub async fn send_text(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<SendStatus>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    state
        .engine
        .request(
            "status.sendText",
            json!({ "sessionId": session_id, "text": body.text }),
        )
        .await?;
    Ok(Json(json!({ "sent": true })))
}
