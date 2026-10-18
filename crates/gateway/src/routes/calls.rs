//! Call routes.

use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde_json::{json, Value};

use crate::auth::{Identity, Role};
use crate::error::ApiError;
use crate::state::AppState;

pub async fn reject(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, call_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    state
        .engine
        .request(
            "calls.reject",
            json!({ "sessionId": session_id, "callId": call_id }),
        )
        .await?;
    Ok(Json(json!({ "rejected": true })))
}
