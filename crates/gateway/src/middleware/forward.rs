//! Forwarding/ownership guard for session-scoped routes.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

use crate::state::AppState;

pub async fn owner_guard(
    State(state): State<AppState>,
    request: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> Response {
    let path = request.uri().path().to_string();
    if let Some(session_id) = session_id_from_path(&path) {
        if !state.is_local_owner(&session_id).await {
            if let Some(owner) = state.owner_url(&session_id).await {
                return (
                    StatusCode::CONFLICT,
                    Json(json!({ "code": "SESSION_NOT_LOCAL", "owner": owner })),
                )
                    .into_response();
            }
        }
    }
    next.run(request).await
}

fn session_id_from_path(path: &str) -> Option<String> {
    let mut segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    if segments.first().copied() == Some("api") {
        segments = segments[1..].to_vec();
    }
    if segments.first().copied() != Some("sessions") {
        return None;
    }
    let id = segments.get(1).copied().unwrap_or("");
    if id.is_empty() {
        None
    } else {
        // Best-effort percent-decode is not required for typical ids.
        Some(id.to_string())
    }
}
