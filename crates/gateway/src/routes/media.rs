//! Media retrieval: streams blobs stored by `send-media`.

use axum::extract::{Path, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};

use crate::auth::{Identity, Role};
use crate::error::ApiError;
use crate::state::AppState;

pub async fn get(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(media_id): Path<String>,
) -> Result<Response, ApiError> {
    identity.require_role(Role::Viewer)?;

    let record = state
        .db
        .get_media(&media_id)
        .await?
        .ok_or_else(|| ApiError::not_found("media not found"))?;
    identity.ensure_session(&record.session_id)?;

    let bytes = state
        .media
        .load(std::path::Path::new(&record.path))
        .await
        .map_err(|error| ApiError::not_found(format!("media file is unavailable: {error}")))?;

    let content_type = record
        .mimetype
        .clone()
        .unwrap_or_else(|| "application/octet-stream".to_string());

    let mut response = bytes.into_response();
    if let Ok(value) = axum::http::HeaderValue::from_str(&content_type) {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    Ok(response)
}

/// Metadata for a stored blob (handy for the dashboard and debugging).
pub async fn meta(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(media_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    identity.require_role(Role::Viewer)?;
    let record = state
        .db
        .get_media(&media_id)
        .await?
        .ok_or_else(|| ApiError::not_found("media not found"))?;
    identity.ensure_session(&record.session_id)?;
    Ok(Json(
        serde_json::to_value(record).unwrap_or(serde_json::Value::Null),
    ))
}
