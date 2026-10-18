//! Session lifecycle routes.

use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::{Identity, Role};
use crate::error::ApiError;
use crate::state::AppState;

/// `(sessionId, name, status)` out of an engine session result.
fn meta(value: &Value) -> (String, String, String) {
    let field = |key: &str| {
        value
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    (field("sessionId"), field("name"), field("status"))
}

pub async fn list(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Viewer)?;
    let value = state.engine.request("session.list", json!({})).await?;

    // The engine is the source of truth for which sessions exist; the database
    // only remembers names and is where scope membership comes from.
    let mut sessions = Vec::new();
    if let Some(items) = value.as_array() {
        for item in items {
            let id = item
                .get("sessionId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if identity.allows_session(id) {
                sessions.push(item.clone());
            }
        }
    }

    Ok(Json(json!({ "sessions": sessions })))
}

#[derive(Deserialize)]
pub struct CreateSession {
    #[serde(default)]
    pub name: Option<String>,
}

pub async fn create(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Json(body): Json<CreateSession>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    if !identity.is_admin() && !identity.allowed_sessions.is_empty() {
        return Err(ApiError::forbidden(
            "API key is not authorized to create sessions",
        ));
    }

    let name = body
        .name
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| format!("session-{}", crate::util::random_hex(4)));

    let value = state
        .engine
        .request("session.create", json!({ "name": name }))
        .await?;
    let (id, name, status) = meta(&value);
    if let Err(error) = state
        .db
        .upsert_session(&id, &name, &status, Some(state.cluster.node_id.as_str()))
        .await
    {
        tracing::warn!(%error, "could not record the new session");
    }
    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("session.create", Some(&actor), Some(&resource), None)
            .await;
    });
    Ok(Json(value))
}

pub async fn get(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Viewer)?;
    identity.ensure_session(&session_id)?;
    let value = state.engine.session("session.get", &session_id).await?;
    Ok(Json(value))
}

pub async fn start(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    transition(state, identity, session_id, "session.start").await
}

pub async fn stop(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    transition(state, identity, session_id, "session.stop").await
}

pub async fn logout(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    transition(state, identity, session_id, "session.logout").await
}

async fn transition(
    state: AppState,
    identity: Identity,
    session_id: String,
    method: &str,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let value = state.engine.session(method, &session_id).await?;
    if let Some(status) = value.get("status").and_then(Value::as_str) {
        if let Err(error) = state.db.set_session_status(&session_id, status).await {
            tracing::warn!(%error, "could not persist session status");
        }
    }

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = session_id.clone();
    let action = method.to_string();
    tokio::spawn(async move {
        let _ = db
            .insert_audit(&action, Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(value))
}

pub async fn delete(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let value = state.engine.session("session.delete", &session_id).await?;

    if let Err(error) = state.db.delete_session(&session_id).await {
        tracing::warn!(%error, "could not delete the stored session");
    }
    if let Err(error) = state.db.delete_session_webhooks(&session_id).await {
        tracing::warn!(%error, "could not delete the session webhooks");
    }
    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = session_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("session.delete", Some(&actor), Some(&resource), None)
            .await;
    });
    Ok(Json(value))
}

pub async fn qr(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Viewer)?;
    identity.ensure_session(&session_id)?;
    let value = state.engine.session("session.qr", &session_id).await?;
    Ok(Json(value))
}

/// Renders the current pairing QR as an SVG image (`image/svg+xml`).
///
/// Served on both `/qr.png` and `/qr.svg`; browsers pick the renderer from the
/// content type, so an `<img>` tag works either way.
pub async fn qr_image(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
) -> Result<Response, ApiError> {
    identity.require_role(Role::Viewer)?;
    identity.ensure_session(&session_id)?;

    let value = state.engine.session("session.qr", &session_id).await?;
    let payload = value
        .get("qr")
        .and_then(Value::as_str)
        .filter(|qr| !qr.is_empty())
        .ok_or_else(|| ApiError::not_found("no QR code is available for this session"))?;

    let code = qrcode::QrCode::new(payload.as_bytes())
        .map_err(|error| ApiError::internal(format!("could not encode the QR code: {error}")))?;
    let svg = code
        .render::<qrcode::render::svg::Color>()
        .min_dimensions(280, 280)
        .quiet_zone(true)
        .build();

    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("image/svg+xml"),
        )],
        svg,
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qr_payload_renders_as_svg() {
        let code = qrcode::QrCode::new(b"mock-qr-data").expect("payload encodes");
        let svg = code
            .render::<qrcode::render::svg::Color>()
            .min_dimensions(280, 280)
            .quiet_zone(true)
            .build();
        assert!(
            svg.contains("<svg") && svg.ends_with("</svg>"),
            "unexpected output: {}",
            &svg[..svg.len().min(80)]
        );
    }

    #[test]
    fn session_meta_reads_engine_results() {
        let value = json!({ "sessionId": "abc", "name": "bot", "status": "created" });
        assert_eq!(
            meta(&value),
            ("abc".to_string(), "bot".to_string(), "created".to_string())
        );
        assert_eq!(
            meta(&json!({})),
            (String::new(), String::new(), String::new())
        );
    }
}
