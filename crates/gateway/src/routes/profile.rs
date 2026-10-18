//! Profile routes.

use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::{Identity, Role};
use crate::error::ApiError;
use crate::state::AppState;

pub async fn get(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Viewer)?;
    identity.ensure_session(&session_id)?;
    let result = state
        .engine
        .request("profile.get", json!({ "sessionId": session_id }))
        .await?;
    Ok(Json(result))
}

#[derive(Deserialize)]
pub struct UpdateProfile {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub about: Option<String>,
}

pub async fn update(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<UpdateProfile>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    if let Some(name) = body.name.as_deref() {
        state
            .engine
            .request(
                "profile.setName",
                json!({ "sessionId": session_id, "name": name }),
            )
            .await?;
    }
    if let Some(about) = body.about.as_deref() {
        state
            .engine
            .request(
                "profile.setAbout",
                json!({ "sessionId": session_id, "about": about }),
            )
            .await?;
    }

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = session_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("profile.update", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(json!({ "updated": true })))
}

#[derive(Deserialize)]
pub struct SetPictureBody {
    pub base64: String,
    pub mimetype: String,
}

pub async fn set_picture(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<SetPictureBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    if body.base64.trim().is_empty() {
        return Err(ApiError::bad_request("base64 is required"));
    }
    state
        .engine
        .request(
            "profile.setPicture",
            json!({ "sessionId": session_id, "base64": body.base64, "mimetype": body.mimetype }),
        )
        .await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = session_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("profile.set_picture", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(json!({ "updated": true })))
}

pub async fn remove_picture(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    state
        .engine
        .request("profile.removePicture", json!({ "sessionId": session_id }))
        .await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = session_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit(
                "profile.remove_picture",
                Some(&actor),
                Some(&resource),
                None,
            )
            .await;
    });

    Ok(Json(json!({ "updated": true })))
}
