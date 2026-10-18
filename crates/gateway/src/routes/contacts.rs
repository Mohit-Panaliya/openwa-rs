//! Contact routes.

use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::{Identity, Role};
use crate::error::ApiError;
use crate::state::AppState;

pub async fn list(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Viewer)?;
    identity.ensure_session(&session_id)?;
    let contacts = state
        .db
        .list_contacts(&session_id)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;
    Ok(Json(json!({ "contacts": contacts })))
}

pub async fn get(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, contact_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Viewer)?;
    identity.ensure_session(&session_id)?;
    identity.ensure_chat(&contact_id).ok();
    let result = state
        .engine
        .request(
            "contact.info",
            json!({ "sessionId": session_id, "contactId": contact_id }),
        )
        .await?;
    Ok(Json(result))
}

pub async fn check(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, number)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let result = state
        .engine
        .request(
            "contact.check",
            json!({ "sessionId": session_id, "number": number }),
        )
        .await?;
    Ok(Json(result))
}

pub async fn profile_picture(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, contact_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Viewer)?;
    identity.ensure_session(&session_id)?;
    let result = state
        .engine
        .request(
            "contact.profilePic",
            json!({ "sessionId": session_id, "contactId": contact_id }),
        )
        .await?;
    Ok(Json(result))
}

pub async fn block(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, contact_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    identity.ensure_chat(&contact_id)?;
    state
        .engine
        .request(
            "contact.block",
            json!({ "sessionId": session_id, "contactId": contact_id }),
        )
        .await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = contact_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("contact.block", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(json!({ "blocked": true })))
}

pub async fn unblock(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, contact_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    identity.ensure_chat(&contact_id)?;
    state
        .engine
        .request(
            "contact.unblock",
            json!({ "sessionId": session_id, "contactId": contact_id }),
        )
        .await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = contact_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("contact.unblock", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(json!({ "unblocked": true })))
}

#[derive(Deserialize)]
pub struct ContactCard {
    pub name: String,
    pub phone: String,
}

#[derive(Deserialize)]
pub struct SendContactBody {
    #[serde(rename = "chatId")]
    pub chat_id: String,
    pub contact: ContactCard,
}

pub async fn send_contact_card(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<SendContactBody>,
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
            "message.sendContact",
            json!({
                "sessionId": session_id,
                "chatId": chat_id,
                "contact": { "name": body.contact.name, "phone": body.contact.phone },
            }),
        )
        .await?;
    Ok(Json(result))
}
