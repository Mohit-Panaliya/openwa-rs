//! Label routes.

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
    let db_labels = state
        .db
        .list_labels(&session_id)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;
    if db_labels.is_empty() {
        let result = state
            .engine
            .request("label.list", json!({ "sessionId": session_id }))
            .await?;
        return Ok(Json(result));
    }
    Ok(Json(json!({ "labels": Value::Array(db_labels) })))
}

#[derive(Deserialize)]
pub struct CreateLabel {
    pub name: String,
    #[serde(default)]
    pub color: Option<i64>,
}

pub async fn create(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<CreateLabel>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let color = body.color.unwrap_or(0);
    let result = state
        .engine
        .request(
            "label.create",
            json!({ "sessionId": session_id, "name": body.name, "color": color }),
        )
        .await?;
    let id = result
        .get("id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_default();
    if !id.is_empty() {
        let _ = state
            .db
            .insert_label(&id, &session_id, &body.name, color)
            .await;
    }

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = if id.is_empty() {
        None
    } else {
        Some(id.clone())
    };
    tokio::spawn(async move {
        let _ = db
            .insert_audit("label.create", Some(&actor), resource.as_deref(), None)
            .await;
    });

    Ok(Json(result))
}

pub async fn delete(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, label_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    state
        .engine
        .request(
            "label.delete",
            json!({ "sessionId": session_id, "labelId": label_id }),
        )
        .await?;
    let _ = state.db.delete_label_db(&session_id, &label_id).await;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = label_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("label.delete", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(json!({ "deleted": true })))
}

#[derive(Deserialize)]
pub struct LabelChatBody {
    pub chat_id: String,
}

pub async fn add_chat(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, label_id)): Path<(String, String)>,
    Json(body): Json<LabelChatBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    identity.ensure_chat(&body.chat_id)?;
    state
        .engine
        .request(
            "label.addChat",
            json!({ "sessionId": session_id, "labelId": label_id, "chatId": body.chat_id }),
        )
        .await?;
    let _ = state
        .db
        .add_label_chat(&session_id, &label_id, &body.chat_id)
        .await;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = label_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("label.add_chat", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(json!({ "added": true })))
}

pub async fn remove_chat(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, label_id)): Path<(String, String)>,
    Json(body): Json<LabelChatBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    identity.ensure_chat(&body.chat_id)?;
    state
        .engine
        .request(
            "label.removeChat",
            json!({ "sessionId": session_id, "labelId": label_id, "chatId": body.chat_id }),
        )
        .await?;
    let _ = state
        .db
        .remove_label_chat(&session_id, &label_id, &body.chat_id)
        .await;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = label_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("label.remove_chat", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(json!({ "removed": true })))
}

pub async fn list_chats(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, label_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Viewer)?;
    identity.ensure_session(&session_id)?;
    let chats = state
        .db
        .list_label_chats(&session_id, &label_id)
        .await
        .unwrap_or_default();
    Ok(Json(json!({ "chats": chats })))
}
