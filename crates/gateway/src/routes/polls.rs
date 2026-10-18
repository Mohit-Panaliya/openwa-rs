//! Poll routes.

use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::{Identity, Role};
use crate::error::ApiError;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct CreatePoll {
    #[serde(rename = "chatId")]
    chat_id: String,
    name: String,
    options: Vec<String>,
    #[serde(default = "default_selectable")]
    selectable_count: u32,
}

fn default_selectable() -> u32 {
    1
}

pub async fn create(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<CreatePoll>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    identity.ensure_chat(&body.chat_id)?;
    let result = state
        .engine
        .request(
            "poll.create",
            json!({
                "sessionId": session_id,
                "chatId": body.chat_id,
                "name": body.name,
                "options": body.options,
                "selectableCount": body.selectable_count,
            }),
        )
        .await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = result
        .get("messageId")
        .and_then(Value::as_str)
        .map(str::to_string);
    tokio::spawn(async move {
        let _ = db
            .insert_audit("poll.create", Some(&actor), resource.as_deref(), None)
            .await;
    });

    Ok(Json(result))
}

#[derive(Deserialize)]
pub struct VotePoll {
    #[serde(rename = "chatId")]
    chat_id: String,
    #[serde(rename = "pollMessageId")]
    poll_message_id: String,
    #[serde(rename = "creatorJid")]
    creator_jid: String,
    secret: String,
    options: Vec<String>,
}

pub async fn vote(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<VotePoll>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    identity.ensure_chat(&body.chat_id)?;
    let poll_message_id = body.poll_message_id.clone();
    state
        .engine
        .request(
            "poll.vote",
            json!({
                "sessionId": session_id,
                "chatId": body.chat_id,
                "pollMessageId": body.poll_message_id,
                "creatorJid": body.creator_jid,
                "secret": body.secret,
                "options": body.options,
            }),
        )
        .await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = poll_message_id;
    tokio::spawn(async move {
        let _ = db
            .insert_audit("poll.vote", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(json!({ "voted": true })))
}
