//! Group routes.

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
    let result = state
        .engine
        .request("group.list", json!({ "sessionId": session_id }))
        .await?;
    // Persist to DB (best effort)
    if let Some(groups) = result.get("groups").and_then(Value::as_array) {
        for g in groups {
            let gid = g.get("id").and_then(Value::as_str).unwrap_or("");
            let subject = g.get("subject").and_then(Value::as_str);
            let size = g.get("size").and_then(Value::as_i64).unwrap_or(0);
            let _ = state
                .db
                .upsert_group(&session_id, gid, subject, None, size)
                .await;
        }
    }
    // Also return DB list for consistency
    let db_groups = state.db.list_groups(&session_id).await.unwrap_or_default();
    Ok(Json(
        json!({ "groups": if db_groups.is_empty() { result.get("groups").cloned().unwrap_or(json!([])) } else { Value::Array(db_groups) } }),
    ))
}

#[derive(Deserialize)]
pub struct GroupPath {
    pub session_id: String,
    pub group_id: String,
}

pub async fn get(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, group_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Viewer)?;
    identity.ensure_session(&session_id)?;
    identity.ensure_chat(&group_id)?;
    let result = state
        .engine
        .request(
            "group.get",
            json!({ "sessionId": session_id, "groupId": group_id }),
        )
        .await?;
    Ok(Json(result))
}

#[derive(Deserialize)]
pub struct CreateBody {
    #[serde(default)]
    pub subject: Option<String>,
    #[serde(default)]
    pub participants: Option<Vec<String>>,
}

pub async fn create(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<CreateBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let subject = body.subject.unwrap_or_else(|| "New Group".to_string());
    let participants = body.participants.unwrap_or_default();
    let result = state
        .engine
        .request(
            "group.create",
            json!({ "sessionId": session_id, "subject": subject, "participants": participants }),
        )
        .await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = result
        .get("groupId")
        .and_then(Value::as_str)
        .map(str::to_string);
    tokio::spawn(async move {
        let _ = db
            .insert_audit("group.create", Some(&actor), resource.as_deref(), None)
            .await;
    });

    Ok(Json(result))
}

pub async fn leave(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, group_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    identity.ensure_chat(&group_id)?;
    state
        .engine
        .request(
            "group.leave",
            json!({ "sessionId": session_id, "groupId": group_id }),
        )
        .await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = group_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("group.leave", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(json!({ "left": true })))
}

#[derive(Deserialize)]
pub struct SubjectBody {
    pub subject: String,
}

pub async fn set_subject(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, group_id)): Path<(String, String)>,
    Json(body): Json<SubjectBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    identity.ensure_chat(&group_id)?;
    state
        .engine
        .request(
            "group.setSubject",
            json!({ "sessionId": session_id, "groupId": group_id, "subject": body.subject }),
        )
        .await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = group_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("group.set_subject", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(json!({ "updated": true })))
}

#[derive(Deserialize)]
pub struct DescriptionBody {
    #[serde(default)]
    pub description: String,
}

pub async fn set_description(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, group_id)): Path<(String, String)>,
    Json(body): Json<DescriptionBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    identity.ensure_chat(&group_id)?;
    state.engine.request("group.setDescription", json!({ "sessionId": session_id, "groupId": group_id, "description": body.description })).await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = group_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("group.set_description", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(json!({ "updated": true })))
}

#[derive(Deserialize)]
pub struct ParticipantsBody {
    #[serde(default)]
    pub participants: Vec<String>,
}

pub async fn add_participants(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, group_id)): Path<(String, String)>,
    Json(body): Json<ParticipantsBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    identity.ensure_chat(&group_id)?;
    state
        .engine
        .request(
            "group.addParticipants",
            json!({ "sessionId": session_id, "groupId": group_id, "participants": body.participants }),
        )
        .await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = group_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit(
                "group.add_participants",
                Some(&actor),
                Some(&resource),
                None,
            )
            .await;
    });

    Ok(Json(json!({ "added": true })))
}

pub async fn remove_participants(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, group_id)): Path<(String, String)>,
    Json(body): Json<ParticipantsBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    identity.ensure_chat(&group_id)?;
    state
        .engine
        .request(
            "group.removeParticipants",
            json!({ "sessionId": session_id, "groupId": group_id, "participants": body.participants }),
        )
        .await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = group_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit(
                "group.remove_participants",
                Some(&actor),
                Some(&resource),
                None,
            )
            .await;
    });

    Ok(Json(json!({ "removed": true })))
}

pub async fn promote(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, group_id)): Path<(String, String)>,
    Json(body): Json<ParticipantsBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    identity.ensure_chat(&group_id)?;
    state
        .engine
        .request(
            "group.promote",
            json!({ "sessionId": session_id, "groupId": group_id, "participants": body.participants }),
        )
        .await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = group_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("group.promote", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(json!({ "promoted": true })))
}

pub async fn demote(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, group_id)): Path<(String, String)>,
    Json(body): Json<ParticipantsBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    identity.ensure_chat(&group_id)?;
    state
        .engine
        .request(
            "group.demote",
            json!({ "sessionId": session_id, "groupId": group_id, "participants": body.participants }),
        )
        .await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = group_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("group.demote", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(json!({ "demoted": true })))
}

pub async fn invite_link(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, group_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Viewer)?;
    identity.ensure_session(&session_id)?;
    identity.ensure_chat(&group_id)?;
    let result = state
        .engine
        .request(
            "group.inviteLink",
            json!({ "sessionId": session_id, "groupId": group_id }),
        )
        .await?;
    Ok(Json(result))
}

pub async fn settings(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, group_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Viewer)?;
    identity.ensure_session(&session_id)?;
    identity.ensure_chat(&group_id)?;
    let result = state
        .engine
        .request(
            "group.settings",
            json!({ "sessionId": session_id, "groupId": group_id }),
        )
        .await?;
    Ok(Json(result))
}

#[derive(Deserialize)]
pub struct JoinBody {
    #[serde(rename = "inviteCode")]
    pub invite_code: String,
}

pub async fn join(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<JoinBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let result = state
        .engine
        .request(
            "group.join",
            json!({ "sessionId": session_id, "inviteCode": body.invite_code }),
        )
        .await?;
    Ok(Json(result))
}

pub async fn membership_requests(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, group_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Viewer)?;
    identity.ensure_session(&session_id)?;
    identity.ensure_chat(&group_id)?;
    let result = state
        .engine
        .request(
            "group.membershipRequests",
            json!({ "sessionId": session_id, "groupId": group_id }),
        )
        .await?;
    Ok(Json(result))
}
