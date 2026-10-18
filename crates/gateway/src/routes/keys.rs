//! API-key management (admin only): create, list, rotate, delete.
//!
//! Plaintext keys are returned once, on create and on rotate; only their sha256
//! hash is stored.

use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::{Identity, Role};
use crate::error::ApiError;
use crate::state::AppState;
use crate::util::{key_prefix, random_hex, sha256_hex};

fn parse_role(value: &str) -> Result<Role, ApiError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "admin" => Ok(Role::Admin),
        "operator" => Ok(Role::Operator),
        "viewer" => Ok(Role::Viewer),
        other => Err(ApiError::bad_request(format!(
            "role must be one of admin, operator, viewer (got `{other}`)"
        ))),
    }
}

pub async fn list(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Admin)?;
    let keys = state.db.list_keys().await?;
    Ok(Json(json!({
        "keys": keys
            .into_iter()
            .map(|key| json!({
                "id": key.id,
                "name": key.name,
                "role": key.role,
                "keyPrefix": key.key_prefix,
                "allowedSessions": key.allowed_sessions,
                "allowedChats": key.allowed_chats,
                "allowedIps": key.allowed_ips,
                "createdAt": key.created_at,
                "lastUsedAt": key.last_used_at,
            }))
            .collect::<Vec<_>>(),
    })))
}

#[derive(Deserialize)]
pub struct CreateKey {
    name: String,
    role: String,
    #[serde(rename = "allowedSessions", default)]
    allowed_sessions: Vec<String>,
    #[serde(rename = "allowedChats", default)]
    allowed_chats: Vec<String>,
    #[serde(rename = "allowedIps", default)]
    allowed_ips: Vec<String>,
}

pub async fn create(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Json(body): Json<CreateKey>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Admin)?;

    let name = body.name.trim().to_string();
    if name.is_empty() {
        return Err(ApiError::bad_request("name is required"));
    }
    let role = parse_role(&body.role)?;

    let api_key = random_hex(24);
    let id = random_hex(8);
    state
        .db
        .insert_key(
            &id,
            &name,
            role.as_str(),
            &sha256_hex(&api_key),
            &key_prefix(&api_key),
            &body.allowed_sessions,
            &body.allowed_chats,
            &body.allowed_ips,
        )
        .await?;

    // Audit (best effort).
    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("key.create", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(json!({
        "id": id,
        "name": name,
        "role": role.as_str(),
        "apiKey": api_key,
        "keyPrefix": key_prefix(&api_key),
        "allowedSessions": body.allowed_sessions,
        "allowedChats": body.allowed_chats,
        "allowedIps": body.allowed_ips,
    })))
}

pub async fn rotate(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(key_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Admin)?;

    let api_key = random_hex(24);
    let rotated = state
        .db
        .rotate_key(&key_id, &sha256_hex(&api_key), &key_prefix(&api_key))
        .await?;
    if !rotated {
        return Err(ApiError::not_found("API key not found"));
    }

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = key_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("key.rotate", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(json!({ "id": key_id, "apiKey": api_key })))
}

pub async fn delete(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(key_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Admin)?;

    let keys = state.db.list_keys().await?;
    let target = keys
        .iter()
        .find(|key| key.id == key_id)
        .ok_or_else(|| ApiError::not_found("API key not found"))?;

    if target.role == Role::Admin.as_str() && state.db.count_keys_by_role("admin").await? <= 1 {
        return Err(ApiError::bad_request(
            "cannot delete the last admin API key",
        ));
    }

    state.db.delete_key(&key_id).await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = key_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("key.delete", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(json!({ "deleted": true })))
}
