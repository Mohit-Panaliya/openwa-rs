//! Newsletter (channel) routes.

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
        .request("newsletter.list", json!({ "sessionId": session_id }))
        .await?;
    Ok(Json(result))
}

pub async fn get(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, newsletter_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Viewer)?;
    identity.ensure_session(&session_id)?;
    let result = state
        .engine
        .request(
            "newsletter.get",
            json!({ "sessionId": session_id, "newsletterId": newsletter_id }),
        )
        .await?;
    Ok(Json(result))
}

#[derive(Deserialize)]
pub struct NewsletterIdBody {
    pub newsletter_id: String,
}

pub async fn follow(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<NewsletterIdBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    state
        .engine
        .request(
            "newsletter.follow",
            json!({ "sessionId": session_id, "newsletterId": body.newsletter_id }),
        )
        .await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = body.newsletter_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("newsletter.follow", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(json!({ "followed": true })))
}

pub async fn unfollow(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<NewsletterIdBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    state
        .engine
        .request(
            "newsletter.leave",
            json!({ "sessionId": session_id, "newsletterId": body.newsletter_id }),
        )
        .await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = body.newsletter_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("newsletter.unfollow", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(json!({ "left": true })))
}
