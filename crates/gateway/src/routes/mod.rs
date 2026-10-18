//! HTTP routes: OpenWA-compatible REST, plus the Phase 1.5 store/keys/dashboard
//! surface. See docs/PHASE1.md and docs/PHASE1_5.md.

pub mod audit;
pub mod bulk;
pub mod calls;
pub mod chats;
pub mod contacts;
pub mod events;
pub mod groups;
pub mod keys;
pub mod labels;
pub mod location;
pub mod mcp;
pub mod media;
pub mod messages;
pub mod newsletters;
pub mod polls;
pub mod presence;
pub mod profile;
pub mod sessions;
pub mod stats;
pub mod status;
pub mod webhooks;

use axum::extract::State;
use axum::response::Html;
use axum::routing::{delete, get, post};
use axum::{middleware, Json, Router};
use serde_json::{json, Value};
use tower_http::services::ServeDir;

use crate::auth;
use crate::state::AppState;

pub fn router(state: AppState) -> Router {
    let protected = Router::new()
        .route("/sessions", get(sessions::list).post(sessions::create))
        .route(
            "/sessions/{session_id}",
            get(sessions::get).delete(sessions::delete),
        )
        .route("/sessions/{session_id}/start", post(sessions::start))
        .route("/sessions/{session_id}/stop", post(sessions::stop))
        .route("/sessions/{session_id}/logout", post(sessions::logout))
        .route("/sessions/{session_id}/qr", get(sessions::qr))
        .route("/sessions/{session_id}/qr.png", get(sessions::qr_image))
        .route("/sessions/{session_id}/qr.svg", get(sessions::qr_image))
        .route("/sessions/{session_id}/messages", get(messages::list))
        .route(
            "/sessions/{session_id}/messages/send-text",
            post(messages::send_text),
        )
        .route(
            "/sessions/{session_id}/messages/send-media",
            post(messages::send_media),
        )
        .route(
            "/sessions/{session_id}/messages/send-bulk",
            post(bulk::send_bulk),
        )
        .route(
            "/sessions/{session_id}/messages/send-location",
            post(location::send_location),
        )
        .route(
            "/sessions/{session_id}/messages/send-contact",
            post(contacts::send_contact_card),
        )
        .route(
            "/sessions/{session_id}/messages/{message_id}/react",
            post(messages::react),
        )
        .route(
            "/sessions/{session_id}/messages/{message_id}",
            axum::routing::patch(messages::edit).delete(messages::delete),
        )
        .route(
            "/sessions/{session_id}/messages/{message_id}/star",
            post(messages::star),
        )
        .route(
            "/sessions/{session_id}/messages/{message_id}/forward",
            post(messages::forward),
        )
        .route(
            "/sessions/{session_id}/messages/{message_id}/pin",
            post(messages::pin),
        )
        .route("/sessions/{session_id}/contacts", get(contacts::list))
        .route(
            "/sessions/{session_id}/contacts/check/{number}",
            get(contacts::check),
        )
        .route(
            "/sessions/{session_id}/contacts/{contact_id}/profile-picture",
            get(contacts::profile_picture),
        )
        .route(
            "/sessions/{session_id}/contacts/{contact_id}/block",
            post(contacts::block),
        )
        .route(
            "/sessions/{session_id}/contacts/{contact_id}/unblock",
            post(contacts::unblock),
        )
        .route(
            "/sessions/{session_id}/contacts/{contact_id}",
            get(contacts::get),
        )
        .route(
            "/sessions/{session_id}/groups",
            get(groups::list).post(groups::create),
        )
        .route("/sessions/{session_id}/groups/{group_id}", get(groups::get))
        .route(
            "/sessions/{session_id}/groups/{group_id}/leave",
            post(groups::leave),
        )
        .route(
            "/sessions/{session_id}/groups/{group_id}/subject",
            axum::routing::patch(groups::set_subject),
        )
        .route(
            "/sessions/{session_id}/groups/{group_id}/description",
            axum::routing::patch(groups::set_description),
        )
        .route(
            "/sessions/{session_id}/groups/{group_id}/participants",
            post(groups::add_participants).delete(groups::remove_participants),
        )
        .route(
            "/sessions/{session_id}/groups/{group_id}/participants/promote",
            post(groups::promote),
        )
        .route(
            "/sessions/{session_id}/groups/{group_id}/participants/demote",
            post(groups::demote),
        )
        .route(
            "/sessions/{session_id}/groups/{group_id}/invite-link",
            get(groups::invite_link),
        )
        .route(
            "/sessions/{session_id}/groups/{group_id}/settings",
            get(groups::settings),
        )
        .route(
            "/sessions/{session_id}/groups/{group_id}/membership-requests",
            get(groups::membership_requests),
        )
        .route("/sessions/{session_id}/groups/join", post(groups::join))
        .route(
            "/sessions/{session_id}/profile",
            get(profile::get).patch(profile::update),
        )
        .route(
            "/sessions/{session_id}/profile/picture",
            post(profile::set_picture).delete(profile::remove_picture),
        )
        .route(
            "/sessions/{session_id}/labels",
            get(labels::list).post(labels::create),
        )
        .route(
            "/sessions/{session_id}/labels/{label_id}",
            delete(labels::delete),
        )
        .route(
            "/sessions/{session_id}/labels/{label_id}/chats",
            get(labels::list_chats).post(labels::add_chat),
        )
        .route(
            "/sessions/{session_id}/labels/{label_id}/chats/remove",
            post(labels::remove_chat),
        )
        .route("/sessions/{session_id}/chats/read", post(chats::mark_read))
        .route("/sessions/{session_id}/chats/archive", post(chats::archive))
        .route(
            "/sessions/{session_id}/chats/unarchive",
            post(chats::unarchive),
        )
        .route("/sessions/{session_id}/chats/pin", post(chats::pin))
        .route("/sessions/{session_id}/chats/unpin", post(chats::unpin))
        .route("/sessions/{session_id}/chats/mute", post(chats::mute))
        .route("/sessions/{session_id}/chats/unmute", post(chats::unmute))
        .route("/sessions/{session_id}/chats/delete", post(chats::delete))
        .route("/sessions/{session_id}/chats/clear", post(chats::clear))
        .route(
            "/sessions/{session_id}/presence/subscribe",
            post(presence::subscribe),
        )
        .route("/sessions/{session_id}/presence", post(presence::set))
        .route("/sessions/{session_id}/status", post(status::send_text))
        .route("/sessions/{session_id}/newsletters", get(newsletters::list))
        .route(
            "/sessions/{session_id}/newsletters/{newsletter_id}",
            get(newsletters::get),
        )
        .route(
            "/sessions/{session_id}/newsletters/follow",
            post(newsletters::follow),
        )
        .route(
            "/sessions/{session_id}/newsletters/unfollow",
            post(newsletters::unfollow),
        )
        .route("/sessions/{session_id}/polls", post(polls::create))
        .route("/sessions/{session_id}/polls/vote", post(polls::vote))
        .route(
            "/sessions/{session_id}/calls/{call_id}/reject",
            post(calls::reject),
        )
        .route(
            "/sessions/{session_id}/webhooks",
            get(webhooks::list).post(webhooks::create),
        )
        .route(
            "/sessions/{session_id}/webhooks/{webhook_id}",
            delete(webhooks::delete),
        )
        .route("/webhooks/deliveries", get(webhooks::deliveries))
        .route(
            "/webhooks/deliveries/{delivery_id}/retry",
            post(webhooks::retry),
        )
        .route("/keys", get(keys::list).post(keys::create))
        .route("/keys/{key_id}", delete(keys::delete))
        .route("/keys/{key_id}/rotate", post(keys::rotate))
        .route("/stats", get(stats::stats))
        .route("/search", get(stats::search))
        .route("/audit", get(audit::list))
        .route("/media/{media_id}", get(media::get))
        .route("/events", get(events::events))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            crate::middleware::forward::owner_guard,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_api_key,
        ))
        .layer(middleware::from_fn(
            crate::rate_limit::rate_limit_middleware,
        ));

    let mcp_protected = Router::new()
        .route("/mcp", post(mcp::handle))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_api_key,
        ))
        .layer(middleware::from_fn(
            crate::rate_limit::rate_limit_middleware,
        ))
        .with_state(state.clone());

    Router::new()
        .route("/api/health", get(health))
        .route("/api/health/live", get(health_live))
        .route("/api/health/ready", get(health_ready))
        .route("/api/openapi.json", get(openapi_spec))
        .route("/api/docs", get(swagger_ui))
        .nest("/api", protected)
        .merge(mcp_protected)
        .nest_service(
            "/assets",
            ServeDir::new(state.dashboard_dir.as_ref().clone()),
        )
        .fallback_service(
            ServeDir::new(state.dashboard_dir.as_ref().clone())
                .append_index_html_on_directories(true),
        )
        .with_state(state)
}

/// Public: `{"status":"ok","sidecar":"up"|"down","engine":"..."}`.
async fn health(State(state): State<AppState>) -> Json<Value> {
    let alive = state.engine.is_alive().await && state.engine.ping().await.is_ok();
    Json(json!({
        "status": "ok",
        "sidecar": if alive { "up" } else { "down" },
        "engine": state.engine.kind(),
        "database": "up",
    }))
}

async fn health_live() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn health_ready(State(state): State<AppState>) -> Json<Value> {
    let alive = state.engine.is_alive().await && state.engine.ping().await.is_ok();
    Json(json!({ "status": if alive { "ready" } else { "not_ready" } }))
}

async fn openapi_spec() -> Json<Value> {
    let raw = include_str!("../openapi.json");
    let value: Value = serde_json::from_str(raw).unwrap_or_else(|_| json!({}));
    Json(value)
}

async fn swagger_ui() -> Html<&'static str> {
    Html(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>OpenWA API Docs</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css" />
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.onload = function () {
      window.ui = SwaggerUIBundle({
        url: "/api/openapi.json",
        dom_id: "#swagger-ui",
      });
    };
  </script>
</body>
</html>"##,
    )
}
