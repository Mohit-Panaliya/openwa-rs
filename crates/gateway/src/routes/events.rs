//! Server-sent events: one stream of engine events per authenticated client.

use std::convert::Infallible;

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::Extension;
use serde_json::Value;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::auth::{Identity, Role};
use crate::error::ApiError;
use crate::state::AppState;

pub async fn events(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    identity.require_role(Role::Viewer)?;

    let receiver = state.engine.subscribe();
    let stream = BroadcastStream::new(receiver).filter_map(move |result| match result {
        Ok(value) => {
            let visible = value
                .get("payload")
                .and_then(|payload| payload.get("sessionId"))
                .and_then(Value::as_str)
                .map(|session_id| identity.allows_session(session_id))
                .unwrap_or(true);
            visible.then(|| Ok(Event::default().data(value.to_string())))
        }
        Err(_) => None,
    });

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
