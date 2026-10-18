//! Request dispatch, session registry and the event fan-out.
//!
//! This is the JSON-in/JSON-out heart of the engine, shared by the stdio
//! sidecar (`crates/engine-sidecar`) and any future in-process embedder. Every
//! piece of protocol wording the gateway keys off (`"session not found"`,
//! `"session is not connected"`, `"media payload required: base64 or url"`, ...)
//! is produced here, so [`crate::EngineCore::request`] stays the single source
//! of truth for `docs/SIDECAR_PROTOCOL.md`.
//!
//! ## Response-before-events ordering
//!
//! The Node reference queues events emitted *while a request handler runs* and
//! flushes them after the response, so the gateway never sees a status event
//! for a session id it has not been handed yet. [`EngineCore::request`]
//! reproduces that: it installs a queue, dispatches, moves whatever was queued
//! into a pending list and returns; the caller sends the response and then calls
//! [`EngineCore::flush_events`]. Between those two calls the request lock is
//! released, so callers that need strict stdout ordering must not run two
//! requests concurrently around the flush (the sidecar serializes them; see
//! `crates/engine-sidecar`).

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc, Mutex as AsyncMutex};
use whatsapp_rust::Client;

use crate::session::{self, Running};

/// Lifecycle status of a session. The strings match `docs/SIDECAR_PROTOCOL.md`
/// exactly; they are what the gateway persists and shows.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Status {
    Created,
    Connecting,
    QrReady,
    Connected,
    Disconnected,
    LoggedOut,
    Error,
}

impl Status {
    /// The wire string for this status.
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Created => "created",
            Status::Connecting => "connecting",
            Status::QrReady => "qr_ready",
            Status::Connected => "connected",
            Status::Disconnected => "disconnected",
            Status::LoggedOut => "logged_out",
            Status::Error => "error",
        }
    }
}

impl std::fmt::Display for Status {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// State shared with the library's event handler, which only holds a `Weak`
/// reference to the engine to avoid a reference cycle.
pub struct SessionShared {
    /// Stable identifier handed to the gateway.
    pub session_id: String,
    /// Human-readable name (defaults to the session id).
    pub name: String,
    /// `<DATA_DIR>/<sessionId>`; the store and media scratch live here.
    pub dir: PathBuf,
    status: Mutex<Status>,
    qr: Mutex<Option<String>>,
}

impl SessionShared {
    /// The current status.
    pub fn status(&self) -> Status {
        *self.status.lock().expect("session status lock poisoned")
    }

    /// The most recent QR payload, or `None`.
    pub fn qr(&self) -> Option<String> {
        self.qr.lock().expect("session qr lock poisoned").clone()
    }

    pub(crate) fn set_qr(&self, qr: Option<String>) {
        *self.qr.lock().expect("session qr lock poisoned") = qr;
    }

    pub(crate) fn set_status_value(&self, status: Status) {
        *self.status.lock().expect("session status lock poisoned") = status;
    }
}

/// One session: its shared metadata plus the live runtime, when started.
pub struct Session {
    /// Metadata visible to the event handler.
    pub shared: Arc<SessionShared>,
    /// The running bot, `None` when created/stopped/logged out.
    pub running: Mutex<Option<Running>>,
}

/// The shared interior of an [`EngineCore`]. Held behind an `Arc`; the library
/// event handler keeps a `Weak` to it.
pub struct EngineInner {
    data_dir: PathBuf,
    sessions: Mutex<Vec<Arc<Session>>>,
    events: broadcast::Sender<Value>,
    /// Set to `Some` while a request is being dispatched; events emitted then
    /// are buffered so the response can be written first.
    queue: Mutex<Option<Vec<Value>>>,
    /// Events moved out of `queue` at request completion, awaiting
    /// [`EngineCore::flush_events`].
    pending: Mutex<Vec<Value>>,
    /// Sink for protocol lines (responses are written by the caller directly;
    /// events go through here). `None` until the sidecar installs it.
    outbox: Mutex<Option<mpsc::UnboundedSender<Value>>>,
    request_lock: AsyncMutex<()>,
    shutting_down: AtomicBool,
}

impl EngineInner {
    /// Broadcasts an event to subscribers and, if the sidecar attached one,
    /// writes it to stdout.
    fn deliver(&self, envelope: Value) {
        let params = envelope.get("params").cloned().unwrap_or(Value::Null);
        // A send error only means "no subscribers", which is fine.
        let _ = self.events.send(params);
        if let Some(outbox) = self.outbox.lock().expect("outbox lock poisoned").as_ref() {
            let _ = outbox.send(envelope);
        }
    }

    /// Emits one event. While a request is in flight the event is queued so the
    /// response is written first (mirrors the Node reference's `emit`).
    pub(crate) fn emit(&self, event_type: &str, payload: Value) {
        let envelope = json!({
            "jsonrpc": "2.0",
            "method": "event",
            "params": { "type": event_type, "payload": payload },
        });
        {
            let mut queue = self.queue.lock().expect("event queue lock poisoned");
            if let Some(list) = queue.as_mut() {
                list.push(envelope);
                return;
            }
        }
        self.deliver(envelope);
    }

    /// Emits `session.status` unconditionally (the Node `emitStatus`).
    pub(crate) fn emit_status(
        &self,
        shared: &SessionShared,
        status: Status,
        reason: Option<String>,
    ) {
        let mut payload = json!({
            "sessionId": shared.session_id,
            "status": status.as_str(),
        });
        if let Some(reason) = reason {
            payload["reason"] = Value::String(reason);
        }
        self.emit("session.status", payload);
    }

    /// Updates the stored status and emits `session.status`, skipping a no-op
    /// repeat unless a `reason` is given (the Node `setStatus`).
    pub(crate) fn set_status(
        &self,
        shared: &SessionShared,
        status: Status,
        reason: Option<String>,
    ) {
        let changed = {
            let mut current = shared.status.lock().expect("session status lock poisoned");
            let changed = *current != status;
            *current = status;
            changed
        };
        if !changed && reason.is_none() {
            return;
        }
        self.emit_status(shared, status, reason);
    }
}

/// The engine. Cheap to clone; every clone shares the same sessions.
#[derive(Clone)]
pub struct EngineCore {
    inner: Arc<EngineInner>,
}

impl EngineCore {
    /// Creates an engine whose per-session state lives under `data_dir`.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        let (events, _) = broadcast::channel(1024);
        Self {
            inner: Arc::new(EngineInner {
                data_dir: data_dir.into(),
                sessions: Mutex::new(Vec::new()),
                events,
                queue: Mutex::new(None),
                pending: Mutex::new(Vec::new()),
                outbox: Mutex::new(None),
                request_lock: AsyncMutex::new(()),
                shutting_down: AtomicBool::new(false),
            }),
        }
    }

    /// Subscribes to engine events. Each item is the `{type, payload}` object
    /// (the `params` of the stdout event line), matching what the gateway's
    /// `consume_events` reads.
    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.inner.events.subscribe()
    }

    /// Installs (or clears) the stdout sink the sidecar drains. Passing `None`
    /// leaves the engine broadcast-only, which is what tests use.
    pub fn set_outbox(&self, outbox: Option<mpsc::UnboundedSender<Value>>) {
        *self.inner.outbox.lock().expect("outbox lock poisoned") = outbox;
    }

    /// Publishes any events produced by the request that just finished. Call
    /// this after writing the response.
    pub fn flush_events(&self) {
        let pending: Vec<Value> = {
            let mut pending = self.inner.pending.lock().expect("pending lock poisoned");
            std::mem::take(&mut *pending)
        };
        for envelope in pending {
            self.inner.deliver(envelope);
        }
    }

    /// Dispatches one request. `params` may be `Value::Null` when absent.
    ///
    /// See the module docs for the queue/pending contract: the caller must call
    /// [`EngineCore::flush_events`] afterwards to release the events this
    /// request triggered.
    pub async fn request(&self, method: &str, params: Value) -> Result<Value, String> {
        let _guard = self.inner.request_lock.lock().await;
        *self.inner.queue.lock().expect("event queue lock poisoned") = Some(Vec::new());
        let outcome = self.dispatch(method, &params).await;
        let queued = self
            .inner
            .queue
            .lock()
            .expect("event queue lock poisoned")
            .take()
            .unwrap_or_default();
        self.inner
            .pending
            .lock()
            .expect("pending lock poisoned")
            .extend(queued);
        outcome
    }

    /// Stops every running session. Idempotent.
    pub async fn shutdown(&self) {
        if self.inner.shutting_down.swap(true, Ordering::SeqCst) {
            return;
        }
        let sessions: Vec<Arc<Session>> = self
            .inner
            .sessions
            .lock()
            .expect("sessions lock poisoned")
            .clone();
        for session in sessions {
            let running = session
                .running
                .lock()
                .expect("running lock poisoned")
                .take();
            if let Some(running) = running {
                session::shutdown(running).await;
            }
        }
    }

    async fn dispatch(&self, method: &str, params: &Value) -> Result<Value, String> {
        match method {
            "ping" => Ok(json!({ "pong": true, "version": crate::VERSION })),
            "session.create" => self.create(params),
            "session.list" => Ok(Value::Array(
                self.lock_sessions()
                    .iter()
                    .map(|session| session_info(&session.shared))
                    .collect(),
            )),
            "session.get" => Ok(session_info(&self.get_session(params)?.shared)),
            "session.start" => self.start(params).await,
            "session.stop" => self.stop(params).await,
            "session.logout" => self.logout(params).await,
            "session.delete" => self.delete(params).await,
            "session.qr" => {
                let session = self.get_session(params)?;
                Ok(json!({ "qr": session.shared.qr() }))
            }
            "message.sendText" => self.send_text(params).await,
            "message.sendMedia" => self.send_media(params).await,
            "message.sendReaction" => self.send_reaction(params).await,
            "message.sendBulk" => self.send_bulk(params).await,
            "message.sendLocation" => self.send_location(params).await,
            "message.sendContact" => self.send_contact(params).await,
            "message.forward" => self.forward_message(params).await,
            "message.pin" => self.pin_message(params).await,
            "message.edit" => self.edit_message(params).await,
            "message.delete" => self.delete_message(params).await,
            "message.star" => self.star_message(params).await,
            "contact.check" => self.contact_check(params).await,
            "contact.update" => self.contact_update(params).await,
            "contact.profilePic" => self.contact_profile_pic(params).await,
            "contact.info" => self.contact_info(params).await,
            "group.list" => self.group_list(params).await,
            "group.get" => self.group_get(params).await,
            "group.create" => self.group_create(params).await,
            "group.leave" => self.group_leave(params).await,
            "group.setSubject" => self.group_set_subject(params).await,
            "group.setDescription" => self.group_set_description(params).await,
            "group.addParticipants" => self.group_add_participants(params).await,
            "group.removeParticipants" => self.group_remove_participants(params).await,
            "group.promote" => self.group_promote(params).await,
            "group.demote" => self.group_demote(params).await,
            "group.inviteLink" => self.group_invite_link(params).await,
            "group.settings" => self.group_settings(params).await,
            "group.join" => self.group_join(params).await,
            "group.membershipRequests" => self.group_membership_requests(params).await,
            "group.setProfilePicture" => self.group_set_profile_picture(params).await,
            "group.removeProfilePicture" => self.group_remove_profile_picture(params).await,
            "profile.get" => self.profile_get(params).await,
            "profile.setName" => self.profile_set_name(params).await,
            "profile.setAbout" => self.profile_set_about(params).await,
            "profile.setPicture" => self.profile_set_picture(params).await,
            "profile.removePicture" => self.profile_remove_picture(params).await,
            "label.list" => self.label_list(params).await,
            "label.create" => self.label_create(params).await,
            "label.delete" => self.label_delete(params).await,
            "label.addChat" => self.label_add_chat(params).await,
            "label.removeChat" => self.label_remove_chat(params).await,
            "chat.markRead" => self.chat_mark_read(params).await,
            "chat.archive" => self.chat_archive(params).await,
            "chat.unarchive" => self.chat_unarchive(params).await,
            "chat.pin" => self.chat_pin(params).await,
            "chat.unpin" => self.chat_unpin(params).await,
            "chat.mute" => self.chat_mute(params).await,
            "chat.unmute" => self.chat_unmute(params).await,
            "chat.delete" => self.chat_delete(params).await,
            "chat.clear" => self.chat_clear(params).await,
            "presence.subscribe" => self.presence_subscribe(params).await,
            "presence.set" => self.presence_set(params).await,
            "status.sendText" => self.status_send_text(params).await,
            "contact.block" => self.contact_block(params).await,
            "contact.unblock" => self.contact_unblock(params).await,
            "newsletter.list" => self.newsletter_list(params).await,
            "newsletter.get" => self.newsletter_get(params).await,
            "newsletter.follow" => self.newsletter_follow(params).await,
            "newsletter.leave" => self.newsletter_leave(params).await,
            "poll.create" => self.poll_create(params).await,
            "poll.vote" => self.poll_vote(params).await,
            "calls.reject" => self.calls_reject(params).await,
            other => Err(format!("unknown method: {other}")),
        }
    }

    fn lock_sessions(&self) -> std::sync::MutexGuard<'_, Vec<Arc<Session>>> {
        self.inner.sessions.lock().expect("sessions lock poisoned")
    }

    fn get_session(&self, params: &Value) -> Result<Arc<Session>, String> {
        if let Some(session_id) = params.get("sessionId").and_then(Value::as_str) {
            if let Some(session) = self
                .lock_sessions()
                .iter()
                .find(|session| session.shared.session_id == session_id)
            {
                return Ok(session.clone());
            }
        }
        Err("session not found".to_string())
    }

    fn require_client(&self, session: &Arc<Session>) -> Result<Arc<Client>, String> {
        let client = session
            .running
            .lock()
            .expect("running lock poisoned")
            .as_ref()
            .map(|running| running.client.clone());
        match client {
            Some(client)
                if session.shared.status() == Status::Connected && client.is_connected() =>
            {
                Ok(client)
            }
            _ => Err("session is not connected".to_string()),
        }
    }

    /// Tears down any live runtime and clears the cached QR.
    async fn stop_runtime(&self, session: &Arc<Session>) {
        let running = session
            .running
            .lock()
            .expect("running lock poisoned")
            .take();
        if let Some(running) = running {
            session::shutdown(running).await;
        }
        session.shared.set_qr(None);
    }

    fn create(&self, params: &Value) -> Result<Value, String> {
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_string);
        let session_id = uuid::Uuid::new_v4().to_string();
        let name = name.unwrap_or_else(|| session_id.clone());
        let shared = Arc::new(SessionShared {
            dir: self.inner.data_dir.join(&session_id),
            session_id,
            name,
            status: Mutex::new(Status::Created),
            qr: Mutex::new(None),
        });
        let session = Arc::new(Session {
            shared: shared.clone(),
            running: Mutex::new(None),
        });
        self.lock_sessions().push(session);
        self.inner.emit_status(&shared, Status::Created, None);
        Ok(session_info(&shared))
    }

    async fn start(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;

        let already_live = {
            let running = session.running.lock().expect("running lock poisoned");
            running.is_some() && session.shared.status() != Status::LoggedOut
        };
        if already_live {
            return Ok(json!({
                "sessionId": session.shared.session_id,
                "status": session.shared.status().as_str(),
            }));
        }

        self.stop_runtime(&session).await;

        match session::start_runtime(self.inner.clone(), session.clone()).await {
            Ok(running) => {
                *session.running.lock().expect("running lock poisoned") = Some(running);
                Ok(json!({
                    "sessionId": session.shared.session_id,
                    "status": Status::Connecting.as_str(),
                }))
            }
            Err(error) => {
                self.inner
                    .set_status(&session.shared, Status::Error, Some(error.clone()));
                Err(error)
            }
        }
    }

    async fn stop(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        self.stop_runtime(&session).await;
        self.inner
            .set_status(&session.shared, Status::Disconnected, None);
        Ok(json!({
            "sessionId": session.shared.session_id,
            "status": Status::Disconnected.as_str(),
        }))
    }

    async fn logout(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = session
            .running
            .lock()
            .expect("running lock poisoned")
            .as_ref()
            .map(|running| running.client.clone());
        if let Some(client) = client {
            // Deregisters the companion device, dispatches `LoggedOut` (which
            // our handler turns into `logged_out`) and disconnects. Auto
            // reconnect is disabled inside the library, but set it here too in
            // case the logout IQ fails before that point.
            client.enable_auto_reconnect.store(false, Ordering::Relaxed);
            client.logout().await;
        }
        self.stop_runtime(&session).await;
        self.inner
            .set_status(&session.shared, Status::LoggedOut, None);
        Ok(json!({
            "sessionId": session.shared.session_id,
            "status": Status::LoggedOut.as_str(),
        }))
    }

    async fn delete(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        self.stop_runtime(&session).await;
        self.lock_sessions()
            .retain(|candidate| !Arc::ptr_eq(candidate, &session));
        if let Err(error) = tokio::fs::remove_dir_all(&session.shared.dir).await {
            if error.kind() != std::io::ErrorKind::NotFound {
                tracing::debug!(%error, "could not remove session directory");
            }
        }
        Ok(json!({ "deleted": true }))
    }

    async fn send_text(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        let text = params
            .get("text")
            .and_then(Value::as_str)
            .ok_or_else(|| "text is required".to_string())?;
        let message_id = session::send_text(&client, chat_id, text).await?;
        Ok(json!({ "messageId": message_id, "chatId": chat_id }))
    }

    async fn send_media(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        let message_id = session::send_media(&client, params).await?;
        Ok(json!({ "messageId": message_id, "chatId": chat_id }))
    }

    async fn send_reaction(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        let message_id =
            string_param(params, "messageId").ok_or_else(|| "messageId is required".to_string())?;
        let emoji = params.get("emoji").and_then(Value::as_str).unwrap_or("");
        let sender = params.get("sender").and_then(Value::as_str);
        let mid = session::send_reaction(&client, chat_id, message_id, sender, emoji).await?;
        Ok(json!({ "messageId": mid, "chatId": chat_id }))
    }

    async fn edit_message(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        let message_id =
            string_param(params, "messageId").ok_or_else(|| "messageId is required".to_string())?;
        let text = params.get("text").and_then(Value::as_str).unwrap_or("");
        let mid = session::edit_message(&client, chat_id, message_id, text).await?;
        Ok(json!({ "messageId": mid, "chatId": chat_id }))
    }

    async fn delete_message(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        let message_id =
            string_param(params, "messageId").ok_or_else(|| "messageId is required".to_string())?;
        session::delete_message(&client, chat_id, message_id).await?;
        Ok(json!({ "deleted": true, "chatId": chat_id }))
    }

    async fn star_message(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        let message_id =
            string_param(params, "messageId").ok_or_else(|| "messageId is required".to_string())?;
        let star = params.get("star").and_then(Value::as_bool).unwrap_or(true);
        session::star_message(&client, chat_id, message_id, star).await?;
        Ok(json!({ "starred": star, "chatId": chat_id }))
    }

    async fn send_bulk(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let messages = params
            .get("messages")
            .and_then(Value::as_array)
            .ok_or_else(|| "messages is required".to_string())?;
        let mut parsed: Vec<(String, String)> = Vec::new();
        for message in messages {
            let chat_id = message
                .get("chatId")
                .and_then(Value::as_str)
                .ok_or_else(|| "chatId is required".to_string())?
                .to_string();
            let text = message
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            parsed.push((chat_id, text));
        }
        let sent = session::send_bulk(&client, &parsed).await?;
        Ok(json!({ "sent": sent }))
    }

    async fn send_location(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        let latitude = params
            .get("latitude")
            .and_then(Value::as_f64)
            .ok_or_else(|| "latitude is required".to_string())?;
        let longitude = params
            .get("longitude")
            .and_then(Value::as_f64)
            .ok_or_else(|| "longitude is required".to_string())?;
        let name = params.get("name").and_then(Value::as_str);
        let address = params.get("address").and_then(Value::as_str);
        let message_id =
            session::send_location(&client, chat_id, latitude, longitude, name, address).await?;
        Ok(json!({ "messageId": message_id, "chatId": chat_id }))
    }

    async fn send_contact(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        let contact = params.get("contact").cloned().unwrap_or(Value::Null);
        let message_id = session::send_contact(&client, chat_id, &contact).await?;
        Ok(json!({ "messageId": message_id, "chatId": chat_id }))
    }

    async fn forward_message(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        let message_id =
            string_param(params, "messageId").ok_or_else(|| "messageId is required".to_string())?;
        let to_chat_id =
            string_param(params, "toChatId").ok_or_else(|| "toChatId is required".to_string())?;
        // Stub: engine has no message store, so no real forwarding is performed.
        session::forward_message(&client, chat_id, message_id, to_chat_id).await?;
        Ok(json!({ "forwarded": true, "messageId": null }))
    }

    async fn pin_message(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        let message_id =
            string_param(params, "messageId").ok_or_else(|| "messageId is required".to_string())?;
        let pin = params.get("pin").and_then(Value::as_bool).unwrap_or(true);
        session::pin_message(&client, chat_id, message_id, pin).await?;
        Ok(json!({ "pinned": pin }))
    }

    async fn contact_update(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let contact_id =
            string_param(params, "contactId").ok_or_else(|| "contactId is required".to_string())?;
        let name = params.get("name").and_then(Value::as_str);
        session::contact_update(&client, contact_id, name).await?;
        Ok(json!({ "updated": true }))
    }

    async fn profile_set_picture(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let base64 = params
            .get("base64")
            .and_then(Value::as_str)
            .ok_or_else(|| "base64 is required".to_string())?;
        let mimetype = params.get("mimetype").and_then(Value::as_str);
        session::profile_set_picture(&client, base64, mimetype).await?;
        Ok(json!({ "updated": true }))
    }

    async fn profile_remove_picture(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        session::profile_remove_picture(&client).await?;
        Ok(json!({ "updated": true }))
    }

    async fn group_add_participants(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let group_id =
            string_param(params, "groupId").ok_or_else(|| "groupId is required".to_string())?;
        let participants: Vec<String> = params
            .get("participants")
            .and_then(Value::as_array)
            .map(|array| {
                array
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        session::group_add_participants(&client, group_id, &participants).await?;
        Ok(json!({ "added": true }))
    }

    async fn group_remove_participants(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let group_id =
            string_param(params, "groupId").ok_or_else(|| "groupId is required".to_string())?;
        let participants: Vec<String> = params
            .get("participants")
            .and_then(Value::as_array)
            .map(|array| {
                array
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        session::group_remove_participants(&client, group_id, &participants).await?;
        Ok(json!({ "removed": true }))
    }

    async fn group_promote(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let group_id =
            string_param(params, "groupId").ok_or_else(|| "groupId is required".to_string())?;
        let participants: Vec<String> = params
            .get("participants")
            .and_then(Value::as_array)
            .map(|array| {
                array
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        session::group_promote(&client, group_id, &participants).await?;
        Ok(json!({ "promoted": true }))
    }

    async fn group_demote(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let group_id =
            string_param(params, "groupId").ok_or_else(|| "groupId is required".to_string())?;
        let participants: Vec<String> = params
            .get("participants")
            .and_then(Value::as_array)
            .map(|array| {
                array
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        session::group_demote(&client, group_id, &participants).await?;
        Ok(json!({ "demoted": true }))
    }

    async fn group_invite_link(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let group_id =
            string_param(params, "groupId").ok_or_else(|| "groupId is required".to_string())?;
        let link = session::group_invite_link(&client, group_id).await?;
        Ok(json!({ "link": link }))
    }

    async fn group_settings(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let group_id =
            string_param(params, "groupId").ok_or_else(|| "groupId is required".to_string())?;
        let settings = session::group_settings(&client, group_id).await?;
        Ok(json!({ "settings": settings }))
    }

    async fn group_join(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let invite_code = string_param(params, "inviteCode")
            .ok_or_else(|| "inviteCode is required".to_string())?;
        let group_id = session::group_join(&client, invite_code).await?;
        Ok(json!({ "groupId": group_id }))
    }

    async fn group_membership_requests(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let group_id =
            string_param(params, "groupId").ok_or_else(|| "groupId is required".to_string())?;
        let requests = session::group_membership_requests(&client, group_id).await?;
        Ok(json!({ "requests": requests }))
    }

    async fn group_set_profile_picture(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let group_id =
            string_param(params, "groupId").ok_or_else(|| "groupId is required".to_string())?;
        let base64 = params
            .get("base64")
            .and_then(Value::as_str)
            .ok_or_else(|| "base64 is required".to_string())?;
        let mimetype = params.get("mimetype").and_then(Value::as_str);
        session::group_set_profile_picture(&client, group_id, base64, mimetype).await?;
        Ok(json!({ "updated": true }))
    }

    async fn group_remove_profile_picture(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let group_id =
            string_param(params, "groupId").ok_or_else(|| "groupId is required".to_string())?;
        session::group_remove_profile_picture(&client, group_id).await?;
        Ok(json!({ "updated": true }))
    }

    async fn calls_reject(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let call_id =
            string_param(params, "callId").ok_or_else(|| "callId is required".to_string())?;
        session::calls_reject(&client, call_id).await?;
        Ok(json!({ "rejected": true }))
    }

    async fn contact_check(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let number = params
            .get("number")
            .and_then(Value::as_str)
            .or_else(|| params.get("phone").and_then(Value::as_str))
            .ok_or_else(|| "number is required".to_string())?;
        session::contact_check(&client, number).await
    }

    async fn contact_profile_pic(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let contact_id =
            string_param(params, "contactId").ok_or_else(|| "contactId is required".to_string())?;
        session::contact_profile_pic(&client, contact_id).await
    }

    async fn contact_info(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let contact_id =
            string_param(params, "contactId").ok_or_else(|| "contactId is required".to_string())?;
        session::contact_info(&client, contact_id).await
    }

    async fn group_list(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let groups = session::group_list(&client).await?;
        Ok(json!({ "groups": groups }))
    }

    async fn group_get(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let group_id =
            string_param(params, "groupId").ok_or_else(|| "groupId is required".to_string())?;
        session::group_get(&client, group_id).await
    }

    async fn group_create(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let subject = params
            .get("subject")
            .and_then(Value::as_str)
            .unwrap_or("New Group");
        let participants: Vec<String> = params
            .get("participants")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        session::group_create(&client, subject, &participants).await
    }

    async fn group_leave(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let group_id =
            string_param(params, "groupId").ok_or_else(|| "groupId is required".to_string())?;
        session::group_leave(&client, group_id).await?;
        Ok(json!({ "left": true }))
    }

    async fn group_set_subject(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let group_id =
            string_param(params, "groupId").ok_or_else(|| "groupId is required".to_string())?;
        let subject =
            string_param(params, "subject").ok_or_else(|| "subject is required".to_string())?;
        session::group_set_subject(&client, group_id, subject).await?;
        Ok(json!({ "updated": true }))
    }

    async fn group_set_description(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let group_id =
            string_param(params, "groupId").ok_or_else(|| "groupId is required".to_string())?;
        let description = params
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("");
        session::group_set_description(&client, group_id, description).await?;
        Ok(json!({ "updated": true }))
    }

    async fn profile_get(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let name = session.shared.name.clone();
        let status = session.shared.status().as_str().to_string();
        Ok(json!({ "name": name, "status": status }))
    }

    async fn profile_set_name(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let name = string_param(params, "name").ok_or_else(|| "name is required".to_string())?;
        session::profile_set_name(&client, name).await?;
        Ok(json!({ "updated": true }))
    }

    async fn profile_set_about(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let about = params.get("about").and_then(Value::as_str).unwrap_or("");
        session::profile_set_about(&client, about).await?;
        Ok(json!({ "updated": true }))
    }

    async fn label_list(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let labels = session::label_list(&client).await?;
        Ok(json!({ "labels": labels }))
    }

    async fn label_create(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let name = string_param(params, "name").ok_or_else(|| "name is required".to_string())?;
        let color = params.get("color").and_then(Value::as_i64).unwrap_or(0) as i32;
        let id = session::label_create(&client, name, color).await?;
        Ok(json!({ "id": id }))
    }

    async fn label_delete(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let label_id =
            string_param(params, "labelId").ok_or_else(|| "labelId is required".to_string())?;
        session::label_delete(&client, label_id).await?;
        Ok(json!({ "deleted": true }))
    }

    async fn label_add_chat(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let label_id =
            string_param(params, "labelId").ok_or_else(|| "labelId is required".to_string())?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        session::label_add_chat(&client, label_id, chat_id).await?;
        Ok(json!({ "added": true }))
    }

    async fn label_remove_chat(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let label_id =
            string_param(params, "labelId").ok_or_else(|| "labelId is required".to_string())?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        session::label_remove_chat(&client, label_id, chat_id).await?;
        Ok(json!({ "removed": true }))
    }

    async fn chat_mark_read(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        session::chat_mark_read(&client, chat_id).await?;
        Ok(json!({ "read": true }))
    }

    async fn chat_archive(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        session::chat_archive(&client, chat_id).await?;
        Ok(json!({ "archived": true }))
    }

    async fn chat_unarchive(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        session::chat_unarchive(&client, chat_id).await?;
        Ok(json!({ "archived": false }))
    }

    async fn chat_pin(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        session::chat_pin(&client, chat_id).await?;
        Ok(json!({ "pinned": true }))
    }

    async fn chat_unpin(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        session::chat_unpin(&client, chat_id).await?;
        Ok(json!({ "pinned": false }))
    }

    async fn chat_mute(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        session::chat_mute(&client, chat_id).await?;
        Ok(json!({ "muted": true }))
    }

    async fn chat_unmute(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        session::chat_unmute(&client, chat_id).await?;
        Ok(json!({ "muted": false }))
    }

    async fn chat_delete(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        session::chat_delete(&client, chat_id).await?;
        Ok(json!({ "deleted": true }))
    }

    async fn chat_clear(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        session::chat_clear(&client, chat_id).await?;
        Ok(json!({ "cleared": true }))
    }

    async fn presence_subscribe(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        session::presence_subscribe(&client, chat_id).await?;
        Ok(json!({ "subscribed": true }))
    }

    async fn presence_set(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let state = params
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("available");
        session::presence_set(&client, state).await?;
        Ok(json!({ "state": state }))
    }

    async fn status_send_text(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let text = params
            .get("text")
            .and_then(Value::as_str)
            .ok_or_else(|| "text is required".to_string())?;
        session::status_send_text(&client, text).await?;
        Ok(json!({ "sent": true }))
    }

    async fn contact_block(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let contact_id =
            string_param(params, "contactId").ok_or_else(|| "contactId is required".to_string())?;
        session::contact_block(&client, contact_id).await?;
        Ok(json!({ "blocked": true }))
    }

    async fn contact_unblock(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let contact_id =
            string_param(params, "contactId").ok_or_else(|| "contactId is required".to_string())?;
        session::contact_unblock(&client, contact_id).await?;
        Ok(json!({ "unblocked": true }))
    }

    async fn newsletter_list(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let list = session::newsletter_list(&client).await?;
        Ok(json!({ "newsletters": list }))
    }

    async fn newsletter_get(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let jid = string_param(params, "newsletterId")
            .ok_or_else(|| "newsletterId is required".to_string())?;
        let result = session::newsletter_get(&client, jid).await?;
        Ok(result)
    }

    async fn newsletter_follow(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let jid = string_param(params, "newsletterId")
            .ok_or_else(|| "newsletterId is required".to_string())?;
        session::newsletter_follow(&client, jid).await?;
        Ok(json!({ "followed": true }))
    }

    async fn newsletter_leave(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let jid = string_param(params, "newsletterId")
            .ok_or_else(|| "newsletterId is required".to_string())?;
        session::newsletter_leave(&client, jid).await?;
        Ok(json!({ "left": true }))
    }

    async fn poll_create(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        let name = params.get("name").and_then(Value::as_str).unwrap_or("Poll");
        let options: Vec<String> = params
            .get("options")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        let selectable = params
            .get("selectableCount")
            .and_then(Value::as_u64)
            .unwrap_or(1) as u32;
        let (mid, secret) =
            session::poll_create(&client, chat_id, name, &options, selectable).await?;
        Ok(json!({ "messageId": mid, "secret": hex::encode(secret) }))
    }

    async fn poll_vote(&self, params: &Value) -> Result<Value, String> {
        let session = self.get_session(params)?;
        let client = self.require_client(&session)?;
        let chat_id =
            string_param(params, "chatId").ok_or_else(|| "chatId is required".to_string())?;
        let poll_msg_id = string_param(params, "pollMessageId")
            .ok_or_else(|| "pollMessageId is required".to_string())?;
        let creator = string_param(params, "creatorJid")
            .ok_or_else(|| "creatorJid is required".to_string())?;
        let secret_hex =
            string_param(params, "secret").ok_or_else(|| "secret is required".to_string())?;
        let secret = hex::decode(secret_hex).map_err(|_| "invalid secret".to_string())?;
        let chosen: Vec<String> = params
            .get("options")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        session::poll_vote(&client, chat_id, poll_msg_id, creator, &secret, &chosen).await?;
        Ok(json!({ "voted": true }))
    }
}

fn session_info(shared: &SessionShared) -> Value {
    json!({
        "sessionId": shared.session_id,
        "name": shared.name,
        "status": shared.status().as_str(),
    })
}

fn string_param<'a>(params: &'a Value, key: &str) -> Option<&'a str> {
    params
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
}
