//! Per-session runtime: bot construction, event bridging and outbound sends.
//!
//! One [`Running`] owns a spawned `whatsapp_rust` bot plus the file-backed store
//! and the periodic flush task. [`start_runtime`] builds it (network-free, so a
//! failed build never touches the network) and hands it to the engine;
//! [`shutdown`] stops it cleanly and persists a final snapshot.
//!
//! The event closure captures a `Weak<EngineInner>` and the session's
//! `Arc<SessionShared>` — never a strong `Arc<EngineInner>` or `Arc<Session>` —
//! so the library's handler does not keep the engine or the session alive.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::task::JoinHandle;
use tracing::{debug, warn};
use whatsapp_rust::prelude::*;
use whatsapp_rust::store::Backend;
use whatsapp_rust::waproto::whatsapp as wa;
use whatsapp_rust::UploadOptions;

use crate::core::{EngineInner, Session, SessionShared, Status};
use crate::media::{self, MediaKind};
use crate::store::FileBackend;

/// Events the engine cares about; the bus skips materializing the rest.
// `EventKind::Call` / `Event::Call` do not exist in whatsapp-rust 0.7; the
// closest equivalent is `IncomingCall`, which we map onto `call.received`.
const EVENT_INTEREST: [EventKind; 6] = [
    EventKind::PairingQrCode,
    EventKind::Connected,
    EventKind::Disconnected,
    EventKind::LoggedOut,
    EventKind::Messages,
    EventKind::IncomingCall,
];

/// How often the store snapshot is persisted while a session runs.
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
/// Ordered-delivery mailbox capacity. Events are dropped past this bound, but
/// the store remains authoritative and the server redelivers; 256 is far above
/// the steady-state rate of the three kinds we consume.
const EVENT_MAILBOX: usize = 256;

/// A live session: the bot handle, its client and store, and the flush task.
pub(crate) struct Running {
    pub(crate) client: Arc<Client>,
    pub(crate) backend: Arc<FileBackend>,
    handle: BotHandle,
    flush_task: JoinHandle<()>,
}

/// Builds and spawns the client for one session.
///
/// `Bot::build` only wires caches and background loops; the connection is opened
/// by `spawn`, so a build failure never reaches WhatsApp. On success the session
/// status is moved to `connecting` before returning, with no `await` between
/// `spawn` and the status write, so no early QR event can be overtaken.
pub(crate) async fn start_runtime(
    inner: Arc<EngineInner>,
    session: Arc<Session>,
) -> Result<Running, String> {
    let shared = session.shared.clone();
    tokio::fs::create_dir_all(&shared.dir)
        .await
        .map_err(|error| format!("could not create session directory: {error}"))?;
    let backend = FileBackend::open(&shared.dir)
        .await
        .map_err(|error| format!("could not open session store: {error}"))?;

    let weak: Weak<EngineInner> = Arc::downgrade(&inner);
    let event_shared = shared.clone();
    let backend_dyn: Arc<dyn Backend> = backend.clone();

    let bot = Bot::builder()
        .with_backend_arc(backend_dyn)
        .with_event_delivery(EventDelivery::Ordered {
            capacity: EVENT_MAILBOX,
        })
        .skip_history_sync()
        .with_push_name(shared.name.clone())
        .on_event_for(&EVENT_INTEREST, move |event, _client| {
            let weak = weak.clone();
            let shared = event_shared.clone();
            async move {
                if let Some(inner) = weak.upgrade() {
                    handle_event(&inner, &shared, event.as_ref()).await;
                }
            }
        })
        .build()
        .await
        .map_err(|error| format!("could not build session client: {error}"))?;

    let client = bot.client();
    client.enable_auto_reconnect.store(true, Ordering::Relaxed);
    let handle = bot.spawn();

    inner.set_status(&shared, Status::Connecting, None);

    let flush_backend = backend.clone();
    let flush_task = tokio::spawn(async move {
        loop {
            tokio::time::sleep(FLUSH_INTERVAL).await;
            if let Err(error) = flush_backend.flush().await {
                warn!(%error, "could not persist session state");
                break;
            }
        }
    });

    Ok(Running {
        client,
        backend,
        handle,
        flush_task,
    })
}

/// Stops a running session and persists a final snapshot.
pub(crate) async fn shutdown(running: Running) {
    running.flush_task.abort();
    running.handle.shutdown().await;
    if let Err(error) = running.backend.flush().await {
        debug!(%error, "final session state flush failed");
    }
}

/// Sends a plain-text message; returns the engine message id.
pub(crate) async fn send_text(
    client: &Arc<Client>,
    chat_id: &str,
    text: &str,
) -> Result<String, String> {
    let to = parse_jid(chat_id)?;
    let result = client
        .send_text(to, text.to_string())
        .await
        .map_err(|error| error.to_string())?;
    Ok(result.message_id)
}

/// Uploads and sends a media message; returns the engine message id.
pub(crate) async fn send_media(client: &Arc<Client>, params: &Value) -> Result<String, String> {
    let media_type = params
        .get("mediaType")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let kind =
        MediaKind::parse(media_type).ok_or_else(|| format!("invalid mediaType: {media_type}"))?;
    let chat_id = params
        .get("chatId")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let to = parse_jid(chat_id)?;

    // Decode/download off the runtime's worker threads.
    let payload_params = params.clone();
    let data = tokio::task::spawn_blocking(move || media::resolve_payload(&payload_params))
        .await
        .map_err(|error| format!("media payload task failed: {error}"))??;

    let upload = client
        .upload(data, kind.media_type(), UploadOptions::new())
        .await
        .map_err(|error| error.to_string())?;
    let message = media::build(kind, upload, params);
    let result = client
        .send_message(to, message)
        .await
        .map_err(|error| error.to_string())?;
    Ok(result.message_id)
}

fn parse_jid(value: &str) -> Result<Jid, String> {
    value
        .parse::<Jid>()
        .map_err(|_| format!("invalid chatId: {value}"))
}

/// Bridges one library event onto the protocol.
async fn handle_event(inner: &EngineInner, shared: &Arc<SessionShared>, event: &Event) {
    match event {
        Event::PairingQrCode(qr) => {
            // The Node reference sets `qr_ready` and emits both events
            // unconditionally on every code, so a rotation re-emits.
            shared.set_qr(Some(qr.code.clone()));
            shared.set_status_value(Status::QrReady);
            inner.emit_status(shared, Status::QrReady, None);
            inner.emit(
                "session.qr",
                json!({ "sessionId": shared.session_id, "qr": qr.code }),
            );
        }
        Event::Connected(_) => {
            shared.set_qr(None);
            inner.set_status(shared, Status::Connected, None);
        }
        Event::Disconnected(disconnected) => {
            // Only dispatched for an *unexpected* transport end; an intentional
            // stop/logout is suppressed by the library, so this cannot clobber
            // `disconnected`/`logged_out` set by us.
            shared.set_qr(None);
            inner.set_status(
                shared,
                Status::Disconnected,
                Some(disconnected.reason.to_string()),
            );
        }
        Event::LoggedOut(logged_out) => {
            shared.set_qr(None);
            inner.set_status(
                shared,
                Status::LoggedOut,
                Some(format!("{:?}", logged_out.reason)),
            );
        }
        Event::Messages(batch) => {
            // Mirror the reference's `type === 'notify'`: offline backlog is not
            // replayed to the gateway.
            if matches!(batch.origin, BatchOrigin::Live) {
                for message in batch.messages.iter() {
                    emit_inbound(inner, shared, message);
                    // Reactions get their own dedicated event in addition to the
                    // generic `message.received` (kind = "reaction").
                    let base = message.message.get_base_message();
                    if base.reaction_message.is_set() {
                        emit_reaction(inner, shared, message);
                    }
                }
            }
        }
        Event::IncomingCall(call) => {
            // whatsapp-rust has no `Event::Call`; surface incoming calls as the
            // protocol-level `call.received` event.
            let call_id = call.action.call_id().to_string();
            let is_video = match &call.action {
                whatsapp_rust::wacore::types::call::CallAction::Offer { is_video, .. } => *is_video,
                whatsapp_rust::wacore::types::call::CallAction::OfferNotice {
                    is_video, ..
                } => *is_video,
                _ => false,
            };
            inner.emit(
                "call.received",
                json!({
                    "sessionId": shared.session_id,
                    "callId": call_id,
                    "from": call.from.to_string(),
                    "isVideo": is_video,
                }),
            );
        }
        _ => {}
    }
}

/// Emits `message.received` for one inbound message.
fn emit_inbound(inner: &EngineInner, shared: &SessionShared, message: &InboundMessage) {
    let info = &message.info;
    if info.source.is_from_me {
        // Deliberate deviation from the Node reference: the gateway persists
        // outbound messages from the `message.send*` response itself, so
        // re-emitting our own echo here would double-store it.
        return;
    }

    let base = message.message.get_base_message();
    let kind = classify(base);
    let text = base.text_content().unwrap_or_default();
    let has_media = matches!(kind, "image" | "video" | "audio" | "document" | "sticker");

    inner.emit(
        "message.received",
        json!({
            "sessionId": shared.session_id,
            "messageId": info.id,
            "chatId": info.source.chat.to_string(),
            "sender": info.source.sender.to_string(),
            "fromMe": info.source.is_from_me,
            "timestamp": info.timestamp.timestamp(),
            "text": text,
            "hasMedia": has_media,
            "kind": kind,
        }),
    );
}

/// Emits `message.reaction` for one inbound reaction message.
fn emit_reaction(inner: &EngineInner, shared: &SessionShared, message: &InboundMessage) {
    let info = &message.info;
    let base = message.message.get_base_message();
    let Some(reaction) = base.reaction_message.as_option() else {
        return;
    };
    let emoji = reaction.text.clone().unwrap_or_default();
    let target = reaction.key.as_option();
    let target_message_id = target.and_then(|key| key.id.clone()).unwrap_or_default();
    let target_participant = target.and_then(|key| key.participant.clone());
    let target_from_me = target.and_then(|key| key.from_me).unwrap_or(false);

    inner.emit(
        "message.reaction",
        json!({
            "sessionId": shared.session_id,
            "messageId": info.id,
            "chatId": info.source.chat.to_string(),
            "sender": info.source.sender.to_string(),
            "fromMe": info.source.is_from_me,
            "timestamp": info.timestamp.timestamp(),
            "emoji": emoji,
            "targetMessageId": target_message_id,
            "targetParticipant": target_participant,
            "targetFromMe": target_from_me,
        }),
    );
}

pub(crate) async fn contact_block(client: &Arc<Client>, contact_id: &str) -> Result<(), String> {
    let jid: Jid = contact_id
        .parse()
        .map_err(|_| format!("invalid contactId: {contact_id}"))?;
    client
        .blocking()
        .block(&jid)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn contact_unblock(client: &Arc<Client>, contact_id: &str) -> Result<(), String> {
    let jid: Jid = contact_id
        .parse()
        .map_err(|_| format!("invalid contactId: {contact_id}"))?;
    client
        .blocking()
        .unblock(&jid)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn newsletter_list(client: &Arc<Client>) -> Result<Vec<Value>, String> {
    let list = client
        .newsletter()
        .list_subscribed()
        .await
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for n in list {
        out.push(json!({ "id": n.jid.to_string(), "name": n.name, "description": n.description }));
    }
    Ok(out)
}

pub(crate) async fn newsletter_get(
    client: &Arc<Client>,
    newsletter_id: &str,
) -> Result<Value, String> {
    let jid: Jid = newsletter_id
        .parse()
        .map_err(|_| format!("invalid newsletterId: {newsletter_id}"))?;
    let meta = client
        .newsletter()
        .get_metadata(&jid)
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!({ "id": meta.jid.to_string(), "name": meta.name, "description": meta.description }))
}

pub(crate) async fn newsletter_follow(
    client: &Arc<Client>,
    newsletter_id: &str,
) -> Result<(), String> {
    let jid: Jid = newsletter_id
        .parse()
        .map_err(|_| format!("invalid newsletterId: {newsletter_id}"))?;
    client
        .newsletter()
        .join(&jid)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn newsletter_leave(
    client: &Arc<Client>,
    newsletter_id: &str,
) -> Result<(), String> {
    let jid: Jid = newsletter_id
        .parse()
        .map_err(|_| format!("invalid newsletterId: {newsletter_id}"))?;
    client
        .newsletter()
        .leave(&jid)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn poll_create(
    client: &Arc<Client>,
    chat_id: &str,
    name: &str,
    options: &[String],
    selectable_count: u32,
) -> Result<(String, Vec<u8>), String> {
    let to = parse_jid(chat_id)?;
    let (result, secret) = client
        .polls()
        .create(to, name, options, selectable_count)
        .await
        .map_err(|e| e.to_string())?;
    Ok((result.message_id, secret))
}

pub(crate) async fn poll_vote(
    client: &Arc<Client>,
    chat_id: &str,
    poll_msg_id: &str,
    creator_jid: &str,
    secret: &[u8],
    option_names: &[String],
) -> Result<(), String> {
    let chat_jid = parse_jid(chat_id)?;
    let creator: Jid = creator_jid
        .parse()
        .map_err(|_| format!("invalid creatorJid: {creator_jid}"))?;
    client
        .polls()
        .vote(chat_jid, poll_msg_id, &creator, secret, option_names)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

// ---------- additional protocol helpers ----------

pub(crate) async fn send_bulk(
    client: &Arc<Client>,
    messages: &[(String, String)],
) -> Result<u64, String> {
    let mut sent: u64 = 0;
    for (chat_id, text) in messages {
        send_text(client, chat_id, text).await?;
        sent += 1;
    }
    Ok(sent)
}

pub(crate) async fn send_location(
    client: &Arc<Client>,
    chat_id: &str,
    latitude: f64,
    longitude: f64,
    name: Option<&str>,
    address: Option<&str>,
) -> Result<String, String> {
    let to = parse_jid(chat_id)?;
    let msg = wa::Message {
        location_message: MessageField::some(wa::message::LocationMessage {
            degrees_latitude: Some(latitude),
            degrees_longitude: Some(longitude),
            name: name.map(str::to_string),
            address: address.map(str::to_string),
            ..Default::default()
        }),
        ..Default::default()
    };
    let result = client
        .send_message(to, msg)
        .await
        .map_err(|error| error.to_string())?;
    Ok(result.message_id)
}

/// Sends a contact card as a vCard text message; returns the engine message id.
pub(crate) async fn send_contact(
    client: &Arc<Client>,
    chat_id: &str,
    contact: &Value,
) -> Result<String, String> {
    let to = parse_jid(chat_id)?;
    let name = contact
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("Contact");
    let phone = contact
        .get("phone")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let vcard = format!("BEGIN:VCARD\nVERSION:3.0\nFN:{name}\nTEL:{phone}\nEND:VCARD");
    let result = client
        .send_text(to, vcard)
        .await
        .map_err(|error| error.to_string())?;
    Ok(result.message_id)
}

/// Stub: forwarding needs the original stored message payload, which this engine
/// does not keep. Returns `Ok(None)` so callers can report `messageId: null`.
pub(crate) async fn forward_message(
    _client: &Arc<Client>,
    _chat_id: &str,
    _message_id: &str,
    _to_chat_id: &str,
) -> Result<Option<String>, String> {
    // Stub: engine has no message store to replay the original payload.
    Ok(None)
}

/// Stub: message pinning is not exposed by the client API used here.
pub(crate) async fn pin_message(
    _client: &Arc<Client>,
    _chat_id: &str,
    _message_id: &str,
    _pin: bool,
) -> Result<(), String> {
    Ok(())
}

/// Best-effort contact rename via app-state sync.
pub(crate) async fn contact_update(
    client: &Arc<Client>,
    contact_id: &str,
    name: Option<&str>,
) -> Result<(), String> {
    let jid: Jid = contact_id
        .parse()
        .map_err(|_| format!("invalid contactId: {contact_id}"))?;
    client
        .chat_actions()
        .save_contact(&jid, name.map(str::to_string), None, true)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) async fn profile_set_picture(
    client: &Arc<Client>,
    base64: &str,
    _mimetype: Option<&str>,
) -> Result<(), String> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(base64)
        .map_err(|error| format!("invalid base64 payload: {error}"))?;
    client
        .profile()
        .set_profile_picture(bytes)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) async fn profile_remove_picture(client: &Arc<Client>) -> Result<(), String> {
    client
        .profile()
        .remove_profile_picture()
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) async fn group_add_participants(
    client: &Arc<Client>,
    group_id: &str,
    participants: &[String],
) -> Result<(), String> {
    let jid: Jid = group_id
        .parse()
        .map_err(|_| format!("invalid groupId: {group_id}"))?;
    let parsed: Vec<Jid> = participants
        .iter()
        .filter_map(|value| value.parse().ok())
        .collect();
    client
        .groups()
        .add_participants(jid, &parsed)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) async fn group_remove_participants(
    client: &Arc<Client>,
    group_id: &str,
    participants: &[String],
) -> Result<(), String> {
    let jid: Jid = group_id
        .parse()
        .map_err(|_| format!("invalid groupId: {group_id}"))?;
    let parsed: Vec<Jid> = participants
        .iter()
        .filter_map(|value| value.parse().ok())
        .collect();
    client
        .groups()
        .remove_participants(jid, &parsed)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) async fn group_promote(
    client: &Arc<Client>,
    group_id: &str,
    participants: &[String],
) -> Result<(), String> {
    let jid: Jid = group_id
        .parse()
        .map_err(|_| format!("invalid groupId: {group_id}"))?;
    let parsed: Vec<Jid> = participants
        .iter()
        .filter_map(|value| value.parse().ok())
        .collect();
    client
        .groups()
        .promote_participants(jid, &parsed)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) async fn group_demote(
    client: &Arc<Client>,
    group_id: &str,
    participants: &[String],
) -> Result<(), String> {
    let jid: Jid = group_id
        .parse()
        .map_err(|_| format!("invalid groupId: {group_id}"))?;
    let parsed: Vec<Jid> = participants
        .iter()
        .filter_map(|value| value.parse().ok())
        .collect();
    client
        .groups()
        .demote_participants(jid, &parsed)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) async fn group_invite_link(
    client: &Arc<Client>,
    group_id: &str,
) -> Result<String, String> {
    let jid: Jid = group_id
        .parse()
        .map_err(|_| format!("invalid groupId: {group_id}"))?;
    client
        .groups()
        .get_invite_link(jid, false)
        .await
        .map_err(|error| error.to_string())
}

pub(crate) async fn group_settings(client: &Arc<Client>, group_id: &str) -> Result<Value, String> {
    let jid: Jid = group_id
        .parse()
        .map_err(|_| format!("invalid groupId: {group_id}"))?;
    let meta = client
        .groups()
        .get_metadata(&jid)
        .await
        .map_err(|error| error.to_string())?;
    Ok(json!({
        "isLocked": meta.is_locked,
        "isAnnouncement": meta.is_announcement,
        "membershipApproval": meta.membership_approval,
        "ephemeral": meta.ephemeral.as_ref().and_then(|e| e.expiration),
    }))
}

pub(crate) async fn group_join(client: &Arc<Client>, invite_code: &str) -> Result<String, String> {
    let result = client
        .groups()
        .join_with_invite_code(invite_code)
        .await
        .map_err(|error| error.to_string())?;
    Ok(result.group_jid().to_string())
}

pub(crate) async fn group_membership_requests(
    client: &Arc<Client>,
    group_id: &str,
) -> Result<Vec<Value>, String> {
    let jid: Jid = group_id
        .parse()
        .map_err(|_| format!("invalid groupId: {group_id}"))?;
    let requests = client
        .groups()
        .get_membership_requests(jid)
        .await
        .map_err(|error| error.to_string())?;
    let mut out = Vec::new();
    for request in requests {
        out.push(json!({ "jid": request.jid.to_string(), "requestTime": request.request_time }));
    }
    Ok(out)
}

/// Set a group's profile picture (admin operation).
pub(crate) async fn group_set_profile_picture(
    client: &Arc<Client>,
    group_id: &str,
    base64: &str,
    _mimetype: Option<&str>,
) -> Result<(), String> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(base64)
        .map_err(|error| format!("invalid base64 payload: {error}"))?;
    let jid: Jid = group_id
        .parse()
        .map_err(|_| format!("invalid groupId: {group_id}"))?;
    client
        .groups()
        .set_profile_picture(jid, bytes)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// Remove a group's profile picture (admin operation).
pub(crate) async fn group_remove_profile_picture(
    client: &Arc<Client>,
    group_id: &str,
) -> Result<(), String> {
    let jid: Jid = group_id
        .parse()
        .map_err(|_| format!("invalid groupId: {group_id}"))?;
    client
        .groups()
        .remove_profile_picture(jid)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) async fn calls_reject(_client: &Arc<Client>, _call_id: &str) -> Result<(), String> {
    // Stub: call rejection is not exposed by the client API used here.
    Ok(())
}

// Normalized message kind. The Node reference compares Baileys' `*Message`
// names against a set of unprefixed names and so always reports
// `hasMedia: false`; we return the correct values instead.
// ---------- new parity helpers ----------

pub(crate) async fn send_reaction(
    client: &Arc<Client>,
    chat_id: &str,
    message_id: &str,
    sender: Option<&str>,
    emoji: &str,
) -> Result<String, String> {
    let to = parse_jid(chat_id)?;
    let key = wa::MessageKey {
        remote_jid: Some(chat_id.to_string()),
        id: Some(message_id.to_string()),
        participant: sender.map(str::to_string),
        ..Default::default()
    };
    let result = client
        .send_reaction(to, key, emoji)
        .await
        .map_err(|e| e.to_string())?;
    Ok(result.message_id)
}

pub(crate) async fn edit_message(
    client: &Arc<Client>,
    chat_id: &str,
    message_id: &str,
    text: &str,
) -> Result<String, String> {
    let to = parse_jid(chat_id)?;
    let msg = wa::Message {
        conversation: Some(text.to_string()),
        ..Default::default()
    };
    client
        .edit_message(to, message_id, msg)
        .await
        .map_err(|e| e.to_string())
}

pub(crate) async fn delete_message(
    client: &Arc<Client>,
    chat_id: &str,
    message_id: &str,
) -> Result<(), String> {
    let to = parse_jid(chat_id)?;
    client
        .revoke_message(to, message_id, whatsapp_rust::send::RevokeType::Sender)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn star_message(
    client: &Arc<Client>,
    chat_id: &str,
    message_id: &str,
    star: bool,
) -> Result<(), String> {
    let jid = parse_jid(chat_id)?;
    if star {
        client
            .chat_actions()
            .star_message(&jid, None, message_id, true)
            .await
            .map_err(|e| e.to_string())?;
    } else {
        client
            .chat_actions()
            .unstar_message(&jid, None, message_id, true)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

pub(crate) async fn contact_check(client: &Arc<Client>, number: &str) -> Result<Value, String> {
    let jid_str = if number.contains('@') {
        number.to_string()
    } else {
        format!("{number}@c.us")
    };
    let jid: Jid = jid_str
        .parse()
        .map_err(|_| format!("invalid number: {number}"))?;
    let results = client
        .contacts()
        .is_on_whatsapp(&[jid])
        .await
        .map_err(|e| e.to_string())?;
    let entry = results.into_iter().next();
    let (exists, whatsapp_id) = match entry {
        Some(r) => (r.is_registered, Some(r.jid.to_string())),
        None => (false, None),
    };
    Ok(json!({ "exists": exists, "whatsappId": whatsapp_id, "number": number }))
}

pub(crate) async fn contact_profile_pic(
    client: &Arc<Client>,
    contact_id: &str,
) -> Result<Value, String> {
    let jid: Jid = contact_id
        .parse()
        .map_err(|_| format!("invalid contactId: {contact_id}"))?;
    match client.contacts().get_profile_picture(&jid, false).await {
        Ok(Some(pic)) => Ok(json!({ "url": pic.url, "id": pic.id, "contactId": contact_id })),
        Ok(None) => Ok(json!({ "url": null, "contactId": contact_id })),
        Err(e) => Ok(json!({ "url": null, "contactId": contact_id, "error": e.to_string() })),
    }
}

pub(crate) async fn contact_info(client: &Arc<Client>, contact_id: &str) -> Result<Value, String> {
    let jid: Jid = contact_id
        .parse()
        .map_err(|_| format!("invalid contactId: {contact_id}"))?;
    match client.contacts().get_user_info(&[jid]).await {
        Ok(info) => {
            let entry = info.values().next();
            let v = match entry {
                Some(u) => {
                    json!({ "jid": u.jid.to_string(), "status": u.status, "pictureId": u.picture_id, "isBusiness": u.is_business })
                }
                None => Value::Null,
            };
            Ok(json!({ "contactId": contact_id, "info": v }))
        }
        Err(e) => Err(e.to_string()),
    }
}

pub(crate) async fn group_list(client: &Arc<Client>) -> Result<Vec<Value>, String> {
    let groups = client
        .groups()
        .get_participating()
        .await
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for (jid, meta) in groups {
        out.push(json!({ "id": jid.to_string(), "subject": meta.subject, "owner": meta.creator.map(|o| o.to_string()), "creation": meta.creation_time, "size": meta.participants.len() }));
    }
    Ok(out)
}

pub(crate) async fn group_get(client: &Arc<Client>, group_id: &str) -> Result<Value, String> {
    let jid: Jid = group_id
        .parse()
        .map_err(|_| format!("invalid groupId: {group_id}"))?;
    let meta = client
        .groups()
        .get_metadata(&jid)
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!({
        "id": group_id,
        "subject": meta.subject,
        "owner": meta.creator.map(|o| o.to_string()),
        "creation": meta.creation_time,
        "participants": meta.participants.iter().map(|p| json!({ "id": p.jid.to_string(), "admin": p.is_admin(), "superAdmin": p.is_super_admin() })).collect::<Vec<_>>(),
    }))
}

pub(crate) async fn group_create(
    client: &Arc<Client>,
    subject: &str,
    participants: &[String],
) -> Result<Value, String> {
    let mut options = whatsapp_rust::GroupCreateOptions::new(subject);
    let parsed: Vec<whatsapp_rust::GroupParticipantOptions> = participants
        .iter()
        .filter_map(|value| value.parse::<Jid>().ok())
        .map(whatsapp_rust::GroupParticipantOptions::new)
        .collect();
    options = options.with_participants(parsed);
    let result = client
        .groups()
        .create_group(options)
        .await
        .map_err(|error| error.to_string())?;
    Ok(json!({ "groupId": result.metadata.id.to_string(), "subject": subject }))
}

pub(crate) async fn group_leave(client: &Arc<Client>, group_id: &str) -> Result<(), String> {
    let jid: Jid = group_id
        .parse()
        .map_err(|_| format!("invalid groupId: {group_id}"))?;
    client
        .groups()
        .leave(jid)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn group_set_subject(
    client: &Arc<Client>,
    group_id: &str,
    subject: &str,
) -> Result<(), String> {
    let jid: Jid = group_id
        .parse()
        .map_err(|_| format!("invalid groupId: {group_id}"))?;
    let subj = whatsapp_rust::GroupSubject::new(subject).map_err(|e| e.to_string())?;
    client
        .groups()
        .set_subject(jid, subj)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn group_set_description(
    client: &Arc<Client>,
    group_id: &str,
    description: &str,
) -> Result<(), String> {
    let jid: Jid = group_id
        .parse()
        .map_err(|_| format!("invalid groupId: {group_id}"))?;
    let desc =
        whatsapp_rust::GroupDescription::new(description).map_err(|error| error.to_string())?;
    client
        .groups()
        .set_description(jid, Some(desc), whatsapp_rust::PreviousDescription::Resolve)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) async fn profile_set_name(client: &Arc<Client>, name: &str) -> Result<(), String> {
    client
        .profile()
        .set_push_name(name)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn profile_set_about(client: &Arc<Client>, about: &str) -> Result<(), String> {
    client
        .profile()
        .set_status_text(about)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn label_list(_client: &Arc<Client>) -> Result<Vec<Value>, String> {
    Ok(vec![])
}

pub(crate) async fn label_create(
    client: &Arc<Client>,
    name: &str,
    color: i32,
) -> Result<String, String> {
    let id = uuid::Uuid::new_v4().to_string();
    client
        .labels()
        .create_label(&id, name, color)
        .await
        .map_err(|e| e.to_string())?;
    Ok(id)
}

pub(crate) async fn label_delete(client: &Arc<Client>, label_id: &str) -> Result<(), String> {
    client
        .labels()
        .delete_label(label_id)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn label_add_chat(
    client: &Arc<Client>,
    label_id: &str,
    chat_id: &str,
) -> Result<(), String> {
    let jid: Jid = chat_id
        .parse()
        .map_err(|_| format!("invalid chatId: {chat_id}"))?;
    client
        .labels()
        .add_chat_label(label_id, &jid)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn label_remove_chat(
    client: &Arc<Client>,
    label_id: &str,
    chat_id: &str,
) -> Result<(), String> {
    let jid: Jid = chat_id
        .parse()
        .map_err(|_| format!("invalid chatId: {chat_id}"))?;
    client
        .labels()
        .remove_chat_label(label_id, &jid)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn chat_mark_read(client: &Arc<Client>, chat_id: &str) -> Result<(), String> {
    let jid: Jid = chat_id
        .parse()
        .map_err(|_| format!("invalid chatId: {chat_id}"))?;
    client
        .chat_actions()
        .mark_chat_as_read(&jid, true, None)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn chat_archive(client: &Arc<Client>, chat_id: &str) -> Result<(), String> {
    let jid: Jid = chat_id
        .parse()
        .map_err(|_| format!("invalid chatId: {chat_id}"))?;
    client
        .chat_actions()
        .archive_chat(&jid, None)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn chat_unarchive(client: &Arc<Client>, chat_id: &str) -> Result<(), String> {
    let jid: Jid = chat_id
        .parse()
        .map_err(|_| format!("invalid chatId: {chat_id}"))?;
    client
        .chat_actions()
        .unarchive_chat(&jid, None)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn chat_pin(client: &Arc<Client>, chat_id: &str) -> Result<(), String> {
    let jid: Jid = chat_id
        .parse()
        .map_err(|_| format!("invalid chatId: {chat_id}"))?;
    client
        .chat_actions()
        .pin_chat(&jid)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn chat_unpin(client: &Arc<Client>, chat_id: &str) -> Result<(), String> {
    let jid: Jid = chat_id
        .parse()
        .map_err(|_| format!("invalid chatId: {chat_id}"))?;
    client
        .chat_actions()
        .unpin_chat(&jid)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn chat_mute(client: &Arc<Client>, chat_id: &str) -> Result<(), String> {
    let jid: Jid = chat_id
        .parse()
        .map_err(|_| format!("invalid chatId: {chat_id}"))?;
    client
        .chat_actions()
        .mute_chat(&jid)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn chat_unmute(client: &Arc<Client>, chat_id: &str) -> Result<(), String> {
    let jid: Jid = chat_id
        .parse()
        .map_err(|_| format!("invalid chatId: {chat_id}"))?;
    client
        .chat_actions()
        .unmute_chat(&jid)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn chat_delete(client: &Arc<Client>, chat_id: &str) -> Result<(), String> {
    let jid: Jid = chat_id
        .parse()
        .map_err(|_| format!("invalid chatId: {chat_id}"))?;
    client
        .chat_actions()
        .delete_chat(&jid, false, None)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn chat_clear(client: &Arc<Client>, chat_id: &str) -> Result<(), String> {
    let jid: Jid = chat_id
        .parse()
        .map_err(|_| format!("invalid chatId: {chat_id}"))?;
    client
        .chat_actions()
        .clear_chat(&jid, false, false, None)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn presence_subscribe(client: &Arc<Client>, chat_id: &str) -> Result<(), String> {
    let jid: Jid = chat_id
        .parse()
        .map_err(|_| format!("invalid chatId: {chat_id}"))?;
    client
        .presence()
        .subscribe(jid)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn presence_set(client: &Arc<Client>, state: &str) -> Result<(), String> {
    use whatsapp_rust::features::PresenceStatus;
    let status = match state {
        "available" => PresenceStatus::Available,
        "unavailable" => PresenceStatus::Unavailable,
        _ => PresenceStatus::Available,
    };
    client
        .presence()
        .set(status)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) async fn status_send_text(client: &Arc<Client>, text: &str) -> Result<(), String> {
    let options = whatsapp_rust::StatusSendOptions {
        privacy: whatsapp_rust::StatusPrivacySetting::Contacts,
        message_id: None,
        extra_stanza_nodes: Vec::new(),
        device_freshness: whatsapp_rust::Freshness::default(),
    };
    client
        .status()
        .send_text(
            text,
            0xFF1E6E4F,
            wa::message::extended_text_message::FontType::SYSTEM,
            &[],
            options,
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn classify(base: &wa::Message) -> &'static str {
    if base.conversation.is_some() || base.extended_text_message.is_set() {
        "text"
    } else if base.image_message.is_set() {
        "image"
    } else if base.video_message.is_set() {
        "video"
    } else if base.audio_message.is_set() {
        "audio"
    } else if base.document_message.is_set() {
        "document"
    } else if base.sticker_message.is_set() {
        "sticker"
    } else if base.reaction_message.is_set() {
        "reaction"
    } else if base.protocol_message.is_set() {
        "protocol"
    } else {
        "unknown"
    }
}
