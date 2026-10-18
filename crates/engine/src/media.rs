//! Media handling for `message.sendMedia`.
//!
//! Two responsibilities:
//!
//! * turn the protocol's `base64`/`url` payload into plaintext bytes
//!   ([`resolve_payload`]), downloading `url` payloads over HTTP — the Node
//!   reference hands the URL straight to Baileys, but the Rust client uploads
//!   plaintext, so we fetch it ourselves;
//! * build the typed `wa::Message` from the resulting [`UploadResponse`]
//!   ([`build`]).
//!
//! `resolve_payload` does blocking work (base64 decode, `ureq` download) and is
//! therefore always called from `spawn_blocking`.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use serde_json::Value;
use whatsapp_rust::media::{self, AudioOptions, DocumentOptions, ImageOptions, VideoOptions};
use whatsapp_rust::upload::UploadResponse;
use whatsapp_rust::wacore::download::MediaType;
use whatsapp_rust::waproto::whatsapp as wa;

/// Cap on a downloaded payload. WhatsApp's CDN rejects media far below this, so
/// it only exists to bound a hostile `url`.
const MAX_MEDIA_BYTES: u64 = 256 * 1024 * 1024;

/// The `mediaType` values the protocol accepts.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum MediaKind {
    Image,
    Video,
    Audio,
    Document,
}

impl MediaKind {
    /// Parses the wire `mediaType`; `None` when it is not one of the four.
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "image" => Some(MediaKind::Image),
            "video" => Some(MediaKind::Video),
            "audio" => Some(MediaKind::Audio),
            "document" => Some(MediaKind::Document),
            _ => None,
        }
    }

    /// The CDN media type this kind uploads as.
    pub(crate) fn media_type(self) -> MediaType {
        match self {
            MediaKind::Image => MediaType::Image,
            MediaKind::Video => MediaType::Video,
            MediaKind::Audio => MediaType::Audio,
            MediaKind::Document => MediaType::Document,
        }
    }
}

/// Resolves the plaintext payload from `base64` (preferred) or `url`.
///
/// The error text contains `base64 or url` when neither is present, which the
/// gateway maps to a 400; the same wording the Node reference uses.
pub(crate) fn resolve_payload(params: &Value) -> Result<Vec<u8>, String> {
    if let Some(encoded) = params
        .get("base64")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    {
        return B64
            .decode(encoded)
            .map_err(|error| format!("invalid base64 payload: {error}"));
    }
    if let Some(url) = params
        .get("url")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    {
        return download(url);
    }
    Err("media payload required: base64 or url".to_string())
}

fn download(url: &str) -> Result<Vec<u8>, String> {
    let mut response = ureq::get(url)
        .call()
        .map_err(|error| format!("could not download media: {error}"))?;
    response
        .body_mut()
        .with_config()
        .limit(MAX_MEDIA_BYTES)
        .read_to_vec()
        .map_err(|error| format!("could not download media: {error}"))
}

/// Builds the ready-to-send message for an uploaded media payload, carrying the
/// protocol's `mimetype`, `caption` and `filename` through to the right
/// sub-message.
pub(crate) fn build(kind: MediaKind, upload: UploadResponse, params: &Value) -> wa::Message {
    let caption = params
        .get("caption")
        .and_then(Value::as_str)
        .map(str::to_string);
    let mimetype = params
        .get("mimetype")
        .and_then(Value::as_str)
        .map(str::to_string);
    let filename = params
        .get("filename")
        .and_then(Value::as_str)
        .map(str::to_string);

    match kind {
        MediaKind::Image => media::image_message(
            upload,
            ImageOptions {
                caption,
                mimetype,
                ..Default::default()
            },
        ),
        MediaKind::Video => media::video_message(
            upload,
            VideoOptions {
                caption,
                mimetype,
                ..Default::default()
            },
        ),
        MediaKind::Audio => media::audio_message(
            upload,
            AudioOptions {
                mimetype,
                ..Default::default()
            },
        ),
        MediaKind::Document => media::document_message(
            upload,
            DocumentOptions {
                caption,
                mimetype,
                file_name: filename,
                ..Default::default()
            },
        ),
    }
}
