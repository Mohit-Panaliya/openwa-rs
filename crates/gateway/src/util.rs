//! Small helpers.

use rand::Rng;
use sha2::{Digest, Sha256};

/// Lowercase hex string of `bytes` cryptographically-random bytes.
pub fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::rng().fill(&mut buf[..]);
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

/// Lowercase hex sha256 of `input` — used to store API keys hashed.
pub fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}

/// First 8 characters of an API key, safe to show in listings.
pub fn key_prefix(key: &str) -> String {
    key.chars().take(8).collect()
}

/// Best-effort client IP from a request (connect info or proxy headers).
pub fn client_ip(request: &axum::extract::Request) -> Option<String> {
    if let Some(connect) = request
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
    {
        return Some(connect.0.ip().to_string());
    }
    if let Some(value) = request.headers().get("x-forwarded-for") {
        if let Ok(text) = value.to_str() {
            let first = text.split(',').next().unwrap_or("").trim();
            if !first.is_empty() {
                return Some(first.to_string());
            }
        }
    }
    if let Some(value) = request.headers().get("x-real-ip") {
        if let Ok(text) = value.to_str() {
            let text = text.trim();
            if !text.is_empty() {
                return Some(text.to_string());
            }
        }
    }
    None
}

/// Current unix time in milliseconds.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
