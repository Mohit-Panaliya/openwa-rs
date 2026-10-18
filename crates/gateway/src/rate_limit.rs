//! Simple in-memory per-IP token bucket middleware.

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::SystemTime;

struct Bucket {
    max: u64,
    window_ms: u64,
    state: Mutex<HashMap<String, (u64, i64)>>,
}

impl Bucket {
    fn global() -> &'static Bucket {
        static INSTANCE: OnceLock<Bucket> = OnceLock::new();
        INSTANCE.get_or_init(|| {
            let max: u64 = std::env::var("RATE_LIMIT_MAX")
                .ok()
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(100);
            let window_ms: u64 = std::env::var("RATE_LIMIT_WINDOW_MS")
                .ok()
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(60_000);
            Bucket {
                max,
                window_ms,
                state: Mutex::new(HashMap::new()),
            }
        })
    }

    fn allow(&self, ip: &str) -> bool {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let mut map = self.state.lock().expect("rate limit mutex poisoned");
        let entry = map.entry(ip.to_string()).or_insert((0, now));
        if now - entry.1 >= self.window_ms as i64 {
            entry.0 = 0;
            entry.1 = now;
        }
        if entry.0 >= self.max {
            return false;
        }
        entry.0 += 1;
        true
    }
}

pub async fn rate_limit_middleware(
    request: Request,
    next: Next,
) -> Result<Response, axum::http::StatusCode> {
    let ip = crate::util::client_ip(&request).unwrap_or_else(|| "unknown".to_string());
    if !Bucket::global().allow(&ip) {
        return Err(axum::http::StatusCode::TOO_MANY_REQUESTS);
    }
    Ok(next.run(request).await)
}
