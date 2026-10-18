//! API-key authentication, roles, and session/chat scoping.
//!
//! Keys live in SQLite as sha256 hashes. `X-API-Key` or `?apiKey=` resolves to an
//! `Identity` that is attached to the request; route handlers enforce roles and
//! scopes through its helpers. See docs/PHASE1_5.md.

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::state::AppState;
use crate::util::sha256_hex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Viewer,
    Operator,
    Admin,
}

impl Role {
    pub fn parse(value: &str) -> Self {
        match value.to_ascii_lowercase().as_str() {
            "admin" => Role::Admin,
            "operator" => Role::Operator,
            _ => Role::Viewer,
        }
    }

    pub fn rank(self) -> u8 {
        match self {
            Role::Viewer => 1,
            Role::Operator => 2,
            Role::Admin => 3,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Role::Viewer => "viewer",
            Role::Operator => "operator",
            Role::Admin => "admin",
        }
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Identity {
    pub key_id: String,
    pub name: String,
    pub role: Role,
    pub allowed_sessions: Vec<String>,
    pub allowed_chats: Vec<String>,
    pub allowed_ips: Vec<String>,
}

impl Identity {
    pub fn is_admin(&self) -> bool {
        self.role == Role::Admin
    }

    pub fn require_role(&self, role: Role) -> Result<(), ApiError> {
        if self.role.rank() >= role.rank() {
            Ok(())
        } else {
            Err(ApiError::forbidden(format!(
                "role {} is required for this endpoint",
                role.as_str()
            )))
        }
    }

    pub fn allows_session(&self, session_id: &str) -> bool {
        self.is_admin()
            || self.allowed_sessions.is_empty()
            || self.allowed_sessions.iter().any(|id| id == session_id)
    }

    pub fn ensure_session(&self, session_id: &str) -> Result<(), ApiError> {
        if self.allows_session(session_id) {
            Ok(())
        } else {
            Err(ApiError::forbidden(
                "API key is not authorized for this session",
            ))
        }
    }

    pub fn allows_chat(&self, chat_id: &str) -> bool {
        self.is_admin()
            || self.allowed_chats.is_empty()
            || self.allowed_chats.iter().any(|id| id == chat_id)
    }

    pub fn ensure_chat(&self, chat_id: &str) -> Result<(), ApiError> {
        if self.allows_chat(chat_id) {
            Ok(())
        } else {
            Err(ApiError::forbidden(
                "API key is not authorized for this chat",
            ))
        }
    }

    /// Filters a list of session ids down to the ones this key may see.
    pub fn visible_sessions(&self, ids: impl IntoIterator<Item = String>) -> Vec<String> {
        ids.into_iter()
            .filter(|id| self.allows_session(id))
            .collect()
    }
}

pub async fn require_api_key(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let provided =
        api_key_from_request(&request).ok_or_else(|| ApiError::unauthorized("missing API key"))?;

    let record = state
        .db
        .find_key_by_hash(&sha256_hex(&provided))
        .await?
        .ok_or_else(|| ApiError::unauthorized("invalid API key"))?;

    let identity = Identity {
        key_id: record.id.clone(),
        name: record.name.clone(),
        role: Role::parse(&record.role),
        allowed_sessions: record.allowed_sessions.clone(),
        allowed_chats: record.allowed_chats.clone(),
        allowed_ips: record.allowed_ips.clone(),
    };

    // IP allow-list (CIDR). If configured, the source IP must match.
    if !record.allowed_ips.is_empty() {
        let source = crate::util::client_ip(&request);
        let allowed = source
            .as_deref()
            .map(|ip| {
                record
                    .allowed_ips
                    .iter()
                    .any(|entry| cidr_contains(entry, ip))
            })
            .unwrap_or(false);
        if !allowed {
            return Err(ApiError::forbidden("source IP is not allowed"));
        }
    }

    // Usage tracking must not slow the request down.
    let db = state.db.clone();
    let key_id = record.id;
    tokio::spawn(async move {
        let _ = db.touch_key(&key_id).await;
    });

    request.extensions_mut().insert(identity);
    Ok(next.run(request).await)
}

fn api_key_from_request(request: &Request) -> Option<String> {
    if let Some(value) = request
        .headers()
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
    {
        let value = value.trim();
        if !value.is_empty() {
            return Some(value.to_string());
        }
    }

    if let Some(value) = request
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
    {
        let value = value.trim();
        if let Some(token) = value.strip_prefix("Bearer ") {
            let token = token.trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        }
    }

    let query = request.uri().query()?;
    for pair in query.split('&') {
        let (name, value) = match pair.split_once('=') {
            Some(split) => split,
            None => continue,
        };
        if name.eq_ignore_ascii_case("apikey") || name.eq_ignore_ascii_case("api_key") {
            let value = value.trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

/// Returns true if `ip` is contained in `cidr`, where `cidr` may be a bare IP
/// or a CIDR range ("10.0.0.0/8", "2001:db8::/32").
pub fn cidr_contains(cidr: &str, ip: &str) -> bool {
    let ip: std::net::IpAddr = match ip.trim().parse() {
        Ok(value) => value,
        Err(_) => return false,
    };
    let cidr = cidr.trim();
    if cidr.is_empty() {
        return false;
    }
    if !cidr.contains('/') {
        return cidr
            .parse::<std::net::IpAddr>()
            .map(|net| net == ip)
            .unwrap_or(false);
    }
    let (net_str, prefix_str) = match cidr.split_once('/') {
        Some(split) => split,
        None => return false,
    };
    let net: std::net::IpAddr = match net_str.parse() {
        Ok(value) => value,
        Err(_) => return false,
    };
    let prefix: u8 = match prefix_str.parse() {
        Ok(value) => value,
        Err(_) => return false,
    };
    match (net, ip) {
        (std::net::IpAddr::V4(net), std::net::IpAddr::V4(ip)) => {
            if prefix > 32 {
                return false;
            }
            if prefix == 0 {
                return true;
            }
            let net = u32::from(net);
            let ip = u32::from(ip);
            let mask = !0u32 << (32 - prefix);
            (net & mask) == (ip & mask)
        }
        (std::net::IpAddr::V6(net), std::net::IpAddr::V6(ip)) => {
            if prefix > 128 {
                return false;
            }
            if prefix == 0 {
                return true;
            }
            let net = u128::from(net);
            let ip = u128::from(ip);
            let mask = !0u128 << (128 - prefix);
            (net & mask) == (ip & mask)
        }
        _ => false,
    }
}

pub fn ip_in_cidr(cidr: &str, ip: &str) -> bool {
    cidr_contains(cidr, ip)
}

pub fn contains_ip(cidr: &str, ip: &str) -> bool {
    cidr_contains(cidr, ip)
}

pub fn is_ip_allowed(allowed: &[String], ip: &str) -> bool {
    allowed.is_empty() || allowed.iter().any(|entry| cidr_contains(entry, ip))
}

pub fn cidr_match(cidr: &str, ip: &str) -> bool {
    cidr_contains(cidr, ip)
}

pub fn in_cidr(cidr: &str, ip: &str) -> bool {
    cidr_contains(cidr, ip)
}

pub fn ip_allowed(allowed: &[String], ip: &str) -> bool {
    is_ip_allowed(allowed, ip)
}

pub fn allowed_ip(allowed: &[String], ip: &str) -> bool {
    is_ip_allowed(allowed, ip)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(role: Role, sessions: &[&str], chats: &[&str]) -> Identity {
        Identity {
            key_id: "k".into(),
            name: "test".into(),
            role,
            allowed_sessions: sessions.iter().map(|s| s.to_string()).collect(),
            allowed_chats: chats.iter().map(|c| c.to_string()).collect(),
            allowed_ips: Vec::new(),
        }
    }

    #[test]
    fn roles_rank_and_parse() {
        assert!(Role::Admin.rank() > Role::Operator.rank());
        assert!(Role::Operator.rank() > Role::Viewer.rank());
        assert_eq!(Role::parse("OPERATOR"), Role::Operator);
        assert_eq!(Role::parse("nonsense"), Role::Viewer);
    }

    #[test]
    fn empty_scopes_mean_everything_but_admin_bypasses_anyway() {
        let operator = identity(Role::Operator, &[], &[]);
        assert!(operator.allows_session("anything"));
        assert!(operator.allows_chat("anything"));

        let scoped = identity(Role::Operator, &["a"], &["chat@c.us"]);
        assert!(scoped.allows_session("a"));
        assert!(!scoped.allows_session("b"));
        assert!(scoped.ensure_session("b").is_err());
        assert!(scoped.ensure_chat("other@c.us").is_err());

        let admin = identity(Role::Admin, &["a"], &["chat@c.us"]);
        assert!(admin.allows_session("b"), "admin is never scoped");
    }

    #[test]
    fn visible_sessions_filters() {
        let scoped = identity(Role::Operator, &["a", "c"], &[]);
        let visible = scoped.visible_sessions(["a".to_string(), "b".to_string(), "c".to_string()]);
        assert_eq!(visible, vec!["a".to_string(), "c".to_string()]);
    }

    #[test]
    fn role_requirements_enforced() {
        let viewer = identity(Role::Viewer, &[], &[]);
        assert!(viewer.require_role(Role::Viewer).is_ok());
        assert!(viewer.require_role(Role::Operator).is_err());
        assert!(identity(Role::Admin, &[], &[])
            .require_role(Role::Admin)
            .is_ok());
    }
}
