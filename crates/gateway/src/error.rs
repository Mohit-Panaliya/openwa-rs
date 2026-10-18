//! HTTP error type shared by every route.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

use crate::sidecar::SidecarError;

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
    pub code: Option<String>,
}

impl ApiError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            code: None,
        }
    }

    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }

    pub fn too_many_requests(message: impl Into<String>) -> Self {
        Self::new(StatusCode::TOO_MANY_REQUESTS, message)
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, message)
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, message)
    }

    pub fn bad_gateway(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_GATEWAY, message)
    }

    pub fn gateway_timeout(message: impl Into<String>) -> Self {
        Self::new(StatusCode::GATEWAY_TIMEOUT, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut value = json!({ "error": self.message });
        if let Some(code) = self.code {
            value["code"] = json!(code);
        }
        (self.status, Json(value)).into_response()
    }
}

impl From<SidecarError> for ApiError {
    fn from(error: SidecarError) -> Self {
        match error {
            SidecarError::NotRunning => Self::bad_gateway("engine is not running"),
            SidecarError::Timeout => Self::gateway_timeout("engine request timed out"),
            SidecarError::Io(io) => Self::bad_gateway(format!("engine io error: {io}")),
            SidecarError::Remote(message) => {
                let lowered = message.to_lowercase();
                if lowered.contains("not found") {
                    Self::not_found(message)
                } else if lowered.contains("not connected")
                    || lowered.contains("already")
                    || lowered.contains("invalid")
                {
                    Self::bad_request(message)
                } else {
                    Self::bad_gateway(message)
                }
            }
        }
    }
}

impl From<rusqlite::Error> for ApiError {
    fn from(error: rusqlite::Error) -> Self {
        tracing::error!(%error, "database error");
        Self::internal(format!("database error: {error}"))
    }
}
