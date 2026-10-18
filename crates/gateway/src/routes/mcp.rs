//! Minimal Model Context Protocol (MCP) endpoint.
//!
//! Implements a small subset of the MCP Streamable-HTTP transport so AI agents
//! can drive read-only and write operations. Mounted at POST /mcp.

use axum::extract::State;
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::{Identity, Role};
use crate::error::ApiError;
use crate::state::AppState;

#[derive(Deserialize, Default)]
pub struct McpRequest {
    #[serde(default)]
    id: Value,
    #[serde(default)]
    method: String,
    #[serde(default)]
    params: Value,
}

fn tools() -> Value {
    json!([
        { "name": "sessions_list", "description": "List WhatsApp sessions", "inputSchema": { "type": "object", "properties": {} } },
        { "name": "sessions_get", "description": "Get a session by id", "inputSchema": { "type": "object", "properties": { "sessionId": { "type": "string" } }, "required": ["sessionId"] } },
        { "name": "messages_list", "description": "List messages for a session/chat", "inputSchema": { "type": "object", "properties": { "sessionId": { "type": "string" }, "chatId": { "type": "string" }, "limit": { "type": "integer" } }, "required": ["sessionId"] } },
        { "name": "groups_list", "description": "List groups for a session", "inputSchema": { "type": "object", "properties": { "sessionId": { "type": "string" } }, "required": ["sessionId"] } },
        { "name": "stats", "description": "Gateway statistics", "inputSchema": { "type": "object", "properties": {} } },
        { "name": "send_text", "description": "Send a text message", "inputSchema": { "type": "object", "properties": { "sessionId": { "type": "string" }, "chatId": { "type": "string" }, "text": { "type": "string" } }, "required": ["sessionId", "chatId", "text"] } },
        { "name": "send_media", "description": "Send a media message", "inputSchema": { "type": "object", "properties": { "sessionId": { "type": "string" }, "chatId": { "type": "string" }, "mediaType": { "type": "string" }, "base64": { "type": "string" }, "url": { "type": "string" }, "mimetype": { "type": "string" }, "filename": { "type": "string" }, "caption": { "type": "string" } }, "required": ["sessionId", "chatId", "mediaType"] } },
        { "name": "send_reaction", "description": "React to a message", "inputSchema": { "type": "object", "properties": { "sessionId": { "type": "string" }, "chatId": { "type": "string" }, "messageId": { "type": "string" }, "emoji": { "type": "string" } }, "required": ["sessionId", "chatId", "messageId", "emoji"] } },
        { "name": "sessions_start", "description": "Start a session", "inputSchema": { "type": "object", "properties": { "sessionId": { "type": "string" } }, "required": ["sessionId"] } },
        { "name": "sessions_stop", "description": "Stop a session", "inputSchema": { "type": "object", "properties": { "sessionId": { "type": "string" } }, "required": ["sessionId"] } }
    ])
}

pub async fn handle(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Json(req): Json<McpRequest>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Viewer)?;
    let id = req.id.clone();
    match req.method.as_str() {
        "initialize" => Ok(Json(json!({
            "jsonrpc": "2.0", "id": id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": { "tools": {} },
                "serverInfo": { "name": "openwa-rs", "version": "0.1.0" }
            }
        }))),
        "tools/list" => Ok(Json(
            json!({ "jsonrpc": "2.0", "id": id, "result": { "tools": tools() } }),
        )),
        "tools/call" => {
            let name = req.params.get("name").and_then(Value::as_str).unwrap_or("");
            let args = req.params.get("arguments").cloned().unwrap_or(Value::Null);
            let result = match name {
                "sessions_list" => {
                    let v = state
                        .engine
                        .request("session.list", json!({}))
                        .await
                        .map_err(ApiError::from)?;
                    json!({ "content": [{ "type": "text", "text": v.to_string() }] })
                }
                "sessions_get" => {
                    let sid = args.get("sessionId").and_then(Value::as_str).unwrap_or("");
                    identity.ensure_session(sid)?;
                    let v = state
                        .engine
                        .request("session.get", json!({ "sessionId": sid }))
                        .await
                        .map_err(ApiError::from)?;
                    json!({ "content": [{ "type": "text", "text": v.to_string() }] })
                }
                "messages_list" => {
                    let sid = args.get("sessionId").and_then(Value::as_str).unwrap_or("");
                    identity.ensure_session(sid)?;
                    let chat = args.get("chatId").and_then(Value::as_str);
                    if let Some(chat) = chat {
                        identity.ensure_chat(chat)?;
                    }
                    let limit = args.get("limit").and_then(Value::as_i64).unwrap_or(50);
                    let msgs = state
                        .db
                        .list_messages(sid, chat, limit, None, None)
                        .await
                        .map_err(ApiError::from)?;
                    json!({ "content": [{ "type": "text", "text": serde_json::to_string(&msgs).unwrap_or_default() }] })
                }
                "groups_list" => {
                    let sid = args.get("sessionId").and_then(Value::as_str).unwrap_or("");
                    identity.ensure_session(sid)?;
                    let v = state
                        .engine
                        .request("group.list", json!({ "sessionId": sid }))
                        .await
                        .map_err(ApiError::from)?;
                    json!({ "content": [{ "type": "text", "text": v.to_string() }] })
                }
                "stats" => {
                    let sessions = state.db.count_sessions().await.unwrap_or(0);
                    let messages = state.db.count_messages().await.unwrap_or(0);
                    json!({ "content": [{ "type": "text", "text": format!("sessions={sessions} messages={messages}") }] })
                }
                "send_text" => {
                    identity.require_role(Role::Operator)?;
                    let sid = args.get("sessionId").and_then(Value::as_str).unwrap_or("");
                    identity.ensure_session(sid)?;
                    let chat_id = args.get("chatId").and_then(Value::as_str).unwrap_or("");
                    identity.ensure_chat(chat_id)?;
                    let text = args.get("text").and_then(Value::as_str).unwrap_or("");
                    let v = state
                        .engine
                        .request(
                            "message.sendText",
                            json!({ "sessionId": sid, "chatId": chat_id, "text": text }),
                        )
                        .await
                        .map_err(ApiError::from)?;
                    json!({ "content": [{ "type": "text", "text": v.to_string() }] })
                }
                "send_media" => {
                    identity.require_role(Role::Operator)?;
                    let sid = args.get("sessionId").and_then(Value::as_str).unwrap_or("");
                    identity.ensure_session(sid)?;
                    let chat_id = args.get("chatId").and_then(Value::as_str).unwrap_or("");
                    identity.ensure_chat(chat_id)?;
                    let media_type = args
                        .get("mediaType")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_lowercase();
                    let base64 = args.get("base64").and_then(Value::as_str);
                    let url = args.get("url").and_then(Value::as_str);
                    let mimetype = args.get("mimetype").and_then(Value::as_str);
                    let filename = args.get("filename").and_then(Value::as_str);
                    let caption = args.get("caption").and_then(Value::as_str);
                    let v = state
                        .engine
                        .request(
                            "message.sendMedia",
                            json!({
                                "sessionId": sid,
                                "chatId": chat_id,
                                "mediaType": media_type,
                                "base64": base64,
                                "url": url,
                                "mimetype": mimetype,
                                "filename": filename,
                                "caption": caption,
                            }),
                        )
                        .await
                        .map_err(ApiError::from)?;
                    json!({ "content": [{ "type": "text", "text": v.to_string() }] })
                }
                "send_reaction" => {
                    identity.require_role(Role::Operator)?;
                    let sid = args.get("sessionId").and_then(Value::as_str).unwrap_or("");
                    identity.ensure_session(sid)?;
                    let chat_id = args.get("chatId").and_then(Value::as_str).unwrap_or("");
                    identity.ensure_chat(chat_id)?;
                    let message_id = args.get("messageId").and_then(Value::as_str).unwrap_or("");
                    let emoji = args.get("emoji").and_then(Value::as_str).unwrap_or("");
                    let v = state
                        .engine
                        .request(
                            "message.sendReaction",
                            json!({
                                "sessionId": sid,
                                "chatId": chat_id,
                                "messageId": message_id,
                                "emoji": emoji,
                            }),
                        )
                        .await
                        .map_err(ApiError::from)?;
                    json!({ "content": [{ "type": "text", "text": v.to_string() }] })
                }
                "sessions_start" => {
                    identity.require_role(Role::Operator)?;
                    let sid = args.get("sessionId").and_then(Value::as_str).unwrap_or("");
                    identity.ensure_session(sid)?;
                    let v = state
                        .engine
                        .session("session.start", sid)
                        .await
                        .map_err(ApiError::from)?;
                    json!({ "content": [{ "type": "text", "text": v.to_string() }] })
                }
                "sessions_stop" => {
                    identity.require_role(Role::Operator)?;
                    let sid = args.get("sessionId").and_then(Value::as_str).unwrap_or("");
                    identity.ensure_session(sid)?;
                    let v = state
                        .engine
                        .session("session.stop", sid)
                        .await
                        .map_err(ApiError::from)?;
                    json!({ "content": [{ "type": "text", "text": v.to_string() }] })
                }
                _ => return Err(ApiError::bad_request("unknown tool")),
            };
            Ok(Json(
                json!({ "jsonrpc": "2.0", "id": id, "result": result }),
            ))
        }
        _ => Err(ApiError::bad_request("unknown method")),
    }
}
