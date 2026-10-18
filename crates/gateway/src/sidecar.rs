//! Sidecar process manager: spawns the engine child process and speaks
//! newline-delimited JSON-RPC 2.0 with it over stdio (see docs/SIDECAR_PROTOCOL.md).
//!
//! The command is configurable through `ENGINE_CMD`, so the same gateway can run
//! the Node/Baileys sidecar (`node sidecar/index.js`, the default) or the Rust
//! engine binary built in Phase 2 (`openwa-engine-sidecar`).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{broadcast, oneshot, Mutex};
use tracing::{error, info, warn};

#[derive(Debug, thiserror::Error)]
pub enum SidecarError {
    #[error("sidecar is not running")]
    NotRunning,
    #[error("sidecar request timed out")]
    Timeout,
    #[error("{0}")]
    Remote(String),
    #[error("sidecar io error: {0}")]
    Io(#[from] std::io::Error),
}

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value, String>>>>>;

/// How to launch the engine process.
#[derive(Debug, Clone)]
pub struct EngineCommand {
    pub program: String,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub data_dir: PathBuf,
    /// Only the default Node command needs `index.js` to exist up front.
    pub requires_script: bool,
    pub label: String,
}

impl EngineCommand {
    /// `ENGINE_CMD` wins (whitespace-split, run from the current directory);
    /// otherwise `NODE_BIN <SIDECAR_DIR>/index.js`.
    pub fn from_env() -> Self {
        if let Ok(raw) = std::env::var("ENGINE_CMD") {
            let raw = raw.trim();
            if !raw.is_empty() {
                let mut parts = raw.split_whitespace();
                let program = parts.next().unwrap_or("node").to_string();
                let args: Vec<String> = parts.map(str::to_string).collect();
                return Self {
                    program,
                    args,
                    cwd: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
                    data_dir: default_data_dir(),
                    requires_script: false,
                    label: raw.to_string(),
                };
            }
        }
        let node_bin = std::env::var("NODE_BIN").unwrap_or_else(|_| "node".to_string());
        Self::node(&node_bin, &sidecar_dir_env())
    }

    /// The Phase 1 default: `node <dir>/index.js`.
    pub fn node(node_bin: &str, dir: &Path) -> Self {
        let script = dir.join("index.js");
        Self {
            program: node_bin.to_string(),
            args: vec![script.to_string_lossy().into_owned()],
            cwd: dir.to_path_buf(),
            data_dir: std::env::var("DATA_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| dir.join("data")),
            requires_script: true,
            label: format!("{node_bin} {}", script.display()),
        }
    }
}

fn sidecar_dir_env() -> PathBuf {
    PathBuf::from(std::env::var("SIDECAR_DIR").unwrap_or_else(|_| "sidecar".to_string()))
}

fn default_data_dir() -> PathBuf {
    std::env::var("DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data"))
}

pub struct Sidecar {
    stdin: Mutex<ChildStdin>,
    child: Mutex<Child>,
    next_id: AtomicU64,
    pending: Pending,
    events: broadcast::Sender<Value>,
    timeout: Duration,
}

impl Sidecar {
    /// Phase 1 convenience wrapper around [`EngineCommand::node`].
    pub async fn spawn(node_bin: &str, dir: &Path, timeout_ms: u64) -> anyhow::Result<Arc<Self>> {
        Self::spawn_command(EngineCommand::node(node_bin, dir), timeout_ms).await
    }

    pub async fn spawn_command(
        command: EngineCommand,
        timeout_ms: u64,
    ) -> anyhow::Result<Arc<Self>> {
        if command.requires_script {
            let script = command.cwd.join("index.js");
            if !script.exists() {
                anyhow::bail!(
                    "sidecar script not found at {}\n  set SIDECAR_DIR to the sidecar directory and run `npm --prefix <dir> install`",
                    script.display()
                );
            }
        }

        let mut child = Command::new(&command.program)
            .args(&command.args)
            .current_dir(&command.cwd)
            .env("DATA_DIR", &command.data_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| {
                anyhow::anyhow!(
                    "failed to start `{}`: {error} (is it installed and on PATH?)",
                    command.program
                )
            })?;

        let stdin = child.stdin.take().expect("stdin is piped");
        let stdout = child.stdout.take().expect("stdout is piped");
        let pending: Pending = Arc::default();
        let (events, _) = broadcast::channel(512);
        tokio::spawn(read_loop(stdout, pending.clone(), events.clone()));

        info!(engine = %command.label, "stdio engine started");
        Ok(Arc::new(Self {
            stdin: Mutex::new(stdin),
            child: Mutex::new(child),
            next_id: AtomicU64::new(1),
            pending,
            events,
            timeout: Duration::from_millis(timeout_ms.max(1_000)),
        }))
    }

    /// Subscribe to engine events (`session.status`, `session.qr`, `message.received`, ...).
    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }

    pub async fn is_alive(&self) -> bool {
        matches!(self.child.lock().await.try_wait(), Ok(None))
    }

    /// Send a JSON-RPC request and await its response.
    pub async fn request(&self, method: &str, params: Value) -> Result<Value, SidecarError> {
        if !self.is_alive().await {
            return Err(SidecarError::NotRunning);
        }

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().await.insert(id, sender);

        let line = format!(
            "{}\n",
            json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
        );
        {
            let mut stdin = self.stdin.lock().await;
            if let Err(error) = stdin.write_all(line.as_bytes()).await {
                self.pending.lock().await.remove(&id);
                warn!(%error, "writing to engine failed");
                return Err(SidecarError::NotRunning);
            }
            if let Err(error) = stdin.flush().await {
                warn!(%error, "flushing engine stdin failed");
            }
        }

        match tokio::time::timeout(self.timeout, receiver).await {
            Ok(Ok(Ok(value))) => Ok(value),
            Ok(Ok(Err(message))) => Err(SidecarError::Remote(message)),
            Ok(Err(_)) => Err(SidecarError::NotRunning),
            Err(_) => {
                self.pending.lock().await.remove(&id);
                Err(SidecarError::Timeout)
            }
        }
    }

    pub async fn ping(&self) -> Result<Value, SidecarError> {
        self.request("ping", json!({})).await
    }

    /// Convenience wrapper keeps route handlers free of stringly-typed JSON.
    pub async fn session(&self, method: &str, session_id: &str) -> Result<Value, SidecarError> {
        self.request(method, json!({ "sessionId": session_id }))
            .await
    }
}

async fn read_loop(stdout: ChildStdout, pending: Pending, events: broadcast::Sender<Value>) {
    let mut lines = BufReader::new(stdout).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let message: Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(_) => {
                warn!(%line, "engine produced non-JSON output on stdout");
                continue;
            }
        };

        if let Some(id) = message.get("id").and_then(Value::as_u64) {
            let outcome = match message.get("error") {
                Some(error) => Err(error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown engine error")
                    .to_string()),
                None => Ok(message.get("result").cloned().unwrap_or(Value::Null)),
            };
            if let Some(sender) = pending.lock().await.remove(&id) {
                let _ = sender.send(outcome);
            }
        } else if message.get("method").and_then(Value::as_str) == Some("event") {
            if let Some(params) = message.get("params") {
                let _ = events.send(params.clone());
            }
        } else {
            warn!(%line, "unrecognised engine message");
        }
    }

    error!("engine stdout closed - engine exited");
    let mut guard = pending.lock().await;
    for (_, sender) in guard.drain() {
        let _ = sender.send(Err("engine terminated".to_string()));
    }
}
