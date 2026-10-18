//! Shared child-process harness for the sidecar integration tests.
//!
//! Spawns the built `openwa-engine-sidecar` binary exactly as the gateway does
//! (piped stdin/stdout, `DATA_DIR` in the environment) and gives the tests a
//! request/response + event API over the newline-delimited JSON-RPC stream.

#![allow(dead_code)]

use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};

pub const RESPONSE_TIMEOUT: Duration = Duration::from_secs(15);

/// A running sidecar process plus the parsed protocol traffic.
pub struct Harness {
    child: Child,
    stdin: ChildStdin,
    responses: Arc<Mutex<HashMap<i64, Value>>>,
    events: Arc<Mutex<Vec<Value>>>,
    stdout_lines: Arc<Mutex<Vec<String>>>,
}

impl Harness {
    /// Spawns the sidecar with `data_dir` as `DATA_DIR`.
    pub fn start(data_dir: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_openwa-engine-sidecar"))
            .env("DATA_DIR", data_dir)
            .env("LOG_LEVEL", "error")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn openwa-engine-sidecar");
        let stdin = child.stdin.take().expect("child stdin");
        let stdout = child.stdout.take().expect("child stdout");

        let responses = Arc::new(Mutex::new(HashMap::new()));
        let events = Arc::new(Mutex::new(Vec::new()));
        let stdout_lines = Arc::new(Mutex::new(Vec::new()));

        let reader_responses = responses.clone();
        let reader_events = events.clone();
        let reader_lines = stdout_lines.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                reader_lines.lock().unwrap().push(line.clone());
                let Ok(value) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                if value.get("method").and_then(Value::as_str) == Some("event") {
                    reader_events.lock().unwrap().push(value);
                } else if let Some(id) = value.get("id").and_then(Value::as_i64) {
                    reader_responses.lock().unwrap().insert(id, value);
                }
            }
        });

        Self {
            child,
            stdin,
            responses,
            events,
            stdout_lines,
        }
    }

    /// Sends one request and waits for its response.
    pub async fn call(&mut self, id: i64, method: &str, params: Value) -> Value {
        let request = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        self.stdin
            .write_all(format!("{request}\n").as_bytes())
            .await
            .expect("write request");
        self.stdin.flush().await.expect("flush request");
        self.await_response(id, RESPONSE_TIMEOUT)
            .await
            .unwrap_or_else(|| panic!("timeout waiting for `{method}` response"))
    }

    async fn await_response(&self, id: i64, timeout: Duration) -> Option<Value> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(value) = self.responses.lock().unwrap().get(&id).cloned() {
                return Some(value);
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Waits for an event matching `predicate`.
    pub async fn wait_for_event<F: Fn(&Value) -> bool>(
        &self,
        predicate: F,
        timeout: Duration,
    ) -> Option<Value> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(value) = self
                .events
                .lock()
                .unwrap()
                .iter()
                .find(|event| predicate(event))
                .cloned()
            {
                return Some(value);
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// All events seen so far.
    pub fn events(&self) -> Vec<Value> {
        self.events.lock().unwrap().clone()
    }

    /// All raw stdout lines seen so far.
    pub fn stdout_lines(&self) -> Vec<String> {
        self.stdout_lines.lock().unwrap().clone()
    }

    /// Terminates the child and waits for it to exit.
    pub async fn stop(mut self) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }
}

/// Renders the `type` of an event envelope.
pub fn event_type(event: &Value) -> &str {
    event
        .get("params")
        .and_then(|params| params.get("type"))
        .and_then(Value::as_str)
        .unwrap_or_default()
}

/// Renders the `payload` of an event envelope.
pub fn event_payload(event: &Value) -> Value {
    event
        .get("params")
        .and_then(|params| params.get("payload"))
        .cloned()
        .unwrap_or(Value::Null)
}

/// A tiny PASS/FAIL accumulator that prints like the Node dev-selftest.
#[derive(Default)]
pub struct Checks {
    results: Vec<(String, bool, String)>,
}

impl Checks {
    /// Records and prints one check.
    pub fn check(&mut self, name: &str, ok: bool, detail: impl Into<String>) {
        let detail = detail.into();
        println!(
            "  {}  {}{}",
            if ok { "PASS" } else { "FAIL" },
            name,
            if ok || detail.is_empty() {
                String::new()
            } else {
                format!("  -> {detail}")
            }
        );
        self.results.push((name.to_string(), ok, detail));
    }

    /// Number of checks that failed.
    pub fn failed(&self) -> usize {
        self.results.iter().filter(|(_, ok, _)| !ok).count()
    }

    /// Total number of checks.
    pub fn total(&self) -> usize {
        self.results.len()
    }

    /// Prints the summary line and panics if anything failed.
    pub fn finish(self) {
        let failed: Vec<&str> = self
            .results
            .iter()
            .filter(|(_, ok, _)| !ok)
            .map(|(name, _, _)| name.as_str())
            .collect();
        println!();
        println!(
            "{} - {}/{} checks passed",
            if failed.is_empty() { "PASS" } else { "FAIL" },
            self.total() - failed.len(),
            self.total()
        );
        assert!(failed.is_empty(), "failed checks: {}", failed.join("; "));
    }
}

/// A fresh temp data directory, removed first if it already exists.
pub fn temp_data_dir(label: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("openwa-engine-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}
