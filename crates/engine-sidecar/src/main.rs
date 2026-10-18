//! openwa-engine-sidecar: stdio JSON-RPC front end for `openwa-engine`.
//!
//! Speaks the contract in `docs/SIDECAR_PROTOCOL.md`: newline-delimited JSON-RPC
//! 2.0 on stdin, responses and `method: "event"` notifications on stdout,
//! diagnostics on stderr. stdout carries protocol lines only.
//!
//! Requests are handled on their own task but serialized by a gate, so a
//! response is always written before the events its request triggered (the
//! engine queues those events until the request completes). Everything reaches
//! a single stdout writer task through one FIFO channel, which is what makes
//! that ordering hold on the wire.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use openwa_engine::EngineCore;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

/// Longest unparsable line echoed to stderr, matching the Node reference.
const LOG_SNIPPET: usize = 200;
/// How long to wait for in-flight requests and the writer to drain at exit.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() {
    let data_dir = data_dir();
    if let Err(error) = std::fs::create_dir_all(&data_dir) {
        eprintln!(
            "[sidecar] could not create DATA_DIR {}: {error}",
            data_dir.display()
        );
    }
    eprintln!("[sidecar] starting; DATA_DIR={}", data_dir.display());

    let core = EngineCore::new(&data_dir);
    let (outbox, receiver) = mpsc::unbounded_channel::<Value>();
    core.set_outbox(Some(outbox.clone()));

    let writer = tokio::spawn(stdout_writer(receiver));
    let gate = Arc::new(tokio::sync::Mutex::new(()));

    let mut requests = JoinSet::new();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let signal = wait_for_signal();
    tokio::pin!(signal);

    loop {
        tokio::select! {
            line = lines.next_line() => match line {
                Ok(Some(line)) => {
                    if let Some(request) = parse_line(&line) {
                        let core = core.clone();
                        let outbox = outbox.clone();
                        let gate = gate.clone();
                        requests.spawn(handle_request(core, outbox, gate, request));
                    }
                }
                Ok(None) => {
                    eprintln!("[sidecar] stdin closed");
                    break;
                }
                Err(error) => {
                    eprintln!("[sidecar] stdin error: {error}");
                    break;
                }
            },
            () = &mut signal => {
                eprintln!("[sidecar] signal received; shutting down");
                break;
            }
        }
    }

    // Let in-flight requests finish so their responses are not lost.
    let _ = tokio::time::timeout(DRAIN_TIMEOUT, async {
        while requests.join_next().await.is_some() {}
    })
    .await;

    core.set_outbox(None);
    core.shutdown().await;
    drop(outbox);
    let _ = tokio::time::timeout(DRAIN_TIMEOUT, writer).await;
    eprintln!("[sidecar] exiting");
}

/// Handles one request, guaranteeing exactly one response with the same id, and
/// flushing any events it produced afterwards.
async fn handle_request(
    core: EngineCore,
    outbox: mpsc::UnboundedSender<Value>,
    gate: Arc<tokio::sync::Mutex<()>>,
    request: Value,
) {
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let params = request.get("params").cloned().unwrap_or_else(|| json!({}));

    let outcome = match request.get("method").and_then(Value::as_str) {
        Some(method) => {
            // Hold the gate across dispatch + response + event flush so a
            // second request cannot slip its response between ours and the
            // events our request produced.
            let _guard = gate.lock().await;
            let outcome = core.request(method, params).await;
            let _ = outbox.send(response_envelope(&id, &outcome));
            core.flush_events();
            return;
        }
        None => Err(format!("unknown method: {}", method_text(&request))),
    };

    let _ = outbox.send(response_envelope(&id, &outcome));
}

/// Builds the response line; logs the error message like the reference does.
fn response_envelope(id: &Value, outcome: &Result<Value, String>) -> Value {
    match outcome {
        Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
        Err(message) => {
            eprintln!("[sidecar] request failed: {message}");
            json!({ "jsonrpc": "2.0", "id": id, "error": { "message": message } })
        }
    }
}

/// Renders a non-string `method` the way `String(method)` would in JS.
fn method_text(request: &Value) -> String {
    match request.get("method") {
        None | Some(Value::Null) => "undefined".to_string(),
        Some(Value::String(method)) => method.clone(),
        Some(other) => other.to_string(),
    }
}

fn parse_line(line: &str) -> Option<Value> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    match serde_json::from_str(trimmed) {
        Ok(request) => Some(request),
        Err(_) => {
            eprintln!("[sidecar] ignoring unparsable line: {}", snippet(trimmed));
            None
        }
    }
}

fn snippet(value: &str) -> &str {
    if value.len() <= LOG_SNIPPET {
        value
    } else {
        &value[..LOG_SNIPPET]
    }
}

async fn stdout_writer(mut receiver: mpsc::UnboundedReceiver<Value>) {
    let mut stdout = tokio::io::stdout();
    while let Some(line) = receiver.recv().await {
        let mut encoded = match serde_json::to_vec(&line) {
            Ok(encoded) => encoded,
            Err(error) => {
                eprintln!("[sidecar] could not encode a protocol line: {error}");
                continue;
            }
        };
        encoded.push(b'\n');
        if stdout.write_all(&encoded).await.is_err() {
            break;
        }
        if stdout.flush().await.is_err() {
            break;
        }
    }
}

/// Resolves when the process receives SIGTERM/SIGINT (unix) or a console
/// control event (windows).
async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(stream) => stream,
            Err(error) => {
                eprintln!("[sidecar] could not install SIGTERM handler: {error}");
                std::future::pending::<()>().await;
                return;
            }
        };
        let mut interrupt = match signal(SignalKind::interrupt()) {
            Ok(stream) => stream,
            Err(error) => {
                eprintln!("[sidecar] could not install SIGINT handler: {error}");
                std::future::pending::<()>().await;
                return;
            }
        };
        tokio::select! {
            _ = terminate.recv() => {}
            _ = interrupt.recv() => {}
        }
    }

    #[cfg(windows)]
    {
        let mut ctrl_c = match tokio::signal::windows::ctrl_c() {
            Ok(stream) => stream,
            Err(error) => {
                eprintln!("[sidecar] could not install Ctrl-C handler: {error}");
                std::future::pending::<()>().await;
                return;
            }
        };
        let mut ctrl_break = match tokio::signal::windows::ctrl_break() {
            Ok(stream) => stream,
            Err(error) => {
                eprintln!("[sidecar] could not install Ctrl-Break handler: {error}");
                std::future::pending::<()>().await;
                return;
            }
        };
        tokio::select! {
            _ = ctrl_c.recv() => {}
            _ = ctrl_break.recv() => {}
        }
    }
}

fn data_dir() -> PathBuf {
    match std::env::var_os("DATA_DIR") {
        Some(value) if !value.is_empty() => {
            std::fs::canonicalize(&value).unwrap_or_else(|_| PathBuf::from(value))
        }
        _ => std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join("sidecar")
            .join("data"),
    }
}
