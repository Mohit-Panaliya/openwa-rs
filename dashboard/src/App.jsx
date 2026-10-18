import React, { useState, useEffect, useCallback } from "react";

const API = "";

function useKey() {
  const [key, setKey] = useState(() => localStorage.getItem("openwa_key") || "");
  const save = (k) => { localStorage.setItem("openwa_key", k); setKey(k); };
  const clear = () => { localStorage.removeItem("openwa_key"); setKey(""); };
  return { key, save, clear };
}

async function api(key, path, opts = {}) {
  const res = await fetch(API + path, {
    ...opts,
    headers: { "Content-Type": "application/json", "X-API-Key": key, ...(opts.headers || {}) },
  });
  const text = await res.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }
  if (!res.ok) throw new Error((data && data.message) || res.statusText);
  return data;
}

export default function App() {
  const { key, save, clear } = useKey();
  const [input, setInput] = useState("");
  const [tab, setTab] = useState("sessions");
  const [sessions, setSessions] = useState([]);
  const [selected, setSelected] = useState(null);
  const [messages, setMessages] = useState([]);
  const [text, setText] = useState("");
  const [chatId, setChatId] = useState("");
  const [stats, setStats] = useState(null);
  const [webhooks, setWebhooks] = useState([]);
  const [error, setError] = useState("");

  const loadSessions = useCallback(async () => {
    try { const d = await api(key, "/api/sessions"); setSessions(d.sessions || []); }
    catch (e) { setError(e.message); }
  }, [key]);

  const loadStats = useCallback(async () => {
    try { setStats(await api(key, "/api/stats")); } catch (e) { setError(e.message); }
  }, [key]);

  useEffect(() => { if (key) { loadSessions(); loadStats(); } }, [key, loadSessions, loadStats]);

  const createSession = async () => {
    const name = prompt("Session name?") || undefined;
    try { await api(key, "/api/sessions", { method: "POST", body: JSON.stringify({ name }) }); loadSessions(); }
    catch (e) { setError(e.message); }
  };

  const startSession = async (id) => {
    try { await api(key, `/api/sessions/${id}/start`, { method: "POST" }); loadSessions(); }
    catch (e) { setError(e.message); }
  };

  const loadMessages = async (sid, cid) => {
    try {
      const q = cid ? `?chatId=${encodeURIComponent(cid)}` : "";
      const d = await api(key, `/api/sessions/${sid}/messages${q}`);
      setMessages(d.messages || []);
    } catch (e) { setError(e.message); }
  };

  const sendText = async () => {
    if (!selected || !chatId || !text) return;
    try {
      await api(key, `/api/sessions/${selected}/messages/send-text`, { method: "POST", body: JSON.stringify({ chatId, text }) });
      setText(""); loadMessages(selected, chatId);
    } catch (e) { setError(e.message); }
  };

  if (!key) {
    return (
      <div className="login">
        <h1>OpenWA Dashboard</h1>
        <input value={input} onChange={(e) => setInput(e.target.value)} placeholder="API key" />
        <button onClick={() => input && save(input)}>Sign in</button>
      </div>
    );
  }

  return (
    <div className="app">
      <header>
        <h1>OpenWA</h1>
        <nav>
          {["sessions", "messages", "webhooks", "stats"].map((t) => (
            <button key={t} className={tab === t ? "active" : ""} onClick={() => setTab(t)}>{t}</button>
          ))}
        </nav>
        <button onClick={clear}>Sign out</button>
      </header>
      {error && <div className="error" onClick={() => setError("")}>{error}</div>}
      <main>
        {tab === "sessions" && (
          <section>
            <div className="row">
              <h2>Sessions</h2>
              <button onClick={createSession}>+ New</button>
              <button onClick={loadSessions}>Refresh</button>
            </div>
            <ul className="sessions">
              {sessions.map((s) => (
                <li key={s.id || s.sessionId} className={selected === (s.id || s.sessionId) ? "sel" : ""}>
                  <div onClick={() => { setSelected(s.id || s.sessionId); setTab("messages"); }}>
                    <strong>{s.name}</strong> <span className={`status ${s.status}`}>{s.status}</span>
                  </div>
                  <div className="actions">
                    <button onClick={() => startSession(s.id || s.sessionId)}>Start</button>
                  </div>
                  {(s.status === "qr_ready") && (
                    <img className="qr" src={`/api/sessions/${s.id || s.sessionId}/qr.svg?apiKey=${key}`} alt="qr" />
                  )}
                </li>
              ))}
            </ul>
          </section>
        )}
        {tab === "messages" && (
          <section>
            <div className="row">
              <select value={selected || ""} onChange={(e) => setSelected(e.target.value)}>
                <option value="">select session</option>
                {sessions.map((s) => <option key={s.id || s.sessionId} value={s.id || s.sessionId}>{s.name}</option>)}
              </select>
              <input value={chatId} onChange={(e) => setChatId(e.target.value)} placeholder="chatId" />
              <button onClick={() => selected && loadMessages(selected, chatId)}>Load</button>
            </div>
            <div className="send">
              <input value={text} onChange={(e) => setText(e.target.value)} placeholder="message text" onKeyDown={(e) => e.key === "Enter" && sendText()} />
              <button onClick={sendText}>Send</button>
            </div>
            <ul className="messages">
              {messages.map((m) => (
                <li key={m.id} className={m.fromMe ? "me" : "them"}>
                  <span className="kind">{m.kind}</span> {m.text}
                  <small>{m.timestamp}</small>
                </li>
              ))}
            </ul>
          </section>
        )}
        {tab === "webhooks" && (
          <section>
            <h2>Webhooks</h2>
            <p>Manage via API. Count: {webhooks.length}</p>
          </section>
        )}
        {tab === "stats" && stats && (
          <section>
            <h2>Stats</h2>
            <pre>{JSON.stringify(stats, null, 2)}</pre>
          </section>
        )}
      </main>
    </div>
  );
}