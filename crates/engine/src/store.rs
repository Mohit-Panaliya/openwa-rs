//! File-backed persistence for the WhatsApp client.
//!
//! `whatsapp-rust` reaches all of its persistent state through the `Backend`
//! trait (`SignalStore + AppSyncStore + ProtocolStore + MsgSecretStore +
//! DeviceStore`). The upstream SQLite backend cannot be used here: it links
//! `libsqlite3-sys` 0.37 while the gateway links its own copy, and Cargo refuses
//! two versions of a crate that declares `links = "sqlite3"`. So this module
//! implements the same trait bundled as a thin decorator:
//!
//! * every call is forwarded to `wacore::store::InMemoryBackend`, which keeps the
//!   runtime authoritative copy (and its exact semantics, including the batch,
//!   merge and expiry rules);
//! * every *mutation* additionally records the resulting row in [`Snapshot`];
//! * [`FileBackend::flush`] writes that snapshot as one JSON document to
//!   `<session dir>/state.json`, and [`FileBackend::open`] replays it back into a
//!   fresh in-memory backend, so a restart resumes with the same credentials,
//!   Signal sessions and keys.
//!
//! Reads are never served from the snapshot, so mirroring can only ever lose
//! data (a missed mirror call) — never invent it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use whatsapp_rust::async_trait;
use whatsapp_rust::bytes::Bytes;
use whatsapp_rust::wacore::appstate::hash::HashState;
use whatsapp_rust::wacore::appstate::processor::AppStateMutationMAC;
use whatsapp_rust::wacore::store::device::Device;
use whatsapp_rust::wacore::store::error::{Result as StoreResult, StoreError};
use whatsapp_rust::wacore::store::in_memory::InMemoryBackend;
use whatsapp_rust::wacore::store::traits::{
    merge_msg_secret_expiry, merge_msg_secret_message_ts, AppSyncStore, DeviceStore,
    MsgSecretEntry, MsgSecretStore, PendingInboundKey, PendingInboundRow, ProtocolStore,
    SignalStore, TcTokenEntry,
};
use whatsapp_rust::wacore::time::now_secs;

/// Separator for composite text keys ("chat\x1fid"). Chosen because no JID or
/// message id can contain it.
const SEP: char = '\u{1f}';

fn composite(parts: &[&str]) -> String {
    parts.join(&SEP.to_string())
}

fn b64(bytes: &[u8]) -> String {
    B64.encode(bytes)
}

fn unb64(value: &str) -> Vec<u8> {
    B64.decode(value).unwrap_or_default()
}

fn to_value<T: Serialize>(value: &T) -> StoreResult<serde_json::Value> {
    serde_json::to_value(value).map_err(|error| StoreError::Serialization(Box::new(error)))
}

fn from_value<T: DeserializeOwned>(value: serde_json::Value) -> StoreResult<T> {
    serde_json::from_value(value).map_err(|error| StoreError::Serialization(Box::new(error)))
}

/// A stored sent message, kept with its own timestamp so the retention sweep
/// can reproduce the backend's expiry rule after a restart.
#[derive(Clone, Serialize, Deserialize)]
struct SentRow {
    created_at: i64,
    payload: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct PendingRow {
    stored_at: i64,
    payload: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct SecretRow {
    secret: String,
    #[serde(default)]
    expires_at: i64,
    #[serde(default)]
    message_ts: i64,
}

/// The persisted mirror of one session's store.
#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(default)]
struct Snapshot {
    device: Option<serde_json::Value>,
    identities: HashMap<String, String>,
    sessions: HashMap<String, String>,
    prekeys: HashMap<String, String>,
    signed_prekeys: HashMap<String, String>,
    sender_keys: HashMap<String, String>,
    /// Insertion ordered: the backend reports the last written key as "latest".
    sync_keys: Vec<(String, serde_json::Value)>,
    versions: HashMap<String, serde_json::Value>,
    mutation_macs: HashMap<String, HashMap<String, String>>,
    sender_key_devices: HashMap<String, HashMap<String, bool>>,
    /// Insertion ordered, mirroring the backend's "last mapping for a phone wins".
    lid_mappings: Vec<serde_json::Value>,
    base_keys: HashMap<String, String>,
    device_lists: HashMap<String, serde_json::Value>,
    group_metadata: HashMap<String, String>,
    tc_tokens: HashMap<String, serde_json::Value>,
    sent_messages: HashMap<String, SentRow>,
    msg_secrets: HashMap<String, SecretRow>,
    pending_inbound: HashMap<String, PendingRow>,
}

impl Snapshot {
    fn upsert_lid_mapping(&mut self, entry: &serde_json::Value) {
        let lid = entry
            .get("lid")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        self.lid_mappings.retain(|existing| {
            existing
                .get("lid")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                != lid
        });
        self.lid_mappings.push(entry.clone());
    }

    fn set_sync_key(&mut self, key: &str, value: serde_json::Value) {
        self.sync_keys.retain(|(existing, _)| existing != key);
        self.sync_keys.push((key.to_string(), value));
    }
}

/// A `Backend` that keeps everything in memory and mirrors it to `state.json`.
pub struct FileBackend {
    dir: PathBuf,
    inner: Arc<InMemoryBackend>,
    snapshot: Mutex<Snapshot>,
    dirty: AtomicBool,
}

impl FileBackend {
    /// Opens (loading any previously persisted state) the store for one session.
    pub async fn open(dir: impl AsRef<Path>) -> StoreResult<Arc<Self>> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let backend = Arc::new(Self {
            dir,
            inner: Arc::new(InMemoryBackend::new()),
            snapshot: Mutex::new(Snapshot::default()),
            dirty: AtomicBool::new(false),
        });

        if let Some(snapshot) = backend.read_snapshot()? {
            backend.replay(snapshot.clone()).await?;
            *backend.snapshot.lock().await = snapshot;
        }
        Ok(backend)
    }

    /// Absolute path of the per-session state document.
    pub fn state_path(&self) -> PathBuf {
        self.dir.join("state.json")
    }

    /// True when a previous run already paired (credentials are on disk).
    ///
    /// Kept as part of the store's surface for embedders that want to report
    /// "already linked" before starting a session.
    #[allow(dead_code)]
    pub async fn has_credentials(&self) -> bool {
        self.inner.exists().await.unwrap_or(false)
    }

    /// Persists the snapshot if anything changed since the last flush.
    pub async fn flush(&self) -> StoreResult<()> {
        if !self.dirty.swap(false, Ordering::SeqCst) {
            return Ok(());
        }
        let snapshot = self.snapshot.lock().await.clone();
        let path = self.state_path();
        let temporary = path.with_extension("json.tmp");
        let encoded = serde_json::to_vec(&snapshot)
            .map_err(|error| StoreError::Serialization(Box::new(error)))?;
        // Write-then-rename so a crash mid-write never truncates live state.
        tokio::fs::write(&temporary, encoded).await?;
        tokio::fs::rename(&temporary, &path).await?;
        Ok(())
    }

    fn read_snapshot(&self) -> StoreResult<Option<Snapshot>> {
        let path = self.state_path();
        let data = match std::fs::read(&path) {
            Ok(data) => data,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(StoreError::Io(error)),
        };
        let snapshot = serde_json::from_slice(&data)
            .map_err(|error| StoreError::Serialization(Box::new(error)))?;
        Ok(Some(snapshot))
    }

    /// Replays a snapshot into the in-memory backend, which then serves reads.
    async fn replay(&self, snapshot: Snapshot) -> StoreResult<()> {
        let inner = &self.inner;

        for (address, key) in &snapshot.identities {
            let mut raw = [0u8; 32];
            let decoded = unb64(key);
            if decoded.len() != 32 {
                continue;
            }
            raw.copy_from_slice(&decoded);
            inner.put_identity(address, raw).await?;
        }
        for (address, session) in &snapshot.sessions {
            inner.put_session(address, &unb64(session)).await?;
        }
        for (id, record) in &snapshot.prekeys {
            if let Ok(id) = id.parse::<u32>() {
                inner.store_prekey(id, &unb64(record), true).await?;
            }
        }
        for (id, record) in &snapshot.signed_prekeys {
            if let Ok(id) = id.parse::<u32>() {
                inner.store_signed_prekey(id, &unb64(record)).await?;
            }
        }
        for (address, record) in &snapshot.sender_keys {
            inner.put_sender_key(address, &unb64(record)).await?;
        }
        for (key_id, key) in &snapshot.sync_keys {
            let id = unb64(key_id);
            inner.set_sync_key(&id, from_value(key.clone())?).await?;
        }
        for (name, state) in &snapshot.versions {
            inner.set_version(name, from_value(state.clone())?).await?;
        }
        for (name, macs) in &snapshot.mutation_macs {
            let mutations: Vec<AppStateMutationMAC> = macs
                .iter()
                .map(|(index, value)| AppStateMutationMAC {
                    index_mac: unb64(index),
                    value_mac: unb64(value),
                })
                .collect();
            inner.put_mutation_macs(name, 0, &mutations).await?;
        }
        for (group, devices) in &snapshot.sender_key_devices {
            let entries: Vec<(&str, bool)> = devices
                .iter()
                .map(|(jid, has)| (jid.as_str(), *has))
                .collect();
            inner.set_sender_key_status(group, &entries).await?;
        }
        for entry in &snapshot.lid_mappings {
            let entry = from_value(entry.clone())?;
            inner.put_lid_mapping(&entry).await?;
        }
        for (key, value) in &snapshot.base_keys {
            if let Some((address, message_id)) = key.split_once(SEP) {
                inner
                    .save_base_key(address, message_id, &unb64(value))
                    .await?;
            }
        }
        for record in snapshot.device_lists.values() {
            inner
                .update_device_list(from_value(record.clone())?)
                .await?;
        }
        for (jid, blob) in &snapshot.group_metadata {
            inner.put_group_metadata(jid, &unb64(blob)).await?;
        }
        for (jid, token) in &snapshot.tc_tokens {
            inner.put_tc_token(jid, &from_value(token.clone())?).await?;
        }
        for (key, row) in &snapshot.sent_messages {
            if let Some((chat, message_id)) = key.split_once(SEP) {
                inner
                    .store_sent_message(chat, message_id, &unb64(&row.payload))
                    .await?;
            }
        }
        for (key, row) in &snapshot.msg_secrets {
            let mut parts = key.split(SEP);
            let (Some(chat), Some(sender), Some(msg_id)) =
                (parts.next(), parts.next(), parts.next())
            else {
                continue;
            };
            let secret = unb64(&row.secret);
            let entry = MsgSecretEntry {
                chat: Arc::from(chat),
                sender: Arc::from(sender),
                msg_id: Arc::from(msg_id),
                secret: if secret.len()
                    == whatsapp_rust::wacore::reporting_token::MESSAGE_SECRET_SIZE
                {
                    let mut raw =
                        [0u8; whatsapp_rust::wacore::reporting_token::MESSAGE_SECRET_SIZE];
                    raw.copy_from_slice(&secret);
                    raw
                } else {
                    continue;
                },
                expires_at: row.expires_at,
                message_ts: row.message_ts,
            };
            inner.put_msg_secrets(vec![entry]).await?;
        }
        for (key, row) in &snapshot.pending_inbound {
            let mut parts = key.split(SEP);
            let (Some(chat), Some(sender), Some(id)) = (parts.next(), parts.next(), parts.next())
            else {
                continue;
            };
            inner
                .store_pending_inbound(chat, sender, id, &unb64(&row.payload))
                .await?;
        }
        if let Some(device) = &snapshot.device {
            inner.save(&from_value(device.clone())?).await?;
        }
        Ok(())
    }

    fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::SeqCst);
    }

    /// Mutates the snapshot and marks it dirty.
    async fn edit<F: FnOnce(&mut Snapshot)>(&self, edit: F) {
        let mut snapshot = self.snapshot.lock().await;
        edit(&mut snapshot);
        drop(snapshot);
        self.mark_dirty();
    }
}

// ---------------------------------------------------------------------------
// SignalStore
// ---------------------------------------------------------------------------

#[async_trait]
impl SignalStore for FileBackend {
    async fn put_identity(&self, address: &str, key: [u8; 32]) -> StoreResult<()> {
        self.inner.put_identity(address, key).await?;
        self.edit(|snapshot| {
            snapshot.identities.insert(address.to_string(), b64(&key));
        })
        .await;
        Ok(())
    }

    async fn put_identities_batch(&self, identities: &[(Arc<str>, [u8; 32])]) -> StoreResult<()> {
        self.inner.put_identities_batch(identities).await?;
        self.edit(|snapshot| {
            for (address, key) in identities {
                snapshot.identities.insert(address.to_string(), b64(key));
            }
        })
        .await;
        Ok(())
    }

    async fn load_identity(&self, address: &str) -> StoreResult<Option<[u8; 32]>> {
        self.inner.load_identity(address).await
    }

    async fn delete_identity(&self, address: &str) -> StoreResult<()> {
        self.inner.delete_identity(address).await?;
        self.edit(|snapshot| {
            snapshot.identities.remove(address);
        })
        .await;
        Ok(())
    }

    async fn get_session(&self, address: &str) -> StoreResult<Option<Bytes>> {
        self.inner.get_session(address).await
    }

    async fn put_session(&self, address: &str, session: &[u8]) -> StoreResult<()> {
        self.inner.put_session(address, session).await?;
        self.edit(|snapshot| {
            snapshot.sessions.insert(address.to_string(), b64(session));
        })
        .await;
        Ok(())
    }

    async fn put_sessions_batch(&self, sessions: &[(Arc<str>, Bytes)]) -> StoreResult<()> {
        self.inner.put_sessions_batch(sessions).await?;
        self.edit(|snapshot| {
            for (address, session) in sessions {
                snapshot
                    .sessions
                    .insert(address.to_string(), b64(session.as_ref()));
            }
        })
        .await;
        Ok(())
    }

    async fn delete_session(&self, address: &str) -> StoreResult<()> {
        self.inner.delete_session(address).await?;
        self.edit(|snapshot| {
            snapshot.sessions.remove(address);
        })
        .await;
        Ok(())
    }

    async fn has_session(&self, address: &str) -> StoreResult<bool> {
        self.inner.has_session(address).await
    }

    async fn has_signal_state_for_user(&self, user: &str) -> StoreResult<bool> {
        self.inner.has_signal_state_for_user(user).await
    }

    async fn store_prekey(&self, id: u32, record: &[u8], uploaded: bool) -> StoreResult<()> {
        self.inner.store_prekey(id, record, uploaded).await?;
        self.edit(|snapshot| {
            snapshot.prekeys.insert(id.to_string(), b64(record));
        })
        .await;
        Ok(())
    }

    async fn store_prekeys_batch(&self, keys: &[(u32, Bytes)], uploaded: bool) -> StoreResult<()> {
        self.inner.store_prekeys_batch(keys, uploaded).await?;
        self.edit(|snapshot| {
            for (id, record) in keys {
                snapshot
                    .prekeys
                    .insert(id.to_string(), b64(record.as_ref()));
            }
        })
        .await;
        Ok(())
    }

    async fn load_prekey(&self, id: u32) -> StoreResult<Option<Bytes>> {
        self.inner.load_prekey(id).await
    }

    async fn load_prekeys_batch(&self, ids: &[u32]) -> StoreResult<Vec<(u32, Bytes)>> {
        self.inner.load_prekeys_batch(ids).await
    }

    async fn mark_prekeys_uploaded(&self, ids: &[u32]) -> StoreResult<()> {
        self.inner.mark_prekeys_uploaded(ids).await
    }

    async fn remove_prekey(&self, id: u32) -> StoreResult<()> {
        self.inner.remove_prekey(id).await?;
        self.edit(|snapshot| {
            snapshot.prekeys.remove(&id.to_string());
        })
        .await;
        Ok(())
    }

    async fn get_max_prekey_id(&self) -> StoreResult<u32> {
        self.inner.get_max_prekey_id().await
    }

    async fn store_signed_prekey(&self, id: u32, record: &[u8]) -> StoreResult<()> {
        self.inner.store_signed_prekey(id, record).await?;
        self.edit(|snapshot| {
            snapshot.signed_prekeys.insert(id.to_string(), b64(record));
        })
        .await;
        Ok(())
    }

    async fn load_signed_prekey(&self, id: u32) -> StoreResult<Option<Vec<u8>>> {
        self.inner.load_signed_prekey(id).await
    }

    async fn load_all_signed_prekeys(&self) -> StoreResult<Vec<(u32, Vec<u8>)>> {
        self.inner.load_all_signed_prekeys().await
    }

    async fn remove_signed_prekey(&self, id: u32) -> StoreResult<()> {
        self.inner.remove_signed_prekey(id).await?;
        self.edit(|snapshot| {
            snapshot.signed_prekeys.remove(&id.to_string());
        })
        .await;
        Ok(())
    }

    async fn put_sender_key(&self, address: &str, record: &[u8]) -> StoreResult<()> {
        self.inner.put_sender_key(address, record).await?;
        self.edit(|snapshot| {
            snapshot
                .sender_keys
                .insert(address.to_string(), b64(record));
        })
        .await;
        Ok(())
    }

    async fn put_sender_keys_batch(&self, sender_keys: &[(Arc<str>, Bytes)]) -> StoreResult<()> {
        self.inner.put_sender_keys_batch(sender_keys).await?;
        self.edit(|snapshot| {
            for (address, record) in sender_keys {
                snapshot
                    .sender_keys
                    .insert(address.to_string(), b64(record.as_ref()));
            }
        })
        .await;
        Ok(())
    }

    async fn get_sender_key(&self, address: &str) -> StoreResult<Option<Vec<u8>>> {
        self.inner.get_sender_key(address).await
    }

    async fn delete_sender_key(&self, address: &str) -> StoreResult<()> {
        self.inner.delete_sender_key(address).await?;
        self.edit(|snapshot| {
            snapshot.sender_keys.remove(address);
        })
        .await;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// AppSyncStore
// ---------------------------------------------------------------------------

#[async_trait]
impl AppSyncStore for FileBackend {
    async fn get_sync_key(
        &self,
        key_id: &[u8],
    ) -> StoreResult<Option<whatsapp_rust::store::AppStateSyncKey>> {
        self.inner.get_sync_key(key_id).await
    }

    async fn set_sync_key(
        &self,
        key_id: &[u8],
        key: whatsapp_rust::store::AppStateSyncKey,
    ) -> StoreResult<()> {
        self.inner.set_sync_key(key_id, key.clone()).await?;
        let value = to_value(&key)?;
        let encoded = b64(key_id);
        self.edit(|snapshot| snapshot.set_sync_key(&encoded, value))
            .await;
        Ok(())
    }

    async fn get_version(&self, name: &str) -> StoreResult<HashState> {
        self.inner.get_version(name).await
    }

    async fn set_version(&self, name: &str, state: HashState) -> StoreResult<()> {
        self.inner.set_version(name, state.clone()).await?;
        let value = to_value(&state)?;
        self.edit(|snapshot| {
            snapshot.versions.insert(name.to_string(), value);
        })
        .await;
        Ok(())
    }

    async fn put_mutation_macs(
        &self,
        name: &str,
        version: u64,
        mutations: &[AppStateMutationMAC],
    ) -> StoreResult<()> {
        self.inner
            .put_mutation_macs(name, version, mutations)
            .await?;
        let rows: Vec<(String, String)> = mutations
            .iter()
            .map(|mutation| (b64(&mutation.index_mac), b64(&mutation.value_mac)))
            .collect();
        self.edit(|snapshot| {
            let entry = snapshot.mutation_macs.entry(name.to_string()).or_default();
            for (index, value) in rows {
                entry.insert(index, value);
            }
        })
        .await;
        Ok(())
    }

    async fn get_mutation_mac(&self, name: &str, index_mac: &[u8]) -> StoreResult<Option<Vec<u8>>> {
        self.inner.get_mutation_mac(name, index_mac).await
    }

    async fn get_mutation_macs(
        &self,
        name: &str,
        index_macs: &[[u8; 32]],
    ) -> StoreResult<HashMap<[u8; 32], Vec<u8>>> {
        self.inner.get_mutation_macs(name, index_macs).await
    }

    async fn delete_mutation_macs(&self, name: &str, index_macs: &[Vec<u8>]) -> StoreResult<()> {
        self.inner.delete_mutation_macs(name, index_macs).await?;
        let keys: Vec<String> = index_macs.iter().map(|mac| b64(mac)).collect();
        self.edit(|snapshot| {
            if let Some(entry) = snapshot.mutation_macs.get_mut(name) {
                for key in keys {
                    entry.remove(&key);
                }
            }
        })
        .await;
        Ok(())
    }

    async fn clear_mutation_macs(&self, name: &str) -> StoreResult<()> {
        self.inner.clear_mutation_macs(name).await?;
        self.edit(|snapshot| {
            snapshot.mutation_macs.remove(name);
        })
        .await;
        Ok(())
    }

    async fn get_latest_sync_key_id(&self) -> StoreResult<Option<Vec<u8>>> {
        self.inner.get_latest_sync_key_id().await
    }
}

// ---------------------------------------------------------------------------
// ProtocolStore
// ---------------------------------------------------------------------------

#[async_trait]
impl ProtocolStore for FileBackend {
    async fn get_sender_key_devices(&self, group_jid: &str) -> StoreResult<Vec<(String, bool)>> {
        self.inner.get_sender_key_devices(group_jid).await
    }

    async fn set_sender_key_status(
        &self,
        group_jid: &str,
        entries: &[(&str, bool)],
    ) -> StoreResult<()> {
        self.inner.set_sender_key_status(group_jid, entries).await?;
        let rows: Vec<(String, bool)> = entries
            .iter()
            .map(|(jid, has)| (jid.to_string(), *has))
            .collect();
        self.edit(|snapshot| {
            let group = snapshot
                .sender_key_devices
                .entry(group_jid.to_string())
                .or_default();
            for (jid, has) in rows {
                group.insert(jid, has);
            }
        })
        .await;
        Ok(())
    }

    async fn clear_sender_key_devices(&self, group_jid: &str) -> StoreResult<()> {
        self.inner.clear_sender_key_devices(group_jid).await?;
        self.edit(|snapshot| {
            snapshot.sender_key_devices.remove(group_jid);
        })
        .await;
        Ok(())
    }

    async fn delete_sender_key_device_rows(&self, device_jids: &[&str]) -> StoreResult<()> {
        self.inner
            .delete_sender_key_device_rows(device_jids)
            .await?;
        let victims: Vec<String> = device_jids.iter().map(|jid| jid.to_string()).collect();
        self.edit(|snapshot| {
            for group in snapshot.sender_key_devices.values_mut() {
                group.retain(|jid, _| !victims.contains(jid));
            }
        })
        .await;
        Ok(())
    }

    async fn clear_all_sender_key_devices(&self) -> StoreResult<()> {
        self.inner.clear_all_sender_key_devices().await?;
        self.edit(|snapshot| snapshot.sender_key_devices.clear())
            .await;
        Ok(())
    }

    async fn get_lid_mapping(
        &self,
        lid: &str,
    ) -> StoreResult<Option<whatsapp_rust::store::LidPnMappingEntry>> {
        self.inner.get_lid_mapping(lid).await
    }

    async fn get_pn_mapping(
        &self,
        phone: &str,
    ) -> StoreResult<Option<whatsapp_rust::store::LidPnMappingEntry>> {
        self.inner.get_pn_mapping(phone).await
    }

    async fn put_lid_mapping(
        &self,
        entry: &whatsapp_rust::store::LidPnMappingEntry,
    ) -> StoreResult<()> {
        self.inner.put_lid_mapping(entry).await?;
        let value = to_value(entry)?;
        self.edit(|snapshot| snapshot.upsert_lid_mapping(&value))
            .await;
        Ok(())
    }

    async fn put_lid_mappings(
        &self,
        entries: &[whatsapp_rust::store::LidPnMappingEntry],
    ) -> StoreResult<()> {
        self.inner.put_lid_mappings(entries).await?;
        let values: Vec<serde_json::Value> =
            entries.iter().map(to_value).collect::<StoreResult<_>>()?;
        self.edit(|snapshot| {
            for value in values {
                snapshot.upsert_lid_mapping(&value);
            }
        })
        .await;
        Ok(())
    }

    async fn get_all_lid_mappings(
        &self,
    ) -> StoreResult<Vec<whatsapp_rust::store::LidPnMappingEntry>> {
        self.inner.get_all_lid_mappings().await
    }

    async fn save_base_key(
        &self,
        address: &str,
        message_id: &str,
        base_key: &[u8],
    ) -> StoreResult<()> {
        self.inner
            .save_base_key(address, message_id, base_key)
            .await?;
        let key = composite(&[address, message_id]);
        let value = b64(base_key);
        self.edit(|snapshot| {
            snapshot.base_keys.insert(key, value);
        })
        .await;
        Ok(())
    }

    async fn has_same_base_key(
        &self,
        address: &str,
        message_id: &str,
        current_base_key: &[u8],
    ) -> StoreResult<bool> {
        self.inner
            .has_same_base_key(address, message_id, current_base_key)
            .await
    }

    async fn delete_base_key(&self, address: &str, message_id: &str) -> StoreResult<()> {
        self.inner.delete_base_key(address, message_id).await?;
        let key = composite(&[address, message_id]);
        self.edit(|snapshot| {
            snapshot.base_keys.remove(&key);
        })
        .await;
        Ok(())
    }

    async fn update_device_list(
        &self,
        record: whatsapp_rust::store::DeviceListRecord,
    ) -> StoreResult<()> {
        self.inner.update_device_list(record.clone()).await?;
        let value = to_value(&record)?;
        let user = record.user.clone();
        self.edit(|snapshot| {
            snapshot.device_lists.insert(user, value);
        })
        .await;
        Ok(())
    }

    async fn update_device_lists(
        &self,
        records: Vec<whatsapp_rust::store::DeviceListRecord>,
    ) -> StoreResult<()> {
        self.inner.update_device_lists(records.clone()).await?;
        let rows: Vec<(String, serde_json::Value)> = records
            .iter()
            .map(|record| Ok((record.user.clone(), to_value(record)?)))
            .collect::<StoreResult<_>>()?;
        self.edit(|snapshot| {
            for (user, value) in rows {
                snapshot.device_lists.insert(user, value);
            }
        })
        .await;
        Ok(())
    }

    async fn get_devices(
        &self,
        user: &str,
    ) -> StoreResult<Option<whatsapp_rust::store::DeviceListRecord>> {
        self.inner.get_devices(user).await
    }

    async fn delete_devices(&self, user: &str) -> StoreResult<()> {
        self.inner.delete_devices(user).await?;
        self.edit(|snapshot| {
            snapshot.device_lists.remove(user);
        })
        .await;
        Ok(())
    }

    async fn get_group_metadata(&self, group_jid: &str) -> StoreResult<Option<Vec<u8>>> {
        self.inner.get_group_metadata(group_jid).await
    }

    async fn put_group_metadata(&self, group_jid: &str, blob: &[u8]) -> StoreResult<()> {
        self.inner.put_group_metadata(group_jid, blob).await?;
        let value = b64(blob);
        self.edit(|snapshot| {
            snapshot.group_metadata.insert(group_jid.to_string(), value);
        })
        .await;
        Ok(())
    }

    async fn delete_group_metadata(&self, group_jid: &str) -> StoreResult<()> {
        self.inner.delete_group_metadata(group_jid).await?;
        self.edit(|snapshot| {
            snapshot.group_metadata.remove(group_jid);
        })
        .await;
        Ok(())
    }

    async fn get_tc_token(&self, jid: &str) -> StoreResult<Option<TcTokenEntry>> {
        self.inner.get_tc_token(jid).await
    }

    async fn put_tc_token(&self, jid: &str, entry: &TcTokenEntry) -> StoreResult<()> {
        self.inner.put_tc_token(jid, entry).await?;
        let value = to_value(entry)?;
        self.edit(|snapshot| {
            snapshot.tc_tokens.insert(jid.to_string(), value);
        })
        .await;
        Ok(())
    }

    async fn delete_tc_token(&self, jid: &str) -> StoreResult<()> {
        self.inner.delete_tc_token(jid).await?;
        self.edit(|snapshot| {
            snapshot.tc_tokens.remove(jid);
        })
        .await;
        Ok(())
    }

    async fn get_all_tc_token_jids(&self) -> StoreResult<Vec<String>> {
        self.inner.get_all_tc_token_jids().await
    }

    async fn delete_expired_tc_tokens(
        &self,
        token_cutoff: i64,
        sender_cutoff: i64,
    ) -> StoreResult<u32> {
        let removed = self
            .inner
            .delete_expired_tc_tokens(token_cutoff, sender_cutoff)
            .await?;
        if removed > 0 {
            self.edit(|snapshot| {
                snapshot.tc_tokens.retain(|_, token| {
                    let Ok(entry) = serde_json::from_value::<TcTokenEntry>(token.clone()) else {
                        // Unreadable rows were already dropped by the in-memory
                        // backend, so keeping them would resurrect dead state.
                        return false;
                    };
                    let token_live =
                        !entry.token.is_empty() && entry.token_timestamp >= token_cutoff;
                    let sender_live = entry
                        .sender_timestamp
                        .is_some_and(|stamp| stamp >= sender_cutoff);
                    token_live || sender_live
                });
            })
            .await;
        }
        Ok(removed)
    }

    async fn touch_tc_token_sender_timestamp(
        &self,
        jid: &str,
        sender_timestamp: i64,
    ) -> StoreResult<()> {
        self.inner
            .touch_tc_token_sender_timestamp(jid, sender_timestamp)
            .await?;
        if let Some(entry) = self.inner.get_tc_token(jid).await? {
            let value = to_value(&entry)?;
            self.edit(|snapshot| {
                snapshot.tc_tokens.insert(jid.to_string(), value);
            })
            .await;
        }
        Ok(())
    }

    async fn store_received_tc_token(
        &self,
        jid: &str,
        token: &[u8],
        token_timestamp: i64,
    ) -> StoreResult<()> {
        self.inner
            .store_received_tc_token(jid, token, token_timestamp)
            .await?;
        if let Some(entry) = self.inner.get_tc_token(jid).await? {
            let value = to_value(&entry)?;
            self.edit(|snapshot| {
                snapshot.tc_tokens.insert(jid.to_string(), value);
            })
            .await;
        }
        Ok(())
    }

    async fn store_sent_message(
        &self,
        chat_jid: &str,
        message_id: &str,
        payload: &[u8],
    ) -> StoreResult<()> {
        self.inner
            .store_sent_message(chat_jid, message_id, payload)
            .await?;
        let key = composite(&[chat_jid, message_id]);
        let row = SentRow {
            created_at: now_secs(),
            payload: b64(payload),
        };
        self.edit(|snapshot| {
            snapshot.sent_messages.insert(key, row);
        })
        .await;
        Ok(())
    }

    async fn take_sent_message(
        &self,
        chat_jid: &str,
        message_id: &str,
    ) -> StoreResult<Option<Vec<u8>>> {
        let taken = self.inner.take_sent_message(chat_jid, message_id).await?;
        let key = composite(&[chat_jid, message_id]);
        self.edit(|snapshot| {
            snapshot.sent_messages.remove(&key);
        })
        .await;
        Ok(taken)
    }

    async fn delete_expired_sent_messages(&self, cutoff_timestamp: i64) -> StoreResult<u32> {
        let removed = self
            .inner
            .delete_expired_sent_messages(cutoff_timestamp)
            .await?;
        if removed > 0 {
            self.edit(|snapshot| {
                snapshot
                    .sent_messages
                    .retain(|_, row| row.created_at >= cutoff_timestamp);
            })
            .await;
        }
        Ok(removed)
    }

    async fn store_pending_inbound(
        &self,
        chat: &str,
        sender: &str,
        id: &str,
        message: &[u8],
    ) -> StoreResult<()> {
        self.inner
            .store_pending_inbound(chat, sender, id, message)
            .await?;
        let key = composite(&[chat, sender, id]);
        let row = PendingRow {
            stored_at: now_secs(),
            payload: b64(message),
        };
        self.edit(|snapshot| {
            snapshot.pending_inbound.insert(key, row);
        })
        .await;
        Ok(())
    }

    async fn get_pending_inbound(
        &self,
        chat: &str,
        sender: &str,
        id: &str,
    ) -> StoreResult<Option<Vec<u8>>> {
        self.inner.get_pending_inbound(chat, sender, id).await
    }

    async fn delete_pending_inbound(&self, chat: &str, sender: &str, id: &str) -> StoreResult<()> {
        self.inner.delete_pending_inbound(chat, sender, id).await?;
        let key = composite(&[chat, sender, id]);
        self.edit(|snapshot| {
            snapshot.pending_inbound.remove(&key);
        })
        .await;
        Ok(())
    }

    async fn delete_expired_pending_inbound(&self, cutoff_timestamp: i64) -> StoreResult<u32> {
        let removed = self
            .inner
            .delete_expired_pending_inbound(cutoff_timestamp)
            .await?;
        if removed > 0 {
            self.edit(|snapshot| {
                snapshot
                    .pending_inbound
                    .retain(|_, row| row.stored_at >= cutoff_timestamp);
            })
            .await;
        }
        Ok(removed)
    }

    async fn store_pending_inbound_batch(&self, rows: &[PendingInboundRow<'_>]) -> StoreResult<()> {
        self.inner.store_pending_inbound_batch(rows).await?;
        let stored_at = now_secs();
        let entries: Vec<(String, PendingRow)> = rows
            .iter()
            .map(|row| {
                (
                    composite(&[row.chat, row.sender, row.id]),
                    PendingRow {
                        stored_at,
                        payload: b64(row.message),
                    },
                )
            })
            .collect();
        self.edit(|snapshot| {
            for (key, row) in entries {
                snapshot.pending_inbound.insert(key, row);
            }
        })
        .await;
        Ok(())
    }

    async fn delete_pending_inbound_batch(
        &self,
        keys: &[PendingInboundKey<'_>],
    ) -> StoreResult<()> {
        self.inner.delete_pending_inbound_batch(keys).await?;
        let victims: Vec<String> = keys
            .iter()
            .map(|key| composite(&[key.chat, key.sender, key.id]))
            .collect();
        self.edit(|snapshot| {
            for key in victims {
                snapshot.pending_inbound.remove(&key);
            }
        })
        .await;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// MsgSecretStore
// ---------------------------------------------------------------------------

#[async_trait]
impl MsgSecretStore for FileBackend {
    async fn put_msg_secrets(&self, entries: Vec<MsgSecretEntry>) -> StoreResult<usize> {
        let stored = self.inner.put_msg_secrets(entries.clone()).await?;
        let rows: Vec<(String, SecretRow)> = entries
            .iter()
            .map(|entry| {
                let key = composite(&[&entry.chat, &entry.sender, &entry.msg_id]);
                (key, entry)
            })
            .map(|(key, entry)| {
                let row = SecretRow {
                    secret: b64(&entry.secret),
                    expires_at: entry.expires_at,
                    message_ts: entry.message_ts,
                };
                (key, row)
            })
            .collect();
        self.edit(|snapshot| {
            for (key, incoming) in rows {
                match snapshot.msg_secrets.get_mut(&key) {
                    Some(existing) => {
                        // Same merge rules the in-memory backend applies.
                        existing.secret = incoming.secret;
                        existing.expires_at =
                            merge_msg_secret_expiry(existing.expires_at, incoming.expires_at);
                        existing.message_ts =
                            merge_msg_secret_message_ts(existing.message_ts, incoming.message_ts);
                    }
                    None => {
                        snapshot.msg_secrets.insert(key, incoming);
                    }
                }
            }
        })
        .await;
        Ok(stored)
    }

    async fn get_msg_secret(
        &self,
        chat: &str,
        sender: &str,
        msg_id: &str,
    ) -> StoreResult<Option<Vec<u8>>> {
        self.inner.get_msg_secret(chat, sender, msg_id).await
    }

    async fn get_msg_secret_with_ts(
        &self,
        chat: &str,
        sender: &str,
        msg_id: &str,
    ) -> StoreResult<Option<(Vec<u8>, i64)>> {
        self.inner
            .get_msg_secret_with_ts(chat, sender, msg_id)
            .await
    }

    async fn delete_expired_msg_secrets(&self, cutoff_timestamp: i64) -> StoreResult<u32> {
        let removed = self
            .inner
            .delete_expired_msg_secrets(cutoff_timestamp)
            .await?;
        if removed > 0 {
            self.edit(|snapshot| {
                snapshot
                    .msg_secrets
                    .retain(|_, row| row.expires_at == 0 || row.expires_at > cutoff_timestamp);
            })
            .await;
        }
        Ok(removed)
    }
}

// ---------------------------------------------------------------------------
// DeviceStore
// ---------------------------------------------------------------------------

#[async_trait]
impl DeviceStore for FileBackend {
    async fn save(&self, device: &Device) -> StoreResult<()> {
        self.inner.save(device).await?;
        let value = to_value(device)?;
        self.edit(|snapshot| {
            snapshot.device = Some(value);
        })
        .await;
        Ok(())
    }

    async fn load(&self) -> StoreResult<Option<Device>> {
        self.inner.load().await
    }

    async fn exists(&self) -> StoreResult<bool> {
        self.inner.exists().await
    }

    async fn create(&self) -> StoreResult<i32> {
        let id = self.inner.create().await?;
        if let Some(device) = self.inner.load().await? {
            let value = to_value(&device)?;
            self.edit(|snapshot| {
                snapshot.device = Some(value);
            })
            .await;
        }
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use whatsapp_rust::wacore::store::traits::Backend;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("openwa-engine-test-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[tokio::test]
    async fn sessions_survive_a_reopen() {
        let dir = temp_dir("sessions");
        {
            let backend = FileBackend::open(&dir).await.unwrap();
            let _as_backend: Arc<dyn Backend> = backend.clone();
            backend
                .put_session("15550001:1@s.whatsapp.net", b"session")
                .await
                .unwrap();
            backend
                .put_identity("15550001@s.whatsapp.net", [7u8; 32])
                .await
                .unwrap();
            backend
                .put_group_metadata("120363000000000001@g.us", b"blob")
                .await
                .unwrap();
            backend.flush().await.unwrap();
        }

        let reopened = FileBackend::open(&dir).await.unwrap();
        let session = reopened
            .get_session("15550001:1@s.whatsapp.net")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(session.as_ref(), b"session");
        assert_eq!(
            reopened
                .load_identity("15550001@s.whatsapp.net")
                .await
                .unwrap(),
            Some([7u8; 32])
        );
        assert_eq!(
            reopened
                .get_group_metadata("120363000000000001@g.us")
                .await
                .unwrap()
                .as_deref(),
            Some(&b"blob"[..])
        );
    }

    #[tokio::test]
    async fn device_round_trips_and_latest_sync_key_is_preserved() {
        let dir = temp_dir("device");
        {
            let backend = FileBackend::open(&dir).await.unwrap();
            let mut device = Device::new();
            device.push_name = "openwa".to_string();
            backend.save(&device).await.unwrap();

            let first = whatsapp_rust::store::AppStateSyncKey {
                key_data: vec![1],
                fingerprint: vec![2],
                timestamp: 3,
            };
            let second = whatsapp_rust::store::AppStateSyncKey {
                key_data: vec![4],
                fingerprint: vec![5],
                timestamp: 6,
            };
            backend.set_sync_key(&[1, 1], first).await.unwrap();
            backend.set_sync_key(&[2, 2], second).await.unwrap();
            backend.flush().await.unwrap();
        }

        let reopened = FileBackend::open(&dir).await.unwrap();
        assert!(reopened.exists().await.unwrap());
        let device = reopened.load().await.unwrap().unwrap();
        assert_eq!(device.push_name, "openwa");
        let latest = reopened.get_latest_sync_key_id().await.unwrap().unwrap();
        assert_eq!(latest, vec![2, 2]);
        assert!(reopened.get_sync_key(&[1, 1]).await.unwrap().is_some());
    }
}
