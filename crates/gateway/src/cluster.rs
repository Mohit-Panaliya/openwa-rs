//! Multi-node cluster configuration and ownership helpers.

use std::env;

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub node_id: String,
    pub peers: Vec<String>,
}

impl ClusterConfig {
    pub fn from_env() -> Self {
        let node_id = env::var("NODE_ID").unwrap_or_else(|_| "node-1".to_string());
        let peers = env::var("PEER_NODES")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .collect();
        Self { node_id, peers }
    }

    pub fn default_node_id() -> String {
        env::var("NODE_ID").unwrap_or_else(|_| "node-1".to_string())
    }

    /// Map an owning node id to a peer URL (best-effort).
    pub fn peer_url_for(&self, owner: &str) -> Option<String> {
        if owner == self.node_id {
            return None;
        }
        if owner.starts_with("http://") || owner.starts_with("https://") {
            return Some(owner.to_string());
        }
        for peer in &self.peers {
            if peer.contains(owner) {
                return Some(peer.clone());
            }
        }
        self.peers.first().cloned()
    }

    /// Sync best-effort check: without a database this defaults to local.
    pub fn is_local_owner(&self, _session_id: &str) -> bool {
        true
    }

    /// Sync best-effort owner URL lookup: without a database this returns None.
    pub fn owner_url(&self, _session_id: &str) -> Option<String> {
        None
    }
}
