//! Shared application state.

use std::path::PathBuf;
use std::sync::Arc;

use crate::cluster::ClusterConfig;
use crate::db::Db;
use crate::engine::Engine;
use crate::media::MediaStore;

#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub media: MediaStore,
    pub engine: Engine,
    pub http: reqwest::Client,
    pub dashboard_dir: Arc<PathBuf>,
    pub cluster: ClusterConfig,
}

impl AppState {
    /// Whether `session_id` is owned by this node (or unknown -> treated as local).
    pub async fn is_local_owner(&self, session_id: &str) -> bool {
        match self.db.get_session_owner(session_id).await {
            Ok(Some(owner)) => owner.trim().is_empty() || owner == self.cluster.node_id,
            _ => true,
        }
    }

    /// Peer URL that owns `session_id`, if it is not this node.
    pub async fn owner_url(&self, session_id: &str) -> Option<String> {
        if self.is_local_owner(session_id).await {
            return None;
        }
        let owner = self.db.get_session_owner(session_id).await.ok().flatten()?;
        if owner.trim().is_empty() {
            None
        } else {
            self.cluster.peer_url_for(&owner)
        }
    }
}
