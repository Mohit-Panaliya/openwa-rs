//! Local media store: writes blobs under `data/media` and records them in SQLite.

use std::path::{Path, PathBuf};

use base64::Engine as _;

#[derive(Clone)]
pub struct MediaStore {
    root: PathBuf,
}

impl MediaStore {
    pub fn new(root: PathBuf) -> Self {
        if let Err(error) = std::fs::create_dir_all(&root) {
            tracing::warn!(%error, path = %root.display(), "could not create media directory");
        }
        Self { root }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Decode base64 media and write it to disk.
    /// Returns `(id, absolute path, byte length)`.
    pub async fn save_base64(
        &self,
        media_type: &str,
        mimetype: Option<&str>,
        base64_payload: &str,
    ) -> anyhow::Result<(String, PathBuf, i64)> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(base64_payload.trim())
            .map_err(|error| anyhow::anyhow!("invalid base64 payload: {error}"))?;

        let id = crate::util::random_hex(16);
        let extension = extension_for(media_type, mimetype);
        let path = self.root.join(format!("{id}.{extension}"));
        tokio::fs::write(&path, &bytes).await?;
        Ok((id, path, bytes.len() as i64))
    }

    pub async fn load(&self, path: &Path) -> std::io::Result<Vec<u8>> {
        tokio::fs::read(path).await
    }
}

/// Best-effort extension from the declared mime type, falling back to the media kind.
pub fn extension_for(media_type: &str, mimetype: Option<&str>) -> String {
    let subtype = mimetype
        .and_then(|value| value.split('/').next_back())
        .map(|value| {
            value
                .split(';')
                .next()
                .unwrap_or(value)
                .trim()
                .to_lowercase()
        });
    match subtype.as_deref() {
        Some("jpeg") => "jpg".to_string(),
        Some("png") | Some("gif") | Some("webp") | Some("pdf") | Some("mp4") | Some("mpeg")
        | Some("ogg") => subtype.unwrap_or_else(|| "bin".to_string()),
        _ => match media_type {
            "image" => "jpg",
            "video" => "mp4",
            "audio" => "ogg",
            _ => "bin",
        }
        .to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extension_prefers_mimetype() {
        assert_eq!(extension_for("image", Some("image/png")), "png");
        assert_eq!(extension_for("image", Some("image/jpeg")), "jpg");
        assert_eq!(extension_for("document", Some("application/pdf")), "pdf");
        assert_eq!(extension_for("document", None), "bin");
        assert_eq!(
            extension_for("audio", Some("audio/ogg; codecs=opus")),
            "ogg"
        );
    }
}
