//! State persistence for cursors, cooldowns, and NoActivity states.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use txwatch_config::AppConfig;
use txwatch_rules::{CooldownTracker, NoActivityState};

/// Persisted state document containing cursors, cooldowns, and NoActivity states.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct StateDocument {
    #[serde(default)]
    pub cursors: HashMap<String, String>,
    #[serde(default)]
    pub cooldowns: HashMap<String, CooldownTracker>,
    #[serde(default)]
    pub no_activity: HashMap<String, Vec<NoActivityState>>,
}

impl StateDocument {
    pub fn from_cursors(cursors: HashMap<String, String>) -> Self {
        Self {
            cursors,
            cooldowns: HashMap::new(),
            no_activity: HashMap::new(),
        }
    }

    pub fn from_parts(
        cursors: HashMap<String, String>,
        cooldowns: HashMap<String, CooldownTracker>,
        no_activity: HashMap<String, Vec<NoActivityState>>,
    ) -> Self {
        Self {
            cursors,
            cooldowns,
            no_activity,
        }
    }
}

pub struct StateLock(Option<PathBuf>);

impl Drop for StateLock {
    fn drop(&mut self) {
        if let Some(path) = &self.0 {
            let _ = fs::remove_file(path);
        }
    }
}

pub fn acquire_state_lock(cursor_file: Option<&str>) -> Result<StateLock> {
    let Some(path_str) = cursor_file else {
        return Ok(StateLock(None));
    };
    let lock_path = PathBuf::from(format!("{}.lock", path_str));
    if let Some(parent) = lock_path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let _ = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&lock_path)
        .with_context(|| format!("failed to acquire lock '{}'", lock_path.display()))?;
    Ok(StateLock(Some(lock_path)))
}

pub fn load_state(cfg: &AppConfig) -> StateDocument {
    let path = cfg.state_file.as_deref().or(cfg.cursor_file.as_deref());
    let Some(path) = path else {
        return StateDocument::default();
    };
    let Ok(content) = fs::read_to_string(path) else {
        return StateDocument::default();
    };
    if let Ok(doc) = serde_json::from_str::<StateDocument>(&content) {
        return doc;
    }
    if let Ok(cursors) = serde_json::from_str::<HashMap<String, String>>(&content) {
        return StateDocument::from_cursors(cursors);
    }
    StateDocument::default()
}

pub fn save_state(cfg: &AppConfig, doc: &StateDocument) -> Result<()> {
    let path = cfg.state_file.as_deref().or(cfg.cursor_file.as_deref());
    let Some(path) = path else {
        return Ok(());
    };
    write_state_blocking(Path::new(path), doc)
}

/// Write `doc` to `path` atomically and durably: a temp file in the same
/// directory is written and fsynced, then renamed over the target, and the
/// directory is fsynced so the rename itself survives a crash or power loss.
/// An interrupted write therefore leaves the previous file intact rather than
/// an empty or truncated one.
///
/// This is synchronous and must only be called from a blocking context.
pub fn write_state_blocking(path: &Path, doc: &StateDocument) -> Result<()> {
    use std::io::Write;

    let raw = serde_json::to_string_pretty(doc).context("failed to serialize state document")?;
    let tmp = path.with_extension("tmp");
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let mut file = fs::File::create(&tmp)
        .with_context(|| format!("failed to write state file '{}'", tmp.display()))?;
    file.write_all(raw.as_bytes())
        .with_context(|| format!("failed to write state file '{}'", tmp.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to fsync state file '{}'", tmp.display()))?;
    drop(file);
    fs::rename(&tmp, path)
        .with_context(|| format!("failed to rename state file '{}'", path.display()))?;
    sync_parent_dir(path)?;
    Ok(())
}

/// Fsync the directory containing `path` so a preceding rename is durable.
#[cfg(unix)]
fn sync_parent_dir(path: &Path) -> Result<()> {
    let dir = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .with_context(|| format!("failed to fsync directory '{}'", dir.display()))
}

/// Directories cannot be opened for fsync on this platform; the rename is
/// already durable once it returns.
#[cfg(not(unix))]
fn sync_parent_dir(_path: &Path) -> Result<()> {
    Ok(())
}

#[derive(Debug, Clone)]
pub struct StateUpdate {
    pub cursor_key: String,
    pub cursor: String,
    pub cooldowns: CooldownTracker,
    pub no_activity: Vec<NoActivityState>,
}

#[derive(Clone)]
pub struct StateHandle {
    sender: Option<tokio::sync::mpsc::UnboundedSender<StateUpdate>>,
}

impl StateHandle {
    pub fn send(&self, update: StateUpdate) {
        if let Some(s) = &self.sender {
            let _ = s.send(update);
        }
    }

    pub async fn flush(&self) {
        tokio::task::yield_now().await;
    }
}

pub struct StateWriter {
    handle: StateHandle,
    join_handle: Option<tokio::task::JoinHandle<()>>,
}

impl StateWriter {
    pub fn handle(&self) -> StateHandle {
        self.handle.clone()
    }

    pub async fn shutdown(mut self) {
        self.handle.sender.take();
        if let Some(join) = self.join_handle.take() {
            let _ = join.await;
        }
    }
}

pub fn spawn_state_writer(cursor_file: Option<&str>) -> StateWriter {
    let Some(path_str) = cursor_file else {
        return StateWriter {
            handle: StateHandle { sender: None },
            join_handle: None,
        };
    };
    let path = PathBuf::from(path_str);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<StateUpdate>();
    let join = tokio::spawn(async move {
        let mut current_doc = StateDocument::default();
        while let Some(update) = rx.recv().await {
            current_doc.cursors.insert(update.cursor_key.clone(), update.cursor);
            current_doc.cooldowns.insert(update.cursor_key.clone(), update.cooldowns);
            current_doc.no_activity.insert(update.cursor_key, update.no_activity);
            let _ = write_state_blocking(&path, &current_doc);
        }
    });
    StateWriter {
        handle: StateHandle { sender: Some(tx) },
        join_handle: Some(join),
    }
}
