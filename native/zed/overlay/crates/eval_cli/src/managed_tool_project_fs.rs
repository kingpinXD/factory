use std::{path::PathBuf, sync::Arc};

// The native engine's project layer must not auto-read host skills or open Git
// repositories. Managed tools use separate descriptor-confined file capabilities.
pub fn new() -> Arc<dyn fs::Fs> {
    Arc::new(ManagedProjectFs)
}
struct ManagedProjectFs;
struct NoWatcher;
impl fs::Watcher for NoWatcher {
    fn add(&self, _: &std::path::Path) -> anyhow::Result<()> {
        anyhow::bail!("managed project filesystem disabled")
    }
    fn remove(&self, _: &std::path::Path) -> anyhow::Result<()> {
        Ok(())
    }
}
#[async_trait::async_trait]
impl fs::Fs for ManagedProjectFs {
    async fn create_dir(&self, _: &std::path::Path) -> anyhow::Result<()> {
        anyhow::bail!("managed project filesystem disabled")
    }
    async fn create_symlink(&self, _: &std::path::Path, _: PathBuf) -> anyhow::Result<()> {
        anyhow::bail!("managed project filesystem disabled")
    }
    async fn create_file(&self, _: &std::path::Path, _: fs::CreateOptions) -> anyhow::Result<()> {
        anyhow::bail!("managed project filesystem disabled")
    }
    async fn create_file_with(
        &self,
        _: &std::path::Path,
        _: std::pin::Pin<&mut (dyn futures::AsyncRead + Send)>,
    ) -> anyhow::Result<()> {
        anyhow::bail!("managed project filesystem disabled")
    }
    async fn extract_tar_file(
        &self,
        _: &std::path::Path,
        _: async_tar::Archive<std::pin::Pin<&mut (dyn futures::AsyncRead + Send)>>,
    ) -> anyhow::Result<()> {
        anyhow::bail!("managed project filesystem disabled")
    }
    async fn copy_file(
        &self,
        _: &std::path::Path,
        _: &std::path::Path,
        _: fs::CopyOptions,
    ) -> anyhow::Result<()> {
        anyhow::bail!("managed project filesystem disabled")
    }
    async fn rename(
        &self,
        _: &std::path::Path,
        _: &std::path::Path,
        _: fs::RenameOptions,
    ) -> anyhow::Result<()> {
        anyhow::bail!("managed project filesystem disabled")
    }
    async fn remove_dir(&self, _: &std::path::Path, _: fs::RemoveOptions) -> anyhow::Result<()> {
        anyhow::bail!("managed project filesystem disabled")
    }
    async fn trash(
        &self,
        _: &std::path::Path,
        _: fs::RemoveOptions,
    ) -> anyhow::Result<fs::TrashId> {
        anyhow::bail!("managed project filesystem disabled")
    }
    async fn remove_file(&self, _: &std::path::Path, _: fs::RemoveOptions) -> anyhow::Result<()> {
        anyhow::bail!("managed project filesystem disabled")
    }
    async fn open_handle(&self, _: &std::path::Path) -> anyhow::Result<Arc<dyn fs::FileHandle>> {
        anyhow::bail!("managed project filesystem disabled")
    }
    async fn open_sync(
        &self,
        _: &std::path::Path,
    ) -> anyhow::Result<Box<dyn fs::ReadSeek + Send + Sync>> {
        anyhow::bail!("managed project filesystem disabled")
    }
    async fn load_bytes(&self, _: &std::path::Path) -> anyhow::Result<Vec<u8>> {
        anyhow::bail!("managed project filesystem disabled")
    }
    async fn atomic_write(&self, _: PathBuf, _: String) -> anyhow::Result<()> {
        anyhow::bail!("managed project filesystem disabled")
    }
    async fn save(
        &self,
        _: &std::path::Path,
        _: &rope::Rope,
        _: text::LineEnding,
    ) -> anyhow::Result<()> {
        anyhow::bail!("managed project filesystem disabled")
    }
    async fn write(&self, _: &std::path::Path, _: &[u8]) -> anyhow::Result<()> {
        anyhow::bail!("managed project filesystem disabled")
    }
    async fn canonicalize(&self, _: &std::path::Path) -> anyhow::Result<PathBuf> {
        anyhow::bail!("managed project filesystem disabled")
    }
    async fn is_file(&self, _: &std::path::Path) -> bool {
        false
    }
    async fn is_dir(&self, _: &std::path::Path) -> bool {
        false
    }
    async fn metadata(&self, _: &std::path::Path) -> anyhow::Result<Option<fs::Metadata>> {
        Ok(None)
    }
    async fn read_link(&self, _: &std::path::Path) -> anyhow::Result<PathBuf> {
        anyhow::bail!("managed project filesystem disabled")
    }
    async fn read_dir(
        &self,
        _: &std::path::Path,
    ) -> anyhow::Result<
        std::pin::Pin<Box<dyn Send + futures::Stream<Item = anyhow::Result<PathBuf>>>>,
    > {
        Ok(Box::pin(futures::stream::empty()))
    }
    fn path_exists(&self, _: &std::path::Path) -> bool {
        false
    }
    fn is_path_case_sensitive(&self, _: &std::path::Path) -> bool {
        true
    }
    fn requires_poll_watcher(&self, _: &std::path::Path) -> bool {
        false
    }
    async fn watch(
        &self,
        _: &std::path::Path,
        _: std::time::Duration,
    ) -> (
        std::pin::Pin<Box<dyn Send + futures::Stream<Item = Vec<fs::PathEvent>>>>,
        Arc<dyn fs::Watcher>,
    ) {
        (Box::pin(futures::stream::pending()), Arc::new(NoWatcher))
    }
    fn open_repo(
        &self,
        _: &std::path::Path,
        _: Option<&std::path::Path>,
    ) -> anyhow::Result<Arc<dyn git::repository::GitRepository>> {
        anyhow::bail!("managed host Git disabled")
    }
    async fn git_init(&self, _: &std::path::Path, _: String) -> anyhow::Result<()> {
        anyhow::bail!("managed host Git disabled")
    }
    async fn git_clone(&self, _: &std::path::Path, _: &str) -> anyhow::Result<()> {
        anyhow::bail!("managed host Git disabled")
    }
    async fn git_config(&self, _: &std::path::Path, _: Vec<String>) -> anyhow::Result<String> {
        anyhow::bail!("managed host Git disabled")
    }
    fn is_fake(&self) -> bool {
        false
    }
    async fn is_case_sensitive(&self) -> bool {
        true
    }
    fn subscribe_to_jobs(&self) -> fs::JobEventReceiver {
        let (_, receiver) = futures::channel::mpsc::unbounded();
        receiver
    }
    fn original_path_for_trash_id(&self, _: fs::TrashId) -> Option<PathBuf> {
        None
    }
    async fn restore(&self, _: fs::TrashId) -> Result<PathBuf, fs::TrashRestoreError> {
        Err(fs::TrashRestoreError::Unknown {
            description: "managed project filesystem disabled".into(),
        })
    }
}
