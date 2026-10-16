//! Factory-scoped storage for native OAuth. Storage separation does not prove grant independence.
use super::*;
use futures::future::LocalBoxFuture;
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _};
use std::{
    fs::File,
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

pub const NATIVE_REFRESH_TIMEOUT: Duration = Duration::from_secs(30);

/// Resolves the OS account's real home, never a worker's HOME override.
pub fn credential_lock_path() -> Result<PathBuf, NativeAuthError> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;
        let mut account: libc::passwd = unsafe { std::mem::zeroed() };
        let mut result = std::ptr::null_mut();
        let mut buffer = vec![0u8; 65536];
        let status = unsafe {
            libc::getpwuid_r(
                libc::geteuid(),
                &mut account,
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                &mut result,
            )
        };
        if status != 0 || result.is_null() || account.pw_dir.is_null() {
            return Err(NativeAuthError::MutationLockFailed);
        }
        let home = PathBuf::from(std::ffi::OsStr::from_bytes(
            unsafe { std::ffi::CStr::from_ptr(account.pw_dir) }.to_bytes(),
        ))
        .canonicalize()
        .map_err(|_| NativeAuthError::MutationLockFailed)?;
        let factory = home.join(".factory");
        let metadata =
            std::fs::symlink_metadata(&factory).map_err(|_| NativeAuthError::MutationLockFailed)?;
        if !metadata.is_dir()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o022 != 0
        {
            return Err(NativeAuthError::MutationLockFailed);
        }
        Ok(factory
            .canonicalize()
            .map_err(|_| NativeAuthError::MutationLockFailed)?
            .join("native-auth/factory-openai/credential.lock"))
    }
    #[cfg(not(unix))]
    {
        Err(NativeAuthError::MutationLockFailed)
    }
}

#[cfg(unix)]
fn open_lock(path: &Path) -> Result<File, NativeAuthError> {
    let directory = path.parent().ok_or(NativeAuthError::MutationLockFailed)?;
    let mut missing = Vec::new();
    let mut ancestor = directory;
    while !ancestor.exists() {
        missing.push(ancestor.to_path_buf());
        ancestor = ancestor
            .parent()
            .ok_or(NativeAuthError::MutationLockFailed)?;
    }
    for directory in missing.into_iter().rev() {
        match std::fs::DirBuilder::new().mode(0o700).create(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => return Err(NativeAuthError::MutationLockFailed),
        }
    }
    // Check every factory-owned directory, not the user's pre-existing home/temp parent.
    for directory in directory.ancestors().take(2) {
        let metadata = std::fs::symlink_metadata(directory)
            .map_err(|_| NativeAuthError::MutationLockFailed)?;
        if !metadata.is_dir()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o777 != 0o700
        {
            return Err(NativeAuthError::MutationLockFailed);
        }
    }
    let root = directory
        .ancestors()
        .nth(2)
        .ok_or(NativeAuthError::MutationLockFailed)?;
    let metadata =
        std::fs::symlink_metadata(root).map_err(|_| NativeAuthError::MutationLockFailed)?;
    if !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o022 != 0
    {
        return Err(NativeAuthError::MutationLockFailed);
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| NativeAuthError::MutationLockFailed)?;
    let metadata = file
        .metadata()
        .map_err(|_| NativeAuthError::MutationLockFailed)?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o777 != 0o600
        || metadata.nlink() != 1
    {
        return Err(NativeAuthError::MutationLockFailed);
    }
    Ok(file)
}

#[cfg(not(unix))]
fn open_lock(_: &Path) -> Result<File, NativeAuthError> {
    Err(NativeAuthError::MutationLockFailed)
}

pub const FACTORY_CREDENTIAL_NAMESPACE: &str = "factory-openai";
const FACTORY_CREDENTIALS_KEY: &str = "https://factory.zed.native/openai-subscribed/v1";
pub const CREDENTIAL_LOAD_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeAuthError {
    InvalidNamespace,
    CredentialLoadTimeout,
    CredentialReadFailed,
    InvalidCredentials,
    CredentialWriteFailed,
    CredentialDeleteFailed,
    MissingSetup,
    IdentityMismatch,
    SignInFailed,
    RuntimeUnavailable,
    CatalogFailed,
    CatalogTimeout,
    MutationLockFailed,
    MutationLockTimeout,
    StaleGrant,
    RefreshTimeout,
    RefreshFailed,
    RefreshRejected,
    MutationUncertain,
}
impl std::fmt::Display for NativeAuthError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Only fixed variants, never provider errors or identity material.
        write!(formatter, "{self:?}")
    }
}
impl std::error::Error for NativeAuthError {}

#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct CredentialMetadata {
    pub namespace: &'static str,
    pub nonce: String,
    pub identity_binding: String,
    #[serde(skip)]
    account: String,
    #[serde(skip)]
    revision: String,
}

impl std::fmt::Debug for CredentialMetadata {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CredentialMetadata")
            .field("namespace", &self.namespace)
            .field("nonce", &self.nonce)
            .field("identity_binding", &self.identity_binding)
            .finish_non_exhaustive()
    }
}

#[derive(Serialize, Deserialize)]
struct StoredGrant {
    version: u8,
    nonce: String,
    credentials: CodexCredentials,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationIntent {
    version: u8,
    operation: MutationOperation,
    grant_digest: String,
    revision_digest: String,
    intended_state_digest: String,
}
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MutationOperation {
    Write,
    Delete,
}

fn intent_path(lock_path: &Path) -> PathBuf {
    lock_path.with_file_name("mutation-intent.json")
}
fn digest(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(bytes))
}
fn state_digest(record: Option<&StoredGrant>) -> Result<String, NativeAuthError> {
    match record {
        Some(record) => serde_json::to_vec(record)
            .map(|bytes| digest(&bytes))
            .map_err(|_| NativeAuthError::MutationUncertain),
        None => Ok(digest(b"factory-native-credential-absent-v1")),
    }
}
fn durable_intent_exists(lock_path: &Path) -> bool {
    match std::fs::symlink_metadata(intent_path(lock_path)) {
        Ok(_) => true,
        Err(error) => error.kind() != std::io::ErrorKind::NotFound,
    }
}
#[cfg(unix)]
fn read_intent(lock_path: &Path) -> Result<Option<MutationIntent>, NativeAuthError> {
    let file = match std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(intent_path(lock_path))
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(NativeAuthError::MutationUncertain),
    };
    let metadata = file
        .metadata()
        .map_err(|_| NativeAuthError::MutationUncertain)?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o777 != 0o600
        || metadata.nlink() != 1
        || metadata.len() > 4096
    {
        return Err(NativeAuthError::MutationUncertain);
    }
    let mut bytes = Vec::new();
    file.take(4097)
        .read_to_end(&mut bytes)
        .map_err(|_| NativeAuthError::MutationUncertain)?;
    let intent: MutationIntent =
        serde_json::from_slice(&bytes).map_err(|_| NativeAuthError::MutationUncertain)?;
    if intent.version != 1
        || [
            &intent.grant_digest,
            &intent.revision_digest,
            &intent.intended_state_digest,
        ]
        .into_iter()
        .any(|value| {
            URL_SAFE_NO_PAD
                .decode(value)
                .ok()
                .is_none_or(|value| value.len() != 32)
        })
    {
        return Err(NativeAuthError::MutationUncertain);
    }
    Ok(Some(intent))
}
#[cfg(not(unix))]
fn read_intent(_: &Path) -> Result<Option<MutationIntent>, NativeAuthError> {
    Err(NativeAuthError::MutationUncertain)
}
fn sync_intent_directory(lock_path: &Path) -> Result<(), NativeAuthError> {
    // Persist newly created auth directories as well as the intent's own entry.
    for directory in lock_path
        .parent()
        .ok_or(NativeAuthError::MutationUncertain)?
        .ancestors()
        .take(3)
    {
        File::open(directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| NativeAuthError::MutationUncertain)?;
    }
    Ok(())
}

#[cfg(unix)]
fn create_intent(lock_path: &Path, intent: &MutationIntent) -> Result<(), NativeAuthError> {
    let bytes = serde_json::to_vec(intent).map_err(|_| NativeAuthError::MutationUncertain)?;
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(intent_path(lock_path))
        .map_err(|_| NativeAuthError::MutationUncertain)?;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|_| NativeAuthError::MutationUncertain)?;
    sync_intent_directory(lock_path)
}
#[cfg(not(unix))]
fn create_intent(_: &Path, _: &MutationIntent) -> Result<(), NativeAuthError> {
    Err(NativeAuthError::MutationUncertain)
}

async fn read_stored(
    provider: &dyn CredentialsProvider,
    cx: &AsyncApp,
) -> Result<Option<StoredGrant>, NativeAuthError> {
    let read = provider.read_credentials(FACTORY_CREDENTIALS_KEY, cx);
    let deadline = cx.background_executor().timer(CREDENTIAL_LOAD_TIMEOUT);
    let stored = futures::select_biased! {
        () = deadline.fuse() => return Err(NativeAuthError::CredentialLoadTimeout),
        result = read.fuse() => result.map_err(|_| NativeAuthError::CredentialReadFailed)?,
    };
    stored
        .map(|(_, bytes)| {
            let record: StoredGrant =
                serde_json::from_slice(&bytes).map_err(|_| NativeAuthError::InvalidCredentials)?;
            if record.version != 1
                || record.nonce.len() != 64
                || !record.nonce.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(NativeAuthError::InvalidCredentials);
            }
            NativeCredentials::account(&record.credentials)?;
            Ok(record)
        })
        .transpose()
}
async fn reconcile_intent(
    provider: &dyn CredentialsProvider,
    lock_path: &Path,
    cx: &AsyncApp,
) -> Result<(), NativeAuthError> {
    let Some(intent) = read_intent(lock_path)? else {
        return Ok(());
    };
    let stored = read_stored(provider, cx)
        .await
        .map_err(|_| NativeAuthError::MutationUncertain)?;
    if state_digest(stored.as_ref())? != intent.intended_state_digest {
        return Err(NativeAuthError::MutationUncertain);
    }
    std::fs::remove_file(intent_path(lock_path)).map_err(|_| NativeAuthError::MutationUncertain)?;
    sync_intent_directory(lock_path)
}

/// Share this wrapper and one State between native worker/inspection callers.
/// It deliberately implements no fallback or migration from desktop credentials.
pub struct NativeCredentials {
    inner: Arc<dyn CredentialsProvider>,
    metadata: Arc<Mutex<Option<CredentialMetadata>>>,
    load_error: Arc<Mutex<Option<NativeAuthError>>>,
    lock_path: PathBuf,
    setup_pending: Arc<AtomicBool>,
    pending_mutations: Arc<AtomicUsize>,
}

impl NativeCredentials {
    pub fn new(
        inner: Arc<dyn CredentialsProvider>,
        namespace: &str,
    ) -> Result<Arc<Self>, NativeAuthError> {
        if namespace != FACTORY_CREDENTIAL_NAMESPACE {
            return Err(NativeAuthError::InvalidNamespace);
        }
        Self::new_at(inner, namespace, credential_lock_path()?)
    }

    fn new_at(
        inner: Arc<dyn CredentialsProvider>,
        namespace: &str,
        lock_path: PathBuf,
    ) -> Result<Arc<Self>, NativeAuthError> {
        if namespace != FACTORY_CREDENTIAL_NAMESPACE {
            return Err(NativeAuthError::InvalidNamespace);
        }
        Ok(Arc::new(Self {
            inner,
            metadata: Arc::new(Mutex::new(None)),
            load_error: Arc::new(Mutex::new(None)),
            lock_path,
            setup_pending: Arc::new(AtomicBool::new(false)),
            pending_mutations: Arc::new(AtomicUsize::new(0)),
        }))
    }

    /// A timed-out waiter does not prove the underlying provider mutation stopped.
    /// Shutdown must retain this runtime until detached persistence has settled.
    pub fn has_pending_mutation(&self) -> bool {
        self.pending_mutations.load(Ordering::SeqCst) != 0 || durable_intent_exists(&self.lock_path)
    }

    pub fn metadata(&self) -> Option<CredentialMetadata> {
        self.metadata
            .lock()
            .expect("metadata lock poisoned")
            .clone()
    }

    pub fn load_error(&self) -> Option<NativeAuthError> {
        *self.load_error.lock().expect("load error lock poisoned")
    }

    fn check_key(key: &str) -> Result<(), NativeAuthError> {
        if key == CREDENTIALS_KEY {
            Ok(())
        } else {
            Err(NativeAuthError::InvalidNamespace)
        }
    }

    fn metadata_for(nonce: String, account: &str) -> CredentialMetadata {
        let mut hash = Sha256::new();
        hash.update(b"factory-native-binding-v1\0");
        hash.update(nonce.as_bytes());
        hash.update(b"\0");
        hash.update(account.as_bytes());
        CredentialMetadata {
            namespace: FACTORY_CREDENTIAL_NAMESPACE,
            nonce,
            identity_binding: URL_SAFE_NO_PAD.encode(hash.finalize()),
            account: account.into(),
            revision: String::new(),
        }
    }

    fn account(credentials: &CodexCredentials) -> Result<&str, NativeAuthError> {
        credentials
            .account_id
            .as_deref()
            .filter(|account| !account.is_empty())
            .ok_or(NativeAuthError::InvalidCredentials)
    }

    /// Call only in the explicit setup executable, after the initial load completes.
    /// Each new authorization rotates the local binding, even for the same account.
    pub fn begin_authorized_setup(&self) {
        self.setup_pending.store(true, Ordering::SeqCst);
    }

    /// Read-only native settlement proof; never erases an unproven intent or writes credentials.
    pub async fn settle_pending_mutation(&self, cx: &AsyncApp) -> Result<(), NativeAuthError> {
        let _lock = self.lock(cx).await?;
        Ok(())
    }

    async fn lock(&self, cx: &AsyncApp) -> Result<Arc<File>, NativeAuthError> {
        let lock = self.acquire_lock(cx).await?;
        reconcile_intent(self.inner.as_ref(), &self.lock_path, cx).await?;
        Ok(lock)
    }

    async fn acquire_lock(&self, cx: &AsyncApp) -> Result<Arc<File>, NativeAuthError> {
        let file = open_lock(&self.lock_path)?;
        let wait = async {
            loop {
                match file.try_lock() {
                    Ok(()) => return Ok(Arc::new(file)),
                    Err(std::fs::TryLockError::WouldBlock) => {
                        cx.background_executor()
                            .timer(Duration::from_millis(10))
                            .await
                    }
                    Err(_) => return Err(NativeAuthError::MutationLockFailed),
                }
            }
        };
        let deadline = cx.background_executor().timer(CREDENTIAL_LOAD_TIMEOUT);
        futures::select_biased! {
            () = deadline.fuse() => Err(NativeAuthError::MutationLockTimeout),
            result = wait.fuse() => result,
        }
    }

    async fn stored(&self, cx: &AsyncApp) -> Result<Option<StoredGrant>, NativeAuthError> {
        read_stored(self.inner.as_ref(), cx).await
    }

    fn journal_intent(
        &self,
        operation: MutationOperation,
        intended: Option<&StoredGrant>,
    ) -> Result<(), NativeAuthError> {
        let metadata = self.metadata();
        create_intent(
            &self.lock_path,
            &MutationIntent {
                version: 1,
                operation,
                grant_digest: digest(
                    metadata
                        .as_ref()
                        .map(|value| value.nonce.as_bytes())
                        .unwrap_or(b"absent"),
                ),
                revision_digest: digest(
                    metadata
                        .as_ref()
                        .map(|value| value.revision.as_bytes())
                        .unwrap_or(b"absent"),
                ),
                intended_state_digest: state_digest(intended)?,
            },
        )
    }

    fn revision(credentials: &CodexCredentials) -> Result<String, NativeAuthError> {
        let bytes =
            serde_json::to_vec(credentials).map_err(|_| NativeAuthError::InvalidCredentials)?;
        Ok(URL_SAFE_NO_PAD.encode(Sha256::digest(bytes)))
    }

    async fn check_stored(
        &self,
        credentials: Option<&CodexCredentials>,
        cx: &AsyncApp,
    ) -> Result<(), NativeAuthError> {
        let stored = self.stored(cx).await?;
        let metadata = self.metadata();
        if stored.as_ref().map(|grant| grant.nonce.as_str())
            != metadata.as_ref().map(|metadata| metadata.nonce.as_str())
        {
            return Err(NativeAuthError::StaleGrant);
        }
        if let (Some(stored), Some(metadata)) = (&stored, &metadata) {
            if Self::revision(&stored.credentials)? != metadata.revision {
                return Err(NativeAuthError::StaleGrant);
            }
        }
        if let Some(credentials) = credentials {
            if stored.as_ref().map(|grant| &grant.credentials) != Some(credentials) {
                return Err(NativeAuthError::StaleGrant);
            }
        }
        Ok(())
    }

    async fn persist(
        &self,
        bytes: &[u8],
        lock: Arc<File>,
        cx: &AsyncApp,
    ) -> Result<(), NativeAuthError> {
        self.check_stored(None, cx).await?;
        let credentials: CodexCredentials =
            serde_json::from_slice(bytes).map_err(|_| NativeAuthError::InvalidCredentials)?;
        let account = Self::account(&credentials)?;
        let setup = self.setup_pending.load(Ordering::SeqCst);
        let mut metadata = if setup {
            let mut nonce = [0u8; 32];
            rand::rng().fill_bytes(&mut nonce);
            Self::metadata_for(
                nonce.iter().map(|byte| format!("{byte:02x}")).collect(),
                account,
            )
        } else {
            let metadata = self.metadata().ok_or(NativeAuthError::MissingSetup)?;
            if metadata.account != account {
                return Err(NativeAuthError::IdentityMismatch);
            }
            metadata
        };
        metadata.revision = Self::revision(&credentials)?;
        let record = StoredGrant {
            version: 1,
            nonce: metadata.nonce.clone(),
            credentials,
        };
        if !setup
            && self
                .metadata()
                .is_some_and(|old| old.revision == metadata.revision)
        {
            return Ok(());
        }
        self.journal_intent(MutationOperation::Write, Some(&record))?;
        let bytes = serde_json::to_vec(&record).map_err(|_| NativeAuthError::InvalidCredentials)?;
        let provider = self.inner.clone();
        let lock_path = self.lock_path.clone();
        let shared_metadata = self.metadata.clone();
        let load_error = self.load_error.clone();
        let setup_pending = self.setup_pending.clone();
        let pending = self.pending_mutations.clone();
        pending.fetch_add(1, Ordering::SeqCst);
        let (send, receive) = futures::channel::oneshot::channel();
        cx.spawn(async move |cx| {
            let _lock = lock;
            if provider
                .write_credentials(FACTORY_CREDENTIALS_KEY, "Bearer", &bytes, cx)
                .await
                .is_err()
            {
                log::warn!("native_credential_write_completion_unknown");
            }
            // A returned error may still have committed. Only the exact native state proves settlement.
            let result = reconcile_intent(provider.as_ref(), &lock_path, cx).await;
            if result.is_ok() {
                *shared_metadata.lock().expect("metadata lock poisoned") = Some(metadata);
                *load_error.lock().expect("load error lock poisoned") = None;
                setup_pending.store(false, Ordering::SeqCst);
            }
            // The waiter may have timed out; persistence still owns the lock until settled.
            pending.fetch_sub(1, Ordering::SeqCst);
            if send.send(result).is_err() {
                return;
            }
        })
        .detach();
        receive
            .await
            .map_err(|_| NativeAuthError::RuntimeUnavailable)?
    }

    async fn delete_locked(&self, _lock: Arc<File>, cx: &AsyncApp) -> Result<(), NativeAuthError> {
        self.check_stored(None, cx).await?;
        if self.metadata().is_none() {
            return Ok(());
        }
        self.journal_intent(MutationOperation::Delete, None)?;
        let provider = self.inner.clone();
        let lock_path = self.lock_path.clone();
        let metadata = self.metadata.clone();
        let pending = self.pending_mutations.clone();
        pending.fetch_add(1, Ordering::SeqCst);
        let (send, receive) = futures::channel::oneshot::channel();
        cx.spawn(async move |cx| {
            let _lock = _lock;
            if provider
                .delete_credentials(FACTORY_CREDENTIALS_KEY, cx)
                .await
                .is_err()
            {
                log::warn!("native_credential_delete_completion_unknown");
            }
            let result = reconcile_intent(provider.as_ref(), &lock_path, cx).await;
            if result.is_ok() {
                *metadata.lock().expect("metadata lock poisoned") = None;
            }
            pending.fetch_sub(1, Ordering::SeqCst);
            if send.send(result).is_err() {
                return;
            }
        })
        .detach();
        receive
            .await
            .map_err(|_| NativeAuthError::RuntimeUnavailable)?
    }
}

impl CredentialsProvider for NativeCredentials {
    fn read_credentials<'a>(
        &'a self,
        key: &'a str,
        cx: &'a AsyncApp,
    ) -> LocalBoxFuture<'a, Result<Option<(String, Vec<u8>)>>> {
        async move {
            Self::check_key(key)?;
            let uncertain = durable_intent_exists(&self.lock_path);
            let deadline = cx.background_executor().timer(CREDENTIAL_LOAD_TIMEOUT);
            let load = async {
                let _lock = if durable_intent_exists(&self.lock_path) { Some(self.lock(cx).await?) } else { None };
                let Some(record) = self.stored(cx).await? else {
                    *self.metadata.lock().expect("metadata lock poisoned") = None;
                    return Ok(None);
                };
                let mut metadata =
                    Self::metadata_for(record.nonce, Self::account(&record.credentials)?);
                let bytes = serde_json::to_vec(&record.credentials)
                    .map_err(|_| NativeAuthError::InvalidCredentials)?;
                metadata.revision = Self::revision(&record.credentials)?;
                *self.metadata.lock().expect("metadata lock poisoned") = Some(metadata);
                Ok(Some(("Bearer".into(), bytes)))
            };
            let result = futures::select_biased! {
                () = deadline.fuse() => Err(if uncertain { NativeAuthError::MutationUncertain } else { NativeAuthError::CredentialLoadTimeout }),
                result = load.fuse() => result,
            };
            *self.load_error.lock().expect("load error lock poisoned") =
                result.as_ref().err().copied();
            result.map_err(Into::into)
        }
        .boxed_local()
    }

    fn write_credentials<'a>(
        &'a self,
        key: &'a str,
        _: &'a str,
        bytes: &'a [u8],
        cx: &'a AsyncApp,
    ) -> LocalBoxFuture<'a, Result<()>> {
        async move {
            Self::check_key(key)?;
            let lock = self.lock(cx).await?;
            self.persist(bytes, lock, cx).await.map_err(Into::into)
        }
        .boxed_local()
    }

    fn delete_credentials<'a>(
        &'a self,
        key: &'a str,
        cx: &'a AsyncApp,
    ) -> LocalBoxFuture<'a, Result<()>> {
        async move {
            Self::check_key(key)?;
            let lock = self.lock(cx).await?;
            self.delete_locked(lock, cx).await.map_err(Into::into)
        }
        .boxed_local()
    }
}

pub(super) async fn refresh_native_state(
    state: &WeakEntity<State>,
    provider: Arc<NativeCredentials>,
    http: &Arc<dyn HttpClient>,
    original: &CodexCredentials,
    generation: u64,
    cx: &mut AsyncApp,
) -> Result<CodexCredentials, Arc<anyhow::Error>> {
    let deadline = cx.background_executor().timer(NATIVE_REFRESH_TIMEOUT);
    let operation = async {
        let lock = provider.lock(cx).await?;
        provider.check_stored(Some(original), cx).await?;
        match refresh_token(http, &original.refresh_token).await {
            Ok(credentials) => {
                let current_generation = state
                    .read_with(cx, |state, _| state.auth_generation)
                    .map_err(|_| NativeAuthError::RuntimeUnavailable)?;
                if current_generation != generation {
                    return Err(NativeAuthError::StaleGrant);
                }
                let bytes = serde_json::to_vec(&credentials)
                    .map_err(|_| NativeAuthError::InvalidCredentials)?;
                provider.persist(&bytes, lock, cx).await?;
                Ok(credentials)
            }
            Err(RefreshError::Fatal(_)) => {
                provider.delete_locked(lock, cx).await?;
                Err(NativeAuthError::RefreshRejected)
            }
            Err(RefreshError::Transient(_)) => Err(NativeAuthError::RefreshFailed),
        }
    };
    let result = futures::select_biased! {
        () = deadline.fuse() => Err(NativeAuthError::RefreshTimeout),
        result = operation.fuse() => result,
    };
    state
        .update(cx, |state, cx| {
            if state.auth_generation != generation {
                return Err(Arc::new(anyhow!(NativeAuthError::StaleGrant)));
            }
            state.refresh_task = None;
            match result {
                Ok(credentials) => {
                    state.credentials = Some(credentials.clone());
                    state.last_auth_error = None;
                    Ok(credentials)
                }
                Err(error) => {
                    if error == NativeAuthError::RefreshRejected {
                        state.credentials = None;
                        state.reset_model_catalog();
                    }
                    state.last_auth_error = Some(error.to_string().into());
                    cx.notify();
                    Err(Arc::new(anyhow!(error)))
                }
            }
        })
        .map_err(Arc::new)?
}

impl State {
    /// Scoped native state. No model discovery, browser, refresh, or writes on startup.
    /// Inference retains native single-flight refresh through the scoped provider.
    pub fn new_native(
        http: Arc<dyn HttpClient>,
        credentials: Arc<NativeCredentials>,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut state = Self::new_without_model_discovery(http, credentials.clone(), cx);
        state.native_credentials = Some(credentials);
        state
    }

    pub fn native_identity(&self) -> Result<String, NativeAuthError> {
        let credentials = self
            .native_credentials
            .as_ref()
            .ok_or(NativeAuthError::InvalidNamespace)?;
        if credentials.has_pending_mutation() {
            return Err(NativeAuthError::MutationUncertain);
        }
        if let Some(error) = credentials.load_error() {
            return Err(error);
        }
        let current = self
            .credentials
            .as_ref()
            .ok_or(NativeAuthError::MissingSetup)?;
        let metadata = credentials
            .metadata()
            .ok_or(NativeAuthError::MissingSetup)?;
        if NativeCredentials::account(current)? != metadata.account {
            return Err(NativeAuthError::IdentityMismatch);
        }
        Ok(metadata.identity_binding)
    }

    pub fn native_refresh_required(&self) -> bool {
        self.credentials
            .as_ref()
            .is_none_or(CodexCredentials::is_expired)
    }

    pub fn native_credentials(&self) -> Option<Arc<NativeCredentials>> {
        self.native_credentials.clone()
    }

    /// Explicit managed-startup discovery. Constructors, setup and usage never call this.
    /// Only a catalog fetched under the pinned factory identity can replace the models.
    pub fn discover_native_models(
        &mut self,
        expected_identity: String,
        cx: &mut Context<Self>,
    ) -> Task<Result<(), NativeAuthError>> {
        if self.native_credentials.is_none() {
            return Task::ready(Err(NativeAuthError::InvalidNamespace));
        }
        let load = self.load_task();
        let http = self.http_client.clone();
        let version = self.client_version.clone();
        cx.spawn(async move |this, cx| {
            if let Some(load) = load {
                load.await
                    .map_err(|_| NativeAuthError::RuntimeUnavailable)?;
            }
            let generation = this
                .update(cx, |state, _| {
                    state.check_native_identity(&expected_identity)?;
                    state.model_catalog_generation = state.model_catalog_generation.wrapping_add(1);
                    Ok::<_, NativeAuthError>(state.model_catalog_generation)
                })
                .map_err(|_| NativeAuthError::RuntimeUnavailable)??;
            let deadline = cx
                .background_executor()
                .timer(MODEL_CATALOG_REQUEST_TIMEOUT);
            let request = async {
                let credentials = get_fresh_credentials(&this, &http, cx)
                    .await
                    .map_err(|_| NativeAuthError::CatalogFailed)?;
                // Refresh may have changed authentication while it was pending.
                this.read_with(cx, |state, _| {
                    state.check_native_identity(&expected_identity)
                })
                .map_err(|_| NativeAuthError::RuntimeUnavailable)??;
                list_models(http.as_ref(), &credentials, version.as_ref())
                    .await
                    .map_err(|_| NativeAuthError::CatalogFailed)
            };
            // This covers refresh, persistence, headers and the complete catalog body.
            let result = futures::select_biased! {
                () = deadline.fuse() => Err(NativeAuthError::CatalogTimeout),
                result = request.fuse() => result,
            };
            this.update(cx, |state, cx| {
                state.check_native_identity(&expected_identity)?;
                if state.model_catalog_generation != generation {
                    return Err(NativeAuthError::CatalogFailed);
                }
                match result {
                    Ok(models) => {
                        state.available_models = models;
                        state.last_model_catalog_error = None;
                        cx.notify();
                        Ok(())
                    }
                    Err(error) => {
                        state.last_model_catalog_error = Some(error.to_string().into());
                        cx.notify();
                        Err(error)
                    }
                }
            })
            .map_err(|_| NativeAuthError::RuntimeUnavailable)?
        })
    }

    fn check_native_identity(&self, expected_identity: &str) -> Result<(), NativeAuthError> {
        let identity = self.native_identity()?;
        if expected_identity.is_empty() || identity != expected_identity {
            return Err(NativeAuthError::IdentityMismatch);
        }
        Ok(())
    }

    /// Only emits a binding when both the pinned grant and response account match.
    pub fn read_native_usage(
        &self,
        expected_identity: String,
        cx: &mut Context<Self>,
    ) -> Task<Result<UsageSnapshot, NativeUsageError>> {
        let load = self.load_task();
        cx.spawn(async move |this, cx| {
            if let Some(load) = load {
                load.await
                    .map_err(|_| NativeAuthError::RuntimeUnavailable)?;
            }
            let before = this
                .read_with(cx, |state, _| state.native_identity())
                .map_err(|_| NativeAuthError::RuntimeUnavailable)??;
            if expected_identity.is_empty() || before != expected_identity {
                return Err(NativeAuthError::IdentityMismatch.into());
            }
            let mut snapshot = this
                .update(cx, |state, cx| state.read_usage(cx))
                .map_err(|_| NativeAuthError::RuntimeUnavailable)?
                .await?;
            let after = this
                .read_with(cx, |state, _| state.native_identity())
                .map_err(|_| NativeAuthError::RuntimeUnavailable)??;
            if after != before {
                return Err(NativeAuthError::IdentityMismatch.into());
            }
            if snapshot.account_binding == UsageAccountBinding::ResponseAccountMatched {
                snapshot.identity_binding = Some(before);
            }
            Ok(snapshot)
        })
    }
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum NativeUsageError {
    Auth(NativeAuthError),
    Usage(UsageError),
}
impl From<NativeAuthError> for NativeUsageError {
    fn from(error: NativeAuthError) -> Self {
        Self::Auth(error)
    }
}
impl From<UsageError> for NativeUsageError {
    fn from(error: UsageError) -> Self {
        Self::Usage(error)
    }
}

#[cfg(test)]
mod tests;
