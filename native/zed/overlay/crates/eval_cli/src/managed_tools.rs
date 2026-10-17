use agent::{AgentTool, ToolCallEventStream, ToolInput};
use agent_client_protocol::schema::v1 as acp;
use anyhow::{Context as _, Result, ensure};
use futures::{FutureExt as _, future::BoxFuture};
use gpui::{App, AppContext as _, SharedString, Task};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub const PROFILE: &str = "factory-managed-v1";
pub const TOOL_NAMES: &[&str] = &["managed_files", "managed_terminal", "factory_helper"];
pub const MAX_OUTPUT: usize = 96 * 1024;

pub fn capabilities() -> Value {
    json!({"schema_version":1,"native_runtime":"zed","managed":true,"containment":"docker",
        "model":"openai-subscribed/gpt-6.1-sol","control_protocol":1})
}

#[derive(clap::Args, Debug, Default)]
pub struct ManagedArgs {
    #[arg(long, requires_all=["job_id","generation","instance_token","identity_file","result_file","lock_file","credential_namespace","expected_identity","container_id","docker_bin","factory_bin","brain","output_file","git_dir","git_index"])]
    pub managed: bool,
    #[arg(long, requires = "managed")]
    pub job_id: Option<String>,
    #[arg(long, requires = "managed")]
    pub generation: Option<u64>,
    #[arg(long, requires = "managed")]
    pub instance_token: Option<String>,
    #[arg(long, requires = "managed")]
    pub identity_file: Option<PathBuf>,
    #[arg(long, requires = "managed")]
    pub result_file: Option<PathBuf>,
    #[arg(long, requires = "managed")]
    pub lock_file: Option<PathBuf>,
    #[arg(long, requires = "managed")]
    pub credential_namespace: Option<String>,
    #[arg(long, requires = "managed")]
    pub expected_identity: Option<String>,
    #[arg(long, requires = "managed")]
    pub container_id: Option<String>,
    #[arg(long, requires = "managed")]
    pub docker_bin: Option<PathBuf>,
    #[arg(long, requires = "managed")]
    pub factory_bin: Option<PathBuf>,
    #[arg(long, requires = "managed")]
    pub brain: Option<PathBuf>,
    #[arg(long, requires = "managed")]
    pub output_file: Option<PathBuf>,
    #[arg(long, requires = "managed")]
    pub git_dir: Option<PathBuf>,
    #[arg(long, requires = "managed")]
    pub git_index: Option<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub schema_version: u8,
    pub job_id: String,
    pub generation: u64,
    pub instance_token: String,
    pub pid: u32,
    pub process_group: i32,
    pub start_identity: String,
    pub epoch: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    pub saved: bool,
    pub conversation_id: String,
    pub turn_id: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResultEnvelope {
    pub schema_version: u8,
    pub identity: Identity,
    pub conversation_id: String,
    pub turn_id: String,
    pub observed_model: String,
    pub speed: String,
    pub outcome: String,
    pub reply: String,
    pub checkpoint: Checkpoint,
    pub recorded_at_ms: u64,
    pub error_code: String,
}

pub fn now_ms() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}
#[cfg(target_os = "macos")]
fn start_identity() -> Result<String> {
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let size = std::mem::size_of::<libc::proc_bsdinfo>();
    let read = unsafe {
        libc::proc_pidinfo(
            libc::getpid(),
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            size as i32,
        )
    };
    ensure!(read as usize == size, "process identity unavailable");
    let info = unsafe { info.assume_init() };
    Ok(format!(
        "{}:{}",
        info.pbi_start_tvsec, info.pbi_start_tvusec
    ))
}
#[cfg(not(target_os = "macos"))]
fn start_identity() -> Result<String> {
    anyhow::bail!("managed process identity unsupported on this platform")
}

pub fn private_parent(path: &Path) -> Result<()> {
    let parent = path.parent().context("private parent required")?;
    super::private_directory(parent)?;
    ensure!(
        !std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()),
        "symlink forbidden"
    );
    Ok(())
}
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);
pub fn durable_json(path: &Path, value: &impl Serialize) -> Result<()> {
    private_parent(path)?;
    let parent = path.parent().context("parent required")?;
    let temp = parent.join(format!(
        ".native-{}-{}.tmp",
        std::process::id(),
        TEMP_COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&temp)?;
        let bytes = serde_json::to_vec(value)?;
        ensure!(
            bytes.len() <= super::lifecycle_protocol::MAX_RESPONSE,
            "result too large"
        );
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        std::fs::rename(&temp, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() && temp.exists() {
        std::fs::remove_file(&temp)?;
    }
    result
}

#[derive(Default, Debug)]
pub struct Activity {
    pub active: usize,
    pub uncertain: bool,
}
pub struct ActivityGuard {
    state: Arc<Mutex<Activity>>,
    completed: bool,
}
impl ActivityGuard {
    pub fn new(state: Arc<Mutex<Activity>>) -> Self {
        state.lock().expect("activity lock").active += 1;
        Self {
            state,
            completed: false,
        }
    }
    pub fn complete(mut self) {
        self.completed = true;
    }
}
impl Drop for ActivityGuard {
    fn drop(&mut self) {
        let mut state = self.state.lock().expect("activity lock");
        state.active -= 1;
        if !self.completed {
            state.uncertain = true;
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandSpec {
    pub executable: PathBuf,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub cwd: PathBuf,
    pub stdin: Vec<u8>,
    pub timeout_ms: u64,
}
#[derive(Default)]
pub struct Cancellation {
    cancelled: std::sync::atomic::AtomicBool,
    waker: futures::task::AtomicWaker,
}
impl Cancellation {
    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.waker.wake();
    }
    pub async fn wait(&self) {
        futures::future::poll_fn(|cx| {
            self.waker.register(cx.waker());
            if self.cancelled.load(Ordering::SeqCst) {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        })
        .await
    }
}
struct CancelOnDrop(Arc<Cancellation>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub trait CommandExecutor: Send + Sync {
    fn execute(
        &self,
        spec: CommandSpec,
        activity: Arc<Mutex<Activity>>,
        cancel: Arc<Cancellation>,
    ) -> BoxFuture<'static, Result<String>>;
}
pub struct HostExecutor;
impl CommandExecutor for HostExecutor {
    fn execute(
        &self,
        spec: CommandSpec,
        activity: Arc<Mutex<Activity>>,
        cancel: Arc<Cancellation>,
    ) -> BoxFuture<'static, Result<String>> {
        // Admission is counted before scheduling, so cancellation cannot hide a queued child.
        let guard = ActivityGuard::new(activity);
        run_command(spec, guard, cancel).boxed()
    }
}

async fn run_command(
    spec: CommandSpec,
    guard: ActivityGuard,
    cancel: Arc<Cancellation>,
) -> Result<String> {
    use smol::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use std::process::Stdio;
    if cancel.cancelled.load(Ordering::SeqCst) {
        guard.complete();
        anyhow::bail!("managed command interrupted");
    }
    let mut command = async_process::Command::new(&spec.executable);
    command
        .args(&spec.args)
        .current_dir(&spec.cwd)
        .env_clear()
        .envs(&spec.env)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(_) => {
            guard.complete();
            anyhow::bail!("host command unavailable")
        }
    };
    let mut stdin = child.stdin.take().context("stdin unavailable")?;
    let stdout = child.stdout.take().context("stdout unavailable")?;
    let stderr = child.stderr.take().context("stderr unavailable")?;
    let result = {
        let output = async {
            let read = |stream: Box<dyn smol::io::AsyncRead + Unpin + Send>| async move {
                let mut bytes = Vec::new();
                stream
                    .take((MAX_OUTPUT + 1) as u64)
                    .read_to_end(&mut bytes)
                    .await?;
                ensure!(bytes.len() <= MAX_OUTPUT, "command output too large");
                Ok::<_, anyhow::Error>(bytes)
            };
            let write = async {
                stdin.write_all(&spec.stdin).await?;
                stdin.close().await?;
                Ok::<_, anyhow::Error>(())
            };
            let (out, err, ()) =
                futures::try_join!(read(Box::new(stdout)), read(Box::new(stderr)), write)?;
            let status = child.status().await?;
            ensure!(status.success(), "host command failed");
            let mut bytes = out;
            bytes.extend(err);
            ensure!(bytes.len() <= MAX_OUTPUT, "command output too large");
            Ok::<_, anyhow::Error>(String::from_utf8(bytes)?)
        }
        .fuse();
        let timeout = smol::Timer::after(Duration::from_millis(spec.timeout_ms)).fuse();
        let cancelled = cancel.wait().fuse();
        futures::pin_mut!(output, timeout, cancelled);
        futures::select_biased! {result=output=>result,_=cancelled=>Err(anyhow::anyhow!("managed command interrupted")),_=timeout=>Err(anyhow::anyhow!("host command timed out"))}
    };
    if result.is_err() {
        if child.kill().is_err() && child.try_status()?.is_none() {
            anyhow::bail!("host child termination unknown");
        }
        let wait = child.status().fuse();
        let deadline = smol::Timer::after(Duration::from_secs(2)).fuse();
        futures::pin_mut!(wait, deadline);
        futures::select_biased! {status=wait=>{status?;},_=deadline=>anyhow::bail!("host child termination unknown")}
    }
    guard.complete();
    result
}

pub struct Managed {
    pub identity: Identity,
    pub expected_identity: String,
    pub namespace: String,
    pub container_id: String,
    pub workdir: PathBuf,
    pub output: PathBuf,
    pub brain: PathBuf,
    pub git_dir: PathBuf,
    pub git_index: PathBuf,
    pub docker_bin: PathBuf,
    pub factory_bin: PathBuf,
    pub result_file: PathBuf,
    pub environment: BTreeMap<String, String>,
    pub activity: Arc<Mutex<Activity>>,
    pub executor: Arc<dyn CommandExecutor>,
    pub control_paths: Vec<PathBuf>,
    _lock: File,
}
impl Managed {
    pub fn prepare(args: &ManagedArgs, workdir: &Path, epoch: String) -> Result<Arc<Self>> {
        let text = |value: &Option<String>| -> Result<String> {
            let value = value.clone().context("managed flag missing")?;
            ensure!(
                !value.is_empty() && value.len() <= 512 && !value.contains(['\0', '\n', '\r']),
                "invalid managed value"
            );
            Ok(value)
        };
        let path = |value: &Option<PathBuf>| -> Result<PathBuf> {
            let value = value.clone().context("managed path missing")?;
            ensure!(value.is_absolute(), "absolute managed path required");
            Ok(value)
        };
        let job_id = text(&args.job_id)?;
        let instance_token = text(&args.instance_token)?;
        let generation = args.generation.context("generation required")?;
        ensure!(
            generation > 0 && generation <= i64::MAX as u64,
            "invalid generation"
        );
        let expected_identity = text(&args.expected_identity)?;
        let namespace = text(&args.credential_namespace)?;
        ensure!(
            namespace == openai_subscribed::native_auth::FACTORY_CREDENTIAL_NAMESPACE,
            "scoped namespace required"
        );
        let container_id = text(&args.container_id)?;
        ensure!(
            container_id.len() == 64 && container_id.bytes().all(|b| b.is_ascii_hexdigit()),
            "full attested container ID required"
        );
        let identity_file = path(&args.identity_file)?;
        let result_file = path(&args.result_file)?;
        let lock_file = path(&args.lock_file)?;
        for p in [&identity_file, &result_file, &lock_file] {
            private_parent(p)?;
        }
        ensure!(
            identity_file != result_file && identity_file != lock_file && result_file != lock_file,
            "runtime paths collide"
        );
        let run_root = identity_file
            .parent()
            .context("runtime directory required")?
            .canonicalize()?;
        let workdir = workdir.canonicalize()?;
        let brain = path(&args.brain)?.canonicalize()?;
        ensure!(
            brain.is_dir() && !workdir.starts_with(&brain) && !brain.starts_with(&workdir),
            "brain/worktree overlap"
        );
        ensure!(
            !run_root.starts_with(&workdir) && !workdir.starts_with(&run_root),
            "runtime/worktree overlap"
        );
        let git_dir = path(&args.git_dir)?.canonicalize()?;
        let git_index = path(&args.git_index)?;
        ensure!(
            git_dir != run_root
                && git_dir.is_dir()
                && !git_dir.starts_with(&workdir)
                && !workdir.starts_with(&git_dir),
            "separate private Git directory required"
        );
        for path in [&identity_file, &result_file, &lock_file] {
            let canonical = path
                .parent()
                .context("control parent required")?
                .canonicalize()?
                .join(path.file_name().context("control filename required")?);
            ensure!(
                &canonical == path && !path.starts_with(&git_dir) && !path.starts_with(&workdir),
                "control file must be canonical and outside writable mounts"
            );
        }
        ensure!(
            git_index
                .parent()
                .context("index parent required")?
                .canonicalize()?
                .starts_with(&git_dir),
            "private Git index required"
        );
        let output = path(&args.output_file)?;
        ensure!(
            output
                .parent()
                .context("output parent required")?
                .canonicalize()?
                .join(output.file_name().context("output filename required")?)
                == output,
            "canonical output required"
        );
        ensure!(
            output.extension().is_some_and(|ext| ext == "md") && !output.starts_with(&run_root),
            "markdown output required"
        );
        for path in [&identity_file, &result_file, &lock_file] {
            ensure!(
                !output.starts_with(path.parent().context("private parent required")?),
                "output/control overlap"
            );
        }
        ensure!(
            output != brain.join("AGENTS.md") && !output.starts_with(brain.join("skills")),
            "global instruction writes forbidden"
        );
        ensure!(
            !std::fs::symlink_metadata(&output).is_ok_and(|m| m.file_type().is_symlink()),
            "output symlink forbidden"
        );
        let executable = |value: &Option<PathBuf>| -> Result<PathBuf> {
            let p = path(value)?.canonicalize()?;
            let m = std::fs::metadata(&p)?;
            ensure!(
                m.is_file() && m.permissions().mode() & 0o111 != 0,
                "executable required"
            );
            Ok(p)
        };
        let docker_bin = executable(&args.docker_bin)?;
        let factory_bin = executable(&args.factory_bin)?;
        for binary in [&docker_bin, &factory_bin] {
            ensure!(
                !binary.starts_with(&workdir) && !binary.starts_with(&git_dir) && binary != &output,
                "selected executable must be outside model-writable paths"
            );
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&lock_file)?;
        ensure!(
            lock.metadata()?.permissions().mode() & 0o777 == 0o600
                && lock.metadata()?.uid() == unsafe { libc::geteuid() }
                && lock.metadata()?.nlink() == 1,
            "private lock required"
        );
        lock.try_lock()?;
        ensure!(
            !identity_file.try_exists()? && !result_file.try_exists()?,
            "ambiguous existing launch records"
        );
        let identity = Identity {
            schema_version: 1,
            job_id,
            generation,
            instance_token,
            pid: std::process::id(),
            process_group: unsafe { libc::getpgrp() },
            start_identity: start_identity()?,
            epoch,
        };
        let environment = selected_environment();
        durable_json(&identity_file, &identity)?;
        Ok(Arc::new(Self {
            identity,
            expected_identity,
            namespace,
            container_id,
            workdir,
            output,
            brain,
            git_dir,
            git_index,
            docker_bin,
            factory_bin,
            result_file: result_file.clone(),
            environment,
            activity: Arc::default(),
            executor: Arc::new(HostExecutor),
            control_paths: vec![identity_file, result_file.clone(), lock_file],
            _lock: lock,
        }))
    }
    pub fn turn_id(&self, turn: u64) -> String {
        format!("{}:{turn}", self.identity.epoch)
    }
    pub fn ready(&self) -> bool {
        let a = self.activity.lock().expect("activity lock");
        a.active == 0 && !a.uncertain
    }
    pub fn terminal(&self, input: TerminalInput) -> Result<CommandSpec> {
        ensure!(
            !input.command.trim().is_empty() && input.command.len() <= 64 * 1024,
            "invalid terminal command"
        );
        if let Some(cd) = input.cd {
            ensure!(Path::new(&cd) == self.workdir, "terminal cwd is fixed");
        }
        let mut args = vec![
            "exec".into(),
            "--workdir".into(),
            self.workdir.to_string_lossy().into_owned(),
        ];
        for (key, value) in [
            ("GIT_DIR", self.git_dir.to_string_lossy().into_owned()),
            (
                "GIT_COMMON_DIR",
                self.git_dir.to_string_lossy().into_owned(),
            ),
            ("GIT_WORK_TREE", self.workdir.to_string_lossy().into_owned()),
            (
                "GIT_INDEX_FILE",
                self.git_index.to_string_lossy().into_owned(),
            ),
            ("GIT_OPTIONAL_LOCKS", "0".into()),
        ] {
            args.extend(["--env".into(), format!("{key}={value}")]);
        }
        // Only explicit auth/toolchain inputs cross into the already-attested container.
        for key in [
            "GH_TOKEN",
            "GITHUB_TOKEN",
            "GOPROXY",
            "GONOSUMDB",
            "GOPRIVATE",
            "CARGO_NET_OFFLINE",
        ] {
            if self.environment.contains_key(key) {
                args.extend(["--env".into(), key.into()]);
            }
        }
        args.extend([
            self.container_id.clone(),
            "/bin/sh".into(),
            "-c".into(),
            input.command,
        ]);
        Ok(CommandSpec {
            executable: self.docker_bin.clone(),
            args,
            env: self.environment.clone(),
            cwd: self.workdir.clone(),
            stdin: Vec::new(),
            timeout_ms: input.timeout_ms.unwrap_or(30_000).clamp(1, 120_000),
        })
    }
    pub fn helper(&self, input: HelperInput) -> Result<CommandSpec> {
        ensure!(
            input.payload.is_object(),
            "helper payload must be an object"
        );
        fn reject_scope(value: &Value) -> Result<()> {
            match value {
                Value::Object(map) => {
                    for (key, value) in map {
                        ensure!(
                            ![
                                "owner",
                                "owner_id",
                                "lease",
                                "lease_id",
                                "brain",
                                "factory_bin",
                                "executable",
                                "credential_namespace",
                                "instance_token",
                                "generation",
                                "expected_generation",
                                "parent_job_id",
                                "container_id",
                                "docker_bin",
                                "git_dir",
                                "git_index",
                                "identity_file",
                                "result_file",
                                "lock_file"
                            ]
                            .contains(&key.as_str()),
                            "caller scope forbidden"
                        );
                        reject_scope(value)?;
                    }
                }
                Value::Array(items) => {
                    for item in items {
                        reject_scope(item)?;
                    }
                }
                _ => {}
            }
            Ok(())
        }
        reject_scope(&input.payload)?;
        if input.op == HelperOp::BrainRead {
            self.validate_brain_read(&input.payload)?;
        }
        let stdin = serde_json::to_vec(&input)?;
        ensure!(stdin.len() <= 64 * 1024, "helper request too large");
        let mut env = self.environment.clone();
        env.insert(
            "FACTORY_BIN".into(),
            self.factory_bin.to_string_lossy().into_owned(),
        );
        env.insert(
            "FACTORY_BRAIN".into(),
            self.brain.to_string_lossy().into_owned(),
        );
        env.insert("FACTORY_JOB_ID".into(), self.identity.job_id.clone());
        env.insert(
            "FACTORY_GENERATION".into(),
            self.identity.generation.to_string(),
        );
        Ok(CommandSpec {
            executable: self.factory_bin.clone(),
            args: vec![
                "helper".into(),
                "control".into(),
                "--job".into(),
                self.identity.job_id.clone(),
                "--generation".into(),
                self.identity.generation.to_string(),
                "--brain".into(),
                self.brain.to_string_lossy().into_owned(),
                "--request".into(),
                "-".into(),
            ],
            env,
            cwd: self.workdir.clone(),
            stdin,
            timeout_ms: 30_000,
        })
    }
    fn validate_brain_read(&self, payload: &Value) -> Result<()> {
        let object = payload.as_object().context("brain payload required")?;
        ensure!(
            object
                .keys()
                .all(|key| ["path", "max_bytes"].contains(&key.as_str())),
            "invalid brain read"
        );
        if let Some(limit) = object.get("max_bytes") {
            ensure!(
                limit.as_u64().is_some_and(|n| n > 0 && n <= 64 * 1024),
                "invalid read limit"
            );
        }
        let relative = Path::new(payload["path"].as_str().context("markdown path required")?);
        ensure!(
            !relative.is_absolute() && relative.extension().is_some_and(|ext| ext == "md"),
            "relative markdown required"
        );
        let components = relative
            .components()
            .map(|c| match c {
                std::path::Component::Normal(name) => Ok(name.to_string_lossy().into_owned()),
                _ => Err(anyhow::anyhow!("invalid knowledge path")),
            })
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            components.first().is_some_and(|root| [
                "AGENTS.md",
                "repos",
                "docs",
                "skills",
                "features"
            ]
            .contains(&root.as_str())),
            "knowledge root denied"
        );
        ensure!(
            components.iter().all(|part| !part.starts_with('.')
                && ![
                    "runtime",
                    "journal",
                    "journals",
                    "logs",
                    "connectors",
                    "auth",
                    "credentials"
                ]
                .contains(&part.as_str())),
            "private knowledge denied"
        );
        let path = self.brain.join(relative);
        ensure!(
            path.canonicalize()? == path && path.is_file(),
            "knowledge symlink or escape"
        );
        for control in &self.control_paths {
            ensure!(
                !path.starts_with(control.parent().context("runtime parent required")?),
                "runtime knowledge denied"
            );
        }
        Ok(())
    }
}
fn selected_environment() -> BTreeMap<String, String> {
    [
        "HOME",
        "PATH",
        "TMPDIR",
        "LANG",
        "LC_ALL",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "GH_TOKEN",
        "GITHUB_TOKEN",
        "GOPROXY",
        "GONOSUMDB",
        "GOPRIVATE",
        "CARGO_NET_OFFLINE",
        "DOCKER_HOST",
        "DOCKER_CONTEXT",
        "DOCKER_CONFIG",
        "DOCKER_TLS_VERIFY",
        "DOCKER_CERT_PATH",
    ]
    .into_iter()
    .filter_map(|key| std::env::var(key).ok().map(|value| (key.into(), value)))
    .collect()
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TerminalInput {
    pub command: String,
    pub cd: Option<String>,
    pub timeout_ms: Option<u64>,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HelperOp {
    Submit,
    Continue,
    Result,
    Yield,
    Inbox,
    LeaseCheck,
    Heartbeat,
    Intent,
    Done,
    Error,
    BrainRead,
}
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HelperInput {
    pub op: HelperOp,
    pub payload: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaseReceipt {
    schema_version: u8,
    op: String,
    job_id: String,
    generation: u64,
    allowed: bool,
}

impl Managed {
    pub(crate) fn execute_terminal(
        self: &Arc<Self>,
        spec: CommandSpec,
        cancel: Arc<Cancellation>,
    ) -> BoxFuture<'static, Result<String>> {
        // Keep the barrier occupied across preflight and exec, including queued work.
        let guard = ActivityGuard::new(self.activity.clone());
        let managed = self.clone();
        async move {
            let result = async {
                ensure!(
                    !cancel.cancelled.load(Ordering::SeqCst),
                    "managed command interrupted"
                );
                let check = managed.helper(HelperInput {
                    op: HelperOp::LeaseCheck,
                    payload: json!({}),
                })?;
                let response = managed
                    .executor
                    .execute(check, managed.activity.clone(), cancel.clone())
                    .await?;
                let receipt: LeaseReceipt =
                    serde_json::from_str(&response).context("lease authorization denied")?;
                ensure!(
                    receipt.schema_version == 1
                        && receipt.op == "lease_check"
                        && receipt.job_id == managed.identity.job_id
                        && receipt.generation == managed.identity.generation
                        && receipt.allowed,
                    "lease authorization denied"
                );
                ensure!(
                    !cancel.cancelled.load(Ordering::SeqCst),
                    "managed command interrupted"
                );
                managed
                    .executor
                    .execute(spec, managed.activity.clone(), cancel)
                    .await
            }
            .await;
            // Child guards retain uncertainty when termination cannot be proved.
            guard.complete();
            result
        }
        .boxed()
    }
    fn execute_helper(
        self: &Arc<Self>,
        spec: CommandSpec,
        cancel: Arc<Cancellation>,
    ) -> BoxFuture<'static, Result<String>> {
        self.executor.execute(spec, self.activity.clone(), cancel)
    }
}

pub struct ManagedTerminal(pub Arc<Managed>);
pub struct FactoryHelper(pub Arc<Managed>);
macro_rules! command_tool {
    ($tool:ty,$input:ty,$name:literal,$description:literal,$method:ident,$execute:ident)=>{
        impl AgentTool for $tool {
            type Input=$input;type Output=String;const NAME:&'static str=$name;
            fn description()->SharedString{$description.into()}
            fn kind()->acp::ToolKind{acp::ToolKind::Execute}
            fn initial_title(&self,_:Result<Self::Input,Value>,_:&mut App)->SharedString{Self::NAME.into()}
            fn run(self:Arc<Self>,input:ToolInput<Self::Input>,event_stream:ToolCallEventStream,cx:&mut App)->Task<Result<String,String>>{
                cx.spawn(async move |cx|{
                    let input=input.recv().await.map_err(|_|"invalid_tool_input".to_string())?;
                    if self.0.activity.lock().expect("activity lock").uncertain {return Err("managed_host_held".into());}
                    let spec=self.0.$method(input).map_err(|_|"tool_policy_denied".to_string())?;
                    let cancel=Arc::new(Cancellation::default());let cancel_on_drop=CancelOnDrop(cancel.clone());
                    let future=self.0.$execute(spec,cancel);
                    let (sender,receiver)=futures::channel::oneshot::channel();
                    // Retain execution independently of a cancelled native tool waiter. The
                    // activity barrier waits for kill+reap before checkpoint or shutdown.
                    cx.background_spawn(async move{
                        let result=future.await;
                        if sender.send(result).is_err(){ /* cancelled native waiter */ }
                    }).detach();
                    let result=futures::select_biased! {
                        result=receiver.fuse()=>result.map_err(|_|"managed_command_unknown".to_string())?,
                        _=event_stream.cancelled_by_user().fuse()=>return Err("managed_command_interrupted".into()),
                    };
                    drop(cancel_on_drop);
                    result.map_err(|_|"managed_command_failed".to_string())
                })
            }
        }
    }
}
command_tool!(
    ManagedTerminal,
    TerminalInput,
    "managed_terminal",
    "Run a bounded command only inside the assigned Docker container at its fixed worktree. No host execution or container/cwd selection.",
    terminal,
    execute_terminal
);
command_tool!(
    FactoryHelper,
    HelperInput,
    "factory_helper",
    "Typed control of the selected factory job: submit, continue, result, yield, inbox, lease_check, heartbeat, intent, done, error, brain_read. Scope comes from the stored job; no start/end or inbox acknowledgment.",
    helper,
    execute_helper
);
