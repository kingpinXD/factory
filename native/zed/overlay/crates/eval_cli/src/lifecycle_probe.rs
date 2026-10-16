//! Local, subscription-only native session lifecycle proof. Not a production daemon.
mod headless;
mod lifecycle_protocol;
mod managed_tool_files;
mod managed_tools;

use std::{
    cell::RefCell,
    io::BufReader,
    os::unix::{
        fs::{DirBuilderExt, MetadataExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc as sync_mpsc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use acp_thread::{
    AcpThread, AcpThreadEvent, AgentConnection as _, AgentThreadEntry, SubmissionResponse,
};
use agent::{NativeAgent, NativeAgentConnection, Templates, Thread, ThreadStore};
use agent_client_protocol::schema::{v1 as acp, v2 as acp_v2};
use anyhow::{Context as _, Result, ensure};
use clap::Parser;
use futures::{
    FutureExt as _, StreamExt as _,
    channel::{mpsc, oneshot},
    select_biased,
};
use gpui::{AppContext as _, AsyncApp, Entity, Task, UpdateGlobal as _};
use language_model::{LanguageModelRegistry, Speed};
use lifecycle_protocol::*;
use project::Project;
use serde_json::{Value, json};
use settings::SettingsStore;
use util::path_list::PathList;

const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Parser)]
struct Args {
    #[arg(long)]
    user_data_dir: PathBuf,
    #[arg(long)]
    workdir: PathBuf,
    #[arg(long)]
    socket: PathBuf,
    #[arg(long)]
    load_session: Option<String>,
    /// Report the fixed managed runtime ABI without initializing native state.
    #[arg(long)]
    capabilities: bool,
    #[command(flatten)]
    managed: managed_tools::ManagedArgs,
}

fn private_directory(directory: &Path) -> Result<()> {
    match std::fs::symlink_metadata(directory) {
        Ok(metadata) => ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "private directory required"
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::DirBuilder::new().mode(0o700).create(directory)?;
        }
        Err(error) => return Err(error.into()),
    }
    let metadata = std::fs::symlink_metadata(directory)?;
    // The socket's directory, not just the socket, is the local security boundary.
    ensure!(
        metadata.uid() == unsafe { libc::geteuid() } && metadata.mode() & 0o777 == 0o700,
        "directory must be owned by this user and mode 0700"
    );
    Ok(())
}

fn prepare(args: &mut Args) -> Result<UnixListener> {
    ensure!(
        std::env::var_os("ZED_STATELESS").is_none(),
        "ZED_STATELESS is not allowed"
    );
    ensure!(
        args.user_data_dir.is_absolute() && args.workdir.is_absolute() && args.socket.is_absolute(),
        "all paths must be absolute"
    );
    args.workdir = args.workdir.canonicalize()?;
    ensure!(args.workdir.is_dir(), "workspace must be a directory");
    private_directory(&args.user_data_dir)?;
    args.user_data_dir = args.user_data_dir.canonicalize()?;
    let home = paths::home_dir();
    for installed in [
        home.join("Library/Application Support/Zed"),
        home.join("Library/Application Support/Zed Dev"),
        home.join("Library/Application Support/Zed Preview"),
        home.join(".config/zed"),
    ] {
        let installed = installed.canonicalize().unwrap_or(installed);
        ensure!(
            !args.user_data_dir.starts_with(&installed)
                && !installed.starts_with(&args.user_data_dir),
            "isolated data directory required"
        );
    }
    let listener = bind_private_socket(&args.socket)?;
    paths::set_custom_data_dir(
        args.user_data_dir
            .to_str()
            .context("UTF-8 data directory required")?,
    );
    Ok(listener)
}

fn bind_private_socket(socket: &Path) -> Result<UnixListener> {
    ensure!(
        socket.as_os_str().as_encoded_bytes().len() <= 103,
        "socket path too long"
    );
    private_directory(socket.parent().context("socket parent required")?)?;
    ensure!(
        std::fs::symlink_metadata(socket)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound),
        "socket endpoint already exists or cannot be inspected"
    );
    let listener = UnixListener::bind(socket)?;
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))?;
    listener.set_nonblocking(true)?;
    Ok(listener)
}

struct Command {
    request: Request,
    reply: sync_mpsc::SyncSender<Value>,
    delivered: oneshot::Receiver<bool>,
}

fn serve_client(mut stream: UnixStream, queue: Arc<Mutex<mpsc::Sender<Command>>>) -> Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    loop {
        let frame = match read_frame(&mut reader) {
            Ok(Some(frame)) => frame,
            Ok(None) => return Ok(()),
            Err(code) => {
                write_response(&mut stream, &failure("", code))?;
                return Ok(());
            }
        };
        let request = match parse(&frame) {
            Ok(request) => request,
            Err(code) => {
                write_response(&mut stream, &failure("", code))?;
                continue;
            }
        };
        let id = request.id.clone();
        let (reply, response) = sync_mpsc::sync_channel(1);
        let (delivered, receipt) = oneshot::channel();
        let command = Command {
            request,
            reply,
            delivered: receipt,
        };
        if queue
            .lock()
            .map_err(|_| anyhow::anyhow!("queue unavailable"))?
            .try_send(command)
            .is_err()
        {
            write_response(&mut stream, &failure(&id, "queue_full"))?;
            continue;
        }
        match response.recv_timeout(Duration::from_secs(90)) {
            Ok(response) => {
                let written = write_response(&mut stream, &response);
                if delivered.send(written.is_ok()).is_err() { /* command no longer needs delivery receipt */
                }
                written?;
            }
            Err(_) => {
                write_response(&mut stream, &failure(&id, "response_timeout"))?;
                return Ok(());
            }
        }
    }
}

fn start_socket(listener: UnixListener, sender: mpsc::Sender<Command>, stop: Arc<AtomicBool>) {
    let queue = Arc::new(Mutex::new(sender));
    let clients = Arc::new(AtomicUsize::new(0));
    std::thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _)) => {
                    if clients.load(Ordering::Relaxed) >= 8 {
                        drop(stream);
                        continue;
                    }
                    clients.fetch_add(1, Ordering::Relaxed);
                    let queue = queue.clone();
                    let clients = clients.clone();
                    std::thread::spawn(move || {
                        if serve_client(stream, queue).is_err() { /* disconnected or timed-out local client */
                        }
                        clients.fetch_sub(1, Ordering::Relaxed);
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(20))
                }
                Err(_) => break,
            }
        }
    });
}

fn reply(command: &Command, value: Value) {
    if command.reply.send(value).is_err() { /* native work must outlive a disconnected client */ }
}

#[cfg(test)]
fn check_native(thread: &Thread) -> Result<()> {
    check_native_profile(thread, PROFILE)
}
fn check_native_profile(thread: &Thread, profile: &str) -> Result<()> {
    let model = thread.model().context("model unavailable")?;
    ensure!(
        model.refusal_fallback_model_id().is_none(),
        "model fallback is forbidden"
    );
    ensure!(thread.profile().0.as_ref() == profile, "profile mismatch");
    guard_session(
        &model.provider_id().0,
        &model.id().0,
        PROFILE,
        thread.speed().unwrap_or_default() == Speed::Standard,
    )
    .map_err(anyhow::Error::msg)
}

fn turn_result(result: Result<Option<SubmissionResponse>>) -> &'static str {
    match result {
        Ok(Some(SubmissionResponse::LegacyCompleted(response))) => match response.stop_reason {
            acp::StopReason::EndTurn => "completed",
            acp::StopReason::Cancelled => "interrupted",
            acp::StopReason::MaxTokens => "max_tokens",
            _ => "refused_or_incomplete",
        },
        Ok(Some(SubmissionResponse::Accepted(_))) => "unexpected_acceptance",
        Ok(None) => "no_response",
        Err(error) if error.is::<acp_thread::MaxOutputTokensError>() => "max_tokens",
        Err(_) => "turn_failed",
    }
}

#[cfg(test)]
#[derive(Default)]
struct TestHooks {
    cancel: Option<Task<()>>,
    save: Option<Task<Result<()>>>,
    quit: Option<Rc<std::cell::Cell<bool>>>,
    credential_mutation_pending: Option<Arc<AtomicBool>>,
}

fn current_reply(thread: &AcpThread, start: usize) -> String {
    thread
        .entries()
        .iter()
        .skip(start)
        .filter_map(|entry| match entry {
            AgentThreadEntry::AssistantMessage(message) => Some(message),
            _ => None,
        })
        .flat_map(|message| message.chunks.iter())
        .filter_map(|chunk| match chunk {
            acp_thread::AssistantMessageChunk::Message { block, .. } => Some(block),
            _ => None,
        })
        .flat_map(|block| block.source_blocks().iter())
        .filter_map(|block| match block {
            acp_v2::ContentBlock::Text(text) => Some(text.text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

struct Resident {
    agent: Entity<NativeAgent>,
    thread: Entity<Thread>,
    acp: Entity<AcpThread>,
    _project: Entity<Project>,
    _connection: Rc<NativeAgentConnection>,
    state: Rc<RefCell<Control>>,
    submission: RefCell<Option<Task<()>>>,
    epoch: String,
    managed: Option<Arc<managed_tools::Managed>>,
    auth_state: Option<Entity<openai_subscribed::State>>,
    native_credentials: Option<Arc<openai_subscribed::native_auth::NativeCredentials>>,
    reply_start: Rc<std::cell::Cell<usize>>,
    _subscriptions: Vec<gpui::Subscription>,
    _policy_task: Rc<RefCell<Option<Task<()>>>>,
    #[cfg(test)]
    hooks: RefCell<TestHooks>,
}

impl Resident {
    #[cfg(test)]
    fn new(
        connection: Rc<NativeAgentConnection>,
        acp: Entity<AcpThread>,
        project: Entity<Project>,
        epoch: String,
        cx: &mut AsyncApp,
    ) -> Result<Self> {
        Self::new_with_managed(connection, acp, project, epoch, None, cx)
    }
    fn new_with_managed(
        connection: Rc<NativeAgentConnection>,
        acp: Entity<AcpThread>,
        project: Entity<Project>,
        epoch: String,
        managed: Option<Arc<managed_tools::Managed>>,
        cx: &mut AsyncApp,
    ) -> Result<Self> {
        let agent = connection.0.clone();
        let thread = cx
            .update(|cx| connection.thread(acp.read(cx).session_id(), cx))
            .context("native thread required")?;
        thread.read_with(cx, |thread, _| {
            check_native_profile(
                thread,
                if managed.is_some() {
                    managed_tools::PROFILE
                } else {
                    PROFILE
                },
            )
        })?;
        if let Some(managed) = &managed {
            let files = managed_tool_files::ManagedFiles::new(managed.clone())?;
            thread.update(cx, |thread, _| {
                thread.add_tool(files);
                thread.add_tool(managed_tools::ManagedTerminal(managed.clone()));
                thread.add_tool(managed_tools::FactoryHelper(managed.clone()));
            });
        }
        thread.update(cx, |thread, cx| {
            thread.set_title("Native lifecycle proof".into(), cx)
        });
        let mut control = Control::default();
        control.managed_epoch = managed.as_ref().map(|m| m.identity.epoch.clone());
        let state = Rc::new(RefCell::new(control));
        let policy_task = Rc::new(RefCell::new(None));
        let subscription = cx.subscribe(&acp, {
            let state = state.clone();
            let native = thread.clone();
            let policy_task = policy_task.clone();
            let managed_mode = managed.is_some();
            move |acp, event, cx| {
                state.borrow_mut().sequence += 1;
                let forbidden = matches!(
                    event,
                    AcpThreadEvent::ToolAuthorizationRequested(_)
                        | AcpThreadEvent::SubagentSpawned(_)
                        | AcpThreadEvent::ElicitationRequested(_)
                ) || acp.read(cx).entries().iter().any(|entry| match entry {
                    AgentThreadEntry::ToolCall(call) => {
                        !managed_mode
                            || !call.tool_name.as_ref().is_some_and(|name| {
                                managed_tools::TOOL_NAMES.contains(&name.as_ref())
                            })
                    }
                    _ => false,
                });
                if matches!(event, AcpThreadEvent::Refusal) {
                    if managed_mode {
                        state.borrow_mut().refused = true;
                    } else {
                        state.borrow_mut().policy_violation = true;
                    }
                }
                if forbidden && !state.borrow().held {
                    {
                        let mut state = state.borrow_mut();
                        state.policy_violation = true;
                        state.held = true;
                    }
                    let cancel = native.update(cx, |thread, cx| thread.cancel(cx));
                    let acp = acp.clone();
                    *policy_task.borrow_mut() = Some(cx.spawn(async move |cx| {
                        cancel.await;
                        acp.update(cx, |thread, cx| thread.cancel(cx)).await;
                    }));
                }
            }
        });
        let native_observer = cx.update(|cx| {
            cx.observe(&thread, {
                let state = state.clone();
                move |_, _| {
                    state.borrow_mut().sequence += 1;
                }
            })
        });
        let reply_start = Rc::new(std::cell::Cell::new(
            acp.read_with(cx, |thread, _| thread.entries().len()),
        ));
        Ok(Self {
            agent,
            thread,
            acp,
            _project: project,
            _connection: connection,
            state,
            submission: RefCell::new(None),
            epoch,
            managed,
            auth_state: None,
            native_credentials: None,
            reply_start,
            _subscriptions: vec![subscription, native_observer],
            _policy_task: policy_task,
            #[cfg(test)]
            hooks: RefCell::default(),
        })
    }

    fn credential_mutation_check(&self) -> Box<dyn Fn() -> bool> {
        let credentials = self.native_credentials.clone();
        #[cfg(test)]
        let pending = self.hooks.borrow().credential_mutation_pending.clone();
        Box::new(move || {
            #[cfg(test)]
            if let Some(pending) = &pending {
                return pending.load(Ordering::SeqCst);
            }
            credentials
                .as_ref()
                .is_some_and(|credentials| credentials.has_pending_mutation())
        })
    }

    fn status(&self, cx: &mut AsyncApp) -> Value {
        let credential_mutation_pending = (self.credential_mutation_check())();
        let identity_valid = self.managed.as_ref().is_none_or(|managed| {
            self.auth_state.as_ref().is_none_or(|state| {
                state
                    .read_with(cx, |state, _| state.native_identity())
                    .ok()
                    .as_ref()
                    == Some(&managed.expected_identity)
            })
        });
        if !identity_valid && !credential_mutation_pending {
            self.state.borrow_mut().held = true;
        }
        cx.update(|cx| {
            let native = self.thread.read(cx);
            let state = self.state.borrow();
            let mut transcript = native.to_markdown();
            let truncated = transcript.len() > 96 * 1024;
            if truncated {
                let mut end = 96 * 1024;
                while !transcript.is_char_boundary(end) { end -= 1; }
                transcript.truncate(end);
            }
            let model = native.model().map(|model| format!("{}/{}",model.provider_id().0,model.id().0));
            let mut snapshot=json!({"epoch":self.epoch,"sequence":state.sequence,
                "session_id":self.acp.read(cx).session_id().0,"turn_id":state.turn_value(),
                "state":state.state(),"transcript":transcript,"transcript_truncated":truncated,
                "model":model,"profile":native.profile().0,
                "speed":if native.speed().unwrap_or_default() == Speed::Standard {"standard"} else {"fast"},
                "last_turn_result":state.last_result,"last_control_error":state.last_control_error,
                "tools":self.acp.read(cx).entries().iter().filter(|entry|matches!(entry,AgentThreadEntry::ToolCall(_))).count(),
                "policy_violation":state.policy_violation});
            if let Some(managed)=&self.managed {
                let reply=current_reply(self.acp.read(cx),self.reply_start.get());
                snapshot["reply"]=json!(reply.chars().take(managed_tools::MAX_OUTPUT/4).collect::<String>());
                snapshot["reply_truncated"]=json!(reply.chars().count()>managed_tools::MAX_OUTPUT/4);
                snapshot["managed"]=json!(true);snapshot["containment"]=json!("docker");
                snapshot["job_id"]=json!(managed.identity.job_id);snapshot["generation"]=json!(managed.identity.generation);
                snapshot["instance_token"]=json!(managed.identity.instance_token);snapshot["identity_binding"]=if identity_valid {json!(managed.expected_identity)}else{Value::Null};
                snapshot["container_id"]=json!(managed.container_id);snapshot["identity"]=json!(managed.identity);
                snapshot["credential_mutation_pending"]=json!(credential_mutation_pending);
                if credential_mutation_pending || managed.activity.lock().expect("activity lock").uncertain {snapshot["state"]=json!("held");}
            }
            snapshot
        })
    }

    fn prompt(&self, command: &Command, cx: &mut AsyncApp) {
        let request = &command.request;
        if (self.credential_mutation_check())() {
            reply(command, failure(&request.id, "credential_mutation_pending"));
            return;
        }
        if self.managed.as_ref().is_some_and(|managed| {
            self.auth_state.as_ref().is_some_and(|state| {
                state
                    .read_with(cx, |state, _| state.native_identity())
                    .ok()
                    .as_ref()
                    != Some(&managed.expected_identity)
            })
        }) {
            self.state.borrow_mut().held = true;
            reply(command, failure(&request.id, "scoped_identity_mismatch"));
            return;
        }
        if !self.state.borrow().is_cached(&request.id)
            && self
                .thread
                .read_with(cx, |thread, _| {
                    check_native_profile(
                        thread,
                        if self.managed.is_some() {
                            managed_tools::PROFILE
                        } else {
                            PROFILE
                        },
                    )
                })
                .is_err()
        {
            reply(command, failure(&request.id, "session_policy_mismatch"));
            return;
        }
        let cached = self.state.borrow().is_cached(&request.id);
        if !cached
            && self
                .managed
                .as_ref()
                .is_some_and(|managed| !managed.ready())
        {
            reply(command, failure(&request.id, "host_activity_unknown"));
            return;
        }
        let response = match self.state.borrow_mut().admit(request) {
            Ok(response) => response,
            Err(code) => {
                reply(command, failure(&request.id, code));
                return;
            }
        };
        if !cached {
            self.reply_start
                .set(self.acp.read_with(cx, |thread, _| thread.entries().len()));
            let text = request.text.clone().expect("validated prompt");
            let submission = self.acp.update(cx, |thread, cx| {
                thread.send(
                    vec![acp_v2::ContentBlock::Text(acp_v2::TextContent::new(text))],
                    cx,
                )
            });
            let state = self.state.clone();
            let task = cx.spawn(async move |_| {
                let result = turn_result(submission.await);
                state.borrow_mut().finish_turn(result);
            });
            *self.submission.borrow_mut() = Some(task);
        }
        reply(command, response);
    }

    fn control(&self, command: Command, cx: &mut AsyncApp) -> Option<Task<()>> {
        let op = command.request.op;
        let credential_mutation_pending = self.credential_mutation_check();
        if op != Operation::Interrupt && credential_mutation_pending() {
            self.state.borrow_mut().last_control_error = Some("credential_mutation_pending");
            reply(
                &command,
                failure(&command.request.id, "credential_mutation_pending"),
            );
            return None;
        }
        if let Err(code) = self.state.borrow().check_control(&command.request) {
            reply(&command, failure(&command.request.id, code));
            return None;
        }
        {
            let mut state = self.state.borrow_mut();
            state.active_control = true;
            if op == Operation::Shutdown {
                state.frozen = true;
            }
            if state.running && matches!(op, Operation::Interrupt | Operation::Shutdown) {
                state.interrupted = true;
            }
            state.sequence += 1;
        }
        let state = self.state.clone();
        let agent = self.agent.clone();
        let session_id = self
            .acp
            .read_with(cx, |thread, _| thread.session_id().clone());
        // Native cancel is awaited first: ACP's connection-level cancellation alone detaches it.
        #[cfg(test)]
        let cancel_override = self.hooks.borrow_mut().cancel.take();
        #[cfg(not(test))]
        let cancel_override: Option<Task<()>> = None;
        let native_cancel = cancel_override
            .unwrap_or_else(|| self.thread.update(cx, |thread, cx| thread.cancel(cx)));
        #[cfg(test)]
        let save_override = self.hooks.borrow_mut().save.take();
        #[cfg(not(test))]
        let save_override: Option<Task<Result<()>>> = None;
        #[cfg(test)]
        let quit = self.hooks.borrow().quit.clone();
        let acp_thread = self.acp.clone();
        let managed = self.managed.clone();
        let auth_state = self.auth_state.clone();
        let native_thread = self.thread.clone();
        let reply_start = self.reply_start.get();
        let submission = self.submission.borrow_mut().take();
        Some(cx.spawn(async move |cx| {
            {
                let timeout = cx.background_executor().timer(CONTROL_TIMEOUT).fuse();
                let drain = async {
                    native_cancel.await;
                    acp_thread.update(cx, |thread, cx| thread.cancel(cx)).await;
                    if let Some(submission) = submission {
                        submission.await;
                    }
                }
                .fuse();
                futures::pin_mut!(drain, timeout);
                select_biased! {
                    ()=drain => {},
                    ()=timeout => {
                        { let mut state=state.borrow_mut(); state.held=true; state.sequence+=1; }
                        reply(&command,failure(&command.request.id,"termination_unknown"));
                        drain.await;
                        return;
                    }
                }
            }
            let credentials_settled = || {
                if op == Operation::Interrupt || !credential_mutation_pending() {
                    return true;
                }
                let mut control = state.borrow_mut();
                control.active_control = false;
                control.last_control_error = Some("credential_mutation_pending");
                control.sequence += 1;
                reply(
                    &command,
                    failure(&command.request.id, "credential_mutation_pending"),
                );
                false
            };
            if !credentials_settled() {
                return;
            }
            if let Some(managed) = &managed {
                let started = std::time::Instant::now();
                while managed.activity.lock().expect("activity lock").active > 0 {
                    if started.elapsed() >= CONTROL_TIMEOUT {
                        break;
                    }
                    cx.background_executor()
                        .timer(Duration::from_millis(10))
                        .await;
                }
                if !managed.ready() {
                    state.borrow_mut().held = true;
                    reply(
                        &command,
                        failure(&command.request.id, "host_activity_unknown"),
                    );
                    return;
                }
                if !credentials_settled() {
                    return;
                }
                if auth_state.as_ref().is_some_and(|auth| {
                    auth.read_with(cx, |auth, _| auth.native_identity())
                        .ok()
                        .as_ref()
                        != Some(&managed.expected_identity)
                }) {
                    state.borrow_mut().held = true;
                    reply(
                        &command,
                        failure(&command.request.id, "scoped_identity_mismatch"),
                    );
                    return;
                }
            }
            if state.borrow().policy_violation {
                state.borrow_mut().held = true;
                reply(&command, failure(&command.request.id, "policy_violation"));
                return;
            }
            let persistence_ready = if managed.is_some() && op != Operation::Interrupt {
                cx.update(agent::verify_persistent_threads_database)
                    .await
                    .is_ok()
            } else {
                true
            };
            let saved = if !persistence_ready {
                false
            } else if op == Operation::Interrupt {
                true
            } else {
                let save = save_override.unwrap_or_else(|| {
                    agent.update(cx, |agent, cx| agent.flush_session(&session_id, cx))
                });
                let timeout = cx.background_executor().timer(CONTROL_TIMEOUT);
                select_biased! {
                    result=save.fuse() => result.is_ok(),
                    ()=timeout.fuse() => false,
                }
            };
            if !saved {
                state.borrow_mut().finish_control(false);
                reply(&command, failure(&command.request.id, "save_failed"));
                return;
            }
            if !credentials_settled() {
                return;
            }
            let mut result = json!({"drained":true,"saved":op != Operation::Interrupt});
            if let Some(managed) = &managed {
                let conversation_id = session_id.0.to_string();
                let turn = state.borrow().turn;
                let turn_id = managed.turn_id(turn);
                result["conversation_id"] = json!(conversation_id);
                result["turn_id"] = json!(turn_id);
                if op == Operation::Shutdown {
                    result["exiting"] = json!(true);
                }
                if op != Operation::Interrupt && !(op == Operation::Shutdown && turn == 0) {
                    let checked = native_thread.read_with(cx, |thread, _| {
                        check_native_profile(thread, managed_tools::PROFILE)
                    });
                    let text =
                        acp_thread.read_with(cx, |thread, _| current_reply(thread, reply_start));
                    if turn == 0 || checked.is_err() || text.len() > managed_tools::MAX_OUTPUT {
                        state.borrow_mut().held = true;
                        reply(
                            &command,
                            failure(&command.request.id, "result_evidence_invalid"),
                        );
                        return;
                    }
                    let last = state.borrow().last_result.unwrap_or("unknown");
                    let outcome = match last {
                        "completed" => "completed",
                        "interrupted" => "interrupted",
                        "unknown" => "unknown",
                        _ => "failed",
                    };
                    let envelope = managed_tools::ResultEnvelope {
                        schema_version: 1,
                        identity: managed.identity.clone(),
                        conversation_id: conversation_id.clone(),
                        turn_id: turn_id.clone(),
                        observed_model: format!("{PROVIDER}/{MODEL}"),
                        speed: "standard".into(),
                        outcome: outcome.into(),
                        reply: text,
                        checkpoint: managed_tools::Checkpoint {
                            saved: true,
                            conversation_id,
                            turn_id,
                        },
                        recorded_at_ms: match managed_tools::now_ms() {
                            Ok(time) => time,
                            Err(_) => {
                                state.borrow_mut().held = true;
                                reply(
                                    &command,
                                    failure(&command.request.id, "result_write_failed"),
                                );
                                return;
                            }
                        },
                        error_code: if ["completed", "interrupted"].contains(&last) {
                            String::new()
                        } else {
                            last.into()
                        },
                    };
                    let path = managed.result_file.clone();
                    // Do not yield between the credential check, result publication and
                    // acknowledgment: a new native mutation must not enter that gap.
                    if managed_tools::durable_json(&path, &envelope).is_err() {
                        state.borrow_mut().held = true;
                        reply(
                            &command,
                            failure(&command.request.id, "result_write_failed"),
                        );
                        return;
                    }
                }
            }
            state.borrow_mut().finish_control(true);
            reply(&command, success(&command.request.id, result));
            if op == Operation::Shutdown {
                // Attempt the acknowledgment before GPUI quits.
                let timeout = cx.background_executor().timer(Duration::from_secs(3));
                select_biased! { _=command.delivered.fuse()=>{}, ()=timeout.fuse()=>{} }
                // Delivery yields to native tasks. A mutation admitted since the response
                // must retain the runtime even though shutdown had previously been ready.
                if credential_mutation_pending() {
                    let mut control = state.borrow_mut();
                    control.active_control = false;
                    control.last_control_error = Some("credential_mutation_pending");
                    control.sequence += 1;
                    return;
                }
                #[cfg(test)]
                if let Some(quit) = quit {
                    quit.set(true);
                    return;
                }
                cx.update(|cx| cx.quit());
            }
        }))
    }
}

async fn run(
    args: Args,
    listener: UnixListener,
    managed: Option<Arc<managed_tools::Managed>>,
    cx: &mut AsyncApp,
) -> Result<()> {
    if managed.is_some() {
        cx.update(|cx| agent::init_persistent_threads_database(args.user_data_dir.clone(), cx))
            .await?;
    }
    let (app, provider_state) = match &managed {
        Some(managed) => cx.update(|cx| headless::init_managed(cx, &managed.namespace))?,
        None => cx.update(headless::init_subscription_only),
    };
    let auth = cx.update(|cx| {
        let registry = LanguageModelRegistry::global(cx);
        let registry = registry.read(cx);
        ensure!(
            registry.providers().len() == 1,
            "subscription-only registry required"
        );
        let provider = registry
            .provider(&openai_subscribed::PROVIDER_ID)
            .context("subscription provider required")?;
        Ok(provider.authenticate(cx))
    })?;
    let timeout = cx.background_executor().timer(Duration::from_secs(30));
    select_biased! {
        result=auth.fuse()=>{result.map_err(|_|anyhow::anyhow!("subscription authentication failed"))?;},
        ()=timeout.fuse()=>anyhow::bail!("subscription authentication timed out"),
    }
    if let Some(managed) = &managed {
        let discovery = provider_state.update(cx, |state, cx| {
            state.discover_native_models(managed.expected_identity.clone(), cx)
        });
        let timeout = cx.background_executor().timer(Duration::from_secs(30));
        select_biased! {
            result=discovery.fuse()=>result.map_err(|_|anyhow::anyhow!("scoped discovery failed"))?,
            ()=timeout.fuse()=>anyhow::bail!("scoped discovery timed out"),
        }
        ensure!(
            provider_state
                .read_with(cx, |state, _| state.native_identity())
                .ok()
                .as_ref()
                == Some(&managed.expected_identity),
            "scoped identity mismatch"
        );
    }
    let started = std::time::Instant::now();
    loop {
        let discovered = provider_state.read_with(cx, |state, _| {
            state.is_authenticated()
                && state.model_catalog_error().is_none()
                && state.available_models().iter().any(|model| {
                    matches!(model, openai_subscribed::ChatGptModel::Discovered(_))
                        && model.id() == MODEL
                })
        });
        if discovered {
            break;
        }
        ensure!(
            started.elapsed() < Duration::from_secs(30),
            "exact discovered model unavailable"
        );
        cx.background_executor()
            .timer(Duration::from_millis(100))
            .await;
    }
    cx.update(|cx| {
        let model = LanguageModelRegistry::global(cx)
            .read(cx)
            .available_models(cx)
            .find(|model| {
                model.provider_id() == openai_subscribed::PROVIDER_ID
                    && model.id().0.as_ref() == MODEL
            })
            .context("exact native model required")?;
        LanguageModelRegistry::global(cx).update(cx, |registry, cx| {
            registry.set_default_model(Some(model.clone()), cx);
            if managed.is_some() {
                registry.set_thread_summary_model(Some(model.clone()), cx);
                registry.set_compaction_model(Some(model), cx);
            }
        });
        SettingsStore::update_global(cx, |store, cx| {
            store
                .set_user_settings(
                    &if managed.is_some() {
                        managed_policy_settings()
                    } else {
                        policy_settings()
                    }
                    .to_string(),
                    cx,
                )
                .result()
        })
    })?;
    let project = cx.update(|cx| {
        Project::local(
            app.client.clone(),
            app.node_runtime.clone(),
            app.user_store.clone(),
            app.languages.clone(),
            app.fs.clone(),
            managed.as_ref().map(|_| Default::default()),
            project::LocalProjectFlags {
                init_worktree_trust: false,
                watch_global_configs: managed.is_none(),
            },
            cx,
        )
    });
    if managed.is_none() {
        let worktree = project
            .update(cx, |project, cx| {
                project.create_worktree(&args.workdir, true, cx)
            })
            .await?;
        worktree
            .read_with(cx, |tree, _| {
                tree.as_local()
                    .context("local worktree required")
                    .map(|tree| tree.scan_complete())
            })?
            .await;
    }
    let agent = cx.update(|cx| {
        let store = cx.new(ThreadStore::new);
        NativeAgent::new(store, Templates::new(), app.fs.clone(), cx)
    });
    let connection = Rc::new(NativeAgentConnection(agent.clone()));
    let acp = cx
        .update(|cx| match &args.load_session {
            Some(id) => connection.clone().load_session(
                acp::SessionId::new(id.clone()),
                project.clone(),
                PathList::new(&[&args.workdir]),
                None,
                cx,
            ),
            None => {
                connection
                    .clone()
                    .new_session(project.clone(), PathList::new(&[&args.workdir]), cx)
            }
        })
        .await?;
    let epoch = match &managed {
        Some(managed) => managed.identity.epoch.clone(),
        None => format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ),
    };
    let mut resident = Resident::new_with_managed(connection, acp, project, epoch, managed, cx)?;
    resident.native_credentials =
        provider_state.read_with(cx, |state, _| state.native_credentials());
    ensure!(
        resident.managed.is_none() || resident.native_credentials.is_some(),
        "scoped credentials unavailable"
    );
    resident.auth_state = Some(provider_state);
    let stop = Arc::new(AtomicBool::new(false));
    let (sender, mut commands) = mpsc::channel(QUEUE_LIMIT);
    start_socket(listener, sender, stop.clone());
    let mut control_task = None;
    while let Some(command) = commands.next().await {
        match command.request.op {
            Operation::Status => {
                let response = if resident.state.borrow().is_cached(&command.request.id) {
                    failure(&command.request.id, "id_conflict")
                } else {
                    success(&command.request.id, resident.status(cx))
                };
                reply(&command, response);
            }
            Operation::Prompt => resident.prompt(&command, cx),
            _ => {
                if let Some(task) = resident.control(command, cx) {
                    control_task = Some(task);
                }
            }
        }
    }
    stop.store(true, Ordering::Relaxed);
    drop(control_task);
    Ok(())
}

fn main() {
    // Shell environment capture invokes this executable without the required probe flags.
    if std::env::args_os()
        .skip(1)
        .any(|arg| arg == "--capabilities")
    {
        println!("{}", managed_tools::capabilities());
        return;
    }
    if std::env::args_os()
        .nth(1)
        .is_some_and(|arg| arg == "--printenv")
    {
        util::shell_env::print_env();
        return;
    }
    let mut args = Args::parse();
    let listener = match prepare(&mut args) {
        Ok(listener) => listener,
        Err(_) => {
            eprintln!("lifecycle probe: invalid or non-private startup paths/state");
            std::process::exit(1);
        }
    };
    let managed_guard = if args.managed.managed {
        if args.user_data_dir.starts_with(&args.workdir)
            || args.workdir.starts_with(&args.user_data_dir)
        {
            eprintln!("lifecycle probe: managed data/worktree overlap");
            std::process::exit(1);
        }
        let timestamp = match managed_tools::now_ms() {
            Ok(time) => time,
            Err(_) => {
                eprintln!("lifecycle probe: process clock unavailable");
                std::process::exit(1);
            }
        };
        let epoch = format!("{}-{timestamp}", std::process::id());
        match managed_tools::Managed::prepare(&args.managed, &args.workdir, epoch) {
            Ok(managed) => {
                if args.user_data_dir.starts_with(&managed.git_dir) {
                    eprintln!("lifecycle probe: managed data/Git mount overlap");
                    std::process::exit(1);
                }
                Some(managed)
            }
            Err(_) => {
                eprintln!("lifecycle probe: invalid or ambiguous managed launch");
                std::process::exit(1);
            }
        }
    } else {
        None
    };
    let managed = managed_guard.clone();
    let app =
        gpui_platform::headless().with_http_client(Arc::new(reqwest_client::ReqwestClient::new()));
    let failed = Rc::new(std::cell::Cell::new(false));
    let initialization_failed = failed.clone();
    app.run(move |cx| {
        cx.spawn(async move |cx| {
            if run(args, listener, managed, cx).await.is_err() {
                eprintln!("lifecycle probe: native initialization failed");
                initialization_failed.set(true);
                cx.update(|cx| cx.quit());
            }
        })
        .detach();
    });
    if failed.get() {
        std::process::exit(1);
    }
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "lifecycle_managed_tests.rs"]
mod managed_tests;
