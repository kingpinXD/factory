use super::managed_tool_files::{FileInput, ManagedFiles};
use super::managed_tools::*;
use super::*;
use gpui::TestAppContext;
use language_model::{
    LanguageModel, LanguageModelCompletionEvent, LanguageModelProviderId,
    LanguageModelProviderName, LanguageModelToolUse, LanguageModelToolUseInput, StopReason,
    fake_provider::FakeLanguageModelProvider,
};
use std::os::unix::fs::symlink;

struct FixtureDir(PathBuf);
impl FixtureDir {
    fn new() -> Self {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = PathBuf::from(format!(
            "/tmp/zm-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self(path.canonicalize().unwrap())
    }
    fn args(&self) -> ManagedArgs {
        for directory in [
            "run",
            "run/git",
            "work",
            "brain",
            "brain/docs",
            "reports",
            "data",
        ] {
            std::fs::create_dir(self.0.join(directory)).unwrap();
        }
        std::fs::set_permissions(self.0.join("run"), std::fs::Permissions::from_mode(0o700))
            .unwrap();
        for name in ["docker", "factory"] {
            std::fs::write(self.0.join(name), b"test fixture, never execute").unwrap();
            std::fs::set_permissions(self.0.join(name), std::fs::Permissions::from_mode(0o700))
                .unwrap();
        }
        std::fs::write(self.0.join("brain/AGENTS.md"), "knowledge").unwrap();
        std::fs::write(self.0.join("brain/docs/safe.md"), "knowledge").unwrap();
        ManagedArgs {
            managed: true,
            job_id: Some("job-fixture".into()),
            generation: Some(3),
            instance_token: Some("launch-fixture".into()),
            identity_file: Some(self.0.join("run/identity.json")),
            result_file: Some(self.0.join("run/result.json")),
            lock_file: Some(self.0.join("run/owner.lock")),
            credential_namespace: Some(
                openai_subscribed::native_auth::FACTORY_CREDENTIAL_NAMESPACE.into(),
            ),
            expected_identity: Some("binding-fixture".into()),
            container_id: Some("a".repeat(64)),
            docker_bin: Some(self.0.join("docker")),
            factory_bin: Some(self.0.join("factory")),
            brain: Some(self.0.join("brain")),
            output_file: Some(self.0.join("reports/out.md")),
            git_dir: Some(self.0.join("run/git")),
            git_index: Some(self.0.join("run/git/index")),
        }
    }
}
impl Drop for FixtureDir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
#[derive(Default)]
struct FakeExecutor {
    specs: std::sync::Mutex<Vec<CommandSpec>>,
    wait: std::sync::atomic::AtomicBool,
    cancelled: Arc<AtomicUsize>,
    lease_reply: std::sync::Mutex<Option<Result<String>>>,
    cancel_after_lease: std::sync::atomic::AtomicBool,
}
impl CommandExecutor for FakeExecutor {
    fn execute(
        &self,
        spec: CommandSpec,
        activity: Arc<std::sync::Mutex<Activity>>,
        _cancel: Arc<Cancellation>,
    ) -> futures::future::BoxFuture<'static, Result<String>> {
        let lease = serde_json::from_slice::<Value>(&spec.stdin)
            .is_ok_and(|request| request["op"] == "lease_check");
        let docker = spec
            .executable
            .file_name()
            .is_some_and(|name| name == "docker");
        self.specs.lock().unwrap().push(spec);
        let reply = if lease {
            self.lease_reply.lock().unwrap().take().unwrap_or_else(|| Ok(
                json!({"schema_version":1,"op":"lease_check","job_id":"job-fixture","generation":3,"allowed":true}).to_string()
            ))
        } else {
            Ok(r#"{"schema_version":1,"ok":true}"#.into())
        };
        let guard = ActivityGuard::new(activity);
        let wait = docker && self.wait.load(Ordering::SeqCst);
        let cancelled = self.cancelled.clone();
        let cancel_after_lease = lease && self.cancel_after_lease.load(Ordering::SeqCst);
        async move {
            if wait {
                _cancel.wait().await;
                cancelled.fetch_add(1, Ordering::SeqCst);
                guard.complete();
                return Err(anyhow::anyhow!("fake cancelled child reaped"));
            }
            if cancel_after_lease {
                _cancel.cancel();
            }
            guard.complete();
            reply
        }
        .boxed()
    }
}
fn configured() -> (FixtureDir, ManagedArgs, Arc<Managed>, Arc<FakeExecutor>) {
    let dir = FixtureDir::new();
    let args = dir.args();
    let mut managed = Managed::prepare(&args, &dir.0.join("work"), "epoch-fixture".into()).unwrap();
    let executor = Arc::new(FakeExecutor::default());
    let config = Arc::get_mut(&mut managed).unwrap();
    config.executor = executor.clone();
    config.environment = std::collections::BTreeMap::from([
        (
            "HOME".into(),
            dir.0.join("home-fixture").to_string_lossy().into_owned(),
        ),
        ("PATH".into(), "/fixture/bin".into()),
        ("GH_TOKEN".into(), "offline-sentinel".into()),
    ]);
    (dir, args, managed, executor)
}
#[test]
fn managed_flags_and_capabilities_match_abi() {
    assert_eq!(
        capabilities(),
        json!({"schema_version":1,"native_runtime":"zed","managed":true,"containment":"docker","model":"openai-subscribed/gpt-6.1-sol","control_protocol":1})
    );
    let base = [
        "probe",
        "--workdir",
        "/work",
        "--user-data-dir",
        "/data",
        "--socket",
        "/socket",
    ];
    let mut incomplete = base.to_vec();
    incomplete.push("--managed");
    assert!(Args::try_parse_from(incomplete).is_err());
    let mut wrong = base.to_vec();
    wrong.extend(["--job-id", "not-managed"]);
    assert!(Args::try_parse_from(wrong).is_err());
    let mut full = base.to_vec();
    full.extend([
        "--managed",
        "--job-id",
        "job",
        "--generation",
        "1",
        "--instance-token",
        "nonce",
        "--identity-file",
        "/run/identity",
        "--result-file",
        "/run/result",
        "--lock-file",
        "/run/lock",
        "--credential-namespace",
        "namespace",
        "--expected-identity",
        "binding",
        "--container-id",
        "container",
        "--docker-bin",
        "/bin/docker",
        "--factory-bin",
        "/bin/factory",
        "--brain",
        "/brain",
        "--output-file",
        "/output.md",
        "--git-dir",
        "/run/git",
        "--git-index",
        "/run/git/index",
    ]);
    assert!(Args::try_parse_from(full).unwrap().managed.managed);
}
#[test]
fn identity_is_private_durable_early_and_lock_does_not_take_over() {
    let (dir, args, managed, _) = configured();
    let value: Value =
        serde_json::from_slice(&std::fs::read(args.identity_file.as_ref().unwrap()).unwrap())
            .unwrap();
    assert_eq!(
        value,
        json!({"schema_version":1,"job_id":"job-fixture","generation":3,"instance_token":"launch-fixture","pid":std::process::id(),"process_group":unsafe{libc::getpgrp()},"start_identity":managed.identity.start_identity,"epoch":"epoch-fixture"})
    );
    assert!(!managed.identity.start_identity.is_empty());
    assert_eq!(
        std::fs::metadata(args.identity_file.as_ref().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert!(Managed::prepare(&args, &dir.0.join("work"), "replacement".into()).is_err());
    assert!(!args.result_file.as_ref().unwrap().exists());
    drop(managed);
    // An unlocked stale identity is still ambiguous, not permission to replace it.
    assert!(Managed::prepare(&args, &dir.0.join("work"), "replacement".into()).is_err());
}
#[test]
fn container_command_is_fixed_and_helper_ops_are_typed() {
    let (_dir, _args, managed, executor) = configured();
    let shell = "printf '%s' '$(host-injection)'";
    let spec = managed
        .terminal(TerminalInput {
            command: shell.into(),
            cd: None,
            timeout_ms: Some(1_000_000),
        })
        .unwrap();
    assert_eq!(spec.executable, managed.docker_bin);
    assert_eq!(spec.args[0], "exec");
    assert_eq!(
        &spec.args[spec.args.len() - 4..],
        &["a".repeat(64), "/bin/sh".into(), "-c".into(), shell.into()]
    );
    assert!(
        spec.args
            .contains(&format!("GIT_DIR={}", managed.git_dir.display()))
    );
    assert!(
        spec.args
            .contains(&format!("GIT_INDEX_FILE={}", managed.git_index.display()))
    );
    assert!(spec.args.contains(&"GIT_OPTIONAL_LOCKS=0".into()));
    assert_eq!(spec.timeout_ms, 120_000);
    assert!(!spec.args.iter().any(|arg| arg.contains("FACTORY_BRAIN")));
    assert!(
        managed
            .terminal(TerminalInput {
                command: "id".into(),
                cd: Some("/other".into()),
                timeout_ms: None
            })
            .is_err()
    );
    for op in [
        "submit",
        "continue",
        "result",
        "yield",
        "inbox",
        "lease_check",
        "heartbeat",
        "intent",
        "done",
        "error",
        "brain_read",
    ] {
        let payload = if op == "brain_read" {
            json!({"path":"docs/safe.md","max_bytes":100})
        } else {
            json!({})
        };
        let request: HelperInput =
            serde_json::from_value(json!({"op":op,"payload":payload})).unwrap();
        let spec = managed.helper(request).unwrap();
        assert_eq!(
            spec.args,
            vec![
                "helper",
                "control",
                "--job",
                "job-fixture",
                "--generation",
                "3",
                "--brain",
                managed.brain.to_str().unwrap(),
                "--request",
                "-"
            ]
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&spec.stdin).unwrap()["op"],
            op
        );
        assert_eq!(spec.executable, managed.factory_bin);
    }
    for op in [
        "start",
        "end",
        "managed_start",
        "managed_end",
        "inbox_ack",
        "exec",
    ] {
        assert!(serde_json::from_value::<HelperInput>(json!({"op":op,"payload":{}})).is_err());
    }
    for field in [
        "owner",
        "lease",
        "executable",
        "factory_bin",
        "brain",
        "generation",
        "container_id",
    ] {
        assert!(
            managed
                .helper(HelperInput {
                    op: HelperOp::Submit,
                    payload: json!({(field):"escape"})
                })
                .is_err()
        );
    }
    assert!(executor.specs.lock().unwrap().is_empty());
}
#[test]
fn file_capabilities_confine_writes_and_reject_symlinks_and_hardlink_mutation() {
    let (dir, _args, managed, _) = configured();
    let files = ManagedFiles::new(managed.clone()).unwrap();
    let call = |value: Value| files.execute(serde_json::from_value::<FileInput>(value).unwrap());
    call(json!({"op":"write","path":"code.txt","content":"original"})).unwrap();
    assert_eq!(
        call(json!({"op":"read","path":"code.txt"})).unwrap(),
        "original"
    );
    call(json!({"op":"edit","path":"code.txt","old_text":"original","new_text":"edited"})).unwrap();
    call(json!({"op":"write","path":managed.output,"content":"# Assigned output"})).unwrap();
    call(json!({"op":"delete","path":managed.output})).unwrap();
    call(json!({"op":"create_directory","path":"tree"})).unwrap();
    call(json!({"op":"write","path":"tree/child.txt","content":"owned"})).unwrap();
    assert!(call(json!({"op":"move","path":"tree","target":managed.output})).is_err());
    assert!(call(json!({"op":"create_directory","path":managed.output})).is_err());
    assert!(!managed.output.exists());
    for path in [
        "../escape",
        ".git/config",
        ".env.production",
        managed.result_file.to_str().unwrap(),
    ] {
        assert!(
            call(json!({"op":"write","path":path,"content":"bad"})).is_err(),
            "{path}"
        );
    }
    symlink(&managed.brain, managed.workdir.join("escape")).unwrap();
    assert!(call(json!({"op":"write","path":"escape/AGENTS.md","content":"bad"})).is_err());
    symlink(
        managed.brain.join("AGENTS.md"),
        managed.workdir.join("link"),
    )
    .unwrap();
    assert!(call(json!({"op":"read","path":"link"})).is_err());
    std::fs::hard_link(
        managed.brain.join("AGENTS.md"),
        managed.workdir.join("hard-link"),
    )
    .unwrap();
    call(json!({"op":"write","path":"hard-link","content":"replacement"})).unwrap();
    assert_eq!(
        std::fs::read_to_string(dir.0.join("brain/AGENTS.md")).unwrap(),
        "knowledge"
    );
    for path in [
        "../run/result.json",
        "connectors/auth.md",
        "docs/../../secret.md",
        "features/runtime/job.md",
    ] {
        assert!(
            managed
                .helper(HelperInput {
                    op: HelperOp::BrainRead,
                    payload: json!({"path":path})
                })
                .is_err()
        );
    }
    symlink(&managed.workdir, managed.brain.join("docs/outside")).unwrap();
    assert!(
        managed
            .helper(HelperInput {
                op: HelperOp::BrainRead,
                payload: json!({"path":"docs/outside/code.txt"})
            })
            .is_err()
    );
}

async fn native_fixture(
    cx: &mut TestAppContext,
) -> (
    FixtureDir,
    Resident,
    Arc<FakeLanguageModelProvider>,
    LanguageModel,
    Arc<FakeExecutor>,
) {
    native_fixture_database(cx, false).await
}

async fn native_fixture_database(
    cx: &mut TestAppContext,
    broken_database: bool,
) -> (
    FixtureDir,
    Resident,
    Arc<FakeLanguageModelProvider>,
    LanguageModel,
    Arc<FakeExecutor>,
) {
    let (dir, _, managed, executor) = configured();
    let data_dir = dir.0.join("data");
    if !data_dir.exists() {
        std::fs::create_dir(&data_dir).unwrap();
    }
    std::fs::set_permissions(&data_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    if broken_database {
        std::fs::create_dir_all(data_dir.join("threads/threads.db")).unwrap();
    }
    let persistence = cx
        .update(|cx| agent::init_persistent_threads_database(data_dir.clone(), cx))
        .await;
    assert_eq!(persistence.is_ok(), !broken_database);
    assert_eq!(
        std::fs::metadata(&data_dir).unwrap().permissions().mode() & 0o777,
        0o700
    );
    let fake = Arc::new(FakeLanguageModelProvider::new(
        LanguageModelProviderId::from(PROVIDER.to_string()),
        LanguageModelProviderName::from("Offline".to_string()),
    ));
    let model = fake.model(MODEL);
    let http = http_client::FakeHttpClient::create(|_| async {
        panic!("no managed HTTP in offline fixture")
    });
    cx.update(|cx| {
        let settings = SettingsStore::test(cx);
        cx.set_global(settings);
        cx.set_http_client(http.clone());
        language_model::init(cx);
        LanguageModelRegistry::global(cx).update(cx, |registry, cx| {
            registry.register_provider(fake.clone(), cx);
            registry.set_default_model(Some(model.clone()), cx);
        });
        SettingsStore::update_global(cx, |store, cx| {
            store
                .set_user_settings(&managed_policy_settings().to_string(), cx)
                .result()
        })
        .unwrap();
    });
    let fs = headless::managed_project_fs();
    cx.update(|cx| <dyn fs::Fs>::set_global(fs.clone(), cx));
    let client =
        cx.update(|cx| client::Client::new(Arc::new(clock::FakeSystemClock::new()), http, cx));
    let user = cx.new(|cx| client::UserStore::new(client.clone(), cx));
    let languages = Arc::new(language::LanguageRegistry::test(cx.executor()));
    let project = cx.update(|cx| {
        Project::local(
            client,
            node_runtime::NodeRuntime::unavailable(),
            user,
            languages,
            fs.clone(),
            Some(Default::default()),
            project::LocalProjectFlags {
                init_worktree_trust: false,
                watch_global_configs: false,
            },
            cx,
        )
    });
    let agent = cx.update(|cx| {
        let store = cx.new(ThreadStore::new);
        NativeAgent::new(store, Templates::new(), fs, cx)
    });
    let connection = Rc::new(NativeAgentConnection(agent));
    let acp = cx
        .update(|cx| {
            connection
                .clone()
                .new_session(project.clone(), PathList::new(&[] as &[&Path]), cx)
        })
        .await
        .unwrap();
    let resident = Resident::new_with_managed(
        connection,
        acp,
        project,
        "epoch-fixture".into(),
        Some(managed),
        &mut cx.to_async(),
    )
    .unwrap();
    (dir, resident, fake, model, executor)
}
fn command(op: Operation, id: &str, text: Option<&str>) -> (Command, sync_mpsc::Receiver<Value>) {
    let (reply, response) = sync_mpsc::sync_channel(1);
    let (_, delivered) = oneshot::channel();
    (
        Command {
            request: Request {
                v: 1,
                id: id.into(),
                op,
                text: text.map(str::to_string),
            },
            reply,
            delivered,
        },
        response,
    )
}
#[gpui::test]
async fn managed_native_allowlist_and_durable_result_use_structured_current_reply(
    cx: &mut TestAppContext,
) {
    let (_dir, resident, fake, model, executor) = native_fixture(cx).await;
    let mut app = cx.to_async();
    assert_eq!(
        resident
            ._project
            .read_with(cx, |project, cx| project.visible_worktrees(cx).count()),
        0
    );
    let (prompt, response) = command(
        Operation::Prompt,
        "round-key",
        Some("user text must not be reply"),
    );
    resident.prompt(&prompt, &mut app);
    assert_eq!(
        response.try_recv().unwrap(),
        json!({"v":1,"id":"round-key","ok":true,"result":{"admitted":true,"turn_id":"epoch-fixture:1"}})
    );
    cx.run_until_parked();
    let names = fake.pending_completions()[0]
        .tools
        .iter()
        .map(|tool| tool.name.to_string())
        .collect::<Vec<_>>();
    let mut names = names;
    names.sort();
    assert_eq!(
        names,
        vec!["factory_helper", "managed_files", "managed_terminal"]
    );
    fake.send_last_text(&model, "current answer only");
    fake.send_last_event(
        &model,
        LanguageModelCompletionEvent::Stop(StopReason::EndTurn),
    );
    fake.end_last(&model);
    cx.run_until_parked();
    let status = resident.status(&mut app);
    for (key, value) in [
        ("turn_id", json!("epoch-fixture:1")),
        ("reply", json!("current answer only")),
        ("managed", json!(true)),
        ("containment", json!("docker")),
        ("profile", json!("factory-managed-v1")),
        ("identity_binding", json!("binding-fixture")),
    ] {
        assert_eq!(status[key], value, "{key}");
    }
    let (checkpoint, response) = command(Operation::Checkpoint, "save", None);
    resident.control(checkpoint, &mut app).unwrap().await;
    let conversation = status["session_id"].as_str().unwrap();
    assert_eq!(
        response.try_recv().unwrap(),
        json!({"v":1,"id":"save","ok":true,"result":{"saved":true,"drained":true,"conversation_id":conversation,"turn_id":"epoch-fixture:1"}})
    );
    let managed = resident.managed.as_ref().unwrap();
    let bytes = std::fs::read(&managed.result_file).unwrap();
    let envelope: ResultEnvelope = serde_json::from_slice(&bytes).unwrap();
    assert!(envelope.recorded_at_ms > 0);
    assert_eq!(
        serde_json::to_value(&envelope).unwrap(),
        json!({"schema_version":1,"identity":managed.identity,"conversation_id":conversation,"turn_id":"epoch-fixture:1","observed_model":"openai-subscribed/gpt-6.1-sol","speed":"standard","outcome":"completed","reply":"current answer only","checkpoint":{"saved":true,"conversation_id":conversation,"turn_id":"epoch-fixture:1"},"recorded_at_ms":envelope.recorded_at_ms,"error_code":""})
    );
    assert_eq!(
        std::fs::metadata(&managed.result_file)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert!(executor.specs.lock().unwrap().is_empty());
}
#[gpui::test]
async fn managed_terminal_native_dispatch_reaches_only_fake_docker_executor(
    cx: &mut TestAppContext,
) {
    let (_dir, resident, fake, model, executor) = native_fixture(cx).await;
    let mut app = cx.to_async();
    let (prompt, _) = command(Operation::Prompt, "run", Some("test"));
    resident.prompt(&prompt, &mut app);
    cx.run_until_parked();
    let input = json!({"command":"echo fixture","timeout_ms":1000});
    fake.send_last_event(
        &model,
        LanguageModelCompletionEvent::ToolUse(LanguageModelToolUse {
            id: "tool-terminal".into(),
            name: "managed_terminal".into(),
            raw_input: input.to_string(),
            input: LanguageModelToolUseInput::Json(input),
            is_input_complete: true,
            thought_signature: None,
        }),
    );
    fake.send_last_event(
        &model,
        LanguageModelCompletionEvent::Stop(StopReason::ToolUse),
    );
    fake.end_last(&model);
    cx.run_until_parked();
    assert_eq!(executor.specs.lock().unwrap().len(), 2);
    assert_eq!(
        executor.specs.lock().unwrap()[1].executable,
        resident.managed.as_ref().unwrap().docker_bin
    );
    assert!(!resident.state.borrow().held);
    fake.send_last_text(&model, "tool reply");
    fake.send_last_event(
        &model,
        LanguageModelCompletionEvent::Stop(StopReason::EndTurn),
    );
    fake.end_last(&model);
    cx.run_until_parked();
    assert_eq!(resident.status(&mut app)["last_turn_result"], "completed");
}
#[gpui::test]
async fn managed_failed_save_or_uncertain_host_child_cannot_publish_result(
    cx: &mut TestAppContext,
) {
    let (_dir, resident, fake, model, _) = native_fixture(cx).await;
    let mut app = cx.to_async();
    let (prompt, _) = command(Operation::Prompt, "run", Some("test"));
    resident.prompt(&prompt, &mut app);
    cx.run_until_parked();
    fake.send_last_text(&model, "answer");
    fake.send_last_event(
        &model,
        LanguageModelCompletionEvent::Stop(StopReason::EndTurn),
    );
    fake.end_last(&model);
    cx.run_until_parked();
    resident.hooks.borrow_mut().save =
        Some(Task::ready(Err(anyhow::anyhow!("injected private error"))));
    let (checkpoint, response) = command(Operation::Checkpoint, "save", None);
    resident.control(checkpoint, &mut app).unwrap().await;
    assert_eq!(response.try_recv().unwrap()["error"]["code"], "save_failed");
    let managed = resident.managed.as_ref().unwrap();
    assert!(!managed.result_file.exists());
    drop(ActivityGuard::new(managed.activity.clone()));
    let (shutdown, response) = command(Operation::Shutdown, "quit", None);
    resident.control(shutdown, &mut app).unwrap().await;
    assert_eq!(
        response.try_recv().unwrap()["error"]["code"],
        "host_activity_unknown"
    );
    assert!(!managed.result_file.exists());
}

#[gpui::test]
async fn every_native_helper_operation_uses_fixed_scope_and_fake_executor(cx: &mut TestAppContext) {
    let (_dir, resident, fake, model, executor) = native_fixture(cx).await;
    let mut app = cx.to_async();
    let (prompt, _) = command(Operation::Prompt, "bridge", Some("offline bridge"));
    resident.prompt(&prompt, &mut app);
    cx.run_until_parked();
    for (index, op) in [
        "submit",
        "continue",
        "result",
        "yield",
        "inbox",
        "lease_check",
        "heartbeat",
        "intent",
        "done",
        "error",
        "brain_read",
    ]
    .iter()
    .enumerate()
    {
        let payload = if *op == "brain_read" {
            json!({"path":"docs/safe.md","max_bytes":128})
        } else {
            json!({})
        };
        let input = json!({"op":op,"payload":payload});
        fake.send_last_event(
            &model,
            LanguageModelCompletionEvent::ToolUse(LanguageModelToolUse {
                id: format!("tool-{index}").into(),
                name: "factory_helper".into(),
                raw_input: input.to_string(),
                input: LanguageModelToolUseInput::Json(input),
                is_input_complete: true,
                thought_signature: None,
            }),
        );
        fake.send_last_event(
            &model,
            LanguageModelCompletionEvent::Stop(StopReason::ToolUse),
        );
        fake.end_last(&model);
        cx.run_until_parked();
        let specs = executor.specs.lock().unwrap();
        assert_eq!(specs.len(), index + 1);
        assert_eq!(
            serde_json::from_slice::<Value>(&specs[index].stdin).unwrap()["op"],
            *op
        );
        assert_eq!(specs[index].args[3], "job-fixture");
        assert_eq!(specs[index].args[5], "3");
        assert!(!resident.state.borrow().held);
    }
    fake.send_last_text(&model, "bridge finished");
    fake.send_last_event(
        &model,
        LanguageModelCompletionEvent::Stop(StopReason::EndTurn),
    );
    fake.end_last(&model);
    cx.run_until_parked();
    assert_eq!(resident.status(&mut app)["last_turn_result"], "completed");
}
#[gpui::test]
async fn managed_checkpoint_waits_for_save_and_refuses_result_write_failure(
    cx: &mut TestAppContext,
) {
    let (_dir, resident, fake, model, _) = native_fixture(cx).await;
    let mut app = cx.to_async();
    let (prompt, _) = command(Operation::Prompt, "bridge", Some("test"));
    resident.prompt(&prompt, &mut app);
    cx.run_until_parked();
    fake.send_last_text(&model, "answer");
    fake.send_last_event(
        &model,
        LanguageModelCompletionEvent::Stop(StopReason::EndTurn),
    );
    fake.end_last(&model);
    cx.run_until_parked();
    let (release, wait) = oneshot::channel();
    resident.hooks.borrow_mut().save = Some(cx.spawn(async move |_| wait.await.unwrap()));
    let (checkpoint, response) = command(Operation::Checkpoint, "save", None);
    let task = resident.control(checkpoint, &mut app).unwrap();
    cx.run_until_parked();
    assert!(response.try_recv().is_err());
    assert!(!resident.managed.as_ref().unwrap().result_file.exists());
    let (prompt, rejected) = command(Operation::Prompt, "other", Some("not while saving"));
    resident.prompt(&prompt, &mut app);
    assert_eq!(rejected.try_recv().unwrap()["error"]["code"], "busy");
    std::fs::create_dir(&resident.managed.as_ref().unwrap().result_file).unwrap();
    release.send(Ok(())).unwrap();
    task.await;
    assert_eq!(
        response.try_recv().unwrap()["error"]["code"],
        "result_write_failed"
    );
    assert_eq!(resident.status(&mut app)["state"], "held");
}
#[gpui::test]
async fn managed_shutdown_json_includes_matching_checkpoint_before_exit(cx: &mut TestAppContext) {
    let (_dir, resident, fake, model, _) = native_fixture(cx).await;
    let mut app = cx.to_async();
    let quit = Rc::new(std::cell::Cell::new(false));
    resident.hooks.borrow_mut().quit = Some(quit.clone());
    let (prompt, _) = command(Operation::Prompt, "round", Some("test"));
    resident.prompt(&prompt, &mut app);
    cx.run_until_parked();
    fake.send_last_text(&model, "answer");
    fake.send_last_event(
        &model,
        LanguageModelCompletionEvent::Stop(StopReason::EndTurn),
    );
    fake.end_last(&model);
    cx.run_until_parked();
    let conversation = resident.status(&mut app)["session_id"].clone();
    let (shutdown, response) = command(Operation::Shutdown, "quit", None);
    resident.control(shutdown, &mut app).unwrap().await;
    assert_eq!(
        response.try_recv().unwrap(),
        json!({"v":1,"id":"quit","ok":true,"result":{"saved":true,"drained":true,"conversation_id":conversation,"turn_id":"epoch-fixture:1","exiting":true}})
    );
    assert!(quit.get());
    assert!(resident.managed.as_ref().unwrap().result_file.is_file());
}

struct EmptyNativeCredentials(Arc<std::sync::Mutex<Vec<String>>>);
impl credentials_provider::CredentialsProvider for EmptyNativeCredentials {
    fn read_credentials<'a>(
        &'a self,
        key: &'a str,
        _: &'a gpui::AsyncApp,
    ) -> futures::future::LocalBoxFuture<'a, Result<Option<(String, Vec<u8>)>>> {
        self.0.lock().unwrap().push(key.into());
        async { Ok(None) }.boxed_local()
    }
    fn write_credentials<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
        _: &'a [u8],
        _: &'a gpui::AsyncApp,
    ) -> futures::future::LocalBoxFuture<'a, Result<()>> {
        async { panic!("no credential writes in fixture") }.boxed_local()
    }
    fn delete_credentials<'a>(
        &'a self,
        _: &'a str,
        _: &'a gpui::AsyncApp,
    ) -> futures::future::LocalBoxFuture<'a, Result<()>> {
        async { panic!("no credential deletes in fixture") }.boxed_local()
    }
}
#[gpui::test]
async fn scoped_subscription_constructor_never_reads_desktop_key_or_discovers_on_startup(
    cx: &mut TestAppContext,
) {
    use language_model::LanguageModelProviderState as _;
    assert_eq!(
        openai_subscribed::native_auth::FACTORY_CREDENTIAL_NAMESPACE,
        "factory-openai"
    );
    // The public namespace alias is distinct from its private backing credential key.
    let keys = Arc::new(std::sync::Mutex::new(Vec::new()));
    let credentials = openai_subscribed::native_auth::NativeCredentials::new(
        Arc::new(EmptyNativeCredentials(keys.clone())),
        openai_subscribed::native_auth::FACTORY_CREDENTIAL_NAMESPACE,
    )
    .unwrap();
    let http = http_client::FakeHttpClient::create(|_| async {
        panic!("no discovery or OAuth in constructor fixture")
    });
    let state = cx.update(|cx| {
        language_models::provider::openai_subscribed::OpenAiSubscribedProvider::new_native(
            http,
            credentials,
            cx,
        )
        .observable_entity()
        .unwrap()
    });
    let load = state.read_with(cx, |state, _| state.load_task()).unwrap();
    load.await.unwrap();
    assert_eq!(
        *keys.lock().unwrap(),
        vec!["https://factory.zed.native/openai-subscribed/v1"]
    );
    assert_eq!(
        state
            .read_with(cx, |state, _| state.native_identity())
            .unwrap_err(),
        openai_subscribed::native_auth::NativeAuthError::MissingSetup
    );
}

#[gpui::test]
async fn managed_interrupt_waits_for_cancelled_command_executor_before_checkpoint(
    cx: &mut TestAppContext,
) {
    let (_dir, resident, fake, model, executor) = native_fixture(cx).await;
    let mut app = cx.to_async();
    executor.wait.store(true, Ordering::SeqCst);
    let (prompt, _) = command(Operation::Prompt, "round", Some("test"));
    resident.prompt(&prompt, &mut app);
    cx.run_until_parked();
    let input = json!({"command":"fake long command"});
    fake.send_last_event(
        &model,
        LanguageModelCompletionEvent::ToolUse(LanguageModelToolUse {
            id: "pending-terminal".into(),
            name: "managed_terminal".into(),
            raw_input: input.to_string(),
            input: LanguageModelToolUseInput::Json(input),
            is_input_complete: true,
            thought_signature: None,
        }),
    );
    fake.send_last_event(
        &model,
        LanguageModelCompletionEvent::Stop(StopReason::ToolUse),
    );
    fake.end_last(&model);
    cx.run_until_parked();
    assert_eq!(executor.specs.lock().unwrap().len(), 2);
    assert_eq!(
        resident
            .managed
            .as_ref()
            .unwrap()
            .activity
            .lock()
            .unwrap()
            .active,
        2
    );
    let (interrupt, response) = command(Operation::Interrupt, "interrupt", None);
    resident.control(interrupt, &mut app).unwrap().await;
    assert_eq!(response.try_recv().unwrap()["result"]["drained"], true);
    assert_eq!(executor.cancelled.load(Ordering::SeqCst), 1);
    assert!(resident.managed.as_ref().unwrap().ready());
    let (checkpoint, response) = command(Operation::Checkpoint, "save", None);
    resident.control(checkpoint, &mut app).unwrap().await;
    assert_eq!(response.try_recv().unwrap()["result"]["saved"], true);
    let envelope: ResultEnvelope = serde_json::from_slice(
        &std::fs::read(&resident.managed.as_ref().unwrap().result_file).unwrap(),
    )
    .unwrap();
    assert_eq!(envelope.outcome, "interrupted");
}

#[gpui::test]
async fn managed_native_project_filesystem_cannot_read_skills_open_git_or_write_host(
    cx: &mut TestAppContext,
) {
    let (dir, _, managed, _) = configured();
    let fs = headless::managed_project_fs();
    assert!(!fs.is_fake());
    assert!(fs.metadata(&managed.workdir).await.unwrap().is_none());
    assert!(fs.load(&dir.0.join("brain/AGENTS.md")).await.is_err());
    assert!(fs.open_repo(&managed.workdir.join(".git"), None).is_err());
    assert!(
        fs.write(&dir.0.join("brain/AGENTS.md"), b"bad")
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read_to_string(dir.0.join("brain/AGENTS.md")).unwrap(),
        "knowledge"
    );
    cx.run_until_parked();
}

#[gpui::test]
async fn managed_loaded_session_pins_model_profile_speed_without_workspace_scanning(
    cx: &mut TestAppContext,
) {
    let (_dir, resident, fake, _, _) = native_fixture(cx).await;
    for case in 0..4 {
        let mut saved = resident
            .thread
            .read_with(cx, |thread, cx| thread.to_db(cx))
            .await;
        match case {
            1 => saved.speed = Some(Speed::Fast),
            2 => saved.profile.as_mut().unwrap().0 = "code".into(),
            3 => saved.model.as_mut().unwrap().model = "wrong-model".into(),
            _ => {}
        }
        let id = acp::SessionId::new(format!("managed-loaded-{case}"));
        let store = cx.new(ThreadStore::new);
        store
            .update(cx, |store, cx| {
                store.save_thread(id.clone(), saved, PathList::new(&[] as &[&Path]), cx)
            })
            .await
            .unwrap();
        let agent = cx.update(|cx| {
            let store = cx.new(ThreadStore::new);
            NativeAgent::new(store, Templates::new(), headless::managed_project_fs(), cx)
        });
        let connection = Rc::new(NativeAgentConnection(agent));
        let loaded = cx
            .update(|cx| {
                connection.clone().load_session(
                    id,
                    resident._project.clone(),
                    PathList::new(&[] as &[&Path]),
                    None,
                    cx,
                )
            })
            .await
            .unwrap();
        let checked = Resident::new_with_managed(
            connection,
            loaded,
            resident._project.clone(),
            "epoch-fixture".into(),
            resident.managed.clone(),
            &mut cx.to_async(),
        );
        assert_eq!(checked.is_ok(), case == 0, "case {case}");
    }
    assert_eq!(fake.completion_count(), 0);
}
#[gpui::test]
async fn managed_current_reply_excludes_loaded_history_and_previous_turns(cx: &mut TestAppContext) {
    let (_dir, resident, fake, model, _) = native_fixture(cx).await;
    let mut app = cx.to_async();
    for (id, answer) in [("first", "first answer"), ("second", "second answer")] {
        let (prompt, response) = command(Operation::Prompt, id, Some("user control text"));
        resident.prompt(&prompt, &mut app);
        assert!(response.try_recv().unwrap()["result"]["turn_id"].is_string());
        cx.run_until_parked();
        fake.send_last_text(&model, answer);
        fake.send_last_event(
            &model,
            LanguageModelCompletionEvent::Stop(StopReason::EndTurn),
        );
        fake.end_last(&model);
        cx.run_until_parked();
        assert_eq!(resident.status(&mut app)["reply"], answer);
    }
    assert_eq!(resident.status(&mut app)["turn_id"], "epoch-fixture:2");
}

#[gpui::test]
async fn managed_token_limit_is_failed_not_completed_and_is_checkpointed(cx: &mut TestAppContext) {
    let (_dir, resident, fake, model, _) = native_fixture(cx).await;
    let mut app = cx.to_async();
    let (prompt, _) = command(Operation::Prompt, "limit", Some("test"));
    resident.prompt(&prompt, &mut app);
    cx.run_until_parked();
    fake.send_last_text(&model, "partial answer");
    fake.send_last_event(
        &model,
        LanguageModelCompletionEvent::Stop(StopReason::MaxTokens),
    );
    fake.end_last(&model);
    cx.run_until_parked();
    assert_ne!(resident.status(&mut app)["last_turn_result"], "completed");
    let (checkpoint, response) = command(Operation::Checkpoint, "save", None);
    resident.control(checkpoint, &mut app).unwrap().await;
    assert_eq!(response.try_recv().unwrap()["result"]["saved"], true);
    let envelope: ResultEnvelope = serde_json::from_slice(
        &std::fs::read(&resident.managed.as_ref().unwrap().result_file).unwrap(),
    )
    .unwrap();
    assert_eq!(envelope.outcome, "failed");
    assert_eq!(envelope.error_code, "max_tokens");
}

#[test]
fn managed_model_cannot_replace_selected_host_executables() {
    let dir = FixtureDir::new();
    let mut args = dir.args();
    let unsafe_bin = dir.0.join("work/worker-controlled");
    std::fs::write(&unsafe_bin, "fixture, never execute").unwrap();
    std::fs::set_permissions(&unsafe_bin, std::fs::Permissions::from_mode(0o700)).unwrap();
    args.factory_bin = Some(unsafe_bin);
    assert!(Managed::prepare(&args, &dir.0.join("work"), "epoch".into()).is_err());
    assert!(!args.identity_file.as_ref().unwrap().exists());
    assert!(
        serde_json::from_value::<TerminalInput>(
            json!({"command":"id","executable":"/host/escape"})
        )
        .is_err()
    );
}

#[gpui::test]
async fn managed_actual_disk_failure_never_acknowledges_checkpoint_or_shutdown(
    cx: &mut TestAppContext,
) {
    let (dir, resident, fake, model, _) = native_fixture_database(cx, true).await;
    let mut app = cx.to_async();
    let quit = Rc::new(std::cell::Cell::new(false));
    resident.hooks.borrow_mut().quit = Some(quit.clone());
    let (prompt, _) = command(
        Operation::Prompt,
        "disk-failure",
        Some("offline fixture deliberately ignores failed startup preflight"),
    );
    resident.prompt(&prompt, &mut app);
    cx.run_until_parked();
    fake.send_last_text(&model, "completed model is not proof of durable save");
    fake.send_last_event(
        &model,
        LanguageModelCompletionEvent::Stop(StopReason::EndTurn),
    );
    fake.end_last(&model);
    cx.run_until_parked();
    for op in [Operation::Checkpoint, Operation::Shutdown] {
        let (request, response) = command(op, "save", None);
        resident.control(request, &mut app).unwrap().await;
        assert_eq!(
            response.try_recv().unwrap(),
            json!({"v":1,"id":"save","ok":false,"error":{"code":"save_failed"}})
        );
        assert!(!resident.managed.as_ref().unwrap().result_file.exists());
        assert!(!quit.get());
    }
    assert!(dir.0.join("data/threads/threads.db").is_dir());
}

#[gpui::test]
async fn managed_shutdown_before_first_prompt_exits_without_turn_result(cx: &mut TestAppContext) {
    let (_dir, resident, fake, _, _) = native_fixture(cx).await;
    fake.forbid_requests();
    let mut app = cx.to_async();
    let quit = Rc::new(std::cell::Cell::new(false));
    resident.hooks.borrow_mut().quit = Some(quit.clone());
    let session = resident.status(&mut app)["session_id"].clone();
    let (shutdown, response) = command(Operation::Shutdown, "quit", None);
    resident.control(shutdown, &mut app).unwrap().await;
    assert_eq!(
        response.try_recv().unwrap(),
        json!({"v":1,"id":"quit","ok":true,"result":{"saved":true,"drained":true,"conversation_id":session,"turn_id":"epoch-fixture:0","exiting":true}})
    );
    assert!(quit.get());
    assert_eq!(fake.completion_count(), 0);
    assert!(!resident.managed.as_ref().unwrap().result_file.exists());
    assert!(!resident.state.borrow().held);
}

#[gpui::test]
async fn managed_loaded_session_shutdown_without_new_prompt_does_not_publish_prior_turn(
    cx: &mut TestAppContext,
) {
    let (_old_dir, resident, fake, model, _) = native_fixture(cx).await;
    let mut app = cx.to_async();
    let (prompt, _) = command(
        Operation::Prompt,
        "prior",
        Some("persist prior conversation"),
    );
    resident.prompt(&prompt, &mut app);
    cx.run_until_parked();
    fake.send_last_text(&model, "prior reply must not be a new round result");
    fake.send_last_event(
        &model,
        LanguageModelCompletionEvent::Stop(StopReason::EndTurn),
    );
    fake.end_last(&model);
    cx.run_until_parked();
    let (checkpoint, response) = command(Operation::Checkpoint, "save", None);
    resident.control(checkpoint, &mut app).unwrap().await;
    assert_eq!(response.try_recv().unwrap()["result"]["saved"], true);
    let session = resident
        .acp
        .read_with(cx, |thread, _| thread.session_id().clone());
    let (_new_dir, _, managed, _) = configured();
    fake.forbid_requests();
    let agent = cx.update(|cx| {
        let store = cx.new(ThreadStore::new);
        NativeAgent::new(store, Templates::new(), headless::managed_project_fs(), cx)
    });
    let connection = Rc::new(NativeAgentConnection(agent));
    let acp = cx
        .update(|cx| {
            connection.clone().load_session(
                session.clone(),
                resident._project.clone(),
                PathList::new(&[] as &[&Path]),
                None,
                cx,
            )
        })
        .await
        .unwrap();
    let loaded = Resident::new_with_managed(
        connection,
        acp,
        resident._project.clone(),
        "epoch-fixture".into(),
        Some(managed),
        &mut app,
    )
    .unwrap();
    assert!(
        !loaded
            .acp
            .read_with(cx, |thread, _| thread.entries().is_empty())
    );
    assert_eq!(loaded.state.borrow().turn, 0);
    let quit = Rc::new(std::cell::Cell::new(false));
    loaded.hooks.borrow_mut().quit = Some(quit.clone());
    let (shutdown, response) = command(Operation::Shutdown, "quit", None);
    loaded.control(shutdown, &mut app).unwrap().await;
    assert_eq!(
        response.try_recv().unwrap(),
        json!({"v":1,"id":"quit","ok":true,"result":{"saved":true,"drained":true,"conversation_id":session.0,"turn_id":"epoch-fixture:0","exiting":true}})
    );
    assert!(quit.get());
    assert!(!loaded.managed.as_ref().unwrap().result_file.exists());
    assert_eq!(fake.completion_count(), 0);
}

#[gpui::test]
async fn managed_terminal_denies_expired_lease_without_model_precheck(cx: &mut TestAppContext) {
    let (_dir, resident, fake, model, executor) = native_fixture(cx).await;
    *executor.lease_reply.lock().unwrap() = Some(Ok(json!({"schema_version":1,"op":"lease_check","job_id":"job-fixture","generation":3,"allowed":false}).to_string()));
    let mut app = cx.to_async();
    let (prompt, _) = command(Operation::Prompt, "expired", Some("test"));
    resident.prompt(&prompt, &mut app);
    cx.run_until_parked();
    let input = json!({"command":"must not execute"});
    fake.send_last_event(
        &model,
        LanguageModelCompletionEvent::ToolUse(LanguageModelToolUse {
            id: "expired-terminal".into(),
            name: "managed_terminal".into(),
            raw_input: input.to_string(),
            input: LanguageModelToolUseInput::Json(input),
            is_input_complete: true,
            thought_signature: None,
        }),
    );
    fake.send_last_event(
        &model,
        LanguageModelCompletionEvent::Stop(StopReason::ToolUse),
    );
    fake.end_last(&model);
    cx.run_until_parked();
    let specs = executor.specs.lock().unwrap();
    assert_eq!(specs.len(), 1);
    assert_eq!(
        specs[0].executable,
        resident.managed.as_ref().unwrap().factory_bin
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&specs[0].stdin).unwrap(),
        json!({"op":"lease_check","payload":{}})
    );
    assert!(resident.managed.as_ref().unwrap().ready());
    drop(specs);
    fake.send_last_text(&model, "lease denied");
    fake.send_last_event(
        &model,
        LanguageModelCompletionEvent::Stop(StopReason::EndTurn),
    );
    fake.end_last(&model);
    cx.run_until_parked();
}

#[test]
fn managed_terminal_requires_exact_positive_bound_lease_receipt() {
    for response in [
        Ok("not json".into()),
        Ok(json!({"schema_version":1,"ok":true}).to_string()),
        Ok(json!({"schema_version":1,"op":"lease_check","job_id":"other","generation":3,"allowed":true}).to_string()),
        Ok(json!({"schema_version":1,"op":"lease_check","job_id":"job-fixture","generation":4,"allowed":true}).to_string()),
        Ok(json!({"schema_version":1,"op":"lease_check","job_id":"job-fixture","generation":3,"allowed":false}).to_string()),
        Ok(json!({"schema_version":1,"op":"lease_check","job_id":"job-fixture","generation":3,"allowed":true,"extra":true}).to_string()),
        Err(anyhow::anyhow!("fake helper failed")),
    ] {
        let (_dir, _, managed, executor) = configured();
        *executor.lease_reply.lock().unwrap() = Some(response);
        let spec = managed.terminal(TerminalInput {command:"never".into(),cd:None,timeout_ms:None}).unwrap();
        assert!(smol::block_on(managed.execute_terminal(spec, Arc::new(Cancellation::default()))).is_err());
        let specs = executor.specs.lock().unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].executable, managed.factory_bin);
        assert_eq!(specs[0].args, vec!["helper","control","--job","job-fixture","--generation","3","--brain",managed.brain.to_str().unwrap(),"--request","-"]);
        assert!(managed.ready());
    }
}

#[test]
fn managed_terminal_pre_cancelled_pipeline_never_starts_a_child() {
    let (_dir, _, managed, executor) = configured();
    let spec = managed
        .terminal(TerminalInput {
            command: "never".into(),
            cd: None,
            timeout_ms: None,
        })
        .unwrap();
    let cancel = Arc::new(Cancellation::default());
    cancel.cancel();
    let future = managed.execute_terminal(spec, cancel);
    assert_eq!(managed.activity.lock().unwrap().active, 1);
    assert!(smol::block_on(future).is_err());
    assert!(executor.specs.lock().unwrap().is_empty());
    assert!(managed.ready());
}

#[test]
fn managed_terminal_cancelled_after_preflight_never_starts_docker() {
    let (_dir, _, managed, executor) = configured();
    executor.cancel_after_lease.store(true, Ordering::SeqCst);
    let spec = managed
        .terminal(TerminalInput {
            command: "never".into(),
            cd: None,
            timeout_ms: None,
        })
        .unwrap();
    let error = smol::block_on(managed.execute_terminal(spec, Arc::new(Cancellation::default())))
        .unwrap_err();
    assert_eq!(error.to_string(), "managed command interrupted");
    let specs = executor.specs.lock().unwrap();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].executable, managed.factory_bin);
    assert!(managed.ready());
}

#[test]
fn managed_terminal_checks_lease_for_every_execution() {
    let (_dir, _, managed, executor) = configured();
    for _ in 0..2 {
        let spec = managed
            .terminal(TerminalInput {
                command: "fake".into(),
                cd: None,
                timeout_ms: None,
            })
            .unwrap();
        smol::block_on(managed.execute_terminal(spec, Arc::new(Cancellation::default()))).unwrap();
        assert!(managed.ready());
    }
    let specs = executor.specs.lock().unwrap();
    assert_eq!(specs.len(), 4);
    for pair in specs.chunks_exact(2) {
        assert_eq!(pair[0].executable, managed.factory_bin);
        assert_eq!(
            serde_json::from_slice::<Value>(&pair[0].stdin).unwrap(),
            json!({"op":"lease_check","payload":{}})
        );
        assert_eq!(pair[1].executable, managed.docker_bin);
    }
}

struct PendingFakeCredentialWrite {
    pending: Arc<AtomicBool>,
    release: std::sync::Mutex<Option<oneshot::Receiver<()>>>,
}
impl credentials_provider::CredentialsProvider for PendingFakeCredentialWrite {
    fn read_credentials<'a>(
        &'a self,
        _: &'a str,
        _: &'a AsyncApp,
    ) -> futures::future::LocalBoxFuture<'a, Result<Option<(String, Vec<u8>)>>> {
        async { panic!("pending-write fixture does not read credentials") }.boxed_local()
    }
    fn write_credentials<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
        _: &'a [u8],
        _: &'a AsyncApp,
    ) -> futures::future::LocalBoxFuture<'a, Result<()>> {
        let release = self.release.lock().unwrap().take().unwrap();
        let pending = self.pending.clone();
        async move {
            pending.store(true, Ordering::SeqCst);
            release.await?;
            pending.store(false, Ordering::SeqCst);
            Ok(())
        }
        .boxed_local()
    }
    fn delete_credentials<'a>(
        &'a self,
        _: &'a str,
        _: &'a AsyncApp,
    ) -> futures::future::LocalBoxFuture<'a, Result<()>> {
        async { panic!("pending-write fixture does not delete credentials") }.boxed_local()
    }
}
fn pending_fake_credential_write(
    resident: &Resident,
    cx: &mut TestAppContext,
) -> (oneshot::Sender<()>, Task<Result<()>>) {
    use credentials_provider::CredentialsProvider as _;
    let pending = Arc::new(AtomicBool::new(false));
    let (release, receiver) = oneshot::channel();
    // Only the credential barrier is injected. Native sessions, saves and controls are real.
    resident.hooks.borrow_mut().credential_mutation_pending = Some(pending.clone());
    let provider = PendingFakeCredentialWrite {
        pending,
        release: std::sync::Mutex::new(Some(receiver)),
    };
    let write = cx.to_async().spawn(async move |cx| {
        provider
            .write_credentials("offline", "offline", b"offline", cx)
            .await
    });
    cx.run_until_parked();
    (release, write)
}

#[gpui::test]
async fn managed_pending_credential_write_blocks_result_then_recovers_shutdown(
    cx: &mut TestAppContext,
) {
    let (_dir, resident, fake, model, _) = native_fixture(cx).await;
    let mut app = cx.to_async();
    let quit = Rc::new(std::cell::Cell::new(false));
    resident.hooks.borrow_mut().quit = Some(quit.clone());
    let (prompt, _) = command(Operation::Prompt, "round", Some("test"));
    resident.prompt(&prompt, &mut app);
    cx.run_until_parked();
    fake.send_last_text(&model, "settled answer");
    fake.send_last_event(
        &model,
        LanguageModelCompletionEvent::Stop(StopReason::EndTurn),
    );
    fake.end_last(&model);
    cx.run_until_parked();
    let (release, write) = pending_fake_credential_write(&resident, cx);
    let status = resident.status(&mut app);
    assert_eq!(status["credential_mutation_pending"], true);
    assert_eq!(status["state"], "held");
    for op in [Operation::Checkpoint, Operation::Shutdown] {
        let (control, response) = command(op, "pending", None);
        if let Some(task) = resident.control(control, &mut app) {
            task.await;
        }
        assert_eq!(
            response.try_recv().unwrap(),
            json!({"v":1,"id":"pending","ok":false,"error":{"code":"credential_mutation_pending"}})
        );
        assert!(!resident.managed.as_ref().unwrap().result_file.exists());
        assert!(!quit.get());
    }
    let (prompt, response) = command(Operation::Prompt, "not-admitted", Some("test"));
    resident.prompt(&prompt, &mut app);
    assert_eq!(
        response.try_recv().unwrap()["error"]["code"],
        "credential_mutation_pending"
    );
    release.send(()).unwrap();
    write.await.unwrap();
    let status = resident.status(&mut app);
    assert_eq!(status["credential_mutation_pending"], false);
    assert_eq!(status["state"], "idle");
    assert!(!resident.state.borrow().held);
    let (checkpoint, response) = command(Operation::Checkpoint, "settled", None);
    resident.control(checkpoint, &mut app).unwrap().await;
    assert_eq!(response.try_recv().unwrap()["result"]["saved"], true);
    let (shutdown, response) = command(Operation::Shutdown, "quit", None);
    resident.control(shutdown, &mut app).unwrap().await;
    assert_eq!(response.try_recv().unwrap()["result"]["exiting"], true);
    assert!(quit.get());
}

#[gpui::test]
async fn managed_no_turn_pending_credential_shutdown_is_retryable(cx: &mut TestAppContext) {
    let (_dir, resident, _, _, _) = native_fixture(cx).await;
    let mut app = cx.to_async();
    let quit = Rc::new(std::cell::Cell::new(false));
    resident.hooks.borrow_mut().quit = Some(quit.clone());
    let (release, write) = pending_fake_credential_write(&resident, cx);
    let (shutdown, response) = command(Operation::Shutdown, "pending", None);
    if let Some(task) = resident.control(shutdown, &mut app) {
        task.await;
    }
    assert_eq!(
        response.try_recv().unwrap()["error"]["code"],
        "credential_mutation_pending"
    );
    assert!(!quit.get());
    release.send(()).unwrap();
    write.await.unwrap();
    let conversation = resident.status(&mut app)["session_id"].clone();
    let (shutdown, response) = command(Operation::Shutdown, "settled", None);
    resident.control(shutdown, &mut app).unwrap().await;
    assert_eq!(
        response.try_recv().unwrap(),
        json!({"v":1,"id":"settled","ok":true,"result":{"saved":true,"drained":true,"conversation_id":conversation,"turn_id":"epoch-fixture:0","exiting":true}})
    );
    assert!(quit.get());
    assert!(!resident.managed.as_ref().unwrap().result_file.exists());
}

#[gpui::test]
async fn managed_credential_mutation_during_flush_blocks_shutdown_publication(
    cx: &mut TestAppContext,
) {
    let (_dir, resident, fake, model, _) = native_fixture(cx).await;
    let mut app = cx.to_async();
    let quit = Rc::new(std::cell::Cell::new(false));
    resident.hooks.borrow_mut().quit = Some(quit.clone());
    let (prompt, _) = command(Operation::Prompt, "round", Some("test"));
    resident.prompt(&prompt, &mut app);
    cx.run_until_parked();
    fake.send_last_text(&model, "answer");
    fake.send_last_event(
        &model,
        LanguageModelCompletionEvent::Stop(StopReason::EndTurn),
    );
    fake.end_last(&model);
    cx.run_until_parked();
    // Install the shared pending indicator before the control captures its barrier.
    let pending = Arc::new(AtomicBool::new(false));
    resident.hooks.borrow_mut().credential_mutation_pending = Some(pending.clone());
    let (save_release, save_wait) = oneshot::channel();
    resident.hooks.borrow_mut().save = Some(app.spawn(async move |_| {
        save_wait.await?;
        Ok(())
    }));
    let (shutdown, response) = command(Operation::Shutdown, "during-save", None);
    let shutdown = resident.control(shutdown, &mut app).unwrap();
    cx.run_until_parked();
    pending.store(true, Ordering::SeqCst);
    save_release.send(()).unwrap();
    shutdown.await;
    assert_eq!(
        response.try_recv().unwrap()["error"]["code"],
        "credential_mutation_pending"
    );
    assert!(!quit.get());
    assert!(!resident.managed.as_ref().unwrap().result_file.exists());
    assert!(!resident.state.borrow().held);
    assert!(!resident.state.borrow().active_control);
    pending.store(false, Ordering::SeqCst);
    let (shutdown, response) = command(Operation::Shutdown, "retry", None);
    resident.control(shutdown, &mut app).unwrap().await;
    assert_eq!(response.try_recv().unwrap()["result"]["saved"], true);
    assert!(quit.get());
}

#[gpui::test]
async fn managed_shutdown_retains_runtime_if_mutation_starts_during_response_delivery(
    cx: &mut TestAppContext,
) {
    let (_dir, resident, _, _, _) = native_fixture(cx).await;
    let mut app = cx.to_async();
    let quit = Rc::new(std::cell::Cell::new(false));
    let pending = Arc::new(AtomicBool::new(false));
    resident.hooks.borrow_mut().quit = Some(quit.clone());
    resident.hooks.borrow_mut().credential_mutation_pending = Some(pending.clone());
    let (mut shutdown, response) = command(Operation::Shutdown, "delayed-delivery", None);
    let (delivered, receipt) = oneshot::channel();
    shutdown.delivered = receipt;
    let task = resident.control(shutdown, &mut app).unwrap();
    cx.run_until_parked();
    assert_eq!(response.try_recv().unwrap()["result"]["exiting"], true);
    pending.store(true, Ordering::SeqCst);
    delivered.send(true).unwrap();
    task.await;
    assert!(!quit.get());
    let status = resident.status(&mut app);
    assert_eq!(status["credential_mutation_pending"], true);
    assert_eq!(status["state"], "held");
    assert_eq!(status["last_control_error"], "credential_mutation_pending");
    pending.store(false, Ordering::SeqCst);
    let (shutdown, response) = command(Operation::Shutdown, "retry", None);
    resident.control(shutdown, &mut app).unwrap().await;
    assert_eq!(response.try_recv().unwrap()["result"]["exiting"], true);
    assert!(quit.get());
}
