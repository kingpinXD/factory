use super::*;
use gpui::TestAppContext;
use language_model::{
    LanguageModel, LanguageModelProviderId, LanguageModelProviderName,
    fake_provider::FakeLanguageModelProvider,
};

async fn fixture(
    cx: &mut TestAppContext,
) -> (Resident, Arc<FakeLanguageModelProvider>, LanguageModel) {
    // No headless initialization: install the fake before any real provider can be constructed.
    let provider = Arc::new(FakeLanguageModelProvider::new(
        LanguageModelProviderId::from(PROVIDER.to_string()),
        LanguageModelProviderName::from("Offline fixture".to_string()),
    ));
    let model = provider.model(MODEL);
    cx.update(|cx| {
        let store = SettingsStore::test(cx);
        cx.set_global(store);
        language_model::init(cx);
        LanguageModelRegistry::global(cx).update(cx, |registry, cx| {
            registry.register_provider(provider.clone(), cx);
            registry.set_default_model(Some(model.clone()), cx);
        });
        SettingsStore::update_global(cx, |store, cx| {
            store
                .set_user_settings(&policy_settings().to_string(), cx)
                .result()
        })
        .unwrap();
    });
    let fs = fs::FakeFs::new(cx.executor());
    fs.insert_tree(Path::new("/lifecycle"), json!({})).await;
    cx.update(|cx| <dyn fs::Fs>::set_global(fs.clone(), cx));
    let project = Project::test(fs.clone(), [Path::new("/lifecycle")], cx).await;
    let agent = cx.update(|cx| {
        let store = cx.new(ThreadStore::new);
        NativeAgent::new(store, Templates::new(), fs, cx)
    });
    let connection = Rc::new(NativeAgentConnection(agent.clone()));
    let acp = cx
        .update(|cx| {
            connection.clone().new_session(
                project.clone(),
                PathList::new(&[Path::new("/lifecycle")]),
                cx,
            )
        })
        .await
        .unwrap();
    let resident = Resident::new(
        connection,
        acp,
        project,
        "fixture-epoch".into(),
        &mut cx.to_async(),
    )
    .unwrap();
    (resident, provider, model)
}

fn command(op: Operation, id: &str) -> (Command, sync_mpsc::Receiver<Value>) {
    let (reply, response) = sync_mpsc::sync_channel(1);
    let (_, delivered) = oneshot::channel();
    (
        Command {
            request: Request {
                v: 1,
                id: id.into(),
                op,
                text: if op == Operation::Prompt {
                    Some("Remember offline nonce".into())
                } else {
                    None
                },
            },
            reply,
            delivered,
        },
        response,
    )
}

#[gpui::test]
async fn native_pending_status_disconnect_interrupt_and_checkpoint(cx: &mut TestAppContext) {
    let (resident, provider, model) = fixture(cx).await;
    let mut app = cx.to_async();
    assert!(
        resident
            .thread
            .read_with(cx, |thread, _| check_native(thread))
            .is_ok(),
        "{}",
        resident.status(&mut app)
    );
    let (prompt, response) = command(Operation::Prompt, "p1");
    resident.prompt(&prompt, &mut app);
    assert_eq!(
        response.try_recv().unwrap(),
        json!({"v":1,"id":"p1","ok":true,"result":{"admitted":true,"turn_id":1}})
    );
    drop((prompt, response));
    cx.run_until_parked();
    assert_eq!(provider.completion_count(), 1);
    assert!(provider.pending_completions()[0].tools.is_empty());
    let status = resident.status(&mut app);
    let pending_sequence = status["sequence"].as_u64().unwrap();
    assert_eq!(status["state"], "running");
    assert_eq!(status["model"], "openai-subscribed/gpt-6.1-sol");
    assert_eq!(status["profile"], "lifecycle-proof");
    assert_eq!(status["speed"], "standard");
    let (busy, response) = command(Operation::Prompt, "p2");
    resident.prompt(&busy, &mut app);
    assert_eq!(response.try_recv().unwrap()["error"]["code"], "busy");
    provider.send_last_text(&model, "offline partial nonce");
    cx.run_until_parked();
    assert!(
        resident.status(&mut app)["transcript"]
            .as_str()
            .unwrap()
            .contains("offline partial nonce")
    );
    assert!(resident.status(&mut app)["sequence"].as_u64().unwrap() > pending_sequence);
    let (interrupt, response) = command(Operation::Interrupt, "i1");
    resident.control(interrupt, &mut app).unwrap().await;
    assert_eq!(response.try_recv().unwrap()["result"]["drained"], true);
    let status = resident.status(&mut app);
    assert_eq!(status["state"], "idle");
    assert_eq!(status["last_turn_result"], "interrupted");
    let (checkpoint, response) = command(Operation::Checkpoint, "c1");
    resident.control(checkpoint, &mut app).unwrap().await;
    assert_eq!(response.try_recv().unwrap()["result"]["saved"], true);
    let (retry, response) = command(Operation::Prompt, "p1");
    resident.prompt(&retry, &mut app);
    assert_eq!(response.try_recv().unwrap()["result"]["turn_id"], 1);
    assert_eq!(resident.status(&mut app)["turn_id"], 1);
}

#[gpui::test]
async fn effective_native_speed_is_checked_not_just_settings(cx: &mut TestAppContext) {
    let (resident, provider, _) = fixture(cx).await;
    resident
        .thread
        .update(cx, |thread, cx| thread.set_speed(Speed::Fast, cx));
    let mut app = cx.to_async();
    assert_eq!(resident.status(&mut app)["speed"], "fast");
    let (prompt, response) = command(Operation::Prompt, "p1");
    resident.prompt(&prompt, &mut app);
    assert_eq!(
        response.try_recv().unwrap()["error"]["code"],
        "session_policy_mismatch"
    );
    assert_eq!(provider.completion_count(), 0);
    assert_eq!(resident.status(&mut app)["turn_id"], 0);
}

#[test]
fn queue_is_bounded_and_rejects_without_admitting() {
    let (mut sender, _receiver) = mpsc::channel(QUEUE_LIMIT);
    let mut admitted = 0;
    for _ in 0..QUEUE_LIMIT + 2 {
        let (command, _) = command(Operation::Status, "s");
        if sender.try_send(command).is_ok() {
            admitted += 1;
        }
    }
    assert_eq!(admitted, QUEUE_LIMIT + 1); // futures reserves one slot for the sole sender.
}

#[test]
fn active_control_retry_is_busy_and_freezes_prompts() {
    let mut state = Control::default();
    state.active_control = true;
    let (checkpoint, _) = command(Operation::Checkpoint, "c");
    assert_eq!(
        state.check_control(&checkpoint.request).unwrap_err(),
        "busy"
    );
    let (prompt, _) = command(Operation::Prompt, "p");
    assert_eq!(state.admit(&prompt.request).unwrap_err(), "busy");
    state.frozen = true;
    assert_eq!(
        state.admit(&prompt.request).unwrap_err(),
        "admission_closed"
    );
}

#[gpui::test]
async fn shutdown_freezes_admission_then_saves_then_acknowledges_before_quit(
    cx: &mut TestAppContext,
) {
    let (resident, provider, _) = fixture(cx).await;
    let quit = Rc::new(std::cell::Cell::new(false));
    let (save_sender, save_receiver) = oneshot::channel();
    resident.hooks.borrow_mut().save = Some(cx.spawn(async move |_| save_receiver.await.unwrap()));
    resident.hooks.borrow_mut().quit = Some(quit.clone());
    let mut app = cx.to_async();
    let (mut shutdown, response) = command(Operation::Shutdown, "q");
    let (delivered, receipt) = oneshot::channel();
    shutdown.delivered = receipt;
    let task = resident.control(shutdown, &mut app).unwrap();
    cx.run_until_parked();
    assert!(!quit.get());
    assert!(response.try_recv().is_err());
    let (prompt, rejected) = command(Operation::Prompt, "p");
    resident.prompt(&prompt, &mut app);
    assert_eq!(
        rejected.try_recv().unwrap()["error"]["code"],
        "admission_closed"
    );
    assert_eq!(resident.status(&mut app)["state"], "shutting_down");
    save_sender.send(Ok(())).unwrap();
    cx.run_until_parked();
    assert_eq!(
        response.try_recv().unwrap(),
        json!({"v":1,"id":"q","ok":true,"result":{"drained":true,"saved":true}})
    );
    assert!(!quit.get());
    delivered.send(true).unwrap();
    task.await;
    assert!(quit.get());
    assert_eq!(provider.completion_count(), 0);
}

#[gpui::test]
async fn checkpoint_and_shutdown_never_acknowledge_failed_save(cx: &mut TestAppContext) {
    let (resident, _, _) = fixture(cx).await;
    let quit = Rc::new(std::cell::Cell::new(false));
    resident.hooks.borrow_mut().quit = Some(quit.clone());
    let mut app = cx.to_async();
    for op in [Operation::Checkpoint, Operation::Shutdown] {
        resident.hooks.borrow_mut().save = Some(Task::ready(Err(anyhow::anyhow!(
            "private injected write failure"
        ))));
        let (request, response) = command(op, "save");
        resident.control(request, &mut app).unwrap().await;
        assert_eq!(
            response.try_recv().unwrap(),
            json!({"v":1,"id":"save","ok":false,"error":{"code":"save_failed"}})
        );
        assert!(!quit.get());
    }
    assert_eq!(resident.status(&mut app)["state"], "shutting_down");
}

#[gpui::test]
async fn unknown_cancel_holds_session_and_never_allows_shutdown_success(cx: &mut TestAppContext) {
    let (resident, _, _) = fixture(cx).await;
    let (release, wait) = oneshot::channel();
    resident.hooks.borrow_mut().cancel = Some(cx.spawn(async move |_| wait.await.unwrap()));
    let mut app = cx.to_async();
    let (interrupt, response) = command(Operation::Interrupt, "i");
    let task = resident.control(interrupt, &mut app).unwrap();
    cx.run_until_parked();
    cx.executor().advance_clock(CONTROL_TIMEOUT);
    cx.run_until_parked();
    assert_eq!(
        response.try_recv().unwrap()["error"]["code"],
        "termination_unknown"
    );
    assert_eq!(resident.status(&mut app)["state"], "held");
    let (shutdown, response) = command(Operation::Shutdown, "q");
    assert!(resident.control(shutdown, &mut app).is_none());
    assert_eq!(
        response.try_recv().unwrap()["error"]["code"],
        "termination_unknown"
    );
    release.send(()).unwrap();
    task.await;
    assert_eq!(resident.status(&mut app)["state"], "held");
}

#[test]
fn socket_transport_rejects_bad_frames_and_survives_a_disconnected_waiter() {
    use std::io::{BufRead as _, Write as _};
    let (server, mut client) = UnixStream::pair().unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let (sender, mut receiver) = mpsc::channel(QUEUE_LIMIT);
    let worker = std::thread::spawn(move || serve_client(server, Arc::new(Mutex::new(sender))));
    client.write_all(b"{broken}\n").unwrap();
    let mut reader = BufReader::new(client.try_clone().unwrap());
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&line).unwrap()["error"]["code"],
        "invalid_request"
    );
    client
        .write_all(b"{\"v\":1,\"id\":\"s\",\"op\":\"status\"}\n")
        .unwrap();
    let request = futures::executor::block_on(receiver.next()).unwrap();
    drop((client, reader));
    reply(&request, success("s", json!({"state":"running"})));
    assert!(!futures::executor::block_on(request.delivered).unwrap());
    assert!(worker.join().unwrap().is_err());
}

fn accepted_socket_status_exchange(drain: bool) {
    use std::io::{Read as _, Write as _};
    use std::os::fd::AsRawFd as _;
    use std::time::Instant;

    let directory = Path::new("/tmp").join(format!(
        "nlp-transport-{}-{drain}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    private_directory(&directory).unwrap();
    let socket = directory.join("s");
    let listener = bind_private_socket(&socket).unwrap();
    let mut client = UnixStream::connect(&socket).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    client
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    // Queue all requests before accept so an inherited nonblocking read cannot mask the write bug.
    client.write_all(b"{\"v\":1,\"id\":\"small\",\"op\":\"status\"}\n{\"v\":1,\"id\":\"large\",\"op\":\"status\"}\n{\"v\":1,\"id\":\"last\",\"op\":\"status\"}\n").unwrap();
    client.shutdown(std::net::Shutdown::Write).unwrap();
    let (server, _) = listener.accept().unwrap();
    drop(listener);
    std::fs::remove_file(&socket).unwrap();
    std::fs::remove_dir(&directory).unwrap();
    #[cfg(target_os = "macos")]
    {
        let flags = unsafe { libc::fcntl(server.as_raw_fd(), libc::F_GETFL) };
        assert!(flags >= 0);
        assert_ne!(
            flags & libc::O_NONBLOCK,
            0,
            "Darwin inherits the listener mode"
        );
    }
    // Exercise the same entry condition on platforms that do not inherit listener flags.
    server.set_nonblocking(true).unwrap();
    let mut send_buffer: libc::c_int = 4096;
    let mut option_size = std::mem::size_of_val(&send_buffer) as libc::socklen_t;
    assert_eq!(
        unsafe {
            libc::setsockopt(
                server.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                (&send_buffer as *const libc::c_int).cast(),
                option_size,
            )
        },
        0
    );
    assert_eq!(
        unsafe {
            libc::getsockopt(
                server.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                (&mut send_buffer as *mut libc::c_int).cast(),
                &mut option_size,
            )
        },
        0
    );
    let expected: Vec<Value> = [("small", 128), ("large", 96 * 1024), ("last", 256)]
        .into_iter()
        .map(|(id, size)| {
            json!({"v":1,"id":id,"ok":true,"result":{
            "state":"running","transcript":"x".repeat(size),"reply":""}})
        })
        .collect();
    let large_size = serde_json::to_vec(&expected[1]).unwrap().len();
    assert!(send_buffer > 0 && large_size > 4 * send_buffer as usize);
    assert!(large_size < MAX_RESPONSE);
    let responses = expected.clone();
    let (sender, mut receiver) = mpsc::channel(QUEUE_LIMIT);
    let (finished, completion) = sync_mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        finished
            .send(serve_client(server, Arc::new(Mutex::new(sender))))
            .unwrap();
    });
    let (ready, writing_large) = sync_mpsc::sync_channel(1);
    let (receipts, delivered) = sync_mpsc::sync_channel(1);
    let responder = std::thread::spawn(move || {
        let mut outcomes = Vec::new();
        for response in responses {
            let command = futures::executor::block_on(async {
                futures::select! {
                    command = receiver.next().fuse() => command.expect("status command"),
                    _ = futures::FutureExt::fuse(smol::Timer::after(Duration::from_secs(5))) => panic!("command timeout"),
                }
            });
            assert_eq!(
                command.request,
                Request {
                    v: 1,
                    id: response["id"].as_str().unwrap().into(),
                    op: Operation::Status,
                    text: None,
                }
            );
            if command.request.id == "large" {
                ready.send(Instant::now()).unwrap();
            }
            reply(&command, response);
            let outcome = futures::executor::block_on(async {
                futures::select! {
                    receipt = command.delivered.fuse() => receipt.unwrap(),
                    _ = futures::FutureExt::fuse(smol::Timer::after(Duration::from_secs(5))) => panic!("delivery timeout"),
                }
            });
            outcomes.push(outcome);
            if !outcome {
                break;
            }
        }
        receipts.send(outcomes).unwrap();
    });
    let started = writing_large.recv_timeout(Duration::from_secs(5)).unwrap();
    // Withhold every read while the response exceeds the send buffer. This interval tests
    // backpressure, rather than hoping the reader happens to lose a scheduling race.
    let early = completion.recv_timeout(if drain {
        Duration::from_millis(100)
    } else {
        Duration::from_secs(5)
    });
    let elapsed = started.elapsed();
    let mut bytes = Vec::new();
    let read = client.read_to_end(&mut bytes);
    let result = match early {
        Ok(result) => result,
        Err(sync_mpsc::RecvTimeoutError::Timeout) => {
            completion.recv_timeout(Duration::from_secs(5)).unwrap()
        }
        Err(error) => panic!("handler completion: {error}"),
    };
    let outcomes = delivered.recv_timeout(Duration::from_secs(5)).unwrap();
    worker.join().unwrap();
    responder.join().unwrap();
    read.unwrap();
    eprintln!(
        "drain={drain} send_buffer={send_buffer} large_size={large_size} received={} elapsed={elapsed:?} result={result:?} delivered={outcomes:?}",
        bytes.len()
    );
    if drain {
        assert!(
            bytes.ends_with(b"\n"),
            "truncated JSONL frame: {} bytes; {result:?}",
            bytes.len()
        );
        let actual: Vec<Value> = bytes
            .split_inclusive(|byte| *byte == b'\n')
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        assert_eq!(actual, expected);
        assert_eq!(outcomes, [true, true, true]);
        result.unwrap();
    } else {
        let error = result.unwrap_err();
        let error = error.downcast_ref::<std::io::Error>().unwrap();
        assert!(
            matches!(
                error.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ),
            "{error}"
        );
        assert!(
            elapsed >= Duration::from_millis(1500) && elapsed < Duration::from_secs(5),
            "write timeout: {elapsed:?}"
        );
        assert_eq!(outcomes, [true, false]);
        assert!(!bytes.is_empty() && bytes.len() < large_size);
        assert!(!bytes.ends_with(b"\n"));
    }
}

#[test]
fn accepted_socket_status_frames_survive_backpressure() {
    accepted_socket_status_exchange(true);
}

#[test]
fn accepted_socket_non_draining_client_times_out() {
    accepted_socket_status_exchange(false);
}

#[test]
fn submission_outcomes_are_not_false_success() {
    assert_eq!(turn_result(Ok(None)), "no_response");
    assert_eq!(
        turn_result(Err(anyhow::anyhow!("private upstream error"))),
        "turn_failed"
    );
    for (reason, expected) in [
        (acp::StopReason::EndTurn, "completed"),
        (acp::StopReason::Cancelled, "interrupted"),
        (acp::StopReason::MaxTokens, "max_tokens"),
        (acp::StopReason::Refusal, "refused_or_incomplete"),
    ] {
        assert_eq!(
            turn_result(Ok(Some(SubmissionResponse::LegacyCompleted(
                acp::PromptResponse::new(reason)
            )))),
            expected
        );
    }
}

#[gpui::test]
async fn loaded_native_session_rejects_wrong_model_profile_and_fast_speed(cx: &mut TestAppContext) {
    let (resident, provider, _) = fixture(cx).await;
    for index in 0..5 {
        let mut saved = resident
            .thread
            .read_with(cx, |thread, cx| thread.to_db(cx))
            .await;
        match index {
            1 => saved.model.as_mut().unwrap().model = "unavailable-model".into(),
            2 => saved.profile.as_mut().unwrap().0 = "other-profile".into(),
            3 => saved.speed = Some(Speed::Fast),
            4 => saved.model.as_mut().unwrap().provider = "other-provider".into(),
            _ => {}
        }
        let id = acp::SessionId::new(format!("loaded-case-{index}"));
        let store = cx.new(ThreadStore::new);
        store
            .update(cx, |store, cx| {
                store.save_thread(
                    id.clone(),
                    saved,
                    PathList::new(&[Path::new("/lifecycle")]),
                    cx,
                )
            })
            .await
            .unwrap();
        let fs = cx.update(|cx| <dyn fs::Fs>::global(cx));
        let agent = cx.update(|cx| {
            let store = cx.new(ThreadStore::new);
            NativeAgent::new(store, Templates::new(), fs, cx)
        });
        let connection = Rc::new(NativeAgentConnection(agent));
        let loaded = cx
            .update(|cx| {
                connection.clone().load_session(
                    id.clone(),
                    resident._project.clone(),
                    PathList::new(&[Path::new("/lifecycle")]),
                    None,
                    cx,
                )
            })
            .await
            .unwrap();
        let thread = cx
            .update(|cx| connection.thread(loaded.read(cx).session_id(), cx))
            .unwrap();
        let checked = thread.read_with(cx, |thread, _| check_native(thread));
        assert_eq!(checked.is_ok(), index == 0, "case {index}: {checked:?}");
        if index == 3 {
            assert_eq!(
                thread.read_with(cx, |thread, _| thread.speed()),
                Some(Speed::Fast)
            );
        }
    }
    assert_eq!(provider.completion_count(), 0);
}

#[test]
fn private_socket_permissions_and_no_takeover() {
    let directory = Path::new("/tmp").join(format!(
        "nlp-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    private_directory(&directory).unwrap();
    let socket = directory.join("s");
    let listener = bind_private_socket(&socket).unwrap();
    assert_eq!(std::fs::metadata(&directory).unwrap().mode() & 0o777, 0o700);
    assert_eq!(std::fs::metadata(&socket).unwrap().mode() & 0o777, 0o600);
    assert!(bind_private_socket(&socket).is_err());
    drop(listener);
    assert!(bind_private_socket(&socket).is_err());
    std::fs::remove_file(&socket).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(private_directory(&directory).is_err());
    std::fs::remove_dir(&directory).unwrap();
}

#[gpui::test]
async fn completed_native_turns_share_the_same_conversation(cx: &mut TestAppContext) {
    let (resident, provider, model) = fixture(cx).await;
    let mut app = cx.to_async();
    let session = resident.status(&mut app)["session_id"].clone();
    for index in 1..=2 {
        let (prompt, response) = command(Operation::Prompt, &format!("p{index}"));
        resident.prompt(&prompt, &mut app);
        assert_eq!(response.try_recv().unwrap()["result"]["turn_id"], index);
        cx.run_until_parked();
        if index == 2 {
            let request = &provider.pending_completions()[0];
            assert!(format!("{:?}", request.messages).contains("retained offline answer"));
            assert!(request.tools.is_empty());
        }
        provider.send_last_text(&model, "retained offline answer");
        provider.send_last_event(
            &model,
            language_model::LanguageModelCompletionEvent::Stop(language_model::StopReason::EndTurn),
        );
        provider.end_last(&model);
        cx.run_until_parked();
        let status = resident.status(&mut app);
        assert_eq!(status["state"], "idle");
        assert_eq!(status["last_turn_result"], "completed");
        assert_eq!(status["session_id"], session);
    }
}

#[gpui::test]
async fn native_subagent_event_holds_session_and_refuses_further_prompts(cx: &mut TestAppContext) {
    let (resident, provider, _) = fixture(cx).await;
    resident.acp.update(cx, |_, cx| {
        cx.emit(AcpThreadEvent::SubagentSpawned(acp::SessionId::new(
            "forbidden-child",
        )))
    });
    cx.run_until_parked();
    let mut app = cx.to_async();
    let status = resident.status(&mut app);
    assert_eq!(status["state"], "held");
    assert_eq!(status["policy_violation"], true);
    let (prompt, response) = command(Operation::Prompt, "p");
    resident.prompt(&prompt, &mut app);
    assert_eq!(
        response.try_recv().unwrap()["error"]["code"],
        "admission_closed"
    );
    assert_eq!(provider.completion_count(), 0);
}

#[test]
fn required_flags_parse_and_help_does_not_initialize_native_runtime() {
    assert!(Args::try_parse_from(["probe"]).is_err());
    let Err(help) = Args::try_parse_from(["probe", "--help"]) else {
        panic!("expected help")
    };
    assert_eq!(help.kind(), clap::error::ErrorKind::DisplayHelp);
    let args = Args::try_parse_from([
        "probe",
        "--user-data-dir",
        "/isolated",
        "--workdir",
        "/workspace",
        "--socket",
        "/private/s",
        "--load-session",
        "saved-id",
    ])
    .unwrap();
    assert_eq!(args.load_session.as_deref(), Some("saved-id"));
}

async fn production_project_with_workspace_mcp(
    servers: Value,
    trap_factory: bool,
    cx: &mut TestAppContext,
) -> (Entity<Project>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    use settings::Settings as _;
    let http_calls = Arc::new(AtomicUsize::new(0));
    let http = http_client::FakeHttpClient::create({
        let calls = http_calls.clone();
        move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            async {
                Err(anyhow::anyhow!(
                    "unexpected offline context-server transport"
                ))
            }
        }
    });
    cx.update(|cx| {
        let settings = SettingsStore::test(cx);
        cx.set_global(settings);
        cx.set_http_client(http.clone());
        SettingsStore::update_global(cx, |store, cx| {
            store
                .set_user_settings(&policy_settings().to_string(), cx)
                .result()
        })
        .unwrap();
    });
    let fs = fs::FakeFs::new(cx.executor());
    fs.insert_tree(
        Path::new("/workspace-mcp"),
        json!({".zed":{"settings.json":json!({
        "disable_ai":false,"context_servers":servers
    }).to_string()}}),
    )
    .await;
    let client =
        cx.update(|cx| client::Client::new(Arc::new(clock::FakeSystemClock::new()), http, cx));
    let user_store = cx.new(|cx| client::UserStore::new(client.clone(), cx));
    let languages = Arc::new(language::LanguageRegistry::test(cx.executor()));
    // Keep the production maintenance loop: do not use Project::test or replace its store.
    let project = cx.update(|cx| {
        Project::local(
            client,
            node_runtime::NodeRuntime::unavailable(),
            user_store,
            languages,
            fs,
            None,
            project::LocalProjectFlags {
                init_worktree_trust: false,
                ..Default::default()
            },
            cx,
        )
    });
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let store = project.read_with(cx, |project, _| project.context_server_store());
    if trap_factory {
        let calls = factory_calls.clone();
        store.update(cx, |store, _| {
            store.set_context_server_factory(Box::new(move |_, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                panic!("workspace MCP factory must not be invoked");
            }))
        });
    }
    let worktree = project
        .update(cx, |project, cx| {
            project.create_worktree(Path::new("/workspace-mcp"), true, cx)
        })
        .await
        .unwrap();
    worktree
        .read_with(cx, |tree, _| tree.as_local().unwrap().scan_complete())
        .await;
    cx.run_until_parked();
    cx.update(|cx| {
        assert!(project::DisableAiSettings::get_global(cx).disable_ai);
        let location = settings::SettingsLocation {
            worktree_id: worktree.read(cx).id(),
            path: util::rel_path::RelPath::empty(),
        };
        let loaded = project::project_settings::ProjectSettings::get(Some(location), cx);
        assert!(
            !loaded.context_servers.is_empty(),
            "workspace settings must actually load"
        );
    });
    (project, factory_calls, http_calls)
}

#[gpui::test]
async fn production_project_blocks_workspace_mcp_factory_before_worktree_load(
    cx: &mut TestAppContext,
) {
    let (project,factory_calls,http_calls)=production_project_with_workspace_mcp(json!({
        "workspace-stdio":{"command":"must-not-execute","args":[],"enabled":true},
        "workspace-http":{"url":"https://workspace-mcp.invalid/mcp","headers":{"Authorization":"offline-fixture"},"enabled":true}
    }),true,cx).await;
    assert_eq!(factory_calls.load(Ordering::SeqCst), 0);
    assert_eq!(http_calls.load(Ordering::SeqCst), 0);
    project.read_with(cx, |project, cx| {
        assert!(
            project
                .context_server_store()
                .read(cx)
                .server_ids()
                .is_empty()
        )
    });
}

#[gpui::test]
async fn production_project_blocks_workspace_mcp_http_transport(cx: &mut TestAppContext) {
    let (project,factory_calls,http_calls)=production_project_with_workspace_mcp(json!({
        "workspace-http":{"url":"https://workspace-mcp.invalid/mcp","headers":{"Authorization":"offline-fixture"},"enabled":true}
    }),false,cx).await;
    assert_eq!(factory_calls.load(Ordering::SeqCst), 0);
    assert_eq!(http_calls.load(Ordering::SeqCst), 0);
    project.read_with(cx, |project, cx| {
        assert!(
            project
                .context_server_store()
                .read(cx)
                .server_ids()
                .is_empty()
        )
    });
}
