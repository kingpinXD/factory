use super::*;
use gpui::{AppContext as _, TestAppContext};
use http_client::{FakeHttpClient, Response};
use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
};

struct FakeStore {
    lock_path: PathBuf,
    _directory: TestLockDirectory,
    values: Mutex<HashMap<String, Vec<u8>>>,
    calls: Mutex<Vec<(String, String)>>,
    gate: Mutex<Option<futures::channel::oneshot::Receiver<()>>>,
    write_gate: Mutex<Option<futures::channel::oneshot::Receiver<()>>>,
    stall: bool,
    fail: bool,
}
impl Default for FakeStore {
    fn default() -> Self {
        let root = std::env::temp_dir().join(format!("n{:08x}", rand::random::<u32>()));
        Self {
            lock_path: root.join("native-auth/factory-openai/credential.lock"),
            _directory: TestLockDirectory(root),
            values: Mutex::new(HashMap::new()),
            calls: Mutex::new(Vec::new()),
            gate: Mutex::new(None),
            write_gate: Mutex::new(None),
            stall: false,
            fail: false,
        }
    }
}
struct TestLockDirectory(PathBuf);
impl Drop for TestLockDirectory {
    fn drop(&mut self) {
        if self.0.exists() {
            std::fs::remove_dir_all(&self.0).expect("remove owned test lock directory");
        }
    }
}
fn new_credentials(
    store: Arc<FakeStore>,
    namespace: &str,
) -> Result<Arc<NativeCredentials>, NativeAuthError> {
    NativeCredentials::new_at(store.clone(), namespace, store.lock_path.clone())
}

impl CredentialsProvider for FakeStore {
    fn read_credentials<'a>(
        &'a self,
        key: &'a str,
        _: &'a AsyncApp,
    ) -> LocalBoxFuture<'a, Result<Option<(String, Vec<u8>)>>> {
        self.calls.lock().unwrap().push(("read".into(), key.into()));
        async move {
            let gate = self.gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.await?;
            }
            if self.stall {
                futures::future::pending::<()>().await;
            }
            if self.fail {
                return Err(anyhow!("SYNTHETIC_SECRET"));
            }
            Ok(self
                .values
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .map(|bytes| ("Bearer".into(), bytes)))
        }
        .boxed_local()
    }
    fn write_credentials<'a>(
        &'a self,
        key: &'a str,
        _: &'a str,
        bytes: &'a [u8],
        _: &'a AsyncApp,
    ) -> LocalBoxFuture<'a, Result<()>> {
        async move {
            let intent = read_intent(&self.lock_path)
                .unwrap()
                .expect("intent must precede external write");
            let record: StoredGrant = serde_json::from_slice(bytes).unwrap();
            assert_eq!(
                intent.intended_state_digest,
                state_digest(Some(&record)).unwrap()
            );
            self.calls
                .lock()
                .unwrap()
                .push(("write".into(), key.into()));
            let gate = self.write_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.await?;
            }
            self.values
                .lock()
                .unwrap()
                .insert(key.into(), bytes.to_vec());
            Ok(())
        }
        .boxed_local()
    }
    fn delete_credentials<'a>(
        &'a self,
        key: &'a str,
        _: &'a AsyncApp,
    ) -> LocalBoxFuture<'a, Result<()>> {
        async move {
            let intent = read_intent(&self.lock_path)
                .unwrap()
                .expect("intent must precede external delete");
            assert_eq!(intent.intended_state_digest, state_digest(None).unwrap());
            self.calls
                .lock()
                .unwrap()
                .push(("delete".into(), key.into()));
            self.values.lock().unwrap().remove(key);
            Ok(())
        }
        .boxed_local()
    }
}
fn credentials(account: &str, expired: bool) -> CodexCredentials {
    CodexCredentials {
        access_token: "SYNTHETIC_SECRET".into(),
        refresh_token: "SYNTHETIC_SECRET".into(),
        expires_at_ms: if expired { 0 } else { now_ms() + 3_600_000 },
        account_id: Some(account.into()),
        email: None,
    }
}
fn seed(store: &FakeStore, account: &str, expired: bool) {
    let record = StoredGrant {
        version: 1,
        nonce: "a".repeat(64),
        credentials: credentials(account, expired),
    };
    store.values.lock().unwrap().insert(
        FACTORY_CREDENTIALS_KEY.into(),
        serde_json::to_vec(&record).unwrap(),
    );
    store
        .values
        .lock()
        .unwrap()
        .insert(CREDENTIALS_KEY.into(), b"desktop-marker".to_vec());
}
fn no_http() -> Arc<dyn HttpClient> {
    FakeHttpClient::create(|_| async { panic!("no network expected") })
}
async fn load(state: &Entity<State>, cx: &mut gpui::AsyncApp) {
    if let Some(load) = state.read_with(cx, |state, _| state.load_task()) {
        load.await.unwrap();
    }
}

#[gpui::test]
async fn missing_namespace_never_falls_back(cx: &mut TestAppContext) {
    let store = Arc::new(FakeStore::default());
    store.values.lock().unwrap().insert(
        CREDENTIALS_KEY.into(),
        serde_json::to_vec(&credentials("desktop", false)).unwrap(),
    );
    let provider = new_credentials(store.clone(), FACTORY_CREDENTIAL_NAMESPACE).unwrap();
    let state = cx.new(|cx| State::new_native(no_http(), provider.clone(), cx));
    let cx = &mut cx.to_async();
    load(&state, cx).await;
    assert_eq!(
        state.read_with(cx, |state, _| state.native_identity()),
        Err(NativeAuthError::MissingSetup)
    );
    assert!(!state.read_with(cx, |state, _| state.is_signing_in()));
    assert_eq!(
        *store.calls.lock().unwrap(),
        vec![("read".into(), FACTORY_CREDENTIALS_KEY.into())]
    );
    assert!(new_credentials(store, CREDENTIALS_KEY).is_err());
}

#[gpui::test]
async fn distinct_read_write_delete_and_binding_rotation(cx: &mut TestAppContext) {
    let store = Arc::new(FakeStore::default());
    seed(&store, "account", false);
    let provider = new_credentials(store.clone(), FACTORY_CREDENTIAL_NAMESPACE).unwrap();
    let cx = &cx.to_async();
    provider
        .read_credentials(CREDENTIALS_KEY, cx)
        .await
        .unwrap();
    let before = provider.metadata().unwrap();
    let bytes = serde_json::to_vec(&credentials("account", false)).unwrap();
    provider
        .write_credentials(CREDENTIALS_KEY, "Bearer", &bytes, cx)
        .await
        .unwrap();
    assert_eq!(
        provider.metadata().unwrap().identity_binding,
        before.identity_binding
    );
    assert_eq!(provider.metadata().unwrap().nonce, before.nonce);
    let mismatch = serde_json::to_vec(&credentials("other", false)).unwrap();
    let error = provider
        .write_credentials(CREDENTIALS_KEY, "Bearer", &mismatch, cx)
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "IdentityMismatch");
    provider.begin_authorized_setup();
    provider
        .write_credentials(CREDENTIALS_KEY, "Bearer", &bytes, cx)
        .await
        .unwrap();
    assert_ne!(
        provider.metadata().unwrap().identity_binding,
        before.identity_binding
    );
    let reloaded = new_credentials(store.clone(), FACTORY_CREDENTIAL_NAMESPACE).unwrap();
    reloaded
        .read_credentials(CREDENTIALS_KEY, cx)
        .await
        .unwrap();
    assert_eq!(reloaded.metadata(), provider.metadata());
    provider
        .delete_credentials(CREDENTIALS_KEY, cx)
        .await
        .unwrap();
    assert!(provider.metadata().is_none());
    assert_eq!(
        store.values.lock().unwrap().get(CREDENTIALS_KEY).unwrap(),
        b"desktop-marker"
    );
    assert!(
        store
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|(_, key)| key == FACTORY_CREDENTIALS_KEY)
    );
}

#[gpui::test]
async fn credential_load_timeout_and_provider_errors_are_sanitized(cx: &mut TestAppContext) {
    for (stall, expected) in [
        (true, NativeAuthError::CredentialLoadTimeout),
        (false, NativeAuthError::CredentialReadFailed),
    ] {
        let store = Arc::new(FakeStore {
            stall,
            fail: !stall,
            ..Default::default()
        });
        let provider = new_credentials(store, FACTORY_CREDENTIAL_NAMESPACE).unwrap();
        let state = cx.new(|cx| State::new_native(no_http(), provider.clone(), cx));
        let cx = &mut cx.to_async();
        load(&state, cx).await;
        assert_eq!(
            state.read_with(cx, |state, _| state.native_identity()),
            Err(expected)
        );
        assert!(
            !serde_json::to_string(&expected)
                .unwrap()
                .contains("SYNTHETIC_SECRET")
        );
    }
}

#[gpui::test]
async fn native_usage_pins_identity_and_requires_response_account(cx: &mut TestAppContext) {
    for response in [Some("account"), Some("other"), None] {
        let store = Arc::new(FakeStore::default());
        seed(&store, "account", false);
        let provider = new_credentials(store.clone(), FACTORY_CREDENTIAL_NAMESPACE).unwrap();
        let http = FakeHttpClient::create(move |_| async move {
            Ok(Response::builder()
                .status(200)
                .body(AsyncBody::from(
                    serde_json::json!({"account_id": response}).to_string(),
                ))
                .unwrap())
        });
        let state = cx.new(|cx| State::new_native(http, provider.clone(), cx));
        let cx = &mut cx.to_async();
        load(&state, cx).await;
        let binding = provider.metadata().unwrap().identity_binding;
        let error = state
            .update(cx, |state, cx| state.read_native_usage("wrong".into(), cx))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            NativeUsageError::Auth(NativeAuthError::IdentityMismatch)
        ));
        let result = state
            .update(cx, |state, cx| state.read_native_usage(binding.clone(), cx))
            .await;
        match response {
            Some("account") => assert_eq!(result.unwrap().identity_binding, Some(binding)),
            Some(_) => assert!(matches!(
                result,
                Err(NativeUsageError::Usage(UsageError::AccountMismatch))
            )),
            None => assert_eq!(result.unwrap().identity_binding, None),
        }
        assert_eq!(store.calls.lock().unwrap().len(), 1);
    }
}

#[gpui::test]
async fn native_refresh_single_flight_and_fatal_are_scoped(cx: &mut TestAppContext) {
    for fatal in [false, true] {
        let store = Arc::new(FakeStore::default());
        seed(&store, "account", true);
        let provider = new_credentials(store.clone(), FACTORY_CREDENTIAL_NAMESPACE).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let http = FakeHttpClient::create({
            let calls = calls.clone();
            move |request| {
                calls.fetch_add(1, Ordering::SeqCst);
                assert_eq!(request.uri().to_string(), OPENAI_TOKEN_URL);
                async move {
                    let claims = URL_SAFE_NO_PAD.encode(br#"{"chatgpt_account_id":"account"}"#);
                    let body = if fatal {
                        "SYNTHETIC_SECRET".into()
                    } else {
                        serde_json::json!({"access_token":"new-access", "refresh_token":"new-refresh", "expires_in":3600,
                            "id_token":format!("header.{claims}.signature")}).to_string()
                    };
                    Ok(Response::builder()
                        .status(if fatal { 400 } else { 200 })
                        .body(AsyncBody::from(body))
                        .unwrap())
                }
            }
        });
        let state = cx.new(|cx| State::new_native(http.clone(), provider.clone(), cx));
        let cx = &mut cx.to_async();
        load(&state, cx).await;
        let before = provider.metadata().unwrap().identity_binding;
        let weak = state.downgrade();
        let mut second_cx = cx.clone();
        let (first, second) = futures::join!(
            get_fresh_credentials(&weak, &http, cx),
            get_fresh_credentials(&weak, &http, &mut second_cx)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        if fatal {
            assert!(first.is_err() && second.is_err());
            assert!(provider.metadata().is_none());
            assert!(!first.unwrap_err().to_string().contains("SYNTHETIC_SECRET"));
        } else {
            assert_eq!(first.unwrap().access_token, "new-access");
            assert_eq!(second.unwrap().access_token, "new-access");
            assert_eq!(provider.metadata().unwrap().identity_binding, before);
        }
        assert_eq!(
            store.values.lock().unwrap().get(CREDENTIALS_KEY).unwrap(),
            b"desktop-marker"
        );
        assert!(
            store
                .calls
                .lock()
                .unwrap()
                .iter()
                .all(|(_, key)| key == FACTORY_CREDENTIALS_KEY)
        );
    }
}

#[gpui::test]
async fn credential_timeout_is_five_seconds_and_cancels_read(cx: &mut TestAppContext) {
    let (release, gate) = futures::channel::oneshot::channel();
    let store = Arc::new(FakeStore {
        gate: Mutex::new(Some(gate)),
        ..Default::default()
    });
    seed(&store, "account", false);
    let provider = new_credentials(store.clone(), FACTORY_CREDENTIAL_NAMESPACE).unwrap();
    let state = cx.new(|cx| State::new_native(no_http(), provider.clone(), cx));
    let mut task = state.update(cx, |state, _| state.load_task().unwrap());
    cx.run_until_parked();
    cx.executor().advance_clock(Duration::from_millis(4999));
    cx.run_until_parked();
    assert!((&mut task).now_or_never().is_none());
    cx.executor().advance_clock(Duration::from_millis(1));
    cx.run_until_parked();
    (&mut task)
        .now_or_never()
        .expect("load must finish at five seconds")
        .unwrap();
    assert_eq!(
        provider.load_error(),
        Some(NativeAuthError::CredentialLoadTimeout)
    );
    assert!(
        release.send(()).is_err(),
        "timed-out read future was not dropped"
    );
    assert!(provider.metadata().is_none());
    assert_eq!(store.calls.lock().unwrap().len(), 1);
}

#[gpui::test]
async fn binding_change_during_usage_rejects_response(cx: &mut TestAppContext) {
    let store = Arc::new(FakeStore::default());
    seed(&store, "account", false);
    let provider = new_credentials(store, FACTORY_CREDENTIAL_NAMESPACE).unwrap();
    let (release, gate) = futures::channel::oneshot::channel();
    let gate = Arc::new(Mutex::new(Some(gate)));
    let http = FakeHttpClient::create(move |_| {
        let gate = gate.lock().unwrap().take().unwrap();
        async move {
            gate.await?;
            Ok(Response::builder()
                .body(AsyncBody::from(r#"{"account_id":"account"}"#))
                .unwrap())
        }
    });
    let state = cx.new(|cx| State::new_native(http, provider.clone(), cx));
    load(&state, &mut cx.to_async()).await;
    let binding = provider.metadata().unwrap().identity_binding;
    let task = state.update(cx, |state, cx| state.read_native_usage(binding, cx));
    cx.run_until_parked();
    provider.begin_authorized_setup();
    provider
        .write_credentials(
            CREDENTIALS_KEY,
            "Bearer",
            &serde_json::to_vec(&credentials("account", false)).unwrap(),
            &cx.to_async(),
        )
        .await
        .unwrap();
    release.send(()).unwrap();
    assert!(matches!(
        task.await,
        Err(NativeUsageError::Auth(NativeAuthError::IdentityMismatch))
    ));
}

fn native_catalog() -> String {
    serde_json::json!({"models":[
        {"slug":"gpt-6.1-sol", "display_name":"Discovered Sol", "visibility":"list", "priority":1,
         "context_window":321000,"input_modalities":["text","image"],"default_reasoning_level":"high",
         "supported_reasoning_levels":[{"effort":"high"}],"service_tiers":[{"id":"priority"}]},
        {"slug":"hidden-model", "display_name":"Hidden", "visibility":"hide", "priority":0}
    ]}).to_string()
}

#[gpui::test]
async fn native_discovery_is_explicit_and_uses_exact_scoped_catalog(cx: &mut TestAppContext) {
    let store = Arc::new(FakeStore::default());
    seed(&store, "account", false);
    let provider = new_credentials(store.clone(), FACTORY_CREDENTIAL_NAMESPACE).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let http = FakeHttpClient::create({
        let calls = calls.clone();
        move |request| {
            calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.method(), Method::GET);
            assert_eq!(
                request.uri().to_string(),
                format!("{CODEX_BASE_URL}/models?client_version={MODEL_CATALOG_CLIENT_VERSION}")
            );
            assert_eq!(
                request.headers()["Authorization"],
                "Bearer SYNTHETIC_SECRET"
            );
            assert_eq!(request.headers()["chatgpt-account-id"], "account");
            async {
                Ok(Response::builder()
                    .body(AsyncBody::from(native_catalog()))
                    .unwrap())
            }
        }
    });
    let state = cx.new(|cx| State::new_native(http, provider.clone(), cx));
    load(&state, &mut cx.to_async()).await;
    cx.run_until_parked();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "constructor must not discover models"
    );
    cx.read(|cx| assert_eq!(state.read(cx).available_models(), ChatGptModel::all()));
    let expected = provider.metadata().unwrap().identity_binding;
    state
        .update(cx, |state, cx| state.discover_native_models(expected, cx))
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    cx.read(|cx| {
        let models = state.read(cx).available_models();
        assert_eq!(models.len(), 1);
        let ChatGptModel::Discovered(model) = &models[0] else {
            panic!("must use discovered capabilities")
        };
        assert_eq!(model.id, "gpt-6.1-sol");
        assert_eq!(model.max_token_count, 321000);
        assert!(model.supports_images);
        assert_eq!(model.default_reasoning_effort, Some(ReasoningEffort::High));
        assert_eq!(
            model.supported_reasoning_efforts,
            vec![ReasoningEffort::High]
        );
        assert!(model.supports_priority);
    });
    assert_eq!(
        *store.calls.lock().unwrap(),
        vec![("read".into(), FACTORY_CREDENTIALS_KEY.into())]
    );
}

#[gpui::test]
async fn native_discovery_missing_wrong_binding_and_load_timeout_do_not_send(
    cx: &mut TestAppContext,
) {
    for (present, stall, pin, expected) in [
        (false, false, "wrong", NativeAuthError::MissingSetup),
        (true, false, "wrong", NativeAuthError::IdentityMismatch),
        (true, false, "", NativeAuthError::IdentityMismatch),
        (true, true, "wrong", NativeAuthError::CredentialLoadTimeout),
    ] {
        let store = Arc::new(FakeStore {
            stall,
            ..Default::default()
        });
        if present {
            seed(&store, "account", true);
        }
        let provider = new_credentials(store, FACTORY_CREDENTIAL_NAMESPACE).unwrap();
        let state = cx.new(|cx| State::new_native(no_http(), provider, cx));
        // Do not await loading separately: discovery must wait for the bounded scoped load.
        let error = state
            .update(cx, |state, cx| state.discover_native_models(pin.into(), cx))
            .await
            .unwrap_err();
        assert_eq!(error, expected);
        cx.read(|cx| assert_eq!(state.read(cx).available_models(), ChatGptModel::all()));
    }
}

#[gpui::test]
async fn native_discovery_rejects_late_binding_change_without_publishing(cx: &mut TestAppContext) {
    let store = Arc::new(FakeStore::default());
    seed(&store, "account", false);
    let provider = new_credentials(store, FACTORY_CREDENTIAL_NAMESPACE).unwrap();
    let (release, gate) = futures::channel::oneshot::channel();
    let gate = Arc::new(Mutex::new(Some(gate)));
    let http = FakeHttpClient::create(move |_| {
        let gate = gate.lock().unwrap().take().unwrap();
        async move {
            gate.await?;
            Ok(Response::builder()
                .body(AsyncBody::from(native_catalog()))
                .unwrap())
        }
    });
    let state = cx.new(|cx| State::new_native(http, provider.clone(), cx));
    load(&state, &mut cx.to_async()).await;
    let pin = provider.metadata().unwrap().identity_binding;
    let task = state.update(cx, |state, cx| state.discover_native_models(pin, cx));
    cx.run_until_parked();
    provider.begin_authorized_setup();
    provider
        .write_credentials(
            CREDENTIALS_KEY,
            "Bearer",
            &serde_json::to_vec(&credentials("account", false)).unwrap(),
            &cx.to_async(),
        )
        .await
        .unwrap();
    release.send(()).unwrap();
    assert_eq!(task.await.unwrap_err(), NativeAuthError::IdentityMismatch);
    cx.read(|cx| assert_eq!(state.read(cx).available_models(), ChatGptModel::all()));
}

#[gpui::test]
async fn native_discovery_deadline_covers_catalog_and_refresh(cx: &mut TestAppContext) {
    for expired in [false] {
        let store = Arc::new(FakeStore::default());
        seed(&store, "account", expired);
        let provider = new_credentials(store, FACTORY_CREDENTIAL_NAMESPACE).unwrap();
        let http = FakeHttpClient::create(move |request| {
            assert_eq!(
                request.method(),
                if expired { Method::POST } else { Method::GET }
            );
            futures::future::pending()
        });
        let state = cx.new(|cx| State::new_native(http, provider.clone(), cx));
        load(&state, &mut cx.to_async()).await;
        let pin = provider.metadata().unwrap().identity_binding;
        let mut task = state.update(cx, |state, cx| state.discover_native_models(pin, cx));
        cx.run_until_parked();
        cx.executor().advance_clock(Duration::from_millis(59999));
        cx.run_until_parked();
        assert!((&mut task).now_or_never().is_none());
        cx.executor().advance_clock(Duration::from_millis(1));
        cx.run_until_parked();
        assert_eq!(
            task.now_or_never()
                .expect("discovery must finish at sixty seconds"),
            Err(NativeAuthError::CatalogTimeout)
        );
        cx.read(|cx| {
            assert_eq!(state.read(cx).available_models(), ChatGptModel::all());
            assert_eq!(
                state.read(cx).model_catalog_error().unwrap().as_ref(),
                "CatalogTimeout"
            );
        });
    }
}

#[gpui::test]
async fn native_discovery_errors_and_private_catalog_logging_are_sanitized(
    cx: &mut TestAppContext,
) {
    const SENTINEL: &str = "PRIVATE_NATIVE_CATALOG_ERROR_SENTINEL";
    crate::usage::tests::capture_logs();
    for transport in [false, true] {
        let store = Arc::new(FakeStore::default());
        seed(&store, "account", false);
        let provider = new_credentials(store, FACTORY_CREDENTIAL_NAMESPACE).unwrap();
        let http = FakeHttpClient::create(move |_| async move {
            if transport {
                return Err(anyhow!(SENTINEL));
            }
            Ok(Response::builder()
                .status(403)
                .body(AsyncBody::from(SENTINEL))
                .unwrap())
        });
        let state = cx.new(|cx| State::new_native(http, provider.clone(), cx));
        load(&state, &mut cx.to_async()).await;
        let pin = provider.metadata().unwrap().identity_binding;
        let error = state
            .update(cx, |state, cx| state.discover_native_models(pin, cx))
            .await
            .unwrap_err();
        assert_eq!(error, NativeAuthError::CatalogFailed);
        assert!(!serde_json::to_string(&error).unwrap().contains(SENTINEL));
        // The legacy helper can also be called internally on scoped State. Its
        // returned error and stored message must be safe for the existing logger.
        let result = state
            .update(cx, |state, cx| state.refresh_model_catalog(cx))
            .await;
        assert!(!format!("{result:?}").contains(SENTINEL));
        log_model_catalog_error(result);
        cx.read(|cx| {
            assert!(
                !state
                    .read(cx)
                    .model_catalog_error()
                    .unwrap()
                    .contains(SENTINEL)
            )
        });
    }
    crate::usage::tests::assert_no_logged(SENTINEL);
}

#[gpui::test]
async fn native_discovery_refreshes_only_factory_grant_before_catalog(cx: &mut TestAppContext) {
    let store = Arc::new(FakeStore::default());
    seed(&store, "account", true);
    let provider = new_credentials(store.clone(), FACTORY_CREDENTIAL_NAMESPACE).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let http = FakeHttpClient::create({
        let calls = calls.clone();
        move |request| {
            let call = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                let body = match call {
                    0 => {
                        assert_eq!(request.method(), Method::POST);
                        assert_eq!(request.uri().to_string(), OPENAI_TOKEN_URL);
                        let claims = URL_SAFE_NO_PAD.encode(br#"{"chatgpt_account_id":"account"}"#);
                        serde_json::json!({"access_token":"scoped-refreshed", "refresh_token":"refreshed-secret", "expires_in":3600,
                            "id_token":format!("header.{claims}.signature")}).to_string()
                    }
                    1 => {
                        assert_eq!(request.method(), Method::GET);
                        assert_eq!(
                            request.uri().to_string(),
                            format!(
                                "{CODEX_BASE_URL}/models?client_version={MODEL_CATALOG_CLIENT_VERSION}"
                            )
                        );
                        assert_eq!(
                            request.headers()["Authorization"],
                            "Bearer scoped-refreshed"
                        );
                        assert_eq!(request.headers()["chatgpt-account-id"], "account");
                        native_catalog()
                    }
                    _ => panic!("unexpected HTTP call"),
                };
                Ok(Response::builder().body(AsyncBody::from(body)).unwrap())
            }
        }
    });
    let state = cx.new(|cx| State::new_native(http, provider.clone(), cx));
    load(&state, &mut cx.to_async()).await;
    let pin = provider.metadata().unwrap().identity_binding;
    state
        .update(cx, |state, cx| {
            state.discover_native_models(pin.clone(), cx)
        })
        .await
        .unwrap();
    assert_eq!(provider.metadata().unwrap().identity_binding, pin);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        *store.calls.lock().unwrap(),
        vec![
            ("read".into(), FACTORY_CREDENTIALS_KEY.into()),
            ("read".into(), FACTORY_CREDENTIALS_KEY.into()),
            ("read".into(), FACTORY_CREDENTIALS_KEY.into()),
            ("write".into(), FACTORY_CREDENTIALS_KEY.into()),
            ("read".into(), FACTORY_CREDENTIALS_KEY.into())
        ]
    );
    assert_eq!(
        store.values.lock().unwrap().get(CREDENTIALS_KEY).unwrap(),
        b"desktop-marker"
    );
}

#[gpui::test]
async fn public_namespace_alias_maps_only_to_internal_key(cx: &mut TestAppContext) {
    let store = Arc::new(FakeStore::default());
    assert_eq!(FACTORY_CREDENTIAL_NAMESPACE, "factory-openai");
    assert_eq!(
        FACTORY_CREDENTIALS_KEY,
        "https://factory.zed.native/openai-subscribed/v1"
    );
    for invalid in [
        FACTORY_CREDENTIALS_KEY,
        CREDENTIALS_KEY,
        "arbitrary-key",
        "",
        "Factory-OpenAI",
    ] {
        assert!(matches!(
            new_credentials(store.clone(), invalid),
            Err(NativeAuthError::InvalidNamespace)
        ));
    }
    assert!(store.calls.lock().unwrap().is_empty());
    seed(&store, "account", false);
    let provider = new_credentials(store.clone(), "factory-openai").unwrap();
    provider
        .read_credentials(CREDENTIALS_KEY, &cx.to_async())
        .await
        .unwrap();
    let metadata = provider.metadata().unwrap();
    assert_eq!(metadata.namespace, "factory-openai");
    let public = serde_json::to_string(&metadata).unwrap();
    assert!(!public.contains(FACTORY_CREDENTIALS_KEY));
    assert!(!public.contains("account"));
    assert_eq!(
        *store.calls.lock().unwrap(),
        vec![("read".into(), FACTORY_CREDENTIALS_KEY.into())]
    );
}

fn start_refresh(
    state: &Entity<State>,
    cx: &mut TestAppContext,
) -> Task<Result<CodexCredentials, LanguageModelCompletionError>> {
    state.update(cx, |state, cx| {
        let http = state.http_client.clone();
        cx.spawn(async move |this, cx| get_fresh_credentials(&this, &http, cx).await)
    })
}
fn token_response() -> AsyncBody {
    let claims = URL_SAFE_NO_PAD.encode(br#"{"chatgpt_account_id":"account"}"#);
    AsyncBody::from(serde_json::json!({"access_token":"refreshed-access", "refresh_token":"refreshed-token", "expires_in":3600,
        "id_token":format!("header.{claims}.signature")}).to_string())
}
struct PendingTokenBody;
impl futures::io::AsyncRead for PendingTokenBody {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        _: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Pending
    }
}

#[gpui::test]
async fn scoped_refresh_http_and_body_timeout_clear_cancelled_waiter_and_retry(
    cx: &mut TestAppContext,
) {
    for body in [false, true] {
        let store = Arc::new(FakeStore::default());
        seed(&store, "account", true);
        let provider = new_credentials(store.clone(), FACTORY_CREDENTIAL_NAMESPACE).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let http = FakeHttpClient::create({
            let calls = calls.clone();
            move |_| {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if call == 0 {
                        if body {
                            return Ok(Response::builder()
                                .body(AsyncBody::from_reader(PendingTokenBody))
                                .unwrap());
                        }
                        futures::future::pending::<()>().await;
                    }
                    Ok(Response::builder().body(token_response()).unwrap())
                }
            }
        });
        let state = cx.new(|cx| State::new_native(http, provider, cx));
        load(&state, &mut cx.to_async()).await;
        let waiter = start_refresh(&state, cx);
        cx.run_until_parked();
        drop(waiter);
        cx.executor().advance_clock(Duration::from_millis(29999));
        cx.run_until_parked();
        cx.read(|cx| assert!(state.read(cx).refresh_task.is_some()));
        cx.executor().advance_clock(Duration::from_millis(1));
        cx.run_until_parked();
        cx.read(|cx| {
            assert!(state.read(cx).refresh_task.is_none());
            assert_eq!(
                state.read(cx).last_auth_error().unwrap().as_ref(),
                "RefreshTimeout"
            );
        });
        let refreshed = start_refresh(&state, cx).await.unwrap();
        assert_eq!(refreshed.access_token, "refreshed-access");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            store.values.lock().unwrap().get(CREDENTIALS_KEY).unwrap(),
            b"desktop-marker"
        );
    }
}

#[gpui::test]
async fn old_pending_refresh_cannot_overwrite_or_delete_new_setup(cx: &mut TestAppContext) {
    let store = Arc::new(FakeStore::default());
    seed(&store, "account", true);
    let old_provider = new_credentials(store.clone(), FACTORY_CREDENTIAL_NAMESPACE).unwrap();
    let new_provider = new_credentials(store.clone(), FACTORY_CREDENTIAL_NAMESPACE).unwrap();
    let (late, gate) = futures::channel::oneshot::channel();
    let gate = Arc::new(Mutex::new(Some(gate)));
    let calls = Arc::new(AtomicUsize::new(0));
    let http = FakeHttpClient::create({
        let calls = calls.clone();
        move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            let gate = gate
                .lock()
                .unwrap()
                .take()
                .expect("stale worker must not send again");
            async move {
                gate.await?;
                Ok(Response::builder().body(token_response()).unwrap())
            }
        }
    });
    let old_state = cx.new(|cx| State::new_native(http, old_provider.clone(), cx));
    let setup_state = cx.new(|cx| State::new_native(no_http(), new_provider.clone(), cx));
    load(&old_state, &mut cx.to_async()).await;
    load(&setup_state, &mut cx.to_async()).await;
    let old_binding = old_provider.metadata().unwrap().identity_binding;
    let pending = start_refresh(&old_state, cx);
    cx.run_until_parked();
    let competing_lock = open_lock(&store.lock_path).unwrap();
    assert!(matches!(
        competing_lock.try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    cx.executor().advance_clock(NATIVE_REFRESH_TIMEOUT);
    cx.run_until_parked();
    assert!(
        pending
            .await
            .unwrap_err()
            .to_string()
            .contains("RefreshTimeout")
    );
    assert!(
        late.send(()).is_err(),
        "timed out HTTP future must be dropped"
    );
    new_provider.begin_authorized_setup();
    new_provider
        .write_credentials(
            CREDENTIALS_KEY,
            "Bearer",
            &serde_json::to_vec(&credentials("account", true)).unwrap(),
            &cx.to_async(),
        )
        .await
        .unwrap();
    assert_ne!(
        new_provider.metadata().unwrap().identity_binding,
        old_binding
    );
    let protected = store
        .values
        .lock()
        .unwrap()
        .get(FACTORY_CREDENTIALS_KEY)
        .unwrap()
        .clone();
    let old_bytes = serde_json::to_vec(&credentials("account", false)).unwrap();
    for error in [
        old_provider
            .write_credentials(CREDENTIALS_KEY, "Bearer", &old_bytes, &cx.to_async())
            .await
            .unwrap_err(),
        old_provider
            .delete_credentials(CREDENTIALS_KEY, &cx.to_async())
            .await
            .unwrap_err(),
    ] {
        assert_eq!(error.to_string(), "StaleGrant");
    }
    let retry = start_refresh(&old_state, cx).await.unwrap_err();
    assert!(retry.to_string().contains("StaleGrant"));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        *store
            .values
            .lock()
            .unwrap()
            .get(FACTORY_CREDENTIALS_KEY)
            .unwrap(),
        protected
    );
    assert_eq!(
        store.values.lock().unwrap().get(CREDENTIALS_KEY).unwrap(),
        b"desktop-marker"
    );
}

#[gpui::test]
async fn scoped_refresh_write_timeout_retains_lock_until_settled_then_requires_reload(
    cx: &mut TestAppContext,
) {
    let store = Arc::new(FakeStore::default());
    seed(&store, "account", true);
    let provider = new_credentials(store.clone(), FACTORY_CREDENTIAL_NAMESPACE).unwrap();
    let setup = new_credentials(store.clone(), FACTORY_CREDENTIAL_NAMESPACE).unwrap();
    let (release, gate) = futures::channel::oneshot::channel();
    *store.write_gate.lock().unwrap() = Some(gate);
    let http = FakeHttpClient::create(|_| async {
        Ok(Response::builder().body(token_response()).unwrap())
    });
    let state = cx.new(|cx| State::new_native(http, provider.clone(), cx));
    let setup_state = cx.new(|cx| State::new_native(no_http(), setup.clone(), cx));
    load(&state, &mut cx.to_async()).await;
    load(&setup_state, &mut cx.to_async()).await;
    let waiter = start_refresh(&state, cx);
    cx.run_until_parked();
    drop(waiter);
    cx.executor().advance_clock(NATIVE_REFRESH_TIMEOUT);
    cx.run_until_parked();
    cx.read(|cx| assert!(state.read(cx).refresh_task.is_none()));
    assert!(provider.has_pending_mutation());
    let competing_lock = open_lock(&store.lock_path).unwrap();
    assert!(matches!(
        competing_lock.try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    let retry = start_refresh(&state, cx).await.unwrap_err();
    assert!(retry.to_string().contains("MutationLockTimeout"));
    setup.begin_authorized_setup();
    let bytes = serde_json::to_vec(&credentials("account", false)).unwrap();
    let error = setup
        .write_credentials(CREDENTIALS_KEY, "Bearer", &bytes, &cx.to_async())
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "MutationLockTimeout");
    release.send(()).unwrap();
    cx.run_until_parked();
    assert!(!provider.has_pending_mutation());
    // Another process has changed this grant's credentials even though its nonce survived.
    let error = setup
        .delete_credentials(CREDENTIALS_KEY, &cx.to_async())
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "StaleGrant");
    assert!(
        start_refresh(&state, cx)
            .await
            .unwrap_err()
            .to_string()
            .contains("StaleGrant")
    );
    setup
        .read_credentials(CREDENTIALS_KEY, &cx.to_async())
        .await
        .unwrap();
    setup.begin_authorized_setup();
    setup
        .write_credentials(CREDENTIALS_KEY, "Bearer", &bytes, &cx.to_async())
        .await
        .unwrap();
    assert_eq!(
        provider
            .delete_credentials(CREDENTIALS_KEY, &cx.to_async())
            .await
            .unwrap_err()
            .to_string(),
        "StaleGrant"
    );
    let reloaded = new_credentials(store.clone(), FACTORY_CREDENTIAL_NAMESPACE).unwrap();
    let fresh = cx.new(|cx| State::new_native(no_http(), reloaded, cx));
    load(&fresh, &mut cx.to_async()).await;
    start_refresh(&fresh, cx).await.unwrap();
    assert_eq!(
        store.values.lock().unwrap().get(CREDENTIALS_KEY).unwrap(),
        b"desktop-marker"
    );
}

#[test]
fn native_mutation_lock_child() {
    let Some(path) = std::env::var_os("NATIVE_AUTH_TEST_LOCK") else {
        return;
    };
    let file = open_lock(Path::new(&path)).unwrap();
    assert!(matches!(
        file.try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
}
#[gpui::test]
async fn mutation_lock_excludes_another_process(cx: &mut TestAppContext) {
    let store = Arc::new(FakeStore::default());
    let provider = new_credentials(store.clone(), FACTORY_CREDENTIAL_NAMESPACE).unwrap();
    let lock = provider.lock(&cx.to_async()).await.unwrap();
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "native_auth::tests::native_mutation_lock_child",
            "--nocapture",
        ])
        .env("NATIVE_AUTH_TEST_LOCK", &store.lock_path)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "child lock test failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    drop(lock);
    provider.lock(&cx.to_async()).await.unwrap();
}

#[gpui::test]
async fn unsafe_mutation_lock_permissions_and_symlinks_are_rejected(cx: &mut TestAppContext) {
    use std::os::unix::fs::{PermissionsExt as _, symlink};
    let store = Arc::new(FakeStore::default());
    let provider = new_credentials(store.clone(), FACTORY_CREDENTIAL_NAMESPACE).unwrap();
    drop(provider.lock(&cx.to_async()).await.unwrap());
    std::fs::set_permissions(&store.lock_path, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(matches!(
        provider.lock(&cx.to_async()).await,
        Err(NativeAuthError::MutationLockFailed)
    ));
    std::fs::remove_file(&store.lock_path).unwrap();
    let target = store.lock_path.with_file_name("target");
    std::fs::write(&target, b"not a lock").unwrap();
    symlink(&target, &store.lock_path).unwrap();
    assert!(matches!(
        provider.lock(&cx.to_async()).await,
        Err(NativeAuthError::MutationLockFailed)
    ));
    assert_eq!(std::fs::read(target).unwrap(), b"not a lock");
}

#[derive(Serialize, Deserialize)]
struct FakeServiceRequest {
    operation: MutationOperation,
    intended: Option<StoredGrant>,
}

#[test]
fn native_crash_after_external_service_accepts_child() {
    use std::{
        io::{Read as _, Write as _},
        os::unix::net::UnixStream,
    };
    let Some(path) = std::env::var_os("NATIVE_AUTH_CRASH_LOCK") else {
        return;
    };
    let lock = open_lock(Path::new(&path)).unwrap();
    lock.try_lock().unwrap();
    let intent: MutationIntent =
        serde_json::from_str(&std::env::var("NATIVE_AUTH_FAKE_INTENT").unwrap()).unwrap();
    create_intent(Path::new(&path), &intent).unwrap();
    let mut service =
        UnixStream::connect(std::env::var_os("NATIVE_AUTH_FAKE_SERVICE").unwrap()).unwrap();
    service
        .write_all(
            std::env::var("NATIVE_AUTH_FAKE_REQUEST")
                .unwrap()
                .as_bytes(),
        )
        .unwrap();
    service.shutdown(std::net::Shutdown::Write).unwrap();
    let mut accepted = [0u8; 1];
    service.read_exact(&mut accepted).unwrap();
    assert_eq!(accepted, [1]);
    // Simulate abrupt process death after a separate service accepted, but before
    // that service commits. No destructor/intent cleanup runs in this process.
    std::process::exit(0);
}

#[gpui::test]
async fn crash_intent_blocks_new_actor_until_delayed_service_commit_is_positively_read(
    cx: &mut TestAppContext,
) {
    use std::{
        io::{Read as _, Write as _},
        os::unix::net::UnixListener,
    };
    for delete in [false, true] {
        let store = Arc::new(FakeStore::default());
        seed(&store, "account", false);
        let old = new_credentials(store.clone(), FACTORY_CREDENTIAL_NAMESPACE).unwrap();
        old.read_credentials(CREDENTIALS_KEY, &cx.to_async())
            .await
            .unwrap();
        drop(old.acquire_lock(&cx.to_async()).await.unwrap());
        let metadata = old.metadata().unwrap();
        let intended = (!delete).then(|| StoredGrant {
            version: 1,
            nonce: "b".repeat(64),
            credentials: credentials("account", true),
        });
        let intent = MutationIntent {
            version: 1,
            operation: if delete {
                MutationOperation::Delete
            } else {
                MutationOperation::Write
            },
            grant_digest: digest(metadata.nonce.as_bytes()),
            revision_digest: digest(metadata.revision.as_bytes()),
            intended_state_digest: state_digest(intended.as_ref()).unwrap(),
        };
        let request = FakeServiceRequest {
            operation: intent.operation,
            intended,
        };
        let socket = store.lock_path.ancestors().nth(3).unwrap().join("s");
        let listener = UnixListener::bind(&socket).unwrap();
        let external_service = std::thread::spawn(move || {
            let (mut client, _) = listener.accept().unwrap();
            let mut bytes = Vec::new();
            client.read_to_end(&mut bytes).unwrap();
            let request: FakeServiceRequest = serde_json::from_slice(&bytes).unwrap();
            // The service accepts the operation but does not commit it yet.
            client.write_all(&[1]).unwrap();
            request
        });
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "native_auth::tests::native_crash_after_external_service_accepts_child",
            ])
            .env("NATIVE_AUTH_CRASH_LOCK", &store.lock_path)
            .env(
                "NATIVE_AUTH_FAKE_INTENT",
                serde_json::to_string(&intent).unwrap(),
            )
            .env(
                "NATIVE_AUTH_FAKE_REQUEST",
                serde_json::to_string(&request).unwrap(),
            )
            .env("NATIVE_AUTH_FAKE_SERVICE", &socket)
            .output()
            .unwrap();
        assert!(
            child.status.success(),
            "fake issuer failed: {}",
            String::from_utf8_lossy(&child.stderr)
        );
        let accepted = external_service.join().unwrap();
        let new = new_credentials(store.clone(), FACTORY_CREDENTIAL_NAMESPACE).unwrap();
        assert!(new.has_pending_mutation());
        // The OS lock is free after death. That is deliberately NOT settlement.
        let lock = new.acquire_lock(&cx.to_async()).await.unwrap();
        drop(lock);
        let marker = std::fs::read_to_string(intent_path(&store.lock_path)).unwrap();
        for forbidden in [
            "SYNTHETIC_SECRET",
            "account",
            &metadata.nonce,
            &metadata.revision,
        ] {
            assert!(!marker.contains(forbidden));
        }
        assert_eq!(
            new.settle_pending_mutation(&cx.to_async()).await,
            Err(NativeAuthError::MutationUncertain)
        );
        assert!(
            new.read_credentials(CREDENTIALS_KEY, &cx.to_async())
                .await
                .unwrap_err()
                .to_string()
                .contains("MutationUncertain")
        );
        new.begin_authorized_setup();
        let fresh = serde_json::to_vec(&credentials("account", false)).unwrap();
        assert_eq!(
            new.write_credentials(CREDENTIALS_KEY, "Bearer", &fresh, &cx.to_async())
                .await
                .unwrap_err()
                .to_string(),
            "MutationUncertain"
        );
        assert_eq!(
            new.delete_credentials(CREDENTIALS_KEY, &cx.to_async())
                .await
                .unwrap_err()
                .to_string(),
            "MutationUncertain"
        );
        cx.executor().advance_clock(Duration::from_secs(86400));
        assert_eq!(
            new.settle_pending_mutation(&cx.to_async()).await,
            Err(NativeAuthError::MutationUncertain)
        );
        assert_eq!(
            std::fs::read_to_string(intent_path(&store.lock_path)).unwrap(),
            marker
        );
        // A matching account/nonce with different credentials is not exact settlement.
        if let Some(intended) = &accepted.intended {
            let mut wrong: StoredGrant =
                serde_json::from_slice(&serde_json::to_vec(intended).unwrap()).unwrap();
            wrong.credentials.access_token = "different-service-state".into();
            store.values.lock().unwrap().insert(
                FACTORY_CREDENTIALS_KEY.into(),
                serde_json::to_vec(&wrong).unwrap(),
            );
            assert_eq!(
                new.settle_pending_mutation(&cx.to_async()).await,
                Err(NativeAuthError::MutationUncertain)
            );
            assert!(new.has_pending_mutation());
        }
        // The fake external service commits only AFTER its issuing process died.
        match accepted.operation {
            MutationOperation::Write => {
                store.values.lock().unwrap().insert(
                    FACTORY_CREDENTIALS_KEY.into(),
                    serde_json::to_vec(&accepted.intended.unwrap()).unwrap(),
                );
            }
            MutationOperation::Delete => {
                store.values.lock().unwrap().remove(FACTORY_CREDENTIALS_KEY);
            }
        }
        new.settle_pending_mutation(&cx.to_async()).await.unwrap();
        assert!(!new.has_pending_mutation());
        assert!(!intent_path(&store.lock_path).exists());
        new.read_credentials(CREDENTIALS_KEY, &cx.to_async())
            .await
            .unwrap();
        new.begin_authorized_setup();
        new.write_credentials(CREDENTIALS_KEY, "Bearer", &fresh, &cx.to_async())
            .await
            .unwrap();
        assert_ne!(
            new.metadata().unwrap().identity_binding,
            metadata.identity_binding
        );
        let protected = store
            .values
            .lock()
            .unwrap()
            .get(FACTORY_CREDENTIALS_KEY)
            .unwrap()
            .clone();
        assert_eq!(
            old.write_credentials(CREDENTIALS_KEY, "Bearer", &fresh, &cx.to_async())
                .await
                .unwrap_err()
                .to_string(),
            "StaleGrant"
        );
        assert_eq!(
            old.delete_credentials(CREDENTIALS_KEY, &cx.to_async())
                .await
                .unwrap_err()
                .to_string(),
            "StaleGrant"
        );
        assert_eq!(
            *store
                .values
                .lock()
                .unwrap()
                .get(FACTORY_CREDENTIALS_KEY)
                .unwrap(),
            protected
        );
        assert_eq!(
            store.values.lock().unwrap().get(CREDENTIALS_KEY).unwrap(),
            b"desktop-marker"
        );
    }
}

#[gpui::test]
async fn identical_write_and_absent_delete_skip_external_mutations_and_intents(
    cx: &mut TestAppContext,
) {
    for present in [true, false] {
        let store = Arc::new(FakeStore::default());
        if present {
            seed(&store, "account", false);
        }
        let provider = new_credentials(store.clone(), FACTORY_CREDENTIAL_NAMESPACE).unwrap();
        let loaded = provider
            .read_credentials(CREDENTIALS_KEY, &cx.to_async())
            .await
            .unwrap();
        if let Some((_, bytes)) = loaded {
            provider
                .write_credentials(CREDENTIALS_KEY, "Bearer", &bytes, &cx.to_async())
                .await
                .unwrap();
        } else {
            provider
                .delete_credentials(CREDENTIALS_KEY, &cx.to_async())
                .await
                .unwrap();
        }
        assert!(!provider.has_pending_mutation());
        assert!(!intent_path(&store.lock_path).exists());
        assert!(
            store
                .calls
                .lock()
                .unwrap()
                .iter()
                .all(|(operation, _)| operation == "read")
        );
    }
}

#[gpui::test]
async fn invalid_intent_and_unreadable_native_state_remain_uncertain(cx: &mut TestAppContext) {
    let store = Arc::new(FakeStore::default());
    seed(&store, "account", false);
    let provider = new_credentials(store.clone(), FACTORY_CREDENTIAL_NAMESPACE).unwrap();
    provider
        .read_credentials(CREDENTIALS_KEY, &cx.to_async())
        .await
        .unwrap();
    let state = cx.new(|cx| State::new_native(no_http(), provider.clone(), cx));
    load(&state, &mut cx.to_async()).await;
    let pin = provider.metadata().unwrap().identity_binding;
    let lock = provider.acquire_lock(&cx.to_async()).await.unwrap();
    let intended = StoredGrant {
        version: 1,
        nonce: "d".repeat(64),
        credentials: credentials("account", false),
    };
    provider
        .journal_intent(MutationOperation::Write, Some(&intended))
        .unwrap();
    drop(lock);
    cx.read(|cx| {
        assert_eq!(
            state.read(cx).native_identity(),
            Err(NativeAuthError::MutationUncertain)
        )
    });
    assert!(matches!(
        state
            .update(cx, |state, cx| state.read_native_usage(pin, cx))
            .await,
        Err(NativeUsageError::Auth(NativeAuthError::MutationUncertain))
    ));
    assert!(
        start_refresh(&state, cx)
            .await
            .unwrap_err()
            .to_string()
            .contains("MutationUncertain")
    );

    store.values.lock().unwrap().insert(
        FACTORY_CREDENTIALS_KEY.into(),
        b"malformed SYNTHETIC_SECRET".to_vec(),
    );
    assert_eq!(
        provider.settle_pending_mutation(&cx.to_async()).await,
        Err(NativeAuthError::MutationUncertain)
    );
    assert!(provider.has_pending_mutation());
    std::fs::write(intent_path(&store.lock_path), b"partial-intent").unwrap();
    let reads = store.calls.lock().unwrap().len();
    assert_eq!(
        provider.settle_pending_mutation(&cx.to_async()).await,
        Err(NativeAuthError::MutationUncertain)
    );
    assert_eq!(store.calls.lock().unwrap().len(), reads);
    assert_eq!(
        std::fs::read(intent_path(&store.lock_path)).unwrap(),
        b"partial-intent"
    );
    assert!(provider.has_pending_mutation());
}

#[test]
fn production_lock_location_uses_real_home_existing_factory_root() {
    let path = credential_lock_path().unwrap();
    assert!(path.is_absolute());
    assert!(path.ends_with(".factory/native-auth/factory-openai/credential.lock"));
    assert_eq!(
        path.ancestors().nth(3).unwrap().canonicalize().unwrap(),
        path.ancestors().nth(3).unwrap()
    );
    assert!(!path.to_string_lossy().contains(".anuma-factory"));
}
