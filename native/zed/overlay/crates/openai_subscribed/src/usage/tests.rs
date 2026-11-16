use super::*;
use crate::{CREDENTIALS_KEY, CodexCredentials};
use credentials_provider::CredentialsProvider;
use futures::channel::oneshot;
use gpui::{AppContext as _, AsyncApp, Entity, TestAppContext};
use http_client::{FakeHttpClient, HttpClient, RequestTimeout, Response};
use parking_lot::Mutex;
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Once,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context as PollContext, Poll},
};

const SENTINEL: &str = "SYNTHETIC_PRIVATE_USAGE_SENTINEL";

struct CapturedLogs(Mutex<Vec<String>>);
static LOGS: CapturedLogs = CapturedLogs(Mutex::new(Vec::new()));
static INIT_LOGGER: Once = Once::new();
impl log::Log for CapturedLogs {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }
    fn log(&self, record: &log::Record<'_>) {
        self.0.lock().push(record.args().to_string());
    }
    fn flush(&self) {}
}

pub(crate) fn capture_logs() {
    INIT_LOGGER.call_once(|| {
        log::set_logger(&LOGS).expect("capture native test logs");
        log::set_max_level(log::LevelFilter::Trace);
    });
}

pub(crate) fn assert_no_logged(needle: &str) {
    assert!(
        LOGS.0.lock().iter().all(|line| !line.contains(needle)),
        "private data in native logs"
    );
}

struct FakeCredentialsProvider {
    bytes: Option<Vec<u8>>,
    fail: bool,
    gate: Mutex<Option<oneshot::Receiver<()>>>,
    reads: AtomicUsize,
    writes: AtomicUsize,
    deletes: AtomicUsize,
}

impl FakeCredentialsProvider {
    fn new(bytes: Option<Vec<u8>>) -> Self {
        Self {
            bytes,
            fail: false,
            gate: Mutex::new(None),
            reads: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
            deletes: AtomicUsize::new(0),
        }
    }
    fn assert_read_only(&self) {
        assert_eq!(self.reads.load(Ordering::SeqCst), 1);
        assert_eq!(self.writes.load(Ordering::SeqCst), 0);
        assert_eq!(self.deletes.load(Ordering::SeqCst), 0);
    }
}

impl CredentialsProvider for FakeCredentialsProvider {
    fn read_credentials<'a>(
        &'a self,
        url: &'a str,
        _: &'a AsyncApp,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<(String, Vec<u8>)>>> + 'a>> {
        assert_eq!(url, CREDENTIALS_KEY);
        self.reads.fetch_add(1, Ordering::SeqCst);
        let gate = self.gate.lock().take();
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.await.expect("release credential load");
            }
            if self.fail {
                return Err(anyhow::anyhow!(SENTINEL));
            }
            Ok(self.bytes.clone().map(|bytes| (SENTINEL.into(), bytes)))
        })
    }
    fn write_credentials<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
        _: &'a [u8],
        _: &'a AsyncApp,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'a>> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { panic!("probe must not write credentials") })
    }
    fn delete_credentials<'a>(
        &'a self,
        _: &'a str,
        _: &'a AsyncApp,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'a>> {
        self.deletes.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { panic!("probe must not delete credentials") })
    }
}

fn fresh_credentials() -> CodexCredentials {
    CodexCredentials {
        access_token: SENTINEL.into(),
        refresh_token: SENTINEL.into(),
        expires_at_ms: now_ms() + 3_600_000,
        account_id: Some("fixture-account".into()),
        email: Some(SENTINEL.into()),
    }
}

fn provider() -> Arc<FakeCredentialsProvider> {
    Arc::new(FakeCredentialsProvider::new(Some(
        serde_json::to_vec(&fresh_credentials()).unwrap(),
    )))
}

fn state(
    http: Arc<dyn HttpClient>,
    provider: Arc<FakeCredentialsProvider>,
    cx: &mut TestAppContext,
) -> Entity<State> {
    cx.new(|cx| State::new_without_model_discovery(http, provider, cx))
}

fn window(percent: f64, seconds: u64) -> Value {
    json!({"used_percent":percent,"limit_window_seconds":seconds,"reset_after_seconds":75,"reset_at":1800000000})
}

fn fixture() -> Value {
    json!({
        "account_id":"fixture-account", "user_id":SENTINEL, "unknown":SENTINEL,
        "plan_type":"future-plan",
        "rate_limit": {
            "allowed":true,"limit_reached":false,
            "primary_window":window(100.0,18000),"secondary_window":window(0.0,604800)
        },
        "additional_rate_limits":[{
            "limit_name":"fixture-limit", "metered_feature":"fixture-feature",
            "rate_limit":{"allowed":false,"limit_reached":true,"primary_window":window(0.0,604800),"secondary_window":window(75.0,18000)}
        }],
        "spend_control":{"reached":true},
        "credits":{"balance":SENTINEL,"has_credits":true,"unlimited":false}
    })
}

fn assert_sanitized(error: UsageError) {
    for output in [
        format!("{error:?}"),
        error.to_string(),
        serde_json::to_string(&error).unwrap(),
    ] {
        assert!(!output.contains(SENTINEL), "secret in sanitized error");
    }
}

#[gpui::test]
async fn usage_exact_request_and_no_discovery(cx: &mut TestAppContext) {
    let calls = Arc::new(AtomicUsize::new(0));
    let http = FakeHttpClient::create({
        let calls = calls.clone();
        move |request| {
            calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.method(), Method::GET);
            assert_eq!(
                request.uri().to_string(),
                "https://chatgpt.com/backend-api/wham/usage"
            );
            assert_eq!(request.headers().len(), 2);
            assert_eq!(
                request.headers()["Authorization"],
                format!("Bearer {SENTINEL}")
            );
            assert!(request.headers()["Authorization"].is_sensitive());
            assert_eq!(request.headers()["ChatGPT-Account-Id"], "fixture-account");
            assert!(request.headers()["ChatGPT-Account-Id"].is_sensitive());
            assert_eq!(
                request.extensions().get::<RedirectPolicy>(),
                Some(&RedirectPolicy::NoFollow)
            );
            assert_eq!(
                request.extensions().get::<RequestTimeout>(),
                Some(&RequestTimeout(Duration::from_secs(15)))
            );
            async move {
                let mut request_body = Vec::new();
                request.into_body().read_to_end(&mut request_body).await?;
                assert!(request_body.is_empty());
                Ok(Response::builder()
                    .status(200)
                    .body(AsyncBody::from(fixture().to_string()))?)
            }
        }
    });
    let provider = provider();
    let state = state(http, provider.clone(), cx);
    cx.run_until_parked();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "constructor must not discover or refresh"
    );
    let before = now_ms();
    let snapshot = state
        .update(cx, |state, cx| state.read_usage(cx))
        .await
        .unwrap();
    assert!(snapshot.observed_at_ms >= before && snapshot.observed_at_ms <= now_ms());
    assert_eq!(
        snapshot.account_binding,
        UsageAccountBinding::ResponseAccountMatched
    );
    assert_eq!(snapshot.plan_type.as_deref(), Some("future-plan"));
    let limit = snapshot.rate_limit.as_ref().unwrap();
    assert!(limit.allowed && !limit.limit_reached);
    assert_eq!(limit.primary_window.as_ref().unwrap().used_percent, 100.0);
    assert_eq!(
        limit.primary_window.as_ref().unwrap().limit_window_seconds,
        18000
    );
    assert_eq!(
        limit.primary_window.as_ref().unwrap().reset_after_seconds,
        75
    );
    assert_eq!(limit.primary_window.as_ref().unwrap().reset_at, 1800000000);
    assert_eq!(
        limit
            .secondary_window
            .as_ref()
            .unwrap()
            .limit_window_seconds,
        604800
    );
    let extra = &snapshot.additional_rate_limits.as_ref().unwrap()[0];
    assert!(!extra.rate_limit.allowed && extra.rate_limit.limit_reached);
    assert_eq!(
        extra
            .rate_limit
            .primary_window
            .as_ref()
            .unwrap()
            .used_percent,
        0.0
    );
    assert_eq!(
        extra
            .rate_limit
            .primary_window
            .as_ref()
            .unwrap()
            .limit_window_seconds,
        604800
    );
    assert_eq!(
        extra
            .rate_limit
            .secondary_window
            .as_ref()
            .unwrap()
            .limit_window_seconds,
        18000
    );
    assert_eq!(extra.limit_name, "fixture-limit");
    assert_eq!(extra.metered_feature, "fixture-feature");
    assert!(snapshot.spend_control.as_ref().unwrap().reached);
    for output in [
        format!("{snapshot:?}"),
        serde_json::to_string(&snapshot).unwrap(),
    ] {
        assert!(!output.contains(SENTINEL));
        assert!(!output.contains("fixture-account"));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    provider.assert_read_only();
}

#[test]
fn usage_optional_windows_and_account_binding() {
    for body in [
        json!({}),
        json!({"rate_limit":null}),
        json!({"rate_limit":{"allowed":false,"limit_reached":true}}),
        json!({"rate_limit":{"allowed":true,"limit_reached":false,"primary_window":null,"secondary_window":null}}),
    ] {
        let snapshot = parse_usage(body.to_string().as_bytes(), Some("fixture-account")).unwrap();
        assert_eq!(
            snapshot.account_binding,
            UsageAccountBinding::RequestScopedUnverifiedResponse
        );
        if let Some(limit) = snapshot.rate_limit {
            assert!(limit.primary_window.is_none() && limit.secondary_window.is_none());
        }
    }
    let snapshot = parse_usage(fixture().to_string().as_bytes(), None).unwrap();
    assert_eq!(
        snapshot.account_binding,
        UsageAccountBinding::RequestScopedUnverifiedResponse
    );
    let error = parse_usage(fixture().to_string().as_bytes(), Some(SENTINEL)).unwrap_err();
    assert_eq!(error, UsageError::AccountMismatch);
    assert_sanitized(error);
}

#[test]
fn usage_rejects_invalid_schema_and_percentages() {
    let mut invalid = Vec::new();
    for value in [
        json!(-0.1),
        json!(100.1),
        json!(SENTINEL),
        Value::Null,
        json!(true),
    ] {
        let mut body = fixture();
        body["rate_limit"]["primary_window"]["used_percent"] = value;
        invalid.push(body);
    }
    for key in ["allowed", "limit_reached"] {
        let mut body = fixture();
        body["rate_limit"].as_object_mut().unwrap().remove(key);
        invalid.push(body);
        let mut body = fixture();
        body["rate_limit"][key] = json!(SENTINEL);
        invalid.push(body);
    }
    for key in [
        "used_percent",
        "limit_window_seconds",
        "reset_after_seconds",
        "reset_at",
    ] {
        let mut body = fixture();
        body["rate_limit"]["primary_window"]
            .as_object_mut()
            .unwrap()
            .remove(key);
        invalid.push(body);
        for value in [json!(-1), json!(SENTINEL), Value::Null] {
            let mut body = fixture();
            body["rate_limit"]["primary_window"][key] = value;
            invalid.push(body);
        }
    }
    let mut body = fixture();
    body["rate_limit"]["primary_window"]["limit_window_seconds"] = json!(0);
    invalid.push(body);
    let mut body = fixture();
    body["additional_rate_limits"][0]["rate_limit"]["secondary_window"]["used_percent"] =
        json!(101);
    invalid.push(body);
    for key in ["limit_name", "metered_feature", "rate_limit"] {
        let mut body = fixture();
        body["additional_rate_limits"][0]
            .as_object_mut()
            .unwrap()
            .remove(key);
        invalid.push(body);
    }
    for spend in [
        json!({}),
        json!({"reached":SENTINEL}),
        json!({"reached":null}),
    ] {
        let mut body = fixture();
        body["spend_control"] = spend;
        invalid.push(body);
    }
    for body in invalid {
        let error = parse_usage(body.to_string().as_bytes(), None).unwrap_err();
        assert_eq!(error, UsageError::MalformedResponse);
        assert_sanitized(error);
    }
    for body in ["null", "[]", "{", "{\"rate_limit\":NaN}", SENTINEL] {
        assert_eq!(
            parse_usage(body.as_bytes(), None).unwrap_err(),
            UsageError::MalformedResponse
        );
    }
}

#[gpui::test]
async fn usage_load_failures_are_sanitized_and_never_send(cx: &mut TestAppContext) {
    capture_logs();
    let mut failed = FakeCredentialsProvider::new(None);
    failed.fail = true;
    let malformed = format!(
        "{{\"access_token\":\"{SENTINEL}\",\"refresh_token\":\"{SENTINEL}\",\"expires_at_ms\":\"{SENTINEL}\"}}"
    );
    for (provider, expected) in [
        (
            FakeCredentialsProvider::new(None),
            UsageError::MissingCredentials,
        ),
        (failed, UsageError::CredentialReadFailed),
        (
            FakeCredentialsProvider::new(Some(malformed.into_bytes())),
            UsageError::MalformedCredentials,
        ),
    ] {
        let provider = Arc::new(provider);
        let http = FakeHttpClient::create(|_| async {
            panic!("invalid credentials must not issue any HTTP")
        });
        let state = state(http, provider.clone(), cx);
        // read_usage must also await an unfinished initial credential load.
        let error = state
            .update(cx, |state, cx| state.read_usage(cx))
            .await
            .unwrap_err();
        assert_eq!(error, expected);
        assert_sanitized(error);
        provider.assert_read_only();
        cx.read(|cx| assert!(state.read(cx).load_task().is_none()));
    }
    assert!(
        LOGS.0.lock().iter().all(|line| !line.contains(SENTINEL)),
        "loader logged a private provider/parser error"
    );
}

#[gpui::test]
async fn usage_expired_credentials_never_refresh(cx: &mut TestAppContext) {
    let mut credentials = fresh_credentials();
    credentials.expires_at_ms = 0;
    let provider = Arc::new(FakeCredentialsProvider::new(Some(
        serde_json::to_vec(&credentials).unwrap(),
    )));
    let http =
        FakeHttpClient::create(|_| async { panic!("expired credentials must not refresh or GET") });
    let state = state(http, provider.clone(), cx);
    assert_eq!(
        state
            .update(cx, |state, cx| state.read_usage(cx))
            .await
            .unwrap_err(),
        UsageError::RefreshRequired
    );
    provider.assert_read_only();
}

#[gpui::test]
async fn usage_invalid_headers_are_sanitized(cx: &mut TestAppContext) {
    for invalid_account in [false, true] {
        let mut credentials = fresh_credentials();
        if invalid_account {
            credentials.account_id = Some(format!("{SENTINEL}\n"));
        } else {
            credentials.access_token.push('\n');
        }
        let provider = Arc::new(FakeCredentialsProvider::new(Some(
            serde_json::to_vec(&credentials).unwrap(),
        )));
        let http = FakeHttpClient::create(|_| async { panic!("invalid headers must not send") });
        let state = state(http, provider.clone(), cx);
        let error = state
            .update(cx, |state, cx| state.read_usage(cx))
            .await
            .unwrap_err();
        assert_eq!(error, UsageError::InvalidAuthHeaders);
        assert_sanitized(error);
        provider.assert_read_only();
    }
}

#[gpui::test]
async fn usage_http_and_parse_failures_are_sanitized(cx: &mut TestAppContext) {
    for status in [200, 301, 302, 307, 401, 403, 429, 500] {
        let http = FakeHttpClient::create(move |_| async move {
            Ok(Response::builder()
                .status(status)
                .header("Location", format!("https://invalid.example/{SENTINEL}"))
                .body(AsyncBody::from(SENTINEL))?)
        });
        let provider = provider();
        let state = state(http, provider.clone(), cx);
        let error = state
            .update(cx, |state, cx| state.read_usage(cx))
            .await
            .unwrap_err();
        assert_eq!(
            error,
            if status == 200 {
                UsageError::MalformedResponse
            } else {
                UsageError::HttpStatus { status }
            }
        );
        assert_sanitized(error);
        provider.assert_read_only();
    }
    let http = FakeHttpClient::create(|_| async { Err(anyhow::anyhow!(SENTINEL)) });
    let provider = provider();
    let state = state(http, provider.clone(), cx);
    let error = state
        .update(cx, |state, cx| state.read_usage(cx))
        .await
        .unwrap_err();
    assert_eq!(error, UsageError::Transport);
    assert_sanitized(error);
    provider.assert_read_only();
}

struct CountingBody {
    bytes: Vec<u8>,
    consumed: Arc<AtomicUsize>,
    pending_at_end: bool,
    fail_at_end: bool,
}
impl futures::AsyncRead for CountingBody {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut PollContext<'_>,
        buffer: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let position = this.consumed.load(Ordering::SeqCst);
        if position == this.bytes.len() {
            if this.fail_at_end {
                return Poll::Ready(Err(std::io::Error::other(SENTINEL)));
            }
            if this.pending_at_end {
                return Poll::Pending;
            }
        }
        let count = buffer.len().min(this.bytes.len() - position);
        buffer[..count].copy_from_slice(&this.bytes[position..position + count]);
        this.consumed.fetch_add(count, Ordering::SeqCst);
        Poll::Ready(Ok(count))
    }
}

#[gpui::test]
async fn usage_exact_body_limit_and_oversized_valid_prefix(cx: &mut TestAppContext) {
    for size in [65536, 65537, 131072] {
        let consumed = Arc::new(AtomicUsize::new(0));
        let http = FakeHttpClient::create({
            let consumed = consumed.clone();
            move |_| {
                let mut bytes = b"{}".to_vec();
                bytes.resize(size, b' ');
                let body = CountingBody {
                    bytes,
                    consumed: consumed.clone(),
                    pending_at_end: size > 65536,
                    fail_at_end: false,
                };
                async move {
                    Ok(Response::builder()
                        .status(200)
                        .body(AsyncBody::from_reader(body))?)
                }
            }
        });
        let provider = provider();
        let state = state(http, provider.clone(), cx);
        let result = state.update(cx, |state, cx| state.read_usage(cx)).await;
        if size == 65536 {
            assert!(result.is_ok());
        } else {
            assert_eq!(result.unwrap_err(), UsageError::BodyTooLarge);
        }
        assert_eq!(consumed.load(Ordering::SeqCst), size.min(65537));
        provider.assert_read_only();
    }
}

#[gpui::test]
async fn usage_body_read_error_is_sanitized(cx: &mut TestAppContext) {
    let http = FakeHttpClient::create(|_| async {
        Ok(
            Response::builder().body(AsyncBody::from_reader(CountingBody {
                bytes: b"{}".to_vec(),
                consumed: Arc::new(AtomicUsize::new(0)),
                pending_at_end: false,
                fail_at_end: true,
            }))?,
        )
    });
    let provider = provider();
    let state = state(http, provider.clone(), cx);
    let error = state
        .update(cx, |state, cx| state.read_usage(cx))
        .await
        .unwrap_err();
    assert_eq!(error, UsageError::Transport);
    assert_sanitized(error);
    provider.assert_read_only();
}

#[gpui::test]
async fn usage_header_deadline_is_exactly_fifteen_seconds(cx: &mut TestAppContext) {
    let http = FakeHttpClient::create(|_| futures::future::pending());
    let provider = provider();
    let state = state(http, provider.clone(), cx);
    let mut task = state.update(cx, |state, cx| state.read_usage(cx));
    cx.run_until_parked();
    cx.executor().advance_clock(Duration::from_millis(14999));
    cx.run_until_parked();
    assert!((&mut task).now_or_never().is_none());
    cx.executor().advance_clock(Duration::from_millis(1));
    cx.run_until_parked();
    let error = task.await.unwrap_err();
    assert_eq!(error, UsageError::Timeout);
    assert_sanitized(error);
    provider.assert_read_only();
}

#[gpui::test]
async fn usage_deadline_covers_headers_and_partial_pending_body(cx: &mut TestAppContext) {
    let (release, gate) = oneshot::channel();
    let gate = Arc::new(Mutex::new(Some(gate)));
    let consumed = Arc::new(AtomicUsize::new(0));
    let http = FakeHttpClient::create({
        let consumed = consumed.clone();
        move |_| {
            let gate = gate.lock().take().unwrap();
            let consumed = consumed.clone();
            async move {
                gate.await?;
                Ok(
                    Response::builder().body(AsyncBody::from_reader(CountingBody {
                        bytes: b"{}".to_vec(),
                        consumed,
                        pending_at_end: true,
                        fail_at_end: false,
                    }))?,
                )
            }
        }
    });
    let provider = provider();
    let state = state(http, provider.clone(), cx);
    let mut task = state.update(cx, |state, cx| state.read_usage(cx));
    cx.run_until_parked();
    cx.executor().advance_clock(Duration::from_secs(14));
    cx.run_until_parked();
    release.send(()).unwrap();
    cx.run_until_parked();
    assert_eq!(consumed.load(Ordering::SeqCst), 2);
    cx.executor().advance_clock(Duration::from_millis(999));
    cx.run_until_parked();
    assert!((&mut task).now_or_never().is_none());
    cx.executor().advance_clock(Duration::from_millis(1));
    cx.run_until_parked();
    assert_eq!(task.await.unwrap_err(), UsageError::Timeout);
    provider.assert_read_only();
}

#[gpui::test]
async fn usage_rejects_late_auth_generation_and_account_changes(cx: &mut TestAppContext) {
    for change in ["generation", "account", "sign_out"] {
        let (release, gate) = oneshot::channel();
        let gate = Arc::new(Mutex::new(Some(gate)));
        let http = FakeHttpClient::create(move |_| {
            let gate = gate.lock().take().unwrap();
            async move {
                gate.await?;
                Ok(Response::builder().body(AsyncBody::from(fixture().to_string()))?)
            }
        });
        let provider = provider();
        let state = state(http, provider.clone(), cx);
        let task = state.update(cx, |state, cx| state.read_usage(cx));
        cx.run_until_parked();
        state.update(cx, |state, cx| match change {
            "generation" => state.auth_generation += 1,
            "account" => state.credentials.as_mut().unwrap().account_id = Some(SENTINEL.into()),
            // Drop the persistence task: test the immediate native state transition only.
            _ => drop(state.sign_out(cx)),
        });
        release.send(()).unwrap();
        cx.run_until_parked();
        let error = task.await.unwrap_err();
        assert_eq!(error, UsageError::AuthenticationChanged);
        assert_sanitized(error);
        provider.assert_read_only();
    }
}

#[gpui::test]
async fn usage_rejects_auth_change_during_initial_load(cx: &mut TestAppContext) {
    let (release, gate) = oneshot::channel();
    let provider = provider();
    *provider.gate.lock() = Some(gate);
    let http =
        FakeHttpClient::create(|_| async { panic!("late credentials must not trigger GET") });
    let state = state(http, provider.clone(), cx);
    let task = state.update(cx, |state, cx| state.read_usage(cx));
    cx.run_until_parked();
    state.update(cx, |state, cx| drop(state.sign_out(cx)));
    release.send(()).unwrap();
    cx.run_until_parked();
    assert_eq!(task.await.unwrap_err(), UsageError::AuthenticationChanged);
    cx.read(|cx| assert!(!state.read(cx).is_authenticated()));
    provider.assert_read_only();
}

#[gpui::test]
async fn usage_optional_account_header_and_response_mismatch(cx: &mut TestAppContext) {
    for (request_account, response_account) in [
        (None, None),
        (None, Some(SENTINEL)),
        (Some("fixture-account"), None),
        (Some("fixture-account"), Some(SENTINEL)),
    ] {
        let mut credentials = fresh_credentials();
        credentials.account_id = request_account.map(str::to_owned);
        let provider = Arc::new(FakeCredentialsProvider::new(Some(
            serde_json::to_vec(&credentials).unwrap(),
        )));
        let calls = Arc::new(AtomicUsize::new(0));
        let http = FakeHttpClient::create({
            let calls = calls.clone();
            move |request| {
                calls.fetch_add(1, Ordering::SeqCst);
                assert_eq!(
                    request
                        .headers()
                        .get("ChatGPT-Account-Id")
                        .map(|value| value.to_str().unwrap()),
                    request_account
                );
                assert_eq!(
                    request.headers().len(),
                    if request_account.is_some() { 2 } else { 1 }
                );
                async move {
                    Ok(Response::builder().body(AsyncBody::from(
                        json!({"account_id":response_account}).to_string(),
                    ))?)
                }
            }
        });
        let state = state(http, provider.clone(), cx);
        let result = state.update(cx, |state, cx| state.read_usage(cx)).await;
        if request_account.is_some() && response_account.is_some() {
            let error = result.unwrap_err();
            assert_eq!(error, UsageError::AccountMismatch);
            assert_sanitized(error);
        } else {
            let snapshot = result.unwrap();
            assert_eq!(
                snapshot.account_binding,
                UsageAccountBinding::RequestScopedUnverifiedResponse
            );
            assert!(!serde_json::to_string(&snapshot).unwrap().contains(SENTINEL));
            assert!(!format!("{snapshot:?}").contains(SENTINEL));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        provider.assert_read_only();
    }
}
