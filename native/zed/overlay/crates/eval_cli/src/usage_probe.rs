use clap::Parser;
use credentials_provider::CredentialsProvider;
use gpui::{App, AppContext as _, Task};
use http_client::HttpClient;
use openai_subscribed::{State, UsageError, UsageSnapshot};
use serde::Serialize;
use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicI32, Ordering},
    },
};

#[derive(Parser, Debug)]
#[command(
    name = "zed-native-usage-probe",
    about = "Read native ChatGPT subscription usage once; no refresh or inference"
)]
struct Args {
    /// Dedicated application data/configuration directory (does not isolate native credentials).
    #[arg(long, value_name = "PATH", value_hint = clap::ValueHint::DirPath)]
    user_data_dir: PathBuf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "category", rename_all = "snake_case")]
enum SetupError {
    DataDirectoryMustBeAbsolute,
    DataDirectoryMustBeUtf8,
    DataDirectoryUnavailable,
    HttpInitializationFailed,
    RuntimeInitializationFailed,
}

impl SetupError {
    fn message(self) -> &'static str {
        match self {
            Self::DataDirectoryMustBeAbsolute => {
                "Use --user-data-dir with an absolute path to a dedicated data directory."
            }
            Self::DataDirectoryMustBeUtf8 => {
                "Choose a --user-data-dir path containing valid UTF-8."
            }
            Self::DataDirectoryUnavailable => {
                "Cannot create or resolve --user-data-dir. Choose a writable directory, not a file."
            }
            Self::HttpInitializationFailed => "Cannot initialize the native HTTP transport.",
            Self::RuntimeInitializationFailed => {
                "Cannot initialize or complete the native probe runtime."
            }
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum ProbeError {
    Usage(UsageError),
    Setup(SetupError),
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum Envelope {
    Ok { usage: UsageSnapshot },
    Error { error: ProbeError, message: String },
}

fn write_envelope(
    writer: impl Write,
    result: Result<UsageSnapshot, ProbeError>,
) -> Result<bool, ()> {
    let success = result.is_ok();
    let envelope = match result {
        Ok(usage) => Envelope::Ok { usage },
        Err(error) => {
            let message = match &error {
                ProbeError::Usage(error) => error.to_string(),
                ProbeError::Setup(error) => error.message().into(),
            };
            Envelope::Error { error, message }
        }
    };
    let mut writer = writer;
    serde_json::to_writer(&mut writer, &envelope).map_err(|_| ())?;
    writer.write_all(b"\n").map_err(|_| ())?;
    Ok(success)
}

fn prepare_data_dir(directory: &Path) -> Result<String, SetupError> {
    if !directory.is_absolute() {
        return Err(SetupError::DataDirectoryMustBeAbsolute);
    }
    directory
        .to_str()
        .ok_or(SetupError::DataDirectoryMustBeUtf8)?;
    std::fs::create_dir_all(directory).map_err(|_| SetupError::DataDirectoryUnavailable)?;
    let directory = directory
        .canonicalize()
        .map_err(|_| SetupError::DataDirectoryUnavailable)?;
    directory
        .into_os_string()
        .into_string()
        .map_err(|_| SetupError::DataDirectoryMustBeUtf8)
}

fn start_probe(
    http_client: Arc<dyn HttpClient>,
    credentials_provider: Arc<dyn CredentialsProvider>,
    cx: &mut App,
) -> Task<Result<UsageSnapshot, UsageError>> {
    let state =
        cx.new(|cx| State::new_without_model_discovery(http_client, credentials_provider, cx));
    cx.spawn(async move |cx| {
        // Keep the entity alive until both the credential load and usage read finish.
        state.update(cx, |state, cx| state.read_usage(cx)).await
    })
}

fn print_result(result: Result<UsageSnapshot, ProbeError>) -> i32 {
    match write_envelope(std::io::stdout().lock(), result) {
        Ok(true) => 0,
        Ok(false) => 1,
        Err(()) => {
            eprintln!("output_failed");
            1
        }
    }
}

fn main() {
    let args = Args::parse();
    // Upstream initialization may panic with environment or transport details.
    // The probe must never put those details in its output.
    std::panic::set_hook(Box::new(|_| {
        print_result(Err(ProbeError::Setup(
            SetupError::RuntimeInitializationFailed,
        )));
        std::process::exit(1);
    }));
    let directory = match prepare_data_dir(&args.user_data_dir) {
        Ok(directory) => directory,
        Err(error) => std::process::exit(print_result(Err(ProbeError::Setup(error)))),
    };
    paths::set_custom_data_dir(&directory);
    let exit_code = Arc::new(AtomicI32::new(1));
    let completed = exit_code.clone();
    gpui_platform::headless().run(move |cx| {
        let version = env!("ZED_PKG_VERSION")
            .parse()
            .expect("compile-time version must be valid");
        release_channel::init(version, cx);
        gpui_tokio::init(cx);
        let settings_store = settings::SettingsStore::new(cx, &settings::default_settings());
        cx.set_global(settings_store);
        let http_client = match reqwest_client::ReqwestClient::user_agent("Zed Native Usage Probe")
        {
            Ok(client) => Arc::new(client),
            Err(_) => {
                completed.store(
                    print_result(Err(ProbeError::Setup(SetupError::HttpInitializationFailed))),
                    Ordering::SeqCst,
                );
                cx.quit();
                return;
            }
        };
        let credentials_provider = zed_credentials_provider::global(cx);
        let task = start_probe(http_client, credentials_provider, cx);
        cx.spawn(async move |cx| {
            let result = task.await.map_err(ProbeError::Usage);
            completed.store(print_result(result), Ordering::SeqCst);
            cx.update(|cx| cx.quit());
        })
        .detach();
    });
    std::process::exit(exit_code.load(Ordering::SeqCst));
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt as _;
    use gpui::{AsyncApp, TestAppContext};
    use http_client::{AsyncBody, FakeHttpClient, RedirectPolicy, RequestTimeout, Response};
    use serde_json::{Value, json};
    use std::{
        future::Future,
        pin::Pin,
        sync::atomic::AtomicUsize,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    const SENTINEL: &str = "SYNTHETIC_PRIVATE_PROBE_SENTINEL";

    struct FakeCredentialsProvider {
        missing: bool,
        reads: AtomicUsize,
        writes: AtomicUsize,
        deletes: AtomicUsize,
    }
    impl CredentialsProvider for FakeCredentialsProvider {
        fn read_credentials<'a>(
            &'a self,
            url: &'a str,
            _: &'a AsyncApp,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<(String, Vec<u8>)>>> + 'a>> {
            assert_eq!(url, "https://chatgpt.com/backend-api/codex");
            self.reads.fetch_add(1, Ordering::SeqCst);
            async move {
                if self.missing {
                    return Ok(None);
                }
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                Ok(Some((
                    SENTINEL.into(),
                    serde_json::to_vec(&json!({
                        "access_token":SENTINEL,"refresh_token":SENTINEL,
                        "expires_at_ms":now+3600000,"account_id":"fixture-account","email":SENTINEL
                    }))?,
                )))
            }
            .boxed_local()
        }
        fn write_credentials<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
            _: &'a [u8],
            _: &'a AsyncApp,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'a>> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            async { panic!("probe must not write") }.boxed_local()
        }
        fn delete_credentials<'a>(
            &'a self,
            _: &'a str,
            _: &'a AsyncApp,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'a>> {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            async { panic!("probe must not delete") }.boxed_local()
        }
    }

    #[test]
    fn help_and_required_isolated_path_parse_before_startup() {
        let error = Args::try_parse_from(["probe", "--help"]).unwrap_err();
        assert_eq!(error.kind(), clap::error::ErrorKind::DisplayHelp);
        assert!(error.to_string().contains("--user-data-dir <PATH>"));
        let error = Args::try_parse_from(["probe"]).unwrap_err();
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
        assert!(Args::try_parse_from(["probe", "--user-data-dir", "/fixture/probe-data"]).is_ok());
        assert_eq!(
            prepare_data_dir(Path::new("relative-data")).unwrap_err(),
            SetupError::DataDirectoryMustBeAbsolute
        );
    }

    #[test]
    fn preflight_errors_are_actionable_and_sanitized() {
        // The executable itself is a known file, so no fixture directory is written.
        let executable = std::env::current_exe().unwrap();
        assert_eq!(
            prepare_data_dir(&executable).unwrap_err(),
            SetupError::DataDirectoryUnavailable
        );
        let mut output = Vec::new();
        assert!(
            !write_envelope(
                &mut output,
                Err(ProbeError::Setup(SetupError::DataDirectoryUnavailable))
            )
            .unwrap()
        );
        let value: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(value["error"]["category"], "data_directory_unavailable");
        assert!(
            value["message"]
                .as_str()
                .unwrap()
                .contains("writable directory")
        );
        assert!(
            !String::from_utf8(output)
                .unwrap()
                .contains(executable.to_str().unwrap())
        );
    }

    #[cfg(unix)]
    #[test]
    fn preflight_rejects_non_utf8_without_echoing_path() {
        use std::os::unix::ffi::OsStrExt as _;
        let path = Path::new(std::ffi::OsStr::from_bytes(b"/fixture/\xff-private"));
        assert_eq!(
            prepare_data_dir(path).unwrap_err(),
            SetupError::DataDirectoryMustBeUtf8
        );
    }

    #[gpui::test]
    async fn probe_orchestration_one_native_read_one_get_one_envelope(cx: &mut TestAppContext) {
        for missing in [false, true] {
            let provider = Arc::new(FakeCredentialsProvider {
                missing,
                reads: AtomicUsize::new(0),
                writes: AtomicUsize::new(0),
                deletes: AtomicUsize::new(0),
            });
            let calls = Arc::new(AtomicUsize::new(0));
            let http = FakeHttpClient::create({
                let calls = calls.clone();
                move |request| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(request.method(), http_client::Method::GET);
                    assert_eq!(
                        request.uri().to_string(),
                        "https://chatgpt.com/backend-api/wham/usage"
                    );
                    assert_eq!(
                        request.headers()["Authorization"],
                        format!("Bearer {SENTINEL}")
                    );
                    assert_eq!(request.headers()["ChatGPT-Account-Id"], "fixture-account");
                    assert_eq!(
                        request.extensions().get::<RedirectPolicy>(),
                        Some(&RedirectPolicy::NoFollow)
                    );
                    assert_eq!(
                        request.extensions().get::<RequestTimeout>(),
                        Some(&RequestTimeout(Duration::from_secs(15)))
                    );
                    async {
                        Ok(Response::builder().body(AsyncBody::from(json!({
                    "account_id":"fixture-account","user_id":SENTINEL,
                    "rate_limit":{"allowed":false,"limit_reached":true,"primary_window":null,"secondary_window":null}
                }).to_string()))?)
                    }
                }
            });
            let task = cx.update(|cx| start_probe(http, provider.clone(), cx));
            let result = task.await.map_err(ProbeError::Usage);
            let mut output = Vec::new();
            assert_eq!(write_envelope(&mut output, result).unwrap(), !missing);
            let output = String::from_utf8(output).unwrap();
            assert_eq!(output.lines().count(), 1);
            assert!(!output.contains(SENTINEL) && !output.contains("fixture-account"));
            let value: Value = serde_json::from_str(&output).unwrap();
            if missing {
                assert_eq!(value["status"], "error");
                assert_eq!(value["error"]["category"], "missing_credentials");
            } else {
                assert_eq!(value["status"], "ok");
                assert_eq!(value["usage"]["rate_limit"]["allowed"], false);
                assert_eq!(value["usage"]["rate_limit"]["limit_reached"], true);
            }
            assert_eq!(provider.reads.load(Ordering::SeqCst), 1);
            assert_eq!(provider.writes.load(Ordering::SeqCst), 0);
            assert_eq!(provider.deletes.load(Ordering::SeqCst), 0);
            assert_eq!(calls.load(Ordering::SeqCst), usize::from(!missing));
            cx.run_until_parked();
        }
    }
}
