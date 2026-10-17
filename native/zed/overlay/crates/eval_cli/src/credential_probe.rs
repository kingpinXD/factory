use clap::Parser;
use gpui::{AppContext as _, Task};
use openai_subscribed::{
    State, UsageSnapshot,
    native_auth::{KeychainAuthorization, NativeAuthError, NativeCredentials, NativeUsageError},
};
use serde::Serialize;
use std::{
    io::Write,
    path::PathBuf,
    sync::{Arc, OnceLock},
    time::Duration,
};

#[derive(Parser, Debug)]
#[command(
    name = "zed-native-credential-setup",
    about = "Inspect factory native credentials; browser login requires --sign-in"
)]
struct SetupArgs {
    #[arg(long)]
    user_data_dir: PathBuf,
    #[arg(long, value_parser = [openai_subscribed::native_auth::FACTORY_CREDENTIAL_NAMESPACE])]
    credential_namespace: String,
    #[arg(long, conflicts_with = "authorize_keychain")]
    sign_in: bool,
    /// Read-only first-use Keychain enrollment; never opens OAuth or reads usage.
    #[arg(long)]
    authorize_keychain: bool,
    #[arg(long, requires = "authorize_keychain")]
    expected_identity: Option<String>,
}

#[derive(Parser, Debug)]
#[command(
    name = "zed-native-usage-probe",
    about = "Read factory-scoped native usage without login or refresh"
)]
struct UsageArgs {
    #[arg(long)]
    user_data_dir: PathBuf,
    #[arg(long, value_parser = [openai_subscribed::native_auth::FACTORY_CREDENTIAL_NAMESPACE])]
    credential_namespace: String,
    #[arg(long)]
    expected_identity: String,
    #[arg(long)]
    authorize_keychain: bool,
}

#[derive(Parser, Debug)]
#[command(
    about = "Read-only factory Keychain enrollment for this exact executable; no OAuth, refresh or inference"
)]
struct AuthorizationArgs {
    #[arg(long)]
    user_data_dir: PathBuf,
    #[arg(long, value_parser = [openai_subscribed::native_auth::FACTORY_CREDENTIAL_NAMESPACE])]
    credential_namespace: String,
    #[arg(long, required = true)]
    authorize_keychain: bool,
    #[arg(long)]
    expected_identity: Option<String>,
}

#[derive(Serialize)]
struct IdentityStatus {
    credential_namespace: &'static str,
    identity_binding: String,
    refresh_required: bool,
}
#[derive(Serialize)]
#[serde(untagged)]
enum Output {
    Identity(IdentityStatus),
    Usage {
        usage: UsageSnapshot,
    },
    Authorization {
        authorization: KeychainAuthorization,
    },
}
#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum Envelope {
    Ok {
        #[serde(flatten)]
        result: Output,
    },
    Error {
        error: NativeUsageError,
    },
}

fn emit(result: Result<Output, NativeUsageError>) -> i32 {
    let code = i32::from(result.is_err());
    let envelope = match result {
        Ok(result) => Envelope::Ok { result },
        Err(error) => Envelope::Error { error },
    };
    let mut writer = std::io::stdout().lock();
    if serde_json::to_writer(&mut writer, &envelope).is_err()
        || writer.write_all(b"\n").is_err()
        || writer.flush().is_err()
    {
        return 1;
    }
    code
}

fn exit_if_settled(code: i32, pending_mutation: bool) {
    if !pending_mutation {
        std::process::exit(code);
    }
}

fn install_panic_hook(provider: Arc<OnceLock<Arc<NativeCredentials>>>) {
    std::panic::set_hook(Box::new(move |_| {
        blocking_exit_with_barrier(
            emit(Err(NativeAuthError::RuntimeUnavailable.into())),
            &provider,
        );
    }));
}

fn blocking_exit_with_barrier(code: i32, provider: &OnceLock<Arc<NativeCredentials>>) -> ! {
    loop {
        exit_if_settled(
            code,
            provider
                .get()
                .is_some_and(|provider| provider.has_pending_mutation()),
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

async fn complete(
    result: Result<Output, NativeUsageError>,
    provider: &NativeCredentials,
    cx: &gpui::AsyncApp,
) -> ! {
    let code = emit(result);
    loop {
        // AppKit terminate: exits zero without returning from headless().run().
        // Keep the executor alive for detached persistence; never reconcile intent here.
        exit_if_settled(code, provider.has_pending_mutation());
        cx.background_executor()
            .timer(Duration::from_millis(100))
            .await;
    }
}

pub(crate) fn authorization_requested(
    arguments: impl IntoIterator<Item = std::ffi::OsString>,
) -> bool {
    arguments.into_iter().any(|arg| {
        arg.to_str().is_some_and(|arg| {
            arg == "--authorize-keychain" || arg.starts_with("--authorize-keychain=")
        })
    })
}

/// Runs in the calling executable: never execs a different identity to enroll it.
pub(crate) fn authorize_keychain_if_requested() {
    if !authorization_requested(std::env::args_os().skip(1)) {
        return;
    }
    let args = AuthorizationArgs::parse();
    let provider_slot = Arc::new(OnceLock::new());
    install_panic_hook(provider_slot.clone());
    if !args.user_data_dir.is_absolute() || args.user_data_dir.to_str().is_none() {
        std::process::exit(emit(Err(NativeAuthError::RuntimeUnavailable.into())));
    }
    // Enrollment does not need settings, a data directory, HTTP, Tokio, or State.
    // The required data-dir flag preserves explicit invocation intent but is not touched.
    let fallback_provider = provider_slot.clone();
    gpui_platform::headless().run(move |cx| {
        let version = env!("ZED_PKG_VERSION")
            .parse()
            .expect("compile-time version");
        release_channel::init(version, cx);
        let provider = match NativeCredentials::new(
            zed_credentials_provider::global(cx),
            &args.credential_namespace,
        ) {
            Ok(provider) => provider,
            Err(error) => std::process::exit(emit(Err(error.into()))),
        };
        if provider_slot.set(provider.clone()).is_err() {
            panic!("native credential provider initialized twice");
        }
        cx.spawn(async move |cx| {
            let result = provider
                .authorize_keychain(args.expected_identity.as_deref(), cx)
                .await
                .map(|authorization| Output::Authorization { authorization })
                .map_err(Into::into);
            complete(result, &provider, cx).await;
        })
        .detach();
    });
    // A normal completion exits explicitly, never via AppKit's zero-status quit.
    blocking_exit_with_barrier(
        emit(Err(NativeAuthError::RuntimeUnavailable.into())),
        &fallback_provider,
    );
}

pub(crate) fn run(usage: bool) {
    authorize_keychain_if_requested();
    // Parse before starting GPUI or accessing credentials. Usage has no login flag.
    let (directory, namespace, sign_in, expected) = if usage {
        let args = UsageArgs::parse();
        (
            args.user_data_dir,
            args.credential_namespace,
            false,
            Some(args.expected_identity),
        )
    } else {
        let args = SetupArgs::parse();
        (
            args.user_data_dir,
            args.credential_namespace,
            args.sign_in,
            None,
        )
    };
    let provider_slot = Arc::new(OnceLock::new());
    install_panic_hook(provider_slot.clone());
    if namespace != openai_subscribed::native_auth::FACTORY_CREDENTIAL_NAMESPACE {
        std::process::exit(emit(Err(NativeAuthError::InvalidNamespace.into())));
    }
    if !directory.is_absolute()
        || directory.to_str().is_none()
        || std::fs::create_dir_all(&directory).is_err()
    {
        std::process::exit(emit(Err(NativeAuthError::RuntimeUnavailable.into())));
    }
    let Ok(directory) = directory.canonicalize() else {
        std::process::exit(emit(Err(NativeAuthError::RuntimeUnavailable.into())));
    };
    let Some(directory) = directory.to_str() else {
        std::process::exit(emit(Err(NativeAuthError::RuntimeUnavailable.into())));
    };
    paths::set_custom_data_dir(directory);
    let fallback_provider = provider_slot.clone();
    gpui_platform::headless().run(move |cx| {
        let version = env!("ZED_PKG_VERSION")
            .parse()
            .expect("compile-time version");
        release_channel::init(version, cx);
        gpui_tokio::init(cx);
        let settings = settings::SettingsStore::new(cx, &settings::default_settings());
        cx.set_global(settings);
        let http: Arc<dyn http_client::HttpClient> =
            match reqwest_client::ReqwestClient::user_agent("Zed Native Credential Setup") {
                Ok(http) => Arc::new(http),
                Err(_) => {
                    std::process::exit(emit(Err(NativeAuthError::RuntimeUnavailable.into())));
                }
            };
        let credentials =
            match NativeCredentials::new(zed_credentials_provider::global(cx), &namespace) {
                Ok(credentials) => credentials,
                Err(error) => {
                    std::process::exit(emit(Err(error.into())));
                }
            };
        if provider_slot.set(credentials.clone()).is_err() {
            panic!("native credential provider initialized twice");
        }
        let completion_provider = credentials.clone();
        let state = cx.new(|cx| State::new_native(http, credentials.clone(), cx));
        let task: Task<Result<Output, NativeUsageError>> = cx.spawn(async move |cx| {
            if let Some(load) = state.read_with(cx, |state, _| state.load_task()) {
                load.await
                    .map_err(|_| NativeAuthError::RuntimeUnavailable)?;
            }
            if let Some(error) = credentials.load_error() {
                return Err(error.into());
            }
            if sign_in {
                credentials.begin_authorized_setup();
                state.update(cx, |state, cx| state.sign_in(cx));
                let deadline = std::time::Instant::now() + Duration::from_secs(180);
                while state.read_with(cx, |state, _| state.is_signing_in()) {
                    if std::time::Instant::now() >= deadline {
                        state.update(cx, |state, cx| state.cancel_sign_in(cx));
                        return Err(NativeAuthError::SignInFailed.into());
                    }
                    cx.background_executor()
                        .timer(Duration::from_millis(50))
                        .await;
                }
                if state.read_with(cx, |state, _| state.last_auth_error().is_some()) {
                    return Err(NativeAuthError::SignInFailed.into());
                }
            }
            if let Some(expected) = expected {
                let usage = state
                    .update(cx, |state, cx| state.read_native_usage(expected, cx))
                    .await?;
                Ok(Output::Usage { usage })
            } else {
                let identity_binding = state.read_with(cx, |state, _| state.native_identity())?;
                Ok(Output::Identity(IdentityStatus {
                    credential_namespace:
                        openai_subscribed::native_auth::FACTORY_CREDENTIAL_NAMESPACE,
                    identity_binding,
                    refresh_required: state
                        .read_with(cx, |state, _| state.native_refresh_required()),
                }))
            }
        });
        cx.spawn(async move |cx| {
            complete(task.await, &completion_provider, cx).await;
        })
        .detach();
    });
    blocking_exit_with_barrier(
        emit(Err(NativeAuthError::RuntimeUnavailable.into())),
        &fallback_provider,
    );
}

fn main() {
    run(false);
}

#[cfg(test)]
mod tests {
    use super::*;
    const NAMESPACE: &str = openai_subscribed::native_auth::FACTORY_CREDENTIAL_NAMESPACE;
    #[test]
    fn runtime_assets_load_without_native_startup() {
        use agent::Template as _;

        assert!(!settings::default_settings().is_empty());
        assert!(!settings::default_keymap().is_empty());
        assert!(!settings::default_semantic_token_rules().is_empty());
        let project = prompt_store::ProjectContext::default();
        let prompt = agent::SystemPromptTemplate {
            project: &project,
            available_tools: Vec::new(),
            model_name: None,
            date: "2026-10-16".into(),
            user_agents_md: None,
            sandboxing: false,
            is_linux: false,
            is_windows: false,
        };
        let rendered = prompt.render(&agent::Templates::new()).unwrap();
        assert!(rendered.contains("You are the Zed coding agent"));
    }

    #[test]
    fn login_is_explicit_and_usage_cannot_request_it() {
        let args = SetupArgs::try_parse_from([
            "setup",
            "--user-data-dir",
            "/fixture",
            "--credential-namespace",
            NAMESPACE,
        ])
        .unwrap();
        assert!(!args.sign_in);
        assert!(
            SetupArgs::try_parse_from([
                "setup",
                "--user-data-dir",
                "/fixture",
                "--credential-namespace",
                NAMESPACE,
                "--sign-in"
            ])
            .unwrap()
            .sign_in
        );
        assert!(
            UsageArgs::try_parse_from([
                "usage",
                "--user-data-dir",
                "/fixture",
                "--credential-namespace",
                NAMESPACE,
                "--expected-identity",
                "opaque",
                "--sign-in"
            ])
            .is_err()
        );
        assert!(
            UsageArgs::try_parse_from([
                "usage",
                "--user-data-dir",
                "/fixture",
                "--credential-namespace",
                NAMESPACE
            ])
            .is_err()
        );
        assert!(SetupArgs::try_parse_from(["setup", "--user-data-dir", "/fixture"]).is_err());
    }
    #[test]
    fn success_envelopes_keep_usage_and_only_opaque_identity() {
        let status = serde_json::to_value(Envelope::Ok {
            result: Output::Identity(IdentityStatus {
                credential_namespace: openai_subscribed::native_auth::FACTORY_CREDENTIAL_NAMESPACE,
                identity_binding: "opaque".into(),
                refresh_required: false,
            }),
        })
        .unwrap();
        assert_eq!(status["identity_binding"], "opaque");
        assert!(status.get("result").is_none());
        let usage = serde_json::to_value(Envelope::Ok {
            result: Output::Usage {
                usage: UsageSnapshot {
                    identity_binding: None,
                    plan_type: None,
                    rate_limit: None,
                    additional_rate_limits: None,
                    spend_control: None,
                    account_binding:
                        openai_subscribed::UsageAccountBinding::RequestScopedUnverifiedResponse,
                    observed_at_ms: 1,
                },
            },
        })
        .unwrap();
        assert_eq!(usage["status"], "ok");
        assert!(usage.get("usage").is_some());
        assert!(usage["usage"].get("identity_binding").is_none());
    }
    #[test]
    fn namespace_parser_accepts_only_public_alias() {
        for namespace in [
            "factory-openai",
            "https://factory.zed.native/openai-subscribed/v1",
            "arbitrary-key",
            "",
        ] {
            let valid = namespace == "factory-openai";
            assert_eq!(
                SetupArgs::try_parse_from([
                    "setup",
                    "--user-data-dir",
                    "/fixture",
                    "--credential-namespace",
                    namespace,
                ])
                .is_ok(),
                valid
            );
            assert_eq!(
                UsageArgs::try_parse_from([
                    "usage",
                    "--user-data-dir",
                    "/fixture",
                    "--credential-namespace",
                    namespace,
                    "--expected-identity",
                    "opaque",
                ])
                .is_ok(),
                valid
            );
        }
    }
    #[test]
    fn errors_are_fixed_json() {
        let text = serde_json::to_string(&Envelope::Error {
            error: NativeAuthError::CredentialLoadTimeout.into(),
        })
        .unwrap();
        assert_eq!(
            text,
            r#"{"status":"error","error":"credential_load_timeout"}"#
        );
    }
}

#[cfg(test)]
mod authorization_tests {
    use super::*;
    #[test]
    fn authorization_parser_is_explicit_strict_and_identity_pin_is_optional() {
        for executable in ["setup", "usage", "worker"] {
            let args = AuthorizationArgs::try_parse_from([
                executable,
                "--authorize-keychain",
                "--user-data-dir",
                "/fixture",
                "--credential-namespace",
                "factory-openai",
            ])
            .unwrap();
            assert!(args.authorize_keychain);
            assert!(args.expected_identity.is_none());
            assert!(
                AuthorizationArgs::try_parse_from([
                    executable,
                    "--authorize-keychain",
                    "--user-data-dir",
                    "/fixture",
                    "--credential-namespace",
                    "factory-openai",
                    "--expected-identity",
                    "opaque"
                ])
                .is_ok()
            );
            for forbidden in [
                "--sign-in",
                "--managed",
                "--unknown",
                "--capabilities",
                "--printenv",
            ] {
                assert!(
                    AuthorizationArgs::try_parse_from([
                        executable,
                        "--authorize-keychain",
                        "--user-data-dir",
                        "/fixture",
                        "--credential-namespace",
                        "factory-openai",
                        forbidden
                    ])
                    .is_err()
                );
            }
        }
        assert!(
            AuthorizationArgs::try_parse_from([
                "setup",
                "--authorize-keychain=false",
                "--user-data-dir",
                "/fixture",
                "--credential-namespace",
                "factory-openai"
            ])
            .is_err()
        );
        assert!(authorization_requested(["--authorize-keychain".into()]));
        assert!(authorization_requested([
            "--authorize-keychain=false".into()
        ]));
        assert!(!authorization_requested([
            "--authorize-keychain-copy".into()
        ]));
        assert!(!authorization_requested(["--sign-in".into()]));
        assert!(
            AuthorizationArgs::try_parse_from([
                "setup",
                "--user-data-dir",
                "/fixture",
                "--credential-namespace",
                "factory-openai"
            ])
            .is_err()
        );
    }
    #[test]
    fn authorization_success_does_not_claim_grant_settlement_or_usage() {
        let value = serde_json::to_value(Envelope::Ok {
            result: Output::Authorization {
                authorization: KeychainAuthorization {
                    credential_namespace: "factory-openai",
                    expected_identity_checked: true,
                },
            },
        })
        .unwrap();
        assert_eq!(
            value["authorization"]["credential_namespace"],
            "factory-openai"
        );
        assert_eq!(value["authorization"]["expected_identity_checked"], true);
        for field in [
            "usage",
            "identity_binding",
            "authenticated",
            "mutation_settled",
            "nonce",
            "access_token",
        ] {
            assert!(value.get(field).is_none());
            assert!(value["authorization"].get(field).is_none());
        }
    }
    #[test]
    fn exit_helper_refuses_pending_mutation() {
        exit_if_settled(1, true);
    }
    #[test]
    fn error_exit_fixture_child() {
        if std::env::var_os("NATIVE_AUTH_EXIT_FIXTURE").is_some() {
            exit_if_settled(emit(Err(NativeAuthError::RuntimeUnavailable.into())), false);
            panic!("settled exit must not return");
        }
    }
    #[test]
    fn error_envelope_exits_one_in_credential_free_subprocess() {
        let module = module_path!().split_once("::").unwrap().1;
        let child = format!("{module}::error_exit_fixture_child");
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &child, "--nocapture"])
            .env("NATIVE_AUTH_EXIT_FIXTURE", "1")
            .output()
            .unwrap();
        assert_eq!(result.status.code(), Some(1));
        let stdout = String::from_utf8(result.stdout).unwrap();
        let json = stdout
            .lines()
            .find_map(|line| line.find('{').map(|offset| &line[offset..]))
            .expect("error envelope");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(json).unwrap(),
            serde_json::json!({"status":"error", "error":"runtime_unavailable"})
        );
        assert!(result.stderr.is_empty());
    }
}
