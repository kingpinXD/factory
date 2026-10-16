use clap::Parser;
use gpui::{AppContext as _, Task};
use openai_subscribed::{
    State, UsageSnapshot,
    native_auth::{NativeAuthError, NativeCredentials, NativeUsageError},
};
use serde::Serialize;
use std::{
    io::Write,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicI32, Ordering},
    },
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
    #[arg(long)]
    sign_in: bool,
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
    Usage { usage: UsageSnapshot },
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
    if serde_json::to_writer(&mut writer, &envelope).is_err() || writer.write_all(b"\n").is_err() {
        return 1;
    }
    code
}

pub(crate) fn run(usage: bool) {
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
    std::panic::set_hook(Box::new(|_| {
        std::process::exit(emit(Err(NativeAuthError::RuntimeUnavailable.into())));
    }));
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
    let exit = Arc::new(AtomicI32::new(1));
    let completed = exit.clone();
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
                    completed.store(
                        emit(Err(NativeAuthError::RuntimeUnavailable.into())),
                        Ordering::SeqCst,
                    );
                    cx.quit();
                    return;
                }
            };
        let credentials =
            match NativeCredentials::new(zed_credentials_provider::global(cx), &namespace) {
                Ok(credentials) => credentials,
                Err(error) => {
                    completed.store(emit(Err(error.into())), Ordering::SeqCst);
                    cx.quit();
                    return;
                }
            };
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
            completed.store(emit(task.await), Ordering::SeqCst);
            cx.update(|cx| cx.quit());
        })
        .detach();
    });
    std::process::exit(exit.load(Ordering::SeqCst));
}

fn main() {
    run(false);
}

#[cfg(test)]
mod tests {
    use super::*;
    const NAMESPACE: &str = openai_subscribed::native_auth::FACTORY_CREDENTIAL_NAMESPACE;
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
