use std::path::PathBuf;
use std::sync::Arc;

use client::{Client, ProxySettings, RefreshLlmTokenListener, UserStore};
use db::AppDatabase;
use extension::ExtensionHostProxy;
use fs::RealFs;
use gpui::http_client::read_proxy_from_env;
use gpui::{App, AppContext as _, Entity};
use gpui_tokio::Tokio;
use language::LanguageRegistry;
use language_extension::LspAccess;
use node_runtime::{NodeBinaryOptions, NodeRuntime};
use project::project_settings::ProjectSettings;
use prompt_store::PromptBuilder;
use release_channel::{AppCommitSha, AppVersion};
use reqwest_client::ReqwestClient;
use settings::{Settings, SettingsStore};
use util::ResultExt as _;

pub struct AgentCliAppState {
    pub languages: Arc<LanguageRegistry>,
    pub client: Arc<Client>,
    pub user_store: Entity<UserStore>,
    pub fs: Arc<dyn fs::Fs>,
    pub node_runtime: NodeRuntime,
}

pub fn init(cx: &mut App) -> Arc<AgentCliAppState> {
    init_with_providers(cx, false, |client, user_store, cx| {
        RefreshLlmTokenListener::register(client.clone(), user_store.clone(), cx);
        language_models::init(user_store.clone(), client.clone(), cx);
    })
}

pub fn init_subscription_only(
    cx: &mut App,
) -> (Arc<AgentCliAppState>, Entity<openai_subscribed::State>) {
    use language_model::{LanguageModelProviderState as _, LanguageModelRegistry};
    use language_models::provider::openai_subscribed::OpenAiSubscribedProvider;

    let mut state = None;
    let app = init_with_providers(cx, false, |client, _, cx| {
        let provider = Arc::new(OpenAiSubscribedProvider::new(
            client.http_client(),
            client.credentials_provider(),
            cx,
        ));
        state = provider.observable_entity();
        LanguageModelRegistry::global(cx).update(cx, |registry, cx| {
            registry.register_provider(provider, cx);
        });
    });
    (
        app,
        state.expect("subscription provider has observable state"),
    )
}

pub fn init_managed(
    cx: &mut App,
    namespace: &str,
) -> anyhow::Result<(Arc<AgentCliAppState>, Entity<openai_subscribed::State>)> {
    use language_model::{LanguageModelProviderState as _, LanguageModelRegistry};
    use language_models::provider::openai_subscribed::OpenAiSubscribedProvider;
    let mut state = None;
    let app = init_with_providers(cx, true, |client, _, cx| {
        match openai_subscribed::native_auth::NativeCredentials::new(
            client.credentials_provider(),
            namespace,
        ) {
            Ok(credentials) => {
                let provider = Arc::new(OpenAiSubscribedProvider::new_native(
                    client.http_client(),
                    credentials,
                    cx,
                ));
                state = provider.observable_entity().map(Ok);
                LanguageModelRegistry::global(cx)
                    .update(cx, |registry, cx| registry.register_provider(provider, cx));
            }
            Err(_) => state = Some(Err(anyhow::anyhow!("invalid native credential namespace"))),
        }
    });
    Ok((
        app,
        state.ok_or_else(|| anyhow::anyhow!("scoped state unavailable"))??,
    ))
}

fn init_with_providers(
    cx: &mut App,
    managed: bool,
    register: impl FnOnce(&Arc<Client>, &Entity<UserStore>, &mut App),
) -> Arc<AgentCliAppState> {
    let app_commit_sha = option_env!("ZED_COMMIT_SHA").map(|s| AppCommitSha::new(s.to_owned()));

    let app_version = AppVersion::load(
        env!("ZED_PKG_VERSION"),
        option_env!("ZED_BUILD_ID"),
        app_commit_sha,
    );

    release_channel::init(app_version.clone(), cx);
    gpui_tokio::init(cx);

    let settings_store = SettingsStore::new(cx, &settings::default_settings());
    cx.set_global(settings_store);
    theme_settings::init(theme::LoadThemes::JustBase, cx);

    let user_agent = format!(
        "Zed Agent CLI/{} ({}; {})",
        app_version,
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    let proxy_str = ProxySettings::get_global(cx).proxy.to_owned();
    let proxy_url = proxy_str
        .as_ref()
        .and_then(|input| input.parse().ok())
        .or_else(read_proxy_from_env);
    let http = {
        let _guard = Tokio::handle(cx).enter();
        ReqwestClient::proxy_and_user_agent(proxy_url, &user_agent)
            .expect("could not start HTTP client")
    };
    cx.set_http_client(Arc::new(http));

    let client = Client::production(cx);
    cx.set_http_client(client.http_client());

    let app_db = AppDatabase::new();
    cx.set_global(app_db);

    let git_binary_path = None;
    let fs: Arc<dyn fs::Fs> = if managed {
        managed_project_fs()
    } else {
        RealFs::new(git_binary_path, cx.background_executor().clone())
    };
    <dyn fs::Fs>::set_global(fs.clone(), cx);

    let mut languages = LanguageRegistry::new(cx.background_executor().clone());
    languages.set_language_server_download_dir(paths::languages_dir().clone());
    let languages = Arc::new(languages);

    let user_store = cx.new(|cx| UserStore::new(client.clone(), cx));

    if managed {
        language_model::init(cx);
        register(&client, &user_store, cx);
        prompt_store::init(cx);
        cx.set_global(acp_thread::HeadlessTerminal(true));
        return Arc::new(AgentCliAppState {
            languages,
            client,
            user_store,
            fs,
            node_runtime: NodeRuntime::unavailable(),
        });
    }

    extension::init(cx);

    let (mut node_options_tx, node_options_rx) = watch::channel(None);
    cx.observe_global::<SettingsStore>(move |cx| {
        let settings = &ProjectSettings::get_global(cx).node;
        let options = NodeBinaryOptions {
            allow_path_lookup: !settings.ignore_system_version,
            allow_binary_download: true,
            use_paths: settings.path.as_ref().map(|node_path| {
                let node_path = PathBuf::from(shellexpand::tilde(node_path).as_ref());
                let npm_path = settings
                    .npm_path
                    .as_ref()
                    .map(|path| PathBuf::from(shellexpand::tilde(&path).as_ref()));
                (
                    node_path.clone(),
                    npm_path.unwrap_or_else(|| {
                        let base_path = PathBuf::new();
                        node_path.parent().unwrap_or(&base_path).join("npm")
                    }),
                )
            }),
        };
        node_options_tx.send(Some(options)).log_err();
    })
    .detach();
    let node_runtime = NodeRuntime::new(client.http_client(), None, node_options_rx);

    let extension_host_proxy = ExtensionHostProxy::global(cx);
    debug_adapter_extension::init(extension_host_proxy.clone(), cx);
    language_extension::init(LspAccess::Noop, extension_host_proxy, languages.clone());
    language_model::init(cx);
    register(&client, &user_store, cx);
    languages::init(languages.clone(), fs.clone(), node_runtime.clone(), cx);
    prompt_store::init(cx);
    terminal_view::init(cx);

    // The eval CLI runs headless with no controlling TTY, so PTY allocation and
    // acquiring a controlling terminal fail with `ENOTTY`. Tell the agent to run
    // its terminal commands without a PTY (and non-interactively) instead.
    cx.set_global(acp_thread::HeadlessTerminal(true));

    let stdout_is_a_pty = false;
    let prompt_builder = PromptBuilder::load(fs.clone(), stdout_is_a_pty, cx);
    agent_ui::init(
        fs.clone(),
        prompt_builder,
        languages.clone(),
        true,
        true,
        cx,
    );

    Arc::new(AgentCliAppState {
        languages,
        client,
        user_store,
        fs,
        node_runtime,
    })
}

#[path = "managed_tool_project_fs.rs"]
mod managed_fs;

pub fn managed_project_fs() -> Arc<dyn fs::Fs> {
    managed_fs::new()
}
