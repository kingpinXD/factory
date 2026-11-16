use super::{State, now_ms};
use futures::{AsyncReadExt as _, FutureExt as _};
use gpui::{Context, Task};
use http_client::{AsyncBody, HttpRequestExt as _, Method, RedirectPolicy, Request};
use serde::{Deserialize, Serialize};
use std::time::Duration;

const USAGE_URL: &str = "https://chatgpt.com/backend-api/wham/usage";
const USAGE_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_USAGE_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "category", rename_all = "snake_case")]
pub enum UsageError {
    MissingCredentials,
    CredentialReadFailed,
    MalformedCredentials,
    RefreshRequired,
    InvalidAuthHeaders,
    Transport,
    HttpStatus { status: u16 },
    Timeout,
    BodyTooLarge,
    MalformedResponse,
    AccountMismatch,
    AuthenticationChanged,
    StateUnavailable,
}

impl std::fmt::Display for UsageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingCredentials => f.write_str("missing_credentials"),
            Self::CredentialReadFailed => f.write_str("credential_read_failed"),
            Self::MalformedCredentials => f.write_str("malformed_credentials"),
            Self::RefreshRequired => f.write_str("refresh_required"),
            Self::InvalidAuthHeaders => f.write_str("invalid_auth_headers"),
            Self::Transport => f.write_str("transport"),
            Self::HttpStatus { status } => write!(f, "http_status ({status})"),
            Self::Timeout => f.write_str("timeout"),
            Self::BodyTooLarge => f.write_str("body_too_large"),
            Self::MalformedResponse => f.write_str("malformed_response"),
            Self::AccountMismatch => f.write_str("account_mismatch"),
            Self::AuthenticationChanged => f.write_str("authentication_changed"),
            Self::StateUnavailable => f.write_str("state_unavailable"),
        }
    }
}

impl std::error::Error for UsageError {}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UsageWindow {
    pub used_percent: f64,
    pub limit_window_seconds: u64,
    pub reset_after_seconds: u64,
    pub reset_at: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UsageLimit {
    pub allowed: bool,
    pub limit_reached: bool,
    pub primary_window: Option<UsageWindow>,
    pub secondary_window: Option<UsageWindow>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AdditionalUsageLimit {
    pub limit_name: String,
    pub metered_feature: String,
    pub rate_limit: UsageLimit,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UsageSpendControl {
    pub reached: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageAccountBinding {
    ResponseAccountMatched,
    RequestScopedUnverifiedResponse,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct UsageSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity_binding: Option<String>,
    pub plan_type: Option<String>,
    pub rate_limit: Option<UsageLimit>,
    pub additional_rate_limits: Option<Vec<AdditionalUsageLimit>>,
    pub spend_control: Option<UsageSpendControl>,
    pub account_binding: UsageAccountBinding,
    /// Local observation time, not the backend's data-update time.
    pub observed_at_ms: u64,
}

// Identity is used only for comparison; it is never part of the public snapshot.
#[derive(Deserialize)]
struct UsageResponse {
    account_id: Option<String>,
    plan_type: Option<String>,
    rate_limit: Option<UsageLimit>,
    additional_rate_limits: Option<Vec<AdditionalUsageLimit>>,
    spend_control: Option<UsageSpendControl>,
}

impl UsageLimit {
    fn is_valid(&self) -> bool {
        [&self.primary_window, &self.secondary_window]
            .into_iter()
            .flatten()
            .all(|window| {
                window.used_percent.is_finite()
                    && (0.0..=100.0).contains(&window.used_percent)
                    && window.limit_window_seconds > 0
            })
    }
}

fn parse_usage(body: &[u8], account_id: Option<&str>) -> Result<UsageSnapshot, UsageError> {
    let response: UsageResponse =
        serde_json::from_slice(body).map_err(|_| UsageError::MalformedResponse)?;
    if response
        .rate_limit
        .as_ref()
        .is_some_and(|limit| !limit.is_valid())
        || response
            .additional_rate_limits
            .as_ref()
            .is_some_and(|limits| {
                limits.iter().any(|limit| {
                    limit.limit_name.is_empty()
                        || limit.metered_feature.is_empty()
                        || !limit.rate_limit.is_valid()
                })
            })
    {
        return Err(UsageError::MalformedResponse);
    }
    let account_binding = match (account_id, response.account_id.as_deref()) {
        (Some(request), Some(response)) if request != response => {
            return Err(UsageError::AccountMismatch);
        }
        (Some(_), Some(_)) => UsageAccountBinding::ResponseAccountMatched,
        _ => UsageAccountBinding::RequestScopedUnverifiedResponse,
    };
    Ok(UsageSnapshot {
        identity_binding: None,
        plan_type: response.plan_type,
        rate_limit: response.rate_limit,
        additional_rate_limits: response.additional_rate_limits,
        spend_control: response.spend_control,
        account_binding,
        observed_at_ms: now_ms(),
    })
}

impl State {
    /// Reads usage once using fresh native credentials, without refresh or retries.
    /// Waiting for the native credential load is part of this task.
    pub fn read_usage(&self, cx: &mut Context<Self>) -> Task<Result<UsageSnapshot, UsageError>> {
        let load_task = self.load_task();
        let http_client = self.http_client.clone();
        cx.spawn(async move |this, cx| {
            if let Some(load_task) = load_task {
                load_task.await.map_err(|_| UsageError::StateUnavailable)?;
            }
            let (credentials, generation) = this
                .read_with(cx, |state, _| {
                    if let Some(error) = state.usage_load_error {
                        return Err(error);
                    }
                    let credentials = state
                        .credentials
                        .as_ref()
                        .ok_or(UsageError::MissingCredentials)?;
                    if credentials.is_expired() {
                        return Err(UsageError::RefreshRequired);
                    }
                    Ok((credentials.clone(), state.auth_generation))
                })
                .map_err(|_| UsageError::StateUnavailable)??;

            let mut authorization = http_client::http::HeaderValue::from_str(&format!(
                "Bearer {}",
                credentials.access_token
            ))
            .map_err(|_| UsageError::InvalidAuthHeaders)?;
            authorization.set_sensitive(true);
            let mut request = Request::builder()
                .method(Method::GET)
                .uri(USAGE_URL)
                .header("Authorization", authorization)
                .follow_redirects(RedirectPolicy::NoFollow)
                .timeout(USAGE_TIMEOUT);
            if let Some(account_id) = &credentials.account_id {
                let mut account = http_client::http::HeaderValue::from_str(account_id)
                    .map_err(|_| UsageError::InvalidAuthHeaders)?;
                account.set_sensitive(true);
                request = request.header("ChatGPT-Account-Id", account);
            }
            let request = request
                .body(AsyncBody::empty())
                .map_err(|_| UsageError::InvalidAuthHeaders)?;
            let read = async {
                let response = http_client
                    .send(request)
                    .await
                    .map_err(|_| UsageError::Transport)?;
                if !response.status().is_success() {
                    return Err(UsageError::HttpStatus {
                        status: response.status().as_u16(),
                    });
                }
                let mut body = Vec::new();
                response
                    .into_body()
                    .take((MAX_USAGE_BYTES + 1) as u64)
                    .read_to_end(&mut body)
                    .await
                    .map_err(|_| UsageError::Transport)?;
                if body.len() > MAX_USAGE_BYTES {
                    return Err(UsageError::BodyTooLarge);
                }
                parse_usage(&body, credentials.account_id.as_deref())
            };
            let deadline = cx.background_executor().timer(USAGE_TIMEOUT);
            let snapshot = futures::select_biased! {
                () = deadline.fuse() => Err(UsageError::Timeout),
                result = read.fuse() => result,
            }?;
            this.read_with(cx, |state, _| {
                if state.auth_generation != generation
                    || state
                        .credentials
                        .as_ref()
                        .map(|credentials| &credentials.account_id)
                        != Some(&credentials.account_id)
                {
                    return Err(UsageError::AuthenticationChanged);
                }
                Ok(snapshot)
            })
            .map_err(|_| UsageError::StateUnavailable)?
        })
    }
}

#[cfg(test)]
pub(crate) mod tests;
