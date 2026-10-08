//! An authenticated, rate-limited transport for the Spotify Web API.
//!
//! Typed calls share one `request` helper that adds the bearer token, limits
//! concurrency, honors `Retry-After`, and formats API errors. The gateway
//! handles capability differences before dispatch.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use reqwest::{Method, StatusCode};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::Semaphore;

use super::ApiSource;
use super::models::*;
use crate::http::Http;
use crate::telemetry;

const BASE_URL: &str = "https://api.spotify.com/v1";
const MAX_IN_FLIGHT: usize = 6;
const RATE_LIMIT_RETRIES: u32 = 3;
const MAX_RETRY_AFTER: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Error)]
pub enum ApiError {
    #[error("not signed in")]
    NotSignedIn,
    #[error("{message}")]
    Status { status: u16, message: String },
    #[error("Spotify is rate limiting requests; try again in a moment")]
    RateLimited,
    #[error("Spotify's Development Mode quota is exhausted; try again after the quota resets")]
    QuotaExhausted,
    #[error("your Spotify sign-in expired; please sign in again")]
    SignInExpired { api_source: ApiSource },
    #[error("network error: {0}")]
    Network(String),
    #[error("unexpected response from Spotify: {0}")]
    Decode(String),
}

impl ApiError {
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Status { status, .. } => Some(*status),
            _ => None,
        }
    }
}

impl From<reqwest::Error> for ApiError {
    fn from(error: reqwest::Error) -> Self {
        if error.is_decode() {
            Self::Decode(error.to_string())
        } else {
            Self::Network(error.to_string())
        }
    }
}

pub type Result<T> = std::result::Result<T, ApiError>;

fn is_quota_exhausted(body: &str) -> bool {
    serde_json::from_str::<ApiErrorBody>(body)
        .ok()
        .and_then(|body| body.error.reason)
        .is_some_and(|reason| reason == "QUOTA_EXCEEDED")
}

/// Where bearer tokens come from.
///
/// The Web API is driven by a registered application's PKCE grant, refreshed
/// on demand and persisted so the browser is needed once per machine. Tokens
/// minted for Spotify's own desktop client are throttled on the Web API, so
/// they are never used here. `Fixed` exists only for tests.
#[derive(Clone)]
pub enum TokenProvider {
    Web(std::sync::Arc<WebTokens>),
}

impl TokenProvider {
    /// `lock_wait` receives how long the request queued for the token lock.
    async fn access_token(&self, lock_wait: &mut Duration) -> Result<String> {
        match self {
            Self::Web(tokens) => tokens.access_token_timed(false, lock_wait).await,
        }
    }

    async fn invalidate(&self) {
        let Self::Web(tokens) = self;
        let _ = tokens.access_token(true).await;
    }
}

/// The Web API grant, refreshed and persisted as it ages.
pub struct WebTokens {
    http: Http,
    token: tokio::sync::Mutex<crate::auth::StoredToken>,
    lease: crate::credentials::Lease,
    remember: std::sync::atomic::AtomicBool,
    storage_error: std::sync::Arc<dyn Fn(crate::credentials::Error) + Send + Sync>,
    source: ApiSource,
}

impl WebTokens {
    pub fn new(
        http: impl Into<Http>,
        token: crate::auth::StoredToken,
        lease: crate::credentials::Lease,
        source: ApiSource,
        storage_error: std::sync::Arc<dyn Fn(crate::credentials::Error) + Send + Sync>,
    ) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            http: http.into(),
            token: tokio::sync::Mutex::new(token),
            lease,
            remember: std::sync::atomic::AtomicBool::new(false),
            storage_error,
            source,
        })
    }

    /// Start persistence only after the Web API has verified the account.
    pub async fn remember(&self) -> std::result::Result<(), crate::credentials::Error> {
        let token = self.token.lock().await;
        self.remember
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let pending = self
            .lease
            .save(crate::credentials::Grant::Web(token.clone()));
        drop(token);
        pending.await
    }

    /// A valid access token, refreshing first when it is close to expiry or
    /// `force` asks for a fresh one after a 401.
    async fn access_token(&self, force: bool) -> Result<String> {
        let mut lock_wait = Duration::ZERO;
        self.access_token_timed(force, &mut lock_wait).await
    }

    async fn access_token_timed(&self, force: bool, lock_wait: &mut Duration) -> Result<String> {
        let asked = Instant::now();
        let mut guard = self.token.lock().await;
        *lock_wait = asked.elapsed();
        if !self.lease.current() {
            return Err(ApiError::SignInExpired {
                api_source: self.source,
            });
        }
        if force || guard.needs_refresh() {
            let refresh = RefreshProbe::new(
                self.source,
                force,
                &guard,
                *lock_wait,
                self.remember.load(std::sync::atomic::Ordering::Relaxed),
            );
            let client_id = guard.client_id.clone();
            let refresh_token = guard.refresh_token.clone();
            let http = match self.http.client() {
                Ok(http) => http,
                Err(error) => {
                    refresh.end("client_unavailable", None, None);
                    return Err(ApiError::Network(error));
                }
            };
            let requested = Instant::now();
            let answer = crate::auth::refresh(&http, &client_id, &refresh_token).await;
            let refresh = refresh.answered(requested.elapsed());
            match answer {
                Ok(response) => match crate::auth::StoredToken::from_response(
                    &client_id,
                    response,
                    Some(&refresh_token),
                ) {
                    Ok(updated) => {
                        if !self.lease.current() {
                            refresh.end("lease_stale", None, None);
                            return Err(ApiError::SignInExpired {
                                api_source: self.source,
                            });
                        }
                        if self.remember.load(std::sync::atomic::Ordering::Relaxed) {
                            let pending = self
                                .lease
                                .save(crate::credentials::Grant::Web(updated.clone()));
                            let notice = self.storage_error.clone();
                            let lease = self.lease.clone();
                            tokio::spawn(async move {
                                if let Err(error) = pending.await
                                    && lease.current()
                                {
                                    notice(error);
                                }
                            });
                        }
                        *guard = updated;
                        refresh.end("ok", None, None);
                    }
                    Err(error) => {
                        refresh.end("unusable_response", None, Some(&error.to_string()));
                        log::warn!("token refresh returned an unusable response: {error}")
                    }
                },
                Err(crate::auth::TokenEndpointError::Rejected { status, .. }) => {
                    refresh.end("rejected", Some(status), None);
                    return Err(ApiError::SignInExpired {
                        api_source: self.source,
                    });
                }
                Err(crate::auth::TokenEndpointError::Unreachable(detail)) => {
                    if force || guard.expired() {
                        refresh.end("unreachable", None, Some(&detail));
                        return Err(ApiError::Network(detail));
                    }
                    refresh.end("unreachable_kept_token", None, Some(&detail));
                    log::warn!("token refresh failed, using the current token: {detail}");
                }
            }
        }
        Ok(guard.access_token.clone())
    }
}

/// One token refresh as telemetry sees it. Never holds token material.
struct RefreshProbe {
    source: ApiSource,
    reason: &'static str,
    lock_wait: Duration,
    remaining_s: i64,
    since_last_ms: Option<u64>,
    persisted: bool,
    took: Option<Duration>,
}

impl RefreshProbe {
    fn new(
        source: ApiSource,
        force: bool,
        token: &crate::auth::StoredToken,
        lock_wait: Duration,
        persisted: bool,
    ) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_secs());
        let last = LAST_REFRESH_MS[grant_index(source)].load(Ordering::Relaxed);
        Self {
            source,
            reason: if force {
                "forced_401"
            } else if token.expired() {
                "expired"
            } else {
                "expiring"
            },
            lock_wait,
            remaining_s: token.expires_at as i64 - now as i64,
            since_last_ms: (last != 0).then(|| clock_ms().saturating_sub(last)),
            persisted,
            took: None,
        }
    }

    fn answered(mut self, took: Duration) -> Self {
        self.took = Some(took);
        self
    }

    fn end(&self, outcome: &'static str, status: Option<u16>, detail: Option<&str>) {
        LAST_REFRESH_MS[grant_index(self.source)].store(clock_ms(), Ordering::Relaxed);
        meters::REFRESHES.incr();
        if outcome != "ok" {
            meters::REFRESH_FAILURES.incr();
        }
        // A forced refresh right after another one: concurrent 401s each
        // asking for a token that was already renewed.
        let redundant =
            self.reason == "forced_401" && self.since_last_ms.is_some_and(|since| since < 5_000);
        if redundant {
            meters::REDUNDANT_REFRESHES.incr();
        }
        let record = telemetry::event("auth.refresh")
            .field("grant", grant_name(self.source))
            .field("reason", self.reason)
            .field("outcome", outcome)
            .field("status", status)
            .field("ms", self.took.map(telemetry::duration_ms))
            .ms("lock_wait_ms", self.lock_wait)
            .field("remaining_s_before", self.remaining_s)
            .field("since_last_refresh_ms", self.since_last_ms)
            .field("persisted", self.persisted)
            .field("redundant", redundant);
        match detail {
            Some(detail) => record.text("detail", detail).emit(),
            None => record.emit(),
        }
    }
}

/// What to start playing.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PlayRequest {
    pub context_uri: Option<String>,
    pub uris: Vec<String>,
    pub offset_uri: Option<String>,
    pub offset_position: Option<u32>,
    pub position_ms: u32,
}

impl PlayRequest {
    pub fn context(uri: impl Into<String>) -> Self {
        Self {
            context_uri: Some(uri.into()),
            ..Self::default()
        }
    }

    pub fn tracks(uris: Vec<String>) -> Self {
        Self {
            uris,
            ..Self::default()
        }
    }

    pub fn starting_at_uri(mut self, uri: impl Into<String>) -> Self {
        self.offset_uri = Some(uri.into());
        self
    }

    pub fn starting_at_index(mut self, index: u32) -> Self {
        self.offset_position = Some(index);
        self
    }

    fn body(&self) -> Value {
        let mut body = serde_json::Map::new();
        // The Web API plays a lone track or episode only as `uris`; as a
        // `context_uri` it answers 400 "Non supported context uri". (librespot
        // takes a track as a context, so local playback keeps that form.)
        let single_item = self
            .context_uri
            .as_deref()
            .filter(|uri| matches!(crate::util::uri_kind(uri), Some("track" | "episode")))
            // A resume can name a later song than the context's (autoplay moved
            // on): the named song is the one to play.
            .map(|context| self.offset_uri.as_deref().unwrap_or(context));
        if let Some(item) = single_item {
            body.insert("uris".into(), json!([item]));
        } else if let Some(context) = &self.context_uri {
            body.insert("context_uri".into(), json!(context));
        } else if !self.uris.is_empty() {
            body.insert("uris".into(), json!(self.uris));
        }
        // A single item is the start: an offset naming it adds nothing.
        let offset = single_item.is_none();
        if let Some(uri) = self.offset_uri.as_ref().filter(|_| offset) {
            body.insert("offset".into(), json!({ "uri": uri }));
        } else if let Some(position) = self.offset_position.filter(|_| offset) {
            body.insert("offset".into(), json!({ "position": position }));
        }
        if self.position_ms > 0 {
            body.insert("position_ms".into(), json!(self.position_ms));
        }
        Value::Object(body)
    }
}

/// Live view of the client's traffic, shared with the interface so it can
/// show that the app is talking to Spotify rather than being slow itself.
pub struct NetActivity {
    started_at: Instant,
    in_flight: AtomicUsize,
    /// Milliseconds since `started_at` when the oldest current burst began.
    busy_since_ms: AtomicU64,
}

impl Default for NetActivity {
    fn default() -> Self {
        Self {
            started_at: Instant::now(),
            in_flight: AtomicUsize::new(0),
            busy_since_ms: AtomicU64::new(0),
        }
    }
}

impl NetActivity {
    fn now_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }

    fn begin(&self) {
        if self.in_flight.fetch_add(1, Ordering::SeqCst) == 0 {
            self.busy_since_ms.store(self.now_ms(), Ordering::SeqCst);
        }
    }

    fn end(&self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }

    /// Requests have been in flight continuously for at least `for_at_least`.
    pub fn busy(&self, for_at_least: Duration) -> bool {
        self.in_flight.load(Ordering::SeqCst) > 0
            && self
                .now_ms()
                .saturating_sub(self.busy_since_ms.load(Ordering::SeqCst))
                >= for_at_least.as_millis() as u64
    }
}

/// Decrements the in-flight count even if the request future is dropped.
struct ActivityGuard<'a>(&'a NetActivity);

impl Drop for ActivityGuard<'_> {
    fn drop(&mut self) {
        self.0.end();
    }
}

pub struct ApiClient {
    #[cfg(test)]
    base_url: Option<String>,
    http: Http,
    tokens: Mutex<Option<TokenProvider>>,
    limiter: Semaphore,
    queue_writes: tokio::sync::Mutex<()>,
    cooldown_until: tokio::sync::Mutex<Instant>,
    search_limit: u32,
    artist_albums_limit: u32,
    source: ApiSource,
    activity: Arc<NetActivity>,
    probe: ClientProbe,
}

impl ApiClient {
    pub fn new(
        http: impl Into<Http>,
        activity: Arc<NetActivity>,
        search_limit: u32,
        artist_albums_limit: u32,
        source: ApiSource,
    ) -> Self {
        register_meters();
        Self {
            #[cfg(test)]
            base_url: None,
            http: http.into(),
            tokens: Mutex::new(None),
            limiter: Semaphore::new(MAX_IN_FLIGHT),
            queue_writes: tokio::sync::Mutex::new(()),
            cooldown_until: tokio::sync::Mutex::new(Instant::now()),
            search_limit,
            artist_albums_limit,
            source,
            activity,
            probe: ClientProbe::new(),
        }
    }

    /// Which instance this is, for telemetry: a new authorization gets a new
    /// client, and with it a fresh cooldown.
    pub(crate) fn telemetry_generation(&self) -> u64 {
        self.probe.generation
    }

    /// The cooldown left, read without its lock.
    fn cooldown_remaining_ms(&self) -> u64 {
        self.probe
            .cooldown_until_ms
            .load(Ordering::Relaxed)
            .saturating_sub(clock_ms())
    }

    /// Reports the end of a rate-limit episode once an answer arrives after
    /// the cooldown has run out.
    fn end_cooldown_episode(&self, status: u16) {
        let probe = &self.probe;
        if probe.episode_started_ms.load(Ordering::Relaxed) == 0 || self.cooldown_remaining_ms() > 0
        {
            return;
        }
        let started = probe.episode_started_ms.swap(0, Ordering::Relaxed);
        if started == 0 {
            return;
        }
        telemetry::event("api.cooldown")
            .field("phase", "end")
            .field("grant", grant_name(self.source))
            .field("duration_ms", clock_ms().saturating_sub(started))
            .field("next_status", status)
            .field(
                "limited_in_episode",
                probe.episode_limited.swap(0, Ordering::Relaxed),
            )
            .field(
                "requests_waited",
                probe.episode_waited.swap(0, Ordering::Relaxed),
            )
            .field("client_generation", probe.generation)
            .emit();
    }

    pub fn set_token_provider(&self, provider: Option<TokenProvider>) {
        *self.tokens.lock().unwrap_or_else(|p| p.into_inner()) = provider;
    }

    /// A new authorization gets its own provider and request cooldown. An old
    /// request must never pick up a replacement account's credentials.
    pub fn for_authorization(&self, provider: TokenProvider) -> Self {
        let client = Self::new(
            self.http.clone(),
            self.activity.clone(),
            self.search_limit,
            self.artist_albums_limit,
            self.source,
        );
        client.set_token_provider(Some(provider));
        client
    }

    fn provider(&self) -> Result<TokenProvider> {
        self.tokens
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
            .ok_or(ApiError::NotSignedIn)
    }

    async fn wait_for_cooldown(&self) {
        loop {
            let until = *self.cooldown_until.lock().await;
            let Some(wait) = until.checked_duration_since(Instant::now()) else {
                return;
            };
            tokio::time::sleep(wait).await;
        }
    }

    async fn extend_cooldown(&self, wait: Duration) {
        let mut until = self.cooldown_until.lock().await;
        *until = (*until).max(Instant::now() + wait);
        self.probe
            .cooldown_until_ms
            .store(clock_at(*until), Ordering::Relaxed);
    }

    // ---- transport -------------------------------------------------------

    async fn send(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<&Value>,
    ) -> Result<String> {
        self.send_body(method, path, query, body, None).await
    }

    async fn send_body(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<&Value>,
        jpeg: Option<&str>,
    ) -> Result<String> {
        let url = if path.starts_with("http") {
            path.to_string()
        } else {
            #[cfg(test)]
            let base = self.base_url.as_deref().unwrap_or(BASE_URL);
            #[cfg(not(test))]
            let base = BASE_URL;
            format!("{base}{path}")
        };
        let provider = self.provider()?;
        let started = Instant::now();
        // This is one logical request even when it waits for another request
        // or for a Retry-After cooldown. Keep the interface's activity signal
        // alive for that whole wait, not only while bytes are on the wire.
        self.activity.begin();
        let _activity = ActivityGuard(&self.activity);

        let mut attempt = 0;
        let queue_write = method == Method::POST && path == "/me/player/queue";
        // Telemetry only: one `api.request` per attempt, and one if this
        // future is dropped before it answers.
        let mut probe = RequestProbe::new(self, &method, path, query, body, jpeg, queue_write);
        loop {
            attempt = u32::saturating_add(attempt, 1);
            probe.begin(attempt);
            self.wait_for_cooldown().await;
            probe.cooled();
            let waiting = Waiting::new(&self.probe.permit_waiters);
            let permit = self
                .limiter
                .acquire()
                .await
                .map_err(|_| ApiError::NotSignedIn)?;
            drop(waiting);
            probe.permitted();
            let mut token_lock = Duration::ZERO;
            let token = match provider.access_token(&mut token_lock).await {
                Ok(token) => token,
                Err(error) => {
                    probe.tokened(token_lock);
                    probe.finish("token_failed", None, None, |event| {
                        event.text("error", &error.to_string())
                    });
                    return Err(error);
                }
            };
            probe.tokened(token_lock);
            let http = match self.http.client() {
                Ok(http) => http,
                Err(error) => {
                    probe.finish("client_unavailable", None, None, |event| event);
                    return Err(ApiError::Network(error));
                }
            };
            let mut request = http
                .request(method.clone(), &url)
                .bearer_auth(&token)
                .query(query);
            if let Some(jpeg) = jpeg {
                request = request
                    .header(reqwest::header::CONTENT_TYPE, "image/jpeg")
                    .body(jpeg.to_owned());
            } else if let Some(body) = body {
                request = request.json(body);
            } else if matches!(method, Method::PUT | Method::POST | Method::DELETE) {
                request = request.header(reqwest::header::CONTENT_LENGTH, "0");
            }
            probe.sending();
            let response = match request.send().await {
                Ok(response) => response,
                Err(error) => {
                    probe.failed(None, &error);
                    return Err(error.into());
                }
            };
            let status = response.status();
            probe.answered(&response);

            if status == StatusCode::UNAUTHORIZED && attempt == 1 {
                drop(permit);
                probe.report("unauthorized_retry", Some(401), None, |event| event);
                probe.enter("invalidate");
                provider.invalidate().await;
                continue;
            }
            if status == StatusCode::TOO_MANY_REQUESTS {
                let wait = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok())
                    .map_or(Duration::from_secs(1), Duration::from_secs);
                let text = response.text().await.unwrap_or_default();
                if is_quota_exhausted(&text) {
                    probe.rate_limited(wait, None, &text, 0, false);
                    probe.finished = true;
                    return Err(ApiError::QuotaExhausted);
                }
                let asked = wait;
                // A rejected queue append is safe to retry. Keep its place
                // in the write lock and honor the full server-requested wait.
                // Other requests retain their existing bounded retry policy.
                let wait = if queue_write {
                    wait
                } else {
                    wait.min(MAX_RETRY_AFTER)
                };
                log::warn!("Spotify rate limit source={} wait={wait:?}", self.source);
                log::info!(
                    "Spotify cooldown source={} duration_ms={}",
                    self.source,
                    wait.as_millis()
                );
                drop(permit);
                probe.enter("cooldown_extend");
                let before = self.cooldown_remaining_ms();
                self.extend_cooldown(wait).await;
                let gave_up = !queue_write && attempt > RATE_LIMIT_RETRIES;
                probe.rate_limited(asked, Some(wait), &text, before, gave_up);
                if !queue_write && attempt > RATE_LIMIT_RETRIES {
                    probe.finished = true;
                    return Err(ApiError::RateLimited);
                }
                continue;
            }
            if status.is_server_error() && method == Method::GET && attempt == 1 {
                drop(permit);
                probe.report("server_error_retry", Some(status.as_u16()), None, |event| {
                    event
                });
                probe.enter("retry_sleep");
                tokio::time::sleep(Duration::from_millis(800)).await;
                continue;
            }
            let text = match response.text().await {
                Ok(text) => text,
                Err(error) => {
                    probe.failed(Some(status.as_u16()), &error);
                    return Err(error.into());
                }
            };
            log::debug!(
                "Spotify request source={} method={} status={} duration_ms={}",
                self.source,
                method,
                status.as_u16(),
                started.elapsed().as_millis()
            );
            if status.is_success() {
                probe.finish("ok", Some(status.as_u16()), Some(text.len()), |event| event);
                return Ok(text);
            }
            let message = serde_json::from_str::<ApiErrorBody>(&text)
                .ok()
                .map(|body| body.error.message)
                .filter(|message| !message.is_empty())
                .unwrap_or_else(|| {
                    status
                        .canonical_reason()
                        .unwrap_or("request failed")
                        .to_string()
                });
            probe.finish("status", Some(status.as_u16()), Some(text.len()), |event| {
                event
                    .field_with("reason", || error_reason(&text))
                    .text("message", &message)
            });
            return Err(ApiError::Status {
                status: status.as_u16(),
                message,
            });
        }
    }

    async fn get<T: DeserializeOwned>(&self, path: &str, query: &[(&str, String)]) -> Result<T> {
        let text = self.send(Method::GET, path, query, None).await?;
        if text.trim().is_empty() {
            return serde_json::from_value(Value::Null)
                .map_err(|error| ApiError::Decode(error.to_string()));
        }
        serde_json::from_str(&text).map_err(|error| ApiError::Decode(error.to_string()))
    }

    async fn get_optional<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<Option<T>> {
        // Spotify answers 204 with no body when nothing is playing.
        let text = self.send(Method::GET, path, query, None).await?;
        if text.trim().is_empty() || text.trim() == "null" {
            return Ok(None);
        }
        serde_json::from_str(&text)
            .map(Some)
            .map_err(|error| ApiError::Decode(error.to_string()))
    }

    /// Performs a change. The status decides success; the body is only
    /// consulted where a caller needs something from it, because Spotify's
    /// replies to player commands are not reliably JSON and contain nothing
    /// this client uses. Treating an unparseable body as failure told people
    /// their music had not started while it was already playing.
    async fn write(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<&Value>,
    ) -> Result<Option<Value>> {
        let text = self.send(method, path, query, body).await?;
        if text.trim().is_empty() {
            return Ok(None);
        }
        match serde_json::from_str(&text) {
            Ok(value) => Ok(Some(value)),
            Err(error) => {
                log::debug!(
                    "Spotify write source={} returned a non-JSON success body: {error}",
                    self.source
                );
                Ok(None)
            }
        }
    }

    // ---- identity and player ---------------------------------------------

    pub async fn me(&self) -> Result<User> {
        self.get("/me", &[]).await
    }

    pub async fn devices(&self) -> Result<Vec<Device>> {
        let list: DeviceList = self.get("/me/player/devices", &[]).await?;
        Ok(list.devices)
    }

    pub async fn playback_state(&self) -> Result<Option<PlaybackState>> {
        self.get_optional(
            "/me/player",
            &[("additional_types", "track,episode".to_string())],
        )
        .await
    }

    pub async fn queue(&self) -> Result<Queue> {
        self.get("/me/player/queue", &[]).await
    }

    pub async fn recently_played(
        &self,
        limit: u32,
        after: Option<&str>,
        before: Option<&str>,
    ) -> Result<CursorPage<PlayHistory>> {
        let mut query = vec![("limit", limit.to_string())];
        if let Some(after) = after {
            query.push(("after", after.to_string()));
        }
        if let Some(before) = before {
            query.push(("before", before.to_string()));
        }
        self.get("/me/player/recently-played", &query).await
    }

    fn device_query(device_id: Option<&str>) -> Vec<(&'static str, String)> {
        device_id
            .map(|id| vec![("device_id", id.to_string())])
            .unwrap_or_default()
    }

    pub async fn play(&self, device_id: Option<&str>, request: Option<&PlayRequest>) -> Result<()> {
        let body = request.map(PlayRequest::body);
        self.write(
            Method::PUT,
            "/me/player/play",
            &Self::device_query(device_id),
            body.as_ref(),
        )
        .await?;
        Ok(())
    }

    pub async fn pause(&self, device_id: Option<&str>) -> Result<()> {
        self.write(
            Method::PUT,
            "/me/player/pause",
            &Self::device_query(device_id),
            None,
        )
        .await?;
        Ok(())
    }

    pub async fn next(&self, device_id: Option<&str>) -> Result<()> {
        self.write(
            Method::POST,
            "/me/player/next",
            &Self::device_query(device_id),
            None,
        )
        .await?;
        Ok(())
    }

    pub async fn previous(&self, device_id: Option<&str>) -> Result<()> {
        self.write(
            Method::POST,
            "/me/player/previous",
            &Self::device_query(device_id),
            None,
        )
        .await?;
        Ok(())
    }

    pub async fn seek(&self, position_ms: u32, device_id: Option<&str>) -> Result<()> {
        let mut query = Self::device_query(device_id);
        query.push(("position_ms", position_ms.to_string()));
        self.write(Method::PUT, "/me/player/seek", &query, None)
            .await?;
        Ok(())
    }

    pub async fn set_volume(&self, percent: u8, device_id: Option<&str>) -> Result<()> {
        let mut query = Self::device_query(device_id);
        query.push(("volume_percent", percent.min(100).to_string()));
        self.write(Method::PUT, "/me/player/volume", &query, None)
            .await?;
        Ok(())
    }

    pub async fn set_shuffle(&self, state: bool, device_id: Option<&str>) -> Result<()> {
        let mut query = Self::device_query(device_id);
        query.push(("state", state.to_string()));
        self.write(Method::PUT, "/me/player/shuffle", &query, None)
            .await?;
        Ok(())
    }

    /// `state` is `off`, `context`, or `track`.
    pub async fn set_repeat(&self, state: &str, device_id: Option<&str>) -> Result<()> {
        let mut query = Self::device_query(device_id);
        query.push(("state", state.to_string()));
        self.write(Method::PUT, "/me/player/repeat", &query, None)
            .await?;
        Ok(())
    }

    pub async fn transfer(&self, device_id: &str, play: bool) -> Result<()> {
        let body = json!({ "device_ids": [device_id], "play": play });
        self.write(Method::PUT, "/me/player", &[], Some(&body))
            .await?;
        Ok(())
    }

    pub async fn add_to_queue(&self, uri: &str, device_id: Option<&str>) -> Result<()> {
        let _write = self.queue_writes.lock().await;
        self.append_to_queue(uri, device_id).await
    }

    async fn append_to_queue(&self, uri: &str, device_id: Option<&str>) -> Result<()> {
        let mut query = Self::device_query(device_id);
        query.push(("uri", uri.to_string()));
        self.write(Method::POST, "/me/player/queue", &query, None)
            .await?;
        Ok(())
    }

    /// Spotify appends one song per request. Await each write to keep an
    /// album's order, including repeated tracks, and stop on the first error.
    pub async fn add_many_to_queue(
        &self,
        uris: &[String],
        device_id: Option<&str>,
    ) -> (usize, Result<()>) {
        let _write = self.queue_writes.lock().await;
        for (added, uri) in uris.iter().enumerate() {
            if let Err(error) = self.append_to_queue(uri, device_id).await {
                return (added, Err(error));
            }
        }
        (uris.len(), Ok(()))
    }

    // ---- playlists ---------------------------------------------------------

    pub async fn my_playlists(&self, offset: u32, limit: u32) -> Result<Page<Playlist>> {
        self.get(
            "/me/playlists",
            &[("limit", limit.to_string()), ("offset", offset.to_string())],
        )
        .await
    }

    pub async fn playlist(&self, id: &str) -> Result<Playlist> {
        self.get(&format!("/playlists/{id}"), &[]).await
    }

    pub async fn playlist_items(
        &self,
        id: &str,
        offset: u32,
        limit: u32,
    ) -> Result<Page<PlaylistItem>> {
        self.get::<PositionedPage<PlaylistItem>>(
            &format!("/playlists/{id}/items"),
            &[
                ("limit", limit.to_string()),
                ("offset", offset.to_string()),
                ("additional_types", "track,episode".to_string()),
            ],
        )
        .await
        .map(Into::into)
    }

    /// Requested songs already present in a playlist.
    ///
    /// Spotify has no membership endpoint for playlists, so walk its pages
    /// until every requested URI has been found or the playlist ends.
    pub async fn playlist_duplicates(&self, id: &str, uris: &[String]) -> Result<Vec<String>> {
        let wanted: HashSet<&str> = uris.iter().map(String::as_str).collect();
        if wanted.is_empty() {
            return Ok(Vec::new());
        }
        let mut found = HashSet::new();
        let mut offset = 0;
        loop {
            let page = self.playlist_items(id, offset, 50).await?;
            for uri in page
                .items
                .iter()
                .filter_map(PlaylistItem::playable)
                .map(PlayableItem::uri)
            {
                if wanted.contains(uri) {
                    found.insert(uri.to_string());
                }
            }
            if found.len() == wanted.len() {
                break;
            }
            let Some(next) = page.next_offset() else {
                break;
            };
            offset = next;
        }
        Ok(uris
            .iter()
            .filter(|uri| found.contains(uri.as_str()))
            .cloned()
            .collect())
    }

    pub async fn create_playlist(
        &self,
        name: &str,
        public: bool,
        description: &str,
    ) -> Result<Playlist> {
        let body = json!({ "name": name, "public": public, "description": description });
        let value = self
            .write(Method::POST, "/me/playlists", &[], Some(&body))
            .await?
            .unwrap_or(Value::Null);
        serde_json::from_value(value).map_err(|error| ApiError::Decode(error.to_string()))
    }

    pub async fn upload_playlist_cover(&self, id: &str, encoded: &str) -> Result<()> {
        if encoded.is_empty() || encoded.len() > crate::playlist_cover::MAX_PAYLOAD {
            return Err(ApiError::Status {
                status: 413,
                message: "Choose a smaller image.".into(),
            });
        }
        self.send_body(
            Method::PUT,
            &format!("/playlists/{id}/images"),
            &[],
            None,
            Some(encoded),
        )
        .await?;
        Ok(())
    }

    pub async fn update_playlist(
        &self,
        id: &str,
        name: Option<&str>,
        description: Option<&str>,
        public: Option<bool>,
    ) -> Result<()> {
        let mut body = serde_json::Map::new();
        if let Some(name) = name {
            body.insert("name".into(), json!(name));
        }
        if let Some(description) = description {
            body.insert("description".into(), json!(description));
        }
        if let Some(public) = public {
            body.insert("public".into(), json!(public));
        }
        self.write(
            Method::PUT,
            &format!("/playlists/{id}"),
            &[],
            Some(&Value::Object(body)),
        )
        .await?;
        Ok(())
    }

    pub async fn add_playlist_items(
        &self,
        id: &str,
        uris: &[String],
        position: Option<u32>,
    ) -> Result<Option<String>> {
        let mut body = json!({ "uris": uris });
        if let Some(position) = position {
            body["position"] = json!(position);
        }
        let value = self
            .write(
                Method::POST,
                &format!("/playlists/{id}/items"),
                &[],
                Some(&body),
            )
            .await?;
        Ok(Self::snapshot(value))
    }

    pub async fn remove_playlist_items(
        &self,
        id: &str,
        uris: &[String],
        snapshot_id: Option<&str>,
    ) -> Result<Option<String>> {
        let entries: Vec<Value> = uris.iter().map(|uri| json!({ "uri": uri })).collect();
        let mut body = json!({ "items": entries });
        if let Some(snapshot) = snapshot_id {
            body["snapshot_id"] = json!(snapshot);
        }
        let value = self
            .write(
                Method::DELETE,
                &format!("/playlists/{id}/items"),
                &[],
                Some(&body),
            )
            .await?;
        Ok(Self::snapshot(value))
    }

    pub async fn reorder_playlist(
        &self,
        id: &str,
        range_start: u32,
        insert_before: u32,
        snapshot_id: Option<&str>,
    ) -> Result<Option<String>> {
        let mut body = json!({
            "range_start": range_start,
            "insert_before": insert_before,
            "range_length": 1,
        });
        if let Some(snapshot) = snapshot_id {
            body["snapshot_id"] = json!(snapshot);
        }
        let value = self
            .write(
                Method::PUT,
                &format!("/playlists/{id}/items"),
                &[],
                Some(&body),
            )
            .await?;
        Ok(Self::snapshot(value))
    }

    fn snapshot(value: Option<Value>) -> Option<String> {
        value
            .and_then(|value| serde_json::from_value::<SnapshotId>(value).ok())
            .and_then(|snapshot| snapshot.snapshot_id)
    }

    pub async fn follow_playlist(&self, id: &str) -> Result<()> {
        self.write(
            Method::PUT,
            "/me/library",
            &[("uris", format!("spotify:playlist:{id}"))],
            None,
        )
        .await?;
        Ok(())
    }

    pub async fn unfollow_playlist(&self, id: &str) -> Result<()> {
        self.write(
            Method::DELETE,
            "/me/library",
            &[("uris", format!("spotify:playlist:{id}"))],
            None,
        )
        .await?;
        Ok(())
    }

    // ---- library -----------------------------------------------------------

    pub async fn saved_tracks(&self, offset: u32, limit: u32) -> Result<Page<SavedTrack>> {
        self.get(
            "/me/tracks",
            &[("limit", limit.to_string()), ("offset", offset.to_string())],
        )
        .await
    }

    pub async fn saved_albums(&self, offset: u32, limit: u32) -> Result<Page<SavedAlbum>> {
        self.get(
            "/me/albums",
            &[("limit", limit.to_string()), ("offset", offset.to_string())],
        )
        .await
    }

    pub async fn followed_artists(
        &self,
        after: Option<&str>,
        limit: u32,
    ) -> Result<CursorPage<Artist>> {
        let mut query = vec![("type", "artist".to_string()), ("limit", limit.to_string())];
        if let Some(after) = after {
            query.push(("after", after.to_string()));
        }
        let followed: FollowedArtists = self.get("/me/following", &query).await?;
        Ok(followed.artists)
    }

    pub async fn saved_shows(&self, offset: u32, limit: u32) -> Result<Page<SavedShow>> {
        self.get(
            "/me/shows",
            &[("limit", limit.to_string()), ("offset", offset.to_string())],
        )
        .await
    }

    pub async fn saved_episodes(&self, offset: u32, limit: u32) -> Result<Page<SavedEpisode>> {
        self.get(
            "/me/episodes",
            &[("limit", limit.to_string()), ("offset", offset.to_string())],
        )
        .await
    }

    pub async fn top_tracks(
        &self,
        time_range: &str,
        limit: u32,
        offset: u32,
    ) -> Result<Page<Track>> {
        self.get(
            "/me/top/tracks",
            &[
                ("limit", limit.to_string()),
                ("offset", offset.to_string()),
                ("time_range", time_range.to_string()),
            ],
        )
        .await
    }

    pub async fn top_artists(&self, time_range: &str, limit: u32) -> Result<Page<Artist>> {
        self.get(
            "/me/top/artists",
            &[
                ("limit", limit.to_string()),
                ("time_range", time_range.to_string()),
            ],
        )
        .await
    }

    async fn library_write(&self, method: Method, uris: &[String]) -> Result<()> {
        self.write(method, "/me/library", &[("uris", uris.join(","))], None)
            .await?;
        Ok(())
    }

    /// Saves tracks, albums, artists, shows, episodes, or playlists.
    pub async fn save(&self, uris: &[String]) -> Result<()> {
        self.library_write(Method::PUT, uris).await
    }

    pub async fn unsave(&self, uris: &[String]) -> Result<()> {
        self.library_write(Method::DELETE, uris).await
    }

    /// Whether each URI is in the library, in the same order as `uris`.
    pub async fn contains(&self, uris: &[String]) -> Result<Vec<bool>> {
        self.get("/me/library/contains", &[("uris", uris.join(","))])
            .await
    }

    // ---- catalog -----------------------------------------------------------

    pub async fn search(&self, query: &str, types: &[&str]) -> Result<SearchResults> {
        self.get(
            "/search",
            &[
                ("q", query.to_string()),
                ("type", types.join(",")),
                ("limit", self.search_limit.to_string()),
            ],
        )
        .await
    }

    pub async fn artist(&self, id: &str) -> Result<Artist> {
        self.get(&format!("/artists/{id}"), &[]).await
    }

    pub async fn artist_top_tracks(&self, id: &str) -> Result<Vec<Track>> {
        self.get::<TopTracks>(&format!("/artists/{id}/top-tracks"), &[])
            .await
            .map(|top| top.tracks)
    }

    pub async fn artist_albums(
        &self,
        id: &str,
        include_groups: &str,
        offset: u32,
        limit: u32,
    ) -> Result<Page<Album>> {
        let limit = limit.min(self.artist_albums_limit);
        self.get(
            &format!("/artists/{id}/albums"),
            &[
                ("include_groups", include_groups.to_string()),
                ("limit", limit.to_string()),
                ("offset", offset.to_string()),
            ],
        )
        .await
    }

    pub async fn related_artists(&self, id: &str) -> Result<Vec<Artist>> {
        let related: RelatedArtists = self
            .get(&format!("/artists/{id}/related-artists"), &[])
            .await?;
        Ok(related.artists)
    }

    pub async fn album(&self, id: &str) -> Result<Album> {
        self.get(&format!("/albums/{id}"), &[]).await
    }

    pub async fn album_tracks(&self, id: &str, offset: u32, limit: u32) -> Result<Page<Track>> {
        self.get::<PositionedPage<Track>>(
            &format!("/albums/{id}/tracks"),
            &[("limit", limit.to_string()), ("offset", offset.to_string())],
        )
        .await
        .map(Into::into)
    }

    pub async fn show(&self, id: &str) -> Result<Show> {
        self.get(&format!("/shows/{id}"), &[]).await
    }

    pub async fn show_episodes(&self, id: &str, offset: u32, limit: u32) -> Result<Page<Episode>> {
        self.get(
            &format!("/shows/{id}/episodes"),
            &[("limit", limit.to_string()), ("offset", offset.to_string())],
        )
        .await
    }

    pub async fn track(&self, id: &str) -> Result<Track> {
        self.get(&format!("/tracks/{id}"), &[]).await
    }

    pub async fn episode(&self, id: &str) -> Result<Episode> {
        self.get(&format!("/episodes/{id}"), &[]).await
    }

    pub async fn recommendations(
        &self,
        seed_tracks: &[String],
        seed_artists: &[String],
        limit: u32,
    ) -> Result<Vec<Track>> {
        let mut query = vec![("limit", limit.to_string())];
        if !seed_tracks.is_empty() {
            query.push(("seed_tracks", seed_tracks.join(",")));
        }
        if !seed_artists.is_empty() {
            query.push(("seed_artists", seed_artists.join(",")));
        }
        let recommendations: Recommendations = self.get("/recommendations", &query).await?;
        Ok(recommendations.tracks)
    }
}

// ---- telemetry -------------------------------------------------------------
//
// Everything below only observes requests: nothing in it changes how, when or
// whether one is made. It is inert while telemetry is off, apart from a few
// relaxed atomics.

tokio::task_local! {
    static CALLER: &'static str;
}

/// Runs `future` with the Web API requests it makes attributed to `caller`
/// (the feature asking, such as `playlist_items`) in telemetry.
pub async fn tagged<F: std::future::Future>(caller: &'static str, future: F) -> F::Output {
    CALLER.scope(caller, future).await
}

mod meters {
    use crate::telemetry::{Counter, Gauge};

    pub static ATTEMPTS_SHARED: Counter = Counter::new("api_attempts_shared");
    pub static ATTEMPTS_PERSONAL: Counter = Counter::new("api_attempts_personal");
    pub static LIMITED_SHARED: Counter = Counter::new("api_429_shared");
    pub static LIMITED_PERSONAL: Counter = Counter::new("api_429_personal");
    pub static UNAUTHORIZED: Counter = Counter::new("api_401");
    pub static CLIENT_ERRORS: Counter = Counter::new("api_4xx");
    pub static SERVER_ERRORS: Counter = Counter::new("api_5xx");
    pub static NETWORK_ERRORS: Counter = Counter::new("api_net_errors");
    pub static TIMEOUTS: Counter = Counter::new("api_timeouts");
    pub static CANCELLED: Counter = Counter::new("api_cancelled");
    pub static REPEATS: Counter = Counter::new("api_repeats");
    pub static SENT_IN_COOLDOWN: Counter = Counter::new("api_sent_in_cooldown");
    pub static RESPONSE_BYTES: Counter = Counter::new("api_resp_bytes");
    pub static REFRESHES: Counter = Counter::new("auth_refreshes");
    pub static REFRESH_FAILURES: Counter = Counter::new("auth_refresh_failures");
    pub static REDUNDANT_REFRESHES: Counter = Counter::new("auth_redundant_refreshes");

    pub static IN_FLIGHT: Gauge = Gauge::peak("api_in_flight_peak");
    pub static PENDING: Gauge = Gauge::peak("api_pending_peak");
    pub static WAITERS: Gauge = Gauge::peak("api_permit_waiters_peak");
    pub static LATENCY_MAX_MS: Gauge = Gauge::peak("api_latency_max_ms");
    pub static WAIT_MAX_MS: Gauge = Gauge::peak("api_wait_max_ms");
    pub static COOLDOWN_SHARED_MS: Gauge = Gauge::peak("api_cooldown_max_ms_shared");
    pub static COOLDOWN_PERSONAL_MS: Gauge = Gauge::peak("api_cooldown_max_ms_personal");
    pub static RATE_SHARED: Gauge = Gauge::peak("api_rate_30s_peak_shared");
    pub static RATE_PERSONAL: Gauge = Gauge::peak("api_rate_30s_peak_personal");
}

fn register_meters() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        telemetry::register_counters(&[
            &meters::ATTEMPTS_SHARED,
            &meters::ATTEMPTS_PERSONAL,
            &meters::LIMITED_SHARED,
            &meters::LIMITED_PERSONAL,
            &meters::UNAUTHORIZED,
            &meters::CLIENT_ERRORS,
            &meters::SERVER_ERRORS,
            &meters::NETWORK_ERRORS,
            &meters::TIMEOUTS,
            &meters::CANCELLED,
            &meters::REPEATS,
            &meters::SENT_IN_COOLDOWN,
            &meters::RESPONSE_BYTES,
            &meters::REFRESHES,
            &meters::REFRESH_FAILURES,
            &meters::REDUNDANT_REFRESHES,
        ]);
        telemetry::register_gauges(&[
            &meters::IN_FLIGHT,
            &meters::PENDING,
            &meters::WAITERS,
            &meters::LATENCY_MAX_MS,
            &meters::WAIT_MAX_MS,
            &meters::COOLDOWN_SHARED_MS,
            &meters::COOLDOWN_PERSONAL_MS,
            &meters::RATE_SHARED,
            &meters::RATE_PERSONAL,
        ]);
    });
}

/// Spotify's rate limit is counted over a rolling 30 seconds.
const WINDOW: Duration = Duration::from_secs(30);
const KEPT_ATTEMPTS: Duration = Duration::from_secs(300);
/// Above this many attempts a second, routine answers go to the flight
/// recorder instead of shipping; `api.budget` still counts them.
const FLOOD_PER_SECOND: usize = 8;
/// A first 429 after fewer attempts than this in the window is unlikely to
/// be this process's doing.
const EXTERNAL_BELOW: usize = 20;
const REPEAT_WITHIN: Duration = Duration::from_secs(2);
const STUCK_AFTER: Duration = Duration::from_secs(10);
/// A failed `api.request` ships once per grant, route and outcome in this
/// interval; repeats stay in the flight recorder.
const FAILURE_EVERY: Duration = Duration::from_secs(60);
/// A grant's `api.rate_limited` anomaly past an episode's start, and its
/// exhausted quota, at most this often; repeats stay in the flight recorder.
const RATE_LIMITED_EVERY: Duration = Duration::from_secs(60);
const QUOTA_EVERY: Duration = Duration::from_secs(300);
/// Query keys whose values are safe to report: no ids, no search text.
const SAFE_QUERY: &[&str] = &[
    "limit",
    "offset",
    "time_range",
    "include_groups",
    "additional_types",
    "type",
    "state",
    "volume_percent",
    "position_ms",
];

static CLOCK: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
static GENERATION: AtomicU64 = AtomicU64::new(0);
static LAST_SENT_MS: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];
static LAST_REFRESH_MS: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];
static BUDGET: telemetry::Throttle = telemetry::Throttle::new();
static RATE_LIMITED_REPORT: [telemetry::Throttle; 2] =
    [telemetry::Throttle::new(), telemetry::Throttle::new()];
static QUOTA_REPORT: [telemetry::Throttle; 2] =
    [telemetry::Throttle::new(), telemetry::Throttle::new()];
static ATTEMPTS: Mutex<VecDeque<Attempt>> = Mutex::new(VecDeque::new());
static STARTS: Mutex<VecDeque<(Instant, u64)>> = Mutex::new(VecDeque::new());
/// Grant index, method, path template and outcome of a shipped failure.
type FailureKey = (usize, &'static str, &'static str, &'static str);
static FAILURES: Mutex<Vec<(Instant, FailureKey)>> = Mutex::new(Vec::new());

/// Milliseconds on a process clock, never zero, so deadlines fit an atomic.
fn clock_at(at: Instant) -> u64 {
    at.saturating_duration_since(*CLOCK.get_or_init(Instant::now))
        .as_millis() as u64
        + 1
}

fn clock_ms() -> u64 {
    clock_at(Instant::now())
}

fn grant_index(source: ApiSource) -> usize {
    match source {
        ApiSource::Shared => 0,
        ApiSource::Personal => 1,
    }
}

fn grant_name(source: ApiSource) -> &'static str {
    match source {
        ApiSource::Shared => "shared",
        ApiSource::Personal => "personal",
    }
}

fn method_name(method: &Method) -> &'static str {
    if *method == Method::GET {
        "GET"
    } else if *method == Method::POST {
        "POST"
    } else if *method == Method::PUT {
        "PUT"
    } else if *method == Method::DELETE {
        "DELETE"
    } else {
        "OTHER"
    }
}

/// A Web API path with its ids replaced, from a fixed table so no id can
/// reach a template. A path the table lacks is `other`.
fn template(path: &str) -> &'static str {
    let path = if path.starts_with("http") {
        path.find("/v1/").map_or("", |start| &path[start + 3..])
    } else {
        path
    };
    let path = path.split(['?', '#']).next().unwrap_or("");
    let segments: Vec<&str> = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    match segments.as_slice() {
        ["me"] => "/me",
        ["me", "player"] => "/me/player",
        ["me", "player", "devices"] => "/me/player/devices",
        ["me", "player", "queue"] => "/me/player/queue",
        ["me", "player", "recently-played"] => "/me/player/recently-played",
        ["me", "player", "play"] => "/me/player/play",
        ["me", "player", "pause"] => "/me/player/pause",
        ["me", "player", "next"] => "/me/player/next",
        ["me", "player", "previous"] => "/me/player/previous",
        ["me", "player", "seek"] => "/me/player/seek",
        ["me", "player", "volume"] => "/me/player/volume",
        ["me", "player", "shuffle"] => "/me/player/shuffle",
        ["me", "player", "repeat"] => "/me/player/repeat",
        ["me", "playlists"] => "/me/playlists",
        ["me", "library"] => "/me/library",
        ["me", "library", "contains"] => "/me/library/contains",
        ["me", "tracks"] => "/me/tracks",
        ["me", "albums"] => "/me/albums",
        ["me", "following"] => "/me/following",
        ["me", "shows"] => "/me/shows",
        ["me", "episodes"] => "/me/episodes",
        ["me", "top", "tracks"] => "/me/top/tracks",
        ["me", "top", "artists"] => "/me/top/artists",
        ["search"] => "/search",
        ["recommendations"] => "/recommendations",
        ["playlists", _] => "/playlists/{id}",
        ["playlists", _, "items"] => "/playlists/{id}/items",
        ["playlists", _, "tracks"] => "/playlists/{id}/tracks",
        ["playlists", _, "images"] => "/playlists/{id}/images",
        ["artists", _] => "/artists/{id}",
        ["artists", _, "top-tracks"] => "/artists/{id}/top-tracks",
        ["artists", _, "albums"] => "/artists/{id}/albums",
        ["artists", _, "related-artists"] => "/artists/{id}/related-artists",
        ["albums", _] => "/albums/{id}",
        ["albums", _, "tracks"] => "/albums/{id}/tracks",
        ["shows", _] => "/shows/{id}",
        ["shows", _, "episodes"] => "/shows/{id}/episodes",
        ["tracks", _] => "/tracks/{id}",
        ["episodes", _] => "/episodes/{id}",
        _ => "other",
    }
}

fn query_summary(query: &[(&str, String)]) -> Option<String> {
    let safe: Vec<String> = query
        .iter()
        .filter(|(key, _)| SAFE_QUERY.contains(key))
        .map(|(key, value)| format!("{key}={value}"))
        .collect();
    (!safe.is_empty()).then(|| safe.join("&"))
}

fn request_key(method: &str, path: &str, query: &[(&str, String)]) -> u64 {
    use std::hash::{Hash as _, Hasher as _};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    method.hash(&mut hasher);
    path.hash(&mut hasher);
    query.hash(&mut hasher);
    hasher.finish()
}

/// How long ago the same request last started, when within
/// [`REPEAT_WITHIN`]. Retries of one request are not repeats.
fn repeat_gap(key: u64) -> Option<Duration> {
    let now = Instant::now();
    let mut starts = STARTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    while starts
        .front()
        .is_some_and(|(at, _)| now.saturating_duration_since(*at) > REPEAT_WITHIN)
        || starts.len() >= 512
    {
        starts.pop_front();
    }
    let gap = starts
        .iter()
        .rev()
        .find(|(_, seen)| *seen == key)
        .map(|(at, _)| now.saturating_duration_since(*at));
    starts.push_back((now, key));
    gap
}

/// Whether no failure of this grant, route and outcome shipped in the last
/// [`FAILURE_EVERY`]; if so, this one is noted as shipped.
fn first_failure(
    source: ApiSource,
    method: &'static str,
    endpoint: &'static str,
    outcome: &'static str,
) -> bool {
    let now = Instant::now();
    let key: FailureKey = (grant_index(source), method, endpoint, outcome);
    let mut shipped = FAILURES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    shipped.retain(|(at, _)| now.saturating_duration_since(*at) < FAILURE_EVERY);
    if shipped.iter().any(|(_, seen)| *seen == key) {
        return false;
    }
    shipped.push((now, key));
    true
}

/// Spotify's machine-readable reason, such as `QUOTA_EXCEEDED`; never its
/// free-text body.
fn error_reason(body: &str) -> Option<String> {
    serde_json::from_str::<ApiErrorBody>(body)
        .ok()
        .and_then(|body| body.error.reason)
        .filter(|reason| {
            reason.len() <= 48
                && reason
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        })
}

fn retry_after_kind(raw: Option<&str>) -> &'static str {
    match raw {
        None => "missing",
        // Parsed exactly as the 429 branch parses it.
        Some(raw) if raw.parse::<u64>().is_ok() => "seconds",
        Some(raw) if raw.contains("GMT") => "http_date",
        Some(_) => "other",
    }
}

/// One attempt that reached the wire, kept for the rolling window.
struct Attempt {
    at: Instant,
    source: ApiSource,
    method: &'static str,
    endpoint: &'static str,
    /// Zero when no status arrived.
    status: u16,
    ms: u32,
}

/// A grant's load in the window, this attempt included.
struct Load {
    grant: usize,
    endpoint: usize,
    limited: usize,
    last_second: usize,
}

fn note_attempt(
    source: ApiSource,
    method: &'static str,
    endpoint: &'static str,
    status: u16,
    wire: Duration,
) -> Load {
    let now = Instant::now();
    let mut attempts = ATTEMPTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    while attempts
        .front()
        .is_some_and(|attempt| now.saturating_duration_since(attempt.at) > KEPT_ATTEMPTS)
        || attempts.len() >= 4096
    {
        attempts.pop_front();
    }
    attempts.push_back(Attempt {
        at: now,
        source,
        method,
        endpoint,
        status,
        ms: wire.as_millis().min(u128::from(u32::MAX)) as u32,
    });
    let mut load = Load {
        grant: 0,
        endpoint: 0,
        limited: 0,
        last_second: 0,
    };
    for attempt in attempts
        .iter()
        .rev()
        .take_while(|attempt| now.saturating_duration_since(attempt.at) <= WINDOW)
    {
        if now.saturating_duration_since(attempt.at) <= Duration::from_secs(1) {
            load.last_second += 1;
        }
        if attempt.source != source {
            continue;
        }
        load.grant += 1;
        if attempt.status == 429 {
            load.limited += 1;
        }
        if attempt.method == method && attempt.endpoint == endpoint {
            load.endpoint += 1;
        }
    }
    load
}

/// A grant's recent load before the attempt being judged.
#[derive(Default)]
struct GrantLoad {
    attempts_30s: usize,
    attempts_300s: usize,
    limited_30s: usize,
    other_30s: usize,
    since_last_429: Option<Duration>,
}

fn grant_load(source: ApiSource) -> GrantLoad {
    let now = Instant::now();
    let attempts = ATTEMPTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut load = GrantLoad::default();
    for attempt in attempts.iter().rev() {
        let age = now.saturating_duration_since(attempt.at);
        if age > KEPT_ATTEMPTS {
            break;
        }
        if attempt.source != source {
            if age <= WINDOW {
                load.other_30s += 1;
            }
            continue;
        }
        load.attempts_300s += 1;
        if age <= WINDOW {
            load.attempts_30s += 1;
            if attempt.status == 429 {
                load.limited_30s += 1;
            }
        }
        if attempt.status == 429 && load.since_last_429.is_none() {
            load.since_last_429 = Some(age);
        }
    }
    load
}

struct RouteStats {
    source: ApiSource,
    method: &'static str,
    endpoint: &'static str,
    count: usize,
    limited: usize,
    errors: usize,
    max_ms: u32,
    sum_ms: u64,
}

/// Which grant and endpoint used Spotify's window, for `api.budget`.
fn report_budget() {
    let now = Instant::now();
    let mut grants = [0usize; 2];
    let mut limited = [0usize; 2];
    let mut routes: Vec<RouteStats> = Vec::new();
    {
        let attempts = ATTEMPTS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for attempt in attempts
            .iter()
            .rev()
            .take_while(|attempt| now.saturating_duration_since(attempt.at) <= WINDOW)
        {
            let index = grant_index(attempt.source);
            grants[index] += 1;
            let is_limited = attempt.status == 429;
            let is_error = attempt.status == 0 || attempt.status >= 500;
            if is_limited {
                limited[index] += 1;
            }
            let position = routes.iter().position(|route| {
                route.source == attempt.source
                    && route.method == attempt.method
                    && route.endpoint == attempt.endpoint
            });
            let route = match position {
                Some(position) => &mut routes[position],
                None => {
                    routes.push(RouteStats {
                        source: attempt.source,
                        method: attempt.method,
                        endpoint: attempt.endpoint,
                        count: 0,
                        limited: 0,
                        errors: 0,
                        max_ms: 0,
                        sum_ms: 0,
                    });
                    let last = routes.len() - 1;
                    &mut routes[last]
                }
            };
            route.count += 1;
            route.limited += usize::from(is_limited);
            route.errors += usize::from(is_error);
            route.max_ms = route.max_ms.max(attempt.ms);
            route.sum_ms += u64::from(attempt.ms);
        }
    }
    routes.sort_by_key(|route| std::cmp::Reverse(route.count));
    let top: Vec<Value> = routes
        .iter()
        .take(12)
        .map(|route| {
            json!({
                "route": format!("{} {} {}", grant_name(route.source), route.method, route.endpoint),
                "n": route.count,
                "n429": route.limited,
                "errors": route.errors,
                "max_ms": route.max_ms,
                "avg_ms": route.sum_ms / route.count.max(1) as u64
            })
        })
        .collect();
    telemetry::event("api.budget")
        .field("window_s", WINDOW.as_secs())
        .field("shared_30s", grants[0])
        .field("personal_30s", grants[1])
        .field("shared_429_30s", limited[0])
        .field("personal_429_30s", limited[1])
        .field("routes", top)
        .emit();
}

/// Telemetry's view of one client, read without taking its locks.
struct ClientProbe {
    generation: u64,
    /// The cooldown deadline on [`clock_ms`], mirrored from the lock.
    cooldown_until_ms: AtomicU64,
    episode_started_ms: AtomicU64,
    episode_limited: AtomicU64,
    episode_waited: AtomicU64,
    permit_waiters: AtomicUsize,
}

impl ClientProbe {
    fn new() -> Self {
        Self {
            generation: GENERATION.fetch_add(1, Ordering::Relaxed) + 1,
            cooldown_until_ms: AtomicU64::new(0),
            episode_started_ms: AtomicU64::new(0),
            episode_limited: AtomicU64::new(0),
            episode_waited: AtomicU64::new(0),
            permit_waiters: AtomicUsize::new(0),
        }
    }
}

/// Counts a request queued for a permit, even if its future is dropped.
struct Waiting<'a>(&'a AtomicUsize);

impl<'a> Waiting<'a> {
    fn new(count: &'a AtomicUsize) -> Self {
        count.fetch_add(1, Ordering::Relaxed);
        Self(count)
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// What a response said about itself, captured before its body is read.
#[derive(Default)]
struct Reply {
    version: Option<String>,
    family: Option<&'static str>,
    retry_after: Option<String>,
    upstream_ms: Option<u64>,
    rate_headers: Option<String>,
}

fn reply_info(response: &reqwest::Response) -> Reply {
    let headers = response.headers();
    let retry_after = headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.chars().take(40).collect::<String>());
    let upstream_ms = headers
        .get("x-envoy-upstream-service-time")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok());
    let rate: Vec<String> = headers
        .iter()
        .filter(|(name, _)| {
            let name = name.as_str();
            name.contains("ratelimit") || name.contains("rate-limit")
        })
        .filter_map(|(name, value)| {
            let value = value.to_str().ok()?;
            (value.len() <= 64).then(|| format!("{}={value}", name.as_str()))
        })
        .collect();
    Reply {
        version: Some(format!("{:?}", response.version())),
        family: response
            .remote_addr()
            .map(|address| if address.is_ipv4() { "v4" } else { "v6" }),
        retry_after,
        upstream_ms,
        rate_headers: (!rate.is_empty()).then(|| rate.join("; ")),
    }
}

/// One logical request as telemetry sees it.
struct RequestProbe<'a> {
    client: &'a ApiClient,
    live: bool,
    method: &'static str,
    endpoint: &'static str,
    caller: Option<&'static str>,
    query: Option<String>,
    q_len: Option<usize>,
    uris: Option<usize>,
    device: Option<String>,
    req_bytes: Option<usize>,
    queue_write: bool,
    started: Instant,
    attempt: u32,
    phase: &'static str,
    attempt_started: Instant,
    phase_started: Instant,
    cooldown_wait: Duration,
    permit_wait: Duration,
    token_wait: Duration,
    token_lock: Duration,
    cooldown_at_entry_ms: u64,
    sent_at: Option<Instant>,
    sent_in_cooldown: bool,
    in_flight: usize,
    waiters: usize,
    idle_before_ms: Option<u64>,
    ttfb: Option<Duration>,
    reply: Reply,
    /// The user's action behind a player command, to time it end to end.
    intent: Option<telemetry::Intent>,
    finished: bool,
}

/// The intents a player command carries out, for correlation.
fn intent_actions(method: &str, endpoint: &str) -> &'static [&'static str] {
    match (method, endpoint) {
        ("POST", "/me/player/next") => &["next"],
        ("POST", "/me/player/previous") => &["previous"],
        ("PUT", "/me/player/play") => &["play", "toggle", "play_item"],
        ("PUT", "/me/player/pause") => &["pause", "toggle"],
        ("PUT", "/me/player/seek") => &["seek"],
        ("PUT", "/me/player/volume") => &["volume"],
        ("PUT", "/me/player/shuffle") => &["shuffle"],
        ("PUT", "/me/player/repeat") => &["repeat"],
        ("POST", "/me/player/queue") => &["queue_add"],
        ("PUT", "/me/player") => &["transfer"],
        _ => &[],
    }
}

impl<'a> RequestProbe<'a> {
    fn new(
        client: &'a ApiClient,
        method: &Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<&Value>,
        jpeg: Option<&str>,
        queue_write: bool,
    ) -> Self {
        let now = Instant::now();
        let live = telemetry::enabled();
        let mut probe = Self {
            client,
            live,
            method: method_name(method),
            endpoint: template(path),
            caller: CALLER.try_with(|caller| *caller).ok(),
            query: None,
            q_len: None,
            uris: None,
            device: None,
            req_bytes: None,
            queue_write,
            started: now,
            attempt: 0,
            phase: "cooldown",
            attempt_started: now,
            phase_started: now,
            cooldown_wait: Duration::ZERO,
            permit_wait: Duration::ZERO,
            token_wait: Duration::ZERO,
            token_lock: Duration::ZERO,
            cooldown_at_entry_ms: 0,
            sent_at: None,
            sent_in_cooldown: false,
            in_flight: 0,
            waiters: 0,
            idle_before_ms: None,
            ttfb: None,
            reply: Reply::default(),
            intent: None,
            finished: false,
        };
        if !live {
            return probe;
        }
        let actions = intent_actions(probe.method, probe.endpoint);
        if !actions.is_empty() {
            probe.intent = telemetry::recent_intent(actions, Duration::from_secs(10));
        }
        probe.query = query_summary(query);
        probe.q_len = query
            .iter()
            .find(|(key, _)| *key == "q")
            .map(|(_, value)| value.chars().count());
        probe.uris = query
            .iter()
            .find(|(key, _)| *key == "uris" || *key == "uri")
            .map(|(key, value)| {
                if *key == "uri" {
                    1
                } else {
                    value.split(',').count()
                }
            })
            .or_else(|| {
                body.and_then(|body| body.get("uris"))
                    .and_then(Value::as_array)
                    .map(Vec::len)
            });
        // A digest, so commands aimed at one device group without naming it.
        probe.device = query
            .iter()
            .find(|(key, _)| *key == "device_id")
            .map(|(_, value)| telemetry::digest(value));
        probe.req_bytes = jpeg
            .map(str::len)
            .or_else(|| body.map(|body| body.to_string().len()));
        if let Some(gap) = repeat_gap(request_key(probe.method, path, query)) {
            meters::REPEATS.incr();
            telemetry::crumb("api.repeat")
                .field("grant", grant_name(client.source))
                .field("method", probe.method)
                .field("path_template", probe.endpoint)
                .field("caller", probe.caller)
                .field("query", probe.query.clone())
                .ms("gap_ms", gap)
                .emit();
        }
        probe
    }

    fn begin(&mut self, attempt: u32) {
        let now = Instant::now();
        self.attempt = attempt;
        self.phase = "cooldown";
        self.attempt_started = now;
        self.phase_started = now;
        self.cooldown_wait = Duration::ZERO;
        self.permit_wait = Duration::ZERO;
        self.token_wait = Duration::ZERO;
        self.token_lock = Duration::ZERO;
        self.cooldown_at_entry_ms = self.client.cooldown_remaining_ms();
        self.sent_at = None;
        self.sent_in_cooldown = false;
        self.in_flight = 0;
        self.waiters = 0;
        self.idle_before_ms = None;
        self.ttfb = None;
        self.reply = Reply::default();
    }

    fn enter(&mut self, phase: &'static str) {
        self.phase = phase;
        self.phase_started = Instant::now();
    }

    fn cooled(&mut self) {
        self.cooldown_wait = self.phase_started.elapsed();
        if self.cooldown_wait >= Duration::from_millis(5) {
            self.client
                .probe
                .episode_waited
                .fetch_add(1, Ordering::Relaxed);
        }
        self.enter("permit");
    }

    fn permitted(&mut self) {
        self.permit_wait = self.phase_started.elapsed();
        self.enter("token");
    }

    fn tokened(&mut self, lock: Duration) {
        self.token_wait = self.phase_started.elapsed();
        self.token_lock = lock;
        self.enter("send");
    }

    fn sending(&mut self) {
        let now = Instant::now();
        let client = self.client;
        self.sent_at = Some(now);
        // Sent while the grant is cooling down: it passed the cooldown check
        // and then queued for a permit or the token while a 429 arrived.
        self.sent_in_cooldown = client.cooldown_remaining_ms() > 0;
        if self.sent_in_cooldown {
            meters::SENT_IN_COOLDOWN.incr();
        }
        self.in_flight = MAX_IN_FLIGHT.saturating_sub(client.limiter.available_permits());
        self.waiters = client.probe.permit_waiters.load(Ordering::Relaxed);
        meters::IN_FLIGHT.raise(self.in_flight as i64);
        meters::WAITERS.raise(self.waiters as i64);
        meters::PENDING.raise(client.activity.in_flight.load(Ordering::SeqCst) as i64);
        let waited = self.cooldown_wait + self.permit_wait + self.token_wait;
        meters::WAIT_MAX_MS.raise(waited.as_millis() as i64);
        let now_ms = clock_ms();
        let last = LAST_SENT_MS[grant_index(client.source)].swap(now_ms, Ordering::Relaxed);
        self.idle_before_ms = (last != 0).then_some(now_ms.saturating_sub(last));
    }

    fn answered(&mut self, response: &reqwest::Response) {
        self.ttfb = self.sent_at.map(|at| at.elapsed());
        if self.live {
            self.reply = reply_info(response);
        }
        self.enter("body");
    }

    /// A request error: `status` when the headers arrived and the body did not.
    fn failed(&mut self, status: Option<u16>, error: &reqwest::Error) {
        let class = crate::http::error_class(error);
        if status.is_none() {
            meters::NETWORK_ERRORS.incr();
        }
        if error.is_timeout() {
            meters::TIMEOUTS.incr();
        }
        let outcome = if status.is_some() {
            "body_failed"
        } else {
            "network"
        };
        self.finish(outcome, status, None, |event| {
            event
                .field("error_kind", class)
                .field_with("io_kind", || crate::http::io_kind(error))
                .field_with("cause", || {
                    crate::http::root_cause(error).map(|cause| telemetry::scrub(&cause))
                })
        });
    }

    fn finish(
        &mut self,
        outcome: &'static str,
        status: Option<u16>,
        resp_bytes: Option<usize>,
        extra: impl FnOnce(telemetry::Event) -> telemetry::Event,
    ) {
        self.finished = true;
        self.report(outcome, status, resp_bytes, extra);
    }

    /// Emits `api.request` for the attempt now ending.
    fn report(
        &mut self,
        outcome: &'static str,
        status: Option<u16>,
        resp_bytes: Option<usize>,
        extra: impl FnOnce(telemetry::Event) -> telemetry::Event,
    ) {
        let source = self.client.source;
        let wire = self.sent_at.take().map(|at| at.elapsed());
        if wire.is_some() {
            match source {
                ApiSource::Shared => meters::ATTEMPTS_SHARED.incr(),
                ApiSource::Personal => meters::ATTEMPTS_PERSONAL.incr(),
            }
        }
        match status {
            Some(429) => match source {
                ApiSource::Shared => meters::LIMITED_SHARED.incr(),
                ApiSource::Personal => meters::LIMITED_PERSONAL.incr(),
            },
            Some(401) => meters::UNAUTHORIZED.incr(),
            Some(status) if status >= 500 => meters::SERVER_ERRORS.incr(),
            Some(status) if status >= 400 => meters::CLIENT_ERRORS.incr(),
            _ => {}
        }
        if outcome == "cancelled" {
            meters::CANCELLED.incr();
        }
        if let Some(bytes) = resp_bytes {
            meters::RESPONSE_BYTES.add(bytes as u64);
        }
        if let Some(wire) = wire {
            meters::LATENCY_MAX_MS.raise(wire.as_millis() as i64);
        }
        if !self.live {
            return;
        }
        let load = wire.map(|wire| {
            note_attempt(
                source,
                self.method,
                self.endpoint,
                status.unwrap_or(0),
                wire,
            )
        });
        if let Some(load) = &load {
            match source {
                ApiSource::Shared => meters::RATE_SHARED.raise(load.grant as i64),
                ApiSource::Personal => meters::RATE_PERSONAL.raise(load.grant as i64),
            }
        }
        // A successful player command always ships. A failure ships the
        // first time its grant, route and outcome are seen in a minute;
        // repeats are details, still counted above. A request dropped before
        // it was sent (a superseded search keystroke) is a detail unless it
        // was parked for a while.
        let flooded = load
            .as_ref()
            .is_some_and(|load| load.last_second > FLOOD_PER_SECOND);
        let quiet = match outcome {
            "ok" => flooded && !self.endpoint.starts_with("/me/player"),
            "cancelled" if wire.is_none() && self.started.elapsed() < Duration::from_secs(1) => {
                true
            }
            _ => !first_failure(source, self.method, self.endpoint, outcome),
        };
        let record = if quiet {
            telemetry::crumb("api.request")
        } else {
            telemetry::event("api.request")
        };
        let record = record
            .field("grant", grant_name(source))
            .field("method", self.method)
            .field("path_template", self.endpoint)
            .field("caller", self.caller)
            .field("attempt", self.attempt)
            .field("outcome", outcome)
            .field("status", status)
            .field("phase", (outcome == "cancelled").then_some(self.phase))
            .ms("attempt_ms", self.attempt_started.elapsed())
            .ms("total_ms", self.started.elapsed())
            .field("wire_ms", wire.map(telemetry::duration_ms))
            .field("ttfb_ms", self.ttfb.map(telemetry::duration_ms))
            .ms("cooldown_wait_ms", self.cooldown_wait)
            .ms("permit_wait_ms", self.permit_wait)
            .ms("token_wait_ms", self.token_wait)
            .ms("token_lock_ms", self.token_lock)
            .field(
                "cooldown_at_entry_ms",
                (self.cooldown_at_entry_ms > 0).then_some(self.cooldown_at_entry_ms),
            )
            .field("sent_in_cooldown", self.sent_in_cooldown)
            .field("in_flight", self.in_flight)
            .field("waiters", self.waiters)
            .field("idle_before_ms", self.idle_before_ms)
            .field("http_version", self.reply.version.clone())
            .field("ip_family", self.reply.family)
            .field("upstream_ms", self.reply.upstream_ms)
            .field(
                "retry_after_raw",
                self.reply.retry_after.as_deref().map(telemetry::scrub),
            )
            .field("resp_bytes", resp_bytes)
            .field("req_bytes", self.req_bytes)
            .field("query", self.query.clone())
            .field("q_len", self.q_len)
            .field("uris_count", self.uris)
            .field("device", self.device.clone())
            .field("queue_write", self.queue_write.then_some(true))
            .field("rolling_30s_grant", load.as_ref().map(|load| load.grant))
            .field(
                "rolling_30s_endpoint",
                load.as_ref().map(|load| load.endpoint),
            )
            .field("rolling_30s_429", load.as_ref().map(|load| load.limited))
            .field("client_generation", self.client.probe.generation)
            .field_with("intent_id", || {
                self.intent.as_ref().map(|intent| intent.id.clone())
            })
            .field_with("intent_action", || {
                self.intent.as_ref().map(|intent| intent.action)
            })
            .field_with("intent_source", || {
                self.intent.as_ref().map(|intent| intent.source)
            })
            .field_with("since_intent_ms", || {
                self.intent
                    .as_ref()
                    .map(|intent| telemetry::duration_ms(intent.at.elapsed()))
            });
        let record = match &self.reply.rate_headers {
            Some(headers) => record.text("ratelimit_headers", headers),
            None => record,
        };
        extra(record).emit();
        // A dead pooled connection after sleep or a network change answers
        // only when the 30 s client timeout fires.
        if let Some(wire) = wire.filter(|wire| *wire >= STUCK_AFTER) {
            telemetry::anomaly("api.stuck")
                .field("grant", grant_name(source))
                .field("method", self.method)
                .field("path_template", self.endpoint)
                .field("caller", self.caller)
                .field("outcome", outcome)
                .field("status", status)
                .ms("wire_ms", wire)
                .field("ttfb_ms", self.ttfb.map(telemetry::duration_ms))
                .field("idle_before_ms", self.idle_before_ms)
                .field("http_version", self.reply.version.clone())
                .field("ip_family", self.reply.family)
                .emit();
        }
        if let Some(status) = status.filter(|status| *status != 429) {
            self.client.end_cooldown_episode(status);
        }
        if BUDGET.ready(WINDOW) {
            report_budget();
        }
    }

    /// A 429: the attempt, the `api.rate_limited` anomaly with the cooldown
    /// it causes, and a suspicion of load from outside this process.
    /// `applied` is `None` for an exhausted quota, which sets no cooldown.
    /// Past an episode's start the anomaly is a crumb between throttled
    /// reports, as is a repeated `api.quota_exhausted`.
    fn rate_limited(
        &mut self,
        asked: Duration,
        applied: Option<Duration>,
        body: &str,
        before_ms: u64,
        gave_up: bool,
    ) {
        let client = self.client;
        let source = client.source;
        let quota = applied.is_none();
        let after_ms = client.cooldown_remaining_ms();
        if !quota {
            match source {
                ApiSource::Shared => meters::COOLDOWN_SHARED_MS.raise(after_ms as i64),
                ApiSource::Personal => meters::COOLDOWN_PERSONAL_MS.raise(after_ms as i64),
            }
        }
        // Read before this attempt joins the window.
        let load = if self.live {
            grant_load(source)
        } else {
            GrantLoad::default()
        };
        let raw = self.reply.retry_after.clone();
        let kind = retry_after_kind(raw.as_deref());
        let reason = if self.live { error_reason(body) } else { None };
        let applied_ms = applied.map(telemetry::duration_ms);
        let capped = applied.is_some_and(|applied| applied < asked);
        let outcome = if quota {
            "quota_exhausted"
        } else if gave_up {
            "rate_limited_gave_up"
        } else {
            "rate_limited"
        };
        let attempt_reason = reason.clone();
        self.report(outcome, Some(429), Some(body.len()), |event| {
            event
                .field("retry_after_kind", kind)
                .field("retry_after_s", asked.as_secs())
                .field("applied_wait_ms", applied_ms)
                .field("capped", capped)
                .field("reason", attempt_reason)
        });
        if !self.live {
            return;
        }
        let route = format!("{} {} {}", grant_name(source), self.method, self.endpoint);
        telemetry::note_cause("webapi:rate_limited", route.as_str());
        let probe = &client.probe;
        let episode = if quota {
            None
        } else if probe
            .episode_started_ms
            .compare_exchange(0, clock_ms(), Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            Some("start")
        } else {
            Some("extend")
        };
        let limited_in_episode =
            (!quota).then(|| probe.episode_limited.fetch_add(1, Ordering::Relaxed) + 1);
        let first_in_window = load.since_last_429.is_none_or(|gap| gap > WINDOW);
        let external = first_in_window && load.attempts_30s < EXTERNAL_BELOW;
        let index = grant_index(source);
        let alarm = if quota {
            QUOTA_REPORT[index].ready(QUOTA_EVERY)
        } else {
            // Taken at an episode's start too, so the start counts toward it.
            let ready = RATE_LIMITED_REPORT[index].ready(RATE_LIMITED_EVERY);
            ready || episode == Some("start")
        };
        let record = if alarm {
            telemetry::anomaly("api.rate_limited")
        } else {
            telemetry::crumb("api.rate_limited")
        };
        record
            .field("grant", grant_name(source))
            .field("method", self.method)
            .field("path_template", self.endpoint)
            .field("caller", self.caller)
            .field("attempt", self.attempt)
            .field("retry_after_raw", raw.as_deref().map(telemetry::scrub))
            .field("retry_after_kind", kind)
            .field("retry_after_s", asked.as_secs())
            .field("applied_wait_ms", applied_ms)
            .field("capped", capped)
            .field("queue_write", self.queue_write)
            .field("quota_exhausted", quota)
            .field("reason", reason.clone())
            .field("gave_up", gave_up)
            .field("cooldown_before_ms", before_ms)
            .field("cooldown_after_ms", after_ms)
            .field("episode", episode)
            .field("limited_in_episode", limited_in_episode)
            .field("sent_in_cooldown", self.sent_in_cooldown)
            .field("in_flight", self.in_flight)
            .field("waiters", self.waiters)
            .field("attempts_30s", load.attempts_30s)
            .field("attempts_300s", load.attempts_300s)
            .field("limited_30s", load.limited_30s)
            .field("other_grant_30s", load.other_30s)
            .field(
                "since_last_429_ms",
                load.since_last_429.map(telemetry::duration_ms),
            )
            .field("first_in_window", first_in_window)
            .field("external_suspected", external)
            .field("client_generation", probe.generation)
            .emit();
        if external {
            telemetry::anomaly("api.rate_limited_externally")
                .field("grant", grant_name(source))
                .field("method", self.method)
                .field("path_template", self.endpoint)
                .field("attempts_30s", load.attempts_30s)
                .field("attempts_300s", load.attempts_300s)
                .field("other_grant_30s", load.other_30s)
                .field("retry_after_s", asked.as_secs())
                .field("quota_exhausted", quota)
                .emit();
        }
        if quota {
            let record = if alarm {
                telemetry::event("api.quota_exhausted")
            } else {
                telemetry::crumb("api.quota_exhausted")
            };
            record
                .field("grant", grant_name(source))
                .field("method", self.method)
                .field("path_template", self.endpoint)
                .field("reason", reason)
                .field("attempts_300s", load.attempts_300s)
                .emit();
        }
        if let Some(episode) = episode {
            let record = if episode == "start" {
                telemetry::event("api.cooldown")
            } else {
                telemetry::crumb("api.cooldown")
            };
            record
                .field("phase", episode)
                .field("grant", grant_name(source))
                .field("cause_method", self.method)
                .field("cause_path_template", self.endpoint)
                .field("wait_ms", applied_ms)
                .field("before_ms", before_ms)
                .field("until_ms", after_ms)
                .field("limited_in_episode", limited_in_episode)
                .field("client_generation", probe.generation)
                .emit();
        }
    }
}

impl Drop for RequestProbe<'_> {
    fn drop(&mut self) {
        // The future was dropped before an answer: aborted, superseded, or
        // raced by something else.
        if !self.finished {
            self.finished = true;
            self.report("cancelled", None, None, |event| event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn revoked_provider_cannot_return_or_persist_its_token() {
        let root =
            std::env::temp_dir().join(format!("spotifast-revoked-provider-{}", std::process::id()));
        let store = crate::credentials::Store::in_memory(crate::paths::AppDirs {
            config: root.join("config"),
            state: root.join("state"),
            cache: root.join("cache"),
        });
        let slot = crate::credentials::Slot::Shared;
        let tokens = WebTokens::new(
            reqwest::Client::new(),
            crate::auth::StoredToken {
                client_id: crate::auth::DEFAULT_WEB_CLIENT_ID.into(),
                access_token: "dummy-access".into(),
                refresh_token: "dummy-refresh".into(),
                expires_at: u64::MAX,
                scope: String::new(),
            },
            store.lease(slot),
            ApiSource::Shared,
            std::sync::Arc::new(|_| {}),
        );
        // Verification can use a token in memory without persisting it yet.
        assert_eq!(tokens.access_token(false).await.unwrap(), "dummy-access");
        assert!(store.lease(slot).load().await.unwrap().grant.is_none());
        tokens.remember().await.unwrap();
        assert!(store.lease(slot).load().await.unwrap().grant.is_some());
        store.revoke_spotify().unwrap();
        store.lease(slot).delete().await.unwrap();
        assert!(matches!(
            tokens.access_token(false).await,
            Err(ApiError::SignInExpired { .. })
        ));
        assert_eq!(
            tokens.remember().await,
            Err(crate::credentials::Error::Stale)
        );
        assert!(store.lease(slot).load().await.unwrap().grant.is_none());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn queue_batch_preserves_order_and_duplicates_and_stops_on_failure() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for (fail, rate_limit) in [(false, false), (true, false), (false, true)] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let first_request = Arc::new(tokio::sync::Notify::new());
            let notify = Arc::clone(&first_request);
            let server = tokio::spawn(async move {
                let mut paths = Vec::new();
                let mut cooldown_started: Option<Instant> = None;
                for index in 0..if fail {
                    2
                } else if rate_limit {
                    8
                } else {
                    4
                } {
                    let (mut socket, _) =
                        tokio::time::timeout(Duration::from_secs(5), listener.accept())
                            .await
                            .unwrap()
                            .unwrap();
                    if index == 2 && rate_limit {
                        assert!(cooldown_started.unwrap().elapsed() >= Duration::from_secs(1));
                    }
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        request.push(socket.read_u8().await.unwrap());
                        assert!(request.len() < 8192);
                    }
                    paths.push(
                        String::from_utf8(request)
                            .unwrap()
                            .lines()
                            .next()
                            .unwrap()
                            .to_string(),
                    );
                    if index == 0 {
                        notify.notify_one();
                    }
                    assert!(
                        tokio::time::timeout(Duration::from_millis(10), listener.accept())
                            .await
                            .is_err(),
                        "the next append must wait for this response"
                    );
                    let limited = rate_limit && (1..=4).contains(&index);
                    let status = if limited {
                        "429 Too Many Requests"
                    } else if fail && index == 1 {
                        "403 Forbidden"
                    } else {
                        "204 No Content"
                    };
                    let retry_after = if limited && index == 1 { 1 } else { 0 };
                    cooldown_started = Some(Instant::now());
                    socket.write_all(format!("HTTP/1.1 {status}\r\nRetry-After: {retry_after}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
                }
                paths
            });
            let http = reqwest::Client::builder().no_proxy().build().unwrap();
            let mut client = ApiClient::new(
                http.clone(),
                Arc::new(NetActivity::default()),
                20,
                50,
                ApiSource::Shared,
            );
            client.base_url = Some(format!("http://{address}"));
            client.set_token_provider(Some(TokenProvider::Web(WebTokens::new(
                http,
                crate::auth::StoredToken {
                    access_token: "test-only".into(),
                    expires_at: u64::MAX,
                    ..Default::default()
                },
                crate::credentials::Store::in_memory(crate::paths::AppDirs {
                    config: std::env::temp_dir().join("unused-queue-token/config"),
                    state: std::env::temp_dir().join("unused-queue-token/state"),
                    cache: std::env::temp_dir().join("unused-queue-token/cache"),
                })
                .lease(crate::credentials::Slot::Shared),
                ApiSource::Shared,
                Arc::new(|_| {}),
            ))));
            let mut uris = vec![
                "spotify:track:a".into(),
                "spotify:track:b".into(),
                "spotify:track:a".into(),
            ];
            let (added, result) = if fail {
                client.add_many_to_queue(&uris, Some("phone")).await
            } else {
                let (album, later_song) =
                    tokio::join!(client.add_many_to_queue(&uris, Some("phone")), async {
                        first_request.notified().await;
                        client
                            .add_to_queue("spotify:track:later", Some("phone"))
                            .await
                    });
                later_song.unwrap();
                uris.push("spotify:track:later".into());
                album
            };
            assert_eq!(result.is_err(), fail);
            assert_eq!(added, if fail { 1 } else { 3 });
            if fail {
                assert_eq!(result.unwrap_err().status(), Some(403));
            }
            let paths = server.await.unwrap();
            if rate_limit {
                // Only the rejected song is retried, even beyond the normal
                // request retry budget. The later single still follows the album.
                uris.splice(1..1, std::iter::repeat_n("spotify:track:b".into(), 4));
            }
            assert_eq!(paths.len(), if fail { 2 } else { uris.len() });
            for (path, expected) in paths.iter().zip(uris) {
                assert!(path.starts_with("POST /me/player/queue?"));
                let url = reqwest::Url::parse(&format!(
                    "http://test{}",
                    path.split_whitespace().nth(1).unwrap()
                ))
                .unwrap();
                let query: std::collections::HashMap<_, _> = url.query_pairs().collect();
                assert_eq!(query["uri"], expected);
                assert_eq!(query["device_id"], "phone");
            }
        }
    }

    #[tokio::test]
    async fn cover_upload_sends_raw_base64_jpeg_and_reports_spotify_errors() {
        use std::io::{Read, Write};
        for status in [202, 403, 413] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut received = Vec::new();
                let mut buffer = [0; 1024];
                loop {
                    let count = socket.read(&mut buffer).unwrap();
                    assert!(count > 0);
                    received.extend_from_slice(&buffer[..count]);
                    if let Some(end) = received.windows(4).position(|part| part == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&received[..end]).to_lowercase();
                        let length: usize = headers
                            .lines()
                            .find_map(|line| line.strip_prefix("content-length: "))
                            .unwrap()
                            .parse()
                            .unwrap();
                        if received.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                write!(
                    socket,
                    "HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
                received
            });
            let http = reqwest::Client::new();
            let mut client = ApiClient::new(
                http.clone(),
                Arc::new(NetActivity::default()),
                20,
                50,
                ApiSource::Shared,
            );
            client.base_url = Some(format!("http://{address}"));
            client.set_token_provider(Some(TokenProvider::Web(WebTokens::new(
                http,
                crate::auth::StoredToken {
                    access_token: "test-only".into(),
                    expires_at: u64::MAX,
                    ..Default::default()
                },
                crate::credentials::Store::in_memory(crate::paths::AppDirs {
                    config: std::env::temp_dir().join("unused-cover-token/config"),
                    state: std::env::temp_dir().join("unused-cover-token/state"),
                    cache: std::env::temp_dir().join("unused-cover-token/cache"),
                })
                .lease(crate::credentials::Slot::Shared),
                ApiSource::Shared,
                Arc::new(|_| {}),
            ))));
            let result = client
                .upload_playlist_cover("test-playlist", "/9j/test")
                .await;
            if status == 202 {
                assert!(result.is_ok());
            } else {
                assert_eq!(result.unwrap_err().status(), Some(status));
            }
            let received = String::from_utf8(server.join().unwrap()).unwrap();
            assert!(received.starts_with("PUT /playlists/test-playlist/images HTTP/1.1"));
            assert!(received.to_lowercase().contains("content-type: image/jpeg"));
            assert_eq!(received.split("\r\n\r\n").nth(1), Some("/9j/test"));
        }
    }

    #[tokio::test]
    async fn oversized_cover_is_rejected_before_authentication_or_network() {
        let client = ApiClient::new(
            reqwest::Client::new(),
            Arc::new(NetActivity::default()),
            20,
            50,
            ApiSource::Shared,
        );
        let result = client
            .upload_playlist_cover("test", &"x".repeat(crate::playlist_cover::MAX_PAYLOAD + 1))
            .await;
        assert_eq!(result.unwrap_err().status(), Some(413));
    }

    #[test]
    fn play_request_body_shapes() {
        let context = PlayRequest::context("spotify:album:x").starting_at_uri("spotify:track:y");
        assert_eq!(
            context.body(),
            json!({ "context_uri": "spotify:album:x", "offset": { "uri": "spotify:track:y" } })
        );
        let tracks = PlayRequest::tracks(vec!["spotify:track:a".into()]).starting_at_index(0);
        assert_eq!(
            tracks.body(),
            json!({ "uris": ["spotify:track:a"], "offset": { "position": 0 } })
        );
        // A lone track or episode as a context is sent as the only uri.
        let mut song = PlayRequest::context("spotify:track:t").starting_at_uri("spotify:track:t");
        song.position_ms = 5_000;
        assert_eq!(
            song.body(),
            json!({ "uris": ["spotify:track:t"], "position_ms": 5_000 })
        );
        assert_eq!(
            PlayRequest::context("spotify:episode:e").body(),
            json!({ "uris": ["spotify:episode:e"] })
        );
        // A resume whose song moved past the one-song context plays that song.
        let mut resumed =
            PlayRequest::context("spotify:track:a").starting_at_uri("spotify:track:b");
        resumed.position_ms = 42_000;
        assert_eq!(
            resumed.body(),
            json!({ "uris": ["spotify:track:b"], "position_ms": 42_000 })
        );
    }

    #[test]
    fn telemetry_reports_templates_and_safe_query_keys_only() {
        assert_eq!(
            template("/playlists/37i9dQZF1DXcBWIGoYBM5M/items"),
            "/playlists/{id}/items"
        );
        assert_eq!(
            template("https://api.spotify.com/v1/albums/4aawyAB9vmqN3uQ7FjRGTy/tracks?offset=50"),
            "/albums/{id}/tracks"
        );
        assert_eq!(template("/me/player/next"), "/me/player/next");
        assert_eq!(template("/users/someone/playlists"), "other");
        assert_eq!(
            query_summary(&[
                ("q", "a song someone searched".to_string()),
                ("limit", "20".to_string()),
                ("uris", "spotify:track:a".to_string()),
                ("device_id", "a-device".to_string()),
            ]),
            Some("limit=20".to_string())
        );
        assert_eq!(retry_after_kind(Some("5")), "seconds");
        assert_eq!(
            retry_after_kind(Some("Wed, 21 Oct 2026 07:28:00 GMT")),
            "http_date"
        );
        assert_eq!(retry_after_kind(None), "missing");
        assert_eq!(
            error_reason(r#"{"error":{"status":429,"reason":"QUOTA_EXCEEDED"}}"#),
            Some("QUOTA_EXCEEDED".to_string())
        );
        assert_eq!(error_reason(r#"{"error":{"reason":"free text"}}"#), None);
    }

    #[test]
    fn quota_exhaustion_is_distinct_from_an_ordinary_rate_limit() {
        assert!(is_quota_exhausted(
            r#"{"error":{"status":429,"reason":"QUOTA_EXCEEDED"}}"#
        ));
        assert!(!is_quota_exhausted(
            r#"{"error":{"status":429,"message":"Too many requests"}}"#
        ));
    }

    #[tokio::test]
    async fn cooldown_state_is_owned_by_one_session() {
        let activity = Arc::new(NetActivity::default());
        let shared = ApiClient::new(
            reqwest::Client::new(),
            activity.clone(),
            20,
            50,
            ApiSource::Shared,
        );
        let personal = ApiClient::new(
            reqwest::Client::new(),
            activity,
            10,
            10,
            ApiSource::Personal,
        );
        shared.extend_cooldown(Duration::from_secs(10)).await;
        assert!(*shared.cooldown_until.lock().await > Instant::now());
        assert!(*personal.cooldown_until.lock().await <= Instant::now());
    }
}
