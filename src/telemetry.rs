//! Opt-in diagnostics for a personal build, shipped to Axiom.
//!
//! Nothing is collected or sent unless an Axiom token is configured, either
//! in `telemetry.json` beside `settings.json`:
//!
//! ```json
//! { "axiom_token": "xaat-...", "dataset": "spotifast" }
//! ```
//!
//! or in `SPOTIFAST_AXIOM_TOKEN` (with optional `SPOTIFAST_AXIOM_DATASET` and
//! `SPOTIFAST_AXIOM_URL`). `SPOTIFAST_TELEMETRY=off` turns it off regardless.
//!
//! Callers build an [`event`] and emit it; it costs one atomic load when
//! telemetry is off. Shipped events go through a bounded channel to one
//! background thread that batches, gzips and uploads them, so no caller ever
//! waits on the network. When the channel is full the event is dropped and
//! counted.
//!
//! Kinds of record:
//!
//! - [`event`]: shipped with the next batch (every few seconds);
//! - [`crumb`]: kept only in the in-memory flight recorder (the last few
//!   thousand), unless `verbose` is set in the configuration;
//! - [`anomaly`]: shipped, and ships the flight recorder's last two minutes
//!   with it, each crumb tagged `dump_of` with the anomaly's id, so detail
//!   around a problem arrives without paying for it the rest of the time.
//!
//! Helpers for questions that span modules without passing ids around:
//!
//! - [`intent`] and [`note_cause`] record why something is about to happen
//!   (the user pressed Pause, a remote device sent Play, the output failed),
//!   and [`recent_causes`] answers "did anything ask for this?" when a state
//!   changes;
//! - a [`trace_start`]ed operation (Next, a play request) collects
//!   [`trace_mark`]s from whichever thread reaches each step, and reports
//!   the waterfall and its slowest step when it ends;
//! - [`set_context`] adds a field to every later event (the engine
//!   generation, the output route, whether the personal app's grant is
//!   ready).
//!
//! Realtime code (the audio callback) must not build events: it bumps a
//! [`Counter`] or sets a [`Gauge`], registered with [`register_counters`] or
//! [`register_gauges`], and the heartbeat reports them every 30 seconds.
//!
//! Never put a credential, an authorization code or response, an
//! `Authorization` header, or a URL with its query string in an event.
//! [`scrub`] removes links and token-shaped words from free text.

use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use serde_json::{Map, Value};

/// The configuration file's name, in the config directory.
pub const CONFIG_FILE: &str = "telemetry.json";
/// Unsent events kept across a restart, in the state directory.
const SPOOL_FILE: &str = "telemetry-spool.ndjson";
/// The flight recorder's tail, written synchronously when a panic could not
/// be uploaded (release builds abort right after the hook).
const CRASH_FILE: &str = "telemetry-crash.ndjson";
const DEFAULT_ENDPOINT: &str = "https://us-east-1.aws.edge.axiom.co";
const DEFAULT_DATASET: &str = "spotifast";

/// Events waiting for the uploader thread. A full channel drops new events.
const CHANNEL_CAPACITY: usize = 16_384;
/// Crumbs the flight recorder keeps.
const RECORDER_CAPACITY: usize = 4_000;
/// Events kept while Axiom is unreachable; the oldest go first.
const PENDING_CAPACITY: usize = 50_000;
const BATCH_MAX: usize = 2_000;
const FLUSH_EVERY: Duration = Duration::from_secs(5);
const HEARTBEAT_EVERY: Duration = Duration::from_secs(30);
const WATCH_EVERY: Duration = Duration::from_secs(1);
const NETWORK_EVERY: Duration = Duration::from_secs(5);
/// How far back an anomaly's dump reaches.
const DUMP_WINDOW: Duration = Duration::from_secs(120);
/// One dump per anomaly kind per this long, so a storm stays affordable.
const DUMP_COOLDOWN: Duration = Duration::from_secs(60);
/// Events this soon after a wake or a network change say so.
const AFTERMATH: Duration = Duration::from_secs(120);
/// A trace still open after this long ends as a timeout.
const TRACE_LIMIT: Duration = Duration::from_secs(30);
/// Warnings and errors per minute above which the log is a storm.
const LOG_STORM: u64 = 30;
const MESSAGE_LIMIT: usize = 4_096;

#[derive(Clone, Deserialize)]
pub struct Config {
    #[serde(alias = "token")]
    pub axiom_token: String,
    #[serde(default = "default_dataset")]
    pub dataset: String,
    /// The Axiom ingest base URL: an edge deployment or `https://api.axiom.co`.
    #[serde(default = "default_endpoint")]
    pub endpoint: String,
    /// Ship every crumb as it happens, not only in anomaly dumps.
    #[serde(default)]
    pub verbose: bool,
}

fn default_dataset() -> String {
    DEFAULT_DATASET.to_owned()
}

fn default_endpoint() -> String {
    DEFAULT_ENDPOINT.to_owned()
}

impl std::fmt::Debug for Config {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Config")
            .field("axiom_token", &"<redacted>")
            .field("dataset", &self.dataset)
            .field("endpoint", &self.endpoint)
            .field("verbose", &self.verbose)
            .finish()
    }
}

impl Config {
    /// The environment's configuration, else the file's, else none.
    pub fn load(config_dir: &Path) -> Option<Self> {
        let env = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
        };
        if env("SPOTIFAST_TELEMETRY").is_some_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "off" | "false" | "no"
            )
        }) {
            return None;
        }
        let file = std::fs::read_to_string(config_dir.join(CONFIG_FILE))
            .ok()
            .and_then(|text| Self::parse(&text));
        let mut config = match (env("SPOTIFAST_AXIOM_TOKEN"), file) {
            (Some(token), file) => Self {
                axiom_token: token,
                ..file.unwrap_or_else(|| Self::with_token(String::new()))
            },
            (None, Some(file)) => file,
            (None, None) => return None,
        };
        if let Some(dataset) = env("SPOTIFAST_AXIOM_DATASET") {
            config.dataset = dataset;
        }
        if let Some(endpoint) = env("SPOTIFAST_AXIOM_URL") {
            config.endpoint = endpoint;
        }
        config.valid().then_some(config)
    }

    fn with_token(token: String) -> Self {
        Self {
            axiom_token: token,
            dataset: default_dataset(),
            endpoint: default_endpoint(),
            verbose: false,
        }
    }

    fn parse(text: &str) -> Option<Self> {
        serde_json::from_str::<Self>(text).ok()
    }

    fn valid(&self) -> bool {
        let token = self.axiom_token.trim();
        !token.is_empty()
            && token.bytes().all(|byte| byte.is_ascii_graphic())
            && !self.dataset.is_empty()
            && self
                .dataset
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            && self.endpoint.starts_with("https://")
    }

    /// The ingest URL. The legacy API host names the dataset differently
    /// from the edge deployments.
    fn ingest_url(&self) -> String {
        let base = self.endpoint.trim_end_matches('/');
        if base.ends_with("api.axiom.co") {
            format!("{base}/v1/datasets/{}/ingest", self.dataset)
        } else {
            format!("{base}/v1/ingest/{}", self.dataset)
        }
    }
}

// ---------------------------------------------------------------------------
// Global state

struct Global {
    tx: SyncSender<Message>,
    launch: String,
    install: String,
    start: Instant,
    verbose: bool,
    worker: std::thread::ThreadId,
    state_dir: PathBuf,
}

static GLOBAL: OnceLock<Global> = OnceLock::new();
static ENABLED: AtomicBool = AtomicBool::new(false);
static SEQUENCE: AtomicU64 = AtomicU64::new(0);
static DROPPED: AtomicU64 = AtomicU64::new(0);
static RECORDER: Mutex<VecDeque<Map<String, Value>>> = Mutex::new(VecDeque::new());
static CONTEXT: Mutex<Option<Map<String, Value>>> = Mutex::new(None);
/// `uptime_ms` at the last detected wake and network change, 0 for never.
static WOKE_AT: AtomicU64 = AtomicU64::new(0);
static NETWORK_CHANGED_AT: AtomicU64 = AtomicU64::new(0);
static TIMER_LAG_AT: AtomicU64 = AtomicU64::new(0);

/// Whether the watch thread woke late within `window`: the whole process
/// was throttled (App Nap) or starved, so a late timer elsewhere is no
/// news of its own.
pub fn timer_lag_within(window: Duration) -> bool {
    let at = TIMER_LAG_AT.load(Ordering::Relaxed);
    at != 0 && uptime_ms().saturating_sub(at) <= window.as_millis() as u64
}

enum Message {
    Record(Map<String, Value>),
    Proxy(crate::settings::ProxyConfig),
    Dump { anomaly: String, kind: &'static str },
    Flush(SyncSender<()>),
    Shutdown(SyncSender<()>),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Ship,
    Crumb,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Whether telemetry is on. Guard costly field computations with it.
#[inline]
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// Starts the uploader and watch threads. Does nothing without a
/// configuration, or when called a second time.
pub fn init(config: Option<Config>, proxy: &crate::settings::ProxyConfig, state_dir: &Path) {
    let Some(config) = config else { return };
    if GLOBAL.get().is_some() {
        return;
    }
    let url = config.ingest_url();
    let token = config.axiom_token.trim().to_owned();
    // Built on the uploader thread (system proxy lookup can be slow) and
    // rebuilt by set_proxy, so uploads follow the proxy chosen in Settings.
    let client: Arc<Mutex<Option<reqwest::blocking::Client>>> = Arc::new(Mutex::new(None));
    let uploads = Arc::clone(&client);
    let upload = move |body: &[u8]| -> Upload {
        let Some(client) = lock(&uploads).clone() else {
            return Upload::Retry(None);
        };
        let response = client
            .post(&url)
            .bearer_auth(&token)
            .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
            .header(reqwest::header::CONTENT_ENCODING, "gzip")
            .body(body.to_vec())
            .send();
        match response {
            Ok(response) if response.status().is_success() => Upload::Sent,
            Ok(response) if matches!(response.status().as_u16(), 401 | 403 | 404) => {
                Upload::Refused(response.status().as_u16())
            }
            Ok(response) => Upload::Retry(Some(response.status().as_u16())),
            Err(_) => Upload::Retry(None),
        }
    };
    let configure = move |proxy: &crate::settings::ProxyConfig| {
        // Short timeouts: the uploader must come back to the queue quickly
        // when the network is the thing that is failing. An invalid proxy
        // holds uploads, as it holds every other request.
        let built = crate::http::blocking_builder(proxy).and_then(|builder| {
            builder
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(15))
                .build()
                .map_err(|error| error.without_url().to_string())
        });
        if let Err(error) = &built {
            log::warn!(target: "spotifast::telemetry", "telemetry uploads held: {error}");
        }
        *lock(&client) = built.ok();
    };
    let initial_proxy = proxy.clone();
    let (tx, rx) = mpsc::sync_channel(CHANNEL_CAPACITY);
    let spool = state_dir.join(SPOOL_FILE);
    let crash = state_dir.join(CRASH_FILE);
    let install = install_id(state_dir);
    let launch = random_id(8);
    let verbose = config.verbose;
    let worker = std::thread::Builder::new()
        .name("telemetry".into())
        .spawn(move || {
            configure(&initial_proxy);
            let mut uploader = Uploader::new(Box::new(upload), Some(spool), &RECORDER);
            uploader.configure = Some(Box::new(configure));
            uploader.restore(&crash);
            uploader.run(rx);
        });
    let Ok(worker) = worker else { return };
    let global = Global {
        tx,
        launch,
        install,
        start: Instant::now(),
        verbose,
        worker: worker.thread().id(),
        state_dir: state_dir.to_path_buf(),
    };
    if GLOBAL.set(global).is_err() {
        return;
    }
    register_counters(BUILTIN_COUNTERS);
    register_gauges(BUILTIN_GAUGES);
    ENABLED.store(true, Ordering::Relaxed);
    let _ = std::thread::Builder::new()
        .name("telemetry-watch".into())
        .spawn(watch);
    install_panic_hook();
    install_exit_hook();
    event("app.start")
        .field("pid", std::process::id())
        .field("dataset", config.dataset.as_str())
        .field("verbose", verbose)
        .field(
            "cpus",
            std::thread::available_parallelism().map_or(0, |n| n.get()),
        )
        .field("debug_build", cfg!(debug_assertions))
        .emit();
}

/// Sends what is queued and stops the uploader, waiting at most `timeout`.
/// What could not be sent is kept for the next launch.
pub fn shutdown(timeout: Duration) {
    let Some(global) = GLOBAL.get() else { return };
    if !ENABLED.swap(false, Ordering::Relaxed) {
        return;
    }
    let deadline = Instant::now() + timeout;
    let (done, wait) = mpsc::sync_channel(1);
    let mut message = Message::Shutdown(done);
    // A full channel drains while we wait; never block past the deadline.
    loop {
        match global.tx.try_send(message) {
            Ok(()) => break,
            Err(TrySendError::Full(back)) if Instant::now() < deadline => {
                message = back;
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(_) => return,
        }
    }
    let _ = wait.recv_timeout(deadline.saturating_duration_since(Instant::now()));
}

/// Rebuilds the upload client for a changed or restored proxy setting.
pub fn set_proxy(proxy: &crate::settings::ProxyConfig) {
    if enabled() {
        send(Message::Proxy(proxy.clone()));
    }
}

/// Uploads what is queued, waiting at most `timeout`. Whether the uploader
/// confirmed in time.
pub fn flush(timeout: Duration) -> bool {
    let Some(global) = GLOBAL.get() else {
        return false;
    };
    if !enabled() || std::thread::current().id() == global.worker {
        return false;
    }
    let (done, wait) = mpsc::sync_channel(1);
    global.tx.try_send(Message::Flush(done)).is_ok() && wait.recv_timeout(timeout).is_ok()
}

fn send(message: Message) {
    if let Some(global) = GLOBAL.get()
        && let Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) =
            global.tx.try_send(message)
    {
        DROPPED.fetch_add(1, Ordering::Relaxed);
    }
}

/// Milliseconds since telemetry started, the timeline every event carries.
/// It is monotonic, and on macOS it stops while the machine sleeps.
pub fn uptime_ms() -> u64 {
    GLOBAL
        .get()
        .map_or(0, |global| global.start.elapsed().as_millis() as u64)
}

/// Adds `key` to every later event, or removes it with `Value::Null`.
pub fn set_context(key: &'static str, value: impl Into<Value>) {
    if !enabled() {
        return;
    }
    let value = value.into();
    let mut context = lock(&CONTEXT);
    let context = context.get_or_insert_with(Map::new);
    if value.is_null() {
        context.remove(key);
    } else {
        context.insert(key.to_owned(), value);
    }
}

// ---------------------------------------------------------------------------
// Events

/// An event under construction. Emit it with [`Event::emit`].
#[must_use = "an event does nothing until it is emitted"]
pub struct Event {
    map: Option<Map<String, Value>>,
    kind: Kind,
    anomaly: Option<&'static str>,
}

/// An event that is shipped.
pub fn event(name: &'static str) -> Event {
    Event::new(name, Kind::Ship)
}

/// A detail kept in the flight recorder and shipped only around an anomaly.
/// Not for per-packet or per-frame paths: a few per second at most.
pub fn crumb(name: &'static str) -> Event {
    Event::new(name, Kind::Crumb)
}

/// Something went wrong that we want to understand: shipped as `anomaly`
/// with `kind`, together with the flight recorder's recent crumbs.
pub fn anomaly(kind: &'static str) -> Event {
    let mut event = Event::new("anomaly", Kind::Ship).field("kind", kind);
    if event.map.is_some() {
        event = event.field("anomaly_id", random_id(6));
        event.anomaly = Some(kind);
    }
    event
}

impl Event {
    fn new(name: &'static str, kind: Kind) -> Self {
        if !enabled() {
            return Self {
                map: None,
                kind,
                anomaly: None,
            };
        }
        let mut map = Map::with_capacity(20);
        map.insert("event".into(), Value::from(name));
        Self {
            map: Some(map),
            kind,
            anomaly: None,
        }
    }

    /// Whether this event will be recorded at all.
    pub fn is_live(&self) -> bool {
        self.map.is_some()
    }

    /// Adds a field. `None` values are left out.
    pub fn field(mut self, key: &str, value: impl Into<Value>) -> Self {
        if let Some(map) = &mut self.map {
            let value = value.into();
            if !value.is_null() {
                map.insert(key.to_owned(), value);
            }
        }
        self
    }

    /// Adds a field computed only when the event is live.
    pub fn field_with<V: Into<Value>>(self, key: &str, value: impl FnOnce() -> V) -> Self {
        if self.map.is_some() {
            self.field(key, value())
        } else {
            self
        }
    }

    /// Adds free text after [`scrub`]bing it.
    pub fn text(self, key: &str, value: &str) -> Self {
        if self.map.is_some() {
            let mut value = scrub(value);
            truncate(&mut value, MESSAGE_LIMIT);
            self.field(key, value)
        } else {
            self
        }
    }

    /// Adds a duration in milliseconds, to a tenth.
    pub fn ms(self, key: &str, duration: Duration) -> Self {
        self.field(key, duration_ms(duration))
    }

    /// Adds the milliseconds since `since`.
    pub fn since(self, key: &str, since: Instant) -> Self {
        self.ms(key, since.elapsed())
    }

    /// Adds any serializable value.
    pub fn json(self, key: &str, value: &impl serde::Serialize) -> Self {
        if self.map.is_some() {
            let value = serde_json::to_value(value).unwrap_or(Value::Null);
            self.field(key, value)
        } else {
            self
        }
    }

    /// Adds the most recent cause within `within`: `cause`, `cause_detail`
    /// and `cause_age_ms`, or `cause: "none"`.
    pub fn explained(self, within: Duration) -> Self {
        if self.map.is_none() {
            return self;
        }
        match recent_causes(within).into_iter().next() {
            Some(cause) => self
                .field("cause", cause.kind)
                .field("cause_detail", cause.detail)
                .ms("cause_age_ms", cause.at.elapsed()),
            None => self.field("cause", "none"),
        }
    }

    /// Records the event.
    pub fn emit(self) {
        let Some(mut map) = self.map else { return };
        let Some(global) = GLOBAL.get() else { return };
        stamp(&mut map, global);
        let kind = if self.kind == Kind::Crumb && global.verbose {
            Kind::Ship
        } else {
            self.kind
        };
        // A storm anywhere must not flood the dataset or the channel: past
        // the budget, events stay in the flight recorder instead.
        let kind = match (kind, self.anomaly) {
            (Kind::Ship, None) if !EVENT_BUDGET.take() => {
                metrics::EVENTS_DEMOTED.incr();
                map.insert("demoted".into(), Value::from(true));
                Kind::Crumb
            }
            (Kind::Ship, Some(anomaly)) if !anomaly_allowed(anomaly) => {
                metrics::EVENTS_DEMOTED.incr();
                map.insert("demoted".into(), Value::from(true));
                Kind::Crumb
            }
            (kind, _) => kind,
        };
        match kind {
            Kind::Crumb => record_crumb(map),
            Kind::Ship => {
                let dump = self.anomaly.and_then(|kind| {
                    map.get("anomaly_id")
                        .and_then(Value::as_str)
                        .map(|id| (kind, id.to_owned()))
                });
                send(Message::Record(map));
                if let Some((kind, anomaly)) = dump {
                    send(Message::Dump { anomaly, kind });
                }
            }
        }
    }
}

/// Numbers crumbs in the order they enter the recorder, so a dump can
/// skip what an earlier dump already shipped.
static CRUMB_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn record_crumb(mut map: Map<String, Value>) {
    let mut recorder = lock(&RECORDER);
    let index = CRUMB_SEQUENCE.fetch_add(1, Ordering::Relaxed) + 1;
    map.insert("crumb_seq".into(), Value::from(index));
    if recorder.len() >= RECORDER_CAPACITY {
        recorder.pop_front();
    }
    recorder.push_back(map);
}

/// A token bucket over shipped events: bursts up to its capacity, then
/// its refill rate.
struct Budget {
    state: Mutex<(f64, Option<Instant>)>,
    capacity: f64,
    per_second: f64,
}

impl Budget {
    const fn new(capacity: f64, per_second: f64) -> Self {
        Self {
            state: Mutex::new((capacity, None)),
            capacity,
            per_second,
        }
    }

    fn take(&self) -> bool {
        let mut state = lock(&self.state);
        let now = Instant::now();
        let (tokens, last) = &mut *state;
        if let Some(last) = last {
            let refill = now.duration_since(*last).as_secs_f64() * self.per_second;
            *tokens = (*tokens + refill).min(self.capacity);
        }
        *last = Some(now);
        if *tokens >= 1.0 {
            *tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

static EVENT_BUDGET: Budget = Budget::new(600.0, 20.0);
/// Anomalies of one kind per minute; the rest stay in the recorder.
const ANOMALIES_PER_MINUTE: u32 = 6;
static ANOMALY_COUNTS: Mutex<Option<HashMap<&'static str, (u64, u32)>>> = Mutex::new(None);

fn anomaly_allowed(kind: &'static str) -> bool {
    let minute = uptime_ms() / 60_000;
    let mut counts = lock(&ANOMALY_COUNTS);
    let entry = counts
        .get_or_insert_with(HashMap::new)
        .entry(kind)
        .or_insert((minute, 0));
    if entry.0 != minute {
        *entry = (minute, 0);
    }
    entry.1 += 1;
    entry.1 <= ANOMALIES_PER_MINUTE
}

fn stamp(map: &mut Map<String, Value>, global: &Global) {
    let now = SystemTime::now();
    let uptime = global.start.elapsed().as_millis() as u64;
    if let Some(context) = lock(&CONTEXT).as_ref() {
        for (key, value) in context {
            map.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }
    map.insert("_time".into(), Value::from(rfc3339(now)));
    map.insert(
        "seq".into(),
        Value::from(SEQUENCE.fetch_add(1, Ordering::Relaxed)),
    );
    map.insert("uptime_ms".into(), Value::from(uptime));
    map.insert("launch".into(), Value::from(global.launch.as_str()));
    map.insert("install".into(), Value::from(global.install.as_str()));
    map.insert("v".into(), Value::from(env!("CARGO_PKG_VERSION")));
    map.insert("os".into(), Value::from(std::env::consts::OS));
    map.insert("arch".into(), Value::from(std::env::consts::ARCH));
    let thread = std::thread::current();
    map.insert(
        "thread".into(),
        Value::from(thread.name().unwrap_or("unnamed")),
    );
    for (key, at) in [
        ("since_wake_ms", &WOKE_AT),
        ("since_network_change_ms", &NETWORK_CHANGED_AT),
    ] {
        let at = at.load(Ordering::Relaxed);
        if at != 0 && uptime.saturating_sub(at) <= AFTERMATH.as_millis() as u64 {
            map.insert(key.into(), Value::from(uptime.saturating_sub(at)));
        }
    }
}

/// Milliseconds, to a tenth.
pub fn duration_ms(duration: Duration) -> f64 {
    (duration.as_secs_f64() * 10_000.0).round() / 10.0
}

/// A timed operation: emits `name` with `duration_ms` when ended or dropped.
#[must_use = "a span measures until it is ended or dropped"]
pub struct Span {
    event: Option<Event>,
    start: Instant,
}

/// Starts a [`Span`] that ships.
pub fn span(name: &'static str) -> Span {
    Span {
        event: Some(event(name)),
        start: Instant::now(),
    }
}

/// Starts a [`Span`] kept in the flight recorder.
pub fn crumb_span(name: &'static str) -> Span {
    Span {
        event: Some(crumb(name)),
        start: Instant::now(),
    }
}

impl Span {
    pub fn field(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.set(key, value);
        self
    }

    /// Adds a field to a span already running.
    pub fn set(&mut self, key: &str, value: impl Into<Value>) {
        self.event = self.event.take().map(|event| event.field(key, value));
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    pub fn end(mut self) {
        self.finish();
    }

    /// Ends without recording anything.
    pub fn cancel(mut self) {
        self.event = None;
    }

    fn finish(&mut self) {
        if let Some(event) = self.event.take() {
            event.ms("duration_ms", self.start.elapsed()).emit();
        }
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        self.finish();
    }
}

/// At most one `ready` per interval, for reports from busy paths. Lock-free.
pub struct Throttle {
    last: AtomicU64,
}

impl Throttle {
    pub const fn new() -> Self {
        Self {
            last: AtomicU64::new(0),
        }
    }

    /// Whether at least `every` has passed since the last `true`.
    pub fn ready(&self, every: Duration) -> bool {
        if !enabled() {
            return false;
        }
        let now = uptime_ms().max(1);
        let last = self.last.load(Ordering::Relaxed);
        if last != 0 && now.saturating_sub(last) < every.as_millis() as u64 {
            return false;
        }
        self.last
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }
}

impl Default for Throttle {
    fn default() -> Self {
        Self::new()
    }
}

/// A short random hex id, for correlating the events of one operation.
pub fn random_id(bytes: usize) -> String {
    use rand::RngCore as _;
    let mut buffer = vec![0u8; bytes];
    rand::rng().fill_bytes(&mut buffer);
    buffer.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// A short, stable digest of something private (a device name, a network
/// address), so equal values can be grouped without being shown. Keyed with
/// this installation's random id, so a list of likely names cannot be
/// matched against it.
pub fn digest(value: &str) -> String {
    use std::hash::{Hash as _, Hasher as _};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    "spotifast-telemetry".hash(&mut hasher);
    GLOBAL
        .get()
        .map_or("", |global| global.install.as_str())
        .hash(&mut hasher);
    value.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// A stable random id for this installation, kept in the state directory so
/// events from two machines can be told apart. Not derived from anything.
fn install_id(state_dir: &Path) -> String {
    let path = state_dir.join("telemetry-install-id");
    if let Ok(id) = std::fs::read_to_string(&path) {
        let id = id.trim();
        if id.len() == 16 && id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return id.to_owned();
        }
    }
    let id = random_id(8);
    let temporary = path.with_extension("tmp");
    if std::fs::write(&temporary, &id).is_ok() {
        let _ = crate::util::replace_file(&temporary, &path);
    }
    id
}

// ---------------------------------------------------------------------------
// Intents and causes: what was asked for, so a state change can say whether
// anyone asked for it.

#[derive(Clone, Debug)]
pub struct Intent {
    pub action: &'static str,
    pub source: &'static str,
    pub at: Instant,
    pub id: String,
}

#[derive(Clone, Debug)]
pub struct Cause {
    /// What happened: `intent:pause`, `remote:pause`, `sink:write_error`...
    pub kind: String,
    pub detail: String,
    pub at: Instant,
}

static INTENTS: Mutex<VecDeque<Intent>> = Mutex::new(VecDeque::new());
static CAUSES: Mutex<VecDeque<Cause>> = Mutex::new(VecDeque::new());

/// Records that the user, a media key or a remote asked for `action`
/// (`pause`, `play`, `toggle`, `next`, ...) through `source` (`ui`,
/// `keyboard`, `media_key`, `menu`, `remote`...), returns its correlation id,
/// and notes it as a cause.
pub fn intent(action: &'static str, source: &'static str) -> String {
    let id = random_id(4);
    if !enabled() {
        return id;
    }
    let previous = recent_intent(&[action], Duration::from_millis(600));
    {
        let mut intents = lock(&INTENTS);
        if intents.len() >= 32 {
            intents.pop_front();
        }
        intents.push_back(Intent {
            action,
            source,
            at: Instant::now(),
            id: id.clone(),
        });
    }
    note_cause_quietly(format!("intent:{action}"), source.to_owned());
    let mut record = event("intent")
        .field("action", action)
        .field("source", source)
        .field("intent_id", id.as_str());
    // The same action twice in a blink: key repeat, a double-delivered media
    // command, or two surfaces mapping one press.
    if let Some(previous) = previous {
        record = record
            .field("repeat_of", previous.id.as_str())
            .field("repeat_source", previous.source)
            .ms("repeat_gap_ms", previous.at.elapsed());
    }
    record.emit();
    id
}

/// The most recent intent among `actions` (all when empty) within `within`.
pub fn recent_intent(actions: &[&str], within: Duration) -> Option<Intent> {
    let intents = lock(&INTENTS);
    intents
        .iter()
        .rev()
        .take_while(|intent| intent.at.elapsed() <= within)
        .find(|intent| actions.is_empty() || actions.contains(&intent.action))
        .cloned()
}

/// Notes something that can explain a later state change, and records it
/// as a crumb. `kind` is namespaced: `remote:pause`, `sink:write_error`,
/// `session:lost`, `media:pause`...
pub fn note_cause(kind: impl Into<String>, detail: impl Into<String>) {
    if !enabled() {
        return;
    }
    let kind = kind.into();
    let detail = detail.into();
    crumb("cause")
        .field("cause", kind.as_str())
        .text("detail", &detail)
        .emit();
    note_cause_quietly(kind, detail);
}

fn note_cause_quietly(kind: String, detail: String) {
    let mut causes = lock(&CAUSES);
    if causes.len() >= 64 {
        causes.pop_front();
    }
    causes.push_back(Cause {
        kind,
        detail,
        at: Instant::now(),
    });
}

/// Causes within `within`, newest first.
pub fn recent_causes(within: Duration) -> Vec<Cause> {
    lock(&CAUSES)
        .iter()
        .rev()
        .take_while(|cause| cause.at.elapsed() <= within)
        .cloned()
        .collect()
}

// ---------------------------------------------------------------------------
// Traces: one operation's steps, marked from whichever thread reaches them.

struct Trace {
    name: &'static str,
    id: String,
    start: Instant,
    budget: Duration,
    hops: Vec<(&'static str, Instant)>,
}

static TRACES: Mutex<Vec<Trace>> = Mutex::new(Vec::new());
static OPEN_TRACES: AtomicUsize = AtomicUsize::new(0);

/// Starts trace `name` (`next`, `previous`, `play`, `seek`...), correlated
/// with `id` (usually an intent id). An open trace of the same name ends as
/// `superseded`. Ending later than `budget` raises an anomaly.
pub fn trace_start(name: &'static str, id: &str, budget: Duration) {
    if !enabled() {
        return;
    }
    let superseded = {
        let mut traces = lock(&TRACES);
        let old = traces
            .iter()
            .position(|trace| trace.name == name)
            .map(|index| traces.remove(index));
        traces.push(Trace {
            name,
            id: id.to_owned(),
            start: Instant::now(),
            budget,
            hops: Vec::new(),
        });
        OPEN_TRACES.store(traces.len(), Ordering::Relaxed);
        old
    };
    if let Some(old) = superseded {
        report_trace(old, "superseded");
    }
}

/// Whether any trace is open: a cheap check before marking from busy paths.
#[inline]
pub fn tracing() -> bool {
    OPEN_TRACES.load(Ordering::Relaxed) != 0
}

/// Whether trace `name` is open.
pub fn trace_open(name: &str) -> bool {
    tracing() && lock(&TRACES).iter().any(|trace| trace.name == name)
}

/// Whether trace `name` is open and has reached step `hop`.
pub fn trace_has(name: &str, hop: &str) -> bool {
    tracing()
        && lock(&TRACES)
            .iter()
            .any(|trace| trace.name == name && trace.hops.iter().any(|(seen, _)| *seen == hop))
}

/// Marks step `hop` of trace `name`, once: later marks of the same hop are
/// ignored. Emits `trace.hop` with the time since the start and since the
/// previous step.
pub fn trace_mark(name: &'static str, hop: &'static str) {
    if !tracing() {
        return;
    }
    let mark = {
        let mut traces = lock(&TRACES);
        let Some(trace) = traces.iter_mut().find(|trace| trace.name == name) else {
            return;
        };
        if trace.hops.iter().any(|(seen, _)| *seen == hop) {
            return;
        }
        let now = Instant::now();
        let previous = trace.hops.last().map_or(trace.start, |(_, at)| *at);
        trace.hops.push((hop, now));
        (
            trace.id.clone(),
            now.duration_since(trace.start),
            now.duration_since(previous),
        )
    };
    event("trace.hop")
        .field("trace", name)
        .field("trace_id", mark.0)
        .field("hop", hop)
        .ms("at_ms", mark.1)
        .ms("step_ms", mark.2)
        .emit();
}

/// Ends trace `name` with `outcome` (`ok`, `failed`, `cancelled`...).
pub fn trace_end(name: &'static str, outcome: &'static str) {
    if !tracing() {
        return;
    }
    let ended = {
        let mut traces = lock(&TRACES);
        let ended = traces
            .iter()
            .position(|trace| trace.name == name)
            .map(|index| traces.remove(index));
        OPEN_TRACES.store(traces.len(), Ordering::Relaxed);
        ended
    };
    if let Some(trace) = ended {
        report_trace(trace, outcome);
    }
}

fn expire_traces() {
    if !tracing() {
        return;
    }
    let expired: Vec<Trace> = {
        let mut traces = lock(&TRACES);
        let (expired, open) = std::mem::take(&mut *traces)
            .into_iter()
            .partition(|trace| trace.start.elapsed() >= TRACE_LIMIT);
        *traces = open;
        OPEN_TRACES.store(traces.len(), Ordering::Relaxed);
        expired
    };
    for trace in expired {
        report_trace(trace, "timeout");
    }
}

fn report_trace(trace: Trace, outcome: &'static str) {
    let total = trace.start.elapsed();
    let mut previous = trace.start;
    let mut slowest: Option<(&str, Duration)> = None;
    let hops: Vec<Value> = trace
        .hops
        .iter()
        .map(|(hop, at)| {
            let step = at.duration_since(previous);
            previous = *at;
            if slowest.is_none_or(|(_, longest)| step > longest) {
                slowest = Some((hop, step));
            }
            serde_json::json!({
                "hop": hop,
                "at_ms": duration_ms(at.duration_since(trace.start)),
                "step_ms": duration_ms(step),
            })
        })
        .collect();
    let slow = outcome == "timeout" || (outcome == "ok" && total > trace.budget);
    let record = if slow {
        anomaly("trace.slow")
    } else {
        event("trace.done")
    };
    record
        .field("trace", trace.name)
        .field("trace_id", trace.id)
        .field("outcome", outcome)
        .ms("total_ms", total)
        .ms("budget_ms", trace.budget)
        .field("hops", hops)
        .field_with("slowest_hop", || slowest.map(|(hop, _)| hop.to_owned()))
        .field_with("slowest_ms", || slowest.map(|(_, step)| duration_ms(step)))
        .emit();
}

// ---------------------------------------------------------------------------
// Counters and gauges, safe for realtime code: one relaxed atomic each.

pub struct Counter {
    pub name: &'static str,
    value: AtomicU64,
}

impl Counter {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            value: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn add(&self, amount: u64) {
        self.value.fetch_add(amount, Ordering::Relaxed);
    }

    #[inline]
    pub fn incr(&self) {
        self.add(1);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// A value the heartbeat reports as it stands (`Gauge::last`) or as the
/// largest value since the previous heartbeat (`Gauge::peak`).
pub struct Gauge {
    pub name: &'static str,
    value: AtomicI64,
    peak: bool,
}

impl Gauge {
    /// Reports the latest [`Gauge::set`].
    pub const fn last(name: &'static str) -> Self {
        Self {
            name,
            value: AtomicI64::new(i64::MIN),
            peak: false,
        }
    }

    /// Reports the largest [`Gauge::raise`] since the previous heartbeat.
    pub const fn peak(name: &'static str) -> Self {
        Self {
            name,
            value: AtomicI64::new(i64::MIN),
            peak: true,
        }
    }

    #[inline]
    pub fn set(&self, value: i64) {
        self.value.store(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn raise(&self, value: i64) {
        self.value.fetch_max(value, Ordering::Relaxed);
    }

    /// The value, or `None` before the first `set` or `raise`.
    pub fn get(&self) -> Option<i64> {
        let value = self.value.load(Ordering::Relaxed);
        (value != i64::MIN).then_some(value)
    }

    fn report(&self) -> Option<i64> {
        if self.peak {
            let value = self.value.swap(i64::MIN, Ordering::Relaxed);
            (value != i64::MIN).then_some(value)
        } else {
            self.get()
        }
    }
}

static COUNTER_REGISTRY: Mutex<Vec<&'static Counter>> = Mutex::new(Vec::new());
static SAMPLERS: Mutex<Vec<fn()>> = Mutex::new(Vec::new());

/// Runs `sampler` on the watch thread about once a second, for checks that
/// read atomics left by realtime code (a stuck writer, a closed gate).
pub fn register_sampler(sampler: fn()) {
    let mut samplers = lock(&SAMPLERS);
    if !samplers
        .iter()
        .any(|known| std::ptr::fn_addr_eq(*known, sampler))
    {
        samplers.push(sampler);
    }
}
static GAUGE_REGISTRY: Mutex<Vec<&'static Gauge>> = Mutex::new(Vec::new());

/// Adds counters to the heartbeat. Call from ordinary code (not the audio
/// callback); registering twice is harmless.
pub fn register_counters(counters: &[&'static Counter]) {
    let mut registry = lock(&COUNTER_REGISTRY);
    for counter in counters {
        if !registry.iter().any(|known| std::ptr::eq(*known, *counter)) {
            registry.push(counter);
        }
    }
}

/// Adds gauges to the heartbeat, as [`register_counters`].
pub fn register_gauges(gauges: &[&'static Gauge]) {
    let mut registry = lock(&GAUGE_REGISTRY);
    for gauge in gauges {
        if !registry.iter().any(|known| std::ptr::eq(*known, *gauge)) {
            registry.push(gauge);
        }
    }
}

/// Telemetry's own metrics.
pub mod metrics {
    use super::{Counter, Gauge};

    pub static LOG_WARNINGS: Counter = Counter::new("log_warnings");
    pub static LOG_ERRORS: Counter = Counter::new("log_errors");
    pub static UI_FRAMES: Counter = Counter::new("ui_frames");
    pub static UI_SLOW_FRAMES: Counter = Counter::new("ui_slow_frames");
    pub static EVENTS_DEMOTED: Counter = Counter::new("events_demoted");
    pub static LOG_SUPPRESSED: Counter = Counter::new("log_suppressed");
    pub static TIMER_LAG_TICKS: Counter = Counter::new("system_timer_lag_ticks");

    pub static UI_FRAME_MAX_US: Gauge = Gauge::peak("ui_frame_max_us");
    pub static TIMER_LAG_MAX_MS: Gauge = Gauge::peak("system_timer_lag_max_ms");
}

const BUILTIN_COUNTERS: &[&Counter] = &[
    &metrics::LOG_WARNINGS,
    &metrics::LOG_ERRORS,
    &metrics::UI_FRAMES,
    &metrics::UI_SLOW_FRAMES,
    &metrics::EVENTS_DEMOTED,
    &metrics::LOG_SUPPRESSED,
    &metrics::TIMER_LAG_TICKS,
];

const BUILTIN_GAUGES: &[&Gauge] = &[&metrics::UI_FRAME_MAX_US, &metrics::TIMER_LAG_MAX_MS];

// ---------------------------------------------------------------------------
// UI frames: a slow frame is reported when it ends, and a frame that never
// ends is reported by the watch thread while it is still stuck.

/// Milliseconds since start when the current frame began, or 0 outside one.
static FRAME_BEGAN: AtomicU64 = AtomicU64::new(0);
static FRAME_HANG_REPORTED: AtomicBool = AtomicBool::new(false);
const SLOW_FRAME: Duration = Duration::from_millis(50);
const VERY_SLOW_FRAME: Duration = Duration::from_millis(250);
const HUNG_FRAME: Duration = Duration::from_secs(2);

/// Measures one UI frame until dropped.
pub struct Frame {
    start: Instant,
}

pub fn frame() -> Option<Frame> {
    if enabled() {
        metrics::UI_FRAMES.incr();
    }
    frame_part()
}

/// Measures a further part of a frame already counted by [`frame`].
pub fn frame_part() -> Option<Frame> {
    if !enabled() {
        return None;
    }
    FRAME_BEGAN.store(uptime_ms().max(1), Ordering::Relaxed);
    Some(Frame {
        start: Instant::now(),
    })
}

impl Drop for Frame {
    fn drop(&mut self) {
        FRAME_BEGAN.store(0, Ordering::Relaxed);
        let elapsed = self.start.elapsed();
        let micros = elapsed.as_micros().min(i64::MAX as u128) as i64;
        metrics::UI_FRAME_MAX_US.raise(micros);
        if FRAME_HANG_REPORTED.swap(false, Ordering::Relaxed) {
            event("ui.hang_ended").ms("duration_ms", elapsed).emit();
        }
        if elapsed >= SLOW_FRAME {
            metrics::UI_SLOW_FRAMES.incr();
            let record = if elapsed >= VERY_SLOW_FRAME {
                event("ui.slow_frame")
            } else {
                crumb("ui.slow_frame")
            };
            record.ms("duration_ms", elapsed).emit();
        }
    }
}

// ---------------------------------------------------------------------------
// The watch thread: heartbeat, sleep, timer lag, network changes, hung
// frames, log storms and stale traces.

fn watch() {
    let mut last_wall = SystemTime::now();
    let mut last_mono = Instant::now();
    let mut last_beat = Instant::now();
    let mut last_network_check = Instant::now();
    let mut network = Network::now();
    let mut previous: HashMap<&'static str, u64> = HashMap::new();
    let mut usage = Usage::now();
    let mut storm_window = (Instant::now(), 0u64);
    let mut last_storm: Option<Instant> = None;
    let mut lagging = false;
    let mut late_ticks = 0u64;
    static TIMER_LAG_REPORT: Throttle = Throttle::new();
    event("system.network")
        .field("interfaces", network.names.clone())
        .field("fingerprint", network.fingerprint.as_str())
        .field("ipv4", network.ipv4)
        .field("ipv6", network.ipv6)
        .emit();
    while enabled() {
        std::thread::sleep(WATCH_EVERY);
        let wall = SystemTime::now();
        let mono = Instant::now();
        let mono_delta = mono.duration_since(last_mono);
        match wall.duration_since(last_wall) {
            Ok(wall_delta) => {
                // macOS's monotonic clock stops while the machine sleeps; the
                // wall clock does not. The difference is the nap.
                if let Some(gap) = wall_delta.checked_sub(mono_delta)
                    && gap >= Duration::from_secs(3)
                {
                    WOKE_AT.store(uptime_ms().max(1), Ordering::Relaxed);
                    note_cause_quietly("system:wake".into(), String::new());
                    event("system.sleep_detected").ms("slept_ms", gap).emit();
                }
                // Asked to wake after a second and woken much later, with
                // both clocks agreeing: the process was starved or throttled
                // (App Nap, CPU pressure, swapping).
                if mono_delta >= WATCH_EVERY + Duration::from_millis(750) {
                    metrics::TIMER_LAG_TICKS.incr();
                    metrics::TIMER_LAG_MAX_MS
                        .raise(mono_delta.as_millis().min(i64::MAX as u128) as i64);
                    late_ticks += 1;
                    if !lagging {
                        note_cause_quietly("system:timer_lag".into(), String::new());
                    }
                    if TIMER_LAG_REPORT.ready(Duration::from_secs(60)) {
                        event("system.timer_lag")
                            .ms("expected_ms", WATCH_EVERY)
                            .ms("actual_ms", mono_delta)
                            .field("late_ticks", late_ticks)
                            .emit();
                        late_ticks = 0;
                    }
                    TIMER_LAG_AT.store(uptime_ms().max(1), Ordering::Relaxed);
                    lagging = true;
                } else {
                    lagging = false;
                }
            }
            Err(error) => {
                event("system.clock_jump_back")
                    .ms("jump_ms", error.duration())
                    .emit();
            }
        }
        last_wall = wall;
        last_mono = mono;

        let began = FRAME_BEGAN.load(Ordering::Relaxed);
        if began != 0 {
            let stuck = Duration::from_millis(uptime_ms().saturating_sub(began));
            if stuck >= HUNG_FRAME && !FRAME_HANG_REPORTED.swap(true, Ordering::Relaxed) {
                anomaly("ui.hang").ms("stuck_ms", stuck).emit();
            }
        }

        expire_traces();
        let samplers = lock(&SAMPLERS).clone();
        for sampler in samplers {
            sampler();
        }

        if last_network_check.elapsed() >= NETWORK_EVERY {
            last_network_check = Instant::now();
            let now = Network::now();
            if now.fingerprint != network.fingerprint {
                NETWORK_CHANGED_AT.store(uptime_ms().max(1), Ordering::Relaxed);
                note_cause_quietly("system:network_change".into(), now.names.join(","));
                event("system.network_changed")
                    .field("interfaces", now.names.clone())
                    .field("previous_interfaces", network.names.clone())
                    .field("fingerprint", now.fingerprint.as_str())
                    .field("previous_fingerprint", network.fingerprint.as_str())
                    .field("ipv4", now.ipv4)
                    .field("ipv6", now.ipv6)
                    .emit();
                network = now;
            }
        }

        let problems = metrics::LOG_WARNINGS.get() + metrics::LOG_ERRORS.get();
        if storm_window.0.elapsed() >= Duration::from_secs(60) {
            let rate = problems.saturating_sub(storm_window.1);
            if rate > LOG_STORM
                && last_storm.is_none_or(|last| last.elapsed() >= Duration::from_secs(600))
            {
                last_storm = Some(Instant::now());
                anomaly("log.storm").field("per_minute", rate).emit();
            }
            storm_window = (Instant::now(), problems);
        }

        if last_beat.elapsed() >= HEARTBEAT_EVERY {
            let interval = last_beat.elapsed();
            last_beat = Instant::now();
            let mut beat = event("heartbeat").ms("interval_ms", interval);
            let counters = lock(&COUNTER_REGISTRY).clone();
            for counter in counters {
                let value = counter.get();
                let delta = value.saturating_sub(previous.get(counter.name).copied().unwrap_or(0));
                previous.insert(counter.name, value);
                beat = beat.field(counter.name, delta);
            }
            let gauges = lock(&GAUGE_REGISTRY).clone();
            for gauge in gauges {
                beat = beat.field(gauge.name, gauge.report());
            }
            let now = Usage::now();
            beat = now.describe(&usage, interval, beat);
            usage = now;
            beat.field("events_dropped", DROPPED.load(Ordering::Relaxed))
                .field("events_emitted", SEQUENCE.load(Ordering::Relaxed))
                .field("recorder_len", lock(&RECORDER).len())
                .field("open_traces", OPEN_TRACES.load(Ordering::Relaxed))
                .emit();
        }
    }
}

/// Process resource use from `getrusage`, where there is one.
#[derive(Clone, Copy, Default)]
struct Usage {
    cpu: Duration,
    max_rss_bytes: u64,
    involuntary_switches: u64,
    major_faults: u64,
}

impl Usage {
    #[cfg(unix)]
    fn now() -> Self {
        // SAFETY: getrusage fills the zeroed struct it is given.
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
            return Self::default();
        }
        let time = |value: libc::timeval| {
            Duration::new(
                value.tv_sec.max(0) as u64,
                (value.tv_usec.max(0) as u32) * 1_000,
            )
        };
        // Linux reports kilobytes, macOS bytes.
        let rss_unit = if cfg!(target_os = "macos") { 1 } else { 1024 };
        Self {
            cpu: time(usage.ru_utime) + time(usage.ru_stime),
            max_rss_bytes: usage.ru_maxrss.max(0) as u64 * rss_unit,
            involuntary_switches: usage.ru_nivcsw.max(0) as u64,
            major_faults: usage.ru_majflt.max(0) as u64,
        }
    }

    #[cfg(not(unix))]
    fn now() -> Self {
        Self::default()
    }

    fn describe(&self, before: &Self, interval: Duration, event: Event) -> Event {
        let cpu = self.cpu.saturating_sub(before.cpu);
        let share = if interval.is_zero() {
            0.0
        } else {
            (cpu.as_secs_f64() / interval.as_secs_f64() * 1000.0).round() / 10.0
        };
        event
            .field("cpu_percent", share)
            .field("max_rss_mb", self.max_rss_bytes / (1024 * 1024))
            .field(
                "involuntary_switches",
                self.involuntary_switches
                    .saturating_sub(before.involuntary_switches),
            )
            .field(
                "major_faults",
                self.major_faults.saturating_sub(before.major_faults),
            )
    }
}

/// The machine's active network interfaces: names, address families, and a
/// digest of the addresses (never the addresses themselves), so a Wi-Fi
/// roam, a VPN or a new DHCP lease shows as a change.
#[derive(Default)]
struct Network {
    names: Vec<String>,
    fingerprint: String,
    ipv4: usize,
    ipv6: usize,
}

impl Network {
    #[cfg(unix)]
    fn now() -> Self {
        let mut list: *mut libc::ifaddrs = std::ptr::null_mut();
        // SAFETY: getifaddrs allocates a list we walk read-only and free.
        if unsafe { libc::getifaddrs(&mut list) } != 0 {
            return Self::default();
        }
        let mut entries = Vec::new();
        let mut cursor = list;
        while !cursor.is_null() {
            // SAFETY: cursor is a node of the list getifaddrs returned.
            let entry = unsafe { &*cursor };
            cursor = entry.ifa_next;
            let flags = entry.ifa_flags;
            if flags & libc::IFF_UP as libc::c_uint == 0
                || flags & libc::IFF_LOOPBACK as libc::c_uint != 0
                || entry.ifa_addr.is_null()
                || entry.ifa_name.is_null()
            {
                continue;
            }
            // SAFETY: ifa_name is a NUL-terminated string owned by the list.
            let name = unsafe { std::ffi::CStr::from_ptr(entry.ifa_name) }
                .to_string_lossy()
                .into_owned();
            // SAFETY: ifa_addr is non-null and starts with a sockaddr.
            let family = i32::from(unsafe { (*entry.ifa_addr).sa_family });
            let address = match family {
                libc::AF_INET => {
                    // SAFETY: the family says this is a sockaddr_in.
                    let address = unsafe { &*(entry.ifa_addr as *const libc::sockaddr_in) };
                    format!("4:{}", address.sin_addr.s_addr)
                }
                libc::AF_INET6 => {
                    // SAFETY: the family says this is a sockaddr_in6.
                    let address = unsafe { &*(entry.ifa_addr as *const libc::sockaddr_in6) };
                    let bytes = address.sin6_addr.s6_addr;
                    // Link-local addresses (fe80::/10) churn without meaning.
                    if bytes[0] == 0xfe && bytes[1] & 0xc0 == 0x80 {
                        continue;
                    }
                    format!("6:{bytes:?}")
                }
                _ => continue,
            };
            entries.push((name, family, address));
        }
        // SAFETY: list came from getifaddrs and is freed once.
        unsafe { libc::freeifaddrs(list) };
        entries.sort();
        let mut names: Vec<String> = entries.iter().map(|(name, ..)| name.clone()).collect();
        names.dedup();
        let joined = entries
            .iter()
            .map(|(name, _, address)| format!("{name}={address}"))
            .collect::<Vec<_>>()
            .join(";");
        Self {
            names,
            fingerprint: digest(&joined),
            ipv4: entries
                .iter()
                .filter(|(_, family, _)| *family == libc::AF_INET)
                .count(),
            ipv6: entries
                .iter()
                .filter(|(_, family, _)| *family == libc::AF_INET6)
                .count(),
        }
    }

    #[cfg(not(unix))]
    fn now() -> Self {
        Self::default()
    }
}

// ---------------------------------------------------------------------------
// Log tap, panics and exit

/// The default log filter's additions while telemetry is on: librespot's
/// account of loading, fetching and sessions at info, and Spotify Connect's
/// and the session's decisions at debug (kept as crumbs). They also reach
/// the local log file.
pub const LOG_FILTER_EXTRA: &str = "librespot_playback=info,librespot_connect=info,librespot_core=info,librespot_audio=info,librespot_connect::spirc=debug,librespot_core::session=debug";

/// Targets whose messages may quote credentials or authorization responses.
/// Only their level and target are recorded.
fn sensitive_target(target: &str) -> bool {
    [
        "oauth",
        "auth",
        "credential",
        "login5",
        "token",
        "keyring",
        "keychain",
        "dealer",
    ]
    .iter()
    .any(|word| target.contains(word))
}

enum SiteAllows {
    Ship,
    Record,
    Nothing,
}

/// Lines per call site per minute: the first few ship, more go to the
/// flight recorder, and past that only the counters see them.
const LOG_SHIP_PER_MINUTE: u32 = 5;
const LOG_RECORD_PER_MINUTE: u32 = 60;
/// (target, line) to (minute, lines in that minute).
type LogSites = HashMap<(String, u32), (u64, u32)>;
static LOG_SITES: Mutex<Option<LogSites>> = Mutex::new(None);

fn log_site_allows(target: &str, line: u32, wants_ship: bool) -> SiteAllows {
    let minute = uptime_ms() / 60_000;
    let mut sites = lock(&LOG_SITES);
    let sites = sites.get_or_insert_with(HashMap::new);
    if sites.len() > 4_096 {
        sites.clear();
    }
    let entry = sites
        .entry((target.to_owned(), line))
        .or_insert((minute, 0));
    if entry.0 != minute {
        *entry = (minute, 0);
    }
    entry.1 += 1;
    match entry.1 {
        count if wants_ship && count <= LOG_SHIP_PER_MINUTE => SiteAllows::Ship,
        count if count <= LOG_RECORD_PER_MINUTE => SiteAllows::Record,
        _ => SiteAllows::Nothing,
    }
}

/// Messages that name the account.
fn sensitive_message(message: &str) -> bool {
    message.starts_with("Authenticated as") || message.contains("username")
}

/// A [`fastframe_log::Redactor`] that records each line the logger writes
/// and leaves the line itself unchanged. Warnings and errors ship; info and
/// debug go to the flight recorder, except Spotifast's own info, which ships.
pub fn log_tap(record: &log::Record<'_>, message: &str) -> Option<Cow<'static, str>> {
    if !enabled() {
        return None;
    }
    let target = record.target();
    if target.starts_with("spotifast::telemetry") {
        return None;
    }
    let level = record.level();
    match level {
        log::Level::Error => metrics::LOG_ERRORS.incr(),
        log::Level::Warn => metrics::LOG_WARNINGS.incr(),
        _ => {}
    }
    // Spotify Connect logs each request it handles before acting on it, so
    // a pause from the phone is a known cause by the time the player
    // reports it. The sending device's id is not kept.
    if target == "librespot_connect::spirc"
        && let Some(rest) = message.strip_prefix("handling: '")
        && let Some(command) = rest.split('\'').next()
    {
        let command = command.trim_start_matches("endpoint: ");
        note_cause_quietly("connect:request".into(), command.to_owned());
    }
    let wants_ship =
        level <= log::Level::Warn || (level == log::Level::Info && target.starts_with("spotifast"));
    let ship = match log_site_allows(target, record.line().unwrap_or(0), wants_ship) {
        SiteAllows::Ship => true,
        SiteAllows::Record => false,
        SiteAllows::Nothing => {
            metrics::LOG_SUPPRESSED.incr();
            return None;
        }
    };
    if wants_ship && !ship {
        metrics::LOG_SUPPRESSED.incr();
    }
    let record_event = if ship { event("log") } else { crumb("log") };
    let mut record_event = record_event
        .field("level", level.as_str())
        .field("target", target);
    // fastframe-audio names the device in lines logged before Spotifast
    // can register the name as private.
    let device_detail = target.starts_with("fastframe_audio") && level > log::Level::Warn;
    if sensitive_target(target) || sensitive_message(message) || device_detail {
        record_event = record_event.field("message", "<withheld: sensitive>");
    } else {
        record_event = record_event.text("message", message);
    }
    if let Some(line) = record.line() {
        record_event = record_event.field("line", line);
    }
    record_event.emit();
    None
}

fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if enabled() {
            let message = info
                .payload()
                .downcast_ref::<&str>()
                .map(|text| (*text).to_owned())
                .or_else(|| info.payload().downcast_ref::<String>().cloned())
                .unwrap_or_default();
            let mut backtrace =
                scrub_lines(&std::backtrace::Backtrace::force_capture().to_string());
            truncate(&mut backtrace, 16 * 1024);
            let location = info
                .location()
                .map(|location| format!("{}:{}", location.file(), location.line()));
            let panic = anomaly("panic")
                .text("message", &message)
                .field("location", location)
                .field("backtrace", backtrace);
            // Kept aside so it survives an unreachable Axiom: release builds
            // abort as soon as this hook returns.
            let copy = panic.map.clone();
            panic.emit();
            if !flush(Duration::from_secs(3))
                && let Some(mut copy) = copy
                && let Some(global) = GLOBAL.get()
            {
                stamp(&mut copy, global);
                write_crash(global, copy);
            }
        }
        previous(info);
    }));
}

/// Writes the panic and the flight recorder's tail synchronously, for the
/// next launch to send.
fn write_crash(global: &Global, panic: Map<String, Value>) {
    let path = global.state_dir.join(CRASH_FILE);
    let Ok(mut file) = std::fs::File::create(&path) else {
        return;
    };
    let recorder = lock(&RECORDER);
    let tail = recorder.len().saturating_sub(500);
    for map in recorder.iter().skip(tail).chain(std::iter::once(&panic)) {
        if serde_json::to_writer(&mut file, map).is_err() || file.write_all(b"\n").is_err() {
            return;
        }
    }
    let _ = file.sync_all();
}

/// On macOS, Cmd+Q and logout end the process inside AppKit, so the shell
/// never returns to send the last events. `exit` still runs atexit handlers.
#[cfg(unix)]
fn install_exit_hook() {
    extern "C" fn at_exit() {
        if enabled() {
            event("app.exit").field("via", "atexit").emit();
            shutdown(Duration::from_secs(2));
        }
    }
    // SAFETY: registering a plain extern "C" function that does not unwind.
    unsafe { libc::atexit(at_exit) };
}

#[cfg(not(unix))]
fn install_exit_hook() {}

// ---------------------------------------------------------------------------
// Redaction

static PRIVATE_TERMS: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Registers a private string that free text must not carry, such as an
/// audio device's name ("Someone's AirPods"): [`scrub`] replaces it with its
/// [`digest`]. Call before anything that may log it.
pub fn private_term(term: &str) {
    let term = term.trim();
    if !enabled() || term.chars().count() < 3 {
        return;
    }
    let mut terms = lock(&PRIVATE_TERMS);
    if !terms.iter().any(|known| known == term) {
        if terms.len() >= 64 {
            terms.remove(0);
        }
        terms.push(term.to_owned());
        // Longest first, so a name is never half-replaced by a shorter one.
        terms.sort_by_key(|known| std::cmp::Reverse(known.len()));
    }
}

/// `text` without links (which can carry tokens in their query), without
/// token-shaped words (long runs of base64 or hex) and without registered
/// [`private_term`]s. Spotify IDs (22 characters) and URIs survive.
pub fn scrub(text: &str) -> String {
    let text = replace_private_terms(text, &lock(&PRIVATE_TERMS));
    let text = redact_user_uris(&text);
    let text = match std::env::var("HOME") {
        Ok(home) if home.len() > 1 && text.contains(home.as_str()) => {
            Cow::Owned(text.replace(home.as_str(), "~"))
        }
        _ => text,
    };
    text.lines()
        .map(|line| fastframe_log::redact::words(line, private_word))
        .collect::<Vec<_>>()
        .join("\n")
}

/// `spotify:user:<account>:...` with the account replaced: the Liked Songs
/// context and old playlist URIs name the account.
fn redact_user_uris(text: &str) -> Cow<'_, str> {
    const PREFIX: &str = "spotify:user:";
    if !text.contains(PREFIX) {
        return Cow::Borrowed(text);
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(index) = rest.find(PREFIX) {
        out.push_str(&rest[..index + PREFIX.len()]);
        rest = &rest[index + PREFIX.len()..];
        let end = rest
            .find(|c: char| c == ':' || c.is_whitespace() || "\"'<>()[]{},;".contains(c))
            .unwrap_or(rest.len());
        out.push_str("{user}");
        rest = &rest[end..];
    }
    out.push_str(rest);
    Cow::Owned(out)
}

fn replace_private_terms<'a>(text: &'a str, terms: &[String]) -> Cow<'a, str> {
    let mut text = Cow::Borrowed(text);
    for term in terms {
        if text.contains(term.as_str()) {
            let replacement = format!("<private:{}>", digest(term));
            text = Cow::Owned(text.replace(term.as_str(), &replacement));
        }
    }
    text
}

/// [`scrub`] that keeps each line's indentation, for backtraces.
fn scrub_lines(text: &str) -> String {
    text.lines()
        .map(|line| {
            let indent = line.len() - line.trim_start().len();
            format!("{}{}", &line[..indent], scrub(line))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn private_word(word: &str) -> bool {
    if fastframe_log::redact::is_link(word) {
        return true;
    }
    let lower = word.to_ascii_lowercase();
    if [
        "bearer",
        "token=",
        "code=",
        "secret",
        "password",
        "authorization",
        "access_token",
        "refresh_token",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
    {
        return true;
    }
    // File paths carry account names (home directories, per-account cache
    // folders). Web API paths and Rust module paths survive.
    let unquoted =
        word.trim_matches(|c: char| matches!(c, '"' | '\'' | '(' | ')' | '[' | ']' | ',' | ':'));
    if word.contains('\\') || (unquoted.matches('/').count() >= 2 && !unquoted.starts_with("/v1/"))
    {
        return true;
    }
    let core = word.trim_matches(|c: char| !c.is_ascii_alphanumeric());
    core.len() >= 40
        && core.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'=' | b'+' | b'/')
        })
        && !core.contains("::")
}

fn truncate(text: &mut String, limit: usize) {
    if text.len() > limit {
        let mut end = limit;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        text.push('…');
    }
}

/// A Web API or spclient path with Spotify IDs, user names and numbers
/// replaced, for grouping requests: `/v1/playlists/{id}/tracks`. The query
/// string is dropped.
pub fn path_template(path: &str) -> String {
    let path = path.split(['?', '#']).next().unwrap_or("");
    let mut previous = "";
    path.split('/')
        .map(|segment| {
            let templated = if segment.is_empty() {
                segment.to_owned()
            } else if matches!(previous, "users" | "user") {
                "{user}".to_owned()
            } else if segment.bytes().all(|byte| byte.is_ascii_digit()) {
                "{n}".to_owned()
            } else if spotify_id_like(segment) || segment.contains(':') {
                "{id}".to_owned()
            } else {
                segment.to_owned()
            };
            previous = segment;
            templated
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn spotify_id_like(segment: &str) -> bool {
    (segment.len() == 22 && segment.bytes().all(|byte| byte.is_ascii_alphanumeric()))
        || (segment.len() >= 32 && segment.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

// ---------------------------------------------------------------------------
// Time

/// RFC 3339 in UTC with milliseconds, without a date crate.
fn rfc3339(time: SystemTime) -> String {
    let since = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let seconds = since.as_secs();
    let (year, month, day) = civil_from_days((seconds / 86_400) as i64);
    let rest = seconds % 86_400;
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60,
        since.subsec_millis()
    )
}

/// Howard Hinnant's days-to-civil conversion.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

// ---------------------------------------------------------------------------
// The uploader thread

enum Upload {
    Sent,
    Retry(Option<u16>),
    Refused(u16),
}

type UploadFn = Box<dyn FnMut(&[u8]) -> Upload + Send>;
type ConfigureFn = Box<dyn FnMut(&crate::settings::ProxyConfig) + Send>;

struct Uploader {
    upload: UploadFn,
    spool: Option<PathBuf>,
    recorder: &'static Mutex<VecDeque<Map<String, Value>>>,
    pending: VecDeque<Map<String, Value>>,
    configure: Option<ConfigureFn>,
    last_dump: HashMap<&'static str, Instant>,
    /// The last `crumb_seq` a dump shipped.
    dumped_through: u64,
    last_attempt: Instant,
    retry_after: Option<Instant>,
    backoff: Duration,
    refused: bool,
    lost: u64,
}

impl Uploader {
    fn new(
        upload: UploadFn,
        spool: Option<PathBuf>,
        recorder: &'static Mutex<VecDeque<Map<String, Value>>>,
    ) -> Self {
        let mut uploader = Self {
            upload,
            spool,
            recorder,
            pending: VecDeque::new(),
            configure: None,
            last_dump: HashMap::new(),
            dumped_through: 0,
            last_attempt: Instant::now(),
            retry_after: None,
            backoff: FLUSH_EVERY,
            refused: false,
            lost: 0,
        };
        if let Some(spool) = uploader.spool.clone() {
            uploader.restore(&spool);
        }
        uploader
    }

    fn run(mut self, rx: Receiver<Message>) {
        loop {
            let wait = FLUSH_EVERY.saturating_sub(self.last_attempt.elapsed());
            match rx.recv_timeout(wait.max(Duration::from_millis(50))) {
                Ok(Message::Record(map)) => self.queue(map),
                Ok(Message::Proxy(proxy)) => {
                    if let Some(configure) = &mut self.configure {
                        configure(&proxy);
                    }
                    self.retry_after = None;
                    self.backoff = FLUSH_EVERY;
                }
                Ok(Message::Dump { anomaly, kind }) => self.dump(&anomaly, kind),
                Ok(Message::Flush(done)) => {
                    self.retry_after = None;
                    self.send_pending();
                    let _ = done.try_send(());
                }
                Ok(Message::Shutdown(done)) => {
                    // Whatever is still in the channel goes out too.
                    while let Ok(message) = rx.try_recv() {
                        if let Message::Record(map) = message {
                            self.queue(map);
                        }
                    }
                    self.retry_after = None;
                    self.send_pending();
                    self.write_spool();
                    let _ = done.try_send(());
                    return;
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    self.send_pending();
                    self.write_spool();
                    return;
                }
            }
            if self.pending.len() >= BATCH_MAX || self.last_attempt.elapsed() >= FLUSH_EVERY {
                self.send_pending();
            }
        }
    }

    fn queue(&mut self, map: Map<String, Value>) {
        if self.refused {
            return;
        }
        if self.pending.len() >= PENDING_CAPACITY {
            self.pending.pop_front();
            self.lost += 1;
        }
        self.pending.push_back(map);
    }

    /// Ships the recorder's crumbs from the last [`DUMP_WINDOW`], each
    /// tagged with the anomaly that asked for them.
    fn dump(&mut self, anomaly: &str, kind: &'static str) {
        if self
            .last_dump
            .get(kind)
            .is_some_and(|last| last.elapsed() < DUMP_COOLDOWN)
        {
            return;
        }
        self.last_dump.insert(kind, Instant::now());
        let horizon = uptime_ms().saturating_sub(DUMP_WINDOW.as_millis() as u64);
        let shipped_through = self.dumped_through;
        // Crumbs an earlier dump shipped are not sent again; the summary
        // says where they are.
        let crumbs: Vec<_> = lock(self.recorder)
            .iter()
            .filter(|map| {
                map.get("uptime_ms")
                    .and_then(Value::as_u64)
                    .is_none_or(|at| at >= horizon)
                    && map
                        .get("crumb_seq")
                        .and_then(Value::as_u64)
                        .is_none_or(|index| index > shipped_through)
            })
            .cloned()
            .collect();
        let count = crumbs.len();
        for mut map in crumbs {
            if let Some(index) = map.get("crumb_seq").and_then(Value::as_u64) {
                self.dumped_through = self.dumped_through.max(index);
            }
            map.insert("dump_of".into(), Value::from(anomaly));
            self.queue(map);
        }
        let mut summary = Map::new();
        summary.insert("_time".into(), Value::from(rfc3339(SystemTime::now())));
        summary.insert("event".into(), Value::from("anomaly.dump"));
        summary.insert("dump_of".into(), Value::from(anomaly));
        summary.insert("crumbs".into(), Value::from(count));
        summary.insert(
            "earlier_through_crumb_seq".into(),
            Value::from(shipped_through),
        );
        self.queue(summary);
    }

    fn send_pending(&mut self) {
        self.last_attempt = Instant::now();
        if self.refused || self.pending.is_empty() {
            return;
        }
        if self.retry_after.is_some_and(|after| Instant::now() < after) {
            return;
        }
        if self.lost > 0 {
            let mut lost = Map::new();
            lost.insert("_time".into(), Value::from(rfc3339(SystemTime::now())));
            lost.insert("event".into(), Value::from("telemetry.lost"));
            lost.insert("count".into(), Value::from(self.lost));
            self.pending.push_back(lost);
            self.lost = 0;
        }
        while !self.pending.is_empty() {
            let take = self.pending.len().min(BATCH_MAX);
            let Some(body) = encode(self.pending.iter().take(take)) else {
                self.pending.drain(..take);
                continue;
            };
            match (self.upload)(&body) {
                Upload::Sent => {
                    self.pending.drain(..take);
                    self.backoff = FLUSH_EVERY;
                    self.retry_after = None;
                }
                Upload::Retry(status) => {
                    // Shipped with the next batch that gets through, so an
                    // outage of the telemetry path shows on the timeline.
                    let mut failure = Map::new();
                    failure.insert("_time".into(), Value::from(rfc3339(SystemTime::now())));
                    failure.insert("event".into(), Value::from("telemetry.upload_failed"));
                    failure.insert("status".into(), Value::from(status));
                    failure.insert("pending".into(), Value::from(self.pending.len()));
                    failure.insert("backoff_ms".into(), Value::from(duration_ms(self.backoff)));
                    self.queue(failure);
                    self.retry_after = Some(Instant::now() + self.backoff);
                    self.backoff = (self.backoff * 2).min(Duration::from_secs(300));
                    return;
                }
                Upload::Refused(status) => {
                    log::warn!(
                        target: "spotifast::telemetry",
                        "Axiom refused telemetry (HTTP {status}); check the token and dataset in {CONFIG_FILE}"
                    );
                    self.refused = true;
                    self.pending.clear();
                    return;
                }
            }
        }
    }

    /// Queues the events a previous run left in `path`, and removes it.
    fn restore(&mut self, path: &Path) {
        let Ok(text) = std::fs::read_to_string(path) else {
            return;
        };
        let _ = std::fs::remove_file(path);
        for line in text.lines().take(PENDING_CAPACITY) {
            if let Ok(Value::Object(mut map)) = serde_json::from_str::<Value>(line) {
                map.insert("spooled".into(), Value::from(true));
                self.pending.push_back(map);
            }
        }
    }

    fn write_spool(&self) {
        let Some(path) = &self.spool else { return };
        if self.pending.is_empty() || self.refused {
            return;
        }
        let temporary = path.with_extension("tmp");
        let written = std::fs::File::create(&temporary).and_then(|mut file| {
            for map in self.pending.iter().rev().take(10_000).rev() {
                serde_json::to_writer(&mut file, map)?;
                file.write_all(b"\n")?;
            }
            file.sync_all()
        });
        if written.is_ok() {
            let _ = crate::util::replace_file(&temporary, path);
        } else {
            let _ = std::fs::remove_file(&temporary);
        }
    }
}

/// Gzipped NDJSON.
fn encode<'a>(maps: impl Iterator<Item = &'a Map<String, Value>>) -> Option<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    for map in maps {
        serde_json::to_writer(&mut encoder, map).ok()?;
        encoder.write_all(b"\n").ok()?;
    }
    encoder.finish().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read as _;
    use std::sync::Arc;

    fn recorder() -> &'static Mutex<VecDeque<Map<String, Value>>> {
        Box::leak(Box::new(Mutex::new(VecDeque::new())))
    }

    #[test]
    fn rfc3339_formats_known_instants() {
        assert_eq!(rfc3339(UNIX_EPOCH), "1970-01-01T00:00:00.000Z");
        let leap = UNIX_EPOCH + Duration::from_millis(951_782_400_123);
        assert_eq!(rfc3339(leap), "2000-02-29T00:00:00.123Z");
        let late = UNIX_EPOCH + Duration::from_secs(1_791_400_000);
        assert_eq!(rfc3339(late), "2026-10-07T19:06:40.000Z");
    }

    #[test]
    fn scrub_removes_links_and_tokens_but_keeps_spotify_ids() {
        let token = "BQD".to_owned() + &"x".repeat(150);
        let text = format!(
            "GET https://audio.example/a?token=1 failed for spotify:track:4uLU6hMCjMI75M1A2tKUQC with {token} and Bearer abc"
        );
        let scrubbed = scrub(&text);
        assert!(!scrubbed.contains("audio.example"));
        assert!(!scrubbed.contains(&token));
        assert!(!scrubbed.contains("Bearer"));
        assert!(scrubbed.contains("spotify:track:4uLU6hMCjMI75M1A2tKUQC"));
    }

    /// The formats librespot logs CDN and storage URLs in.
    #[test]
    fn scrub_removes_librespot_cdn_urls() {
        for line in [
            "Streaming from https://audio4-ak.spotifycdn.com/audio/abcdef?__token__=exp=1~hmac=ff",
            "Fetching https://audio-fa.scdn.co/audio/1a2b3c4d?1234_abcDEF",
            "error from cdn url: Uri { https://audio-ak.spotifycdn.com/x?y }",
            "fetch failed: \"https://cdn.example/x?oh=1&__token__=2\"",
        ] {
            let scrubbed = scrub(line);
            assert!(!scrubbed.contains("spotifycdn"), "{scrubbed}");
            assert!(!scrubbed.contains("scdn"), "{scrubbed}");
            assert!(!scrubbed.contains("token"), "{scrubbed}");
        }
    }

    #[test]
    fn private_terms_are_replaced_by_their_digest() {
        let terms = vec!["Someone's AirPods Pro".to_owned(), "AirPods".to_owned()];
        let text = replace_private_terms("audio output: Someone's AirPods Pro at 48000 Hz", &terms);
        assert!(!text.contains("Someone"));
        assert!(text.contains(&format!("<private:{}>", digest("Someone's AirPods Pro"))));
        assert!(text.contains("48000 Hz"));
    }

    #[test]
    fn scrub_keeps_rust_paths_in_backtraces() {
        let line = "   3: spotifast::backend::playback::handle_player_event_with_a_long_name";
        assert!(scrub_lines(line).contains("spotifast::backend::playback"));
    }

    #[test]
    fn sensitive_targets_and_messages_are_recognised() {
        assert!(sensitive_target("librespot_oauth"));
        assert!(sensitive_target("spotifast::auth"));
        assert!(sensitive_target("librespot_core::login5"));
        assert!(sensitive_target("librespot_core::dealer"));
        assert!(!sensitive_target("librespot_playback::player"));
        assert!(sensitive_message("Authenticated as 'someone' !"));
        assert!(!sensitive_message("Loading <Song> with Spotify URI"));
    }

    #[test]
    fn path_templates_group_requests() {
        assert_eq!(
            path_template("/v1/playlists/37i9dQZF1DXcBWIGoYBM5M/tracks?offset=100&limit=50"),
            "/v1/playlists/{id}/tracks"
        );
        assert_eq!(
            path_template("/v1/users/some.one/playlists"),
            "/v1/users/{user}/playlists"
        );
        assert_eq!(path_template("/v1/me/player/next"), "/v1/me/player/next");
        assert_eq!(
            path_template("/metadata/4/track/0123456789abcdef0123456789abcdef"),
            "/metadata/{n}/track/{id}"
        );
        assert_eq!(
            path_template("/context-resolve/v1/spotify:playlist:abc"),
            "/context-resolve/v1/{id}"
        );
    }

    #[test]
    fn config_parses_and_validates() {
        let config = Config::parse(r#"{"axiom_token":"xaat-1","dataset":"spotifast"}"#).unwrap();
        assert!(config.valid());
        assert_eq!(
            config.ingest_url(),
            "https://us-east-1.aws.edge.axiom.co/v1/ingest/spotifast"
        );
        let legacy = Config {
            endpoint: "https://api.axiom.co/".into(),
            ..config.clone()
        };
        assert_eq!(
            legacy.ingest_url(),
            "https://api.axiom.co/v1/datasets/spotifast/ingest"
        );
        assert!(!Config::with_token(" ".into()).valid());
        let plain_http = Config {
            endpoint: "http://example".into(),
            ..config.clone()
        };
        assert!(!plain_http.valid());
        assert!(!format!("{config:?}").contains("xaat-1"));
    }

    #[test]
    fn events_and_helpers_are_inert_while_telemetry_is_off() {
        // Unit tests never call init, so telemetry is off.
        let event = event("test").field("a", 1);
        assert!(!event.is_live());
        event.emit();
        assert!(!Throttle::new().ready(Duration::ZERO));
        trace_start("test", "id", Duration::from_secs(1));
        assert!(!tracing());
        note_cause("test:cause", "");
        assert!(recent_causes(Duration::from_secs(60)).is_empty());
    }

    #[test]
    fn gauges_report_peaks_once() {
        let peak = Gauge::peak("p");
        assert_eq!(peak.report(), None);
        peak.raise(3);
        peak.raise(7);
        peak.raise(5);
        assert_eq!(peak.report(), Some(7));
        assert_eq!(peak.report(), None);
        let last = Gauge::last("l");
        last.set(4);
        assert_eq!(last.report(), Some(4));
        assert_eq!(last.report(), Some(4));
    }

    #[test]
    fn digests_are_stable_and_opaque() {
        assert_eq!(digest("en0"), digest("en0"));
        assert_ne!(digest("en0"), digest("en1"));
        assert_eq!(digest("Someone's AirPods").len(), 16);
    }

    #[test]
    fn scrub_removes_account_names_and_paths() {
        let line = r#"handling next context Some("spotify:user:thalles:collection") from spotify:user:thalles"#;
        let scrubbed = scrub(line);
        assert!(!scrubbed.contains("thalles"), "{scrubbed}");
        assert!(
            scrubbed.contains("spotify:user:{user}:collection"),
            "{scrubbed}"
        );
        for line in [
            r"could not lock C:\Users\Thalles Passos\AppData\Local\spotifast\cache\x.json",
            "could not store /home/joão/.cache/spotifast/playlists/1234/abc.json: denied",
            "failed: \"/Users/thalles/Library/Caches/me.paolino.spotifast/x\"",
        ] {
            let scrubbed = scrub(line);
            assert!(!scrubbed.contains("Thalles"), "{scrubbed}");
            assert!(!scrubbed.contains("Passos"), "{scrubbed}");
            assert!(!scrubbed.contains("joão"), "{scrubbed}");
            assert!(!scrubbed.contains("thalles"), "{scrubbed}");
        }
        assert!(scrub("GET /v1/me/player/next failed").contains("/v1/me/player/next"));
        assert!(scrub("in spotifast::backend::worker").contains("spotifast::backend::worker"));
    }

    #[test]
    fn the_event_budget_refills() {
        let budget = Budget::new(2.0, 1_000.0);
        assert!(budget.take());
        assert!(budget.take());
        std::thread::sleep(Duration::from_millis(5));
        assert!(budget.take());
        let empty = Budget::new(1.0, 0.0);
        assert!(empty.take());
        assert!(!empty.take());
    }

    fn map(name: &str, uptime: u64) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("event".into(), Value::from(name));
        map.insert("uptime_ms".into(), Value::from(uptime));
        map
    }

    fn crumb_map(name: &str, index: u64) -> Map<String, Value> {
        let mut map = map(name, 0);
        map.insert("crumb_seq".into(), Value::from(index));
        map
    }

    fn decode(body: &[u8]) -> Vec<Value> {
        let mut text = String::new();
        flate2::read::GzDecoder::new(body)
            .read_to_string(&mut text)
            .unwrap();
        text.lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[test]
    fn crumbs_ship_only_in_an_anomaly_dump() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let sink = sent.clone();
        let recorder = recorder();
        let mut uploader = Uploader::new(
            Box::new(move |body: &[u8]| {
                sink.lock().unwrap().extend(decode(body));
                Upload::Sent
            }),
            None,
            recorder,
        );
        recorder.lock().unwrap().push_back(crumb_map("crumb.a", 1));
        uploader.queue(map("shipped", 0));
        uploader.send_pending();
        let names = |sent: &Vec<Value>| {
            sent.iter()
                .map(|value| value["event"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(names(&sent.lock().unwrap()), ["shipped"]);
        uploader.dump("abc", "kind");
        uploader.dump("def", "kind");
        uploader.send_pending();
        assert_eq!(
            names(&sent.lock().unwrap()),
            ["shipped", "crumb.a", "anomaly.dump"]
        );
        assert_eq!(sent.lock().unwrap()[1]["dump_of"], "abc");
        // Another kind's dump ships only what is new since.
        recorder.lock().unwrap().push_back(crumb_map("crumb.b", 2));
        uploader.dump("ghi", "other");
        uploader.send_pending();
        let sent = sent.lock().unwrap();
        assert_eq!(
            names(&sent),
            [
                "shipped",
                "crumb.a",
                "anomaly.dump",
                "crumb.b",
                "anomaly.dump"
            ]
        );
        assert_eq!(sent[4]["earlier_through_crumb_seq"], 1);
    }

    #[test]
    fn failed_uploads_are_kept_and_retried_later() {
        let attempts = Arc::new(AtomicU64::new(0));
        let counter = attempts.clone();
        let mut uploader = Uploader::new(
            Box::new(move |_: &[u8]| {
                counter.fetch_add(1, Ordering::Relaxed);
                Upload::Retry(Some(503))
            }),
            None,
            recorder(),
        );
        uploader.queue(map("a", 0));
        uploader.send_pending();
        uploader.send_pending();
        assert_eq!(attempts.load(Ordering::Relaxed), 1, "backs off");
        assert_eq!(uploader.pending.len(), 2, "the event and the failure");
        assert_eq!(uploader.pending[1]["status"], 503);
    }

    #[test]
    fn a_refused_token_stops_uploads() {
        let mut uploader =
            Uploader::new(Box::new(|_: &[u8]| Upload::Refused(403)), None, recorder());
        uploader.queue(map("a", 0));
        uploader.send_pending();
        uploader.queue(map("b", 0));
        assert!(uploader.refused);
        assert!(uploader.pending.is_empty());
    }

    #[test]
    fn the_spool_survives_a_restart() {
        let directory = std::env::temp_dir().join(format!("spotifast-spool-{}", random_id(6)));
        std::fs::create_dir_all(&directory).unwrap();
        let spool = directory.join(SPOOL_FILE);
        let mut offline = Uploader::new(
            Box::new(|_: &[u8]| Upload::Retry(None)),
            Some(spool.clone()),
            recorder(),
        );
        offline.queue(map("kept", 0));
        offline.send_pending();
        offline.write_spool();
        let restored = Uploader::new(
            Box::new(|_: &[u8]| Upload::Sent),
            Some(spool.clone()),
            recorder(),
        );
        assert_eq!(restored.pending.len(), 2, "the event and the failure");
        assert_eq!(restored.pending[0]["event"], "kept");
        assert_eq!(restored.pending[0]["spooled"], true);
        assert!(!spool.exists());
        std::fs::remove_dir_all(directory).unwrap();
    }

    /// Sends real events to the configured Axiom dataset. Run with
    /// `SPOTIFAST_AXIOM_TOKEN=... cargo test --lib telemetry::tests::live_ingest -- --ignored`.
    #[test]
    #[ignore = "needs an Axiom token and the network"]
    fn live_ingest() {
        let config = Config::load(Path::new("/nonexistent")).expect("SPOTIFAST_AXIOM_TOKEN");
        let state = std::env::temp_dir().join(format!("spotifast-live-{}", random_id(6)));
        std::fs::create_dir_all(&state).unwrap();
        init(Some(config), &crate::settings::ProxyConfig::System, &state);
        assert!(enabled());
        set_context("test_context", "yes");
        crumb("test.crumb").field("n", 1).emit();
        event("test.event")
            .field("n", 2)
            .ms("took_ms", Duration::from_micros(1234))
            .emit();
        let id = intent("next", "test");
        trace_start("next", &id, Duration::from_secs(5));
        trace_mark("next", "step_one");
        note_cause("remote:pause", "test");
        event("test.explained")
            .explained(Duration::from_secs(5))
            .emit();
        trace_end("next", "ok");
        anomaly("test.anomaly")
            .text("note", "dump should carry test.crumb")
            .emit();
        shutdown(Duration::from_secs(20));
        assert!(!state.join(SPOOL_FILE).exists(), "everything was sent");
        std::fs::remove_dir_all(state).unwrap();
    }
}
