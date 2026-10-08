//! Local Spotify Connect playback through librespot.
//!
//! The engine owns one librespot session, player, mixer, and Spirc (the
//! Connect state machine). Player events are folded into a [`LocalState`]
//! snapshot that is pushed to the interface whenever something changed;
//! commands from the interface go straight to Spirc, which keeps Spotify's
//! cluster state in sync so phones and other clients see what this device
//! is doing.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use librespot_connect::{
    ConnectConfig, LoadContextOptions, LoadRequest, LoadRequestOptions, Options, PlayingTrack,
    Spirc,
};
use librespot_core::{
    SpotifyUri,
    authentication::Credentials,
    cache::Cache,
    config::{DeviceType, SessionConfig},
    error::ErrorKind,
    session::Session,
    spotify_id::SpotifyId,
};
use librespot_metadata::{
    Album as MetadataAlbum, Metadata,
    album::AlbumType,
    audio::{AudioItem, UniqueFields},
};
use librespot_playback::{
    audio_backend::{self, Sink},
    config::{AudioFormat, Bitrate, NormalisationType, PlayerConfig, VolumeCtrl},
    mixer::{self, Mixer, MixerConfig, NoOpVolume, VolumeGetter},
    player::{Player, PlayerEvent, SinkEventCallback, SinkStatus},
};
use sha1::{Digest, Sha1};

use crate::api::models::ArtistRef;
use crate::sink::{AudioControl, ErrorHook, RodioSink};
use crate::telemetry;
use crate::vis::{AudioTap, Tapped};

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub device_name: String,
    pub bitrate_kbps: u16,
    pub normalisation: bool,
    pub autoplay: bool,
    pub gapless: bool,
    pub backend: Option<String>,
    pub audio_device: Option<String>,
    pub initial_volume: u16,
    pub volume_dir: PathBuf,
    pub audio_cache_dir: Option<PathBuf>,
    pub audio_cache_limit: Option<u64>,
    /// Output buffer length in milliseconds.
    pub buffer_ms: u32,
    pub tap: Arc<AudioTap>,
    /// The equalizer's settings, shared with the window that sets them.
    pub eq: crate::eq::SharedEq,
    /// Proxy used by the Web API client. Librespot only uses the HTTP form.
    pub proxy: crate::settings::ProxyConfig,
}

impl EngineConfig {
    /// A stable Connect device id derived from the name, so Spotify keeps
    /// recognising this computer across restarts.
    pub fn device_id(&self) -> String {
        hex(&Sha1::digest(self.device_name.as_bytes()))
    }

    pub fn open_cache(&self) -> Result<Cache> {
        Cache::new(
            None,
            Some(self.volume_dir.as_path()),
            self.audio_cache_dir.as_deref(),
            self.audio_cache_limit,
        )
        .map(Cache::with_memory_credentials)
        .context("unable to open the playback cache")
    }

    fn bitrate(&self) -> Bitrate {
        match self.bitrate_kbps {
            96 => Bitrate::Bitrate96,
            160 => Bitrate::Bitrate160,
            _ => Bitrate::Bitrate320,
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Playback {
    #[default]
    Stopped,
    Loading,
    Playing,
    Paused,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RepeatMode {
    #[default]
    Off,
    Context,
    Track,
}

impl RepeatMode {
    pub fn next(self) -> Self {
        match self {
            Self::Off => Self::Context,
            Self::Context => Self::Track,
            Self::Track => Self::Off,
        }
    }

    pub fn api_name(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Context => "context",
            Self::Track => "track",
        }
    }

    pub fn from_api(name: &str) -> Self {
        match name {
            "context" => Self::Context,
            "track" => Self::Track,
            _ => Self::Off,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LocalTrack {
    pub uri: String,
    pub title: String,
    pub artists: Vec<ArtistRef>,
    pub album: String,
    pub art_url: Option<String>,
    pub art_small_url: Option<String>,
    pub duration_ms: u32,
    pub is_episode: bool,
}

impl LocalTrack {
    pub fn artist_names(&self) -> String {
        crate::api::models::join_names(self.artists.iter().map(|artist| artist.name.as_str()))
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LocalState {
    pub playback: Playback,
    pub track: Option<LocalTrack>,
    pub position_ms: u32,
    /// When `position_ms` was observed; `None` while not advancing.
    pub position_at: Option<Instant>,
    pub volume: u16,
    pub shuffle: bool,
    pub repeat: RepeatMode,
    /// The librespot engine's Spotify session is alive. Connect device
    /// activity is separate: Spotify may make this device inactive while the
    /// session remains ready to be activated by the next load.
    pub connected: bool,
    pub username: String,
    pub active_client: String,
    pub error: Option<String>,
    pub seek_sequence: u64,
    /// The engine is fetching a track right now, even when the previous
    /// track's `playback` still reads `Playing` so its controls stay visible
    /// through the swap.
    pub loading: bool,
    /// A newly loaded track, including another play of the same URI.
    pub track_sequence: u64,
    /// Another play of the same track started, and its start position has
    /// not arrived yet. The track looks unchanged, so the `Playing` or
    /// `Paused` that brings the position counts as a seek for media
    /// controls, which would otherwise count on past the end (#587).
    pub replay_pending: bool,
}

/// What local playback was doing when its session ended, so the engine
/// can pick it up again after reconnecting.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Interrupted {
    pub uri: String,
    pub position_ms: u32,
    /// Playing or loading, as opposed to paused.
    pub playing: bool,
}

impl LocalState {
    /// The track and position to come back to, if something was on.
    pub fn interrupted(&self) -> Option<Interrupted> {
        let track = self.track.as_ref()?;
        if self.playback == Playback::Stopped {
            return None;
        }
        Some(Interrupted {
            uri: track.uri.clone(),
            position_ms: self.position_now(),
            playing: matches!(self.playback, Playback::Playing | Playback::Loading),
        })
    }

    /// The position now, interpolated from the last report while playing.
    pub fn position_now(&self) -> u32 {
        match (self.playback, self.position_at) {
            (Playback::Playing, Some(at)) => {
                let elapsed = at.elapsed().as_millis() as u32;
                let limit = self
                    .track
                    .as_ref()
                    .map_or(u32::MAX, |track| track.duration_ms.max(self.position_ms));
                self.position_ms.saturating_add(elapsed).min(limit)
            }
            _ => self.position_ms,
        }
    }

    pub fn is_active(&self) -> bool {
        self.track.is_some() && self.playback != Playback::Stopped
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LoadSpec {
    pub context_uri: Option<String>,
    pub uris: Vec<String>,
    pub offset_uri: Option<String>,
    pub offset_index: Option<u32>,
    pub position_ms: u32,
    pub play: bool,
    pub shuffle: Option<bool>,
    /// Explicit repeat preference for a new load. Otherwise retain the
    /// engine's current preference instead of librespot's default (off).
    pub repeat: Option<RepeatMode>,
    /// Play what Spotify would follow `context_uri` with, its autoplay
    /// station, rather than the context itself.
    pub autoplay: bool,
}

/// A dropped Connect session retains its complete playback state. Live
/// replacement for an audio-settings change still uses a track pickup.
#[derive(Clone, Debug)]
pub enum PlaybackResume {
    Session(Arc<librespot_connect::PlaybackSnapshot>),
    Track(LoadSpec),
}

impl LoadSpec {
    fn context_options(&self, current_repeat: RepeatMode) -> LoadContextOptions {
        if self.autoplay {
            LoadContextOptions::Autoplay
        } else {
            let repeat = self.repeat.unwrap_or(current_repeat);
            LoadContextOptions::Options(Options {
                shuffle: self.shuffle.unwrap_or(false),
                repeat: repeat == RepeatMode::Context,
                repeat_track: repeat == RepeatMode::Track,
            })
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum PlayerCommand {
    Toggle,
    Next,
    Previous,
    /// Remove manually queued tracks and keep context tracks.
    ClearQueue,
    /// Queue a track or episode after the ones already queued.
    AddToQueue(String),
    Seek(u32),
    /// The volume to keep: applied at once and told to Spotify Connect.
    Volume(u16),
    /// The slider mid-drag: applied at once, nothing sent. Every Connect
    /// update costs a round trip to Spotify, and librespot makes them one
    /// after another, so dragging through fifty values lagged by seconds.
    VolumePreview(u16),
    Shuffle(bool),
    Repeat(RepeatMode),
    Load(LoadSpec),
    /// Take over the active Connect session, including its queue and position.
    Transfer,
}

#[allow(clippy::large_enum_variant)]
pub enum EngineEvent {
    State(LocalState),
    SessionEnded,
}

pub type Notify = Arc<dyn Fn(EngineEvent) + Send + Sync>;

pub struct Engine {
    player: Arc<Player>,
    spirc: Arc<Spirc>,
    session: Session,
    mixer: Arc<dyn Mixer>,
    device_id: String,
    state: Arc<Mutex<LocalState>>,
    /// What was playing when the session ended on its own.
    interrupted: Arc<Mutex<Option<Interrupted>>>,
    shutting_down: Arc<std::sync::atomic::AtomicBool>,
    audio: Arc<AudioControl>,
    /// Which engine of this run this is, for telemetry.
    generation: u64,
    started: Instant,
}

impl Engine {
    pub(crate) fn credentials(&self) -> Option<Credentials> {
        self.session.cache().and_then(|cache| cache.credentials())
    }
    /// Connects to Spotify and announces this device on Spotify Connect.
    pub async fn connect(
        config: &EngineConfig,
        proxy: Option<reqwest::Url>,
        credentials: Credentials,
        cache: Cache,
        notify: Notify,
    ) -> Result<Self> {
        let generation = ENGINE_GENERATION.fetch_add(1, Ordering::Relaxed) + 1;
        let started = Instant::now();
        let device_id = config.device_id();
        let mut start_span = telemetry::span("player.engine_start");
        if telemetry::enabled() {
            register_metrics();
            telemetry::set_context("engine_gen", generation);
            telemetry::set_context("connect_active", false);
            sent_commands().pending.clear();
            // A new engine's output starts ungated.
            *GATE.lock().unwrap_or_else(|p| p.into_inner()) = None;
            start_span =
                describe_engine(start_span, config, generation, &device_id, proxy.is_some());
        }
        let session_config = SessionConfig {
            device_id: device_id.clone(),
            autoplay: Some(config.autoplay),
            proxy,
            ..SessionConfig::default()
        };
        let normalisation_factor = Arc::new(std::sync::atomic::AtomicU64::new(1.0f64.to_bits()));
        let player_config = PlayerConfig {
            bitrate: config.bitrate(),
            gapless: config.gapless,
            normalisation: config.normalisation,
            normalisation_type: NormalisationType::Auto,
            position_update_interval: Some(Duration::from_secs(1)),
            // The fork reports each track's normalisation factor here, so
            // the tap can undo it for the visualisers: they show the music,
            // not the loudness housekeeping.
            normalisation_report: Some(Arc::clone(&normalisation_factor)),
            ..PlayerConfig::default()
        };

        let mixer_builder =
            mixer::find(Some("softvol")).ok_or_else(|| anyhow!("soft volume mixer missing"))?;
        // librespot's default curve spans 60 dB logarithmically, which puts
        // half the slider below -30 dB and every level anyone wants in its
        // top quarter. The cubic curve reaches -16 dB at the middle and -7 dB
        // at three quarters, spreading the useful range across the slider.
        let mixer = mixer_builder(MixerConfig {
            volume_ctrl: VolumeCtrl::Cubic(VolumeCtrl::DEFAULT_DB_RANGE),
            ..MixerConfig::default()
        })
        .context("unable to create the mixer")?;

        let state = Arc::new(Mutex::new(LocalState {
            volume: config.initial_volume,
            ..LocalState::default()
        }));
        let session = Session::new(session_config, Some(cache));
        let audio = AudioControl::new(config.buffer_ms);
        let (sink_builder, volume) = sink_builder(
            config,
            Arc::clone(&state),
            Arc::clone(&notify),
            &mixer,
            Arc::clone(&normalisation_factor),
            Arc::clone(&audio),
        );
        let player = Player::new(player_config, session.clone(), volume, sink_builder);
        if telemetry::enabled() {
            let callback: SinkEventCallback =
                Box::new(move |status: SinkStatus| report_sink_status(status, generation));
            player.set_sink_event_callback(Some(callback));
        }
        let events = player.get_player_event_channel();
        let shutting_down = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let watch = EventWatch::new(
            generation,
            Arc::clone(&shutting_down),
            Arc::clone(&normalisation_factor),
        );
        tokio::spawn(run_events(
            events,
            Arc::clone(&state),
            Arc::clone(&notify),
            Arc::clone(&audio),
            watch,
        ));

        let connect_config = ConnectConfig {
            name: config.device_name.clone(),
            device_type: DeviceType::Computer,
            initial_volume: config.initial_volume,
            disable_volume: false,
            volume_steps: 64,
            ..ConnectConfig::default()
        };
        start_span.set("setup_ms", telemetry::duration_ms(start_span.elapsed()));
        let spirc_started = Instant::now();
        let connected = Spirc::new(
            connect_config,
            session.clone(),
            credentials,
            Arc::clone(&player),
            Arc::clone(&mixer),
        )
        .await;
        start_span.set(
            "spirc_new_ms",
            telemetry::duration_ms(spirc_started.elapsed()),
        );
        if let Err(error) = &connected {
            start_span.set("outcome", "error");
            if telemetry::enabled() {
                start_span.set("error_kind", format!("{:?}", error.kind));
                start_span.set("error", telemetry::scrub(&error.to_string()));
            }
        }
        let (spirc, spirc_task) = connected.context("unable to connect to Spotify")?;

        {
            let mut current = state.lock().unwrap_or_else(|p| p.into_inner());
            current.connected = true;
            current.username = session.username();
            notify(EngineEvent::State(current.clone()));
        }

        let interrupted: Arc<Mutex<Option<Interrupted>>> = Arc::default();
        let ended_flag = Arc::clone(&shutting_down);
        let ended_notify = Arc::clone(&notify);
        let ended_state = Arc::clone(&state);
        let ended_interrupted = Arc::clone(&interrupted);
        // For telemetry only: whether the session had gone bad by the end.
        let ended_session = telemetry::enabled().then(|| session.clone());
        tokio::spawn(async move {
            spirc_task.await;
            let mut ended = None;
            {
                let mut current = ended_state.lock().unwrap_or_else(|p| p.into_inner());
                // Kept before the state is marked stopped, so a reconnect
                // knows what to pick up.
                *ended_interrupted.lock().unwrap_or_else(|p| p.into_inner()) =
                    current.interrupted();
                if telemetry::enabled() {
                    ended = Some((
                        current.playback,
                        current.position_now(),
                        current.track.as_ref().map(|track| track.uri.clone()),
                    ));
                }
                current.connected = false;
                current.playback = Playback::Stopped;
                current.position_at = None;
                ended_notify(EngineEvent::State(current.clone()));
            }
            if let Some(ended) = ended {
                report_spirc_end(
                    generation,
                    started,
                    ended_flag.load(std::sync::atomic::Ordering::SeqCst),
                    ended_session.as_ref().map(Session::is_invalid),
                    ended,
                );
            }
            if !ended_flag.load(std::sync::atomic::Ordering::SeqCst) {
                ended_notify(EngineEvent::SessionEnded);
            }
        });

        start_span.set("outcome", "ok");
        start_span.end();
        Ok(Self {
            player,
            spirc: Arc::new(spirc),
            session,
            mixer,
            device_id,
            state,
            interrupted,
            shutting_down,
            audio,
            generation,
            started,
        })
    }

    /// Playback state to resume after replacing this engine.
    pub fn interrupted(&self) -> Option<Interrupted> {
        let ended = self
            .interrupted
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        ended.or_else(|| {
            self.state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .interrupted()
        })
    }

    pub fn device_id(&self) -> &str {
        &self.device_id
    }

    /// What this engine is heard at, kept past the engine itself: the next
    /// engine starts there.
    pub(crate) fn heard(&self) -> Heard {
        Heard {
            state: Arc::clone(&self.state),
        }
    }

    /// Applies a level set here to the mixer and the state at once, so the
    /// engine is heard at it from now on. Connect is told on release, not
    /// while the slider is still moving.
    fn note_volume(&self, volume: u16, preview: bool) -> Result<()> {
        self.mixer.set_volume(volume);
        self.state.lock().unwrap_or_else(|p| p.into_inner()).volume = volume;
        if !preview {
            self.spirc.set_volume(volume)?;
        }
        Ok(())
    }

    /// Whether Spotify classifies this album as an EP in its internal metadata.
    pub(crate) async fn album_is_ep(&self, album_uri: &str) -> Result<bool> {
        let uri = SpotifyUri::from_uri(album_uri).context("invalid album URI")?;
        let album = MetadataAlbum::get(&self.session, &uri)
            .await
            .context("album metadata")?;
        Ok(album.album_type == AlbumType::EP)
    }

    /// Spotify's own transcription of a track, as the raw JSON its clients
    /// read; `Ok(None)` when Spotify has none, an error when asking failed.
    pub async fn lyrics_json(&self, track_uri: &str) -> Result<Option<serde_json::Value>> {
        let Some(id) = track_uri
            .rsplit(':')
            .next()
            .and_then(|id| SpotifyId::from_base62(id).ok())
        else {
            return Ok(None);
        };
        match self.session.spclient().get_lyrics(&id).await {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
            Err(error) if error.kind == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(anyhow!("spotify lyrics: {error}")),
        }
    }

    /// Account playlist tree in Spotify order, including folder markers,
    /// and which of its playlists the account may add songs to.
    pub async fn rootlist(&self) -> Result<Rootlist> {
        use protobuf::Message as _;
        let mut uris = Vec::new();
        let mut editable = std::collections::BTreeSet::new();
        let mut from = 0usize;
        loop {
            let bytes = self
                .session
                .spclient()
                .get_rootlist(from, Some(500))
                .await
                .map_err(|error| anyhow!("rootlist: {error}"))?;
            let content =
                librespot_protocol::playlist4_external::SelectedListContent::parse_from_bytes(
                    &bytes,
                )?;
            let Some(contents) = content.contents.into_option() else {
                break;
            };
            let count = contents.items.len();
            let truncated = contents.truncated();
            editable.extend(editable_uris(&contents));
            uris.extend(contents.items.into_iter().filter_map(|item| item.uri));
            if !truncated || count == 0 {
                break;
            }
            from += count;
        }
        Ok(Rootlist {
            entries: parse_rootlist(&uris),
            editable,
        })
    }

    /// The streaming session, for reads that need no Web API quota.
    pub fn session(&self) -> &Session {
        &self.session
    }

    pub fn shutdown(&self) {
        if telemetry::enabled() {
            let (playback, position_ms) = {
                let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                (state.playback, state.position_now())
            };
            telemetry::event("player.engine_shutdown")
                .field("engine_gen", self.generation)
                .ms("engine_uptime_ms", self.started.elapsed())
                .field("playback", playback_name(playback))
                .field("position_ms", position_ms)
                .emit();
        }
        self.shutting_down
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = self.spirc.shutdown();
        self.player.stop();
    }

    pub fn resume_point(&self) -> Option<PlaybackResume> {
        if let Some(snapshot) = self.spirc.disconnected_playback() {
            self.report_resume_point("session_snapshot", None);
            return Some(PlaybackResume::Session(snapshot));
        }
        let point = self.interrupted().map(|interrupted| {
            PlaybackResume::Track(LoadSpec {
                uris: vec![interrupted.uri],
                position_ms: interrupted.position_ms,
                play: interrupted.playing,
                ..LoadSpec::default()
            })
        });
        let track = match &point {
            Some(PlaybackResume::Track(spec)) => Some((spec.position_ms, spec.play)),
            _ => None,
        };
        self.report_resume_point(if point.is_some() { "track" } else { "none" }, track);
        point
    }

    pub fn resume(&self, resume: PlaybackResume) -> Result<()> {
        let (kind, track) = match &resume {
            PlaybackResume::Session(_) => ("session_snapshot", None),
            PlaybackResume::Track(spec) => ("track", Some((spec.position_ms, spec.play))),
        };
        if telemetry::enabled() {
            sent_commands().restored = Some(Instant::now());
        }
        let result: Result<()> = match resume {
            PlaybackResume::Session(snapshot) => {
                self.spirc.restore_playback(snapshot).map_err(Into::into)
            }
            PlaybackResume::Track(spec) => self.command(PlayerCommand::Load(spec)),
        };
        if telemetry::enabled() {
            let record = telemetry::event("player.engine_resume")
                .field("kind", kind)
                .field("engine_gen", self.generation)
                .field("position_ms", track.map(|(position, _)| position))
                .field("playing", track.map(|(_, playing)| playing))
                .field("ok", result.is_ok());
            match &result {
                Ok(()) => record.emit(),
                Err(error) => record.text("error", &format!("{error:#}")).emit(),
            }
        }
        result
    }

    pub fn command(&self, command: PlayerCommand) -> Result<()> {
        // A drag previews the volume every frame; its release reports the level.
        let report = (telemetry::enabled() && !matches!(command, PlayerCommand::VolumePreview(_)))
            .then(|| self.report_command(&command));
        let interrupts_audio = command_interrupts_audio(
            &self.state.lock().unwrap_or_else(|p| p.into_inner()),
            &command,
        );
        let interrupting = Instant::now();
        if interrupts_audio {
            if report.is_some() {
                gate_closed(command_kind(&command), self.generation);
            }
            self.audio.interrupt();
        }
        let interrupt = interrupts_audio.then(|| interrupting.elapsed());
        let trace = command_trace(&command);
        if let Some(trace) = trace {
            telemetry::trace_mark(trace, "spirc_sent");
        }
        let result = self.send_command(command);
        if interrupts_audio && result.is_err() {
            self.audio.stopped();
            gate_opened("send_error", self.generation);
        }
        if let Some(report) = report {
            report.finish(self.generation, interrupt, &result);
        }
        // A command Spirc never took up is not going to produce anything.
        if result.is_err()
            && let Some(trace) = trace
        {
            telemetry::trace_end(trace, "failed");
        }
        result
    }

    fn send_command(&self, command: PlayerCommand) -> Result<()> {
        let spirc = &self.spirc;
        match command {
            PlayerCommand::Toggle => spirc.play_pause()?,
            PlayerCommand::Next => spirc.next()?,
            PlayerCommand::Previous => spirc.prev()?,
            PlayerCommand::ClearQueue => spirc.clear_queue()?,
            PlayerCommand::AddToQueue(uri) => spirc.add_to_queue(uri)?,
            PlayerCommand::Seek(position_ms) => spirc.set_position_ms(position_ms)?,
            PlayerCommand::Volume(volume) => self.note_volume(volume, false)?,
            PlayerCommand::VolumePreview(volume) => self.note_volume(volume, true)?,
            PlayerCommand::Shuffle(enabled) => spirc.shuffle(enabled)?,
            PlayerCommand::Repeat(mode) => match mode {
                RepeatMode::Off => {
                    spirc.repeat_track(false)?;
                    spirc.repeat(false)?;
                }
                RepeatMode::Context => {
                    spirc.repeat_track(false)?;
                    spirc.repeat(true)?;
                }
                RepeatMode::Track => {
                    spirc.repeat(false)?;
                    spirc.repeat_track(true)?;
                }
            },
            PlayerCommand::Transfer => spirc.transfer(None)?,
            PlayerCommand::Load(spec) => {
                let playing_track = spec
                    .offset_uri
                    .clone()
                    .map(PlayingTrack::Uri)
                    .or_else(|| spec.offset_index.map(PlayingTrack::Index));
                // A load resets librespot's options, including repeat. Pass
                // the user's preference even when shuffle is off.
                let repeat = self.state.lock().unwrap_or_else(|p| p.into_inner()).repeat;
                let context_options = Some(spec.context_options(repeat));
                let options = LoadRequestOptions {
                    start_playing: spec.play,
                    seek_to: spec.position_ms,
                    playing_track,
                    context_options,
                };
                let request = if let Some(context) = spec.context_uri {
                    LoadRequest::from_context_uri(context, options)
                } else if !spec.uris.is_empty() {
                    LoadRequest::from_tracks(spec.uris, options)
                } else {
                    anyhow::bail!("nothing to play");
                };
                spirc.activate()?;
                spirc.load(request)?;
            }
        }
        Ok(())
    }
}

fn command_interrupts_audio(state: &LocalState, command: &PlayerCommand) -> bool {
    state.playback == Playback::Playing
        && matches!(
            command,
            PlayerCommand::Next | PlayerCommand::Previous | PlayerCommand::Load(_)
        )
}

/// The librespot backend a saved setting names, when this build has it.
///
/// Spotifast's own output has always been saved as "rodio". librespot's
/// rodio backend is no longer built in, so that name, an empty setting and
/// any backend this build lacks all play through Spotifast's own output.
fn librespot_backend(name: Option<&str>) -> Option<audio_backend::SinkBuilder> {
    let name = name.filter(|name| *name != crate::sink::NAME)?;
    let builder = audio_backend::find(Some(name.to_string()));
    if builder.is_none() {
        log::warn!("audio backend {name:?} is unavailable; using the default");
    }
    builder
}

/// Builds the audio sink and chooses where volume is applied.
///
/// The default sink opens the device on playback and reports errors instead
/// of panicking. It applies volume at output so changes affect queued audio.
/// Other librespot backends remain available through Settings.
type SinkAndVolume = (
    Box<dyn FnOnce() -> Box<dyn Sink> + Send>,
    Box<dyn VolumeGetter + Send>,
);

fn sink_builder(
    config: &EngineConfig,
    state: Arc<Mutex<LocalState>>,
    notify: Notify,
    mixer: &Arc<dyn Mixer>,
    normalisation: Arc<std::sync::atomic::AtomicU64>,
    audio: Arc<AudioControl>,
) -> SinkAndVolume {
    let device = config.audio_device.clone();
    let buffer_ms = config.buffer_ms;
    let tap = Arc::clone(&config.tap);
    let eq = Arc::clone(&config.eq);
    let report: ErrorHook = Arc::new(move |message: String| {
        // The message the interface shows. It can name the output device, so
        // only a digest is kept; the output reports its own detail.
        telemetry::event("player.output_error")
            .field_with("message_digest", || telemetry::digest(&message))
            .field(
                "stopped_working",
                message == "The audio output stopped working",
            )
            .emit();
        let snapshot = {
            let mut current = state.lock().unwrap_or_else(|p| p.into_inner());
            current.error = Some(message);
            current.clone()
        };
        notify(EngineEvent::State(snapshot));
    });
    if let Some(builder) = librespot_backend(config.backend.as_deref()) {
        // Apply volume after the tap so visualizers are independent of
        // volume, including at zero.
        let applied = mixer.get_soft_volume();
        let normalisation = Arc::clone(&normalisation);
        return (
            Box::new(move || {
                let sink = builder(device, AudioFormat::S16);
                Box::new(Tapped::new(
                    sink,
                    audio,
                    tap,
                    applied,
                    true,
                    eq,
                    normalisation,
                )) as Box<dyn Sink>
            }),
            Box::new(NoOpVolume),
        );
    }
    let volume = mixer.get_soft_volume();
    // The output applies volume to queued audio. The wrapper reads the same
    // value to calculate the pre-volume limiter ceiling.
    let ceiling = mixer.get_soft_volume();
    (
        Box::new(move || {
            let sink = Box::new(RodioSink::new(
                device,
                report,
                volume,
                buffer_ms,
                Arc::clone(&audio),
            ));
            Box::new(Tapped::new(
                sink,
                audio,
                tap,
                ceiling,
                false,
                eq,
                normalisation,
            )) as Box<dyn Sink>
        }),
        Box::new(NoOpVolume),
    )
}

/// What an engine is heard at, for the engine that replaces it.
#[derive(Clone)]
pub(crate) struct Heard {
    state: Arc<Mutex<LocalState>>,
}

impl Heard {
    /// The level set last, which the state holds exactly: a level set here
    /// goes into the state with the mixer, and Connect reports every other
    /// change of the level.
    pub(crate) fn level(&self) -> u16 {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).volume
    }

    /// An engine heard at `volume`, for tests that have no engine.
    #[cfg(test)]
    pub(crate) fn at(volume: u16) -> Self {
        Self {
            state: Arc::new(Mutex::new(LocalState {
                volume,
                ..LocalState::default()
            })),
        }
    }
}

async fn run_events(
    mut events: tokio::sync::mpsc::UnboundedReceiver<PlayerEvent>,
    state: Arc<Mutex<LocalState>>,
    notify: Notify,
    audio: Arc<AudioControl>,
    mut watch: EventWatch,
) {
    let mut play_request_id = None;
    while let Some(event) = events.recv().await {
        // Before the audio control sees it, so a trace has its step marked
        // by the time the gate it opens lets audio through.
        watch.observe(&event);
        if let PlayerEvent::PlayRequestIdChanged {
            play_request_id: next,
        } = &event
        {
            play_request_id = Some(*next);
            continue;
        }
        if let (Some(current), Some(incoming)) = (play_request_id, event.get_play_request_id())
            && current != incoming
        {
            continue;
        }
        audio.handle_player_event(&event);
        let snapshot = {
            let mut current = state.lock().unwrap_or_else(|p| p.into_inner());
            if apply_event(&mut current, event) {
                Some(current.clone())
            } else {
                None
            }
        };
        if let Some(snapshot) = snapshot {
            notify(EngineEvent::State(snapshot));
        }
    }
}

fn set<T: PartialEq>(target: &mut T, value: T) -> bool {
    if *target == value {
        false
    } else {
        *target = value;
        true
    }
}

/// Reports a replay's start position as a seek, once.
fn start_replay(state: &mut LocalState) -> bool {
    if std::mem::take(&mut state.replay_pending) {
        state.seek_sequence = state.seek_sequence.wrapping_add(1);
        true
    } else {
        false
    }
}

fn apply_event(state: &mut LocalState, event: PlayerEvent) -> bool {
    match event {
        PlayerEvent::Stopped { .. } => {
            let mut changed = set(&mut state.playback, Playback::Stopped);
            changed |= set(&mut state.loading, false);
            changed |= set(&mut state.position_ms, 0);
            changed |= set(&mut state.position_at, None);
            changed
        }
        PlayerEvent::Loading { position_ms, .. } => {
            let mut changed = if state.playback == Playback::Stopped {
                set(&mut state.playback, Playback::Loading)
            } else {
                false
            };
            changed |= set(&mut state.loading, true);
            changed |= set(&mut state.position_ms, position_ms);
            changed |= set(&mut state.position_at, None);
            changed |= set(&mut state.error, None);
            changed
        }
        PlayerEvent::Playing { position_ms, .. } => {
            set(&mut state.playback, Playback::Playing);
            set(&mut state.position_ms, position_ms);
            state.position_at = Some(Instant::now());
            state.loading = false;
            start_replay(state);
            true
        }
        PlayerEvent::Paused { position_ms, .. } => {
            let mut changed = set(&mut state.playback, Playback::Paused);
            changed |= set(&mut state.loading, false);
            changed |= set(&mut state.position_ms, position_ms);
            changed |= set(&mut state.position_at, None);
            changed | start_replay(state)
        }
        PlayerEvent::PositionCorrection { position_ms, .. }
        | PlayerEvent::PositionChanged { position_ms, .. } => {
            state.position_ms = position_ms;
            if state.playback == Playback::Playing {
                state.position_at = Some(Instant::now());
            }
            true
        }
        PlayerEvent::Seeked { position_ms, .. } => {
            state.position_ms = position_ms;
            if state.playback == Playback::Playing {
                state.position_at = Some(Instant::now());
            }
            state.seek_sequence = state.seek_sequence.wrapping_add(1);
            true
        }
        PlayerEvent::TrackChanged { audio_item } => {
            let track = local_track(&audio_item);
            state.replay_pending = state
                .track
                .as_ref()
                .is_some_and(|previous| previous.uri == track.uri);
            state.track = Some(track);
            state.error = None;
            // librespot emits this when a loaded track starts, including a
            // repeat whose URI and metadata are identical to the previous play.
            state.track_sequence = state.track_sequence.wrapping_add(1);
            true
        }
        PlayerEvent::Unavailable { track_id, .. } => {
            // A failed load never reaches Playing, so nothing else would
            // turn the spinner off.
            let mut changed = set(
                &mut state.error,
                Some(format!(
                    "This item isn't available: {}",
                    track_id.to_uri().unwrap_or_default()
                )),
            );
            changed |= set(&mut state.loading, false);
            changed
        }
        PlayerEvent::AudioKeyUnavailable { .. } => {
            let mut changed = set(
                &mut state.error,
                Some("Spotify refused the audio key. Try again later".into()),
            );
            changed |= set(&mut state.loading, false);
            changed
        }
        PlayerEvent::VolumeChanged { volume } => set(&mut state.volume, volume),
        PlayerEvent::SessionConnected { user_name, .. } => {
            let mut changed = set(&mut state.connected, true);
            changed |= set(&mut state.username, user_name);
            changed
        }
        // In librespot this event means the Connect device became inactive,
        // usually because another device took over. The engine session is
        // still alive, and `Load` activates it again before starting a track.
        PlayerEvent::SessionDisconnected { .. } => set(&mut state.active_client, String::new()),
        PlayerEvent::SessionClientChanged { client_name, .. } => {
            set(&mut state.active_client, client_name)
        }
        PlayerEvent::ShuffleChanged { shuffle } => set(&mut state.shuffle, shuffle),
        PlayerEvent::RepeatChanged { context, track } => {
            let mode = if track {
                RepeatMode::Track
            } else if context {
                RepeatMode::Context
            } else {
                RepeatMode::Off
            };
            set(&mut state.repeat, mode)
        }
        PlayerEvent::Preloading { .. }
        | PlayerEvent::TimeToPreloadNextTrack { .. }
        | PlayerEvent::EndOfTrack { .. }
        | PlayerEvent::PlayRequestIdChanged { .. }
        | PlayerEvent::AutoPlayChanged { .. }
        | PlayerEvent::FilterExplicitContentChanged { .. } => false,
    }
}

fn local_track(item: &AudioItem) -> LocalTrack {
    let (artists, album, is_episode) = match &item.unique_fields {
        UniqueFields::Track { artists, album, .. } => (
            artists
                .iter()
                .map(|artist| {
                    let uri = artist.id.to_uri().ok();
                    ArtistRef {
                        id: uri
                            .as_deref()
                            .and_then(crate::util::uri_id)
                            .map(str::to_string),
                        name: artist.name.clone(),
                        uri,
                    }
                })
                .collect(),
            album.clone(),
            false,
        ),
        UniqueFields::Episode { show_name, .. } => (
            vec![ArtistRef {
                name: show_name.clone(),
                ..ArtistRef::default()
            }],
            show_name.clone(),
            true,
        ),
        UniqueFields::Local { artists, album, .. } => (
            artists
                .iter()
                .map(|name| ArtistRef {
                    name: name.clone(),
                    ..ArtistRef::default()
                })
                .collect(),
            album.clone().unwrap_or_default(),
            false,
        ),
    };
    let mut covers: Vec<_> = item.covers.iter().collect();
    covers.sort_by_key(|cover| std::cmp::Reverse(cover.width));
    let art_url = covers.first().map(|cover| cover.url.clone());
    let art_small_url = covers
        .iter()
        .rev()
        .find(|cover| cover.width >= 64)
        .or(covers.last())
        .map(|cover| cover.url.clone());
    LocalTrack {
        uri: item.uri.clone(),
        title: item.name.clone(),
        artists,
        album,
        art_url,
        art_small_url,
        duration_ms: item.duration_ms,
        is_episode,
    }
}

/// The account's playlist tree, and what Spotify lets the account do to
/// the playlists in it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Rootlist {
    /// The rows in Spotify's order, folder markers included.
    pub entries: Vec<RootlistEntry>,
    /// Playlists the account may add songs to, by URI, as Spotify's own
    /// permission service decorates the rootlist. The Web API's
    /// `collaborative` flag stays false for a playlist shared by
    /// invitation, so this is the only word on those.
    pub editable: std::collections::BTreeSet<String>,
}

/// The playlists in one rootlist page the account may add songs to, read
/// from the `capabilities` Spotify puts beside each row.
pub fn editable_uris(
    contents: &librespot_protocol::playlist4_external::ListItems,
) -> impl Iterator<Item = String> + '_ {
    contents
        .items
        .iter()
        .zip(&contents.meta_items)
        .filter(|(_, meta)| meta.capabilities.can_edit_items())
        .filter_map(|(item, _)| item.uri.clone())
}

/// One row of the account's playlist tree.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RootlistEntry {
    /// A playlist, by its URI.
    Playlist(String),
    /// A folder opens; everything until its end sits inside it.
    FolderStart {
        id: String,
        name: String,
    },
    FolderEnd,
}

/// The rootlist's rows from its URIs: playlists pass through, and the
/// `start-group`/`end-group` markers Spotify brackets folders with become
/// folder rows, their names percent-decoded.
pub fn parse_rootlist(uris: &[String]) -> Vec<RootlistEntry> {
    let mut entries = Vec::new();
    let mut depth = 0usize;
    for uri in uris {
        if let Some(rest) = uri.strip_prefix("spotify:start-group:") {
            let (id, name) = match rest.split_once(':') {
                Some((id, name)) => (id.to_string(), decode_folder_name(name)),
                None => (rest.to_string(), String::new()),
            };
            entries.push(RootlistEntry::FolderStart { id, name });
            depth += 1;
        } else if uri.starts_with("spotify:end-group:") {
            if depth > 0 {
                entries.push(RootlistEntry::FolderEnd);
                depth -= 1;
            }
        } else if uri.starts_with("spotify:playlist:") {
            entries.push(RootlistEntry::Playlist(uri.clone()));
        }
    }
    // A folder Spotify never closed still closes here.
    entries.extend(std::iter::repeat_n(RootlistEntry::FolderEnd, depth));
    entries
}

/// Folder names arrive percent-encoded, with `+` for a space.
fn decode_folder_name(encoded: &str) -> String {
    let bytes = encoded.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            // The digits are read from the bytes this loop is already
            // walking. Taking them by slicing the text instead put the end
            // of the slice two bytes past a `%`, which is inside a character
            // whenever the next one is not ASCII: a panic rather than the
            // parse error the arm below is written for, on exactly the names
            // that arm exists for.
            b'%' if i + 2 < bytes.len() => {
                let digit = |byte: u8| (byte as char).to_digit(16);
                match (digit(bytes[i + 1]), digit(bytes[i + 2])) {
                    (Some(high), Some(low)) => {
                        out.push((high << 4 | low) as u8);
                        i += 3;
                    }
                    _ => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

// ---------------------------------------------------------------------------
// Telemetry: what the engine was asked to do, what it did, and how long it
// took. Inert unless telemetry is on (see `crate::telemetry`).

/// Engines started in this run. librespot numbers play requests from zero
/// again in every engine, so events carry this beside them.
static ENGINE_GENERATION: AtomicU64 = AtomicU64::new(0);

static COMMANDS: telemetry::Counter = telemetry::Counter::new("player_commands");
static COMMAND_ERRORS: telemetry::Counter = telemetry::Counter::new("player_command_errors");
static UNANSWERED_COMMANDS: telemetry::Counter =
    telemetry::Counter::new("player_commands_unanswered");
static EVENTS: telemetry::Counter = telemetry::Counter::new("player_events");
static STALE_EVENTS: telemetry::Counter = telemetry::Counter::new("player_events_stale");
static LOADS: telemetry::Counter = telemetry::Counter::new("player_loads");
static COLD_LOADS: telemetry::Counter = telemetry::Counter::new("player_cold_loads");
static PRELOAD_HITS: telemetry::Counter = telemetry::Counter::new("player_preload_hits");
static UNAVAILABLE: telemetry::Counter = telemetry::Counter::new("player_unavailable");
static POSITION_CORRECTIONS: telemetry::Counter =
    telemetry::Counter::new("player_position_corrections");
static DECODE_GAPS: telemetry::Counter = telemetry::Counter::new("player_decode_gaps");
static LOAD_MAX_MS: telemetry::Gauge = telemetry::Gauge::peak("player_load_max_ms");
static DECODE_STALL_MAX_MS: telemetry::Gauge = telemetry::Gauge::peak("player_decode_stall_max_ms");

/// Bursts that would otherwise ship an event each: a queue rewritten one
/// song at a time, a volume dragged on another device.
static QUEUE_REPORTS: telemetry::Throttle = telemetry::Throttle::new();
static VOLUME_REPORTS: telemetry::Throttle = telemetry::Throttle::new();
/// A decoder that keeps stalling would raise an alarm every second.
static STALL_ALARMS: telemetry::Throttle = telemetry::Throttle::new();
static CORRECTION_ALARMS: telemetry::Throttle = telemetry::Throttle::new();
const ALARM_EVERY: Duration = Duration::from_secs(10);

/// How far back a change of the transport looks for what caused it.
const CAUSE_WINDOW: Duration = Duration::from_secs(5);
/// A command with nothing to show for it after this long is reported.
const UNANSWERED: Duration = Duration::from_secs(5);
/// Commands still unanswered after this long are forgotten.
const FORGOTTEN: Duration = Duration::from_secs(30);
/// How long an end of track, a failed load or an intent explains a load.
const RECENT: Duration = Duration::from_secs(10);
/// Quiet for less than this before playing on again is a flap.
const FLAP: Duration = Duration::from_secs(10);
/// How long a slow load still answers the intent that asked for it.
const SLOW_LOAD: Duration = Duration::from_secs(30);
/// Position reports this much further apart than the position moved mean
/// the decoder stopped for longer than the output's cushion lasts.
const DECODE_STALL: Duration = Duration::from_millis(500);
/// Commands sent this close together queue behind one another in Spirc.
const BURST: Duration = Duration::from_secs(3);

/// Traces a new play request is a step of, and those a start of playback is.
const LOAD_TRACES: [&str; 4] = ["next", "previous", "play", "transfer"];
const PLAY_TRACES: [&str; 5] = ["next", "previous", "play", "resume", "transfer"];
const ALL_TRACES: [&str; 6] = ["next", "previous", "play", "resume", "seek", "transfer"];
/// Intents a start of playback carries out.
const PLAY_INTENTS: [&str; 6] = [
    "toggle",
    "play",
    "next",
    "previous",
    "play_item",
    "transfer",
];
/// Traces a failed load ends.
const LOAD_FAILURE_TRACES: [&str; 3] = ["next", "previous", "play"];

fn register_metrics() {
    telemetry::register_counters(&[
        &COMMANDS,
        &COMMAND_ERRORS,
        &UNANSWERED_COMMANDS,
        &EVENTS,
        &STALE_EVENTS,
        &LOADS,
        &COLD_LOADS,
        &PRELOAD_HITS,
        &UNAVAILABLE,
        &POSITION_CORRECTIONS,
        &DECODE_GAPS,
    ]);
    telemetry::register_gauges(&[&LOAD_MAX_MS, &DECODE_STALL_MAX_MS]);
}

fn mark_traces(traces: &[&'static str], hop: &'static str) {
    for &trace in traces {
        telemetry::trace_mark(trace, hop);
    }
}

fn end_traces(traces: &[&'static str], outcome: &'static str) {
    for &trace in traces {
        telemetry::trace_end(trace, outcome);
    }
}

fn playback_name(playback: Playback) -> &'static str {
    match playback {
        Playback::Stopped => "stopped",
        Playback::Loading => "loading",
        Playback::Playing => "playing",
        Playback::Paused => "paused",
    }
}

/// What a Spotify URI names (`playlist`, `album`, `collection`...), without
/// its id: a user's collection URI carries the account name.
fn uri_kind(uri: &str) -> &str {
    let mut parts = uri.split(':').skip(1);
    match parts.next() {
        Some("user") => parts.nth(1).unwrap_or("user"),
        Some(kind) => kind,
        None => "unknown",
    }
}

fn proxy_kind(proxy: &crate::settings::ProxyConfig) -> &'static str {
    use crate::settings::ProxyConfig;
    match proxy {
        ProxyConfig::Invalid(_) => "invalid",
        ProxyConfig::Off => "off",
        ProxyConfig::System => "system",
        ProxyConfig::Http(_) => "http",
        ProxyConfig::Socks(_) => "socks",
    }
}

/// The engine's configuration, on the span that times its start. Device
/// names often carry their owner's name, so only digests of them are kept;
/// equal digests from two installs mean one Connect device id.
fn describe_engine(
    span: telemetry::Span,
    config: &EngineConfig,
    generation: u64,
    device_id: &str,
    proxied: bool,
) -> telemetry::Span {
    span.field("engine_gen", generation)
        .field("outcome", "incomplete")
        .field("device_id", telemetry::digest(device_id))
        .field("device_name", telemetry::digest(&config.device_name))
        .field("default_device_name", config.device_name == "Spotifast")
        .field("bitrate_kbps", config.bitrate_kbps)
        .field("gapless", config.gapless)
        .field("normalisation", config.normalisation)
        .field("normalisation_type", "auto")
        .field("autoplay", config.autoplay)
        .field(
            "backend",
            config
                .backend
                .as_deref()
                .filter(|name| !name.is_empty())
                .unwrap_or(crate::sink::NAME),
        )
        .field("named_device", config.audio_device.is_some())
        .field(
            "audio_device",
            config.audio_device.as_deref().map(telemetry::digest),
        )
        .field("initial_volume", config.initial_volume)
        .field("volume_ctrl", "cubic")
        .field("volume_steps", 64)
        .field("buffer_ms", config.buffer_ms)
        .field("audio_cache", config.audio_cache_dir.is_some())
        .field(
            "audio_cache_limit_mb",
            config.audio_cache_limit.map(|bytes| bytes / 1_000_000),
        )
        .field("proxy", proxy_kind(&config.proxy))
        .field("librespot_proxy", proxied)
        .field("position_update_ms", 1_000)
}

/// librespot starting or stopping the output, from its player thread.
fn report_sink_status(status: SinkStatus, generation: u64) {
    let status = match status {
        SinkStatus::Running => "running",
        SinkStatus::TemporarilyClosed => "temporarily_closed",
        SinkStatus::Closed => "closed",
    };
    telemetry::crumb("player.sink_status")
        .field("status", status)
        .field("engine_gen", generation)
        .emit();
}

fn command_kind(command: &PlayerCommand) -> &'static str {
    match command {
        PlayerCommand::Toggle => "toggle",
        PlayerCommand::Next => "next",
        PlayerCommand::Previous => "previous",
        PlayerCommand::ClearQueue => "clear_queue",
        PlayerCommand::AddToQueue(_) => "add_to_queue",
        PlayerCommand::Seek(_) => "seek",
        PlayerCommand::Volume(_) => "volume",
        PlayerCommand::VolumePreview(_) => "volume_preview",
        PlayerCommand::Shuffle(_) => "shuffle",
        PlayerCommand::Repeat(_) => "repeat",
        PlayerCommand::Load(_) => "load",
        PlayerCommand::Transfer => "transfer",
    }
}

/// The intents a command carries out.
fn command_intents(command: &PlayerCommand) -> Option<&'static [&'static str]> {
    let actions: &'static [&'static str] = match command {
        PlayerCommand::Toggle => &["toggle", "play", "pause"],
        PlayerCommand::Next => &["next"],
        PlayerCommand::Previous => &["previous"],
        PlayerCommand::AddToQueue(_) => &["queue_add"],
        PlayerCommand::Seek(_) => &["seek"],
        PlayerCommand::Volume(_) | PlayerCommand::VolumePreview(_) => &["volume"],
        PlayerCommand::Shuffle(_) => &["shuffle"],
        PlayerCommand::Repeat(_) => &["repeat"],
        PlayerCommand::Load(_) => &["play_item", "play"],
        PlayerCommand::Transfer => &["transfer"],
        PlayerCommand::ClearQueue => return None,
    };
    Some(actions)
}

/// The trace a command is a step of, when the app started one for it.
fn command_trace(command: &PlayerCommand) -> Option<&'static str> {
    match command {
        PlayerCommand::Next => Some("next"),
        PlayerCommand::Previous => Some("previous"),
        PlayerCommand::Load(_) => Some("play"),
        PlayerCommand::Toggle => Some("resume"),
        PlayerCommand::Seek(_) => Some("seek"),
        PlayerCommand::Transfer => Some("transfer"),
        _ => None,
    }
}

/// The player event that shows a command took effect.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Ack {
    /// A new play request, or for Previous well into a track, a seek.
    Load,
    /// Playing or Paused.
    PlayState,
    Seek,
}

fn command_ack(command: &PlayerCommand) -> Option<Ack> {
    match command {
        PlayerCommand::Next
        | PlayerCommand::Previous
        | PlayerCommand::Load(_)
        | PlayerCommand::Transfer => Some(Ack::Load),
        PlayerCommand::Toggle => Some(Ack::PlayState),
        PlayerCommand::Seek(_) => Some(Ack::Seek),
        _ => None,
    }
}

/// A command waiting for the player event that shows it took effect.
struct Pending {
    kind: &'static str,
    ack: Ack,
    generation: u64,
    at: Instant,
    intent_id: Option<String>,
    /// Already reported as unanswered.
    reported: bool,
}

/// Commands sent to the engine, shared by the backend thread that sends
/// them and the task that follows the engine's events.
struct Sent {
    /// Oldest first: Spirc takes commands up in order.
    pending: VecDeque<Pending>,
    /// When recent commands were sent, to see bursts queued in Spirc.
    recent: VecDeque<Instant>,
    last: Option<(&'static str, Instant)>,
    /// When interrupted playback was last restored.
    restored: Option<Instant>,
}

static SENT: Mutex<Sent> = Mutex::new(Sent {
    pending: VecDeque::new(),
    recent: VecDeque::new(),
    last: None,
    restored: None,
});

fn sent_commands() -> std::sync::MutexGuard<'static, Sent> {
    SENT.lock().unwrap_or_else(|p| p.into_inner())
}

/// Takes the oldest command of this engine that the event `answers`.
fn take_answered(generation: u64, answers: impl Fn(&Pending) -> bool) -> Option<Pending> {
    let mut sent = sent_commands();
    let index = sent
        .pending
        .iter()
        .position(|pending| pending.generation == generation && answers(pending))?;
    sent.pending.remove(index)
}

fn report_answer(command: &Pending, by: &'static str) {
    let waited = command.at.elapsed();
    let record = if command.reported || waited > Duration::from_secs(1) {
        telemetry::event("player.command_ack")
    } else {
        telemetry::crumb("player.command_ack")
    };
    record
        .field("command", command.kind)
        .field("by", by)
        .ms("ack_ms", waited)
        .field("late", command.reported)
        .field("intent_id", command.intent_id.clone())
        .field("engine_gen", command.generation)
        .emit();
}

/// Reports commands of this engine that nothing has come of: ignored by an
/// inactive device, stuck behind Spirc's requests, or lost.
fn report_unanswered(generation: u64) {
    let overdue: Vec<(&'static str, Duration, Option<String>)> = {
        let mut sent = sent_commands();
        let mut overdue = Vec::new();
        for pending in sent.pending.iter_mut() {
            let waited = pending.at.elapsed();
            if pending.generation == generation && !pending.reported && waited >= UNANSWERED {
                pending.reported = true;
                overdue.push((pending.kind, waited, pending.intent_id.clone()));
            }
        }
        sent.pending
            .retain(|pending| pending.at.elapsed() < FORGOTTEN);
        overdue
    };
    for (command, waited, intent_id) in overdue {
        UNANSWERED_COMMANDS.incr();
        telemetry::anomaly("connect.command_unacknowledged")
            .field("command", command)
            .ms("waited_ms", waited)
            .field("intent_id", intent_id)
            .field("engine_gen", generation)
            // Set while the output is silenced waiting for a skip.
            .field("gate_closed_ms", gate_closed_ms(generation))
            .emit();
    }
}

/// A command on its way to Spirc, reported once it was handed over.
struct CommandReport {
    record: telemetry::Event,
    kind: &'static str,
    ack: Option<Ack>,
    intent_id: Option<String>,
    started: Instant,
}

impl CommandReport {
    fn finish(self, generation: u64, interrupt: Option<Duration>, result: &Result<()>) {
        let now = Instant::now();
        let recent = {
            let mut sent = sent_commands();
            sent.recent.retain(|at| now.duration_since(*at) < BURST);
            let recent = sent.recent.len();
            if recent < 64 {
                sent.recent.push_back(now);
            }
            sent.last = Some((self.kind, now));
            if let (Some(ack), Ok(())) = (self.ack, result) {
                if sent.pending.len() >= 16 {
                    sent.pending.pop_front();
                }
                sent.pending.push_back(Pending {
                    kind: self.kind,
                    ack,
                    generation,
                    at: now,
                    intent_id: self.intent_id.clone(),
                    reported: false,
                });
            }
            recent
        };
        COMMANDS.incr();
        let record = self
            .record
            .field("recent_commands", recent)
            .field("interrupts_audio", interrupt.is_some())
            .field("interrupt_ms", interrupt.map(telemetry::duration_ms))
            .ms("elapsed_ms", self.started.elapsed());
        match result {
            Ok(()) => record.field("result", "ok").emit(),
            Err(error) => {
                COMMAND_ERRORS.incr();
                record
                    .field("result", "error")
                    .text("error", &format!("{error:#}"))
                    .emit();
            }
        }
        report_unanswered(generation);
    }
}

impl Engine {
    /// Starts the report of a command, with the state it meets and the
    /// intent it most likely carries out.
    fn report_command(&self, command: &PlayerCommand) -> CommandReport {
        let (playback, loading, position_ms, duration_ms) = {
            let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            (
                state.playback,
                state.loading,
                state.position_now(),
                state.track.as_ref().map(|track| track.duration_ms),
            )
        };
        let intent =
            command_intents(command).and_then(|actions| telemetry::recent_intent(actions, RECENT));
        let quiet = match command {
            PlayerCommand::AddToQueue(_) => !QUEUE_REPORTS.ready(Duration::from_secs(2)),
            _ => false,
        };
        let record = if quiet {
            telemetry::crumb("player.command")
        } else {
            telemetry::event("player.command")
        };
        let record = record
            .field("kind", command_kind(command))
            .field("engine_gen", self.generation)
            .field("playback", playback_name(playback))
            .field("loading", loading)
            .field("position_ms", position_ms)
            .field(
                "remaining_ms",
                duration_ms.map(|duration| duration.saturating_sub(position_ms)),
            )
            .field("intent_id", intent.as_ref().map(|intent| intent.id.clone()))
            .field("intent_source", intent.as_ref().map(|intent| intent.source))
            .field(
                "intent_age_ms",
                intent
                    .as_ref()
                    .map(|intent| telemetry::duration_ms(intent.at.elapsed())),
            );
        let record = match command {
            PlayerCommand::Seek(position) => record.field("to_ms", *position),
            PlayerCommand::Volume(level) | PlayerCommand::VolumePreview(level) => {
                record.field("level", *level)
            }
            PlayerCommand::Shuffle(shuffle) => record.field("shuffle", *shuffle),
            PlayerCommand::Repeat(mode) => record.field("repeat", mode.api_name()),
            PlayerCommand::AddToQueue(uri) => record.field("uri", uri.as_str()),
            PlayerCommand::Load(spec) => record
                .field("context", spec.context_uri.as_deref().map(uri_kind))
                .field(
                    "context_digest",
                    spec.context_uri.as_deref().map(telemetry::digest),
                )
                .field("uris", spec.uris.len())
                .field("offset_uri", spec.offset_uri.as_deref())
                .field("offset_index", spec.offset_index)
                .field("play", spec.play)
                .field("start_ms", spec.position_ms)
                .field("shuffle", spec.shuffle)
                .field("repeat", spec.repeat.map(RepeatMode::api_name))
                .field("autoplay", spec.autoplay),
            _ => record,
        };
        CommandReport {
            record,
            kind: command_kind(command),
            ack: command_ack(command),
            intent_id: intent.map(|intent| intent.id),
            started: Instant::now(),
        }
    }

    fn report_resume_point(&self, kind: &'static str, track: Option<(u32, bool)>) {
        telemetry::event("player.resume_point")
            .field("kind", kind)
            .field("engine_gen", self.generation)
            .field("position_ms", track.map(|(position, _)| position))
            .field("playing", track.map(|(_, playing)| playing))
            .emit();
    }
}

/// The output gate `AudioControl::interrupt` closes before a skip, open
/// again once the engine changes track, seeks or stops. Spirc ignoring the
/// skip leaves the old track playing on unheard.
struct Gate {
    at: Instant,
    command: &'static str,
    generation: u64,
}

static GATE: Mutex<Option<Gate>> = Mutex::new(None);

fn gate_closed(command: &'static str, generation: u64) {
    let mut gate = GATE.lock().unwrap_or_else(|p| p.into_inner());
    // Repeated skips share one closing, as in `AudioControl::interrupt`.
    if gate.is_none() {
        *gate = Some(Gate {
            at: Instant::now(),
            command,
            generation,
        });
    }
}

fn gate_opened(reason: &'static str, generation: u64) {
    let gate = GATE
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take_if(|gate| gate.generation == generation);
    if let Some(gate) = gate {
        telemetry::event("player.gate")
            .field("reason", reason)
            .field("command", gate.command)
            .ms("closed_ms", gate.at.elapsed())
            .field("engine_gen", generation)
            .emit();
    }
}

/// How long the gate has been closed, while it is.
fn gate_closed_ms(generation: u64) -> Option<f64> {
    GATE.lock()
        .unwrap_or_else(|p| p.into_inner())
        .as_ref()
        .filter(|gate| gate.generation == generation)
        .map(|gate| telemetry::duration_ms(gate.at.elapsed()))
}

/// The latest event of any engine, for the report of a session's end.
static LAST_EVENT: Mutex<Option<(u64, &'static str, Instant)>> = Mutex::new(None);

/// Reports the end of an engine's Spirc task: its Spotify Connect session
/// is over, on purpose or not.
fn report_spirc_end(
    generation: u64,
    started: Instant,
    intentional: bool,
    session_invalid: Option<bool>,
    ended: (Playback, u32, Option<String>),
) {
    let (playback, position_ms, track) = ended;
    let last_event = *LAST_EVENT.lock().unwrap_or_else(|p| p.into_inner());
    let last_event = last_event.filter(|(event_generation, _, _)| *event_generation == generation);
    if !intentional {
        telemetry::note_cause("session:ended", "the engine's Spotify session ended");
    }
    if ENGINE_GENERATION.load(Ordering::Relaxed) == generation {
        telemetry::set_context("connect_active", false);
    }
    let record = if intentional {
        telemetry::event("connect.spirc_ended")
    } else {
        telemetry::anomaly("connect.spirc_ended")
    };
    record
        .field("engine_gen", generation)
        .field("intentional", intentional)
        .field("session_invalid", session_invalid)
        .ms("engine_uptime_ms", started.elapsed())
        .field("playback", playback_name(playback))
        .field("position_ms", position_ms)
        .field("track", track.clone())
        .field("last_player_event", last_event.map(|(_, kind, _)| kind))
        .field(
            "since_last_player_event_ms",
            last_event.map(|(_, _, at)| telemetry::duration_ms(at.elapsed())),
        )
        .emit();
    if intentional || playback == Playback::Stopped {
        return;
    }
    // The engine marks itself stopped here without a player event.
    let cause = transport_cause();
    let record = telemetry::event("playback.state")
        .field("state", "stopped")
        .field("previous", playback_name(playback))
        .field("position_ms", position_ms)
        .field("track", track.clone())
        .field("engine_gen", generation)
        .field("via", "session_end");
    explained_by(record, cause.as_ref()).emit();
    if matches!(playback, Playback::Playing | Playback::Loading) {
        note_silence("stopped", cause.as_ref(), track, generation);
    }
}

/// A cause that can pause, resume or stop the local engine. A volume
/// change, a 429 or a queue running dry next to a pause does not explain it.
fn moves_transport(cause: &telemetry::Cause) -> bool {
    let (kind, detail) = (cause.kind.as_str(), cause.detail.as_str());
    match kind {
        "intent:toggle"
        | "intent:play"
        | "intent:pause"
        | "intent:next"
        | "intent:previous"
        | "intent:play_item"
        | "intent:transfer"
        | "webapi:remote_command"
        | "sink:write_error"
        | "sink:route_change"
        | "sink:reopen"
        | "player:end_of_track_error"
        | "system:wake"
        | "system:network_change"
        | "connect:remote_command"
        | "connect:inactive" => true,
        "connect:request" => matches!(
            detail,
            "pause"
                | "resume"
                | "play"
                | "skip_next"
                | "skip_prev"
                | "seek_to"
                | "transfer"
                | "set_queue"
                | "add_to_queue"
                | "update_context"
                | "set_options"
        ),
        "player:unavailable" => detail != "preload",
        _ => {
            (kind.starts_with("media:") && !matches!(detail, "ignored" | "volume"))
                || kind.starts_with("session:")
                || kind.starts_with("engine:")
                || kind.starts_with("app:")
        }
    }
}

/// The newest cause within the window that can move the transport.
fn transport_cause() -> Option<telemetry::Cause> {
    telemetry::recent_causes(CAUSE_WINDOW)
        .into_iter()
        .find(moves_transport)
}

/// Adds a transport change's cause to its record, as
/// `Event::explained` does with any cause.
fn explained_by(record: telemetry::Event, cause: Option<&telemetry::Cause>) -> telemetry::Event {
    match cause {
        Some(cause) => record
            .field("cause", cause.kind.as_str())
            .field("cause_detail", cause.detail.as_str())
            .ms("cause_age_ms", cause.at.elapsed()),
        None => record.field("cause", "none"),
    }
}

/// A pause or a resume that nothing on record asked for.
fn report_unexplained(
    kind: &'static str,
    state: &'static str,
    track: Option<String>,
    position_ms: Option<u32>,
    generation: u64,
    local_command: Option<&'static str>,
) {
    let causes: Vec<String> = telemetry::recent_causes(Duration::from_secs(30))
        .into_iter()
        .map(|cause| cause.kind)
        .collect();
    let last_command = sent_commands().last;
    telemetry::anomaly(kind)
        .field("state", state)
        .field("track", track)
        .field("position_ms", position_ms)
        .field("engine_gen", generation)
        .field("local_command", local_command)
        .field("causes_30s", causes)
        .field("last_command", last_command.map(|(command, _)| command))
        .field(
            "since_last_command_ms",
            last_command.map(|(_, at)| telemetry::duration_ms(at.elapsed())),
        )
        .emit();
}

/// When playing last went quiet, kept across engines: a session that drops
/// and comes back is heard as a pause too.
struct Silence {
    at: Instant,
    state: &'static str,
    cause: Option<(String, String)>,
    track: Option<String>,
    generation: u64,
}

static SILENCED: Mutex<Option<Silence>> = Mutex::new(None);

fn note_silence(
    state: &'static str,
    cause: Option<&telemetry::Cause>,
    track: Option<String>,
    generation: u64,
) {
    *SILENCED.lock().unwrap_or_else(|p| p.into_inner()) = Some(Silence {
        at: Instant::now(),
        state,
        cause: cause.map(|cause| (cause.kind.clone(), cause.detail.clone())),
        track,
        generation,
    });
}

/// Something pressed in the app, as opposed to a media key, a headset,
/// another device or the engine itself. An intent's detail is its source.
fn pressed_in_app(kind: &str, detail: &str) -> bool {
    kind.starts_with("intent:")
        && matches!(
            detail,
            "ui" | "keyboard" | "menu" | "notch" | "touchbar" | "tray" | "winamp"
        )
}

/// A stop or a resume someone meant: pressed in the app, asked for from
/// another Connect device, or an engine restart.
fn deliberate(kind: &str, detail: &str) -> bool {
    pressed_in_app(kind, detail)
        || (kind == "connect:request"
            && matches!(
                detail,
                "pause" | "resume" | "play" | "skip_next" | "skip_prev"
            ))
        || kind.starts_with("engine:")
        || matches!(kind, "app:restart_engine" | "app:apply_proxy")
}

/// Playing again soon after going quiet, on the same track: the pause and
/// resume users hear, unless both were meant.
fn check_flap(
    cause: Option<&telemetry::Cause>,
    track: Option<&str>,
    position_ms: Option<u32>,
    generation: u64,
) {
    let Some(silence) = SILENCED.lock().unwrap_or_else(|p| p.into_inner()).take() else {
        return;
    };
    let silent = silence.at.elapsed();
    if silent > FLAP || silence.track.as_deref() != track {
        return;
    }
    let stop_meant = silence
        .cause
        .as_ref()
        .is_some_and(|(kind, detail)| deliberate(kind, detail));
    let resume_meant = cause.is_some_and(|cause| deliberate(&cause.kind, &cause.detail));
    if stop_meant && resume_meant {
        return;
    }
    let (stop_cause, stop_detail) = silence.cause.unzip();
    telemetry::anomaly("playback.flap")
        .ms("silent_ms", silent)
        .field("stopped_as", silence.state)
        .field("stop_cause", stop_cause)
        .field(
            "stop_cause_detail",
            stop_detail.map(|detail| telemetry::scrub(&detail)),
        )
        .field("resume_cause", cause.map(|cause| cause.kind.clone()))
        .field(
            "resume_cause_detail",
            cause.map(|cause| telemetry::scrub(&cause.detail)),
        )
        .field("engine_changed", silence.generation != generation)
        .field("track", track)
        .field("position_ms", position_ms)
        .field("engine_gen", generation)
        .emit();
}

fn event_kind(event: &PlayerEvent) -> &'static str {
    match event {
        PlayerEvent::PlayRequestIdChanged { .. } => "play_request_id_changed",
        PlayerEvent::Stopped { .. } => "stopped",
        PlayerEvent::Loading { .. } => "loading",
        PlayerEvent::Preloading { .. } => "preloading",
        PlayerEvent::Playing { .. } => "playing",
        PlayerEvent::Paused { .. } => "paused",
        PlayerEvent::TimeToPreloadNextTrack { .. } => "time_to_preload_next_track",
        PlayerEvent::EndOfTrack { .. } => "end_of_track",
        PlayerEvent::Unavailable { .. } => "unavailable",
        PlayerEvent::AudioKeyUnavailable { .. } => "audio_key_unavailable",
        PlayerEvent::VolumeChanged { .. } => "volume_changed",
        PlayerEvent::PositionCorrection { .. } => "position_correction",
        PlayerEvent::PositionChanged { .. } => "position_changed",
        PlayerEvent::Seeked { .. } => "seeked",
        PlayerEvent::TrackChanged { .. } => "track_changed",
        PlayerEvent::SessionConnected { .. } => "session_connected",
        PlayerEvent::SessionDisconnected { .. } => "session_disconnected",
        PlayerEvent::SessionClientChanged { .. } => "session_client_changed",
        PlayerEvent::ShuffleChanged { .. } => "shuffle_changed",
        PlayerEvent::RepeatChanged { .. } => "repeat_changed",
        PlayerEvent::AutoPlayChanged { .. } => "auto_play_changed",
        PlayerEvent::FilterExplicitContentChanged { .. } => "filter_explicit_content_changed",
    }
}

fn event_track(event: &PlayerEvent) -> Option<&SpotifyUri> {
    match event {
        PlayerEvent::Stopped { track_id, .. }
        | PlayerEvent::Loading { track_id, .. }
        | PlayerEvent::Preloading { track_id, .. }
        | PlayerEvent::Playing { track_id, .. }
        | PlayerEvent::Paused { track_id, .. }
        | PlayerEvent::TimeToPreloadNextTrack { track_id, .. }
        | PlayerEvent::EndOfTrack { track_id, .. }
        | PlayerEvent::Unavailable { track_id, .. }
        | PlayerEvent::AudioKeyUnavailable { track_id, .. }
        | PlayerEvent::PositionCorrection { track_id, .. }
        | PlayerEvent::PositionChanged { track_id, .. }
        | PlayerEvent::Seeked { track_id, .. } => Some(track_id),
        _ => None,
    }
}

fn event_position(event: &PlayerEvent) -> Option<u32> {
    match event {
        PlayerEvent::Loading { position_ms, .. }
        | PlayerEvent::Playing { position_ms, .. }
        | PlayerEvent::Paused { position_ms, .. }
        | PlayerEvent::PositionCorrection { position_ms, .. }
        | PlayerEvent::PositionChanged { position_ms, .. }
        | PlayerEvent::Seeked { position_ms, .. } => Some(*position_ms),
        _ => None,
    }
}

/// One play request, from librespot taking it up to playing or failing.
struct LoadTimeline {
    request_id: u64,
    requested: Instant,
    /// What asked for it: a local command, the end of the previous track,
    /// a skip past an unavailable one, a restore, or nothing here (remote).
    trigger: &'static str,
    command: Option<&'static str>,
    /// From the local command being handed to Spirc to the request.
    command_ms: Option<Duration>,
    intent_id: Option<String>,
    /// From the previous track's end to the request.
    end_gap: Option<Duration>,
    loading: Option<Instant>,
    loading_track: Option<String>,
    start_ms: Option<u32>,
    track_changed: Option<Instant>,
    track: Option<String>,
    /// How the preload served it: `hit`, `same_track`, `not_ready`,
    /// `wrong_track`, `ready_unused` or `none` (nothing was preloaded).
    preload: &'static str,
}

/// Position reports further apart than the position moved, waiting for the
/// next report to tell a stalled decoder from reports delivered late.
struct Stall {
    gap: Duration,
    advance_ms: u32,
    stall_ms: i64,
    position_ms: u32,
}

/// Follows one engine's player events for telemetry: each load's timeline,
/// the transport and what changed it, stalls in the decoder, and the steps
/// of the app's traces.
struct EventWatch {
    generation: u64,
    shutting_down: Arc<std::sync::atomic::AtomicBool>,
    normalisation: Arc<AtomicU64>,
    /// The play request the engine is on, as `run_events` follows it.
    request: Option<u64>,
    playback: Playback,
    track: Option<String>,
    duration_ms: Option<u32>,
    last_event: Option<Instant>,
    /// The latest known position and when, to tell where playback should be.
    anchor: Option<(Instant, u32)>,
    /// The latest `PositionChanged` and when it arrived, while playing.
    report: Option<(Instant, u32)>,
    stall: Option<Stall>,
    /// When playback last started, sought or changed track.
    transition: Option<Instant>,
    load: Option<LoadTimeline>,
    load_done: Option<Instant>,
    preload_due: Option<Instant>,
    preloaded: Option<(String, Instant)>,
    /// The current track's end, and whether it came early (a skip after a
    /// read or decode error).
    track_end: Option<(Instant, bool)>,
    unavailable_at: VecDeque<Instant>,
    paused_at: Option<Instant>,
}

impl EventWatch {
    fn new(
        generation: u64,
        shutting_down: Arc<std::sync::atomic::AtomicBool>,
        normalisation: Arc<AtomicU64>,
    ) -> Self {
        Self {
            generation,
            shutting_down,
            normalisation,
            request: None,
            playback: Playback::Stopped,
            track: None,
            duration_ms: None,
            last_event: None,
            anchor: None,
            report: None,
            stall: None,
            transition: None,
            load: None,
            load_done: None,
            preload_due: None,
            preloaded: None,
            track_end: None,
            unavailable_at: VecDeque::new(),
            paused_at: None,
        }
    }

    fn is_current_engine(&self) -> bool {
        ENGINE_GENERATION.load(Ordering::Relaxed) == self.generation
    }

    fn position_now(&self) -> Option<u32> {
        let (at, position) = self.anchor?;
        Some(if self.playback == Playback::Playing {
            position.saturating_add(at.elapsed().as_millis() as u32)
        } else {
            position
        })
    }

    fn remaining_ms(&self) -> Option<u32> {
        let duration = self.duration_ms?;
        Some(duration.saturating_sub(self.position_now()?))
    }

    fn observe(&mut self, event: &PlayerEvent) {
        if !telemetry::enabled() {
            return;
        }
        let now = Instant::now();
        let kind = event_kind(event);
        EVENTS.incr();
        let since_previous = self
            .last_event
            .replace(now)
            .map(|at| now.duration_since(at));
        *LAST_EVENT.lock().unwrap_or_else(|p| p.into_inner()) = Some((self.generation, kind, now));
        // As `run_events` drops them: events of a play request since replaced.
        let stale = !matches!(event, PlayerEvent::PlayRequestIdChanged { .. })
            && matches!(
                (self.request, event.get_play_request_id()),
                (Some(current), Some(incoming)) if current != incoming
            );
        if stale {
            STALE_EVENTS.incr();
            telemetry::crumb("player.event_stale")
                .field("kind", kind)
                .field("play_request_id", event.get_play_request_id())
                .field("current_request_id", self.request)
                .field("engine_gen", self.generation)
                .emit();
            return;
        }
        if !matches!(
            event,
            PlayerEvent::PositionChanged { .. } | PlayerEvent::PositionCorrection { .. }
        ) && let Some(stall) = self.stall.take()
        {
            self.report_stall(stall, None);
        }
        report_unanswered(self.generation);
        let record = match event {
            PlayerEvent::PositionChanged { .. } => telemetry::crumb("player.event"),
            PlayerEvent::VolumeChanged { .. } if !VOLUME_REPORTS.ready(Duration::from_secs(2)) => {
                telemetry::crumb("player.event")
            }
            _ => telemetry::event("player.event"),
        };
        let mut record = record
            .field("kind", kind)
            .field(
                "play_request_id",
                event.get_play_request_id().or(self.request),
            )
            .field("engine_gen", self.generation)
            .field(
                "since_previous_ms",
                since_previous.map(telemetry::duration_ms),
            )
            .field("position_ms", event_position(event));
        if !matches!(event, PlayerEvent::PositionChanged { .. })
            && let Some(track) = event_track(event)
        {
            record = record.field("track", track.to_uri().ok());
        }
        self.follow(event, record, now).emit();
    }

    /// Follows one event: the load it belongs to, the transport, and the
    /// traces it is a step of. Returns its record with what was learnt.
    fn follow(
        &mut self,
        event: &PlayerEvent,
        record: telemetry::Event,
        now: Instant,
    ) -> telemetry::Event {
        match event {
            PlayerEvent::PlayRequestIdChanged { play_request_id } => {
                self.request_changed(*play_request_id, record, now)
            }
            PlayerEvent::Loading {
                track_id,
                position_ms,
                ..
            } => {
                if let Some(load) = &mut self.load
                    && load.loading.is_none()
                {
                    load.loading = Some(now);
                    load.loading_track = track_id.to_uri().ok();
                    load.start_ms = Some(*position_ms);
                }
                if self.playback == Playback::Stopped {
                    self.playback = Playback::Loading;
                }
                self.anchor = Some((now, *position_ms));
                self.report = None;
                self.transition = Some(now);
                mark_traces(&LOAD_TRACES, "loading");
                let since_request = self
                    .load
                    .as_ref()
                    .map(|load| telemetry::duration_ms(now.duration_since(load.requested)));
                record.field("since_request_ms", since_request)
            }
            PlayerEvent::TrackChanged { audio_item } => self.track_changed(audio_item, record, now),
            PlayerEvent::Playing { position_ms, .. } => {
                mark_traces(&PLAY_TRACES, "playing");
                let previous = std::mem::replace(&mut self.playback, Playback::Playing);
                self.anchor = Some((now, *position_ms));
                self.report = None;
                self.transition = Some(now);
                let answered =
                    take_answered(self.generation, |pending| pending.ack == Ack::PlayState);
                if let Some(command) = &answered {
                    report_answer(command, "playing");
                }
                let trigger = self.load.as_ref().map(|load| load.trigger);
                self.finish_load("playing");
                self.report_state(
                    "playing",
                    previous,
                    Some(*position_ms),
                    answered.as_ref(),
                    trigger,
                );
                record
            }
            PlayerEvent::Paused { position_ms, .. } => {
                let previous = std::mem::replace(&mut self.playback, Playback::Paused);
                self.anchor = Some((now, *position_ms));
                self.report = None;
                let answered =
                    take_answered(self.generation, |pending| pending.ack == Ack::PlayState);
                if let Some(command) = &answered {
                    report_answer(command, "paused");
                }
                let trigger = self.load.as_ref().map(|load| load.trigger);
                self.finish_load("paused");
                // A request that ends paused is never heard, so its trace
                // would only time out.
                for trace in LOAD_TRACES {
                    if telemetry::trace_has(trace, "track_changed") {
                        telemetry::trace_end(trace, "paused");
                    }
                }
                telemetry::trace_end("resume", "paused");
                telemetry::trace_end("seek", "interrupted");
                self.report_state(
                    "paused",
                    previous,
                    Some(*position_ms),
                    answered.as_ref(),
                    trigger,
                );
                record
            }
            PlayerEvent::Stopped { .. } => {
                let previous = std::mem::replace(&mut self.playback, Playback::Stopped);
                self.anchor = None;
                self.report = None;
                // Next or Previous with nothing left to play stops instead.
                let answered = take_answered(self.generation, |pending| {
                    pending.ack == Ack::Load && matches!(pending.kind, "next" | "previous")
                });
                if let Some(command) = &answered {
                    report_answer(command, "stopped");
                }
                let trigger = self.load.as_ref().map(|load| load.trigger);
                self.finish_load("stopped");
                gate_opened("stopped", self.generation);
                telemetry::trace_end("seek", "interrupted");
                end_traces(&ALL_TRACES, "stopped");
                self.report_state("stopped", previous, None, answered.as_ref(), trigger);
                record
            }
            PlayerEvent::Seeked { position_ms, .. } => self.seeked(*position_ms, record, now),
            PlayerEvent::PositionChanged { position_ms, .. } => {
                self.position_report(*position_ms, now);
                self.anchor = Some((now, *position_ms));
                record
            }
            PlayerEvent::PositionCorrection { position_ms, .. } => {
                self.position_corrected(*position_ms, record, now)
            }
            PlayerEvent::EndOfTrack { .. } => {
                let remaining = self.remaining_ms();
                // librespot skips a track it cannot read or decode on with
                // an end of track, long before the real end.
                let early = remaining.is_some_and(|remaining| remaining > 5_000);
                self.track_end = Some((now, early));
                self.report = None;
                if early {
                    telemetry::note_cause(
                        "player:end_of_track_error",
                        format!("{} ms before its end", remaining.unwrap_or_default()),
                    );
                    end_traces(&LOAD_FAILURE_TRACES, "failed");
                }
                self.finish_load("end_of_track");
                // A gate still closed means the track played on unheard.
                record
                    .field("remaining_ms", remaining)
                    .field("early", early)
                    .field("gate_closed_ms", gate_closed_ms(self.generation))
            }
            PlayerEvent::Unavailable { track_id, .. } => {
                self.load_failed(track_id, false, record, now)
            }
            PlayerEvent::AudioKeyUnavailable { track_id, .. } => {
                self.load_failed(track_id, true, record, now)
            }
            PlayerEvent::TimeToPreloadNextTrack { .. } => {
                self.preload_due = Some(now);
                record.field("remaining_ms", self.remaining_ms())
            }
            PlayerEvent::Preloading { track_id } => {
                let since_due = self
                    .preload_due
                    .map(|at| telemetry::duration_ms(now.duration_since(at)));
                self.preloaded = track_id.to_uri().ok().map(|track| (track, now));
                record.field("since_due_ms", since_due)
            }
            PlayerEvent::VolumeChanged { volume } => record.field("volume", *volume),
            PlayerEvent::ShuffleChanged { shuffle } => record.field("shuffle", *shuffle),
            PlayerEvent::RepeatChanged { context, track } => record
                .field("repeat_context", *context)
                .field("repeat_track", *track),
            PlayerEvent::AutoPlayChanged { auto_play } => record.field("auto_play", *auto_play),
            PlayerEvent::FilterExplicitContentChanged { filter } => {
                record.field("filter_explicit", *filter)
            }
            // Neither the connection id nor the account name is recorded.
            PlayerEvent::SessionConnected { .. } => {
                if self.is_current_engine() {
                    telemetry::set_context("connect_active", true);
                }
                record
            }
            PlayerEvent::SessionDisconnected { .. } => {
                telemetry::note_cause(
                    "connect:inactive",
                    "this device stopped being the active Connect device",
                );
                if self.is_current_engine() {
                    telemetry::set_context("connect_active", false);
                }
                record
            }
            PlayerEvent::SessionClientChanged {
                client_name,
                client_brand_name,
                client_model_name,
                ..
            } => {
                // A device's name often carries its owner's.
                let client = (!client_name.is_empty()).then(|| telemetry::digest(client_name));
                telemetry::note_cause("connect:client_changed", client.clone().unwrap_or_default());
                record
                    .field("client", client)
                    .text("client_brand", client_brand_name)
                    .text("client_model", client_model_name)
            }
        }
    }

    fn request_changed(
        &mut self,
        request_id: u64,
        record: telemetry::Event,
        now: Instant,
    ) -> telemetry::Event {
        self.finish_load("superseded");
        // A seek during a load restarts it, so no seek is heard as such.
        telemetry::trace_end("seek", "interrupted");
        let previous = self.request.replace(request_id);
        let command = take_answered(self.generation, |pending| pending.ack == Ack::Load);
        let restored = sent_commands().restored;
        let ended = self.track_end.filter(|(at, _)| at.elapsed() < RECENT);
        let skipped = self
            .unavailable_at
            .back()
            .is_some_and(|at| at.elapsed() < RECENT);
        let trigger = match &command {
            Some(command)
                if command.kind == "load"
                    && restored.is_some_and(|at| {
                        command.at.saturating_duration_since(at) < Duration::from_secs(1)
                    }) =>
            {
                "restore"
            }
            Some(command) => command.kind,
            None if ended.is_some_and(|(_, early)| early) => "error_skip",
            None if ended.is_some() => "end_of_track",
            None if skipped => "unavailable_skip",
            None if restored.is_some_and(|at| at.elapsed() < Duration::from_secs(15)) => "restore",
            // Nothing here asked for it: another device or the Web API did.
            None => "remote",
        };
        if trigger == "remote" {
            telemetry::note_cause("connect:remote_command", "load");
        }
        let command_ms = command
            .as_ref()
            .map(|command| now.duration_since(command.at));
        if let Some(command) = &command {
            report_answer(command, "play_request_id_changed");
        }
        self.load = Some(LoadTimeline {
            request_id,
            requested: now,
            trigger,
            command: command.as_ref().map(|command| command.kind),
            command_ms,
            intent_id: command.and_then(|command| command.intent_id),
            end_gap: ended.map(|(at, _)| now.duration_since(at)),
            loading: None,
            loading_track: None,
            start_ms: None,
            track_changed: None,
            track: None,
            preload: "unknown",
        });
        self.report = None;
        mark_traces(&LOAD_TRACES, "play_request");
        record
            .field("play_request_id", request_id)
            .field("previous_request_id", previous)
            .field("trigger", trigger)
            .field(
                "command_to_request_ms",
                command_ms.map(telemetry::duration_ms),
            )
    }

    fn track_changed(
        &mut self,
        item: &AudioItem,
        record: telemetry::Event,
        now: Instant,
    ) -> telemetry::Event {
        let uri = item.uri.clone();
        let previous = self.track.replace(uri.clone());
        self.duration_ms = Some(item.duration_ms);
        let ready = self.preloaded.as_ref().map(|(track, _)| *track == uri);
        let due = self.preload_due.is_some();
        let mut preload = None;
        if let Some(load) = &mut self.load {
            let kind = match (load.loading.is_some(), ready) {
                (false, _) if previous.as_deref() == Some(uri.as_str()) => "same_track",
                (false, _) => "hit",
                (true, Some(true)) => "ready_unused",
                (true, Some(false)) => "wrong_track",
                (true, None) if due => "not_ready",
                (true, None) => "none",
            };
            load.preload = kind;
            load.track_changed = Some(now);
            load.track = Some(uri.clone());
            preload = Some(kind);
        }
        let since_request = self
            .load
            .as_ref()
            .map(|load| telemetry::duration_ms(now.duration_since(load.requested)));
        if self
            .load
            .as_ref()
            .is_some_and(|load| load.loading.is_none())
        {
            mark_traces(&LOAD_TRACES, "preload_hit");
        }
        mark_traces(&LOAD_TRACES, "track_changed");
        gate_opened("track_changed", self.generation);
        self.preloaded = None;
        self.preload_due = None;
        self.track_end = None;
        self.anchor = None;
        self.report = None;
        self.transition = Some(now);
        record
            .field("track", uri)
            .field("duration_ms", item.duration_ms)
            .field("explicit", item.is_explicit)
            .field("preload", preload)
            .field("since_request_ms", since_request)
    }

    fn seeked(
        &mut self,
        position_ms: u32,
        record: telemetry::Event,
        now: Instant,
    ) -> telemetry::Event {
        let answered = take_answered(self.generation, |pending| {
            pending.ack == Ack::Seek || (pending.ack == Ack::Load && pending.kind == "previous")
        });
        mark_traces(&["seek", "previous"], "seeked");
        gate_opened("seeked", self.generation);
        if let Some(command) = &answered {
            report_answer(command, "seeked");
            // Previous well into a track rewinds it instead of loading
            // another, so no track change will follow.
            if command.kind == "previous" {
                telemetry::trace_end("previous", "seeked");
            }
        }
        // A seek while paused is not heard until playback resumes.
        if self.playback != Playback::Playing {
            telemetry::trace_end("seek", "paused");
        }
        let local = answered.is_some()
            || self.load.is_some()
            || self.load_done.is_some_and(|at| at.elapsed() < BURST)
            || sent_commands()
                .last
                .is_some_and(|(_, at)| at.elapsed() < BURST);
        if !local {
            telemetry::note_cause("connect:remote_command", "seek");
        }
        self.anchor = Some((now, position_ms));
        self.report = None;
        self.transition = Some(now);
        record
            .field(
                "local_command",
                answered.as_ref().map(|command| command.kind),
            )
            .field("remote", !local)
    }

    fn position_corrected(
        &mut self,
        position_ms: u32,
        record: telemetry::Event,
        now: Instant,
    ) -> telemetry::Event {
        POSITION_CORRECTIONS.incr();
        let expected = self.position_now();
        let behind_ms = expected.map(|expected| i64::from(expected) - i64::from(position_ms));
        let since_transition = self.transition.map(|at| now.duration_since(at));
        // librespot corrects once the decoder skipped ahead or fell a second
        // behind real time: a stall, unless playback had only just begun.
        let settling = since_transition.is_some_and(|since| since < Duration::from_millis(1_500));
        if !settling && CORRECTION_ALARMS.ready(ALARM_EVERY) {
            telemetry::anomaly("player.position_correction")
                .field("position_ms", position_ms)
                .field("expected_ms", expected)
                .field("behind_ms", behind_ms)
                .field(
                    "since_transition_ms",
                    since_transition.map(telemetry::duration_ms),
                )
                .field("track", self.track.clone())
                .field("play_request_id", self.request)
                .field("engine_gen", self.generation)
                .emit();
        }
        self.anchor = Some((now, position_ms));
        record
            .field("expected_ms", expected)
            .field("behind_ms", behind_ms)
            .field("settling", settling)
    }

    fn load_failed(
        &mut self,
        track_id: &SpotifyUri,
        audio_key: bool,
        record: telemetry::Event,
        now: Instant,
    ) -> telemetry::Event {
        let track = track_id.to_uri().ok();
        // A failed preload names the next track under the current request.
        let current = audio_key
            || self.load.as_ref().is_some_and(|load| {
                load.track_changed.is_none()
                    && load.loading_track.is_some()
                    && load.loading_track == track
            });
        UNAVAILABLE.incr();
        self.unavailable_at
            .retain(|at| now.duration_since(*at) < Duration::from_secs(20));
        self.unavailable_at.push_back(now);
        let detail = if audio_key {
            "audio_key"
        } else if current {
            "current"
        } else {
            "preload"
        };
        telemetry::note_cause("player:unavailable", detail);
        if current {
            self.finish_load(if audio_key {
                "audio_key_unavailable"
            } else {
                "unavailable"
            });
            end_traces(&LOAD_FAILURE_TRACES, "failed");
        }
        record
            .field("current", current)
            .field("count_20s", self.unavailable_at.len())
    }

    /// Position reports come about once a second, each right after a packet
    /// decodes. A wider gap than the position moved is time the decoder spent
    /// blocked: on the network, on the disk, or on a full output.
    fn position_report(&mut self, position_ms: u32, now: Instant) {
        let previous = self.report.replace((now, position_ms));
        if self.playback != Playback::Playing {
            return;
        }
        let Some((at, previous_ms)) = previous else {
            return;
        };
        let gap = now.duration_since(at);
        if let Some(stall) = self.stall.take() {
            self.report_stall(stall, Some(gap));
        }
        let advance_ms = position_ms.saturating_sub(previous_ms);
        let stall_ms = gap.as_millis() as i64 - i64::from(advance_ms);
        if stall_ms >= DECODE_STALL.as_millis() as i64 {
            self.stall = Some(Stall {
                gap,
                advance_ms,
                stall_ms,
                position_ms,
            });
        }
    }

    /// Reports a stall once the report after it arrived (`next` is the gap
    /// before that one), or unverified when playback changed first.
    fn report_stall(&self, stall: Stall, next: Option<Duration>) {
        // Reports held up on the way here arrive bunched, so the one after
        // a late report comes early. After a real stall the decoder reports
        // a full interval later.
        if next.is_some_and(|next| next < Duration::from_millis(600)) {
            let record = if STALL_ALARMS.ready(ALARM_EVERY) {
                telemetry::anomaly("player.event_delay")
            } else {
                telemetry::event("player.event_delay")
            };
            record
                .field("delay_ms", stall.stall_ms)
                .field("next_gap_ms", next.map(telemetry::duration_ms))
                .field("engine_gen", self.generation)
                .emit();
            return;
        }
        DECODE_GAPS.incr();
        DECODE_STALL_MAX_MS.raise(stall.stall_ms);
        let record = if STALL_ALARMS.ready(ALARM_EVERY) {
            telemetry::anomaly("player.decode_gap")
        } else {
            telemetry::event("player.decode_gap")
        };
        record
            .field("stall_ms", stall.stall_ms)
            .ms("gap_ms", stall.gap)
            .field("advance_ms", stall.advance_ms)
            .field("position_ms", stall.position_ms)
            .field("verified", next.is_some())
            .field("next_gap_ms", next.map(telemetry::duration_ms))
            .field("track", self.track.clone())
            .field("play_request_id", self.request)
            .field("engine_gen", self.generation)
            .emit();
    }

    /// Reports the load in progress as finished with `outcome`.
    fn finish_load(&mut self, outcome: &'static str) {
        let Some(load) = self.load.take() else {
            return;
        };
        let now = Instant::now();
        self.load_done = Some(now);
        let requested = load.requested;
        let since_request = move |at: Option<Instant>| {
            at.map(|at| telemetry::duration_ms(at.duration_since(requested)))
        };
        let total = now.duration_since(requested);
        LOADS.incr();
        if load.loading.is_some() {
            COLD_LOADS.incr();
        }
        if matches!(load.preload, "hit" | "same_track") {
            PRELOAD_HITS.incr();
        }
        if matches!(outcome, "playing" | "paused") {
            LOAD_MAX_MS.raise(total.as_millis() as i64);
        }
        let cold_load = load
            .loading
            .zip(load.track_changed)
            .map(|(loading, changed)| telemetry::duration_ms(changed.duration_since(loading)));
        // For a track that followed the previous one's end: how long the
        // output had nothing new to play.
        let end_to_track_changed = load
            .end_gap
            .zip(load.track_changed)
            .map(|(gap, changed)| telemetry::duration_ms(gap + changed.duration_since(requested)));
        telemetry::event("player.load")
            .field("play_request_id", load.request_id)
            .field("engine_gen", self.generation)
            .field("outcome", outcome)
            .field("trigger", load.trigger)
            .field("command", load.command)
            .field("intent_id", load.intent_id)
            .field(
                "command_to_request_ms",
                load.command_ms.map(telemetry::duration_ms),
            )
            .field(
                "since_command_ms",
                load.command_ms
                    .map(|command| telemetry::duration_ms(command + total)),
            )
            .field(
                "end_to_request_ms",
                load.end_gap.map(telemetry::duration_ms),
            )
            .field("end_to_track_changed_ms", end_to_track_changed)
            .field("cold", load.loading.is_some())
            .field("preload", load.preload)
            .field("request_to_loading_ms", since_request(load.loading))
            .field("cold_load_ms", cold_load)
            .field(
                "request_to_track_changed_ms",
                since_request(load.track_changed),
            )
            .field(
                "track_changed_to_state_ms",
                load.track_changed
                    .map(|at| telemetry::duration_ms(now.duration_since(at))),
            )
            .ms("total_ms", total)
            .field("track", load.track.or(load.loading_track))
            .field("start_ms", load.start_ms)
            .field_with("normalisation_factor", || {
                f64::from_bits(self.normalisation.load(Ordering::Relaxed))
            })
            .emit();
    }

    /// Reports a change of the transport with what caused it, and raises
    /// the alarm for a pause or a resume nothing asked for.
    fn report_state(
        &mut self,
        state: &'static str,
        previous: Playback,
        position_ms: Option<u32>,
        answered: Option<&Pending>,
        trigger: Option<&'static str>,
    ) {
        let now = Instant::now();
        let cause = transport_cause();
        let shutting_down = self.shutting_down.load(Ordering::SeqCst);
        let after_end = self.track_end.is_some_and(|(at, _)| at.elapsed() < RECENT);
        let paused_for = match state {
            "playing" => self.paused_at.take().map(|at| now.duration_since(at)),
            "paused" => {
                if previous != Playback::Paused {
                    self.paused_at = Some(now);
                }
                None
            }
            _ => {
                self.paused_at = None;
                None
            }
        };
        let record = telemetry::event("playback.state")
            .field("state", state)
            .field("previous", playback_name(previous))
            .field("position_ms", position_ms)
            .field("track", self.track.clone())
            .field("play_request_id", self.request)
            .field("engine_gen", self.generation)
            .field("load_trigger", trigger)
            .field("local_command", answered.map(|command| command.kind))
            .field(
                "intent_id",
                answered.and_then(|command| command.intent_id.clone()),
            )
            .field("paused_for_ms", paused_for.map(telemetry::duration_ms))
            .field("shutting_down", shutting_down)
            .field("after_end_of_track", after_end)
            .field("via", "player");
        explained_by(record, cause.as_ref()).emit();
        let silenced = state != "playing" && previous == Playback::Playing;
        // An engine going down, or a context running out, is no mystery.
        let ran_out = state == "stopped" && after_end;
        if silenced && cause.is_none() && !shutting_down && !ran_out {
            report_unexplained(
                "playback.pause_without_cause",
                state,
                self.track.clone(),
                position_ms,
                self.generation,
                answered.map(|command| command.kind),
            );
        }
        // A load the user asked for can take longer than the cause window.
        if state == "playing"
            && previous == Playback::Paused
            && cause.is_none()
            && !PLAY_TRACES.iter().any(|trace| telemetry::trace_open(trace))
            && telemetry::recent_intent(&PLAY_INTENTS, SLOW_LOAD).is_none()
        {
            report_unexplained(
                "playback.resume_without_cause",
                state,
                self.track.clone(),
                position_ms,
                self.generation,
                answered.map(|command| command.kind),
            );
        }
        if silenced {
            note_silence(state, cause.as_ref(), self.track.clone(), self.generation);
        } else if state == "playing" && previous != Playback::Playing {
            check_flap(
                cause.as_ref(),
                self.track.as_deref(),
                position_ms,
                self.generation,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn loading_another_song_keeps_repeat_with_or_without_shuffle() {
        for shuffle in [None, Some(false), Some(true)] {
            for mode in [RepeatMode::Off, RepeatMode::Context, RepeatMode::Track] {
                let mut spec = LoadSpec {
                    shuffle,
                    ..LoadSpec::default()
                };
                let LoadContextOptions::Options(options) = spec.context_options(mode) else {
                    panic!("ordinary playback must supply repeat options");
                };
                assert_eq!(options.shuffle, shuffle.unwrap_or(false));
                assert_eq!(options.repeat, mode == RepeatMode::Context);
                assert_eq!(options.repeat_track, mode == RepeatMode::Track);

                // A user's new preference wins over an older player event,
                // including turning repeat off immediately before a load.
                spec.repeat = Some(mode);
                let LoadContextOptions::Options(options) = spec.context_options(mode.next()) else {
                    panic!("ordinary playback must supply repeat options");
                };
                assert_eq!(options.repeat, mode == RepeatMode::Context);
                assert_eq!(options.repeat_track, mode == RepeatMode::Track);
            }
        }
        assert!(matches!(
            LoadSpec {
                autoplay: true,
                ..LoadSpec::default()
            }
            .context_options(RepeatMode::Context),
            LoadContextOptions::Autoplay
        ));
    }

    #[test]
    fn playback_metadata_preserves_each_artist_id_and_name() {
        use librespot_metadata::artist::{ArtistWithRole, ArtistsWithRole};

        let credits = [
            (
                "spotify:artist:0000000000000000000001",
                "Tyler, the Creator",
            ),
            ("spotify:artist:0000000000000000000002", "Guest"),
        ];
        let item = AudioItem {
            track_id: uri(),
            uri: uri().to_uri().unwrap(),
            files: Default::default(),
            name: "Song".into(),
            covers: vec![],
            language: vec![],
            duration_ms: 200_000,
            is_explicit: false,
            availability: Ok(()),
            alternatives: None,
            unique_fields: UniqueFields::Track {
                artists: ArtistsWithRole(
                    credits
                        .iter()
                        .map(|(uri, name)| ArtistWithRole {
                            id: librespot_core::SpotifyUri::from_uri(uri).unwrap(),
                            name: (*name).into(),
                            role: Default::default(),
                        })
                        .collect(),
                ),
                album: "Album".into(),
                album_artists: vec![],
                popularity: 0,
                number: 1,
                disc_number: 1,
            },
        };

        let track = local_track(&item);
        assert_eq!(track.artist_names(), "Tyler, the Creator, Guest");
        assert_eq!(track.artists.len(), 2);
        for (artist, (uri, name)) in track.artists.iter().zip(credits) {
            assert_eq!(artist.id.as_deref(), crate::util::uri_id(uri));
            assert_eq!(artist.uri.as_deref(), Some(uri));
            assert_eq!(artist.name, name);
        }
    }

    #[test]
    fn the_rootlist_markers_become_folders() {
        let uris: Vec<String> = [
            "spotify:playlist:aaa",
            "spotify:start-group:f1:Late%20Night+Mix",
            "spotify:playlist:bbb",
            "spotify:playlist:ccc",
            "spotify:end-group:f1",
            "spotify:playlist:ddd",
            "spotify:start-group:f2:Open",
            "spotify:playlist:eee",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let rows = parse_rootlist(&uris);
        assert_eq!(
            rows[0],
            RootlistEntry::Playlist("spotify:playlist:aaa".into())
        );
        assert_eq!(
            rows[1],
            RootlistEntry::FolderStart {
                id: "f1".into(),
                name: "Late Night Mix".into()
            }
        );
        assert_eq!(rows[4], RootlistEntry::FolderEnd);
        // The unclosed folder still closes.
        assert_eq!(rows.last(), Some(&RootlistEntry::FolderEnd));
        assert_eq!(rows.len(), 9);
    }

    #[test]
    fn a_folder_name_with_a_bare_percent_keeps_its_percent() {
        // The decoder already has an answer for a `%` that begins no escape:
        // it keeps the `%` and moves on. That answer could not be reached
        // when the next character was multi-byte, because the two digits
        // were taken by slicing the `&str` and the second byte of a slice
        // that lands inside a character is a panic, not a parse error.
        let uris: Vec<String> = ["spotify:start-group:f1:100%25 \u{c548}\u{b155}"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(
            parse_rootlist(&uris)[0],
            RootlistEntry::FolderStart {
                id: "f1".into(),
                name: "100% \u{c548}\u{b155}".into()
            }
        );

        // The same shape with nothing to decode at all.
        let raw: Vec<String> = ["spotify:start-group:f2:100% \u{c548}\u{b155}"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(
            parse_rootlist(&raw)[0],
            RootlistEntry::FolderStart {
                id: "f2".into(),
                name: "100% \u{c548}\u{b155}".into()
            }
        );
    }

    /// A playlist shared by invitation is editable by Spotify's word in the
    /// rootlist, never by the Web API's collaborative flag.
    #[test]
    fn the_rootlist_says_which_playlists_take_songs() {
        use librespot_protocol::playlist_permission::Capabilities;
        use librespot_protocol::playlist4_external::{Item, ListItems, MetaItem};

        // #given
        let mut contents = ListItems::new();
        for (uri, can_edit) in [
            ("spotify:playlist:mine", Some(true)),
            ("spotify:playlist:theirs", Some(false)),
            ("spotify:playlist:shared", Some(true)),
            ("spotify:playlist:undecorated", None),
        ] {
            let mut item = Item::new();
            item.set_uri(uri.to_string());
            contents.items.push(item);
            let mut meta = MetaItem::new();
            if let Some(can_edit) = can_edit {
                let mut capabilities = Capabilities::new();
                capabilities.set_can_edit_items(can_edit);
                meta.capabilities = protobuf::MessageField::some(capabilities);
            }
            contents.meta_items.push(meta);
        }

        // #when
        let editable: Vec<String> = editable_uris(&contents).collect();

        // #then
        assert_eq!(
            editable,
            ["spotify:playlist:mine", "spotify:playlist:shared"]
        );
    }

    use super::*;
    use librespot_core::SpotifyUri;

    /// Settings saved before librespot's rodio backend left the build name
    /// "rodio", which has always meant Spotifast's own output; that and any
    /// backend this build lacks still play, through that output.
    #[test]
    fn an_old_rodio_setting_plays_through_spotifasts_own_output() {
        assert!(librespot_backend(Some("rodio")).is_none());
        assert!(librespot_backend(None).is_none());
        assert!(librespot_backend(Some("no-such-backend")).is_none());
        assert!(
            audio_backend::find(Some("rodio".into())).is_none(),
            "librespot's rodio backend is not built in"
        );
        if cfg!(target_os = "linux") {
            assert!(librespot_backend(Some("pulseaudio")).is_some());
        }
    }

    fn uri() -> SpotifyUri {
        SpotifyUri::from_uri("spotify:track:14XWXWv5FoCbFzLksawpEe").unwrap()
    }

    fn interlude() -> AudioItem {
        AudioItem {
            track_id: uri(),
            uri: uri().to_uri().unwrap(),
            files: Default::default(),
            name: "Short interlude".into(),
            covers: vec![],
            language: vec![],
            duration_ms: 40_000,
            is_explicit: false,
            availability: Ok(()),
            alternatives: None,
            unique_fields: UniqueFields::Track {
                artists: librespot_metadata::artist::ArtistsWithRole(vec![]),
                album: "Album".into(),
                album_artists: vec![],
                popularity: 0,
                number: 1,
                disc_number: 1,
            },
        }
    }

    #[test]
    fn each_loaded_track_has_a_new_history_sequence_but_seek_and_pause_do_not() {
        let item = interlude();
        let mut state = LocalState::default();
        for sequence in [1, 2] {
            assert!(apply_event(
                &mut state,
                PlayerEvent::TrackChanged {
                    audio_item: Box::new(item.clone())
                }
            ));
            assert_eq!(state.track_sequence, sequence);
            for event in [
                PlayerEvent::Playing {
                    play_request_id: sequence,
                    track_id: uri(),
                    position_ms: 0,
                },
                PlayerEvent::Paused {
                    play_request_id: sequence,
                    track_id: uri(),
                    position_ms: 20_000,
                },
                PlayerEvent::Seeked {
                    play_request_id: sequence,
                    track_id: uri(),
                    position_ms: 0,
                },
                PlayerEvent::Playing {
                    play_request_id: sequence,
                    track_id: uri(),
                    position_ms: 0,
                },
            ] {
                apply_event(&mut state, event);
                assert_eq!(state.track_sequence, sequence);
            }
        }
    }

    /// A track on repeat plays again with the same metadata, so only a
    /// seek tells media controls that the position went back to the start
    /// (#587). The position arrives with `Playing`, after `TrackChanged`.
    #[test]
    fn a_replay_of_the_same_track_reports_its_start_as_a_seek() {
        let mut state = LocalState::default();
        apply_event(
            &mut state,
            PlayerEvent::TrackChanged {
                audio_item: Box::new(interlude()),
            },
        );
        apply_event(
            &mut state,
            PlayerEvent::Playing {
                play_request_id: 1,
                track_id: uri(),
                position_ms: 0,
            },
        );
        let first_play = state.seek_sequence;
        apply_event(
            &mut state,
            PlayerEvent::PositionChanged {
                play_request_id: 1,
                track_id: uri(),
                position_ms: 39_900,
            },
        );

        apply_event(
            &mut state,
            PlayerEvent::TrackChanged {
                audio_item: Box::new(interlude()),
            },
        );
        assert_eq!(
            state.seek_sequence, first_play,
            "the start position is not known yet"
        );
        apply_event(
            &mut state,
            PlayerEvent::Playing {
                play_request_id: 2,
                track_id: uri(),
                position_ms: 0,
            },
        );
        assert_eq!(state.seek_sequence, first_play + 1);
        assert_eq!(state.position_ms, 0);

        apply_event(
            &mut state,
            PlayerEvent::Paused {
                play_request_id: 2,
                track_id: uri(),
                position_ms: 1_000,
            },
        );
        assert_eq!(state.seek_sequence, first_play + 1, "reported once");
    }
    #[test]
    fn position_interpolates_only_while_playing() {
        let mut state = LocalState {
            playback: Playback::Paused,
            position_ms: 5_000,
            position_at: Some(Instant::now() - Duration::from_secs(2)),
            ..LocalState::default()
        };
        assert_eq!(state.position_now(), 5_000);
        state.playback = Playback::Playing;
        assert!(state.position_now() >= 7_000);
    }

    #[test]
    fn loading_keeps_a_playing_state_visible() {
        let mut state = LocalState {
            playback: Playback::Playing,
            ..LocalState::default()
        };
        apply_event(
            &mut state,
            PlayerEvent::Loading {
                play_request_id: 1,
                track_id: uri(),
                position_ms: 0,
            },
        );
        assert_eq!(state.playback, Playback::Playing);
    }

    /// Every skip and end-of-track advance loads while the previous track
    /// still shows as playing. The load itself is carried by `loading`, so
    /// the button spinner follows the engine without flipping the transport.
    #[test]
    fn a_mid_song_load_marks_the_state_loading_until_it_starts() {
        let mut state = LocalState {
            playback: Playback::Playing,
            ..LocalState::default()
        };
        assert!(apply_event(
            &mut state,
            PlayerEvent::Loading {
                play_request_id: 2,
                track_id: uri(),
                position_ms: 30_000,
            },
        ));
        assert_eq!(state.playback, Playback::Playing);
        assert!(state.loading);

        apply_event(
            &mut state,
            PlayerEvent::Playing {
                play_request_id: 2,
                track_id: uri(),
                position_ms: 0,
            },
        );
        assert!(!state.loading);

        apply_event(
            &mut state,
            PlayerEvent::Loading {
                play_request_id: 3,
                track_id: uri(),
                position_ms: 0,
            },
        );
        assert!(state.loading);
        apply_event(
            &mut state,
            PlayerEvent::Stopped {
                play_request_id: 3,
                track_id: uri(),
            },
        );
        assert!(!state.loading);

        // A load that fails has no Playing to clear the spinner, so the
        // failure events must do it themselves.
        for failure in 4..=5 {
            apply_event(
                &mut state,
                PlayerEvent::Loading {
                    play_request_id: failure,
                    track_id: uri(),
                    position_ms: 0,
                },
            );
            assert!(state.loading);
            let failed = if failure == 4 {
                PlayerEvent::Unavailable {
                    play_request_id: failure,
                    track_id: uri(),
                }
            } else {
                PlayerEvent::AudioKeyUnavailable {
                    play_request_id: failure,
                    track_id: uri(),
                }
            };
            apply_event(&mut state, failed);
            assert!(!state.loading, "failure {failure} stops the spinner");
            assert!(state.error.is_some(), "failure {failure} reports why");
        }
    }

    #[test]
    fn replacing_a_playing_track_interrupts_queued_audio() {
        let playing = LocalState {
            playback: Playback::Playing,
            ..LocalState::default()
        };
        let stopped = LocalState::default();
        let load = PlayerCommand::Load(LoadSpec::default());

        assert!(command_interrupts_audio(&playing, &PlayerCommand::Next));
        assert!(command_interrupts_audio(&playing, &PlayerCommand::Previous));
        assert!(command_interrupts_audio(&playing, &load));
        assert!(!command_interrupts_audio(&stopped, &PlayerCommand::Next));
        assert!(!command_interrupts_audio(
            &playing,
            &PlayerCommand::Seek(10)
        ));
    }

    /// Spotify making this Connect device inactive must not be mistaken for
    /// the engine session ending. A later playlist load can activate the same
    /// Spirc instance; marking it disconnected makes the UI hold that load
    /// forever while waiting for a reconnect that will never happen.
    #[test]
    fn an_inactive_connect_device_keeps_its_engine_session() {
        let mut state = LocalState {
            connected: true,
            active_client: "Spotifast".into(),
            ..LocalState::default()
        };

        assert!(apply_event(
            &mut state,
            PlayerEvent::SessionDisconnected {
                connection_id: "connection".into(),
                user_name: "listener".into(),
            },
        ));

        assert!(state.connected, "the Spotify session is still usable");
        assert!(state.active_client.is_empty());
    }

    #[test]
    fn a_rejected_audio_key_has_its_own_error() {
        let mut state = LocalState::default();

        assert!(apply_event(
            &mut state,
            PlayerEvent::AudioKeyUnavailable {
                play_request_id: 1,
                track_id: uri(),
            },
        ));
        assert_eq!(
            state.error.as_deref(),
            Some("Spotify refused the audio key. Try again later")
        );
    }

    #[test]
    fn repeat_cycles_and_maps() {
        assert_eq!(RepeatMode::Off.next(), RepeatMode::Context);
        assert_eq!(RepeatMode::Track.next(), RepeatMode::Off);
        assert_eq!(RepeatMode::from_api("track"), RepeatMode::Track);
        assert_eq!(RepeatMode::Context.api_name(), "context");
    }

    #[test]
    fn device_id_is_stable_hex() {
        let config = EngineConfig {
            buffer_ms: crate::sink::DEFAULT_BUFFER_MS,
            tap: AudioTap::new(),
            eq: crate::eq::shared(),
            device_name: "Spotifast".into(),
            bitrate_kbps: 320,
            normalisation: false,
            autoplay: true,
            gapless: true,
            backend: None,
            audio_device: None,
            initial_volume: 1,
            volume_dir: PathBuf::new(),
            audio_cache_dir: None,
            audio_cache_limit: None,
            proxy: crate::settings::ProxyConfig::Off,
        };
        let id = config.device_id();
        assert_eq!(id.len(), 40);
        assert_eq!(id, config.device_id());
    }

    /// A track that was playing or paused is remembered with its position;
    /// nothing is once playback has stopped.
    #[test]
    fn an_interrupted_track_is_remembered_with_its_position() {
        let mut state = LocalState {
            track: Some(LocalTrack {
                uri: "spotify:track:x".into(),
                duration_ms: 200_000,
                ..LocalTrack::default()
            }),
            playback: Playback::Playing,
            position_ms: 10_000,
            position_at: Some(Instant::now()),
            ..LocalState::default()
        };
        let resume = state.interrupted().expect("playing");
        assert_eq!(resume.uri, "spotify:track:x");
        assert!(resume.playing);
        assert!(resume.position_ms >= 10_000);
        state.playback = Playback::Paused;
        assert!(!state.interrupted().expect("paused").playing);
        state.playback = Playback::Stopped;
        assert!(state.interrupted().is_none());
        state.playback = Playback::Playing;
        state.track = None;
        assert!(state.interrupted().is_none());
    }

    /// The next engine starts at the level set last, exactly.
    #[test]
    fn an_engine_is_heard_at_the_level_set_last() {
        let set_here = 3276;
        assert_eq!(Heard::at(set_here).level(), set_here);
    }

    /// Telemetry names a context by its kind only: a collection URI
    /// carries the account name.
    #[test]
    fn a_context_is_reported_by_kind_without_the_account() {
        assert_eq!(uri_kind("spotify:user:listener:collection"), "collection");
        assert_eq!(
            uri_kind("spotify:playlist:37i9dQZF1DXcBWIGoYBM5M"),
            "playlist"
        );
        assert_eq!(uri_kind("spotify:user:listener"), "user");
        assert_eq!(uri_kind(""), "unknown");
    }

    /// Position reports come once a second while the decoder keeps up; a
    /// second of audio that took two to arrive is a second of stall.
    #[test]
    fn a_late_position_report_while_playing_is_a_decoder_stall() {
        let mut watch = EventWatch::new(1, Arc::default(), Arc::new(AtomicU64::new(0)));
        watch.playback = Playback::Playing;
        let start = Instant::now();
        watch.position_report(10_000, start);
        watch.position_report(11_000, start + Duration::from_millis(1_010));
        assert!(watch.stall.is_none());
        watch.position_report(12_000, start + Duration::from_millis(3_010));
        assert_eq!(
            watch.stall.as_ref().map(|stall| stall.stall_ms),
            Some(1_000)
        );

        let mut paused = EventWatch::new(1, Arc::default(), Arc::new(AtomicU64::new(0)));
        paused.playback = Playback::Paused;
        paused.position_report(10_000, start);
        paused.position_report(10_000, start + Duration::from_secs(5));
        assert!(paused.stall.is_none(), "a paused decoder is not stalled");
    }

    /// A pause and resume pressed in the app are deliberate; the same pair
    /// from a headset is what a flap looks like.
    #[test]
    fn only_presses_in_the_app_are_deliberate() {
        assert!(pressed_in_app("intent:toggle", "keyboard"));
        assert!(!pressed_in_app("intent:pause", "media_key"));
        assert!(!pressed_in_app("sink:write_error", "ui"));
    }

    /// A pause from the phone or an engine restart is meant; a volume
    /// change or a 429 next to a pause does not explain it.
    #[test]
    fn only_transport_causes_explain_a_pause() {
        let cause = |kind: &str, detail: &str| telemetry::Cause {
            kind: kind.to_owned(),
            detail: detail.to_owned(),
            at: Instant::now(),
        };
        assert!(moves_transport(&cause("connect:request", "pause")));
        assert!(moves_transport(&cause("system:wake", "")));
        assert!(!moves_transport(&cause("connect:request", "set_volume")));
        assert!(!moves_transport(&cause("intent:volume", "ui")));
        assert!(!moves_transport(&cause("webapi:rate_limited", "me/player")));
        assert!(!moves_transport(&cause("sink:ran_dry", "200 ms")));
        assert!(!moves_transport(&cause("player:unavailable", "preload")));
        assert!(deliberate("connect:request", "pause"));
        assert!(deliberate("app:restart_engine", "settings"));
        assert!(deliberate("engine:restore", "playing"));
        assert!(!deliberate("connect:request", "set_volume"));
        assert!(!deliberate("intent:pause", "media_key"));
    }
}
