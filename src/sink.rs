//! Audio output for local playback.
//!
//! librespot's rodio sink panics if no output device is available. Release
//! builds abort on that panic. This sink opens the device when playback starts
//! and reports a device it cannot open through the UI, from the first write
//! (see `start`). Spotifast can then remain available as a Connect remote
//! until an output appears.
//!
//! fastframe-audio owns the device stream: it pauses with playback, so a
//! paused player costs no audio work (#636), follows the system's default
//! output and reopens after a failure. rodio's mixer and queue fill it.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::thread;
use std::time::{Duration, Instant};

use fastframe_audio::{Buffer, BufferSize, Maintained, OutputOptions, Render};
use librespot_playback::audio_backend::{Sink, SinkError, SinkResult};
use librespot_playback::convert::Converter;
use librespot_playback::decoder::AudioPacket;
use librespot_playback::mixer::VolumeGetter;
use librespot_playback::player::PlayerEvent;
use librespot_playback::{NUM_CHANNELS, SAMPLE_RATE};
use rodio::Source;

use crate::resample::Resampler;
use crate::telemetry;

/// The backend name Settings uses for this sink.
pub const NAME: &str = "rodio";

/// Told about output failures, with a message fit for the interface.
pub type ErrorHook = Arc<dyn Fn(String) + Send + Sync>;

/// Reported when the system has no audio output at all. The interface
/// recognises it and shows it in the user's language.
pub const NO_DEVICE: &str =
    "No audio output device was found. Connect or enable one, then press play again.";

/// Opens the output: the device by name, else the default.
type Opener = fn(Option<&str>, u32, &AudioControl) -> Result<Output, OpenError>;

/// Maximum queued rodio chunks before `write` blocks, about 200 ms of audio.
const QUEUE_LIMIT: usize = 12;

/// Maximum time `stop` waits for the queue to drain.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Length of each side of an interrupted-track fade.
const INTERRUPT_FADE: Duration = Duration::from_millis(50);

/// How long Play takes to come up, and Pause and Stop to go down.
const TRANSPORT_FADE: Duration = Duration::from_millis(50);

/// Smooths slider and mute changes at the device's sample rate.
const VOLUME_RAMP: Duration = Duration::from_millis(30);

/// Default Windows device buffer length in milliseconds.
///
/// Small platform defaults can click under load (#88). A 100 ms buffer avoids
/// these underruns while keeping controls responsive.
pub const DEFAULT_BUFFER_MS: u32 = 100;

/// Allowed Windows device buffer range. Lower values can click; higher values
/// delay playback controls.
pub const BUFFER_MS_RANGE: std::ops::RangeInclusive<u32> = 20..=500;

/// Coordinates an explicit track replacement with the audio thread.
///
/// librespot deliberately leaves a gapless sink running between tracks. That
/// is right when one track reaches its end, but an explicit skip otherwise
/// leaves the old queued audio in front of the replacement. The old signal is
/// faded on rodio's output thread before its queue is discarded; writes stay
/// gated until librespot reports that the replacement track is loaded.
/// A confirmed seek also discards queued audio, without gating packets from
/// the decoder that has already moved to the requested position.
pub struct AudioControl {
    target: Mutex<AudioTarget>,
    waiting_for_track: AtomicBool,
    reset_output: AtomicBool,
    reset_processing: AtomicBool,
    buffer_ms: u32,
}

#[derive(Default)]
struct AudioTarget {
    sink: Weak<rodio::Sink>,
    envelope: Option<Arc<Envelope>>,
}

impl AudioControl {
    pub fn new(buffer_ms: u32) -> Arc<Self> {
        metrics::register();
        metrics::control_created();
        Arc::new(Self {
            target: Mutex::new(AudioTarget::default()),
            waiting_for_track: AtomicBool::new(false),
            reset_output: AtomicBool::new(false),
            reset_processing: AtomicBool::new(false),
            buffer_ms: buffer_ms.clamp(*BUFFER_MS_RANGE.start(), *BUFFER_MS_RANGE.end()),
        })
    }

    /// Follows confirmed decoder transitions, including seeks requested by
    /// another Spotify client. Natural track changes retain gapless audio.
    pub(crate) fn handle_player_event(&self, event: &PlayerEvent) {
        match event {
            PlayerEvent::TrackChanged { .. } => {
                metrics::track_changed();
                self.track_changed();
            }
            PlayerEvent::Seeked { .. } => {
                metrics::seeked();
                let target = self.target.lock().unwrap_or_else(PoisonError::into_inner);
                if let Some(sink) = target.sink.upgrade() {
                    metrics::seek_cut(sink.len());
                    sink.stop();
                }
                self.reset_output.store(true, Ordering::SeqCst);
                self.reset_processing.store(true, Ordering::SeqCst);
                // Previous can rewind the current track after interrupting
                // it. Release that gate, but never close it for a seek:
                // the decoder is already sending audio from the new position.
                self.release("seeked");
            }
            PlayerEvent::Stopped { .. } => self.release("stopped"),
            PlayerEvent::Loading { .. } => metrics::loading(),
            _ => {}
        }
    }

    /// Fades and discards the current output before a user-requested track
    /// change. Repeated skips share the same handoff.
    pub fn interrupt(&self) {
        if self.waiting_for_track.swap(true, Ordering::SeqCst) {
            metrics::interrupt_repeated();
            return;
        }
        let began = metrics::gate_closed();
        let (sink, envelope) = {
            let target = self.target.lock().unwrap_or_else(PoisonError::into_inner);
            (target.sink.upgrade(), target.envelope.clone())
        };
        // The chunks queued when the fade began, and whether it reached
        // silence before the deadline.
        let mut fade = None;
        if let (Some(sink), Some(envelope)) = (&sink, &envelope) {
            let queued = sink.len();
            envelope.fade_out();
            let wait =
                Duration::from_millis(u64::from(self.buffer_ms)).saturating_add(INTERRUPT_FADE * 2);
            let deadline = Instant::now() + wait;
            while !envelope.silent() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
            fade = Some((queued, envelope.silent()));
            // Unlike `clear`, this does not wait for every queued source.
            // The replacement gets a fresh rodio sink on its first write.
            sink.stop();
        }
        self.reset_output.store(true, Ordering::SeqCst);
        self.reset_processing.store(true, Ordering::SeqCst);
        metrics::interrupted(began, fade, self.buffer_ms);
    }

    /// Opens the write gate once librespot has left the old decoder behind.
    pub fn track_changed(&self) {
        self.release("track_changed");
    }

    /// Releases the gate if the requested replacement stopped instead.
    /// Engine::command calls it when the command could not be sent.
    pub fn stopped(&self) {
        self.release("command_failed");
    }

    /// Opens the write gate, reporting how long it was closed when it was.
    fn release(&self, by: &'static str) {
        if self.waiting_for_track.swap(false, Ordering::SeqCst) {
            metrics::gate_released(by);
        }
    }

    fn waiting_for_track(&self) -> bool {
        self.waiting_for_track.load(Ordering::SeqCst)
    }

    /// The processing wrapper owns a separate reset from the output queue.
    /// Keep it pending while old decoder packets are still being discarded.
    pub(crate) fn take_processing_reset(&self) -> bool {
        !self.waiting_for_track() && self.reset_processing.swap(false, Ordering::SeqCst)
    }

    fn take_reset(&self) -> bool {
        self.reset_output.swap(false, Ordering::SeqCst)
    }

    fn register(&self, sink: &Arc<rodio::Sink>, envelope: Arc<Envelope>) {
        let mut target = self.target.lock().unwrap_or_else(PoisonError::into_inner);
        target.sink = Arc::downgrade(sink);
        target.envelope = Some(envelope);
    }
}

/// Frames handed to rodio, and frames it has finished with.
/// The difference is what is still queued.
struct Queued {
    appended: AtomicU64,
    consumed: AtomicU64,
}

impl Queued {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            appended: AtomicU64::new(0),
            consumed: AtomicU64::new(0),
        })
    }

    /// Frames handed over and not yet played.
    fn frames(&self) -> u64 {
        self.appended
            .load(Ordering::Relaxed)
            .saturating_sub(self.consumed.load(Ordering::Relaxed))
    }
}

/// The range a level moves over, as a fixed point fraction of full gain.
const SCALE: u32 = 1 << 24;

/// A sample-clocked gain shared by every chunk in one rodio queue.
struct Envelope {
    level: AtomicU32,
    target: AtomicU32,
    /// How far `level` moves each frame. Set per fade, so a ramp can be cut
    /// to fit the sound that is left to carry it.
    step: AtomicU32,
    /// The step for this envelope's nominal length, and its slowest.
    full_step: u32,
}

impl Envelope {
    /// Resting fully open, for a signal that is already sounding.
    fn open(sample_rate: u32, length: Duration) -> Arc<Self> {
        Self::at(sample_rate, length, SCALE)
    }

    /// Resting closed, and staying there until something raises it.
    fn closed(sample_rate: u32, length: Duration) -> Arc<Self> {
        Self::at(sample_rate, length, 0)
    }

    /// Closed, and already on its way up.
    fn rising(sample_rate: u32, length: Duration) -> Arc<Self> {
        let envelope = Self::closed(sample_rate, length);
        envelope.fade_in();
        envelope
    }

    fn at(sample_rate: u32, length: Duration, level: u32) -> Arc<Self> {
        let full_step = step_over(fade_frames(sample_rate, length));
        Arc::new(Self {
            level: AtomicU32::new(level),
            target: AtomicU32::new(level),
            step: AtomicU32::new(full_step),
            full_step,
        })
    }

    fn fade_in(&self) {
        self.step.store(self.full_step, Ordering::Relaxed);
        self.target.store(SCALE, Ordering::Relaxed);
    }

    fn fade_out(&self) {
        self.step.store(self.full_step, Ordering::Relaxed);
        self.target.store(0, Ordering::Relaxed);
    }

    /// Fades out over `frames` of sound, or the nominal length if that is
    /// shorter.
    fn fade_out_over(&self, frames: u64) {
        let frames = frames.clamp(1, u64::from(u32::MAX)) as u32;
        self.step
            .store(step_over(frames).max(self.full_step), Ordering::Relaxed);
        self.target.store(0, Ordering::Relaxed);
    }

    /// Puts the envelope at silence at once, wherever its ramp had reached.
    /// Callers use this once the sound has stopped and there is no longer
    /// anything for a ramp to ride.
    fn close(&self) {
        self.step.store(self.full_step, Ordering::Relaxed);
        self.target.store(0, Ordering::Relaxed);
        self.level.store(0, Ordering::Relaxed);
    }

    fn silent(&self) -> bool {
        self.level.load(Ordering::Relaxed) == 0
    }

    /// Returns this frame's gain, then moves one frame toward the target.
    fn next_gain(&self) -> f32 {
        let level = self.level.load(Ordering::Relaxed);
        let target = self.target.load(Ordering::Relaxed);
        let step = self.step.load(Ordering::Relaxed);
        let next = match level.cmp(&target) {
            std::cmp::Ordering::Less => level.saturating_add(step).min(target),
            std::cmp::Ordering::Greater => level.saturating_sub(step).max(target),
            std::cmp::Ordering::Equal => level,
        };
        self.level.store(next, Ordering::Relaxed);
        level as f32 / SCALE as f32
    }
}

/// The per-frame movement that crosses the whole range in `frames`.
fn step_over(frames: u32) -> u32 {
    SCALE.div_ceil(frames.max(1)).max(1)
}

fn fade_frames(sample_rate: u32, length: Duration) -> u32 {
    (u64::from(sample_rate) * length.as_millis() as u64 / 1_000).max(1) as u32
}

/// Applies the shared interruption envelope on rodio's output thread, so it
/// can smooth audio that was already queued when the user changes track.
struct TransitionSource {
    inner: rodio::buffer::SamplesBuffer,
    /// Smooths a track the listener replaced part way through.
    interrupt: Arc<Envelope>,
    /// Carries Play and Pause.
    transport: Arc<Envelope>,
    /// The count this chunk's frames belong to.
    queued: Arc<Queued>,
    /// Frames of this chunk not yet handed on.
    remaining: u32,
    channel: usize,
    gain: f32,
}

impl TransitionSource {
    fn new(
        inner: rodio::buffer::SamplesBuffer,
        interrupt: Arc<Envelope>,
        transport: Arc<Envelope>,
        queued: Arc<Queued>,
        frames: u32,
    ) -> Self {
        Self {
            inner,
            interrupt,
            transport,
            queued,
            remaining: frames,
            channel: 0,
            gain: 1.0,
        }
    }
}

impl Drop for TransitionSource {
    /// rodio drops whole sources on `stop`, which every track change does, so
    /// a chunk can end without being played. Settling up here is what stops
    /// the count drifting away from the queue it is meant to describe.
    fn drop(&mut self) {
        self.queued
            .consumed
            .fetch_add(u64::from(self.remaining), Ordering::Relaxed);
        metrics::unplayed(self.remaining);
    }
}

impl Iterator for TransitionSource {
    type Item = f32;

    fn next(&mut self) -> Option<Self::Item> {
        let sample = self.inner.next()?;
        if self.channel == 0 {
            // Both step every frame. They are independent ramps that happen
            // to share a signal, so a skip during a pause rides them at once.
            self.gain = self.interrupt.next_gain() * self.transport.next_gain();
            self.remaining = self.remaining.saturating_sub(1);
            self.queued.consumed.fetch_add(1, Ordering::Relaxed);
            metrics::frame_supplied();
        }
        self.channel = (self.channel + 1) % NUM_CHANNELS as usize;
        Some(sample * self.gain)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl Source for TransitionSource {
    fn current_span_len(&self) -> Option<usize> {
        self.inner.current_span_len()
    }

    fn channels(&self) -> rodio::ChannelCount {
        self.inner.channels()
    }

    fn sample_rate(&self) -> rodio::SampleRate {
        self.inner.sample_rate()
    }

    fn total_duration(&self) -> Option<Duration> {
        self.inner.total_duration()
    }
}

pub struct RodioSink {
    /// The output device name from Settings; `None` means the default.
    device: Option<String>,
    output: Option<Output>,
    on_error: ErrorHook,
    /// Player volume, applied at output so changes affect queued audio.
    volume: Box<dyn VolumeGetter + Send>,
    applied_volume: f32,
    /// How much sound to ask the device to hold, in milliseconds. Taken
    /// when the stream opens, so a change lands with the next restart.
    buffer_ms: u32,
    control: Arc<AudioControl>,
    open: Opener,
    /// What diagnostics remember between writes.
    diag: metrics::Writer,
}

struct Output {
    device: fastframe_audio::Output<MixerRender>,
    volume: Arc<AtomicU32>,
    /// Where a mixer made for a new stream format waits for this thread.
    made: MixerSlot,
    mixer: rodio::mixer::Mixer,
    sink: Arc<rodio::Sink>,
    /// The rate the mixer runs at, and the converter to it when that is
    /// not Spotify's.
    sample_rate: u32,
    resampler: Option<Resampler>,
    envelope: Arc<Envelope>,
    /// The Play and Pause ramp, kept across track changes so a skip during a
    /// fade does not snap the level back.
    transport: Arc<Envelope>,
    /// How much sound is queued, so Pause can cut its ramp to fit.
    queued: Arc<Queued>,
    /// Whether this track has supplied audio since its last stop.
    fed: bool,
    last_write: Option<Instant>,
}

impl Output {
    fn failed(&self) -> bool {
        self.device.failed()
    }

    /// Plays from `mixer`, made for the format the device now runs at, with
    /// a fresh queue and ramps measured at its rate.
    fn attach(&mut self, (mixer, sample_rate): MadeMixer, control: &AudioControl) {
        let sink = Arc::new(rodio::Sink::connect_new(&mixer));
        let envelope = Envelope::open(sample_rate, INTERRUPT_FADE);
        control.register(&sink, Arc::clone(&envelope));
        self.resampler = converter_to(sample_rate);
        self.mixer = mixer;
        self.sink = sink;
        self.sample_rate = sample_rate;
        self.envelope = envelope;
        // The first sound has silence to come up from instead of a hard edge.
        self.transport = Envelope::closed(sample_rate, TRANSPORT_FADE);
        self.queued = Queued::new();
        self.fed = false;
        self.last_write = None;
    }

    /// Has the device ask for sound, reopening it if it failed, moved to a
    /// new default output, or was let go after a long pause. Returns whether
    /// the queue was replaced, which needs the volume set again.
    fn run(&mut self, control: &AudioControl) -> Result<bool, OpenError> {
        if self.device.is_paused() {
            metrics::device_resuming();
        }
        self.device.resume();
        self.drain_errors();
        let maintaining = Instant::now();
        match self.device.maintain() {
            Maintained::Reopened {
                device,
                sample_rate,
                channels,
                reason,
            } => {
                crate::telemetry::private_term(&device);
                log::info!("audio output reopened ({reason:?}): {device} at {sample_rate} Hz");
                metrics::reopened(
                    &device,
                    (sample_rate, channels),
                    reason,
                    maintaining.elapsed(),
                    self.sample_rate,
                );
            }
            Maintained::Failed(error) => {
                metrics::reopen_failed(&error, maintaining.elapsed());
                return Err(error.into());
            }
            Maintained::Released => metrics::released(),
            Maintained::Unchanged => {}
        }
        let made = self
            .made
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let Some(mixer) = made else {
            return Ok(false);
        };
        let from_rate = self.sample_rate;
        let dropped = self.queued.frames();
        self.attach(mixer, control);
        metrics::format_changed(
            self.device.device_name(),
            from_rate,
            self.sample_rate,
            dropped,
            self.resampler.is_some(),
        );
        Ok(true)
    }

    /// Logs the errors the device reported since the last call, and returns
    /// what kind each was.
    fn drain_errors(&mut self) -> Vec<&'static str> {
        let mut kinds = Vec::new();
        for error in self.device.take_errors() {
            let class = metrics::output_error(&error);
            if error.is_fatal() {
                log::error!("audio stream error: {error}");
            } else if class != "xrun" || metrics::xrun_warning_due() {
                log::warn!("audio stream error: {error}");
            } else {
                log::debug!("audio stream error: {error}");
            }
            kinds.push(class);
        }
        kinds
    }
}

/// The converter from Spotify's rate to `sample_rate`, when they differ.
fn converter_to(sample_rate: u32) -> Option<Resampler> {
    let resampler = Resampler::new(SAMPLE_RATE, sample_rate, NUM_CHANNELS as usize);
    if resampler.is_some() {
        log::info!(
            "the output runs at {sample_rate} Hz; the music is converted from {SAMPLE_RATE} Hz"
        );
    }
    resampler
}

/// A mixer and the rate it runs at.
type MadeMixer = (rodio::mixer::Mixer, u32);

type MixerSlot = Arc<Mutex<Option<MadeMixer>>>;

/// A linear gain ramp, shared by every channel of an output frame.
#[derive(Default)]
struct VolumeRamp {
    current: f32,
    target: f32,
    remaining: u32,
    sample_rate: u32,
}

impl VolumeRamp {
    fn next_gain(&mut self, target: f32, sample_rate: u32) -> f32 {
        if target != self.target || sample_rate != self.sample_rate {
            self.target = target;
            self.sample_rate = sample_rate;
            self.remaining = fade_frames(sample_rate, VOLUME_RAMP);
        }
        let gain = self.current;
        if self.remaining > 0 {
            self.current += (self.target - self.current) / self.remaining as f32;
            self.remaining -= 1;
            if self.remaining == 0 {
                self.current = self.target;
            }
        }
        gain
    }
}

/// Fills the device from rodio's mixer, or with silence before there is one.
///
/// fastframe-audio configures it on the sink's thread whenever it opens a
/// stream. A stream in the format the mixer already has keeps it, so a
/// reopen on another device carries on from the same sample; another rate or
/// channel count gets a new mixer, which waits in `made` for the sink.
struct MixerRender {
    volume: Arc<AtomicU32>,
    ramp: VolumeRamp,
    source: Option<rodio::mixer::MixerSource>,
    format: (u32, u16),
    made: MixerSlot,
}

impl Render for MixerRender {
    fn configure(&mut self, sample_rate: u32, channels: u16) {
        if self.source.is_some() && self.format == (sample_rate, channels) {
            return;
        }
        let (mixer, source) = rodio::mixer::mixer(
            channels as rodio::ChannelCount,
            sample_rate as rodio::SampleRate,
        );
        self.source = Some(source);
        self.format = (sample_rate, channels);
        *self.made.lock().unwrap_or_else(PoisonError::into_inner) = Some((mixer, sample_rate));
    }

    fn render(&mut self, out: &mut [f32]) {
        // Diagnostics here are relaxed atomics only: this is the audio
        // callback.
        let began = metrics::callback_began();
        // Samples with no source, and the loudest before volume, so that a
        // turned-down output never reads as a silent one.
        let mut missing = 0;
        let mut peak = 0.0f32;
        match &mut self.source {
            Some(source) => {
                let target = f32::from_bits(self.volume.load(Ordering::Relaxed));
                for frame in out.chunks_mut(usize::from(self.format.1).max(1)) {
                    let gain = self.ramp.next_gain(target, self.format.0);
                    for sample in frame {
                        let value = match source.next() {
                            Some(value) => value,
                            None => {
                                missing += 1;
                                0.0
                            }
                        };
                        peak = peak.max(value.abs());
                        *sample = value * gain;
                    }
                }
            }
            None => {
                out.fill(0.0);
                missing = out.len();
            }
        }
        metrics::callback_ended(began, out.len(), self.format, missing, peak);
    }
}

impl RodioSink {
    pub fn new(
        device: Option<String>,
        on_error: ErrorHook,
        volume: Box<dyn VolumeGetter + Send>,
        buffer_ms: u32,
        control: Arc<AudioControl>,
    ) -> Self {
        Self {
            device,
            output: None,
            on_error,
            volume,
            applied_volume: -1.0,
            buffer_ms,
            control,
            open: open_output,
            diag: metrics::Writer::default(),
        }
    }

    fn apply_volume(&mut self) {
        let factor = self.volume.attenuation_factor() as f32;
        if let Some(output) = &self.output
            && factor != self.applied_volume
        {
            output.volume.store(factor.to_bits(), Ordering::Relaxed);
            self.applied_volume = factor;
        }
    }

    /// Opens the output if it is not open, and has it ask for sound.
    fn open_if_needed(&mut self) -> Result<(), OpenError> {
        match &mut self.output {
            Some(output) => {
                if output.run(&self.control)? {
                    self.applied_volume = -1.0;
                }
            }
            None => {
                self.output = Some((self.open)(
                    self.device.as_deref(),
                    self.buffer_ms,
                    &self.control,
                )?);
                self.applied_volume = -1.0;
            }
        }
        Ok(())
    }

    /// As `open_if_needed`, reporting a failure to the interface.
    fn ensure_open(&mut self) -> SinkResult<()> {
        let reopening = self.output.is_some();
        self.open_if_needed().map_err(|error| {
            let message = error.to_string();
            log::error!("{message}");
            let reason = match (&error, reopening) {
                (OpenError::NoDevice, _) => "no_device",
                (OpenError::Device(_), true) => "reopen_failed",
                (OpenError::Device(_), false) => "open_failed",
            };
            metrics::write_error(reason, &message, &[], None, self.device.is_some());
            (self.on_error)(message.clone());
            SinkError::ConnectionRefused(message)
        })
    }
}

impl Sink for RodioSink {
    /// Never fails: an output that cannot open is reported by the first
    /// `write` instead (#623).
    ///
    /// librespot starts the sink from inside its playing loop and, when
    /// `start` fails, pauses and then carries on as if it were still
    /// playing. It finds itself paused, calls that an invalid state and
    /// exits the process. A failed `write` pauses too, but at a point
    /// where librespot expects it, so playback stops with a message and
    /// the app stays up as a Connect remote.
    fn start(&mut self) -> SinkResult<()> {
        take_precedence();
        let starting = Instant::now();
        let was_open = self.output.is_some();
        let was_paused = self.output.as_ref().map(|output| output.device.is_paused());
        self.diag.restart();
        if let Err(error) = self.open_if_needed() {
            log::debug!("audio output not open at start: {error}");
            metrics::sink_started(starting.elapsed(), false, was_paused, Some(&error));
            return Ok(());
        }
        metrics::sink_started(starting.elapsed(), !was_open, was_paused, None);
        self.apply_volume();
        if let Some(output) = &mut self.output {
            output.transport.fade_in();
            output.sink.play();
        }
        Ok(())
    }

    /// Never fails: librespot exits the process when a sink cannot stop.
    fn stop(&mut self) -> SinkResult<()> {
        metrics::stopping();
        if let Some(output) = &mut self.output {
            let queued_ms = metrics::frames_ms(output.queued.frames(), output.sample_rate);
            // The drain below plays the queue out, so the ramp is cut to
            // what is in it. During steady playback that is the whole
            // 50 ms; just after a seek or a track change it is whatever has
            // been decoded since.
            output.transport.fade_out_over(output.queued.frames());
            let draining = Instant::now();
            let deadline = Instant::now() + DRAIN_TIMEOUT;
            while !output.sink.empty() && !output.failed() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
            }
            let drain = metrics::Drain {
                queued_ms,
                took: draining.elapsed(),
                drained: output.sink.empty(),
                failed: output.failed(),
                callback_age_ms: metrics::callback_age_ms(),
            };
            output.sink.pause();
            // With the queue played out, the device can stop asking for
            // sound until Play: a paused app costs no audio work (#636).
            let pausing = Instant::now();
            output.device.pause();
            metrics::device_paused();
            let pause_took = pausing.elapsed();
            output.transport.close();
            output.fed = false;
            output.last_write = None;
            metrics::sink_stopped(drain, pause_took);
        }
        Ok(())
    }

    fn write(&mut self, packet: AudioPacket, converter: &mut Converter) -> SinkResult<()> {
        let samples = packet
            .samples()
            .map_err(|error| SinkError::OnWrite(error.to_string()))?;
        if self.control.waiting_for_track() {
            metrics::phase(metrics::GATE);
            // Muting must not remove decoder backpressure. Otherwise cached
            // audio races to EndOfTrack while Connect is still handling the
            // replacement load, and that old event can skip the chosen song.
            // Pace one discarded packet, then let librespot process commands.
            let frames = samples.len() / NUM_CHANNELS as usize;
            metrics::gated(frames);
            thread::sleep(Duration::from_secs_f64(frames as f64 / SAMPLE_RATE as f64));
            return Ok(());
        }
        let samples = converter.f64_to_f32(samples);
        // Sound arriving without a Play first still has a device to go to.
        metrics::phase(metrics::OPEN);
        let opening = Instant::now();
        self.ensure_open()?;
        metrics::inside(metrics::OPEN, opening.elapsed());
        if self.control.take_reset()
            && let Some(output) = &mut self.output
        {
            let sink = Arc::new(rodio::Sink::connect_new(&output.mixer));
            let envelope = Envelope::rising(output.sample_rate, INTERRUPT_FADE);
            self.control.register(&sink, Arc::clone(&envelope));
            output.sink = sink;
            output.envelope = envelope;
            output.queued = Queued::new();
            output.resampler =
                Resampler::new(SAMPLE_RATE, output.sample_rate, NUM_CHANNELS as usize);
            output.fed = false;
            output.last_write = None;
            self.applied_volume = -1.0;
            self.diag.restart();
            metrics::sink_reset(output.sample_rate, output.resampler.is_some());
        }
        self.apply_volume();
        let Some(output) = &mut self.output else {
            return Err(SinkError::NotConnected(
                "the audio output is not open".into(),
            ));
        };
        let samples = match &mut output.resampler {
            Some(resampler) => resampler.process(&samples),
            None => samples,
        };
        let now = Instant::now();
        if output.fed && output.sink.empty() && !output.sink.is_paused() {
            let late_ms = output
                .last_write
                .map(|last| now.duration_since(last).as_millis())
                .unwrap_or(0);
            log::warn!("audio queue ran dry; next packet arrived after {late_ms} ms");
            let late = output
                .last_write
                .map_or(Duration::ZERO, |last| now.duration_since(last));
            self.diag
                .ran_dry(late, output.sample_rate, output.resampler.is_some());
        }
        output.transport.fade_in();
        let frames = (samples.len() / NUM_CHANNELS as usize) as u32;
        // Post-limiter and pre-volume: the sound the device should play.
        let peak = telemetry::enabled().then(|| metrics::peak(&samples));
        let source = rodio::buffer::SamplesBuffer::new(
            NUM_CHANNELS as rodio::ChannelCount,
            output.sample_rate as rodio::SampleRate,
            samples,
        );
        let queued_before = output.queued.frames();
        output
            .queued
            .appended
            .fetch_add(u64::from(frames), Ordering::Relaxed);
        metrics::phase(metrics::QUEUE);
        let appending = Instant::now();
        output.sink.append(TransitionSource::new(
            source,
            Arc::clone(&output.envelope),
            Arc::clone(&output.transport),
            Arc::clone(&output.queued),
            frames,
        ));
        metrics::inside(metrics::QUEUE, appending.elapsed());
        output.fed = true;
        output.last_write = Some(now);
        self.diag.appended(output, queued_before, peak);
        // Let rodio drain a little; without this the whole track would be
        // decoded into memory at once.
        metrics::phase(metrics::BACKPRESSURE);
        let waiting = Instant::now();
        let mut stall_reported = false;
        while output.sink.len() > QUEUE_LIMIT {
            if output.failed() {
                let message = "The audio output stopped working".to_string();
                let output_errors = output.drain_errors();
                metrics::write_error(
                    "output_failed",
                    &message,
                    &output_errors,
                    Some(metrics::frames_ms(
                        output.queued.frames(),
                        output.sample_rate,
                    )),
                    self.device.is_some(),
                );
                (self.on_error)(message.clone());
                return Err(SinkError::OnWrite(message));
            }
            if !stall_reported && waiting.elapsed() >= metrics::OUTPUT_STALL {
                stall_reported = true;
                metrics::output_stalled(waiting.elapsed(), output);
            }
            thread::sleep(Duration::from_millis(10));
        }
        metrics::write_done(output);
        Ok(())
    }
}

/// Raises the Windows decoder thread one step above normal to prevent queued
/// audio from running out under load (#88).
///
/// Linux requires rtkit; CoreAudio owns its real-time callback on macOS.
#[cfg(windows)]
fn take_precedence() {
    use windows_sys::Win32::System::Threading::{
        GetCurrentThread, SetThreadPriority, THREAD_PRIORITY_ABOVE_NORMAL,
    };
    // SAFETY: the current thread's pseudo-handle needs no closing, and the
    // call takes nothing else.
    unsafe {
        SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_ABOVE_NORMAL);
    }
}

#[cfg(not(windows))]
fn take_precedence() {}

#[derive(Debug, thiserror::Error)]
enum OpenError {
    #[error("{NO_DEVICE}")]
    NoDevice,
    #[error("{0}")]
    Device(fastframe_audio::OpenError),
}

impl From<fastframe_audio::OpenError> for OpenError {
    fn from(error: fastframe_audio::OpenError) -> Self {
        match error {
            fastframe_audio::OpenError::NoDevice => Self::NoDevice,
            other => Self::Device(other),
        }
    }
}

/// What the output asks of the device: Spotify's stereo 44.1 kHz first, so
/// nothing is converted, then whatever the device takes. A named device
/// that has gone falls back to the default. The fixed buffer addresses
/// Windows shared-mode underruns (#88); CoreAudio, ALSA, PulseAudio and
/// PipeWire keep their proven driver-selected periods.
fn output_options(preferred: Option<&str>, buffer_ms: u32) -> OutputOptions {
    let device = match preferred.map(str::trim).filter(|name| !name.is_empty()) {
        Some(name) => fastframe_audio::Device::Named(name.to_string()),
        None => fastframe_audio::Device::Default,
    };
    let buffer_ms = buffer_ms.clamp(*BUFFER_MS_RANGE.start(), *BUFFER_MS_RANGE.end());
    OutputOptions {
        device,
        channels: NUM_CHANNELS as u16,
        sample_rate: Some(SAMPLE_RATE),
        buffer: Buffer::FixedOnWindows(BufferSize::Duration(Duration::from_millis(u64::from(
            buffer_ms,
        )))),
        follow_default: true,
        ..OutputOptions::default()
    }
}

fn open_output(
    preferred: Option<&str>,
    buffer_ms: u32,
    control: &AudioControl,
) -> Result<Output, OpenError> {
    // The callback's diagnostics read this clock, so it starts first.
    metrics::start_clock();
    // Device names are often a person's name; keep them out of telemetry.
    if let Some(name) = preferred {
        crate::telemetry::private_term(name);
    }
    let made = MixerSlot::default();
    let volume = Arc::new(AtomicU32::new(0.0f32.to_bits()));
    let render = MixerRender {
        volume: Arc::clone(&volume),
        ramp: VolumeRamp::default(),
        source: None,
        format: (0, 0),
        made: Arc::clone(&made),
    };
    let opening = Instant::now();
    let device = fastframe_audio::Output::open(output_options(preferred, buffer_ms), render)
        .inspect_err(|error| metrics::open_failed(error, opening.elapsed(), preferred))?;
    crate::telemetry::private_term(device.device_name());
    log::info!("audio output: {}", device.device_name());
    metrics::opened(
        device.device_name(),
        (device.sample_rate(), device.channels()),
        opening.elapsed(),
        preferred,
        buffer_ms,
    );
    // The open configured the renderer, which made the mixer.
    let (mixer, sample_rate) = made
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
        .ok_or(OpenError::NoDevice)?;
    let mut output = Output {
        device,
        volume,
        made,
        sink: Arc::new(rodio::Sink::connect_new(&mixer)),
        mixer: mixer.clone(),
        sample_rate,
        resampler: None,
        envelope: Envelope::open(sample_rate, INTERRUPT_FADE),
        transport: Envelope::closed(sample_rate, TRANSPORT_FADE),
        queued: Queued::new(),
        fed: false,
        last_write: None,
    };
    output.attach((mixer, sample_rate), control);
    Ok(output)
}

/// Measurements of the audio output for diagnostics (see the telemetry
/// module).
///
/// The audio callback touches nothing here but relaxed atomics: counters,
/// gauges and timestamps. Events are built on the player's, the backend's
/// or the runtime's threads, and only while telemetry is on. Device names
/// can carry a person's name, so only a guess at the route and a digest of
/// the name leave the machine.
pub mod metrics {
    use std::sync::OnceLock;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64};
    use std::time::{Duration, Instant};

    use librespot_playback::SAMPLE_RATE;

    use super::{Envelope, OpenError, Output, QUEUE_LIMIT, SCALE};
    use crate::telemetry::{self, Counter, Gauge, Throttle};

    // Reported by the heartbeat: counters as deltas, gauges as they stand
    // or at their peak since the previous heartbeat.
    static CB_CALLS: Counter = Counter::new("audio_cb_calls");
    static CB_FRAMES: Counter = Counter::new("audio_cb_frames");
    static CB_OVER_BUDGET: Counter = Counter::new("audio_cb_over_budget");
    static CB_STALLS: Counter = Counter::new("audio_cb_stalls");
    static UNDERRUN_FRAMES: Counter = Counter::new("audio_underrun_frames");
    static UNDERRUN_CALLBACKS: Counter = Counter::new("audio_underrun_callbacks");
    static NO_SOURCE_FRAMES: Counter = Counter::new("audio_no_source_frames");
    static SILENT_CALLBACKS: Counter = Counter::new("audio_silent_callbacks");
    static UNPLAYED_FRAMES: Counter = Counter::new("audio_unplayed_frames");
    static WRITES: Counter = Counter::new("audio_writes");
    static GATE_PACKETS: Counter = Counter::new("audio_gate_discarded_packets");
    static GATE_FRAMES: Counter = Counter::new("audio_gate_discarded_frames");
    static RAN_DRY: Counter = Counter::new("audio_ran_dry");
    static WRITER_STALLS: Counter = Counter::new("audio_writer_stalls");
    static OUTPUT_ERRORS: Counter = Counter::new("audio_output_errors");
    static XRUNS: Counter = Counter::new("audio_xruns");
    static REOPENS: Counter = Counter::new("audio_output_reopens");
    static NONFINITE: Counter = Counter::new("audio_nonfinite_samples");
    static LIMITER_POISONED: Counter = Counter::new("audio_limiter_poisoned");
    static EQ_LOCK_WAITS: Counter = Counter::new("audio_eq_lock_waits");
    static TAP_LOCK_WAITS: Counter = Counter::new("audio_tap_lock_waits");

    static CB_MAX_US: Gauge = Gauge::peak("audio_cb_max_us");
    static CB_MAX_GAP_US: Gauge = Gauge::peak("audio_cb_max_gap_us");
    static OUT_PEAK: Gauge = Gauge::peak("audio_out_peak_milli");
    static SIGNAL_PEAK: Gauge = Gauge::peak("audio_signal_peak_milli");
    static WRITE_MAX_GAP_MS: Gauge = Gauge::peak("audio_write_max_gap_ms");
    static PROCESS_MAX_US: Gauge = Gauge::peak("audio_process_max_us");
    static PACKET_MAX_FRAMES: Gauge = Gauge::peak("audio_packet_max_frames");
    static QUEUE_MAX_MS: Gauge = Gauge::peak("audio_queue_max_ms");
    static QUEUE_MIN_MS: Gauge = Gauge::last("audio_queue_min_ms");
    static LIMITER_REDUCTION: Gauge = Gauge::peak("audio_limiter_max_reduction_milli");

    /// Adds the counters and gauges above to the heartbeat.
    pub(super) fn register() {
        telemetry::register_counters(&[
            &CB_CALLS,
            &CB_FRAMES,
            &CB_OVER_BUDGET,
            &CB_STALLS,
            &UNDERRUN_FRAMES,
            &UNDERRUN_CALLBACKS,
            &NO_SOURCE_FRAMES,
            &SILENT_CALLBACKS,
            &UNPLAYED_FRAMES,
            &WRITES,
            &GATE_PACKETS,
            &GATE_FRAMES,
            &RAN_DRY,
            &WRITER_STALLS,
            &OUTPUT_ERRORS,
            &XRUNS,
            &REOPENS,
            &NONFINITE,
            &LIMITER_POISONED,
            &EQ_LOCK_WAITS,
            &TAP_LOCK_WAITS,
        ]);
        telemetry::register_gauges(&[
            &CB_MAX_US,
            &CB_MAX_GAP_US,
            &OUT_PEAK,
            &SIGNAL_PEAK,
            &WRITE_MAX_GAP_MS,
            &PROCESS_MAX_US,
            &PACKET_MAX_FRAMES,
            &QUEUE_MAX_MS,
            &QUEUE_MIN_MS,
            &LIMITER_REDUCTION,
        ]);
        telemetry::register_sampler(sample);
    }

    /// The device is expected to play queued sound: the sink was fed since
    /// it last started, reset or stopped, and is not paused.
    static EXPECT_AUDIO: AtomicBool = AtomicBool::new(false);
    /// The interrupt gate is closed, so silence is intended, and since when.
    static GATE_CLOSED: AtomicBool = AtomicBool::new(false);
    static GATE_CLOSED_AT: AtomicU64 = AtomicU64::new(0);
    static GATE_RELEASED_AT: AtomicU64 = AtomicU64::new(0);
    /// Frames the closed gate has thrown away, at Spotify's rate.
    static GATE_DISCARDED: AtomicU64 = AtomicU64::new(0);
    static GATE_STUCK_REPORTED: AtomicU64 = AtomicU64::new(0);
    /// Frames the queued chunks handed to the device.
    static FRAMES_SUPPLIED: AtomicU64 = AtomicU64::new(0);
    /// The last callback, and the current runs of starved frames and of
    /// silent ones while sound was expected.
    static CALLBACK_AT: AtomicU64 = AtomicU64::new(0);
    static DRY_RUN: AtomicU64 = AtomicU64::new(0);
    static SILENT_RUN: AtomicU64 = AtomicU64::new(0);
    static DEVICE_PAUSED_AT: AtomicU64 = AtomicU64::new(0);
    /// The rate the mixer runs at, to turn frames into time.
    static OUTPUT_RATE: AtomicU32 = AtomicU32::new(0);
    /// A traced change's first packet, waiting for the device to take it.
    static FIRST_RENDER_PENDING: AtomicBool = AtomicBool::new(false);
    static FIRST_RENDER_AT: AtomicU64 = AtomicU64::new(0);

    // The writer's heartbeat: where the player's thread is, and when it
    // last entered and left a write.
    static FEEDING: AtomicBool = AtomicBool::new(false);
    static WRITER_PHASE: AtomicU8 = AtomicU8::new(OUTSIDE);
    static WRITE_ENTERED_AT: AtomicU64 = AtomicU64::new(0);
    static WRITE_LEFT_AT: AtomicU64 = AtomicU64::new(0);
    static WRITE_GAP: AtomicU64 = AtomicU64::new(0);
    static LEFT_QUEUED_MS: AtomicU64 = AtomicU64::new(0);
    static LEFT_UNDERRUN: AtomicU64 = AtomicU64::new(0);
    static STUCK_REPORTED: AtomicU64 = AtomicU64::new(0);
    static LOAD_STARTED_AT: AtomicU64 = AtomicU64::new(0);
    static TRACK_CHANGED_AT: AtomicU64 = AtomicU64::new(0);
    static SEEK_AT: AtomicU64 = AtomicU64::new(0);

    /// Where the player's thread is. Outside a write it is in librespot:
    /// decoding, waiting on the file, loading or handling a command.
    pub(crate) const OUTSIDE: u8 = 0;
    pub(crate) const PROCESS: u8 = 1;
    pub(crate) const TAP: u8 = 2;
    pub(crate) const OPEN: u8 = 3;
    pub(crate) const QUEUE: u8 = 4;
    pub(crate) const BACKPRESSURE: u8 = 5;
    pub(crate) const GATE: u8 = 6;
    pub(crate) const START: u8 = 7;
    pub(crate) const STOP: u8 = 8;

    /// Below this (-80 dBFS) sound counts as silence; above this (-40 dBFS)
    /// a packet holds sound a listener would hear.
    const SILENT: f32 = 1e-4;
    const LOUD: f32 = 0.01;
    /// Silence a listener notices, in milliseconds.
    const NOTICEABLE_MS: f64 = 150.0;
    /// The shortest gap between callbacks that counts as a stall.
    const CALLBACK_STALL_NS: u64 = 50_000_000;
    const WRITER_STALL: Duration = Duration::from_millis(100);
    const INSIDE_STALL: Duration = Duration::from_millis(20);
    /// How long a full queue may wait before the device counts as stalled.
    pub(super) const OUTPUT_STALL: Duration = Duration::from_secs(1);
    const GATE_STUCK: Duration = Duration::from_secs(5);
    const WRITER_STUCK: Duration = Duration::from_secs(3);
    /// How long after a seek or a load librespot may still wait on the file.
    const SETTLE: Duration = Duration::from_secs(3);
    const STATS_EVERY: Duration = Duration::from_secs(2);
    const GAUGE_WINDOW: Duration = Duration::from_secs(30);

    static DRY_EVENT: Throttle = Throttle::new();
    static DRY_CRUMB: Throttle = Throttle::new();
    static DRY_CAUSE: Throttle = Throttle::new();
    static STARVED: Throttle = Throttle::new();
    static SILENT_OUTPUT: Throttle = Throttle::new();
    static STALL_EVENT: Throttle = Throttle::new();
    static STALL_CRUMB: Throttle = Throttle::new();
    static XRUN_CRUMB: Throttle = Throttle::new();
    static XRUN_WARNING: Throttle = Throttle::new();
    static ERROR_EVENT: Throttle = Throttle::new();
    static WRITE_ERROR: Throttle = Throttle::new();
    static DRAIN_TIMEOUT: Throttle = Throttle::new();
    static OUTPUT_STALLED: Throttle = Throttle::new();
    static INTERRUPT_DEADLINE: Throttle = Throttle::new();
    static NONFINITE_ANOMALY: Throttle = Throttle::new();

    static EPOCH: OnceLock<Instant> = OnceLock::new();

    /// Starts the clock the timestamps here use. The audio callback only
    /// reads it, so the output starts it before opening a stream.
    pub(super) fn start_clock() {
        let _ = EPOCH.get_or_init(Instant::now);
    }

    /// Nanoseconds on that clock; never 0, which stands for "never".
    fn now_ns() -> u64 {
        let epoch = *EPOCH.get_or_init(Instant::now);
        (epoch.elapsed().as_nanos() as u64).max(1)
    }

    /// `at` on the same clock without starting it, for the callback.
    fn at_ns(at: Instant) -> u64 {
        EPOCH.get().map_or(0, |epoch| {
            (at.saturating_duration_since(*epoch).as_nanos() as u64).max(1)
        })
    }

    fn since_ns(at: u64) -> Duration {
        Duration::from_nanos(now_ns().saturating_sub(at))
    }

    /// Milliseconds since `at`, or nothing when it never happened.
    fn age_ms(at: u64) -> Option<f64> {
        (at != 0).then(|| telemetry::duration_ms(since_ns(at)))
    }

    /// Whether `at` happened within `window`.
    fn recent(at: u64, window: Duration) -> bool {
        at != 0 && since_ns(at) < window
    }

    /// Milliseconds since the device last asked for sound.
    pub(super) fn callback_age_ms() -> Option<f64> {
        age_ms(CALLBACK_AT.load(Relaxed))
    }

    /// How long `frames` last at `rate`, in milliseconds.
    pub(super) fn frames_ms(frames: u64, rate: u32) -> f64 {
        if rate == 0 {
            0.0
        } else {
            frames as f64 * 1_000.0 / f64::from(rate)
        }
    }

    fn tenth(value: f64) -> f64 {
        (value * 10.0).round() / 10.0
    }

    // The audio callback.

    /// Taken by the callback before it renders.
    #[inline]
    pub(super) fn callback_began() -> (Instant, u64) {
        (Instant::now(), FRAMES_SUPPLIED.load(Relaxed))
    }

    /// Counts a callback that rendered `samples` at `format`, `missing` of
    /// them with no source, `peak` being the loudest before volume. Relaxed
    /// atomics only: this runs on the audio thread.
    pub(super) fn callback_ended(
        (began, supplied_before): (Instant, u64),
        samples: usize,
        (rate, channels): (u32, u16),
        missing: usize,
        peak: f32,
    ) {
        let channels = usize::from(channels).max(1);
        let frames = (samples / channels) as u64;
        let supplied = FRAMES_SUPPLIED.load(Relaxed).wrapping_sub(supplied_before);
        let ended = Instant::now();
        let took = ended.saturating_duration_since(began);
        CB_CALLS.incr();
        CB_FRAMES.add(frames);
        CB_MAX_US.raise(took.as_micros() as i64);
        let period = if rate == 0 {
            0
        } else {
            frames * 1_000_000_000 / u64::from(rate)
        };
        if rate != 0 && took.as_nanos() as u64 > period {
            CB_OVER_BUDGET.incr();
        }
        OUT_PEAK.raise((peak * 1_000.0) as i64);
        let at = at_ns(ended);
        let last = CALLBACK_AT.swap(at, Relaxed);
        if !EXPECT_AUDIO.load(Relaxed) || GATE_CLOSED.load(Relaxed) {
            DRY_RUN.store(0, Relaxed);
            SILENT_RUN.store(0, Relaxed);
            return;
        }
        if last != 0 && at > last {
            let gap = at - last;
            CB_MAX_GAP_US.raise((gap / 1_000) as i64);
            if gap > CALLBACK_STALL_NS.max(period * 3) {
                CB_STALLS.incr();
            }
        }
        if missing > 0 {
            NO_SOURCE_FRAMES.add((missing / channels) as u64);
        }
        // rodio pads an empty queue with silence, so starvation shows only
        // as frames the queue's chunks did not supply.
        let short = frames.saturating_sub(supplied);
        if short > 0 {
            UNDERRUN_FRAMES.add(short);
            UNDERRUN_CALLBACKS.incr();
            DRY_RUN.fetch_add(short, Relaxed);
        } else {
            DRY_RUN.store(0, Relaxed);
        }
        if supplied > 0 {
            if peak < SILENT {
                SILENT_CALLBACKS.incr();
                SILENT_RUN.fetch_add(supplied, Relaxed);
            } else {
                SILENT_RUN.store(0, Relaxed);
            }
            if FIRST_RENDER_PENDING.load(Relaxed) && FIRST_RENDER_PENDING.swap(false, Relaxed) {
                FIRST_RENDER_AT.store(at, Relaxed);
            }
        }
    }

    /// One frame of a queued chunk went to the device.
    #[inline]
    pub(super) fn frame_supplied() {
        FRAMES_SUPPLIED.fetch_add(1, Relaxed);
    }

    /// A chunk was dropped with `frames` unplayed: a skip, a seek or a new
    /// stream format threw it away.
    #[inline]
    pub(super) fn unplayed(frames: u32) {
        if frames > 0 {
            UNPLAYED_FRAMES.add(u64::from(frames));
        }
    }

    // The device.

    /// The device stopped asking for sound.
    pub(super) fn device_paused() {
        CALLBACK_AT.store(0, Relaxed);
        DEVICE_PAUSED_AT.store(now_ns(), Relaxed);
    }

    /// The device is about to ask for sound again. Its next callback starts
    /// afresh rather than closing a gap as long as the pause.
    pub(super) fn device_resuming() {
        CALLBACK_AT.store(0, Relaxed);
        let paused_at = DEVICE_PAUSED_AT.swap(0, Relaxed);
        telemetry::crumb("audio.device.resumed")
            .field("paused_ms", age_ms(paused_at))
            .emit();
    }

    /// The output opened `device` at `format` in `took`.
    pub(super) fn opened(
        device: &str,
        (sample_rate, channels): (u32, u16),
        took: Duration,
        preferred: Option<&str>,
        buffer_ms: u32,
    ) {
        note_route(device, sample_rate);
        if !telemetry::enabled() {
            return;
        }
        let named = preferred.map(str::trim).filter(|name| !name.is_empty());
        telemetry::event("audio.output.open")
            .ms("open_ms", took)
            .field("route", route(device))
            .field("device_digest", telemetry::digest(device))
            .field("named_device", named.is_some())
            .field("named_missing", named.is_some_and(|name| name != device))
            .field("sample_rate", sample_rate)
            .field("channels", channels)
            .field("requested_rate", SAMPLE_RATE)
            .field("resampling", sample_rate != SAMPLE_RATE)
            .field("buffer", if cfg!(windows) { "fixed" } else { "driver" })
            .field("buffer_ms", cfg!(windows).then_some(buffer_ms))
            .field("thread_qos", thread_qos())
            .emit();
    }

    /// Opening the output failed after `took`.
    pub(super) fn open_failed(
        error: &fastframe_audio::OpenError,
        took: Duration,
        preferred: Option<&str>,
    ) {
        if !telemetry::enabled() {
            return;
        }
        telemetry::crumb("audio.output.open_failed")
            .ms("open_ms", took)
            .field(
                "no_device",
                matches!(error, fastframe_audio::OpenError::NoDevice),
            )
            .text("error", &error.to_string())
            .field(
                "named_device",
                preferred.is_some_and(|name| !name.trim().is_empty()),
            )
            .emit();
    }

    /// The output opened its stream again, on `device` at `format`, in
    /// `took`; the mixer ran at `previous_rate` before.
    pub(super) fn reopened(
        device: &str,
        (sample_rate, channels): (u32, u16),
        reason: fastframe_audio::Reason,
        took: Duration,
        previous_rate: u32,
    ) {
        REOPENS.incr();
        note_route(device, sample_rate);
        if !telemetry::enabled() {
            return;
        }
        let reason = match reason {
            fastframe_audio::Reason::Failed => "failed",
            fastframe_audio::Reason::DefaultChanged => "default_changed",
            fastframe_audio::Reason::Resumed => "resumed",
        };
        let class = route(device);
        telemetry::note_cause("sink:reopen", reason);
        if reason == "default_changed" {
            telemetry::note_cause("sink:route_change", class);
        }
        telemetry::event("audio.output.reopened")
            .field("reason", reason)
            .field("route", class)
            .field("device_digest", telemetry::digest(device))
            .field("sample_rate", sample_rate)
            .field("channels", channels)
            .field("rate_changed", sample_rate != previous_rate)
            .ms("open_ms", took)
            .emit();
    }

    /// The output could not open its stream again; the write fails next.
    pub(super) fn reopen_failed(error: &fastframe_audio::OpenError, took: Duration) {
        if !telemetry::enabled() {
            return;
        }
        telemetry::event("audio.output.reopen_failed")
            .ms("open_ms", took)
            .field(
                "no_device",
                matches!(error, fastframe_audio::OpenError::NoDevice),
            )
            .text("error", &error.to_string())
            .emit();
    }

    /// The output let its device go.
    pub(super) fn released() {
        telemetry::crumb("audio.output.released").emit();
    }

    /// The stream now runs at another rate: a new mixer took over, and the
    /// queue went with the old one.
    pub(super) fn format_changed(
        device: &str,
        from_rate: u32,
        to_rate: u32,
        dropped: u64,
        resampling: bool,
    ) {
        note_route(device, to_rate);
        if !telemetry::enabled() {
            return;
        }
        telemetry::event("audio.output.format_changed")
            .field("from_rate", from_rate)
            .field("to_rate", to_rate)
            .field("dropped_ms", tenth(frames_ms(dropped, from_rate)))
            .field("resampling", resampling)
            .emit();
    }

    /// Counts and reports an error the device reported, and returns its
    /// class.
    pub(super) fn output_error(error: &fastframe_audio::OutputError) -> &'static str {
        OUTPUT_ERRORS.incr();
        let message = error.to_string();
        let class = error_class(&message);
        if class == "xrun" {
            XRUNS.incr();
            if XRUN_CRUMB.ready(Duration::from_secs(1)) {
                telemetry::crumb("audio.output.xrun").emit();
            }
            return class;
        }
        if matches!(
            class,
            "route_changed" | "device_gone" | "no_default" | "rate_changed"
        ) {
            telemetry::note_cause("sink:route_change", class);
        }
        if ERROR_EVENT.ready(Duration::from_millis(200)) {
            telemetry::event("audio.output.error")
                .field("class", class)
                .field("fatal", error.is_fatal())
                .text("message", &message)
                .emit();
        }
        class
    }

    /// Whether an xrun is logged as a warning. Warnings ship while telemetry
    /// is on, so then at most one a second; the counter keeps the rest.
    pub(super) fn xrun_warning_due() -> bool {
        !telemetry::enabled() || XRUN_WARNING.ready(Duration::from_secs(1))
    }

    /// What an error from the device is about, from cpal's fixed messages.
    fn error_class(message: &str) -> &'static str {
        let message = message.to_lowercase();
        if mentions(&message, &["underrun", "overrun"]) {
            "xrun"
        } else if mentions(&message, &["no default output"]) {
            "no_default"
        } else if mentions(&message, &["route changed", "device changed"]) {
            "route_changed"
        } else if mentions(&message, &["sample rate"]) {
            "rate_changed"
        } else if mentions(&message, &["disconnected", "not available"]) {
            "device_gone"
        } else if mentions(&message, &["real-time", "realtime"]) {
            "realtime_denied"
        } else if mentions(&message, &["permission"]) {
            "permission"
        } else if mentions(&message, &["busy"]) {
            "busy"
        } else if mentions(&message, &["no longer valid", "invalidated", "rebuilt"]) {
            "invalidated"
        } else {
            "other"
        }
    }

    /// A guess at where the sound goes, from the device's name.
    pub(crate) fn route(device: &str) -> &'static str {
        let name = device.to_lowercase();
        if mentions(
            &name,
            &[
                "blackhole",
                "loopback",
                "soundflower",
                "virtual",
                "vb-audio",
                "voicemeeter",
                "aggregate",
                "multi-output",
                "zoomaudio",
                "teams audio",
                "krisp",
            ],
        ) {
            "virtual"
        } else if mentions(&name, &["airplay", "apple tv", "homepod"]) {
            "airplay"
        } else if mentions(
            &name,
            &[
                "airpods",
                "beats",
                "bluetooth",
                "hands-free",
                "handsfree",
                "bose",
                "jabra",
                "buds",
                "wh-1000",
                "wf-1000",
                "jbl",
                "marshall",
            ],
        ) {
            "bluetooth"
        } else if mentions(
            &name,
            &[
                "hdmi",
                "displayport",
                "display audio",
                "nvidia",
                "amd high definition",
            ],
        ) {
            "hdmi"
        } else if mentions(
            &name,
            &[
                "usb",
                "dac",
                "scarlett",
                "focusrite",
                "motu",
                "audient",
                "behringer",
                "fiio",
                "schiit",
                "apogee",
                "steinberg",
                "universal audio",
                "studio display",
            ],
        ) {
            "usb"
        } else if mentions(
            &name,
            &[
                "macbook",
                "imac",
                "mac mini",
                "mac studio",
                "mac pro",
                "built-in",
                "internal",
                "realtek",
                "conexant",
                "headphones",
                "speakers",
            ],
        ) {
            "builtin"
        } else {
            "unknown"
        }
    }

    fn mentions(text: &str, words: &[&str]) -> bool {
        words.iter().any(|word| text.contains(*word))
    }

    /// Keeps the output's rate, and puts its route and rate on every later
    /// event.
    fn note_route(device: &str, sample_rate: u32) {
        OUTPUT_RATE.store(sample_rate, Relaxed);
        if telemetry::enabled() {
            telemetry::set_context("output_route", route(device));
            telemetry::set_context("output_rate", sample_rate);
        }
    }

    /// The calling thread's QoS class. On Apple silicon a thread below
    /// user-initiated can be kept to the efficiency cores, which is enough
    /// to starve the output under load.
    #[cfg(target_os = "macos")]
    fn thread_qos() -> Option<&'static str> {
        let mut class: u32 = 0;
        let mut priority: libc::c_int = 0;
        // SAFETY: the call writes a `qos_class_t`, a C enum the size of a
        // u32, into `class`, which is only ever read as a u32, and an int
        // into `priority`.
        let status = unsafe {
            libc::pthread_get_qos_class_np(
                libc::pthread_self(),
                (&raw mut class).cast::<libc::qos_class_t>(),
                &raw mut priority,
            )
        };
        (status == 0).then_some(match class {
            0x21 => "user_interactive",
            0x19 => "user_initiated",
            0x15 => "default",
            0x11 => "utility",
            0x09 => "background",
            0x05 => "maintenance",
            0x00 => "unspecified",
            _ => "other",
        })
    }

    #[cfg(not(target_os = "macos"))]
    fn thread_qos() -> Option<&'static str> {
        None
    }

    // The sink.

    /// `start` brought the sink up in `took`, opening the device when
    /// `opened`, or could not open it.
    pub(super) fn sink_started(
        took: Duration,
        opened: bool,
        was_paused: Option<bool>,
        error: Option<&OpenError>,
    ) {
        if !telemetry::enabled() {
            return;
        }
        let mut record = telemetry::crumb("audio.sink.start")
            .ms("start_ms", took)
            .field("opened", opened)
            .field("device_was_paused", was_paused)
            .field("thread_qos", thread_qos());
        if let Some(error) = error {
            record = record.text("open_error", &error.to_string());
        }
        record.emit();
    }

    /// Silence from here is intended: playback paused or stopped.
    pub(super) fn stopping() {
        EXPECT_AUDIO.store(false, Relaxed);
    }

    /// How `stop` played the queue out.
    pub(super) struct Drain {
        pub(super) queued_ms: f64,
        pub(super) took: Duration,
        pub(super) drained: bool,
        pub(super) failed: bool,
        pub(super) callback_age_ms: Option<f64>,
    }

    /// `stop` drained the queue, then paused the device in `pause_took`.
    /// librespot reports Paused only after this returns.
    pub(super) fn sink_stopped(drain: Drain, pause_took: Duration) {
        if !telemetry::enabled() {
            return;
        }
        // A drain that neither emptied nor failed waited out its deadline:
        // the device had stopped taking sound.
        let timed_out = !drain.drained && !drain.failed;
        let record = if timed_out && DRAIN_TIMEOUT.ready(Duration::from_secs(60)) {
            telemetry::anomaly("audio.sink.drain_timeout")
        } else {
            telemetry::crumb("audio.sink.stop")
        };
        record
            .field("queued_ms", tenth(drain.queued_ms))
            .ms("drain_ms", drain.took)
            .ms("pause_ms", pause_took)
            .field("timed_out", timed_out)
            .field("output_failed", drain.failed)
            .field("callback_age_ms", drain.callback_age_ms)
            .emit();
    }

    /// A fresh queue after a skip or a seek.
    pub(super) fn sink_reset(sample_rate: u32, resampling: bool) {
        if !telemetry::enabled() {
            return;
        }
        // How long the new track's first packet took after the gate opened.
        let since_release = age_ms(GATE_RELEASED_AT.load(Relaxed)).filter(|ms| *ms < 10_000.0);
        telemetry::crumb("audio.sink.reset")
            .field("since_release_ms", since_release)
            .field("sample_rate", sample_rate)
            .field("resampling", resampling)
            .emit();
    }

    /// A write failed, which makes librespot pause: the output failed, or
    /// could not be opened.
    pub(super) fn write_error(
        reason: &'static str,
        error: &str,
        output_errors: &[&'static str],
        queued_ms: Option<f64>,
        named_device: bool,
    ) {
        telemetry::note_cause("sink:write_error", reason);
        if !telemetry::enabled() {
            return;
        }
        let record = if WRITE_ERROR.ready(Duration::from_secs(10)) {
            telemetry::anomaly("audio.sink.write_error")
        } else {
            telemetry::event("audio.sink.write_error")
        };
        record
            .field("reason", reason)
            .text("error", error)
            .field("output_errors", output_errors.to_vec())
            .field("queued_ms", queued_ms.map(tenth))
            .field("named_device", named_device)
            .field("callback_age_ms", callback_age_ms())
            .emit();
    }

    /// The queue stayed full for `waited`: the device stopped taking sound
    /// without reporting a failure, and librespot is held in this write.
    pub(super) fn output_stalled(waited: Duration, output: &Output) {
        if !OUTPUT_STALLED.ready(Duration::from_secs(30)) {
            return;
        }
        telemetry::anomaly("audio.output_stalled")
            .ms("waited_ms", waited)
            .field("queued_chunks", output.sink.len())
            .field("callback_age_ms", callback_age_ms())
            .field("device_paused", output.device.is_paused())
            .field("sink_paused", output.sink.is_paused())
            .field("sample_rate", output.sample_rate)
            .emit();
    }

    /// The write queued its packet and found room: what is queued now is
    /// the cushion until librespot writes again.
    pub(super) fn write_done(output: &Output) {
        LEFT_QUEUED_MS.store(
            frames_ms(output.queued.frames(), output.sample_rate) as u64,
            Relaxed,
        );
    }

    /// The loudest sample of a packet the sink queues.
    pub(super) fn peak(samples: &[f32]) -> f32 {
        samples
            .iter()
            .fold(0.0f32, |peak, sample| peak.max(sample.abs()))
    }

    // The interrupt gate.

    /// The interrupt gate closed: silence from here is intended.
    pub(super) fn gate_closed() -> Instant {
        GATE_CLOSED.store(true, Relaxed);
        EXPECT_AUDIO.store(false, Relaxed);
        GATE_CLOSED_AT.store(now_ns(), Relaxed);
        GATE_DISCARDED.store(0, Relaxed);
        Instant::now()
    }

    /// `interrupt` faded the old track out from `began`. `fade` holds the
    /// chunks queued then and whether the fade reached silence, when there
    /// was a sink to fade.
    pub(super) fn interrupted(began: Instant, fade: Option<(usize, bool)>, buffer_ms: u32) {
        if !telemetry::enabled() {
            return;
        }
        // The fade is clocked by the device, so one that never finished
        // means the device stopped asking for sound, and the skip waited
        // out the whole deadline before reaching Connect.
        let hit_deadline = fade.is_some_and(|(_, silent)| !silent);
        let record = if hit_deadline && INTERRUPT_DEADLINE.ready(Duration::from_secs(60)) {
            telemetry::anomaly("audio.interrupt_deadline")
        } else {
            telemetry::crumb("audio.interrupt")
        };
        record
            .since("fade_ms", began)
            .field("had_sink", fade.is_some())
            .field("queued_chunks", fade.map(|(chunks, _)| chunks))
            .field("hit_deadline", hit_deadline)
            .field("buffer_ms", buffer_ms)
            .field("callback_age_ms", callback_age_ms())
            .emit();
    }

    /// `interrupt` found the gate already closed: another skip in a row.
    pub(super) fn interrupt_repeated() {
        telemetry::crumb("audio.interrupt")
            .field("already_waiting", true)
            .field("gate_ms", age_ms(GATE_CLOSED_AT.load(Relaxed)))
            .emit();
    }

    /// The gate opened; `by` says what released it.
    pub(super) fn gate_released(by: &'static str) {
        GATE_CLOSED.store(false, Relaxed);
        EXPECT_AUDIO.store(false, Relaxed);
        GATE_RELEASED_AT.store(now_ns(), Relaxed);
        let closed_at = GATE_CLOSED_AT.swap(0, Relaxed);
        if closed_at == 0 || !telemetry::enabled() {
            return;
        }
        // A crumb: the player ships `player.gate` for the same opening.
        telemetry::crumb("audio.gate")
            .field("released_by", by)
            .ms("gate_ms", since_ns(closed_at))
            .field(
                "discarded_ms",
                tenth(frames_ms(GATE_DISCARDED.load(Relaxed), SAMPLE_RATE)),
            )
            .field("load_seen", LOAD_STARTED_AT.load(Relaxed) > closed_at)
            .emit();
    }

    /// The closed gate threw a packet of `frames` away.
    pub(super) fn gated(frames: usize) {
        GATE_PACKETS.incr();
        GATE_FRAMES.add(frames as u64);
        GATE_DISCARDED.fetch_add(frames as u64, Relaxed);
        check_gate();
    }

    /// Reports a gate closed far longer than a load takes: the song plays
    /// on, unheard.
    fn check_gate() {
        let closed_at = GATE_CLOSED_AT.load(Relaxed);
        if closed_at == 0 || !GATE_CLOSED.load(Relaxed) || !telemetry::enabled() {
            return;
        }
        let closed_for = since_ns(closed_at);
        if closed_for < GATE_STUCK || GATE_STUCK_REPORTED.swap(closed_at, Relaxed) == closed_at {
            return;
        }
        telemetry::anomaly("audio.gate_stuck")
            .ms("gate_ms", closed_for)
            .field(
                "discarded_ms",
                tenth(frames_ms(GATE_DISCARDED.load(Relaxed), SAMPLE_RATE)),
            )
            .field("load_seen", LOAD_STARTED_AT.load(Relaxed) > closed_at)
            .emit();
    }

    /// A confirmed seek dropped `chunks` of queued sound.
    pub(super) fn seek_cut(chunks: usize) {
        EXPECT_AUDIO.store(false, Relaxed);
        telemetry::crumb("audio.seek_cut")
            .field("chunks_dropped", chunks)
            .emit();
    }

    /// librespot finished a seek, from this or another device.
    pub(super) fn seeked() {
        SEEK_AT.store(now_ns(), Relaxed);
    }

    /// librespot started loading a track.
    pub(super) fn loading() {
        LOAD_STARTED_AT.store(now_ns(), Relaxed);
    }

    /// librespot moved on to another track.
    pub(super) fn track_changed() {
        TRACK_CHANGED_AT.store(now_ns(), Relaxed);
    }

    /// A new engine's control: whatever an earlier one left closed is gone.
    pub(super) fn control_created() {
        GATE_CLOSED.store(false, Relaxed);
        GATE_CLOSED_AT.store(0, Relaxed);
        EXPECT_AUDIO.store(false, Relaxed);
    }

    // The player's thread.

    /// Records where the player's thread is.
    pub(crate) fn phase(phase: u8) {
        WRITER_PHASE.store(phase, Relaxed);
    }

    fn phase_name(phase: u8) -> &'static str {
        match phase {
            OUTSIDE => "librespot",
            PROCESS => "process",
            TAP => "tap",
            OPEN => "output",
            QUEUE => "queue",
            BACKPRESSURE => "backpressure",
            GATE => "gate",
            START => "start",
            STOP => "stop",
            _ => "unknown",
        }
    }

    /// Between `start` and `stop` librespot should keep writing, and a gap
    /// across either is no stall.
    pub(crate) fn feeding(on: bool) {
        FEEDING.store(on, Relaxed);
        WRITE_LEFT_AT.store(0, Relaxed);
        if on && telemetry::enabled() {
            // `start` itself is timed from here, not from the last write.
            WRITE_ENTERED_AT.store(now_ns(), Relaxed);
        }
    }

    /// The player's thread entered a write. Reports librespot keeping it
    /// away for long since the last one, and returns when, while telemetry
    /// is on.
    pub(crate) fn write_began() -> Option<Instant> {
        WRITER_PHASE.store(PROCESS, Relaxed);
        if !telemetry::enabled() {
            return None;
        }
        let entered = now_ns();
        WRITE_ENTERED_AT.store(entered, Relaxed);
        let left = WRITE_LEFT_AT.load(Relaxed);
        let mut gap = Duration::ZERO;
        if left != 0 && FEEDING.load(Relaxed) {
            gap = Duration::from_nanos(entered.saturating_sub(left));
            WRITE_MAX_GAP_MS.raise(gap.as_millis() as i64);
            if gap >= WRITER_STALL {
                librespot_stall(gap, left);
            }
        }
        WRITE_GAP.store(gap.as_nanos() as u64, Relaxed);
        Some(Instant::now())
    }

    /// Whether a seek is open here or another device asked for one within
    /// `window`. Takes telemetry's locks, so only once a gap is long.
    fn seek_asked(window: Duration) -> bool {
        telemetry::trace_open("seek")
            || telemetry::recent_causes(window)
                .iter()
                .any(|cause| cause.kind == "connect:request" && cause.detail == "seek_to")
    }

    /// librespot kept the player's thread for `gap` since it `left` the
    /// last write: decoding, waiting on the file, loading or a command.
    fn librespot_stall(gap: Duration, left: u64) {
        let loading = LOAD_STARTED_AT.load(Relaxed) > left;
        let gated = GATE_CLOSED.load(Relaxed);
        let silence_ms = frames_ms(
            UNDERRUN_FRAMES
                .get()
                .saturating_sub(LEFT_UNDERRUN.load(Relaxed)),
            OUTPUT_RATE.load(Relaxed),
        );
        // A load or a closed gate keeps librespot away on purpose. A seek
        // blocks it until the new position downloads, and the first writes
        // after a seek or a load can still wait on the file.
        let settling = !loading
            && !gated
            && (recent(SEEK_AT.load(Relaxed), SETTLE)
                || recent(LOAD_STARTED_AT.load(Relaxed), SETTLE)
                || seek_asked(gap.saturating_add(SETTLE)));
        let expected = loading || gated || settling;
        if !expected {
            WRITER_STALLS.incr();
        }
        let ship = !expected
            && (silence_ms > 0.0 || gap >= Duration::from_millis(500))
            && STALL_EVENT.ready(Duration::from_secs(2));
        let record = if ship {
            telemetry::event("audio.writer_stall")
        } else {
            telemetry::crumb("audio.writer_stall")
        };
        record
            .ms("gap_ms", gap)
            .field("phase", phase_name(OUTSIDE))
            .field("loading", loading)
            .field("gated", gated)
            .field("settling", settling)
            .field("cushion_ms", LEFT_QUEUED_MS.load(Relaxed))
            .field("silence_ms", tenth(silence_ms))
            .emit();
    }

    /// One of our own steps held the player's thread for `took`.
    pub(crate) fn inside(phase: u8, took: Duration) {
        if took < INSIDE_STALL || !telemetry::enabled() {
            return;
        }
        let record =
            if took >= Duration::from_millis(250) && STALL_EVENT.ready(Duration::from_secs(2)) {
                telemetry::event("audio.writer_stall")
            } else if STALL_CRUMB.ready(Duration::from_millis(200)) {
                telemetry::crumb("audio.writer_stall")
            } else {
                return;
            };
        record
            .ms("gap_ms", took)
            .field("phase", phase_name(phase))
            .emit();
    }

    /// The player's thread is leaving a write.
    pub(crate) fn write_ended() {
        WRITER_PHASE.store(OUTSIDE, Relaxed);
        if telemetry::enabled() {
            WRITE_LEFT_AT.store(now_ns(), Relaxed);
            LEFT_UNDERRUN.store(UNDERRUN_FRAMES.get(), Relaxed);
        }
    }

    /// The loudest finite sample, and how many were NaN or infinite.
    pub(crate) fn level(samples: &[f64]) -> (f64, u64) {
        let mut peak = 0.0f64;
        let mut nonfinite = 0;
        for sample in samples {
            if sample.is_finite() {
                peak = peak.max(sample.abs());
            } else {
                nonfinite += 1;
            }
        }
        (peak, nonfinite)
    }

    /// The equalizer and limiter stage shaped a packet of `frames` in
    /// `took`. `level` describes it after the equalizer and before volume;
    /// `limiter_gain` is the limiter's gain after it, when it ran.
    pub(crate) fn shaped(
        took: Duration,
        frames: usize,
        (peak, nonfinite): (f64, u64),
        limiter_gain: Option<f64>,
        eq_on: bool,
    ) {
        PROCESS_MAX_US.raise(took.as_micros() as i64);
        PACKET_MAX_FRAMES.raise(frames as i64);
        SIGNAL_PEAK.raise((peak * 1_000.0) as i64);
        if let Some(gain) = limiter_gain
            && gain.is_finite()
        {
            LIMITER_REDUCTION.raise(((1.0 - gain) * 1_000.0) as i64);
        }
        // One NaN poisons the limiter's running gain, and with it every
        // sample after, until a skip or a seek rebuilds it.
        let poisoned = limiter_gain.is_some_and(|gain| !gain.is_finite());
        NONFINITE.add(nonfinite);
        if poisoned {
            LIMITER_POISONED.incr();
        }
        if (nonfinite > 0 || poisoned) && NONFINITE_ANOMALY.ready(Duration::from_secs(60)) {
            telemetry::anomaly("audio.nonfinite_samples")
                .field("count", nonfinite)
                .field("limiter_poisoned", poisoned)
                .field("eq_on", eq_on)
                .field("packet_frames", frames)
                .emit();
        }
        inside(PROCESS, took);
    }

    /// The equalizer's settings were locked, by the interface, when the
    /// player's thread wanted them.
    pub(crate) fn eq_lock_waited() {
        EQ_LOCK_WAITS.incr();
    }

    /// The visualisers' buffer was locked when the player's thread wanted
    /// it.
    pub(crate) fn tap_lock_waited() {
        TAP_LOCK_WAITS.incr();
    }

    /// The step each traced change waits for before its first sound counts.
    const FIRST_AUDIO_AFTER: [(&str, &str); 7] = [
        ("next", "track_changed"),
        ("previous", "track_changed"),
        // Previous a few seconds into a song rewinds it instead.
        ("previous", "seeked"),
        ("play", "track_changed"),
        ("resume", "playing"),
        ("transfer", "playing"),
        ("seek", "seeked"),
    ];

    /// Ends each open trace this packet completes: the first sound accepted
    /// for output after the step it waits for. `Tapped` calls it for
    /// librespot's own sinks, which have no `Writer`.
    pub(crate) fn first_audio() -> Option<&'static str> {
        let mut ended = None;
        for (trace, after) in FIRST_AUDIO_AFTER {
            if telemetry::trace_has(trace, after) {
                telemetry::trace_mark(trace, "first_audio");
                telemetry::trace_end(trace, "ok");
                ended = Some(trace);
            }
        }
        ended
    }

    /// Reports what the write path cannot while it is stuck itself: the
    /// player's thread gone for seconds while it should be writing, or an
    /// interrupt gate that never opened. `register` hands it to telemetry's
    /// watch thread, which runs it about once a second. Atomics only, until
    /// the writer has been away for seconds.
    pub fn sample() {
        if !telemetry::enabled() {
            return;
        }
        check_gate();
        let phase = WRITER_PHASE.load(Relaxed);
        // A full queue that will not drain is reported by the write itself.
        if !FEEDING.load(Relaxed) || phase == BACKPRESSURE {
            return;
        }
        let left = WRITE_LEFT_AT.load(Relaxed);
        let since = if phase == OUTSIDE {
            left
        } else {
            WRITE_ENTERED_AT.load(Relaxed)
        };
        if since == 0 {
            return;
        }
        let stuck = since_ns(since);
        let loading = LOAD_STARTED_AT.load(Relaxed) > left;
        // A seek into sound not yet downloaded waits on the network, and its
        // trace reports it if it takes too long.
        if stuck < WRITER_STUCK
            || (phase == OUTSIDE && (loading || seek_asked(stuck.saturating_add(SETTLE))))
            || STUCK_REPORTED.swap(since, Relaxed) == since
        {
            return;
        }
        telemetry::anomaly("audio.writer_stuck")
            .ms("stuck_ms", stuck)
            .field("phase", phase_name(phase))
            .field("gated", GATE_CLOSED.load(Relaxed))
            .field(
                "dry_ms",
                tenth(frames_ms(DRY_RUN.load(Relaxed), OUTPUT_RATE.load(Relaxed))),
            )
            .field("callback_age_ms", callback_age_ms())
            .emit();
    }

    /// What the player's thread remembers between writes.
    #[derive(Default)]
    pub(super) struct Writer {
        /// When a packet last went out quieter than `LOUD`, and since when
        /// packets have been digitally silent.
        quiet_at: Option<Instant>,
        silent_since: Option<Instant>,
        /// The underrun count at the last packet.
        underrun_seen: u64,
        /// The last stats crumb, and the shortest queue, the device's clock
        /// and the underrun count since.
        stats_at: Option<Instant>,
        stats_min_ms: Option<f64>,
        stats_played: Duration,
        stats_underrun: u64,
        /// The heartbeat gauge's window, and the shortest queue in it.
        window_at: Option<Instant>,
        window_min_ms: Option<f64>,
        /// A traced change's first packet: the trace, when it was queued,
        /// and how much sound was queued ahead of it.
        first: Option<(&'static str, u64, f64)>,
    }

    impl Writer {
        /// Playback started or a new queue began: nothing heard yet.
        pub(super) fn restart(&mut self) {
            self.quiet_at = Some(Instant::now());
            self.silent_since = None;
            self.underrun_seen = UNDERRUN_FRAMES.get();
            self.stats_at = None;
        }

        /// The queue was empty when a packet came, `late` after the last:
        /// the device played silence for want of sound.
        pub(super) fn ran_dry(&self, late: Duration, sample_rate: u32, resampling: bool) {
            RAN_DRY.incr();
            if !telemetry::enabled() {
                return;
            }
            let silence_ms = tenth(frames_ms(
                UNDERRUN_FRAMES.get().saturating_sub(self.underrun_seen),
                sample_rate,
            ));
            if DRY_CAUSE.ready(Duration::from_secs(1)) {
                telemetry::note_cause("sink:ran_dry", format!("{silence_ms} ms"));
            }
            let record = if silence_ms >= NOTICEABLE_MS && STARVED.ready(Duration::from_secs(30)) {
                telemetry::anomaly("audio.starved")
            } else if DRY_EVENT.ready(Duration::from_secs(1)) {
                telemetry::event("audio.ran_dry")
            } else if DRY_CRUMB.ready(Duration::from_millis(250)) {
                telemetry::crumb("audio.ran_dry")
            } else {
                return;
            };
            record
                .ms("late_ms", late)
                .field("silence_ms", silence_ms)
                .ms(
                    "writer_gap_ms",
                    Duration::from_nanos(WRITE_GAP.load(Relaxed)),
                )
                .field("cushion_ms", LEFT_QUEUED_MS.load(Relaxed))
                .field("queue_limit", QUEUE_LIMIT)
                .field("sample_rate", sample_rate)
                .field("resampling", resampling)
                .field(
                    "since_track_change_ms",
                    age_ms(TRACK_CHANGED_AT.load(Relaxed)),
                )
                .field("since_load_ms", age_ms(LOAD_STARTED_AT.load(Relaxed)))
                .emit();
        }

        /// A packet joined `queued_before` frames of sound in the queue.
        /// `peak` is its loudest sample, measured while telemetry is on.
        pub(super) fn appended(&mut self, output: &Output, queued_before: u64, peak: Option<f32>) {
            WRITES.incr();
            EXPECT_AUDIO.store(!output.sink.is_paused(), Relaxed);
            if telemetry::enabled() {
                let queued_ms = frames_ms(queued_before, output.sample_rate);
                QUEUE_MAX_MS.raise(queued_ms as i64);
                self.stats_min_ms = Some(
                    self.stats_min_ms
                        .map_or(queued_ms, |min| min.min(queued_ms)),
                );
                self.window_min_ms = Some(
                    self.window_min_ms
                        .map_or(queued_ms, |min| min.min(queued_ms)),
                );
                if let Some(peak) = peak {
                    self.heard(peak);
                }
                self.check_output(output, queued_ms);
                if telemetry::tracing()
                    && let Some(trace) = first_audio()
                {
                    FIRST_RENDER_AT.store(0, Relaxed);
                    FIRST_RENDER_PENDING.store(true, Relaxed);
                    self.first = Some((trace, now_ns(), queued_ms));
                } else {
                    self.report_first_render(output);
                }
                self.stats(output, queued_ms);
            }
            self.underrun_seen = UNDERRUN_FRAMES.get();
        }

        fn heard(&mut self, peak: f32) {
            let now = Instant::now();
            if peak < LOUD {
                self.quiet_at = Some(now);
            }
            if peak < SILENT {
                self.silent_since = Some(self.silent_since.unwrap_or(now));
            } else if let Some(since) = self.silent_since.take() {
                // Silence in the music itself, not a fault: kept so a gap a
                // listener reports can be told from one in the recording.
                let silent = now.saturating_duration_since(since);
                if silent >= Duration::from_secs(2) {
                    telemetry::crumb("audio.signal_gap")
                        .ms("silent_ms", silent)
                        .emit();
                }
            }
        }

        /// The device has rendered silence for a while, although what was
        /// queued ahead of it was loud: the sound is lost between the queue
        /// and the device, in a ramp left closed or a mixer nobody plays.
        fn check_output(&self, output: &Output, queued_ms: f64) {
            let silent_ms = frames_ms(SILENT_RUN.load(Relaxed), output.sample_rate);
            if silent_ms < NOTICEABLE_MS {
                return;
            }
            let Some(quiet_at) = self.quiet_at else {
                return;
            };
            let loud_ms = telemetry::duration_ms(quiet_at.elapsed());
            if loud_ms < silent_ms + queued_ms + 100.0
                || !SILENT_OUTPUT.ready(Duration::from_secs(60))
            {
                return;
            }
            telemetry::anomaly("audio.silent_output")
                .field("silent_ms", tenth(silent_ms))
                .field("signal_ms", loud_ms)
                .field("queued_ms", tenth(queued_ms))
                .field("interrupt_gain", envelope_gain(&output.envelope))
                .field("interrupt_target", envelope_target(&output.envelope))
                .field("transport_gain", envelope_gain(&output.transport))
                .field("transport_target", envelope_target(&output.transport))
                .field(
                    "volume",
                    f64::from(f32::from_bits(output.volume.load(Relaxed))),
                )
                .field("sink_paused", output.sink.is_paused())
                .field("device_paused", output.device.is_paused())
                .field("sample_rate", output.sample_rate)
                .field("resampling", output.resampler.is_some())
                .emit();
        }

        /// Reports how long the device took to ask for a traced change's
        /// first packet, once it has.
        fn report_first_render(&mut self, output: &Output) {
            let Some((trace, queued_at, ahead_ms)) = self.first else {
                return;
            };
            let rendered_at = FIRST_RENDER_AT.load(Relaxed);
            if rendered_at == 0 {
                if since_ns(queued_at) > Duration::from_secs(5) {
                    self.first = None;
                    FIRST_RENDER_PENDING.store(false, Relaxed);
                }
                return;
            }
            self.first = None;
            telemetry::event("audio.first_render")
                .field("trace", trace)
                .ms(
                    "render_delay_ms",
                    Duration::from_nanos(rendered_at.saturating_sub(queued_at)),
                )
                .field("queued_ahead_ms", tenth(ahead_ms))
                .ms("latency_ms", output.device.clock().latency())
                .emit();
        }

        /// Every few seconds, a crumb on how the output keeps up, for the
        /// flight recorder to show around an anomaly. A device that plays
        /// less than the window has fallen behind.
        fn stats(&mut self, output: &Output, queued_ms: f64) {
            let now = Instant::now();
            let Some(at) = self.stats_at else {
                self.stats_at = Some(now);
                self.stats_played = output.device.clock().played();
                self.stats_underrun = UNDERRUN_FRAMES.get();
                return;
            };
            let window = now.saturating_duration_since(at);
            if window < STATS_EVERY {
                return;
            }
            let clock = output.device.clock();
            let played = clock.played();
            let underrun = UNDERRUN_FRAMES.get();
            telemetry::crumb("audio.stats")
                .ms("window_ms", window)
                .ms("played_ms", played.saturating_sub(self.stats_played))
                .field("queued_ms", tenth(queued_ms))
                .field("queue_min_ms", self.stats_min_ms.map(tenth))
                .field(
                    "underrun_ms",
                    tenth(frames_ms(
                        underrun.saturating_sub(self.stats_underrun),
                        output.sample_rate,
                    )),
                )
                .ms("latency_ms", clock.latency())
                .field("sample_rate", output.sample_rate)
                .field("resampling", output.resampler.is_some())
                .emit();
            self.stats_at = Some(now);
            self.stats_played = played;
            self.stats_underrun = underrun;
            self.stats_min_ms = None;
            match self.window_at {
                Some(start) if now.saturating_duration_since(start) >= GAUGE_WINDOW => {
                    if let Some(min) = self.window_min_ms.take() {
                        QUEUE_MIN_MS.set(min as i64);
                    }
                    self.window_at = Some(now);
                }
                None => self.window_at = Some(now),
                Some(_) => {}
            }
        }
    }

    fn envelope_gain(envelope: &Envelope) -> f64 {
        f64::from(envelope.level.load(Relaxed)) / f64::from(SCALE)
    }

    fn envelope_target(envelope: &Envelope) -> f64 {
        f64::from(envelope.target.load(Relaxed)) / f64::from(SCALE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn volume_changes_take_thirty_ms_at_each_output_rate() {
        for rate in [44_100, 48_000, 96_000] {
            let frames = rate * 30 / 1_000;
            let mut ramp = VolumeRamp::default();
            for target in [1.0, 0.25, 0.0, 0.8] {
                let start = ramp.current;
                for frame in 0..frames {
                    let gain = ramp.next_gain(target, rate);
                    let expected = start + (target - start) * frame as f32 / frames as f32;
                    assert!((gain - expected).abs() < 0.0001);
                }
                assert_eq!(ramp.next_gain(target, rate), target);
                assert_eq!(ramp.remaining, 0);
            }
        }
    }

    #[test]
    fn retargeting_volume_continues_from_the_current_gain() {
        let mut ramp = VolumeRamp::default();
        for _ in 0..480 {
            ramp.next_gain(1.0, 48_000);
        }
        let current = ramp.current;
        assert!(current > 0.0 && current < 1.0);
        assert_eq!(ramp.next_gain(0.0, 48_000), current);
        for _ in 1..1440 {
            ramp.next_gain(0.0, 48_000);
        }
        assert_eq!(ramp.next_gain(0.0, 48_000), 0.0);
    }

    #[test]
    fn rendered_volume_is_stereo_linked_and_spans_callbacks() {
        let volume = Arc::new(AtomicU32::new(1.0f32.to_bits()));
        let made = MixerSlot::default();
        let mut render = MixerRender {
            volume: Arc::clone(&volume),
            ramp: VolumeRamp::default(),
            source: None,
            format: (0, 0),
            made: Arc::clone(&made),
        };
        render.configure(48_000, 2);
        let (mixer, _) = made.lock().unwrap().take().unwrap();
        mixer.add(rodio::buffer::SamplesBuffer::new(
            2,
            48_000,
            vec![1.0; 10_000],
        ));
        let mut rising = Vec::new();
        for _ in 0..6 {
            let mut block = [0.0; 480];
            render.render(&mut block);
            rising.extend(block);
        }
        assert_eq!(rising[0], 0.0);
        assert!(rising.windows(2).all(|pair| pair[0] <= pair[1]));
        assert!(
            rising
                .as_chunks::<2>()
                .0
                .iter()
                .all(|pair| pair[0] == pair[1])
        );
        let mut settled = [0.0; 2];
        render.render(&mut settled);
        assert_eq!(settled, [1.0; 2]);
        volume.store(0.0f32.to_bits(), Ordering::Relaxed);
        let mut falling = vec![0.0; 2882];
        render.render(&mut falling);
        assert_eq!(falling[0], 1.0);
        assert_eq!(&falling[2880..], &[0.0; 2]);
        assert!(falling.windows(2).all(|pair| pair[0] >= pair[1]));
        assert!(
            falling
                .as_chunks::<2>()
                .0
                .iter()
                .all(|pair| pair[0] == pair[1])
        );
    }
    /// The buffer setting reaches the device on Windows only (#88), and a
    /// settings file with a wild number in it still opens a stream: the
    /// range is the range whoever wrote the file thought of.
    #[test]
    fn the_buffer_follows_the_setting_within_its_range() {
        let buffer = |ms| output_options(None, ms).buffer;
        let fixed = |ms| Buffer::FixedOnWindows(BufferSize::Duration(Duration::from_millis(ms)));
        assert_eq!(buffer(100), fixed(100));
        assert_eq!(buffer(0), fixed(u64::from(*BUFFER_MS_RANGE.start())));
        assert_eq!(buffer(100_000), fixed(u64::from(*BUFFER_MS_RANGE.end())));
    }

    /// Spotify's own format is asked for first, so nothing is converted, and
    /// a blank device name means the system's default.
    #[test]
    fn the_output_asks_for_spotifys_format_on_the_chosen_device() {
        let options = output_options(Some("USB DAC"), DEFAULT_BUFFER_MS);
        assert_eq!(
            options.device,
            fastframe_audio::Device::Named("USB DAC".into())
        );
        assert_eq!(options.sample_rate, Some(SAMPLE_RATE));
        assert_eq!(options.channels, NUM_CHANNELS as u16);
        assert!(options.follow_default);
        for blank in [None, Some(""), Some("  ")] {
            assert_eq!(
                output_options(blank, DEFAULT_BUFFER_MS).device,
                fastframe_audio::Device::Default
            );
        }
    }

    /// A stream reopened in the format the mixer already plays keeps it, so
    /// what is queued carries on; another rate gets a new mixer for the sink
    /// to pick up.
    #[test]
    fn only_a_new_format_makes_a_new_mixer() {
        let made = MixerSlot::default();
        let mut render = MixerRender {
            volume: Arc::new(AtomicU32::new(1.0f32.to_bits())),
            ramp: VolumeRamp::default(),
            source: None,
            format: (0, 0),
            made: Arc::clone(&made),
        };
        let mut out = [1.0; 8];
        render.render(&mut out);
        assert_eq!(out, [0.0; 8], "silence before there is a mixer");

        render.configure(44_100, 2);
        let (mixer, rate) = made.lock().unwrap().take().expect("a first mixer");
        assert_eq!(rate, 44_100);
        render.configure(44_100, 2);
        assert!(made.lock().unwrap().is_none(), "the same format keeps it");
        render.configure(48_000, 2);
        assert_eq!(
            made.lock().unwrap().as_ref().map(|made| made.1),
            Some(48_000)
        );
        drop(mixer);
    }

    /// A machine without audio (CI, a PC with nothing plugged in) must get
    /// an error and a message for the interface, never a panic. A machine
    /// with audio opens its default device.
    #[test]
    fn starting_without_a_device_is_an_error_not_a_panic() {
        let reported: Arc<Mutex<Option<String>>> = Arc::default();
        let store = Arc::clone(&reported);
        let mut sink = RodioSink::new(
            Some("no such device".into()),
            Arc::new(move |message| *store.lock().unwrap() = Some(message)),
            Box::new(librespot_playback::mixer::NoOpVolume),
            DEFAULT_BUFFER_MS,
            AudioControl::new(DEFAULT_BUFFER_MS),
        );
        match sink.start() {
            Ok(()) => assert!(reported.lock().unwrap().is_none()),
            Err(SinkError::ConnectionRefused(message)) => {
                assert_eq!(reported.lock().unwrap().as_deref(), Some(message.as_str()));
            }
            Err(other) => panic!("unexpected error: {other}"),
        }
        assert!(sink.stop().is_ok());
    }

    /// #636: a paused player stops the device asking for sound, and Play,
    /// or sound arriving without one, starts it again. Needs an output, so
    /// a machine without audio has nothing to check.
    #[test]
    fn pause_stops_the_device_and_play_starts_it_again() {
        let mut sink = RodioSink::new(
            None,
            Arc::new(|_| {}),
            Box::new(librespot_playback::mixer::NoOpVolume),
            DEFAULT_BUFFER_MS,
            AudioControl::new(DEFAULT_BUFFER_MS),
        );
        assert!(sink.start().is_ok());
        let running = |sink: &RodioSink| {
            sink.output
                .as_ref()
                .map(|output| !output.device.is_paused())
        };
        if running(&sink).is_none() {
            return;
        }
        let mut converter = Converter::new(None);
        // Silence, so a test run plays nothing on the speakers.
        let packet = || AudioPacket::Samples(vec![0.0; 441 * NUM_CHANNELS as usize]);
        sink.write(packet(), &mut converter).unwrap();
        assert_eq!(running(&sink), Some(true));

        assert!(sink.stop().is_ok());
        assert_eq!(running(&sink), Some(false), "paused, the device is quiet");
        assert!(sink.stop().is_ok(), "stopping twice is harmless");

        assert!(sink.start().is_ok());
        assert_eq!(running(&sink), Some(true), "Play starts it again");

        assert!(sink.stop().is_ok());
        sink.write(packet(), &mut converter).unwrap();
        assert_eq!(running(&sink), Some(true), "and so does sound");
        assert!(sink.stop().is_ok());
    }

    fn no_device(_: Option<&str>, _: u32, _: &AudioControl) -> Result<Output, OpenError> {
        Err(OpenError::NoDevice)
    }

    /// #623: a PC with no output at all. librespot exits the process when
    /// `start` fails from its playing loop, but pauses cleanly when `write`
    /// fails, so the failure has to surface from `write`, reported to the
    /// interface once per attempt to play.
    #[test]
    fn with_no_output_at_all_playing_fails_at_the_first_packet_not_at_start() {
        let reported: Arc<Mutex<Vec<String>>> = Arc::default();
        let store = Arc::clone(&reported);
        let mut sink = RodioSink {
            open: no_device,
            ..RodioSink::new(
                None,
                Arc::new(move |message| store.lock().unwrap().push(message)),
                Box::new(librespot_playback::mixer::NoOpVolume),
                DEFAULT_BUFFER_MS,
                AudioControl::new(DEFAULT_BUFFER_MS),
            )
        };
        let mut converter = Converter::new(None);
        let packet = || AudioPacket::Samples(vec![0.0; 441 * NUM_CHANNELS as usize]);

        for attempt in 1..=2 {
            assert!(sink.start().is_ok(), "librespot exits when start fails");
            assert_eq!(reported.lock().unwrap().len(), attempt - 1);

            let Err(SinkError::ConnectionRefused(message)) = sink.write(packet(), &mut converter)
            else {
                panic!("the first packet must report the missing output");
            };
            assert_eq!(message, NO_DEVICE);
            assert_eq!(reported.lock().unwrap().len(), attempt);
            assert_eq!(reported.lock().unwrap().last().unwrap(), NO_DEVICE);

            // librespot pauses on the failed write, which stops the sink.
            assert!(sink.stop().is_ok());
        }
    }

    /// A rate that keeps a ramp short enough to step through in a test.
    const RATE: u32 = 1_000;

    /// Ramps that stay out of the way, for tests about something else.
    fn wide_open() -> (Arc<Envelope>, Arc<Envelope>) {
        (
            Envelope::open(RATE, INTERRUPT_FADE),
            Envelope::open(RATE, TRANSPORT_FADE),
        )
    }

    /// A chunk of full scale sound, counted into `queued` the way `write`
    /// counts one, and shaped by the ramps it is handed.
    fn chunk(
        frames: u32,
        interrupt: &Arc<Envelope>,
        transport: &Arc<Envelope>,
        queued: &Arc<Queued>,
    ) -> TransitionSource {
        queued
            .appended
            .fetch_add(u64::from(frames), Ordering::Relaxed);
        TransitionSource::new(
            rodio::buffer::SamplesBuffer::new(
                NUM_CHANNELS.into(),
                RATE,
                vec![1.0; frames as usize * NUM_CHANNELS as usize],
            ),
            Arc::clone(interrupt),
            Arc::clone(transport),
            Arc::clone(queued),
            frames,
        )
    }

    /// The gain each frame comes out at. The sound is full scale, so every
    /// sample is the gain that shaped it.
    fn gains(frames: u32, interrupt: &Arc<Envelope>, transport: &Arc<Envelope>) -> Vec<f32> {
        chunk(frames, interrupt, transport, &Queued::new())
            .step_by(NUM_CHANNELS as usize)
            .collect()
    }

    /// Asserts a ramp still sounds for every one of `frames`, and is silent
    /// on the frame after.
    fn falls_silent_after(envelope: &Envelope, frames: u32) {
        for step in 0..frames {
            assert!(envelope.next_gain() > 0.0, "silent {step} frames early");
        }
        assert_eq!(envelope.next_gain(), 0.0);
        assert!(envelope.silent());
    }

    #[test]
    fn an_interrupted_decoder_cannot_race_to_the_end_while_a_new_track_loads() {
        let control = AudioControl::new(DEFAULT_BUFFER_MS);
        control.interrupt();
        let mut sink = RodioSink::new(
            None,
            Arc::new(|error| panic!("no audio device should be opened: {error}")),
            Box::new(librespot_playback::mixer::NoOpVolume),
            DEFAULT_BUFFER_MS,
            control,
        );
        let mut converter = Converter::new(None);
        let frames = SAMPLE_RATE as usize / 100;
        let started = Instant::now();
        for _ in 0..4 {
            sink.write(
                AudioPacket::Samples(vec![0.0; frames * NUM_CHANNELS as usize]),
                &mut converter,
            )
            .unwrap();
        }
        assert!(
            started.elapsed() >= Duration::from_millis(40),
            "discarded audio must retain backpressure until the new load reaches the decoder"
        );
        assert!(sink.output.is_none());
    }

    #[test]
    fn confirmed_seek_discards_the_old_position_without_gating_new_packets() {
        let control = AudioControl::new(DEFAULT_BUFFER_MS);
        let (sink, mut output) = rodio::Sink::new();
        let sink = Arc::new(sink);
        let (interrupt, transport) = wide_open();
        let queued = Queued::new();
        control.register(&sink, Arc::clone(&interrupt));
        sink.append(chunk(500, &interrupt, &transport, &queued));
        assert_eq!(output.next(), Some(1.0));

        control.handle_player_event(&PlayerEvent::Seeked {
            play_request_id: 1,
            track_id: librespot_core::SpotifyUri::from_uri("spotify:track:14XWXWv5FoCbFzLksawpEe")
                .unwrap(),
            position_ms: 90_000,
        });

        // Rodio checks stop every 5 ms of output. After that, none of the
        // half-second of sound from before the seek may still play.
        output
            .by_ref()
            .take(20 * NUM_CHANNELS as usize)
            .for_each(drop);
        assert!(output.take(50).all(|sample| sample == 0.0));
        assert_eq!(queued.frames(), 0);
        assert!(!control.waiting_for_track());
        assert!(control.take_reset(), "the next packet gets a fresh queue");
    }

    #[test]
    fn track_changes_and_position_updates_preserve_gapless_queued_audio() {
        use librespot_metadata::audio::item::{AudioItem, UniqueFields};

        let control = AudioControl::new(DEFAULT_BUFFER_MS);
        let (sink, mut output) = rodio::Sink::new();
        let sink = Arc::new(sink);
        let (interrupt, transport) = wide_open();
        control.register(&sink, Arc::clone(&interrupt));
        sink.append(chunk(500, &interrupt, &transport, &Queued::new()));
        assert_eq!(output.next(), Some(1.0));
        let track_id =
            librespot_core::SpotifyUri::from_uri("spotify:track:14XWXWv5FoCbFzLksawpEe").unwrap();
        let item = AudioItem {
            track_id: track_id.clone(),
            uri: track_id.to_uri().unwrap(),
            files: Default::default(),
            name: "Next song".into(),
            covers: vec![],
            language: vec![],
            duration_ms: 200_000,
            is_explicit: false,
            availability: Ok(()),
            alternatives: None,
            unique_fields: UniqueFields::Track {
                artists: Default::default(),
                album: "Album".into(),
                album_artists: vec![],
                popularity: 0,
                number: 1,
                disc_number: 1,
            },
        };
        for event in [
            PlayerEvent::TrackChanged {
                audio_item: Box::new(item),
            },
            PlayerEvent::PositionCorrection {
                play_request_id: 1,
                track_id: track_id.clone(),
                position_ms: 100,
            },
            PlayerEvent::PositionChanged {
                play_request_id: 1,
                track_id,
                position_ms: 200,
            },
        ] {
            control.handle_player_event(&event);
            assert!(output.by_ref().take(100).all(|sample| sample == 1.0));
            assert!(!control.take_reset());
        }
    }

    #[test]
    fn a_previous_that_rewinds_releases_the_interrupted_track_gate() {
        let control = AudioControl::new(DEFAULT_BUFFER_MS);
        control.interrupt();
        assert!(control.waiting_for_track());

        control.handle_player_event(&PlayerEvent::Seeked {
            play_request_id: 1,
            track_id: librespot_core::SpotifyUri::from_uri("spotify:track:14XWXWv5FoCbFzLksawpEe")
                .unwrap(),
            position_ms: 0,
        });
        assert!(!control.waiting_for_track());
        assert!(control.take_reset());
    }

    #[test]
    fn an_interrupted_signal_fades_out_and_a_replacement_fades_in() {
        let frames = fade_frames(RATE, INTERRUPT_FADE);
        let (interrupt, transport) = wide_open();
        interrupt.fade_out();

        let faded = gains(frames + 2, &interrupt, &transport);
        assert_eq!(faded[0], 1.0);
        assert_eq!(faded[frames as usize], 0.0);

        let incoming = Envelope::rising(RATE, INTERRUPT_FADE);
        let risen = gains(frames + 2, &incoming, &transport);
        assert_eq!(risen[0], 0.0);
        assert_eq!(risen[frames as usize], 1.0);
    }

    /// One gain per frame rather than per sample, so the two channels of a
    /// frame stay level with each other.
    #[test]
    fn both_channels_of_a_frame_share_a_gain() {
        let (interrupt, transport) = wide_open();
        interrupt.fade_out();

        let played: Vec<_> = chunk(8, &interrupt, &transport, &Queued::new()).collect();
        for pair in played.chunks(NUM_CHANNELS as usize) {
            assert_eq!(pair[0], pair[1]);
        }
    }

    /// A fresh output has played nothing, so the first Play must have silence
    /// to come up from rather than starting already open.
    #[test]
    fn the_first_play_ramps_up_instead_of_starting_open() {
        let transport = Envelope::closed(RATE, TRANSPORT_FADE);
        assert!(transport.silent());
        for _ in 0..fade_frames(RATE, TRANSPORT_FADE) {
            assert_eq!(transport.next_gain(), 0.0);
        }

        transport.fade_in();
        assert_eq!(transport.next_gain(), 0.0);
        assert!(transport.next_gain() > 0.0);
    }

    #[test]
    fn a_pause_reaches_silence_only_after_the_whole_ramp() {
        let transport = Envelope::open(RATE, TRANSPORT_FADE);
        transport.fade_out();
        falls_silent_after(&transport, fade_frames(RATE, TRANSPORT_FADE));
    }

    /// An underrun leaves the pause with no frames to fade through,
    /// so the ramp never moves and the level is still up.
    /// Settling it at the stop is what keeps the next Play coming up
    /// from silence rather than resuming at full gain.
    #[test]
    fn a_pause_with_nothing_queued_still_resumes_from_silence() {
        let transport = Envelope::open(RATE, TRANSPORT_FADE);
        transport.fade_out_over(0);
        // No frame is pulled here, because there is none to pull. That is
        // the underrun, and it leaves the ramp exactly where it started.
        assert!(!transport.silent());

        transport.close();
        assert!(transport.silent());

        transport.fade_in();
        assert_eq!(transport.next_gain(), 0.0);
        assert!(transport.next_gain() > 0.0);
    }

    /// The ramp is clocked by the sound, not by the wall, so it cannot run
    /// past the audio it is shaping however long it is left waiting.
    #[test]
    fn the_fade_advances_with_the_music_not_the_clock() {
        let transport = Envelope::open(RATE, TRANSPORT_FADE);
        transport.fade_out();
        thread::sleep(TRANSPORT_FADE * 2);
        assert_eq!(transport.next_gain(), 1.0);
    }

    /// Skipping during a pause rides both ramps at once, and the shorter one
    /// decides when silence arrives.
    #[test]
    fn a_skip_during_a_pause_carries_both_ramps() {
        let frames = fade_frames(RATE, INTERRUPT_FADE);
        let (interrupt, transport) = wide_open();
        interrupt.fade_out();
        transport.fade_out();

        let faded = gains(frames + 2, &interrupt, &transport);
        assert_eq!(faded[0], 1.0);
        assert_eq!(faded[frames as usize], 0.0);
        assert!(faded.windows(2).all(|pair| pair[0] >= pair[1]));
    }

    #[test]
    fn a_short_queue_still_gets_a_whole_ramp() {
        let left = 20;
        assert!(left < fade_frames(RATE, TRANSPORT_FADE));

        let transport = Envelope::open(RATE, TRANSPORT_FADE);
        transport.fade_out_over(u64::from(left));
        falls_silent_after(&transport, left);
    }

    #[test]
    fn a_deep_queue_does_not_stretch_the_ramp() {
        let nominal = fade_frames(RATE, TRANSPORT_FADE);
        let transport = Envelope::open(RATE, TRANSPORT_FADE);
        transport.fade_out_over(u64::from(nominal) * 10);
        falls_silent_after(&transport, nominal);
    }

    #[test]
    fn playing_a_chunk_takes_it_out_of_the_count() {
        let queued = Queued::new();
        let (interrupt, transport) = wide_open();

        let played = chunk(40, &interrupt, &transport, &queued);
        assert_eq!(queued.frames(), 40);
        assert_eq!(played.count(), 40 * NUM_CHANNELS as usize);
        assert_eq!(queued.frames(), 0);
    }

    /// rodio discards whole sources on a track change. Their frames never
    /// play, so without settling up on drop the count would keep claiming
    /// sound that no longer exists.
    #[test]
    fn a_discarded_chunk_stops_counting_as_queued() {
        let queued = Queued::new();
        let (interrupt, transport) = wide_open();

        drop(chunk(40, &interrupt, &transport, &queued));
        assert_eq!(queued.frames(), 0);
    }
}
