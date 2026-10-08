//! Bridge between the UI thread and asynchronous work.
//!
//! egui runs on the main thread and must never block. A dedicated tokio
//! runtime hosts the librespot engine, the Web API client, sign-in, and
//! artwork fetches; the two sides talk through channels. Every event wakes
//! the interface with `request_repaint`, so the app stays event-driven and
//! idle when nothing is happening.

use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use librespot_core::authentication::Credentials;
use tokio::sync::{mpsc, watch};

use crate::api::models::*;
use crate::api::{
    AccountId, ApiError, ApiGateway, ApiSource, NetActivity, Operation, PlayRequest, PlaylistId,
    SessionState, TokenProvider, WebTokens,
};
use crate::credentials::{
    Grant as StoredGrant, Lease as CredentialLease, Slot as CredentialSlot,
    Store as CredentialStore,
};
use crate::http::Http;
use crate::images::{ArtLoader, accent_color};
use crate::model::PlaylistCache;
use crate::paths::AppDirs;
use crate::player::{
    Engine, EngineConfig, EngineEvent, Heard, LocalState, PlaybackResume, PlayerCommand,
};
use crate::session_reads;
use crate::settings::ProxyConfig;

pub type ApiResult<T> = Result<T, ApiError>;

const PREMIUM_NEEDED: &str = "Local playback needs Spotify Premium.";
const ALBUM_TYPE_TIMEOUT: Duration = Duration::from_secs(30);
// Leave time for access-point retries plus resolver and authentication work.
const ENGINE_CONNECT_TIMEOUT: Duration = Duration::from_secs(75);
// Keep at most one full Web API album page outstanding for a playback engine.
const MAX_PENDING_ALBUM_TYPES: usize = 50;
// Saved shows asked about in one extended-metadata request.
const AUDIOBOOK_BATCH: usize = 50;
/// How long resolving a radio station and its songs may take.
const RADIO_TIMEOUT: Duration = Duration::from_secs(20);
/// Newest episodes read from each saved podcast for Home's podcast shelf.
const HOME_EPISODES_PER_SHOW: u32 = 5;
pub const PLAYLIST_PAGE_SIZE: u32 = 50;
const RECONNECT_WINDOW: Duration = Duration::from_secs(600);
const RECONNECT_LIMIT: usize = 6;

async fn connect_engine_with_deadline<F: std::future::Future>(
    connect: F,
) -> Result<F::Output, tokio::time::error::Elapsed> {
    tokio::time::timeout(ENGINE_CONNECT_TIMEOUT, connect).await
}

/// True when the session has already dropped this many times in the window,
/// so another reconnect would only flap. Callers that still reconnect must
/// push `now` themselves.
fn session_drops_exhausted(reconnects: &mut Vec<Instant>, now: Instant) -> bool {
    reconnects.retain(|attempt| now.duration_since(*attempt) < RECONNECT_WINDOW);
    reconnects.len() >= RECONNECT_LIMIT
}

/// Record a replacement requested while a connection attempt is still in
/// flight. The finished attempt must be discarded and immediately retried
/// with the newest config instead of installing stale state.
fn defer_engine_replace(engine_busy: bool, restart_pending: &mut bool) -> bool {
    if engine_busy {
        *restart_pending = true;
    }
    engine_busy
}

#[derive(Clone, Debug, PartialEq)]
pub enum AuthStatus {
    Starting,
    SignedOut,
    WaitingForBrowser { url: String },
    Connecting,
    Connected { username: String },
    Failed(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemoteAction {
    Play,
    Pause,
    Next,
    Previous,
    Seek,
    Volume,
    Shuffle,
    Repeat,
}

/// Which of the two readers of the recently-played endpoint an answer
/// belongs to: the shelf on Home, or the Recents tab in the queue panel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecentsFor {
    Home,
    Panel,
}

#[derive(Clone, Debug)]
pub enum ApiRequest {
    Me,
    Devices,
    PlaybackState {
        seq: u64,
    },
    Queue {
        seq: u64,
    },
    RecentlyPlayed {
        /// Request owner. Home and Recents use separate generation counters,
        /// so generation alone cannot route the response.
        who: RecentsFor,
        generation: u64,
        before: Option<String>,
        limit: u32,
    },
    TopTracks {
        offset: u32,
        full: bool,
        generation: u64,
    },
    TopArtists {
        generation: u64,
    },
    Recommendations {
        seed_tracks: Vec<String>,
        seed_artists: Vec<String>,
        generation: u64,
    },
    Discover {
        term: String,
        generation: u64,
    },
    MyPlaylists {
        offset: u32,
        generation: u64,
    },
    Playlist {
        id: String,
        generation: u64,
    },
    PlaylistItems {
        id: String,
        offset: u32,
        generation: u64,
    },
    /// A slice of a playlist read only for who added its songs; the rows
    /// on screen stay untouched.
    PlaylistSample {
        id: String,
        offset: u32,
        generation: u64,
    },
    CreatePlaylist {
        name: String,
        public: bool,
        description: String,
    },
    UploadPlaylistCover {
        id: String,
        request: u64,
        previous_urls: Vec<String>,
        cover: crate::playlist_cover::Cover,
    },
    UpdatePlaylist {
        id: String,
        name: Option<String>,
        description: Option<String>,
        public: Option<bool>,
    },
    CheckPlaylistDuplicates {
        playlist_id: String,
        playlist_name: String,
        items: Vec<PlayableItem>,
        position: Option<u32>,
    },
    AddToPlaylist {
        playlist_id: String,
        playlist_name: String,
        uris: Vec<String>,
        position: Option<u32>,
    },
    RemoveFromPlaylist {
        playlist_id: String,
        uris: Vec<String>,
        snapshot_id: Option<String>,
    },
    ReorderPlaylist {
        playlist_id: String,
        range_start: u32,
        insert_before: u32,
        snapshot_id: Option<String>,
    },
    FollowPlaylist {
        id: String,
        follow: bool,
    },
    SavedTracks {
        offset: u32,
        generation: u64,
    },
    SavedAlbums {
        offset: u32,
    },
    FollowedArtists {
        after: Option<String>,
    },
    SavedShows {
        offset: u32,
    },
    SavedEpisodes {
        offset: u32,
    },
    SetSaved {
        uris: Vec<String>,
        saved: bool,
    },
    Contains {
        uris: Vec<String>,
    },
    Search {
        query: String,
        serial: u64,
    },
    SearchCatalogue {
        query: String,
        serial: u64,
    },
    SearchPlaylists {
        query: String,
        serial: u64,
    },
    Artist {
        id: String,
    },
    ArtistTopTracks {
        id: String,
    },
    ArtistAlbums {
        id: String,
        groups: String,
        offset: u32,
    },
    RelatedArtists {
        id: String,
    },
    Album {
        id: String,
    },
    AlbumTracks {
        id: String,
        offset: u32,
        generation: u64,
    },
    AlbumQueueTracks {
        id: String,
        offset: u32,
        request: u64,
    },
    Show {
        id: String,
    },
    ShowEpisodes {
        id: String,
        offset: u32,
    },
    /// The newest episodes of a few saved podcasts, for Home. The shows
    /// are read one after another, not all at once.
    HomeEpisodes {
        shows: Vec<Show>,
        generation: u64,
    },
    Track {
        id: String,
    },
    /// One episode, asked for by a link to it: the podcast it belongs to
    /// is the page that opens.
    Episode {
        id: String,
    },
    Remote {
        action: RemoteAction,
        device_id: Option<String>,
        play: Option<PlayRequest>,
        position_ms: u32,
        percent: u8,
        flag: bool,
        repeat: String,
    },
    Transfer {
        device_id: String,
        play: bool,
    },
    /// Shuffle on, then start the context, one after the other: sent as two
    /// independent requests they race, and shuffle sometimes lost.
    ShufflePlay {
        device_id: Option<String>,
        play: PlayRequest,
    },
    AddToQueue {
        uri: String,
        device_id: Option<String>,
        label: String,
    },
    AddManyToQueue {
        request: u64,
        uris: Vec<String>,
        device_id: Option<String>,
    },
}

impl ApiRequest {
    fn background(&self) -> bool {
        matches!(
            self,
            Self::PlaybackState { .. }
                | Self::RecentlyPlayed { .. }
                | Self::TopTracks { .. }
                | Self::TopArtists { .. }
                | Self::Recommendations { .. }
                | Self::Discover { .. }
                | Self::MyPlaylists { .. }
                | Self::PlaylistSample { .. }
                | Self::Contains { .. }
        )
    }

    /// The variant's name, for telemetry, which never sees a request's
    /// contents: they hold search text, playlist names and covers.
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Me => "Me",
            Self::Devices => "Devices",
            Self::PlaybackState { .. } => "PlaybackState",
            Self::Queue { .. } => "Queue",
            Self::RecentlyPlayed { .. } => "RecentlyPlayed",
            Self::TopTracks { .. } => "TopTracks",
            Self::TopArtists { .. } => "TopArtists",
            Self::Recommendations { .. } => "Recommendations",
            Self::Discover { .. } => "Discover",
            Self::MyPlaylists { .. } => "MyPlaylists",
            Self::Playlist { .. } => "Playlist",
            Self::PlaylistItems { .. } => "PlaylistItems",
            Self::PlaylistSample { .. } => "PlaylistSample",
            Self::CreatePlaylist { .. } => "CreatePlaylist",
            Self::UploadPlaylistCover { .. } => "UploadPlaylistCover",
            Self::UpdatePlaylist { .. } => "UpdatePlaylist",
            Self::CheckPlaylistDuplicates { .. } => "CheckPlaylistDuplicates",
            Self::AddToPlaylist { .. } => "AddToPlaylist",
            Self::RemoveFromPlaylist { .. } => "RemoveFromPlaylist",
            Self::ReorderPlaylist { .. } => "ReorderPlaylist",
            Self::FollowPlaylist { .. } => "FollowPlaylist",
            Self::SavedTracks { .. } => "SavedTracks",
            Self::SavedAlbums { .. } => "SavedAlbums",
            Self::FollowedArtists { .. } => "FollowedArtists",
            Self::SavedShows { .. } => "SavedShows",
            Self::SavedEpisodes { .. } => "SavedEpisodes",
            Self::SetSaved { .. } => "SetSaved",
            Self::Contains { .. } => "Contains",
            Self::Search { .. } => "Search",
            Self::SearchCatalogue { .. } => "SearchCatalogue",
            Self::SearchPlaylists { .. } => "SearchPlaylists",
            Self::Artist { .. } => "Artist",
            Self::ArtistTopTracks { .. } => "ArtistTopTracks",
            Self::ArtistAlbums { .. } => "ArtistAlbums",
            Self::RelatedArtists { .. } => "RelatedArtists",
            Self::Album { .. } => "Album",
            Self::AlbumTracks { .. } => "AlbumTracks",
            Self::AlbumQueueTracks { .. } => "AlbumQueueTracks",
            Self::Show { .. } => "Show",
            Self::ShowEpisodes { .. } => "ShowEpisodes",
            Self::HomeEpisodes { .. } => "HomeEpisodes",
            Self::Track { .. } => "Track",
            Self::Episode { .. } => "Episode",
            Self::Remote { action, .. } => observe::remote_kind(*action),
            Self::Transfer { .. } => "Transfer",
            Self::ShufflePlay { .. } => "ShufflePlay",
            Self::AddToQueue { .. } => "AddToQueue",
            Self::AddManyToQueue { .. } => "AddManyToQueue",
        }
    }
}

#[derive(Debug)]
pub enum ApiResponse {
    Me(ApiResult<User>),
    Devices(ApiResult<Vec<Device>>),
    PlaybackState {
        seq: u64,
        result: ApiResult<Option<PlaybackState>>,
    },
    Queue {
        seq: u64,
        result: ApiResult<Queue>,
    },
    RecentlyPlayed {
        who: RecentsFor,
        generation: u64,
        limit: u32,
        result: ApiResult<CursorPage<PlayHistory>>,
    },
    TopTracks {
        offset: u32,
        full: bool,
        generation: u64,
        result: ApiResult<Page<Track>>,
    },
    TopArtists {
        generation: u64,
        result: ApiResult<Vec<Artist>>,
    },
    Recommendations {
        generation: u64,
        result: ApiResult<Vec<Track>>,
    },
    Discover {
        term: String,
        generation: u64,
        result: ApiResult<Vec<Playlist>>,
    },
    MyPlaylists {
        offset: u32,
        generation: u64,
        result: ApiResult<Page<Playlist>>,
    },
    Playlist {
        id: String,
        generation: u64,
        result: ApiResult<Playlist>,
    },
    PlaylistItems {
        id: String,
        offset: u32,
        generation: u64,
        result: ApiResult<Page<PlaylistItem>>,
    },
    PlaylistSample {
        id: String,
        generation: u64,
        result: ApiResult<Page<PlaylistItem>>,
    },
    PlaylistCreated(ApiResult<Playlist>),
    PlaylistCoverUploaded {
        id: String,
        request: u64,
        previous_urls: Vec<String>,
        cover: crate::playlist_cover::Cover,
        result: ApiResult<()>,
    },
    PlaylistUpdated {
        id: String,
        result: ApiResult<()>,
    },
    PlaylistDuplicatesChecked {
        playlist_id: String,
        playlist_name: String,
        items: Vec<PlayableItem>,
        position: Option<u32>,
        result: ApiResult<Vec<String>>,
    },
    PlaylistItemsChanged {
        id: String,
        message: String,
        result: ApiResult<Option<String>>,
    },
    PlaylistFollowChanged {
        id: String,
        followed: bool,
        result: ApiResult<()>,
    },
    SavedTracks {
        offset: u32,
        generation: u64,
        account_id: Option<String>,
        result: ApiResult<Page<SavedTrack>>,
    },
    SavedAlbums {
        offset: u32,
        result: ApiResult<Page<SavedAlbum>>,
    },
    FollowedArtists {
        after: Option<String>,
        result: ApiResult<CursorPage<Artist>>,
    },
    SavedShows {
        offset: u32,
        result: ApiResult<Page<SavedShow>>,
    },
    SavedEpisodes {
        offset: u32,
        result: ApiResult<Page<SavedEpisode>>,
    },
    SavedChanged {
        uris: Vec<String>,
        saved: bool,
        result: ApiResult<()>,
    },
    Contains {
        uris: Vec<String>,
        result: ApiResult<Vec<bool>>,
    },
    SearchStarted {
        query: String,
        serial: u64,
        split: bool,
    },
    Search {
        query: String,
        serial: u64,
        result: ApiResult<SearchResults>,
    },
    SearchPlaylists {
        query: String,
        serial: u64,
        result: ApiResult<Page<Playlist>>,
    },
    Artist {
        id: String,
        result: ApiResult<Artist>,
    },
    ArtistTopTracks {
        id: String,
        result: ApiResult<Vec<Track>>,
    },
    ArtistAlbums {
        id: String,
        groups: String,
        offset: u32,
        result: ApiResult<Page<Album>>,
    },
    RelatedArtists {
        id: String,
        result: ApiResult<Vec<Artist>>,
    },
    Album {
        id: String,
        result: ApiResult<Album>,
    },
    AlbumTracks {
        id: String,
        offset: u32,
        generation: u64,
        result: ApiResult<Page<Track>>,
    },
    AlbumQueueTracks {
        offset: u32,
        request: u64,
        result: ApiResult<Page<Track>>,
    },
    Show {
        id: String,
        result: ApiResult<Show>,
    },
    ShowEpisodes {
        id: String,
        offset: u32,
        result: ApiResult<Page<Episode>>,
    },
    /// Each show with its newest episodes, in the order asked for.
    HomeEpisodes {
        generation: u64,
        result: ApiResult<Vec<(Show, Vec<Episode>)>>,
    },
    Track {
        id: String,
        result: ApiResult<Track>,
    },
    Episode {
        id: String,
        result: ApiResult<Episode>,
    },
    Remote {
        action: RemoteAction,
        result: ApiResult<()>,
    },
    Transferred {
        device_id: String,
        result: ApiResult<()>,
    },
    QueueAdded {
        label: String,
        result: ApiResult<()>,
    },
    QueueBatchAdded {
        request: u64,
        added: usize,
        result: ApiResult<()>,
    },
}

pub enum PlaylistCacheRows {
    Replace(Vec<PlaylistItem>),
    Append {
        previous_rows: usize,
        previous_offset: u32,
        items: Vec<PlaylistItem>,
    },
}

struct PlaylistCacheWrite {
    path: std::path::PathBuf,
    account_id: String,
    id: String,
    generation: u64,
    snapshot: String,
    rows: PlaylistCacheRows,
    total: u32,
    next_offset: Option<u32>,
}

pub enum Command {
    OpenThemesFolder,
    ProxyRestored {
        lease: CredentialLease,
        result: Result<crate::credentials::Loaded, crate::credentials::Error>,
    },
    CredentialsRestored {
        slot: CredentialSlot,
        lease: CredentialLease,
        result: Result<crate::credentials::Loaded, crate::credentials::Error>,
    },
    CheckPlaylistCover {
        id: String,
        request: u64,
        cover: crate::playlist_cover::Cover,
        images: Vec<crate::api::models::Image>,
    },
    ChoosePlaylistCover {
        id: String,
        request: u64,
        selected:
            std::pin::Pin<Box<dyn std::future::Future<Output = Option<rfd::FileHandle>> + Send>>,
    },
    /// Start (or restart) the Web API sign-in in the browser.
    SignIn {
        request: u64,
        config: ProxyConfig,
    },
    CancelSignIn,
    SignOut,
    /// Authorize local playback on this computer (a separate browser grant).
    AuthorizePlayback,
    /// Reload the engine config (audio settings changed).
    RestartEngine(EngineConfig),
    /// Rebuild the HTTP client. Restart local playback only when its HTTP
    /// proxy changed; Off, System, and SOCKS5 share a direct engine connection.
    ApplyProxy {
        request: u64,
        config: ProxyConfig,
    },
    Player(PlayerCommand),
    Api(ApiRequest),
    ApiFinished {
        generation: u64,
        response: Box<ApiResponse>,
        expired: Option<ApiSource>,
        shared_lease: CredentialLease,
        personal_lease: CredentialLease,
    },
    Accent {
        url: String,
    },
    Shutdown,
    /// Internal: the Web API browser flow produced a grant.
    WebSignedIn {
        source: ApiSource,
        token: Box<crate::auth::StoredToken>,
        lease: CredentialLease,
        attempt: u64,
    },
    WebVerified {
        source: ApiSource,
        token: Box<crate::auth::StoredToken>,
        user: Box<User>,
        lease: CredentialLease,
        attempt: u64,
    },
    WebVerificationFailed {
        source: ApiSource,
        lease: CredentialLease,
        attempt: u64,
        error: ApiError,
    },
    /// Internal: a Web API browser flow or verification ended (success or not).
    SignInEnded {
        source: ApiSource,
        attempt: u64,
    },
    /// Internal: the playback browser flow ended without a credential.
    PlaybackAuthEnded {
        attempt: u64,
    },
    /// Internal: the playback grant produced a streaming access token.
    PlaybackAuthorized {
        access_token: String,
        lease: CredentialLease,
        attempt: u64,
    },
    /// Internal: an engine connection attempt finished.
    EngineConnected {
        session_generation: u64,
        engine: Box<Option<Engine>>,
        error: Option<String>,
        lease: CredentialLease,
    },
    /// Internal: librespot's session ended on its own.
    Reconnect,
    /// Look for Spotify Connect receivers on the local network.
    DiscoverReceivers,
    /// Send the account to a receiver so it joins Spotify Connect.
    ActivateReceiver(Box<crate::zeroconf::Receiver>),
    /// Ask GitHub whether a newer release exists. Manual checks report every
    /// outcome; the daily check only announces a new release.
    CheckForUpdates {
        manual: bool,
        source: crate::updates::Source,
    },
    InspectUpdate,
    DownloadUpdate {
        release: crate::updates::Release,
        source: crate::updates::Source,
    },
    InstallUpdate {
        prepared: Box<crate::updates::Prepared>,
        arguments: Vec<String>,
    },
    /// The words of a track, from LRCLIB.
    Lyrics(Box<LyricsRequest>),
    /// The account's playlist tree, folders and all, from the session.
    Rootlist,
    /// Internal: a rootlist read completed for this signed-in session.
    RootlistFinished {
        generation: u64,
        result: Result<crate::player::Rootlist, String>,
    },
    /// Check that a reconnect's pickup really started, and try again if not.
    VerifyResume,
    /// Add, replace, or remove the optional personal Web API application.
    ConfigurePersonalWebApp(Option<String>),
    /// Read a playlist's cached items from disk.
    LoadPlaylistCache {
        id: String,
        generation: u64,
    },
    /// Remember a playlist prefix on disk under its snapshot.
    StorePlaylistCache {
        id: String,
        generation: u64,
        snapshot: String,
        rows: PlaylistCacheRows,
        total: u32,
        next_offset: Option<u32>,
    },
    /// Resolve user ids to display names through the streaming session.
    UserNames(Vec<String>),
    LoadLikedSongsCache {
        generation: u64,
    },
    StoreLikedSongsCache(crate::liked::Cache),
    /// Resolve the precise type of Web API singles through the streaming session.
    AlbumTypes(Vec<String>),
    /// Ask the streaming session which saved shows are audiobooks.
    AudiobookShows(Vec<String>),
    /// Resolve Spotify's radio seeded by `seed` through the streaming session.
    Radio {
        seed: String,
        generation: u64,
    },
    /// Internal: a radio finished resolving for the session it started in.
    RadioResolved {
        session_generation: u64,
        seed: String,
        generation: u64,
        result: Result<Vec<crate::api::models::Track>, String>,
    },
    /// Internal: an audiobook lookup finished for the session it started in.
    AudiobookShowsResolved {
        session_generation: u64,
        audiobooks: Vec<String>,
    },
    /// Internal: one precise album type lookup finished.
    AlbumTypeResolved {
        uri: String,
        session_generation: u64,
        engine_generation: u64,
        result: Result<bool, String>,
    },
}

impl Command {
    /// The variant's name, for telemetry: a command can carry tokens,
    /// account data and listening history, so nothing else is reported.
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::OpenThemesFolder => "OpenThemesFolder",
            Self::ProxyRestored { .. } => "ProxyRestored",
            Self::CredentialsRestored { .. } => "CredentialsRestored",
            Self::CheckPlaylistCover { .. } => "CheckPlaylistCover",
            Self::ChoosePlaylistCover { .. } => "ChoosePlaylistCover",
            Self::SignIn { .. } => "SignIn",
            Self::CancelSignIn => "CancelSignIn",
            Self::SignOut => "SignOut",
            Self::AuthorizePlayback => "AuthorizePlayback",
            Self::RestartEngine(_) => "RestartEngine",
            Self::ApplyProxy { .. } => "ApplyProxy",
            Self::Player(_) => "Player",
            Self::Api(_) => "Api",
            Self::ApiFinished { .. } => "ApiFinished",
            Self::Accent { .. } => "Accent",
            Self::Shutdown => "Shutdown",
            Self::WebSignedIn { .. } => "WebSignedIn",
            Self::WebVerified { .. } => "WebVerified",
            Self::WebVerificationFailed { .. } => "WebVerificationFailed",
            Self::SignInEnded { .. } => "SignInEnded",
            Self::PlaybackAuthEnded { .. } => "PlaybackAuthEnded",
            Self::PlaybackAuthorized { .. } => "PlaybackAuthorized",
            Self::EngineConnected { .. } => "EngineConnected",
            Self::Reconnect => "Reconnect",
            Self::DiscoverReceivers => "DiscoverReceivers",
            Self::ActivateReceiver(_) => "ActivateReceiver",
            Self::CheckForUpdates { .. } => "CheckForUpdates",
            Self::InspectUpdate => "InspectUpdate",
            Self::DownloadUpdate { .. } => "DownloadUpdate",
            Self::InstallUpdate { .. } => "InstallUpdate",
            Self::Lyrics(_) => "Lyrics",
            Self::Rootlist => "Rootlist",
            Self::RootlistFinished { .. } => "RootlistFinished",
            Self::VerifyResume => "VerifyResume",
            Self::ConfigurePersonalWebApp(_) => "ConfigurePersonalWebApp",
            Self::LoadPlaylistCache { .. } => "LoadPlaylistCache",
            Self::StorePlaylistCache { .. } => "StorePlaylistCache",
            Self::UserNames(_) => "UserNames",
            Self::LoadLikedSongsCache { .. } => "LoadLikedSongsCache",
            Self::StoreLikedSongsCache(_) => "StoreLikedSongsCache",
            Self::AlbumTypes(_) => "AlbumTypes",
            Self::AudiobookShows(_) => "AudiobookShows",
            Self::Radio { .. } => "Radio",
            Self::RadioResolved { .. } => "RadioResolved",
            Self::AudiobookShowsResolved { .. } => "AudiobookShowsResolved",
            Self::AlbumTypeResolved { .. } => "AlbumTypeResolved",
        }
    }
}

pub struct LyricsRequest {
    /// The track the answer is for, so a stale one is ignored.
    pub uri: String,
    pub query: crate::lyrics::Query,
}

pub enum Event {
    ProxyRestored {
        config: ProxyConfig,
        password: Option<crate::credentials::ProxyPassword>,
    },
    ProxyPasswordStored,
    ProxyStorageFailed(crate::credentials::Error),
    ProxyApplied {
        request: u64,
        config: ProxyConfig,
        result: Result<bool, String>,
    },
    UpdateSupport(Result<crate::updates::Installation, String>),
    UpdateProgress {
        received: u64,
        total: u64,
    },
    UpdateDownloaded(Result<Box<crate::updates::Prepared>, String>),
    UpdateInstalling(Result<(), String>),
    PlaylistCoverChecked {
        id: String,
        request: u64,
        images: Vec<crate::api::models::Image>,
        result: Result<bool, String>,
    },
    PlaylistCoverChosen {
        id: String,
        request: u64,
        result: Result<Option<crate::playlist_cover::Cover>, String>,
    },
    Auth(AuthStatus),
    Playback(LocalPlayback),
    /// Receivers seen on the local network that Spotify has not listed.
    Receivers(Vec<crate::zeroconf::Receiver>),
    ReceiverActivated {
        name: String,
        result: Result<(), String>,
    },
    Local(Box<LocalState>),
    Api(Box<ApiResponse>),
    Accent {
        url: String,
        color: [u8; 3],
    },
    Error(String),
    /// GitHub answered an update check, or the request failed.
    UpdateChecked {
        manual: bool,
        result: Result<Option<crate::updates::Release>, String>,
    },
    /// Track lyrics, or `None` when unavailable.
    Lyrics {
        uri: String,
        result: Result<Option<crate::lyrics::Lyrics>, String>,
    },
    /// The account's playlist tree, folders and all, and which of its
    /// playlists take songs from this account.
    Rootlist {
        result: Result<crate::player::Rootlist, String>,
    },
    /// The result of reading a playlist cache for this load generation.
    PlaylistCache {
        account_id: String,
        id: String,
        generation: u64,
        cache: Option<PlaylistCache>,
    },
    PlaylistCacheStored {
        account_id: String,
        id: String,
        generation: u64,
        snapshot: String,
        success: bool,
    },
    /// A user id resolved to a display name (`None` when nothing answers).
    UserName {
        id: String,
        name: Option<String>,
    },
    /// Saved shows that Spotify's metadata marks as audiobooks. librespot
    /// cannot play them, so the Podcasts shelf leaves them out.
    AudiobookShows(Vec<String>),
    /// The songs of the radio seeded by `seed`, for the request `generation`.
    Radio {
        seed: String,
        generation: u64,
        result: Result<Vec<crate::api::models::Track>, String>,
    },
    /// Whether Spotify's internal metadata positively identifies an album as an EP.
    AlbumType {
        uri: String,
        result: Result<bool, String>,
    },
    /// The verified personal Web API app, or `None` when it is disabled.
    WebApp {
        client_id: Option<String>,
    },
    LikedSongsCache {
        account_id: String,
        generation: u64,
        cache: Option<crate::liked::Cache>,
    },
}

/// The state of playback on this computer, independent of Web API sign-in.
#[derive(Clone, Debug, PartialEq)]
pub enum LocalPlayback {
    /// Not authorized; local playback is unavailable but the app still works.
    Unavailable,
    /// The browser is open for the playback grant.
    Authorizing,
    /// Connecting the librespot engine.
    Connecting,
    /// This computer is a ready Spotify Connect device.
    Ready {
        device_id: String,
    },
    Failed(String),
}

/// Background services (the runtime, MPRIS, the tray) outlive individual
/// windows: the window is destroyed when it closes to the tray and created
/// again on demand. They therefore hold this handle, which repaints
/// whichever window exists, instead of an `egui::Context`.
pub use fastframe_shell::Waker;

/// The interface's handle to the runtime.
pub struct Backend {
    commands: mpsc::UnboundedSender<Command>,
    events: std::sync::mpsc::Receiver<Event>,
    art: ArtLoader,
    activity: Arc<NetActivity>,
    thread: Option<std::thread::JoinHandle<()>>,
    offline: bool,
    #[cfg(test)]
    playlist_item_requests: std::sync::Mutex<Vec<(String, u32, u64)>>,
    #[cfg(test)]
    playlist_sample_requests: std::sync::Mutex<Vec<(String, u32, u64)>>,
    #[cfg(test)]
    playlist_add_requests: std::sync::Mutex<Vec<ApiRequest>>,
    #[cfg(test)]
    remote_play_requests: std::sync::Mutex<Vec<ApiRequest>>,
    #[cfg(test)]
    remote_shuffle_requests: std::sync::Mutex<Vec<ApiRequest>>,
    #[cfg(test)]
    queue_requests: std::sync::Mutex<Vec<ApiRequest>>,
    #[cfg(test)]
    queued_tracks: std::sync::Mutex<Vec<String>>,
    #[cfg(test)]
    player_commands: std::sync::Mutex<Vec<PlayerCommand>>,
    #[cfg(test)]
    album_type_requests: std::sync::Mutex<Vec<Vec<String>>>,
    #[cfg(test)]
    home_episode_requests: std::sync::Mutex<Vec<(Vec<String>, u64)>>,
}

impl Backend {
    pub fn spawn(
        dirs: AppDirs,
        engine_config: EngineConfig,
        web_client_id: Option<String>,
        waker: Waker,
        restore_sign_in: bool,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("spotifast-runtime")
            .enable_all()
            .build()
            .expect("unable to start the async runtime");
        if crate::telemetry::enabled() {
            observe::register();
            runtime.spawn(observe::watch_runtime());
        }
        let http = if restore_sign_in {
            Http::unavailable("Restoring proxy settings".into())
        } else {
            Http::from_proxy(&engine_config.proxy).unwrap_or_else(|error| {
                let _ = event_tx.send(Event::Error(format!(
                    "Network configuration failed: {error}"
                )));
                Http::unavailable(error)
            })
        };
        let art = ArtLoader::new(http.clone(), runtime.handle().clone(), dirs.art_cache_dir());
        let activity = Arc::new(NetActivity::default());

        let worker_activity = Arc::clone(&activity);
        let worker_art = art.clone();
        let worker_commands = command_tx.clone();
        let thread = std::thread::Builder::new()
            .name("spotifast-backend".to_string())
            .spawn(move || {
                runtime.block_on(async move {
                    let mut worker = Worker::new(
                        dirs,
                        engine_config,
                        web_client_id,
                        http,
                        worker_art,
                        worker_activity,
                        event_tx,
                        worker_commands,
                        waker,
                    );
                    if restore_sign_in {
                        worker.restore_session();
                    }
                    worker.run(command_rx).await;
                });
                // Give librespot's own threads a moment to release the audio device.
                runtime.shutdown_timeout(Duration::from_secs(2));
            })
            .expect("unable to start the backend thread");

        Self {
            commands: command_tx,
            events: event_rx,
            art,
            activity,
            thread: Some(thread),
            offline: false,
            #[cfg(test)]
            playlist_item_requests: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            playlist_sample_requests: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            playlist_add_requests: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            remote_play_requests: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            remote_shuffle_requests: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            queue_requests: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            queued_tracks: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            player_commands: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            album_type_requests: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            home_episode_requests: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Live network activity, for the interface's busy indicator.
    pub fn activity(&self) -> &NetActivity {
        &self.activity
    }

    /// Stops Spotify-bound commands from leaving the process; artwork and
    /// shutdown still work. Used by the demo mode and by headless tests.
    #[cfg_attr(not(any(test, feature = "demo")), allow(dead_code))]
    pub fn set_offline(&mut self, offline: bool) {
        self.offline = offline;
    }

    pub fn send(&self, command: Command) {
        if self.offline
            && !matches!(
                command,
                Command::Accent { .. }
                    | Command::Shutdown
                    | Command::CheckForUpdates { .. }
                    | Command::InspectUpdate
                    | Command::DownloadUpdate { .. }
                    | Command::InstallUpdate { .. }
            )
        {
            return;
        }
        observe::sent(&command);
        let kind = command.kind();
        if self.commands.send(command).is_err() {
            observe::command_lost(kind);
        }
    }

    /// Construct the native dialog on the UI thread, then await and read it
    /// on the runtime. AppKit requires its window lookup on the main thread.
    pub fn choose_playlist_cover(&self, id: String, request: u64) {
        if self.offline {
            return;
        }
        let selected = rfd::AsyncFileDialog::new()
            .set_title("Choose playlist cover")
            .add_filter("JPEG or PNG image", &["jpg", "jpeg", "png"])
            .pick_file();
        self.send(Command::ChoosePlaylistCover {
            id,
            request,
            selected: Box::pin(selected),
        });
    }

    pub fn api(&self, request: ApiRequest) {
        #[cfg(test)]
        if matches!(
            request,
            ApiRequest::AlbumQueueTracks { .. }
                | ApiRequest::AddToQueue { .. }
                | ApiRequest::AddManyToQueue { .. }
        ) {
            self.queue_requests.lock().unwrap().push(request.clone());
        }
        #[cfg(test)]
        if matches!(
            request,
            ApiRequest::Remote {
                action: RemoteAction::Play,
                ..
            } | ApiRequest::ShufflePlay { .. }
        ) {
            self.remote_play_requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(request.clone());
        }
        #[cfg(test)]
        if matches!(
            request,
            ApiRequest::Remote {
                action: RemoteAction::Shuffle,
                ..
            }
        ) {
            self.remote_shuffle_requests
                .lock()
                .unwrap()
                .push(request.clone());
        }
        #[cfg(test)]
        if matches!(
            request,
            ApiRequest::AddToPlaylist { .. }
                | ApiRequest::CheckPlaylistDuplicates { .. }
                | ApiRequest::UpdatePlaylist { .. }
        ) {
            self.playlist_add_requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(request.clone());
        }
        #[cfg(test)]
        if let ApiRequest::PlaylistItems {
            id,
            offset,
            generation,
        } = &request
        {
            self.playlist_item_requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push((id.clone(), *offset, *generation));
        }
        #[cfg(test)]
        if let ApiRequest::PlaylistSample {
            id,
            offset,
            generation,
        } = &request
        {
            self.playlist_sample_requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push((id.clone(), *offset, *generation));
        }
        #[cfg(test)]
        if let ApiRequest::HomeEpisodes { shows, generation } = &request {
            self.home_episode_requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push((
                    shows.iter().map(|show| show.id.clone()).collect(),
                    *generation,
                ));
        }
        self.send(Command::Api(request));
    }

    #[cfg(test)]
    pub(crate) fn take_home_episode_requests(&self) -> Vec<(Vec<String>, u64)> {
        std::mem::take(
            &mut *self
                .home_episode_requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }

    #[cfg(test)]
    pub fn take_playlist_item_requests(&self) -> Vec<(String, u32, u64)> {
        std::mem::take(
            &mut *self
                .playlist_item_requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }

    #[cfg(test)]
    pub fn take_playlist_sample_requests(&self) -> Vec<(String, u32, u64)> {
        std::mem::take(
            &mut *self
                .playlist_sample_requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }

    #[cfg(test)]
    pub fn take_playlist_add_requests(&self) -> Vec<ApiRequest> {
        std::mem::take(
            &mut *self
                .playlist_add_requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }

    #[cfg(test)]
    pub fn take_remote_play_requests(&self) -> Vec<ApiRequest> {
        std::mem::take(
            &mut *self
                .remote_play_requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }

    #[cfg(test)]
    pub fn take_remote_shuffle_requests(&self) -> Vec<ApiRequest> {
        std::mem::take(&mut *self.remote_shuffle_requests.lock().unwrap())
    }

    #[cfg(test)]
    pub(crate) fn take_queue_requests(&self) -> Vec<ApiRequest> {
        std::mem::take(&mut *self.queue_requests.lock().unwrap())
    }

    #[cfg(test)]
    pub(crate) fn take_queued_tracks(&self) -> Vec<String> {
        std::mem::take(&mut *self.queued_tracks.lock().unwrap())
    }

    #[cfg(test)]
    pub(crate) fn take_player_commands(&self) -> Vec<PlayerCommand> {
        std::mem::take(&mut *self.player_commands.lock().unwrap())
    }

    pub fn player(&self, command: PlayerCommand) {
        #[cfg(test)]
        self.player_commands.lock().unwrap().push(command.clone());
        #[cfg(test)]
        if let PlayerCommand::AddToQueue(uri) = &command {
            self.queued_tracks.lock().unwrap().push(uri.clone());
        }
        self.send(Command::Player(command));
    }

    pub(crate) fn album_types(&self, uris: Vec<String>) {
        #[cfg(test)]
        self.album_type_requests
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(uris.clone());
        self.send(Command::AlbumTypes(uris));
    }

    #[cfg(test)]
    pub(crate) fn take_album_type_requests(&self) -> Vec<Vec<String>> {
        std::mem::take(
            &mut *self
                .album_type_requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }

    pub fn poll(&self) -> Vec<Event> {
        self.events.try_iter().collect()
    }

    pub fn art(&self) -> &ArtLoader {
        &self.art
    }

    pub fn shutdown(&mut self) {
        self.send(Command::Shutdown);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AlbumTypeRequest {
    uri: String,
    session_generation: u64,
    engine_generation: u64,
}

#[derive(Default)]
struct AlbumTypeLookup {
    session_generation: u64,
    engine_generation: u64,
    pending: VecDeque<String>,
    seen: HashSet<String>,
    active: Option<AlbumTypeRequest>,
}

impl AlbumTypeLookup {
    fn enqueue(&mut self, signed_in: bool, premium: Option<bool>, uris: Vec<String>) {
        if !signed_in || premium == Some(false) {
            return;
        }
        for uri in uris {
            if self.pending.len() + usize::from(self.active.is_some()) >= MAX_PENDING_ALBUM_TYPES {
                break;
            }
            if self.seen.insert(uri.clone()) {
                self.pending.push_back(uri);
            }
        }
    }

    fn next(&mut self) -> Option<AlbumTypeRequest> {
        if self.active.is_some() {
            return None;
        }
        let request = AlbumTypeRequest {
            uri: self.pending.pop_front()?,
            session_generation: self.session_generation,
            engine_generation: self.engine_generation,
        };
        self.active = Some(request.clone());
        Some(request)
    }

    fn finish(&mut self, request: &AlbumTypeRequest) -> bool {
        if self.active.as_ref() != Some(request) {
            return false;
        }
        self.active = None;
        true
    }

    fn retire_engine(&mut self) {
        self.engine_generation = self.engine_generation.wrapping_add(1);
        self.active = None;
    }

    fn requeue_active_for_new_engine(&mut self) {
        self.engine_generation = self.engine_generation.wrapping_add(1);
        if let Some(request) = self.active.take() {
            self.pending.push_front(request.uri);
        }
    }

    fn clear_engine_work(&mut self) {
        self.retire_engine();
        self.pending.clear();
        self.seen.clear();
    }

    fn reset_session(&mut self) {
        self.session_generation = self.session_generation.wrapping_add(1);
        self.clear_engine_work();
    }
}

struct Worker {
    dirs: AppDirs,
    credentials: CredentialStore,
    web_tokens: [Option<Arc<WebTokens>>; 2],
    playback_grant: Option<Credentials>,
    restore_pending: [bool; 3],
    restoring_proxy: bool,
    waiting_for_proxy: VecDeque<Command>,
    spotify_restore_started: bool,
    proxy_revision: u64,
    authorization_attempt: u64,
    session: watch::Sender<u64>,
    engine_config: EngineConfig,
    /// The resolved proxy handed to the current or in-flight engine connection.
    engine_proxy: Option<reqwest::Url>,
    web_client_id: Option<String>,
    http: Http,
    api: Arc<ApiGateway>,
    background_api: Arc<tokio::sync::Semaphore>,
    art: ArtLoader,
    events: std::sync::mpsc::Sender<Event>,
    commands: mpsc::UnboundedSender<Command>,
    waker: Waker,
    engine: Option<Arc<Engine>>,
    /// What the engine is heard at, so the engine that replaces it starts
    /// there. `engine_config` alone knows only the level the app launched
    /// with.
    heard: Option<Heard>,
    /// A rootlist fetch asked for before the engine existed, to run once it
    /// does. The rootlist carries invitation edit permissions, which no
    /// other request reports.
    rootlist_pending: bool,
    album_type_lookup: AlbumTypeLookup,
    /// Saved shows waiting for the streaming session to say which are audiobooks.
    audiobook_lookup: BTreeSet<String>,
    /// Radios asked for before the streaming session was ready, by seed.
    radio_waiting: BTreeMap<String, u64>,
    /// True while a playback grant or engine connection is in flight, so a
    /// second attempt does not pile up.
    engine_busy: bool,
    search_tasks: Vec<tokio::task::AbortHandle>,
    /// A user changed engine-affecting settings while the current connection
    /// attempt was in flight. Its result is stale and must not be installed.
    engine_restart_pending: bool,
    signed_in: bool,
    /// The plan, once the Web API has answered.
    premium: Option<bool>,
    cancel_signin: Option<watch::Sender<bool>>,
    authorizing_source: Option<ApiSource>,
    pending_authorization: Option<ApiSource>,
    reconnects: Vec<Instant>,
    /// What the engine was playing when it went down, to load again once
    /// the next one is up.
    resume: Option<PlaybackResume>,
    /// A pickup in flight: the load to repeat and how often it was tried.
    resume_verify: Option<(PlaybackResume, u8)>,
    /// What telemetry follows across the engine's lifecycle.
    lifecycle: observe::Lifecycle,
}

impl Worker {
    #[allow(clippy::too_many_arguments)]
    fn new(
        dirs: AppDirs,
        engine_config: EngineConfig,
        web_client_id: Option<String>,
        http: Http,
        art: ArtLoader,
        activity: Arc<NetActivity>,
        events: std::sync::mpsc::Sender<Event>,
        commands: mpsc::UnboundedSender<Command>,
        waker: Waker,
    ) -> Self {
        Self {
            #[cfg(not(test))]
            credentials: CredentialStore::new(dirs.clone()),
            #[cfg(test)]
            credentials: CredentialStore::in_memory(dirs.clone()),
            web_tokens: [None, None],
            playback_grant: None,
            restore_pending: [false; 3],
            restoring_proxy: false,
            waiting_for_proxy: VecDeque::new(),
            spotify_restore_started: false,
            proxy_revision: 0,
            authorization_attempt: 0,
            session: watch::channel(0).0,
            dirs,
            engine_config,
            engine_proxy: None,
            web_client_id,
            api: Arc::new(ApiGateway::new(http.clone(), activity)),
            background_api: Arc::new(tokio::sync::Semaphore::new(4)),
            http,
            art,
            events,
            commands,
            waker,
            engine: None,
            heard: None,
            rootlist_pending: false,
            album_type_lookup: AlbumTypeLookup::default(),
            audiobook_lookup: BTreeSet::new(),
            radio_waiting: BTreeMap::new(),
            engine_busy: false,
            search_tasks: Vec::new(),
            engine_restart_pending: false,
            signed_in: false,
            premium: None,
            cancel_signin: None,
            authorizing_source: None,
            pending_authorization: None,
            reconnects: Vec::new(),
            resume: None,
            resume_verify: None,
            lifecycle: observe::Lifecycle::default(),
        }
    }

    fn emit(&self, event: Event) {
        let _ = self.events.send(event);
        self.waker.wake();
    }

    /// Build before replacing either transport configuration. A rejected
    /// change leaves the existing connection in place and is reported to the UI.
    fn apply_proxy(&mut self, proxy: ProxyConfig) -> Result<bool, String> {
        let client = crate::http::build_client(&proxy)?;
        let restart = self.engine_proxy != proxy.librespot_url();
        self.http.replace(client);
        self.engine_config.proxy = proxy;
        Ok(restart)
    }

    fn change_proxy(&mut self, request: u64, proxy: ProxyConfig, sign_in: bool) {
        let result = self.apply_proxy(proxy.clone());
        if result.as_ref().is_ok_and(|restart| *restart) {
            self.lifecycle.reason = Some("proxy");
            self.replace_engine();
        }
        let applied = result.is_ok();
        if applied {
            self.proxy_revision = request;
        }
        self.emit(Event::ProxyApplied {
            request,
            config: proxy.clone(),
            result,
        });
        if applied {
            self.persist_proxy_password(&proxy);
            self.finish_proxy_restore();
            if sign_in {
                self.sign_in();
            }
        }
    }

    fn persist_proxy_password(&mut self, proxy: &ProxyConfig) {
        if !matches!(proxy, ProxyConfig::Http(_) | ProxyConfig::Socks(_)) {
            // Off/System retain the saved manual password for later use.
            return;
        }
        let events = self.events.clone();
        let waker = self.waker.clone();
        let pending = if let Some(password) = proxy.password_record() {
            self.credentials.invalidate(CredentialSlot::Proxy);
            let lease = self.credentials.lease(CredentialSlot::Proxy);
            let saving = lease.save(StoredGrant::Proxy(password));
            (lease, saving, true)
        } else {
            if let Err(error) = self.credentials.revoke(CredentialSlot::Proxy) {
                self.emit(Event::ProxyStorageFailed(error));
                return;
            }
            // The durable revocation marker already prevents a failed delete
            // from restoring an old password. The UI can now scrub legacy JSON.
            self.emit(Event::ProxyPasswordStored);
            let lease = self.credentials.lease(CredentialSlot::Proxy);
            let deleting = lease.delete();
            (lease, deleting, false)
        };
        tokio::spawn(async move {
            let (lease, operation, saving) = pending;
            let result = operation.await;
            if !lease.current() {
                return;
            }
            match result {
                Ok(()) if saving => {
                    let _ = events.send(Event::ProxyPasswordStored);
                }
                Ok(()) => {}
                Err(error) if error != crate::credentials::Error::Stale => {
                    let _ = events.send(Event::ProxyStorageFailed(error));
                }
                Err(_) => {}
            }
            waker.wake();
        });
    }

    fn on_proxy_restored(
        &mut self,
        lease: CredentialLease,
        result: Result<crate::credentials::Loaded, crate::credentials::Error>,
    ) {
        if !lease.current() {
            return;
        }
        let (password, protected) = match result {
            Ok(loaded) => {
                if let Some(error) = loaded.warning {
                    self.emit(Event::ProxyStorageFailed(error));
                }
                let password = match loaded.grant {
                    Some(StoredGrant::Proxy(password)) => Some(password),
                    _ => None,
                };
                (password, loaded.warning.is_none())
            }
            Err(error) => {
                self.emit(Event::ProxyStorageFailed(error));
                (None, false)
            }
        };
        if self.proxy_revision == 0 {
            let mut config = self.engine_config.proxy.clone();
            if let Some(password) = &password {
                config.restore_password(password);
            }
            if let Err(error) = self.apply_proxy(config.clone()) {
                self.http.block(error.clone());
                config = ProxyConfig::Invalid(error.clone());
                self.engine_config.proxy = config.clone();
                self.emit(Event::Error(format!(
                    "Network configuration failed: {error}"
                )));
            }
            self.emit(Event::ProxyRestored { config, password });
        }
        if protected {
            self.emit(Event::ProxyPasswordStored);
        }
        self.finish_proxy_restore();
    }

    fn finish_proxy_restore(&mut self) {
        if !std::mem::take(&mut self.restoring_proxy) {
            return;
        }
        self.lifecycle.proxy_gate(self.waiting_for_proxy.len());
        self.restore_spotify_grants();
        for command in self.waiting_for_proxy.drain(..) {
            let _ = self.commands.send(command);
        }
    }

    async fn run(&mut self, mut commands: mpsc::UnboundedReceiver<Command>) {
        let (cache_writes, cache_write_receiver) = mpsc::channel(1);
        let cache_writer = tokio::spawn(store_playlist_caches(
            cache_write_receiver,
            self.events.clone(),
            self.waker.clone(),
        ));
        while let Some(command) = commands.recv().await {
            let received = Instant::now();
            let kind = command.kind();
            if self.restoring_proxy
                && !matches!(
                    &command,
                    Command::ProxyRestored { .. }
                        | Command::ApplyProxy { .. }
                        | Command::SignIn { .. }
                        | Command::SignOut
                        | Command::CancelSignIn
                        | Command::Shutdown
                )
            {
                observe::parked(kind, self.waiting_for_proxy.len() + 1);
                self.waiting_for_proxy.push_back(command);
                continue;
            }
            let waited = observe::waited(kind, received);
            let _handling = observe::handling(kind, received, waited);
            self.lifecycle.received(
                &command,
                kind,
                waited,
                commands.len(),
                self.engine.as_deref(),
                self.engine_busy,
            );
            match command {
                Command::OpenThemesFolder => {
                    let directory = self.dirs.config.join("themes");
                    let events = self.events.clone();
                    let waker = self.waker.clone();
                    tokio::task::spawn_blocking(move || {
                        if let Err(error) = std::fs::create_dir_all(&directory)
                            .and_then(|()| crate::opener::open(&directory))
                        {
                            let _ = events.send(Event::Error(format!(
                                "Couldn't open the themes folder: {error}"
                            )));
                            waker.wake();
                        }
                    });
                }
                Command::ProxyRestored { lease, result } => self.on_proxy_restored(lease, result),
                Command::CredentialsRestored {
                    slot,
                    lease,
                    result,
                } => self.on_credentials_restored(slot, lease, result),
                Command::CheckPlaylistCover {
                    id,
                    request,
                    cover,
                    images,
                } => {
                    let art = self.art.clone();
                    let events = self.events.clone();
                    let waker = self.waker.clone();
                    let mut session = self.session.subscribe();
                    tokio::spawn(async move {
                        let result = tokio::select! {
                            _ = session.changed() => return,
                            result = async {
                                let url = crate::api::models::pick_image(&images, u32::MAX)
                                    .ok_or_else(|| "No playlist artwork yet.".to_string())?;
                                let bytes = art.fetch(url).await?;
                                tokio::task::spawn_blocking(move || cover.matches_remote(&bytes))
                                    .await.map_err(|_| "Couldn't check playlist artwork.".to_string())?
                            } => result,
                        };
                        let _ = events.send(Event::PlaylistCoverChecked {
                            id,
                            request,
                            images,
                            result,
                        });
                        waker.wake();
                    });
                }
                Command::ChoosePlaylistCover {
                    id,
                    request,
                    selected,
                } => {
                    let events = self.events.clone();
                    let waker = self.waker.clone();
                    tokio::spawn(async move {
                        let selected = selected.await;
                        let result = match selected {
                            None => Ok(None),
                            Some(file) => tokio::task::spawn_blocking(move || {
                                crate::playlist_cover::read(file.path()).map(Some)
                            })
                            .await
                            .unwrap_or_else(|_| {
                                Err("Couldn't prepare that image. Try another file.".into())
                            }),
                        };
                        let _ = events.send(Event::PlaylistCoverChosen {
                            id,
                            request,
                            result,
                        });
                        waker.wake();
                    });
                }
                Command::Shutdown => break,
                Command::SignIn { request, config } => self.change_proxy(request, config, true),
                Command::CancelSignIn => {
                    self.authorization_attempt += 1;
                    if let Some(cancel) = self.cancel_signin.take() {
                        self.credentials.invalidate(
                            self.authorizing_source
                                .map_or(CredentialSlot::Playback, web_slot),
                        );
                        let _ = cancel.send(true);
                    }
                    if let Some(source) = self.authorizing_source.take()
                        && matches!(self.api.state(source), SessionState::Authorizing)
                    {
                        self.api.clear(source);
                    }
                    self.pending_authorization = None;
                }
                Command::SignOut => self.sign_out(),
                Command::AuthorizePlayback => self.authorize_playback(),
                Command::RestartEngine(mut config) => {
                    // Audio settings must not revert a proxy change whose UI
                    // acknowledgement was still in flight when this was clicked.
                    config.proxy = self.engine_config.proxy.clone();
                    observe::engine_restart(&self.engine_config, &config, self.engine.is_some());
                    self.engine_config = config;
                    self.lifecycle.reason = Some("settings");
                    self.replace_engine();
                }
                Command::ApplyProxy { request, config } => {
                    self.change_proxy(request, config, false)
                }
                Command::Player(command) => match &self.engine {
                    Some(engine) => {
                        let name = observe::player_kind(&command);
                        if let Err(error) = engine.command(command) {
                            observe::player_failed(name, "error", Some(&error), self.engine_busy);
                            self.emit(Event::Error(format!("Playback error: {error}")));
                        }
                    }
                    None => {
                        observe::player_failed(
                            observe::player_kind(&command),
                            "no_engine",
                            None,
                            self.engine_busy,
                        );
                        self.emit(Event::Error(
                            "Local playback isn't set up on this computer yet".into(),
                        ))
                    }
                },
                Command::Api(ApiRequest::Search { query, serial }) => self.search(query, serial),
                Command::Api(request) => {
                    self.dispatch(request);
                }
                Command::ApiFinished {
                    generation,
                    response,
                    expired,
                    shared_lease,
                    personal_lease,
                } => {
                    if generation != *self.session.borrow() {
                        continue;
                    }
                    if let Some(source) = expired {
                        let lease = if source == ApiSource::Shared {
                            shared_lease
                        } else {
                            personal_lease
                        };
                        if !lease.current() {
                            continue;
                        }
                        observe::grant_expired(source);
                        self.forget_web_grant(source);
                        if source == ApiSource::Personal {
                            self.emit(Event::WebApp { client_id: None });
                        } else {
                            self.signed_in = false;
                            self.emit(Event::Auth(AuthStatus::Failed(
                                "Your Spotify sign-in expired. Please sign in again.".into(),
                            )));
                        }
                    }
                    if let ApiResponse::Me(Ok(user)) = response.as_ref()
                        && self.signed_in
                        && self.api.account().as_ref().map(|account| account.as_str())
                            == Some(user.id.as_str())
                    {
                        self.on_account_checked(
                            user.product.as_deref().map(|product| product == "premium"),
                        );
                    }
                    self.emit(Event::Api(response));
                }
                Command::Accent { url } => self.accent(url),
                Command::WebSignedIn {
                    source,
                    token,
                    lease,
                    attempt,
                } => {
                    if lease.current()
                        && self.authorizing_source == Some(source)
                        && self.authorization_attempt == attempt
                    {
                        self.on_web_signed_in(source, *token);
                    } else if self.authorization_attempt == attempt {
                        self.finish_authorization(source);
                    }
                }
                Command::WebVerified {
                    source,
                    token,
                    user,
                    lease,
                    attempt,
                } => {
                    if lease.current() {
                        self.on_web_verified(source, *token, *user);
                    } else if self.authorization_attempt == attempt {
                        self.finish_authorization(source);
                    }
                }
                Command::WebVerificationFailed {
                    source,
                    lease,
                    attempt,
                    error,
                } => {
                    if lease.current() {
                        self.on_web_verification_failed(source, error);
                    }
                    if self.authorization_attempt == attempt {
                        self.finish_authorization(source);
                    }
                }
                Command::PlaybackAuthorized {
                    access_token,
                    lease,
                    attempt,
                } => {
                    if lease.current() {
                        self.on_playback_authorized(access_token);
                    } else {
                        self.finish_playback_authorization(attempt);
                    }
                }
                Command::EngineConnected {
                    session_generation,
                    engine,
                    error,
                    lease,
                } => {
                    if lease.current() && self.signed_in {
                        self.on_engine_connected(session_generation, *engine, error)
                    } else if let Some(engine) = *engine {
                        self.lifecycle.discarded("lease_changed");
                        engine.shutdown();
                    }
                }
                Command::SignInEnded { source, attempt } => {
                    if self.authorizing_source == Some(source)
                        && self.authorization_attempt == attempt
                    {
                        self.cancel_signin = None;
                        self.authorizing_source = None;
                        if matches!(self.api.state(source), SessionState::Authorizing) {
                            self.api.clear(source);
                        }
                        if let Some(pending) = self.pending_authorization.take() {
                            self.sign_in_source(pending);
                        }
                    }
                }
                Command::PlaybackAuthEnded { attempt } => {
                    self.finish_playback_authorization(attempt)
                }
                Command::Reconnect => self.reconnect_engine(),
                Command::DiscoverReceivers => self.discover_receivers(),
                Command::ActivateReceiver(receiver) => self.activate_receiver(*receiver),
                Command::CheckForUpdates { manual, source } => {
                    self.check_for_updates(manual, source)
                }
                Command::InspectUpdate => {
                    let proxy = self.engine_config.proxy.clone();
                    let events = self.events.clone();
                    let waker = self.waker.clone();
                    tokio::task::spawn_blocking(move || {
                        let result = crate::updates::updater(&proxy)
                            .map_err(|error| format!("{error:#}"))
                            .and_then(|updater| {
                                updater.installation().map_err(|error| error.to_string())
                            });
                        let _ = events.send(Event::UpdateSupport(result));
                        waker.wake();
                    });
                }
                Command::DownloadUpdate { release, source } => {
                    let proxy = self.engine_config.proxy.clone();
                    let events = self.events.clone();
                    let waker = self.waker.clone();
                    tokio::task::spawn_blocking(move || {
                        let started = Instant::now();
                        let result = crate::updates::updater(&proxy)
                            .and_then(|updater| {
                                updater
                                    .with_source(source)
                                    .download(&release, |received, total| {
                                        let _ =
                                            events.send(Event::UpdateProgress { received, total });
                                        waker.wake();
                                    })
                            })
                            .map(Box::new)
                            .map_err(|error| format!("{error:#}"));
                        observe::job("update_download", started, result.is_ok(), 1);
                        let _ = events.send(Event::UpdateDownloaded(result));
                        waker.wake();
                    });
                }
                Command::InstallUpdate {
                    prepared,
                    arguments,
                } => {
                    let proxy = self.engine_config.proxy.clone();
                    let events = self.events.clone();
                    let waker = self.waker.clone();
                    tokio::task::spawn_blocking(move || {
                        let result = crate::updates::updater(&proxy)
                            .and_then(|updater| updater.handoff(*prepared, arguments))
                            .map_err(|error| format!("{error:#}"));
                        let _ = events.send(Event::UpdateInstalling(result));
                        waker.wake();
                    });
                }
                Command::Lyrics(request) => self.fetch_lyrics(*request),
                Command::Rootlist => self.fetch_rootlist(),
                Command::RootlistFinished { generation, result } => {
                    self.on_rootlist_finished(generation, result);
                }
                Command::VerifyResume => self.verify_resume(),
                Command::LoadPlaylistCache { id, generation } => {
                    self.load_playlist_cache(id, generation)
                }
                Command::StorePlaylistCache {
                    id,
                    generation,
                    snapshot,
                    rows,
                    total,
                    next_offset,
                } => {
                    if let Some(account) = self.api.account() {
                        let account_id = account.as_str().to_string();
                        let path = self
                            .dirs
                            .account_playlist_cache_dir(&account_id)
                            .join(format!("{id}.json"));
                        if let Err(error) = cache_writes.try_send(PlaylistCacheWrite {
                            path,
                            account_id,
                            id,
                            generation,
                            snapshot,
                            rows,
                            total,
                            next_offset,
                        }) {
                            let write = error.into_inner();
                            log::warn!(
                                "unable to queue playlist cache {}: writer unavailable",
                                write.path.display()
                            );
                            self.emit(Event::PlaylistCacheStored {
                                account_id: write.account_id,
                                id: write.id,
                                generation: write.generation,
                                snapshot: write.snapshot,
                                success: false,
                            });
                        }
                    } else {
                        self.emit(Event::PlaylistCacheStored {
                            account_id: String::new(),
                            id,
                            generation,
                            snapshot,
                            success: false,
                        });
                    }
                }
                Command::UserNames(ids) => self.fetch_user_names(ids),
                Command::LoadLikedSongsCache { generation } => {
                    if let Some(account) = self.api.account() {
                        let account_id = account.as_str().to_string();
                        let path = self.dirs.liked_songs_cache_file(&account_id);
                        let events = self.events.clone();
                        let waker = self.waker.clone();
                        tokio::spawn(async move {
                            let cache = crate::liked::read(&path, &account_id).await;
                            let _ = events.send(Event::LikedSongsCache {
                                account_id,
                                generation,
                                cache,
                            });
                            waker.wake();
                        });
                    }
                }
                Command::StoreLikedSongsCache(cache) => {
                    if self
                        .api
                        .account()
                        .is_some_and(|account| account.as_str() == cache.account_id)
                    {
                        let path = self.dirs.liked_songs_cache_file(&cache.account_id);
                        // Awaited on the loop, so every command waits for it.
                        let started = Instant::now();
                        let written = crate::liked::write(&path, &cache).await;
                        observe::job("liked_cache_write", started, written.is_ok(), 0);
                        if let Err(error) = written {
                            log::warn!("unable to store Liked Songs cache: {error}");
                        }
                    }
                }
                Command::AlbumTypes(uris) => self.fetch_album_types(uris),
                Command::AudiobookShows(uris) => {
                    self.audiobook_lookup.extend(uris);
                    self.start_audiobook_lookup();
                }
                Command::Radio { seed, generation } => {
                    self.radio_waiting.insert(seed, generation);
                    self.start_radio();
                }
                Command::RadioResolved {
                    session_generation,
                    seed,
                    generation,
                    result,
                } => {
                    if self.signed_in && session_generation == *self.session.borrow() {
                        self.emit(Event::Radio {
                            seed,
                            generation,
                            result,
                        });
                    }
                }
                Command::AudiobookShowsResolved {
                    session_generation,
                    audiobooks,
                } => {
                    if self.signed_in && session_generation == *self.session.borrow() {
                        self.emit(Event::AudiobookShows(audiobooks));
                    }
                }
                Command::AlbumTypeResolved {
                    uri,
                    session_generation,
                    engine_generation,
                    result,
                } => self.on_album_type_resolved(
                    AlbumTypeRequest {
                        uri,
                        session_generation,
                        engine_generation,
                    },
                    result,
                ),
                Command::ConfigurePersonalWebApp(client_id) => {
                    self.configure_personal_web_app(client_id)
                }
            }
        }
        if let Some(engine) = self.engine.take() {
            engine.shutdown();
        }
        drop(cache_writes);
        let _ = cache_writer.await;
    }

    // ---- Web API sign-in --------------------------------------------------

    fn restore_session(&mut self) {
        self.restoring_proxy = true;
        self.lifecycle.proxy_restoring();
        let lease = self.credentials.lease(CredentialSlot::Proxy);
        let commands = self.commands.clone();
        tokio::spawn(async move {
            let result = lease.load().await;
            let _ = commands.send(Command::ProxyRestored { lease, result });
        });
    }

    fn restore_spotify_grants(&mut self) {
        if std::mem::replace(&mut self.spotify_restore_started, true) {
            return;
        }
        self.restore_pending = [true; 3];
        self.api
            .set_state(ApiSource::Shared, SessionState::Authorizing);
        if self.web_client_id.is_some() {
            self.api
                .set_state(ApiSource::Personal, SessionState::Authorizing);
        }
        for slot in CredentialSlot::SPOTIFY {
            let lease = self.credentials.lease(slot);
            let commands = self.commands.clone();
            tokio::spawn(async move {
                let result = lease.load().await;
                let _ = commands.send(Command::CredentialsRestored {
                    slot,
                    lease,
                    result,
                });
            });
        }
    }

    fn on_credentials_restored(
        &mut self,
        slot: CredentialSlot,
        lease: CredentialLease,
        result: Result<crate::credentials::Loaded, crate::credentials::Error>,
    ) {
        if slot == CredentialSlot::Proxy {
            return;
        }
        if !lease.current() {
            return;
        }
        self.restore_pending[slot.index()] = false;
        let grant = match result {
            Ok(loaded) => {
                if let Some(error) = loaded.warning {
                    self.emit(Event::Error(error.to_string()));
                }
                loaded.grant
            }
            Err(error) => {
                self.emit(Event::Error(error.to_string()));
                None
            }
        };
        match grant {
            Some(StoredGrant::Playback(grant)) => {
                self.playback_grant = Some(grant);
                self.resume_engine();
            }
            Some(StoredGrant::Web(token)) => {
                let source = if slot == CredentialSlot::Shared {
                    ApiSource::Shared
                } else {
                    ApiSource::Personal
                };
                if source == ApiSource::Shared
                    || self.web_client_id.as_deref() == Some(token.client_id.as_str())
                {
                    if token.has_scopes(crate::auth::WEB_SCOPES) {
                        if !self.signed_in {
                            self.emit(Event::Auth(AuthStatus::Connecting));
                        }
                        self.on_web_signed_in(source, token);
                    } else {
                        // Signing in renews only the shared app's grant, so a
                        // personal app has to be authorized again where it was
                        // set up (#634).
                        let message = if source == ApiSource::Shared {
                            "Spotify permissions changed. Sign in again."
                        } else {
                            "Spotify permissions changed for your personal app. \
                             Authorize it again in Settings, under Account."
                        };
                        self.emit(Event::Error(message.into()));
                    }
                }
            }
            None | Some(StoredGrant::Proxy(_)) => {}
        }
        if slot != CredentialSlot::Playback && self.web_tokens[slot.index()].is_none() {
            self.api.clear(if slot == CredentialSlot::Shared {
                ApiSource::Shared
            } else {
                ApiSource::Personal
            });
        }
        if !self.restore_pending.iter().any(|pending| *pending)
            && !self.signed_in
            && self.web_tokens.iter().all(Option::is_none)
        {
            self.emit(Event::Auth(AuthStatus::SignedOut));
        }
    }

    fn storage_notice(
        &self,
        lease: CredentialLease,
    ) -> Arc<dyn Fn(crate::credentials::Error) + Send + Sync> {
        let events = self.events.clone();
        let waker = self.waker.clone();
        Arc::new(move |error| {
            if lease.current() && error != crate::credentials::Error::Stale {
                let _ = events.send(Event::Error(error.to_string()));
                waker.wake();
            }
        })
    }

    fn on_web_signed_in(&mut self, source: ApiSource, token: crate::auth::StoredToken) {
        let lease = self.credentials.lease(web_slot(source));
        let tokens = WebTokens::new(
            self.http.clone(),
            token.clone(),
            lease.clone(),
            source,
            self.storage_notice(lease.clone()),
        );
        self.web_tokens[web_slot(source).index()] = Some(tokens.clone());
        self.api
            .begin_verification(source, TokenProvider::Web(tokens));
        let client = self.api.verification_client(source);
        let gateway = Arc::clone(&self.api);
        let commands = self.commands.clone();
        let attempt = self.authorization_attempt;
        tokio::spawn(async move {
            let mut wait = Duration::from_secs(2);
            let verifying = Instant::now();
            let mut tries = 0u32;
            let error = loop {
                if !lease.current() {
                    let _ = commands.send(Command::SignInEnded { source, attempt });
                    return;
                }
                tries += 1;
                match client.me().await {
                    Ok(user) => {
                        observe::verified(source, tries, verifying, None);
                        let _ = commands.send(Command::WebVerified {
                            source,
                            token: Box::new(token),
                            user: Box::new(user),
                            lease: lease.clone(),
                            attempt,
                        });
                        return;
                    }
                    Err(error @ ApiError::SignInExpired { .. }) => break error,
                    Err(error) if error.status().is_some_and(|status| status < 500) => break error,
                    Err(error) => {
                        log::warn!("Spotify sign-in verification will retry: {error}");
                        observe::verify_retry(source, tries, wait, &error);
                        tokio::time::sleep(wait).await;
                        wait = (wait * 2).min(Duration::from_secs(60));
                        if !matches!(gateway.state(source), SessionState::Authorizing) {
                            let _ = commands.send(Command::SignInEnded { source, attempt });
                            return;
                        }
                    }
                }
            };
            observe::verified(source, tries, verifying, Some(&error));
            if !lease.current() {
                let _ = commands.send(Command::SignInEnded { source, attempt });
                return;
            }
            let _ = commands.send(Command::WebVerificationFailed {
                source,
                lease,
                attempt,
                error,
            });
        });
    }

    fn forget_web_grant(&mut self, source: ApiSource) {
        let slot = web_slot(source);
        if let Err(error) = self.credentials.revoke(slot) {
            self.emit(Event::Error(error.to_string()));
        }
        self.delete_stored_grant(slot);
        self.web_tokens[slot.index()] = None;
        self.api.clear(source);
    }

    fn on_web_verification_failed(&mut self, source: ApiSource, error: ApiError) {
        if matches!(error, ApiError::SignInExpired { .. }) {
            // A rejected refresh grant cannot restore a session next time.
            // Forget only this grant and ask for a fresh browser approval.
            self.forget_web_grant(source);
        } else {
            self.api.clear(source);
        }
        let message = match source {
            ApiSource::Shared => format!("Shared Spotify sign-in failed: {error}"),
            ApiSource::Personal => format!("Personal app authorization failed: {error}"),
        };
        let other_ready = match source {
            ApiSource::Shared => self.api.personal_ready(),
            ApiSource::Personal => matches!(
                self.api.state(ApiSource::Shared),
                SessionState::Ready { .. }
            ),
        };
        observe::verification_failed(source, &error, source == ApiSource::Shared || !other_ready);
        if source == ApiSource::Shared || !other_ready {
            self.signed_in = false;
            self.emit(Event::Auth(AuthStatus::Failed(message.clone())));
        }
        self.emit(Event::Error(message));
    }

    fn on_web_verified(&mut self, source: ApiSource, token: crate::auth::StoredToken, user: User) {
        // Free text must never carry the account's id or name.
        crate::telemetry::private_term(&user.id);
        if let Some(name) = user.display_name.as_deref() {
            crate::telemetry::private_term(name);
        }
        if !matches!(self.api.state(source), SessionState::Authorizing)
            || source == ApiSource::Personal
                && self.web_client_id.as_deref() != Some(token.client_id.as_str())
        {
            return;
        }
        if let Err(error) = self.api.install(source, AccountId::new(user.id.clone())) {
            self.api.clear(source);
            if source == ApiSource::Shared {
                self.signed_in = false;
                self.emit(Event::Auth(AuthStatus::Failed(error.to_string())));
            }
            self.emit(Event::Error(error.to_string()));
            self.finish_authorization(source);
            return;
        }
        if let Some(tokens) = self.web_tokens[web_slot(source).index()].clone() {
            let notice = self.storage_notice(self.credentials.lease(web_slot(source)));
            tokio::spawn(async move {
                if let Err(error) = tokens.remember().await {
                    notice(error);
                }
            });
        }
        match source {
            ApiSource::Shared => {
                if !self.signed_in {
                    self.signed_in = true;
                    self.emit(Event::Auth(AuthStatus::Connected {
                        username: user.name().to_string(),
                    }));
                }
                self.emit(Event::Api(Box::new(ApiResponse::Me(Ok(user.clone())))));
                let premium = user.product.as_deref().map(|product| product == "premium");
                self.on_account_checked(premium);
            }
            ApiSource::Personal => {
                self.emit(Event::WebApp {
                    client_id: Some(token.client_id),
                });
                if !self.signed_in {
                    self.signed_in = true;
                    self.emit(Event::Auth(AuthStatus::Connected {
                        username: user.name().to_string(),
                    }));
                    self.emit(Event::Api(Box::new(ApiResponse::Me(Ok(user.clone())))));
                    let premium = user.product.as_deref().map(|product| product == "premium");
                    self.on_account_checked(premium);
                }
            }
        }
        self.finish_authorization(source);
    }

    fn finish_authorization(&mut self, source: ApiSource) {
        if self.authorizing_source != Some(source) {
            return;
        }
        self.cancel_signin = None;
        self.authorizing_source = None;
        if let Some(pending) = self.pending_authorization.take() {
            self.sign_in_source(pending);
        }
    }

    fn finish_playback_authorization(&mut self, attempt: u64) {
        if self.authorization_attempt == attempt {
            self.cancel_signin = None;
            if let Some(pending) = self.pending_authorization.take() {
                self.sign_in_source(pending);
            }
        }
    }

    fn sign_in(&mut self) {
        self.sign_in_source(ApiSource::Shared);
    }

    fn sign_in_source(&mut self, source: ApiSource) {
        let http = match self.http.client() {
            Ok(http) => http,
            Err(error) => {
                self.emit(Event::Error(error));
                return;
            }
        };
        if self.cancel_signin.is_some() {
            return;
        }
        let grant = match source {
            ApiSource::Shared => crate::auth::Grant::shared_web_api(),
            ApiSource::Personal => {
                let Some(client_id) = self.web_client_id.as_deref() else {
                    return;
                };
                match crate::auth::Grant::personal_web_api(client_id) {
                    Ok(grant) => grant,
                    Err(error) => {
                        self.emit(Event::Error(error.to_string()));
                        return;
                    }
                }
            }
        };
        self.credentials.invalidate(web_slot(source));
        self.restore_pending[web_slot(source).index()] = false;
        let lease = self.credentials.lease(web_slot(source));
        self.authorization_attempt += 1;
        let attempt = self.authorization_attempt;
        let flow = crate::auth::begin(grant.clone());
        let (cancel_tx, cancel_rx) = watch::channel(false);
        self.cancel_signin = Some(cancel_tx);
        self.authorizing_source = Some(source);
        self.api.set_state(source, SessionState::Authorizing);
        if source == ApiSource::Shared {
            self.emit(Event::Auth(AuthStatus::WaitingForBrowser {
                url: flow.url.clone(),
            }));
        }
        let browser_url = flow.url.clone();
        tokio::task::spawn_blocking(move || {
            if let Err(error) = crate::opener::open(&browser_url) {
                log::warn!("unable to open a browser: {error}");
            }
        });
        let events = self.events.clone();
        let waker = self.waker.clone();
        let commands = self.commands.clone();
        tokio::spawn(async move {
            let result = async {
                let code =
                    crate::auth::wait_for_code(grant.redirect_port, &flow.state, cancel_rx).await?;
                let response =
                    crate::auth::exchange_code(&http, &grant, &code, &flow.verifier).await?;
                crate::auth::StoredToken::from_response(&grant.client_id, response, None)
            }
            .await;
            match result {
                Ok(token) => {
                    let _ = commands.send(Command::WebSignedIn {
                        source,
                        token: Box::new(token),
                        lease: lease.clone(),
                        attempt,
                    });
                }
                Err(error) => {
                    if lease.current() && source == ApiSource::Shared {
                        let _ = events.send(Event::Auth(AuthStatus::SignedOut));
                    }
                    let message = error.to_string();
                    if lease.current() && !message.contains("cancelled") {
                        let _ = events.send(Event::Error(format!("Sign-in failed: {message}")));
                    }
                    waker.wake();
                    let _ = commands.send(Command::SignInEnded { source, attempt });
                }
            }
        });
    }

    fn configure_personal_web_app(&mut self, client_id: Option<String>) {
        let authorization_in_flight = if let Some(cancel) = self.cancel_signin.as_ref() {
            let _ = cancel.send(true);
            self.credentials.invalidate(
                self.authorizing_source
                    .map_or(CredentialSlot::Playback, web_slot),
            );
            true
        } else {
            false
        };
        if let Err(error) = self.credentials.revoke(CredentialSlot::Personal) {
            self.emit(Event::Error(error.to_string()));
        }
        self.delete_stored_grant(CredentialSlot::Personal);
        self.web_tokens[CredentialSlot::Personal.index()] = None;
        observe::personal_app(client_id.is_some(), authorization_in_flight);
        self.web_client_id = client_id;
        self.api.clear(ApiSource::Personal);
        self.emit(Event::WebApp { client_id: None });
        if self.web_client_id.is_some() {
            if authorization_in_flight {
                self.pending_authorization = Some(ApiSource::Personal);
            } else {
                self.sign_in_source(ApiSource::Personal);
            }
        } else {
            self.pending_authorization = None;
        }
    }

    fn delete_stored_grant(&self, slot: CredentialSlot) {
        let lease = self.credentials.lease(slot);
        let notice = self.storage_notice(lease.clone());
        // Queue deletion before a new browser flow can enqueue a replacement.
        let pending = lease.delete();
        tokio::spawn(async move {
            if let Err(error) = pending.await {
                notice(error);
            }
        });
    }

    fn sign_out(&mut self) {
        observe::signed_out(self.engine.is_some());
        self.lifecycle.reason = None;
        self.spotify_restore_started = true;
        self.cancel_search();
        self.signed_in = false;
        self.rootlist_pending = false;
        self.session.send_modify(|generation| *generation += 1);
        self.authorization_attempt += 1;
        if let Err(error) = self.credentials.revoke_spotify() {
            self.emit(Event::Error(error.to_string()));
        }
        self.restore_pending = [false; 3];
        self.web_tokens = [None, None];
        self.playback_grant = None;
        self.engine_busy = false;
        self.premium = None;
        self.resume = None;
        self.resume_verify = None;
        self.album_type_lookup.reset_session();
        self.audiobook_lookup.clear();
        self.radio_waiting.clear();
        self.carry_volume();
        if let Some(engine) = self.engine.take() {
            engine.shutdown();
        }
        if let Some(cancel) = self.cancel_signin.take() {
            let _ = cancel.send(true);
        }
        self.authorizing_source = None;
        self.pending_authorization = None;
        self.api.clear_all();
        for slot in CredentialSlot::SPOTIFY {
            self.delete_stored_grant(slot);
        }
        self.emit(Event::Playback(LocalPlayback::Unavailable));
        self.emit(Event::Auth(AuthStatus::SignedOut));
    }

    // ---- local playback engine -------------------------------------------

    fn on_playback_authorized(&mut self, access_token: String) {
        let Some(credentials) = playback_credentials(self.api.account(), access_token) else {
            self.engine_busy = false;
            self.emit(Event::Playback(LocalPlayback::Failed(
                "Finish signing in to Spotify before enabling playback.".into(),
            )));
            return;
        };
        self.lifecycle.reason = Some("authorized");
        self.connect_engine(credentials);
    }

    fn engine_notify(&self) -> crate::player::Notify {
        let events = self.events.clone();
        let commands = self.commands.clone();
        let waker = self.waker.clone();
        let lease = self.credentials.lease(CredentialSlot::Playback);
        let engine_seq = self.lifecycle.engine_seq;
        Arc::new(move |event| {
            if !lease.current() {
                observe::engine_event(engine_seq, false);
                return;
            }
            observe::engine_event(engine_seq, true);
            match event {
                EngineEvent::State(state) => {
                    let _ = events.send(Event::Local(Box::new(state)));
                    waker.wake();
                }
                EngineEvent::SessionEnded => {
                    observe::session_lost(engine_seq);
                    let _ = commands.send(Command::Reconnect);
                }
            }
        })
    }

    /// Bring the engine up from a credential stored by a previous playback
    /// authorization, if there is one. Silent when there is nothing to resume.
    fn resume_engine(&mut self) {
        if !self.signed_in
            || self.engine.is_some()
            || self.engine_busy
            || self.premium == Some(false)
        {
            return;
        }
        if let Some(credentials) = self.playback_grant.clone() {
            if credentials.username.as_deref()
                != self.api.account().as_ref().map(|account| account.as_str())
            {
                self.emit(Event::Playback(LocalPlayback::Failed(
                    "Stored playback belongs to another Spotify account. Enable playback again."
                        .into(),
                )));
                return;
            }
            self.connect_engine(credentials);
        }
    }

    /// Replace the engine after a user-initiated change (audio settings or
    /// an HTTP proxy). Does not count toward the drop limiter: flipping a
    /// setting is not the session falling over.
    fn replace_engine(&mut self) {
        if !self.signed_in {
            self.lifecycle.reason = None;
            return;
        }
        let deferred = defer_engine_replace(self.engine_busy, &mut self.engine_restart_pending);
        self.lifecycle.replacing(deferred, self.engine.is_some());
        if deferred {
            return;
        }
        self.take_engine_for_resume();
        self.resume_engine();
    }

    /// Reconnect the engine after its session dropped on its own. Whatever
    /// was playing comes back at the same spot on the new session, so a
    /// dropped connection is a pause of a few seconds rather than silence.
    /// Six drops in ten minutes stop the loop so a flapping session cannot
    /// sit there reconnecting forever.
    fn reconnect_engine(&mut self) {
        let reason = observe::reconnect_reason();
        if !self.signed_in {
            self.lifecycle
                .reconnect(reason, "signed_out", self.reconnects.len());
            return;
        }
        if self.engine_busy {
            self.lifecycle
                .reconnect(reason, "engine_busy", self.reconnects.len());
            return;
        }
        let now = Instant::now();
        if session_drops_exhausted(&mut self.reconnects, now) {
            self.lifecycle
                .reconnect(reason, "exhausted", self.reconnects.len());
            self.take_engine_for_resume();
            self.lifecycle.reason = None;
            self.resume = None;
            self.emit(Event::Playback(LocalPlayback::Failed(
                "Local playback keeps dropping. Re-enable it from Settings.".into(),
            )));
            return;
        }
        self.reconnects.push(now);
        log::info!(
            "local playback session ended; reconnecting ({} of {RECONNECT_LIMIT} in ten minutes)",
            self.reconnects.len()
        );
        self.lifecycle
            .reconnect(reason, "reconnecting", self.reconnects.len());
        self.replace_engine();
    }

    /// Keeps the level being heard for the next engine, and lets what the
    /// engine was heard at go with it, so a level from an engine that is
    /// gone never overrides a setting that arrives after it.
    fn carry_volume(&mut self) {
        if let Some(heard) = self.heard.take() {
            self.engine_config.initial_volume = heard.level();
        }
    }

    /// Takes the engine down and keeps what its replacement picks up: the
    /// playback, and the level being heard. A session that drops on its own
    /// comes back at the level it was heard at, not at the level the app
    /// launched with.
    fn take_engine_for_resume(&mut self) {
        self.resume_verify = None;
        self.album_type_lookup.requeue_active_for_new_engine();
        self.carry_volume();
        if let Some(engine) = self.engine.take() {
            self.resume = engine.resume_point();
            engine.shutdown();
            self.lifecycle
                .teardown(self.resume.as_ref(), self.engine_config.initial_volume);
        }
    }

    /// Start (or re-enter) the playback authorization in the browser. This is
    /// a distinct grant from the Web API sign-in: it uses Spotify's streaming
    /// client identity, the one librespot can play with.
    fn authorize_playback(&mut self) {
        let http = match self.http.client() {
            Ok(http) => http,
            Err(error) => {
                self.emit(Event::Error(error));
                return;
            }
        };
        if self.engine_busy || self.cancel_signin.is_some() {
            return;
        }
        if self.premium == Some(false) {
            self.emit(Event::Playback(LocalPlayback::Failed(
                PREMIUM_NEEDED.into(),
            )));
            return;
        }
        self.credentials.invalidate(CredentialSlot::Playback);
        self.authorization_attempt += 1;
        let attempt = self.authorization_attempt;
        let lease = self.credentials.lease(CredentialSlot::Playback);
        let grant = crate::auth::Grant::playback();
        let flow = crate::auth::begin(grant.clone());
        let (cancel_tx, cancel_rx) = watch::channel(false);
        self.cancel_signin = Some(cancel_tx);
        self.emit(Event::Playback(LocalPlayback::Authorizing));
        let browser_url = flow.url.clone();
        tokio::task::spawn_blocking(move || {
            if let Err(error) = crate::opener::open(&browser_url) {
                log::warn!("unable to open a browser: {error}");
            }
        });

        let events = self.events.clone();
        let waker = self.waker.clone();
        let commands = self.commands.clone();
        tokio::spawn(async move {
            let result = async {
                let code =
                    crate::auth::wait_for_code(grant.redirect_port, &flow.state, cancel_rx).await?;
                crate::auth::exchange_code(&http, &grant, &code, &flow.verifier).await
            }
            .await;
            match result {
                Ok(token) => {
                    let _ = commands.send(Command::PlaybackAuthorized {
                        access_token: token.access_token,
                        lease: lease.clone(),
                        attempt,
                    });
                }
                Err(error) => {
                    let message = error.to_string();
                    if !lease.current() {
                        let _ = commands.send(Command::PlaybackAuthEnded { attempt });
                        return;
                    }
                    if message.contains("cancelled") {
                        let _ = events.send(Event::Playback(LocalPlayback::Unavailable));
                    } else {
                        let _ = events.send(Event::Playback(LocalPlayback::Failed(message)));
                    }
                    waker.wake();
                    let _ = commands.send(Command::PlaybackAuthEnded { attempt });
                }
            }
        });
    }

    /// Spawn an engine connection so a slow or hung librespot handshake can
    /// never block the command loop (this was the cause of the app freezing
    /// on "Connecting to Spotify"). Reusable credentials stay in memory until
    /// this worker receives the connected engine and persists them securely.
    fn connect_engine(&mut self, credentials: Credentials) {
        if let Err(error) = self.http.client() {
            self.emit(Event::Playback(LocalPlayback::Failed(error)));
            return;
        }
        if self.engine_busy {
            return;
        }
        if self.premium == Some(false) {
            self.emit(Event::Playback(LocalPlayback::Failed(
                PREMIUM_NEEDED.into(),
            )));
            return;
        }
        self.cancel_signin = None;
        self.engine_busy = true;
        // Before `engine_notify`, which tags the new engine's events with it.
        let (engine_seq, reason) = self.lifecycle.connecting();
        self.emit(Event::Playback(LocalPlayback::Connecting));
        let lease = self.credentials.lease(CredentialSlot::Playback);
        let config = self.engine_config.clone();
        let proxy = config.proxy.librespot_url();
        self.engine_proxy = proxy.clone();
        let notify = self.engine_notify();
        let events = self.events.clone();
        let commands = self.commands.clone();
        let waker = self.waker.clone();
        let session_generation = self.album_type_lookup.session_generation;
        tokio::spawn(async move {
            let started = Instant::now();
            let cache = match config.open_cache() {
                Ok(cache) => cache,
                Err(error) => {
                    observe::engine_connected(
                        (engine_seq, reason),
                        started,
                        None,
                        "cache_error",
                        Some(&error),
                        &config,
                    );
                    let _ = commands.send(Command::EngineConnected {
                        lease: lease.clone(),
                        session_generation,
                        engine: Box::new(None),
                        error: Some(error.to_string()),
                    });
                    return;
                }
            };
            let opened = started.elapsed();
            let attempt = connect_engine_with_deadline(Engine::connect(
                &config,
                proxy,
                credentials,
                cache,
                notify,
            ))
            .await;
            match &attempt {
                Ok(Ok(_)) => observe::engine_connected(
                    (engine_seq, reason),
                    started,
                    Some(opened),
                    "ok",
                    None,
                    &config,
                ),
                Ok(Err(error)) => observe::engine_connected(
                    (engine_seq, reason),
                    started,
                    Some(opened),
                    "error",
                    Some(error),
                    &config,
                ),
                Err(_) => observe::engine_connected(
                    (engine_seq, reason),
                    started,
                    Some(opened),
                    "timeout",
                    None,
                    &config,
                ),
            }
            let outcome = match attempt {
                Ok(Ok(engine)) => Command::EngineConnected {
                    lease: lease.clone(),
                    session_generation,
                    engine: Box::new(Some(engine)),
                    error: None,
                },
                Ok(Err(error)) => {
                    log::error!("engine connect failed: {error:#}");
                    Command::EngineConnected {
                        lease: lease.clone(),
                        session_generation,
                        engine: Box::new(None),
                        error: Some(friendly_connect_error(&error)),
                    }
                }
                Err(_) => Command::EngineConnected {
                    lease: lease.clone(),
                    session_generation,
                    engine: Box::new(None),
                    error: Some("Connecting to Spotify timed out".into()),
                },
            };
            let _ = commands.send(outcome);
            let _ = events;
            waker.wake();
        });
    }

    fn on_engine_connected(
        &mut self,
        session_generation: u64,
        engine: Option<Engine>,
        error: Option<String>,
    ) {
        if !self.signed_in || session_generation != self.album_type_lookup.session_generation {
            self.lifecycle.discarded("session_changed");
            if let Some(engine) = engine {
                engine.shutdown();
            }
            return;
        }
        self.engine_busy = false;
        if std::mem::take(&mut self.engine_restart_pending) {
            self.lifecycle.discarded("restart_pending");
            if let Some(engine) = engine {
                engine.shutdown();
            }
            // Keep `resume`: it belongs to the engine which was replaced,
            // not to this stale attempt. The newest config is already stored.
            self.resume_engine();
            return;
        }
        match engine {
            Some(engine) => {
                if let Some(grant) = engine.credentials() {
                    if !playback_account_matches(&grant, self.api.account()) {
                        engine.shutdown();
                        self.lifecycle.failed("account_mismatch", None);
                        self.emit(Event::Playback(LocalPlayback::Failed("Playback was authorized for another Spotify account. Enable playback again with the signed-in account.".into())));
                        return;
                    }
                    self.playback_grant = Some(grant.clone());
                    let lease = self.credentials.lease(CredentialSlot::Playback);
                    let notice = self.storage_notice(lease.clone());
                    let pending = lease.save(StoredGrant::Playback(grant));
                    tokio::spawn(async move {
                        if let Err(error) = pending.await {
                            notice(error);
                        }
                    });
                }
                let device_id = engine.device_id().to_string();
                let engine = Arc::new(engine);
                self.heard = Some(engine.heard());
                self.lifecycle.ready(
                    self.reconnects.len(),
                    self.resume.as_ref(),
                    self.rootlist_pending,
                    self.album_type_lookup.pending.len(),
                    self.audiobook_lookup.len() + self.radio_waiting.len(),
                );
                if let Some(spec) = self.resume.take() {
                    // Delay resume until Spirc finishes registering. An early
                    // load can return 400 and leave playback stopped. Verify
                    // the load and retry if needed.
                    self.resume_verify = Some((spec, 0));
                    self.schedule_resume_check(1_500);
                }
                self.engine = Some(engine);
                self.start_rootlist();
                self.reconnects.clear();
                self.emit(Event::Playback(LocalPlayback::Ready { device_id }));
                self.start_album_type_lookup();
                self.start_audiobook_lookup();
                self.start_radio();
            }
            None => {
                self.lifecycle.failed("error", error.as_deref());
                self.resume = None;
                let message = error.unwrap_or_else(|| "Local playback is unavailable".into());
                self.emit(Event::Playback(LocalPlayback::Failed(message)));
            }
        }
    }

    /// Starts the engine only for Premium accounts. librespot 0.8 calls
    /// `exit(1)` for Free accounts, which cannot be caught. If the plan is
    /// unknown, preserve the previous behavior and start the engine.
    fn on_account_checked(&mut self, premium: Option<bool>) {
        self.premium = premium;
        observe::premium_checked(premium, premium == Some(false) && self.engine.is_some());
        if premium == Some(false) {
            self.album_type_lookup.clear_engine_work();
            self.carry_volume();
            if let Some(engine) = self.engine.take() {
                engine.shutdown();
            }
            let credential_stored = self.playback_grant.is_some();
            if credential_stored {
                self.emit(Event::Playback(LocalPlayback::Failed(
                    PREMIUM_NEEDED.into(),
                )));
            }
            return;
        }
        self.resume_engine();
    }

    // ---- receivers on the local network -----------------------------------

    /// Browses for receivers Spotify's device list does not know about. The
    /// browse blocks, so it runs off the runtime's worker threads.
    fn discover_receivers(&self) {
        let events = self.events.clone();
        let waker = self.waker.clone();
        tokio::task::spawn_blocking(move || {
            let started = Instant::now();
            match crate::zeroconf::discover(std::time::Duration::from_secs(3))
                .and_then(crate::zeroconf::resolve_receivers)
            {
                Ok(receivers) => {
                    // Later log lines that quote a name are scrubbed.
                    for receiver in &receivers {
                        crate::telemetry::private_term(&receiver.name);
                    }
                    observe::receivers(started, Ok(receivers.len()));
                    let _ = events.send(Event::Receivers(receivers));
                    waker.wake();
                }
                Err(error) => {
                    observe::receivers(started, Err(&error));
                    log::debug!("no receivers found on the network: {error}")
                }
            }
        });
    }

    /// Sends the stored playback credential to a receiver so it can sign in.
    fn activate_receiver(&self, receiver: crate::zeroconf::Receiver) {
        let events = self.events.clone();
        let waker = self.waker.clone();
        let credentials = self
            .playback_grant
            .as_ref()
            .filter(|credentials| {
                credentials.username.as_deref()
                    == self.api.account().as_ref().map(|account| account.as_str())
            })
            .and_then(|credentials| crate::zeroconf::Credentials::from_playback(credentials).ok());
        let lease = self.credentials.lease(CredentialSlot::Playback);
        let has_credentials = credentials.is_some();
        tokio::task::spawn_blocking(move || {
            let started = Instant::now();
            let name = receiver.name.clone();
            let result = (|| -> Result<(), String> {
                if !lease.current() {
                    return Err("Sign-in changed before receiver activation.".into());
                }
                let credentials = credentials.ok_or_else(|| "Enable playback on this computer first, so there is an account to hand over".to_string())?;
                let http = reqwest::blocking::Client::builder()
                    // Receiver endpoints are private LAN addresses. Keep
                    // them off environment and OS proxies even when System
                    // mode is active.
                    .no_proxy()
                    .timeout(std::time::Duration::from_secs(8))
                    .build()
                    .map_err(|error| error.to_string())?;
                let info = crate::zeroconf::get_info(&http, &receiver)
                    .map_err(|error| error.to_string())?;
                if !lease.current() {
                    return Err("Sign-in changed before receiver activation.".into());
                }
                crate::zeroconf::add_user(&http, &receiver, &info, &credentials, "Spotifast")
                    .map_err(|error| error.to_string())
            })();
            observe::receiver_activated(&name, started, &result, has_credentials);
            let _ = events.send(Event::ReceiverActivated { name, result });
            waker.wake();
        });
    }

    fn check_for_updates(&self, manual: bool, source: crate::updates::Source) {
        // A proxy still being restored or refused blocks the check, as it
        // blocks every other request.
        let usable = self.http.client().map(|_| ());
        let proxy = self.engine_config.proxy.clone();
        let events = self.events.clone();
        let waker = self.waker.clone();
        tokio::task::spawn_blocking(move || {
            let started = Instant::now();
            let result = usable.and_then(|()| {
                crate::updates::updater(&proxy)
                    .and_then(|updater| updater.with_source(source).check())
                    .map_err(|error| format!("{error:#}"))
            });
            observe::job("update_check", started, result.is_ok(), 1);
            let _ = events.send(Event::UpdateChecked { manual, result });
            waker.wake();
        });
    }

    /// Verifies playback after reconnect and retries loads rejected while
    /// Spirc is still registering. Runs on the backend timer.
    fn verify_resume(&mut self) {
        let Some((spec, attempts)) = self.resume_verify.take() else {
            return;
        };
        let Some(engine) = &self.engine else {
            self.lifecycle.restore("no_engine", attempts, &spec, None);
            return;
        };
        if engine.interrupted().is_some() {
            // Playback resumed or another track started.
            self.lifecycle.restore("active", attempts, &spec, None);
            return;
        }
        if attempts >= 3 {
            log::warn!("gave up picking playback up again after {attempts} tries");
            self.lifecycle.restore("gave_up", attempts, &spec, None);
            return;
        }
        log::info!(
            "restoring playback on the new session (try {})",
            attempts + 1
        );
        self.lifecycle.restoring(&spec);
        let sent = engine.resume(spec.clone());
        if let Err(error) = &sent {
            log::warn!("unable to pick playback up again: {error}");
        }
        let outcome = if sent.is_ok() { "sent" } else { "send_error" };
        self.lifecycle
            .restore(outcome, attempts + 1, &spec, sent.as_ref().err());
        self.resume_verify = Some((spec, attempts + 1));
        self.schedule_resume_check(4_000);
    }

    fn schedule_resume_check(&self, delay_ms: u64) {
        let commands = self.commands.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            observe::timer_late(
                "resume_check",
                std::time::Duration::from_millis(delay_ms),
                started,
            );
            let _ = commands.send(Command::VerifyResume);
        });
    }

    fn fetch_rootlist(&mut self) {
        if !self.signed_in {
            return;
        }
        self.rootlist_pending = true;
        self.start_rootlist();
    }

    fn start_rootlist(&mut self) {
        let fetch = self.engine.clone().map(|engine| async move {
            engine
                .rootlist()
                .await
                .map_err(|error| format!("{error:#}"))
        });
        self.start_pending_rootlist(fetch);
    }

    // Keep the read injectable so startup ordering can be tested without
    // authenticating a real playback engine or using an account's folders.
    fn start_pending_rootlist(
        &mut self,
        fetch: Option<
            impl std::future::Future<Output = Result<crate::player::Rootlist, String>> + Send + 'static,
        >,
    ) {
        if !self.signed_in || !self.rootlist_pending {
            return;
        }
        let Some(fetch) = fetch else {
            return;
        };
        self.rootlist_pending = false;
        let commands = self.commands.clone();
        let mut session = self.session.subscribe();
        let generation = *session.borrow_and_update();
        tokio::spawn(async move {
            let started = Instant::now();
            let result = tokio::select! {
                _ = session.changed() => return,
                result = fetch => result,
            };
            observe::job("rootlist", started, result.is_ok(), 1);
            let _ = commands.send(Command::RootlistFinished { generation, result });
        });
    }

    fn on_rootlist_finished(
        &self,
        generation: u64,
        result: Result<crate::player::Rootlist, String>,
    ) {
        if self.signed_in && generation == *self.session.borrow() {
            self.emit(Event::Rootlist { result });
        }
    }

    fn fetch_album_types(&mut self, uris: Vec<String>) {
        self.album_type_lookup
            .enqueue(self.signed_in, self.premium, uris);
        self.start_album_type_lookup();
    }

    /// Asks the streaming session about the waiting shows. Without a
    /// session they wait; a failed answer leaves them shown.
    fn start_audiobook_lookup(&mut self) {
        let Some(engine) = self.engine.clone() else {
            return;
        };
        if self.audiobook_lookup.is_empty() {
            return;
        }
        let uris: Vec<String> = std::mem::take(&mut self.audiobook_lookup)
            .into_iter()
            .collect();
        let session_generation = *self.session.borrow();
        let commands = self.commands.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let mut audiobooks = Vec::new();
            let mut failed = false;
            for chunk in uris.chunks(AUDIOBOOK_BATCH) {
                match session_reads::audiobook_shows(engine.session(), chunk).await {
                    Ok(found) => audiobooks.extend(found),
                    Err(error) => {
                        failed = true;
                        log::debug!("audiobook lookup failed: {error:#}")
                    }
                }
            }
            observe::job(
                "audiobooks",
                started,
                !failed,
                uris.len().div_ceil(AUDIOBOOK_BATCH),
            );
            let _ = commands.send(Command::AudiobookShowsResolved {
                session_generation,
                audiobooks,
            });
        });
    }

    /// Resolves the waiting radios once the streaming session is ready; the
    /// Web API has no stations.
    fn start_radio(&mut self) {
        let Some(engine) = self.engine.clone() else {
            return;
        };
        let session_generation = *self.session.borrow();
        for (seed, generation) in std::mem::take(&mut self.radio_waiting) {
            let engine = Arc::clone(&engine);
            let commands = self.commands.clone();
            tokio::spawn(async move {
                let started = Instant::now();
                let result = match crate::util::station_uri(&seed) {
                    Some(station) => {
                        match tokio::time::timeout(
                            RADIO_TIMEOUT,
                            session_reads::station(engine.session(), &station),
                        )
                        .await
                        {
                            Ok(Ok(tracks)) => Ok(tracks),
                            Ok(Err(error)) => {
                                log::warn!("radio {station} failed: {error:#}");
                                Err("Couldn't load this radio. Try again.".to_string())
                            }
                            Err(_) => Err("Spotify took too long to answer. Try again.".into()),
                        }
                    }
                    None => Err("There is no radio for this item.".into()),
                };
                observe::job("radio", started, result.is_ok(), 1);
                let _ = commands.send(Command::RadioResolved {
                    session_generation,
                    seed,
                    generation,
                    result,
                });
            });
        }
    }

    fn start_album_type_lookup(&mut self) {
        let Some(engine) = self.engine.clone() else {
            return;
        };
        let Some(request) = self.album_type_lookup.next() else {
            return;
        };
        let commands = self.commands.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let result =
                match tokio::time::timeout(ALBUM_TYPE_TIMEOUT, engine.album_is_ep(&request.uri))
                    .await
                {
                    Ok(result) => result.map_err(|error| format!("{error:#}")),
                    Err(_) => Err("album metadata timed out".into()),
                };
            observe::job("album_type", started, result.is_ok(), 1);
            let _ = commands.send(Command::AlbumTypeResolved {
                uri: request.uri,
                session_generation: request.session_generation,
                engine_generation: request.engine_generation,
                result,
            });
        });
    }

    fn on_album_type_resolved(&mut self, request: AlbumTypeRequest, result: Result<bool, String>) {
        if !self.signed_in || !self.album_type_lookup.finish(&request) {
            return;
        }
        self.emit(Event::AlbumType {
            uri: request.uri,
            result,
        });
        self.start_album_type_lookup();
    }

    fn fetch_lyrics(&self, request: LyricsRequest) {
        let http = self.http.client();
        let events = self.events.clone();
        let waker = self.waker.clone();
        let cache_dir = self.dirs.lyrics_cache_dir();
        let engine = self.engine.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            // Spotify's own words go first: they follow the recording
            // exactly. Everything else, a signed-out session included,
            // falls back to LRCLIB.
            let result = match spotify_lyrics(engine, &request.uri, &cache_dir).await {
                Some(found) => Ok(Some(found)),
                None => match http {
                    Ok(http) => crate::lyrics::fetch(&http, &cache_dir, &request.query)
                        .await
                        .map_err(|error| format!("{error:#}")),
                    Err(error) => Err(error),
                },
            };
            observe::job("lyrics", started, result.is_ok(), 1);
            let _ = events.send(Event::Lyrics {
                uri: request.uri,
                result,
            });
            waker.wake();
        });
    }

    /// Loads cached playlist items. The UI compares the cached snapshot with
    /// the live playlist before using them.
    fn load_playlist_cache(&self, id: String, generation: u64) {
        let Some(account) = self.api.account() else {
            return;
        };
        let events = self.events.clone();
        let waker = self.waker.clone();
        let path = self
            .dirs
            .account_playlist_cache_dir(account.as_str())
            .join(format!("{id}.json"));
        let account_id = account.as_str().to_string();
        tokio::spawn(async move {
            let cache = read_playlist_cache(path)
                .await
                .ok()
                .and_then(|(cached, appendable)| {
                    let total = cached
                        .total
                        .unwrap_or_else(|| cached.items.len().try_into().unwrap_or(u32::MAX));
                    if cached.items.len() > total as usize
                        || cached.next_offset.is_some_and(|offset| offset > total)
                    {
                        return None;
                    }
                    Some(PlaylistCache {
                        snapshot: cached.snapshot,
                        items: cached.items,
                        total,
                        next_offset: cached.next_offset,
                        appendable,
                    })
                });
            let _ = events.send(Event::PlaylistCache {
                account_id,
                id,
                generation,
                cache,
            });
            waker.wake();
        });
    }

    /// Ask Spotify who is behind each user id. Only the streaming session
    /// can ask; without one the interface shows the bare ids.
    fn fetch_user_names(&self, ids: Vec<String>) {
        let Some(engine) = self.engine.clone() else {
            return;
        };
        let events = self.events.clone();
        let waker = self.waker.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let requests = ids.len();
            for id in ids {
                let name = session_reads::user_display_name(engine.session(), &id).await;
                let _ = events.send(Event::UserName { id, name });
                waker.wake();
            }
            observe::job("user_names", started, true, requests);
        });
    }

    // ---- api ----------------------------------------------------------------

    fn cancel_search(&mut self) {
        for task in self.search_tasks.drain(..) {
            task.abort();
        }
    }

    fn search(&mut self, query: String, serial: u64) {
        self.cancel_search();
        if query.is_empty() {
            return;
        }
        let split = self.api.personal_ready();
        self.emit(Event::Api(Box::new(ApiResponse::SearchStarted {
            query: query.clone(),
            serial,
            split,
        })));
        if split {
            self.search_tasks
                .push(self.dispatch(ApiRequest::SearchPlaylists {
                    query: query.clone(),
                    serial,
                }));
        }
        let request = if split {
            ApiRequest::SearchCatalogue { query, serial }
        } else {
            ApiRequest::Search { query, serial }
        };
        self.search_tasks.push(self.dispatch(request));
    }

    fn dispatch(&self, request: ApiRequest) -> tokio::task::AbortHandle {
        let api = Arc::clone(&self.api);
        let shared_lease = self.credentials.lease(CredentialSlot::Shared);
        let personal_lease = self.credentials.lease(CredentialSlot::Personal);
        let background_api = Arc::clone(&self.background_api);
        let background = request.background();
        let engine = self.engine.clone();
        let commands = self.commands.clone();
        let mut session = self.session.subscribe();
        let generation = *session.borrow_and_update();
        let kind = request.kind();
        // Reports the request when it answers, or as aborted when the task
        // ends first (a sign-out, a superseded search).
        let track = observe::ApiTrack::begin(&request, &self.api);
        tokio::spawn(async move {
            let (response, expired) = tokio::select! {
                _ = session.changed() => return,
                result = async {
                    let _background_permit = if background {
                        let asked = Instant::now();
                        let permit = background_api.acquire_owned().await.ok();
                        observe::permit_waited(kind, asked);
                        permit
                    } else {
                        None
                    };
                    handle(&api, engine.as_deref(), request).await
                } => result,
            };
            if let Some(track) = track {
                track.finish(&response);
            }
            // Apply completion on the command loop. A late response cannot
            // clear or repopulate a session created after sign-out.
            let _ = commands.send(Command::ApiFinished {
                generation,
                response: Box::new(response),
                expired,
                shared_lease,
                personal_lease,
            });
        })
        .abort_handle()
    }

    fn accent(&self, url: String) {
        let art = self.art.clone();
        let events = self.events.clone();
        let waker = self.waker.clone();
        tokio::spawn(async move {
            if let Ok(bytes) = art.fetch(&url).await {
                let color = tokio::task::spawn_blocking(move || accent_color(&bytes))
                    .await
                    .ok()
                    .flatten();
                if let Some(color) = color {
                    let _ = events.send(Event::Accent { url, color });
                    waker.wake();
                }
            }
        });
    }
}

fn friendly_connect_error(error: &anyhow::Error) -> String {
    let text = format!("{error:#}");
    let lower = text.to_lowercase();
    if lower.contains("badcredentials") || lower.contains("bad credentials") {
        "Spotify rejected the saved sign-in. Please sign in again.".to_string()
    } else if lower.contains("premium") {
        PREMIUM_NEEDED.to_string()
    } else if lower.contains("dns") || lower.contains("connect") || lower.contains("resolve") {
        format!("Couldn't reach Spotify: {text}")
    } else {
        text
    }
}

fn operation_for(api: &ApiGateway, request: &ApiRequest) -> Operation {
    match request {
        ApiRequest::Me => Operation::CanonicalAccount,
        ApiRequest::Devices
        | ApiRequest::PlaybackState { .. }
        | ApiRequest::Queue { .. }
        | ApiRequest::Remote { .. }
        | ApiRequest::Transfer { .. }
        | ApiRequest::ShufflePlay { .. }
        | ApiRequest::AddToQueue { .. }
        | ApiRequest::AddManyToQueue { .. } => Operation::Playback,
        ApiRequest::RecentlyPlayed { .. }
        | ApiRequest::TopTracks { .. }
        | ApiRequest::TopArtists { .. }
        | ApiRequest::SavedTracks { .. }
        | ApiRequest::SavedAlbums { .. }
        | ApiRequest::FollowedArtists { .. }
        | ApiRequest::SavedShows { .. }
        | ApiRequest::SavedEpisodes { .. }
        | ApiRequest::SetSaved { .. } => Operation::UserData,
        // Development Mode cannot answer membership for playlists it omits.
        ApiRequest::Contains { uris } => {
            if uris.iter().any(|uri| uri.starts_with("spotify:playlist:")) {
                Operation::UnsupportedDevelopmentMode
            } else {
                Operation::UserData
            }
        }
        ApiRequest::MyPlaylists { .. } => Operation::PlaylistLibrary,
        ApiRequest::CreatePlaylist { .. } => Operation::PlaylistCreation,
        ApiRequest::Discover { .. } | ApiRequest::SearchPlaylists { .. } => {
            Operation::PlaylistSearch
        }
        ApiRequest::SearchCatalogue { .. } => Operation::CatalogSearch,
        ApiRequest::Search { .. } => Operation::PlaylistSearch,
        ApiRequest::Playlist { id, .. } => Operation::PlaylistMetadata(api.playlist_access(id)),
        ApiRequest::PlaylistItems { id, .. }
        | ApiRequest::PlaylistSample { id, .. }
        | ApiRequest::CheckPlaylistDuplicates {
            playlist_id: id, ..
        } => Operation::PlaylistItems(api.playlist_access(id)),
        ApiRequest::UploadPlaylistCover { id, .. }
        | ApiRequest::UpdatePlaylist { id, .. }
        | ApiRequest::FollowPlaylist { id, .. } => {
            Operation::PlaylistMutation(api.playlist_access(id))
        }
        ApiRequest::AddToPlaylist { playlist_id, .. }
        | ApiRequest::RemoveFromPlaylist { playlist_id, .. }
        | ApiRequest::ReorderPlaylist { playlist_id, .. } => {
            Operation::PlaylistMutation(api.playlist_access(playlist_id))
        }
        ApiRequest::Recommendations { .. }
        | ApiRequest::ArtistTopTracks { .. }
        | ApiRequest::RelatedArtists { .. } => Operation::UnsupportedDevelopmentMode,
        ApiRequest::Artist { .. }
        | ApiRequest::ArtistAlbums { .. }
        | ApiRequest::Album { .. }
        | ApiRequest::AlbumTracks { .. }
        | ApiRequest::AlbumQueueTracks { .. }
        | ApiRequest::Show { .. }
        | ApiRequest::ShowEpisodes { .. }
        | ApiRequest::HomeEpisodes { .. }
        | ApiRequest::Track { .. }
        | ApiRequest::Episode { .. } => Operation::Catalog,
    }
}

fn observe_playlists(api: &ApiGateway, response: &ApiResponse) {
    match response {
        ApiResponse::Discover {
            result: Ok(playlists),
            ..
        } => api.observe_playlists(playlists),
        ApiResponse::MyPlaylists {
            result: Ok(page), ..
        } => api.observe_playlists(&page.items),
        ApiResponse::Playlist {
            result: Ok(playlist),
            ..
        }
        | ApiResponse::PlaylistCreated(Ok(playlist)) => api.observe_playlist(playlist),
        ApiResponse::Search {
            result: Ok(results),
            ..
        } => {
            if let Some(playlists) = &results.playlists {
                api.observe_playlists(&playlists.items);
            }
        }
        ApiResponse::SearchPlaylists {
            result: Ok(page), ..
        } => api.observe_playlists(&page.items),
        ApiResponse::PlaylistUpdated {
            id,
            result: Err(error),
        }
        | ApiResponse::PlaylistItemsChanged {
            id,
            result: Err(error),
            ..
        }
        | ApiResponse::PlaylistItems {
            id,
            result: Err(error),
            ..
        }
        | ApiResponse::PlaylistSample {
            id,
            result: Err(error),
            ..
        }
        | ApiResponse::PlaylistDuplicatesChecked {
            playlist_id: id,
            result: Err(error),
            ..
        }
        | ApiResponse::PlaylistFollowChanged {
            id,
            result: Err(error),
            ..
        } if error.status() == Some(403) => {
            api.invalidate_playlist_access(&PlaylistId::new(id.clone()));
        }
        _ => {}
    }
}

async fn handle(
    api: &ApiGateway,
    engine: Option<&Engine>,
    request: ApiRequest,
) -> (ApiResponse, Option<ApiSource>) {
    let operation = operation_for(api, &request);
    // A session whose long-lived connection has dropped still answers over
    // its HTTP client, so the engine's presence is the only liveness test;
    // a read the session truly cannot make falls back to the Web API below.
    if api.session_serves(operation)
        && let Some(engine) = engine
            .filter(|engine| same_account(&engine.session().username(), api.account().as_ref()))
        && let Some(response) = over_session(engine, &request).await
    {
        log::debug!("Spotify route operation={operation:?} source=session");
        observe::session_answered(&request);
        observe_playlists(api, &response);
        return (response, None);
    }
    observe::route(api, operation, engine, &request);
    let selected = api.client_for(operation).await;
    let expired = std::cell::Cell::new(None);
    macro_rules! routed {
        ($method:ident($($argument:expr),* $(,)?)) => {{
            let result = match &selected {
                Ok(client) => client.$method($($argument),*).await,
                Err(error) => Err(error.clone()),
            };
            if let Err(ApiError::SignInExpired { api_source }) = &result {
                expired.set(Some(*api_source));
            }
            result
        }};
    }

    let response = match request {
        ApiRequest::Me => ApiResponse::Me(routed!(me())),
        ApiRequest::Devices => {
            let result = routed!(devices());
            observe::devices(&result, engine);
            ApiResponse::Devices(result)
        }
        ApiRequest::PlaybackState { seq } => {
            let result = routed!(playback_state());
            observe::snapshot(&result, engine);
            ApiResponse::PlaybackState { seq, result }
        }
        ApiRequest::Queue { seq } => ApiResponse::Queue {
            seq,
            result: routed!(queue()),
        },
        ApiRequest::RecentlyPlayed {
            who,
            generation,
            before,
            limit,
        } => ApiResponse::RecentlyPlayed {
            who,
            generation,
            limit,
            result: routed!(recently_played(limit, None, before.as_deref())),
        },
        ApiRequest::TopTracks {
            offset,
            full,
            generation,
        } => ApiResponse::TopTracks {
            result: routed!(top_tracks("short_term", if full { 50 } else { 20 }, offset)),
            offset,
            full,
            generation,
        },
        ApiRequest::TopArtists { generation } => ApiResponse::TopArtists {
            generation,
            result: routed!(top_artists("medium_term", 20)).map(|page| page.items),
        },
        ApiRequest::Recommendations {
            seed_tracks,
            seed_artists,
            generation,
        } => ApiResponse::Recommendations {
            generation,
            result: routed!(recommendations(&seed_tracks, &seed_artists, 20)),
        },
        ApiRequest::Discover { term, generation } => {
            let result = routed!(search(&term, &["playlist"]))
                .map(|results| results.playlists.map(|page| page.items).unwrap_or_default());
            ApiResponse::Discover {
                term,
                generation,
                result,
            }
        }
        ApiRequest::MyPlaylists { offset, generation } => ApiResponse::MyPlaylists {
            offset,
            generation,
            result: routed!(my_playlists(offset, 50)),
        },
        ApiRequest::Playlist { id, generation } => ApiResponse::Playlist {
            result: routed!(playlist(&id)),
            id,
            generation,
        },
        ApiRequest::PlaylistItems {
            id,
            offset,
            generation,
        } => ApiResponse::PlaylistItems {
            result: routed!(playlist_items(&id, offset, PLAYLIST_PAGE_SIZE)),
            id,
            offset,
            generation,
        },
        ApiRequest::PlaylistSample {
            id,
            offset,
            generation,
        } => ApiResponse::PlaylistSample {
            result: routed!(playlist_items(&id, offset, PLAYLIST_PAGE_SIZE)),
            id,
            generation,
        },
        ApiRequest::CreatePlaylist {
            name,
            public,
            description,
        } => ApiResponse::PlaylistCreated(routed!(create_playlist(&name, public, &description))),
        ApiRequest::UploadPlaylistCover {
            id,
            request,
            previous_urls,
            cover,
        } => ApiResponse::PlaylistCoverUploaded {
            request,
            previous_urls,
            result: routed!(upload_playlist_cover(&id, &cover.encoded)),
            id,
            cover,
        },
        ApiRequest::UpdatePlaylist {
            id,
            name,
            description,
            public,
        } => ApiResponse::PlaylistUpdated {
            result: routed!(update_playlist(
                &id,
                name.as_deref(),
                description.as_deref(),
                public
            )),
            id,
        },
        ApiRequest::CheckPlaylistDuplicates {
            playlist_id,
            playlist_name,
            items,
            position,
        } => {
            let uris: Vec<String> = items.iter().map(|item| item.uri().to_string()).collect();
            ApiResponse::PlaylistDuplicatesChecked {
                result: routed!(playlist_duplicates(&playlist_id, &uris)),
                playlist_id,
                playlist_name,
                items,
                position,
            }
        }
        ApiRequest::AddToPlaylist {
            playlist_id,
            playlist_name,
            uris,
            position,
        } => ApiResponse::PlaylistItemsChanged {
            result: routed!(add_playlist_items(&playlist_id, &uris, position)),
            id: playlist_id,
            message: format!("Added to {playlist_name}"),
        },
        ApiRequest::RemoveFromPlaylist {
            playlist_id,
            uris,
            snapshot_id,
        } => ApiResponse::PlaylistItemsChanged {
            result: routed!(remove_playlist_items(
                &playlist_id,
                &uris,
                snapshot_id.as_deref()
            )),
            id: playlist_id,
            message: "Removed from playlist".to_string(),
        },
        ApiRequest::ReorderPlaylist {
            playlist_id,
            range_start,
            insert_before,
            snapshot_id,
        } => ApiResponse::PlaylistItemsChanged {
            result: routed!(reorder_playlist(
                &playlist_id,
                range_start,
                insert_before,
                snapshot_id.as_deref()
            )),
            id: playlist_id,
            message: String::new(),
        },
        ApiRequest::FollowPlaylist { id, follow } => ApiResponse::PlaylistFollowChanged {
            result: if follow {
                routed!(follow_playlist(&id))
            } else {
                routed!(unfollow_playlist(&id))
            },
            id,
            followed: follow,
        },
        ApiRequest::SavedTracks { offset, generation } => ApiResponse::SavedTracks {
            offset,
            generation,
            account_id: api.account().map(|account| account.as_str().to_string()),
            result: routed!(saved_tracks(offset, 50)),
        },
        ApiRequest::SavedAlbums { offset } => ApiResponse::SavedAlbums {
            offset,
            result: routed!(saved_albums(offset, 50)),
        },
        ApiRequest::FollowedArtists { after } => ApiResponse::FollowedArtists {
            result: routed!(followed_artists(after.as_deref(), 50)),
            after,
        },
        ApiRequest::SavedShows { offset } => ApiResponse::SavedShows {
            offset,
            result: routed!(saved_shows(offset, 50)),
        },
        ApiRequest::SavedEpisodes { offset } => ApiResponse::SavedEpisodes {
            offset,
            result: routed!(saved_episodes(offset, 50)),
        },
        ApiRequest::SetSaved { uris, saved } => ApiResponse::SavedChanged {
            result: if saved {
                routed!(save(&uris))
            } else {
                routed!(unsave(&uris))
            },
            uris,
            saved,
        },
        ApiRequest::Contains { uris } => ApiResponse::Contains {
            result: routed!(contains(&uris)),
            uris,
        },
        ApiRequest::Search { query, serial } => ApiResponse::Search {
            result: routed!(search(
                &query,
                &["track", "artist", "album", "playlist", "show", "episode"]
            )),
            query,
            serial,
        },
        ApiRequest::SearchCatalogue { query, serial } => ApiResponse::Search {
            result: routed!(search(
                &query,
                &["track", "artist", "album", "show", "episode"]
            )),
            query,
            serial,
        },
        ApiRequest::SearchPlaylists { query, serial } => ApiResponse::SearchPlaylists {
            result: routed!(search(&query, &["playlist"]))
                .map(|results| results.playlists.unwrap_or_default()),
            query,
            serial,
        },
        ApiRequest::Artist { id } => ApiResponse::Artist {
            result: routed!(artist(&id)),
            id,
        },
        ApiRequest::ArtistTopTracks { id } => ApiResponse::ArtistTopTracks {
            result: routed!(artist_top_tracks(&id)),
            id,
        },
        ApiRequest::ArtistAlbums { id, groups, offset } => ApiResponse::ArtistAlbums {
            result: routed!(artist_albums(&id, &groups, offset, 50)),
            id,
            groups,
            offset,
        },
        ApiRequest::RelatedArtists { id } => ApiResponse::RelatedArtists {
            result: routed!(related_artists(&id)),
            id,
        },
        ApiRequest::Album { id } => ApiResponse::Album {
            result: routed!(album(&id)),
            id,
        },
        ApiRequest::AlbumTracks {
            id,
            offset,
            generation,
        } => ApiResponse::AlbumTracks {
            generation,
            result: routed!(album_tracks(&id, offset, 50)),
            id,
            offset,
        },
        ApiRequest::AlbumQueueTracks {
            id,
            offset,
            request,
        } => ApiResponse::AlbumQueueTracks {
            result: routed!(album_tracks(&id, offset, 50)),
            offset,
            request,
        },
        ApiRequest::Show { id } => ApiResponse::Show {
            result: routed!(show(&id)),
            id,
        },
        ApiRequest::ShowEpisodes { id, offset } => ApiResponse::ShowEpisodes {
            result: routed!(show_episodes(&id, offset, 50)),
            id,
            offset,
        },
        ApiRequest::HomeEpisodes {
            shows: asked,
            generation,
        } => {
            let planned = asked.len();
            let mut issued = 0;
            let mut shows = Vec::new();
            let mut failure = None;
            for show in asked {
                issued += 1;
                match routed!(show_episodes(&show.id, 0, HOME_EPISODES_PER_SHOW)) {
                    Ok(page) => shows.push((show, page.items)),
                    // A show that is gone answers on its own; a rate limit,
                    // an exhausted quota or a lost sign-in would answer the
                    // same for every show still to come, so stop asking.
                    Err(error) => {
                        let stop = !matches!(error, ApiError::Status { .. } | ApiError::Decode(_));
                        failure.get_or_insert(error);
                        if stop {
                            break;
                        }
                    }
                }
            }
            observe::fanout("HomeEpisodes", planned, issued, failure.as_ref());
            ApiResponse::HomeEpisodes {
                generation,
                result: match failure {
                    Some(error) if shows.is_empty() => Err(error),
                    _ => Ok(shows),
                },
            }
        }
        ApiRequest::Track { id } => ApiResponse::Track {
            result: routed!(track(&id)),
            id,
        },
        ApiRequest::Episode { id } => ApiResponse::Episode {
            result: routed!(episode(&id)),
            id,
        },
        ApiRequest::Remote {
            action,
            device_id,
            play,
            position_ms,
            percent,
            flag,
            repeat,
        } => {
            let device = device_id.as_deref();
            let control = observe::RemoteControl::send(
                "api.remote_command",
                observe::remote_kind(action),
                observe::remote_trace(action, play.is_some()),
                observe::remote_intents(action),
                device,
                engine,
            );
            let result = match action {
                RemoteAction::Play => routed!(play(device, play.as_ref())),
                RemoteAction::Pause => routed!(pause(device)),
                RemoteAction::Next => routed!(next(device)),
                RemoteAction::Previous => routed!(previous(device)),
                RemoteAction::Seek => routed!(seek(position_ms, device)),
                RemoteAction::Volume => routed!(set_volume(percent, device)),
                RemoteAction::Shuffle => routed!(set_shuffle(flag, device)),
                RemoteAction::Repeat => routed!(set_repeat(&repeat, device)),
            };
            if let Some(control) = control {
                control.done(&result);
            }
            ApiResponse::Remote { action, result }
        }
        ApiRequest::ShufflePlay { device_id, play } => {
            let device = device_id.as_deref();
            let control = observe::RemoteControl::send(
                "api.remote_command",
                "ShufflePlay",
                Some("play"),
                observe::PLAY_ITEM,
                device,
                engine,
            );
            let result = match routed!(set_shuffle(true, device)) {
                Ok(()) => routed!(play(device, Some(&play))),
                Err(error) => Err(error),
            };
            if let Some(control) = control {
                control.done(&result);
            }
            ApiResponse::Remote {
                action: RemoteAction::Play,
                result,
            }
        }
        ApiRequest::Transfer { device_id, play } => {
            let control = observe::RemoteControl::send(
                "connect.transfer",
                "Transfer",
                Some("transfer"),
                observe::TRANSFER,
                Some(device_id.as_str()),
                engine,
            );
            let result = routed!(transfer(&device_id, play));
            if let Some(control) = control {
                control.done(&result);
            }
            ApiResponse::Transferred { result, device_id }
        }
        ApiRequest::AddToQueue {
            uri,
            device_id,
            label,
        } => ApiResponse::QueueAdded {
            result: routed!(add_to_queue(&uri, device_id.as_deref())),
            label,
        },
        ApiRequest::AddManyToQueue {
            request,
            uris,
            device_id,
        } => {
            let (added, result) = match &selected {
                Ok(client) => client.add_many_to_queue(&uris, device_id.as_deref()).await,
                Err(error) => (0, Err(error.clone())),
            };
            if let Err(ApiError::SignInExpired { api_source }) = &result {
                expired.set(Some(*api_source));
            }
            observe::fanout("AddManyToQueue", uris.len(), added, result.as_ref().err());
            ApiResponse::QueueBatchAdded {
                request,
                added,
                result,
            }
        }
    };
    observe_playlists(api, &response);
    (response, expired.get())
}

/// Whether a session signed in as `username` answers for the Web API's
/// account. Local playback is approved separately, and another account's
/// view of a playlist is not this one's.
fn same_account(username: &str, account: Option<&AccountId>) -> bool {
    account.is_some_and(|account| account.as_str() == username)
}

/// How long a session read may take before the Web API is asked instead:
/// what the Web API's own requests get. A stalled line otherwise waits on
/// the operating system, which is far longer than a page should spin.
const SESSION_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Answers a playlist read over the streaming session. `None` when the
/// session could not, leaving the request to the Web API.
async fn over_session(engine: &Engine, request: &ApiRequest) -> Option<ApiResponse> {
    let session = engine.session();
    let started = Instant::now();
    let read = async {
        Some(match session_read(request)? {
            SessionRead::Header { id } => {
                SessionAnswer::Header(settle(session_reads::playlist(session, id).await)?)
            }
            SessionRead::Rows { id, offset } => SessionAnswer::Rows(settle(
                session_reads::items(session, id, offset, PLAYLIST_PAGE_SIZE).await,
            )?),
            SessionRead::Sample { id, offset } => SessionAnswer::Rows(settle(
                session_reads::sample(session, id, offset, PLAYLIST_PAGE_SIZE).await,
            )?),
        })
    };
    let Ok(answer) = tokio::time::timeout(SESSION_READ_TIMEOUT, read).await else {
        log::debug!("session read timed out; asking the Web API");
        observe::session_read(request, "fallback_timeout", started);
        return None;
    };
    let Some(answer) = answer else {
        let outcome = if session_read(request).is_some() {
            "fallback_retry"
        } else {
            "not_served"
        };
        observe::session_read(request, outcome, started);
        return None;
    };
    let outcome = match &answer {
        SessionAnswer::Header(Ok(_)) | SessionAnswer::Rows(Ok(_)) => "answered",
        SessionAnswer::Header(Err(_)) | SessionAnswer::Rows(Err(_)) => "definitive",
    };
    observe::session_read(request, outcome, started);
    session_response(request, answer)
}

/// The read a request asks of the session: a playlist's header, a page of
/// its rows, or a sample of who added them, which needs no song details.
/// Anything else, a duplicate check among them, is the Web API's even
/// where the session serves the operation.
#[derive(Debug, PartialEq)]
enum SessionRead<'a> {
    Header { id: &'a str },
    Rows { id: &'a str, offset: u32 },
    Sample { id: &'a str, offset: u32 },
}

fn session_read(request: &ApiRequest) -> Option<SessionRead<'_>> {
    Some(match request {
        ApiRequest::Playlist { id, .. } => SessionRead::Header { id },
        ApiRequest::PlaylistItems { id, offset, .. } => SessionRead::Rows {
            id,
            offset: *offset,
        },
        ApiRequest::PlaylistSample { id, offset, .. } => SessionRead::Sample {
            id,
            offset: *offset,
        },
        _ => return None,
    })
}

/// What the session read: a playlist's header, or a page of its rows.
enum SessionAnswer {
    Header(ApiResult<Playlist>),
    Rows(ApiResult<Page<PlaylistItem>>),
}

/// The response a session answer becomes, carrying the request's own id,
/// offset, and generation so the app matches it to the page that asked.
fn session_response(request: &ApiRequest, answer: SessionAnswer) -> Option<ApiResponse> {
    Some(match (request, answer) {
        (ApiRequest::Playlist { id, generation }, SessionAnswer::Header(result)) => {
            ApiResponse::Playlist {
                id: id.clone(),
                generation: *generation,
                result,
            }
        }
        (
            ApiRequest::PlaylistItems {
                id,
                offset,
                generation,
            },
            SessionAnswer::Rows(result),
        ) => ApiResponse::PlaylistItems {
            id: id.clone(),
            offset: *offset,
            generation: *generation,
            result,
        },
        (ApiRequest::PlaylistSample { id, generation, .. }, SessionAnswer::Rows(result)) => {
            ApiResponse::PlaylistSample {
                id: id.clone(),
                generation: *generation,
                result,
            }
        }
        _ => return None,
    })
}

/// A session answer as the Web API would have given it. A final refusal is
/// shown as such; a dropped line leaves the Web API to try.
fn settle<T>(result: Result<T, session_reads::Failure>) -> Option<ApiResult<T>> {
    match result {
        Ok(value) => Some(Ok(value)),
        Err(session_reads::Failure::Definitive(error)) => Some(Err(error)),
        Err(session_reads::Failure::Retry(error)) => {
            log::debug!("session read failed: {error}; asking the Web API");
            observe::session_retry(&error);
            None
        }
    }
}

/// Spotify's transcription of the track, when the local session can ask for
/// one. Answers are cached like LRCLIB's, "none" included; `None` falls
/// back to LRCLIB.
async fn spotify_lyrics(
    engine: Option<Arc<Engine>>,
    uri: &str,
    cache_dir: &std::path::Path,
) -> Option<crate::lyrics::Lyrics> {
    let id = uri.strip_prefix("spotify:track:")?;
    let path = cache_dir.join(format!("spotify-{id}.json"));
    if let Some(cached) = crate::lyrics::cached(&path) {
        return cached;
    }
    match engine?.lyrics_json(uri).await {
        Ok(json) => {
            let found = json.as_ref().and_then(crate::lyrics::from_spotify);
            crate::lyrics::store(&path, &found);
            found
        }
        Err(error) => {
            log::debug!("spotify lyrics unavailable: {error:#}");
            None
        }
    }
}

/// A playlist's items on disk, valid for exactly one snapshot.
#[derive(serde::Serialize, serde::Deserialize)]
struct CachedPlaylist {
    snapshot: String,
    items: Vec<PlaylistItem>,
    /// Absent in the original whole-playlist cache format.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    total: Option<u32>,
    /// A value means this is a prefix. Absent means the cache is complete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    next_offset: Option<u32>,
}

#[cfg(test)]
async fn read_cached_playlist(path: std::path::PathBuf) -> std::io::Result<CachedPlaylist> {
    tokio::task::spawn_blocking(move || read_cached_playlist_file(&path))
        .await
        .map_err(std::io::Error::other)?
}

fn read_cached_playlist_file(path: &std::path::Path) -> std::io::Result<CachedPlaylist> {
    let file = std::fs::File::open(path)?;
    // Parse on the file worker without keeping the entire JSON alongside
    // the deserialized playlist. Snapshot/count validation still follows.
    serde_json::from_reader(std::io::BufReader::new(file)).map_err(std::io::Error::other)
}

async fn read_playlist_cache(path: std::path::PathBuf) -> std::io::Result<(CachedPlaylist, bool)> {
    tokio::task::spawn_blocking(move || {
        // A writer may have created a row file that its manifest does not yet
        // reference. Hold the account lock through both reading and recovery.
        let lock = playlist_cache_lock(&path);
        if let Err(error) = &lock {
            log::warn!("unable to lock playlist cache {}: {error}", path.display());
        }
        let cache = read_incremental_playlist_cache_file(&path)
            .map(|cache| (cache, true))
            // A missing or damaged new cache must not hide an older JSON cache.
            .or_else(|_| read_cached_playlist_file(&path).map(|cache| (cache, false)));
        if lock.is_ok()
            && let Err(error) = cleanup_unreferenced_playlist_rows(&path)
        {
            log::warn!("unable to clean playlist cache {}: {error}", path.display());
        }
        cache
    })
    .await
    .map_err(std::io::Error::other)?
}

#[cfg(test)]
async fn write_cached_playlist(
    path: std::path::PathBuf,
    cached: CachedPlaylist,
) -> std::io::Result<()> {
    // Move the checkpoint into the file worker without another model clone.
    // Awaiting it preserves checkpoint order, including after playlist edits.
    tokio::task::spawn_blocking(move || write_cached_playlist_file(&path, &cached))
        .await
        .map_err(std::io::Error::other)?
}

#[cfg(test)]
fn write_cached_playlist_file(
    path: &std::path::Path,
    cached: &CachedPlaylist,
) -> std::io::Result<()> {
    use std::io::Write;

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("json.tmp");
    let file = std::fs::File::create(&temporary)?;
    let result = (|| {
        // Buffer small serializer writes without retaining the whole JSON file.
        let mut writer = std::io::BufWriter::new(file);
        serde_json::to_writer(&mut writer, cached).map_err(std::io::Error::other)?;
        writer.flush()?;
        // Windows requires the temporary file to be closed before replacement.
        drop(writer);
        crate::util::replace_file(&temporary, path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

/// The manifest stays constant-sized as the row file grows. A reader only
/// consumes `bytes`, so an interrupted append cannot become visible.
#[derive(serde::Serialize, serde::Deserialize)]
struct PlaylistCacheManifest {
    version: u8,
    snapshot: String,
    data_file: u64,
    bytes: u64,
    rows: u32,
    total: u32,
    next_offset: Option<u32>,
}

fn playlist_manifest_path(path: &std::path::Path) -> std::path::PathBuf {
    path.with_extension("manifest.json")
}

fn playlist_data_path(path: &std::path::Path, data_file: u64) -> std::path::PathBuf {
    path.with_extension(format!("rows.{data_file:016x}"))
}

fn playlist_cache_lock(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "playlist cache has no parent",
        )
    })?;
    std::fs::create_dir_all(parent)?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(parent.join(".playlist-cache.lock"))?;
    file.lock()?;
    Ok(file)
}

/// Recover row files from interrupted replacements. Run under the account
/// lock so a new, unpublished row file cannot be mistaken for an orphan.
fn cleanup_unreferenced_playlist_rows(path: &std::path::Path) -> std::io::Result<()> {
    let current = match read_playlist_manifest(path) {
        Ok(manifest) => Some(manifest.data_file),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "playlist cache has no parent",
        )
    })?;
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid playlist cache name",
            )
        })?;
    let prefix = format!("{stem}.rows.");
    for entry in std::fs::read_dir(parent)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(suffix) = name.to_str().and_then(|name| name.strip_prefix(&prefix)) else {
            continue;
        };
        if suffix.len() != 16 || !suffix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            continue;
        }
        let data_file = u64::from_str_radix(suffix, 16).map_err(std::io::Error::other)?;
        if Some(data_file) == current {
            continue;
        }
        if let Err(error) = std::fs::remove_file(entry.path())
            && error.kind() != std::io::ErrorKind::NotFound
        {
            log::warn!(
                "unable to remove orphan playlist rows {}: {error}",
                entry.path().display()
            );
        }
    }
    Ok(())
}

fn read_playlist_manifest(path: &std::path::Path) -> std::io::Result<PlaylistCacheManifest> {
    let file = std::fs::File::open(playlist_manifest_path(path))?;
    let manifest: PlaylistCacheManifest =
        serde_json::from_reader(std::io::BufReader::new(file)).map_err(std::io::Error::other)?;
    if manifest.version != 2
        || manifest.rows > manifest.total
        || manifest
            .next_offset
            .is_some_and(|offset| offset > manifest.total)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid playlist cache manifest",
        ));
    }
    Ok(manifest)
}

fn read_incremental_playlist_cache_file(path: &std::path::Path) -> std::io::Result<CachedPlaylist> {
    use std::io::Read;

    let manifest = read_playlist_manifest(path)?;
    let file = std::fs::File::open(playlist_data_path(path, manifest.data_file))?;
    if file.metadata()?.len() < manifest.bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "truncated playlist cache data",
        ));
    }
    let reader = std::io::BufReader::new(file.take(manifest.bytes));
    let mut items = Vec::new();
    for block in serde_json::Deserializer::from_reader(reader).into_iter::<Vec<PlaylistItem>>() {
        let block = block.map_err(std::io::Error::other)?;
        if items.is_empty() {
            items = block;
        } else {
            items.extend(block);
        }
        if items.len() > manifest.rows as usize {
            break;
        }
    }
    if items.len() != manifest.rows as usize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "playlist cache row count does not match its manifest",
        ));
    }
    Ok(CachedPlaylist {
        snapshot: manifest.snapshot,
        items,
        total: Some(manifest.total),
        next_offset: manifest.next_offset,
    })
}

/// Keep writes in command order without making the command loop wait for disk.
async fn store_playlist_caches(
    mut writes: mpsc::Receiver<PlaylistCacheWrite>,
    events: std::sync::mpsc::Sender<Event>,
    waker: Waker,
) {
    while let Some(write) = writes.recv().await {
        let PlaylistCacheWrite {
            path,
            account_id,
            id,
            generation,
            snapshot,
            rows,
            total,
            next_offset,
        } = write;
        let result = write_incremental_playlist_cache(
            path.clone(),
            snapshot.clone(),
            rows,
            total,
            next_offset,
        )
        .await;
        if let Err(error) = &result {
            log::warn!("unable to store playlist cache {}: {error}", path.display());
        }
        let _ = events.send(Event::PlaylistCacheStored {
            account_id,
            id,
            generation,
            snapshot,
            success: result.is_ok(),
        });
        waker.wake();
    }
}

async fn write_incremental_playlist_cache(
    path: std::path::PathBuf,
    snapshot: String,
    rows: PlaylistCacheRows,
    total: u32,
    next_offset: Option<u32>,
) -> std::io::Result<()> {
    tokio::task::spawn_blocking(move || {
        write_incremental_playlist_cache_file(&path, snapshot, rows, total, next_offset)
    })
    .await
    .map_err(std::io::Error::other)?
}

fn write_incremental_playlist_cache_file(
    path: &std::path::Path,
    snapshot: String,
    rows: PlaylistCacheRows,
    total: u32,
    next_offset: Option<u32>,
) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};

    if next_offset.is_some_and(|offset| offset > total) {
        return Err(Error::new(ErrorKind::InvalidInput, "offset exceeds total"));
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let _lock = playlist_cache_lock(path)?;
    match rows {
        PlaylistCacheRows::Replace(items) => {
            let count = u32::try_from(items.len())
                .map_err(|_| Error::new(ErrorKind::InvalidInput, "too many playlist rows"))?;
            if count > total {
                return Err(Error::new(ErrorKind::InvalidInput, "rows exceed total"));
            }
            let (file, data_file) = create_playlist_data_file(path)?;
            let data_path = playlist_data_path(path, data_file);
            let result = (|| {
                let bytes = write_playlist_block(file, &items)?;
                write_playlist_manifest(
                    path,
                    &PlaylistCacheManifest {
                        version: 2,
                        snapshot,
                        data_file,
                        bytes,
                        rows: count,
                        total,
                        next_offset,
                    },
                )
            })();
            if result.is_err() {
                if let Err(error) = std::fs::remove_file(&data_path) {
                    log::warn!(
                        "unable to remove incomplete playlist rows {}: {error}",
                        data_path.display()
                    );
                }
            } else if let Err(error) = cleanup_unreferenced_playlist_rows(path) {
                log::warn!("unable to clean playlist cache {}: {error}", path.display());
            }
            result
        }
        PlaylistCacheRows::Append {
            previous_rows,
            previous_offset,
            items,
        } => {
            let mut manifest = read_playlist_manifest(path)?;
            if manifest.snapshot != snapshot
                || manifest.total != total
                || manifest.rows as usize != previous_rows
                || manifest.next_offset.unwrap_or(manifest.total) != previous_offset
            {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "playlist cache changed before append",
                ));
            }
            let added = u32::try_from(items.len())
                .map_err(|_| Error::new(ErrorKind::InvalidInput, "too many playlist rows"))?;
            let count = manifest
                .rows
                .checked_add(added)
                .filter(|count| *count <= total)
                .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "rows exceed total"))?;
            let data_path = playlist_data_path(path, manifest.data_file);
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(data_path)?;
            if file.metadata()?.len() < manifest.bytes {
                return Err(Error::new(ErrorKind::UnexpectedEof, "truncated cache data"));
            }
            // Drop bytes from a write whose manifest was never published.
            file.set_len(manifest.bytes)?;
            use std::io::{Seek, SeekFrom};
            file.seek(SeekFrom::Start(manifest.bytes))?;
            let bytes = write_playlist_block(file, &items)?;
            manifest.bytes = bytes;
            manifest.rows = count;
            manifest.next_offset = next_offset;
            write_playlist_manifest(path, &manifest)
        }
    }
}

fn create_playlist_data_file(path: &std::path::Path) -> std::io::Result<(std::fs::File, u64)> {
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    for attempt in 0..1024 {
        let data_file = seed.wrapping_add(attempt);
        let candidate = playlist_data_path(path, data_file);
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(candidate)
        {
            Ok(file) => return Ok((file, data_file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "playlist cache data names exhausted",
    ))
}

fn write_playlist_block(file: std::fs::File, items: &[PlaylistItem]) -> std::io::Result<u64> {
    use std::io::Write;

    let mut writer = std::io::BufWriter::new(file);
    serde_json::to_writer(&mut writer, items).map_err(std::io::Error::other)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    let file = writer.into_inner().map_err(|error| error.into_error())?;
    // Publish the manifest only after its referenced bytes are durable.
    file.sync_all()?;
    Ok(file.metadata()?.len())
}

fn write_playlist_manifest(
    path: &std::path::Path,
    manifest: &PlaylistCacheManifest,
) -> std::io::Result<()> {
    use std::io::Write;

    let target = playlist_manifest_path(path);
    let temporary = target.with_extension("json.tmp");
    let result = (|| {
        let file = std::fs::File::create(&temporary)?;
        let mut writer = std::io::BufWriter::new(file);
        serde_json::to_writer(&mut writer, manifest).map_err(std::io::Error::other)?;
        writer.flush()?;
        writer
            .into_inner()
            .map_err(|error| error.into_error())?
            .sync_all()?;
        crate::util::replace_file(&temporary, &target)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temporary);
    }
    result
}

// ---- telemetry --------------------------------------------------------------

/// Telemetry for the command loop, the engine's lifecycle and the Web API
/// requests the loop dispatches (see `crate::telemetry`). Inert while
/// telemetry is off. Only kinds, counts and timings leave here: commands and
/// requests carry tokens, account data, search text and listening history,
/// and device ids and names are reduced to a role or a digest.
mod observe {
    use std::collections::hash_map::DefaultHasher;
    use std::collections::{BTreeMap, VecDeque};
    use std::hash::{Hash as _, Hasher as _};
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU64, Ordering::Relaxed};
    use std::sync::{Mutex, MutexGuard};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use super::{ApiRequest, ApiResponse, ApiResult, Command, RemoteAction};
    use crate::api::models::{Device, PlaybackState};
    use crate::api::{ApiError, ApiGateway, ApiSource, Operation};
    use crate::player::{Engine, EngineConfig, PlaybackResume, PlayerCommand};
    use crate::settings::ProxyConfig;
    use crate::telemetry::{self, Counter, Event, Gauge, Intent, Throttle, duration_ms};

    type Kind = &'static str;

    /// A command the loop holds for this long, or one that waited this long
    /// in the queue, delays every command behind it, Next included.
    const SLOW_HANDLING: Duration = Duration::from_millis(50);
    const SLOW_QUEUE: Duration = Duration::from_millis(100);
    const SLOW_REQUEST: Duration = Duration::from_secs(5);
    const SLOW_JOB: Duration = Duration::from_secs(10);
    /// How far back a command looks for the intent that caused it.
    const INTENT_WINDOW: Duration = Duration::from_secs(5);
    /// How long after a reconnect or a lost session events say so.
    const AFTERMATH: Duration = Duration::from_secs(60);
    const REPEAT_WINDOW: Duration = Duration::from_secs(60);
    const REPEAT_LIMIT: u32 = 6;
    /// The probe measures how far a sleep this long overshoots.
    const RUNTIME_TICK: Duration = Duration::from_secs(1);
    const RUNTIME_LAG_CRUMB: Duration = Duration::from_millis(100);
    const RUNTIME_LAG_ANOMALY: Duration = Duration::from_millis(500);
    const RUNTIME_REPORT: Duration = Duration::from_secs(30);

    const TOGGLE: &[&str] = &["toggle", "play", "pause"];
    const PLAY: &[&str] = &["play", "toggle", "play_item"];
    const PAUSE: &[&str] = &["pause", "toggle"];
    const NEXT: &[&str] = &["next"];
    const PREVIOUS: &[&str] = &["previous"];
    const SEEK: &[&str] = &["seek"];
    const VOLUME: &[&str] = &["volume"];
    const SHUFFLE: &[&str] = &["shuffle"];
    const REPEAT: &[&str] = &["repeat"];
    const QUEUE_ADD: &[&str] = &["queue_add"];
    pub(super) const PLAY_ITEM: &[&str] = &["play_item", "play"];
    pub(super) const TRANSFER: &[&str] = &["transfer"];

    static COMMANDS: Counter = Counter::new("backend_commands");
    static COMMANDS_SLOW: Counter = Counter::new("backend_commands_slow");
    static COMMANDS_PARKED: Counter = Counter::new("backend_commands_parked");
    static PLAYER_COMMANDS: Counter = Counter::new("backend_player_commands");
    static PLAYER_FAILED: Counter = Counter::new("backend_player_commands_failed");
    static ENGINE_EVENTS: Counter = Counter::new("backend_engine_events");
    static ENGINE_EVENTS_OLD: Counter = Counter::new("backend_engine_events_old_engine");
    static ENGINE_EVENTS_STALE: Counter = Counter::new("backend_engine_events_stale_lease");
    static RECONNECTS: Counter = Counter::new("backend_reconnects");
    static API_DISPATCHED: Counter = Counter::new("backend_api_dispatched");
    static API_FAILED: Counter = Counter::new("backend_api_failed");
    static API_RATE_LIMITED: Counter = Counter::new("backend_api_rate_limited");
    static API_ABORTED: Counter = Counter::new("backend_api_aborted");
    static SESSION_READS: Counter = Counter::new("backend_session_reads");
    static SESSION_FALLBACKS: Counter = Counter::new("backend_session_fallbacks");
    static POLLS_PLAYBACK: Counter = Counter::new("backend_poll_playback_state");
    static POLLS_QUEUE: Counter = Counter::new("backend_poll_queue");
    static POLLS_DEVICES: Counter = Counter::new("backend_poll_devices");
    static REMOTE_COMMANDS: Counter = Counter::new("backend_remote_commands");
    static JOBS: Counter = Counter::new("backend_jobs");
    static TIMERS_LATE: Counter = Counter::new("backend_timers_late");

    static QUEUE_WAIT_MAX_MS: Gauge = Gauge::peak("backend_queue_wait_max_ms");
    static HANDLE_MAX_MS: Gauge = Gauge::peak("backend_handle_max_ms");
    static BACKLOG_MAX: Gauge = Gauge::peak("backend_backlog_max");
    static API_IN_FLIGHT_MAX: Gauge = Gauge::peak("backend_api_in_flight_max");
    static PERMIT_WAIT_MAX_MS: Gauge = Gauge::peak("backend_api_permit_wait_max_ms");
    static RUNTIME_LAG_MAX_MS: Gauge = Gauge::peak("backend_runtime_lag_max_ms");
    static RUNTIME_QUEUE_MAX: Gauge = Gauge::peak("backend_runtime_global_queue_max");
    static RUNTIME_TASKS_MAX: Gauge = Gauge::peak("backend_runtime_tasks_max");
    static RUNTIME_TASKS: Gauge = Gauge::last("backend_runtime_tasks");
    static RUNTIME_WORKERS: Gauge = Gauge::last("backend_runtime_workers");
    static RUNTIME_BUSY_PCT: Gauge = Gauge::last("backend_runtime_busy_pct");

    static LOST: Throttle = Throttle::new();
    static SLOW_COMMAND_REPORT: Throttle = Throttle::new();
    static VOLUME_PREVIEW: Throttle = Throttle::new();
    static PLAYER_FAILED_REPORT: Throttle = Throttle::new();
    static FAILED_REQUEST_REPORT: Throttle = Throttle::new();
    static SLOW_REQUEST_REPORT: Throttle = Throttle::new();
    static SLOW_JOB_REPORT: Throttle = Throttle::new();
    static PERMIT_REPORT: Throttle = Throttle::new();
    static REPEAT_REPORT: Throttle = Throttle::new();
    static LAG_REPORT: Throttle = Throttle::new();
    static LAG_SPIKE: Throttle = Throttle::new();

    /// When the interface sent each command, by kind, so the loop can tell
    /// how long one waited. The interface's commands of one kind arrive in
    /// the order it sent them; the loop's own commands match none.
    static SENT: Mutex<VecDeque<(Kind, Instant)>> = Mutex::new(VecDeque::new());
    /// `Reconnect`s the interface asked for that the loop has not taken.
    static APP_RECONNECTS: AtomicU64 = AtomicU64::new(0);
    /// The interface asked the backend to stop, so a closed channel after
    /// it is expected.
    static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);
    /// The latest connection attempt's engine number: events from a lower
    /// number come from an engine already replaced.
    static ENGINE_ATTEMPT: AtomicU64 = AtomicU64::new(0);
    /// `telemetry::uptime_ms` when a streaming session last ended on its own.
    static SESSION_LOST_AT: AtomicU64 = AtomicU64::new(0);
    static NEXT_REQUEST: AtomicU64 = AtomicU64::new(0);
    static IN_FLIGHT: AtomicI64 = AtomicI64::new(0);
    static REMOTE_SEQ: AtomicU64 = AtomicU64::new(0);
    /// `telemetry::uptime_ms` of the last poll of the playback state, the
    /// queue and the device list.
    static LAST_POLL: [AtomicU64; 3] = [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
    /// Requests by kind since the last report.
    static WINDOW: Mutex<BTreeMap<Kind, Tally>> = Mutex::new(BTreeMap::new());
    /// How often each read was asked for lately, by a hash of what it reads.
    static REPEATS: Mutex<BTreeMap<u64, (u32, Instant)>> = Mutex::new(BTreeMap::new());
    /// A digest of the device the Web API last named active.
    static ACTIVE_DEVICE: Mutex<Option<String>> = Mutex::new(None);

    pub(super) fn register() {
        telemetry::register_counters(&[
            &COMMANDS,
            &COMMANDS_SLOW,
            &COMMANDS_PARKED,
            &PLAYER_COMMANDS,
            &PLAYER_FAILED,
            &ENGINE_EVENTS,
            &ENGINE_EVENTS_OLD,
            &ENGINE_EVENTS_STALE,
            &RECONNECTS,
            &API_DISPATCHED,
            &API_FAILED,
            &API_RATE_LIMITED,
            &API_ABORTED,
            &SESSION_READS,
            &SESSION_FALLBACKS,
            &POLLS_PLAYBACK,
            &POLLS_QUEUE,
            &POLLS_DEVICES,
            &REMOTE_COMMANDS,
            &JOBS,
            &TIMERS_LATE,
        ]);
        telemetry::register_gauges(&[
            &QUEUE_WAIT_MAX_MS,
            &HANDLE_MAX_MS,
            &BACKLOG_MAX,
            &API_IN_FLIGHT_MAX,
            &PERMIT_WAIT_MAX_MS,
            &RUNTIME_LAG_MAX_MS,
            &RUNTIME_QUEUE_MAX,
            &RUNTIME_TASKS_MAX,
            &RUNTIME_TASKS,
            &RUNTIME_WORKERS,
            &RUNTIME_BUSY_PCT,
        ]);
    }

    fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn millis(duration: Duration) -> i64 {
        i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
    }

    /// Milliseconds since `at` (a `telemetry::uptime_ms` stamp), within
    /// the aftermath window.
    fn since(at: &AtomicU64) -> Option<u64> {
        let at = at.load(Relaxed);
        let gap = telemetry::uptime_ms().saturating_sub(at);
        let window = u64::try_from(AFTERMATH.as_millis()).unwrap_or(u64::MAX);
        (at != 0 && gap <= window).then_some(gap)
    }

    // ---- the command loop ----------------------------------------------------

    /// The interface sends a command.
    pub(super) fn sent(command: &Command) {
        if !telemetry::enabled() {
            return;
        }
        if matches!(command, Command::Reconnect) {
            APP_RECONNECTS.fetch_add(1, Relaxed);
            return;
        }
        if matches!(command, Command::Shutdown) {
            SHUTTING_DOWN.store(true, Relaxed);
        }
        let mut sent = lock(&SENT);
        if sent.len() >= 512 {
            sent.pop_front();
        }
        sent.push_back((command.kind(), Instant::now()));
    }

    /// How long a command the interface sent waited for the loop.
    pub(super) fn waited(kind: Kind, received: Instant) -> Option<Duration> {
        if !telemetry::enabled() {
            return None;
        }
        let mut sent = lock(&SENT);
        let index = sent.iter().position(|(sent_kind, _)| *sent_kind == kind)?;
        sent.remove(index)
            .map(|(_, at)| received.saturating_duration_since(at))
    }

    /// The backend thread is gone, so nothing the interface asks happens.
    pub(super) fn command_lost(kind: Kind) {
        if !SHUTTING_DOWN.load(Relaxed) && LOST.ready(Duration::from_secs(60)) {
            telemetry::anomaly("backend.command_lost")
                .field("kind", kind)
                .emit();
        }
    }

    /// A command held until the saved proxy is restored.
    pub(super) fn parked(kind: Kind, parked: usize) {
        if !telemetry::enabled() {
            return;
        }
        COMMANDS_PARKED.incr();
        telemetry::crumb("backend.command_parked")
            .field("kind", kind)
            .field("parked", parked)
            .emit();
    }

    /// Why the session is being reconnected: the interface asks after a
    /// burst of unplayable songs, the engine when its session ended.
    pub(super) fn reconnect_reason() -> &'static str {
        if APP_RECONNECTS
            .fetch_update(Relaxed, Relaxed, |count| count.checked_sub(1))
            .is_ok()
        {
            "unavailable_burst"
        } else {
            "session_ended"
        }
    }

    /// Reports how long the loop spent on a command when it moves on,
    /// however the command's arm ends.
    pub(super) struct Handling {
        kind: Kind,
        received: Instant,
        waited: Option<Duration>,
    }

    pub(super) fn handling(
        kind: Kind,
        received: Instant,
        waited: Option<Duration>,
    ) -> Option<Handling> {
        if telemetry::enabled() {
            Some(Handling {
                kind,
                received,
                waited,
            })
        } else {
            None
        }
    }

    impl Drop for Handling {
        fn drop(&mut self) {
            let handled = self.received.elapsed();
            HANDLE_MAX_MS.raise(millis(handled));
            if handled < SLOW_HANDLING && self.waited.unwrap_or_default() < SLOW_QUEUE {
                return;
            }
            COMMANDS_SLOW.incr();
            if SLOW_COMMAND_REPORT.ready(Duration::from_secs(1)) {
                telemetry::event("backend.command_slow")
                    .field("kind", self.kind)
                    .field("queue_wait_ms", self.waited.map(duration_ms))
                    .ms("handle_ms", handled)
                    .emit();
            }
        }
    }

    // ---- the engine's lifecycle ----------------------------------------------

    /// What telemetry follows across the engine's lifecycle, kept by the
    /// command loop.
    #[derive(Default)]
    pub(super) struct Lifecycle {
        /// Connection attempts so far: attempt n makes engine n.
        pub(super) engine_seq: u64,
        /// Why the engine is being replaced or connected, until a connection
        /// attempt takes it.
        pub(super) reason: Option<&'static str>,
        connect_reason: &'static str,
        connect_started: Option<Instant>,
        /// The engine playing now, and since when.
        live_seq: u64,
        live_since: Option<Instant>,
        reconnect_at: Option<Instant>,
        commands_since_reconnect: u32,
        proxy_restore_started: Option<Instant>,
    }

    impl Lifecycle {
        /// The loop takes a command. Playback controls ship, and mark the
        /// trace the interface opened for them; the rest stay in the
        /// recorder. Web API reads report themselves when they answer.
        pub(super) fn received(
            &mut self,
            command: &Command,
            kind: Kind,
            waited: Option<Duration>,
            backlog: usize,
            engine: Option<&Engine>,
            engine_busy: bool,
        ) {
            if !telemetry::enabled() {
                return;
            }
            COMMANDS.incr();
            BACKLOG_MAX.raise(i64::try_from(backlog).unwrap_or(i64::MAX));
            if let Some(waited) = waited {
                QUEUE_WAIT_MAX_MS.raise(millis(waited));
            }
            let (name, trace, intents, device, ship) = match command {
                Command::Player(player) => {
                    PLAYER_COMMANDS.incr();
                    if self.reconnect_at.is_some_and(|at| at.elapsed() < AFTERMATH) {
                        self.commands_since_reconnect += 1;
                    }
                    if matches!(player, PlayerCommand::VolumePreview(_))
                        && !VOLUME_PREVIEW.ready(Duration::from_millis(250))
                    {
                        return;
                    }
                    note_target("local");
                    let name = player_kind(player);
                    let ship = !matches!(
                        player,
                        PlayerCommand::AddToQueue(_) | PlayerCommand::VolumePreview(_)
                    );
                    (
                        Some(name),
                        player_trace(name),
                        player_intents(player),
                        None,
                        ship,
                    )
                }
                Command::Api(request) => {
                    let Some(intents) = control_intents(request) else {
                        return;
                    };
                    let device = device_role(control_device(request), engine);
                    note_target(if device == "local" { "local" } else { "remote" });
                    (
                        Some(request.kind()),
                        control_trace(request),
                        Some(intents),
                        Some(device),
                        true,
                    )
                }
                Command::ApiFinished { .. }
                | Command::Accent { .. }
                | Command::AlbumTypeResolved { .. } => return,
                _ => (None, None, None, None, false),
            };
            if let Some(trace) = trace {
                telemetry::trace_mark(trace, "backend_received");
            }
            let intent =
                intents.and_then(|actions| telemetry::recent_intent(actions, INTENT_WINDOW));
            let record = if ship {
                telemetry::event("backend.command")
            } else {
                telemetry::crumb("backend.command")
            };
            with_intent(record, intent.as_ref())
                .field("kind", kind)
                .field("command", name)
                .field("device", device)
                .field("queue_wait_ms", waited.map(duration_ms))
                .field("backlog", backlog)
                .field("engine", engine.is_some())
                .field("engine_busy", engine_busy)
                .field("engine_seq", self.live_seq)
                .emit();
        }

        /// A connection attempt starts: the new engine's number, and why.
        pub(super) fn connecting(&mut self) -> (u64, &'static str) {
            self.engine_seq += 1;
            ENGINE_ATTEMPT.store(self.engine_seq, Relaxed);
            self.connect_started = Some(Instant::now());
            self.connect_reason = self.reason.take().unwrap_or("start");
            (self.engine_seq, self.connect_reason)
        }

        /// The engine is to be replaced, now or once the attempt in flight
        /// ends. Taking it down stops the audio, so that is noted as the
        /// cause of the pause it makes.
        pub(super) fn replacing(&self, deferred: bool, engine: bool) {
            if !telemetry::enabled() {
                return;
            }
            let reason = self.reason.unwrap_or("unknown");
            if engine && !deferred {
                telemetry::note_cause("engine:reconnect", reason);
            }
            telemetry::event("engine.replace")
                .field("reason", reason)
                .field("deferred", deferred)
                .field("engine", engine)
                .field("engine_seq", self.live_seq)
                .field(
                    "engine_uptime_ms",
                    self.live_since.map(|at| duration_ms(at.elapsed())),
                )
                .emit();
        }

        /// The engine is down; `resume` is what its replacement picks up.
        pub(super) fn teardown(&mut self, resume: Option<&PlaybackResume>, volume: u16) {
            let live_since = self.live_since.take();
            if !telemetry::enabled() {
                return;
            }
            let event = telemetry::event("engine.teardown")
                .field("reason", self.reason.unwrap_or("unknown"))
                .field("engine_seq", self.live_seq)
                .field(
                    "engine_uptime_ms",
                    live_since.map(|at| duration_ms(at.elapsed())),
                )
                .field("volume", volume);
            with_resume(event, resume_facts(resume)).emit();
        }

        /// A new engine is up. `pending_reads` are the session reads that
        /// start with it and compete with the resumed song.
        pub(super) fn ready(
            &mut self,
            drops: usize,
            resume: Option<&PlaybackResume>,
            rootlist: bool,
            album_types: usize,
            pending_reads: usize,
        ) {
            self.live_seq = self.engine_seq;
            self.live_since = Some(Instant::now());
            if !telemetry::enabled() {
                return;
            }
            let event = telemetry::event("engine.ready")
                .field("engine_seq", self.engine_seq)
                .field("reason", self.connect_reason)
                .field(
                    "connect_ms",
                    self.connect_started.map(|at| duration_ms(at.elapsed())),
                )
                .field("since_reconnect_ms", self.since_reconnect())
                .field("since_lost_ms", since(&SESSION_LOST_AT))
                .field("drops_in_window", drops)
                .field("rootlist_pending", rootlist)
                .field("album_types_pending", album_types)
                .field("session_reads_pending", pending_reads);
            with_resume(event, resume_facts(resume)).emit();
        }

        /// A connection attempt brought no usable engine.
        pub(super) fn failed(&self, class: &'static str, error: Option<&str>) {
            if !telemetry::enabled() {
                return;
            }
            let mut event = telemetry::event("engine.failed")
                .field("class", class)
                .field("engine_seq", self.engine_seq)
                .field("reason", self.connect_reason)
                .field(
                    "connect_ms",
                    self.connect_started.map(|at| duration_ms(at.elapsed())),
                )
                .field("since_reconnect_ms", self.since_reconnect());
            if let Some(error) = error {
                event = event.text("error", error);
            }
            event.emit();
        }

        /// A finished connection is thrown away: the sign-in or the
        /// settings changed while it was in flight.
        pub(super) fn discarded(&self, why: &'static str) {
            telemetry::event("engine.connect_discarded")
                .field("why", why)
                .field("engine_seq", self.engine_seq)
                .field("reason", self.connect_reason)
                .emit();
        }

        /// What the loop did with a `Reconnect`: `reconnecting`, or why not.
        pub(super) fn reconnect(
            &mut self,
            reason: &'static str,
            decision: &'static str,
            drops: usize,
        ) {
            if matches!(decision, "reconnecting" | "exhausted") {
                self.reason = Some(reason);
            }
            if decision == "reconnecting" {
                self.reconnect_at = Some(Instant::now());
                self.commands_since_reconnect = 0;
            }
            if !telemetry::enabled() {
                return;
            }
            let event = if decision == "exhausted" {
                telemetry::note_cause("engine:shutdown", "session drops exhausted");
                telemetry::anomaly("session.reconnect_exhausted")
            } else {
                if decision == "reconnecting" {
                    RECONNECTS.incr();
                }
                telemetry::event("session.reconnect")
            };
            event
                .field("reason", reason)
                .field("decision", decision)
                .field("drops_in_window", drops)
                .field("since_lost_ms", since(&SESSION_LOST_AT))
                .field("engine_seq", self.live_seq)
                .emit();
        }

        /// About to load the playback from before a drop again: whatever
        /// starts playing next was not the user's doing.
        pub(super) fn restoring(&self, resume: &PlaybackResume) {
            if !telemetry::enabled() {
                return;
            }
            let kind = resume_facts(Some(resume)).kind.unwrap_or("unknown");
            telemetry::note_cause("engine:restore", kind);
        }

        /// One check of a pickup after a reconnect.
        pub(super) fn restore(
            &self,
            outcome: &'static str,
            attempt: u8,
            resume: &PlaybackResume,
            error: Option<&anyhow::Error>,
        ) {
            if !telemetry::enabled() {
                return;
            }
            let event = if outcome == "gave_up" {
                telemetry::anomaly("engine.restore_gave_up")
            } else {
                telemetry::event("engine.restore")
            };
            let mut event = with_resume(event, resume_facts(Some(resume)))
                .field("outcome", outcome)
                .field("attempt", attempt)
                .field("engine_seq", self.live_seq)
                .field(
                    "since_ready_ms",
                    self.live_since.map(|at| duration_ms(at.elapsed())),
                )
                .field("since_reconnect_ms", self.since_reconnect())
                .field("commands_since_reconnect", self.commands_since_reconnect);
            if let Some(error) = error {
                event = event.text("error", &error.to_string());
            }
            event.emit();
        }

        pub(super) fn proxy_restoring(&mut self) {
            self.proxy_restore_started = Some(Instant::now());
        }

        /// The saved proxy is restored and the commands held for it go on.
        pub(super) fn proxy_gate(&mut self, parked: usize) {
            let started = self.proxy_restore_started.take();
            telemetry::event("backend.proxy_gate")
                .field("parked", parked)
                .field("waited_ms", started.map(|at| duration_ms(at.elapsed())))
                .emit();
        }

        fn since_reconnect(&self) -> Option<f64> {
            self.reconnect_at
                .filter(|at| at.elapsed() < AFTERMATH)
                .map(|at| duration_ms(at.elapsed()))
        }
    }

    /// An engine event on its way to the interface. Runs on the player's
    /// threads too, so counters only.
    pub(super) fn engine_event(engine_seq: u64, current_lease: bool) {
        if !current_lease {
            ENGINE_EVENTS_STALE.incr();
            return;
        }
        ENGINE_EVENTS.incr();
        if engine_seq < ENGINE_ATTEMPT.load(Relaxed) {
            ENGINE_EVENTS_OLD.incr();
        }
    }

    /// The engine's streaming session ended on its own.
    pub(super) fn session_lost(engine_seq: u64) {
        if !telemetry::enabled() {
            return;
        }
        SESSION_LOST_AT.store(telemetry::uptime_ms().max(1), Relaxed);
        telemetry::note_cause("session:lost", "the streaming session ended");
        telemetry::event("session.lost")
            .field("engine_seq", engine_seq)
            .field("old_engine", engine_seq < ENGINE_ATTEMPT.load(Relaxed))
            .emit();
    }

    /// A connection attempt ended: `ok`, `error`, `timeout` or
    /// `cache_error`. Never the credentials or the proxy's address.
    pub(super) fn engine_connected(
        (engine_seq, reason): (u64, &'static str),
        started: Instant,
        opened: Option<Duration>,
        outcome: &'static str,
        error: Option<&anyhow::Error>,
        config: &EngineConfig,
    ) {
        if !telemetry::enabled() {
            return;
        }
        let mut event = telemetry::event("engine.connect")
            .field("engine_seq", engine_seq)
            .field("reason", reason)
            .field("outcome", outcome)
            .since("duration_ms", started)
            .field("open_cache_ms", opened.map(duration_ms))
            .field("bitrate_kbps", config.bitrate_kbps)
            .field("gapless", config.gapless)
            .field("normalisation", config.normalisation)
            .field("buffer_ms", config.buffer_ms)
            .field("audio_backend", config.backend.clone())
            .field(
                "audio_device",
                config.audio_device.as_deref().map(telemetry::digest),
            )
            .field("audio_cache", config.audio_cache_dir.is_some())
            .field("proxy", proxy_kind(&config.proxy));
        if let Some(error) = error {
            let text = format!("{error:#}");
            event = event
                .field("error_class", connect_error_class(&text))
                .text("error", &text);
        }
        event.emit();
    }

    fn connect_error_class(text: &str) -> &'static str {
        let lower = text.to_lowercase();
        if lower.contains("badcredentials") || lower.contains("bad credentials") {
            "bad_credentials"
        } else if lower.contains("premium") {
            "premium"
        } else if lower.contains("timed out") || lower.contains("timeout") {
            "timeout"
        } else if [
            "dns",
            "resolve",
            "refused",
            "unreachable",
            "network",
            "connection",
        ]
        .iter()
        .any(|word| lower.contains(word))
        {
            "network"
        } else {
            "other"
        }
    }

    fn proxy_kind(proxy: &ProxyConfig) -> &'static str {
        match proxy {
            ProxyConfig::Invalid(_) => "invalid",
            ProxyConfig::Off => "off",
            ProxyConfig::System => "system",
            ProxyConfig::Http(_) => "http",
            ProxyConfig::Socks(_) => "socks5",
        }
    }

    /// The audio settings changed, so the engine restarts and only the
    /// current song comes back.
    pub(super) fn engine_restart(old: &EngineConfig, new: &EngineConfig, engine: bool) {
        if !telemetry::enabled() {
            return;
        }
        let changes = [
            ("device_name", old.device_name != new.device_name),
            ("bitrate", old.bitrate_kbps != new.bitrate_kbps),
            ("normalisation", old.normalisation != new.normalisation),
            ("autoplay", old.autoplay != new.autoplay),
            ("gapless", old.gapless != new.gapless),
            ("audio_backend", old.backend != new.backend),
            ("audio_device", old.audio_device != new.audio_device),
            ("buffer", old.buffer_ms != new.buffer_ms),
            (
                "cache",
                old.audio_cache_dir != new.audio_cache_dir
                    || old.audio_cache_limit != new.audio_cache_limit,
            ),
            ("volume", old.initial_volume != new.initial_volume),
        ];
        let changed: Vec<&str> = changes
            .iter()
            .filter(|(_, changed)| *changed)
            .map(|(name, _)| *name)
            .collect();
        telemetry::event("engine.restart_requested")
            .field("changed", changed)
            .field("engine", engine)
            .field("bitrate_kbps", new.bitrate_kbps)
            .field("gapless", new.gapless)
            .field("normalisation", new.normalisation)
            .field("buffer_ms", new.buffer_ms)
            .field("audio_backend", new.backend.clone())
            .field(
                "audio_device",
                new.audio_device.as_deref().map(telemetry::digest),
            )
            .emit();
    }

    /// A playback command that never reached the engine, or that it
    /// refused: its trace ends here, since no audio follows.
    pub(super) fn player_failed(
        command: Kind,
        why: &'static str,
        error: Option<&anyhow::Error>,
        engine_busy: bool,
    ) {
        if !telemetry::enabled() {
            return;
        }
        PLAYER_FAILED.incr();
        if let Some(trace) = player_trace(command) {
            telemetry::trace_end(trace, "failed");
        }
        if command == "VolumePreview" && !PLAYER_FAILED_REPORT.ready(Duration::from_secs(1)) {
            return;
        }
        let mut event = telemetry::event("backend.player_command_failed")
            .field("command", command)
            .field("why", why)
            .field("engine_busy", engine_busy)
            .field("since_lost_ms", since(&SESSION_LOST_AT));
        if let Some(error) = error {
            event = event.text("error", &error.to_string());
        }
        event.emit();
    }

    /// What a pickup after a reconnect restores.
    #[derive(Clone, Copy, Default)]
    struct Resume {
        kind: Option<&'static str>,
        playing: Option<bool>,
        position_ms: Option<u64>,
    }

    fn resume_facts(resume: Option<&PlaybackResume>) -> Resume {
        match resume {
            None => Resume::default(),
            Some(PlaybackResume::Track(spec)) => Resume {
                kind: Some("track"),
                playing: Some(spec.play),
                position_ms: Some(u64::from(spec.position_ms)),
            },
            Some(PlaybackResume::Session(snapshot)) => {
                // The snapshot keeps its fields private; its `Debug` shows
                // these two and nothing else.
                let text = format!("{snapshot:?}");
                Resume {
                    kind: Some("session"),
                    playing: debug_field(&text, "playing")
                        .and_then(|value| value.parse::<bool>().ok()),
                    position_ms: debug_field(&text, "position_ms")
                        .and_then(|value| value.parse::<u64>().ok()),
                }
            }
        }
    }

    fn debug_field<'a>(text: &'a str, key: &str) -> Option<&'a str> {
        let start = text.find(key)? + key.len();
        let rest = text[start..].strip_prefix(": ")?;
        rest.split([',', ' ', '}']).next()
    }

    fn with_resume(event: Event, resume: Resume) -> Event {
        event
            .field("resume", resume.kind)
            .field("resume_playing", resume.playing)
            .field("resume_position_ms", resume.position_ms)
    }

    fn with_intent(event: Event, intent: Option<&Intent>) -> Event {
        event
            .field("intent_id", intent.map(|intent| intent.id.clone()))
            .field("intent_source", intent.map(|intent| intent.source))
            .field(
                "since_intent_ms",
                intent.map(|intent| duration_ms(intent.at.elapsed())),
            )
    }

    // ---- sign-in ---------------------------------------------------------------

    fn source_name(source: ApiSource) -> &'static str {
        match source {
            ApiSource::Shared => "shared",
            ApiSource::Personal => "personal",
        }
    }

    /// A grant was refused, so its traffic moves to the other app or stops.
    pub(super) fn grant_expired(source: ApiSource) {
        telemetry::event("auth.grant_expired")
            .field("source", source_name(source))
            .emit();
    }

    /// The sign-in check (`/me`) ended, after `tries`.
    pub(super) fn verified(
        source: ApiSource,
        tries: u32,
        started: Instant,
        error: Option<&ApiError>,
    ) {
        let event = match error {
            None => telemetry::crumb("auth.verified"),
            Some(error) => telemetry::event("auth.verify_failed")
                .field("outcome", api_error_class(error))
                .field("status", error.status()),
        };
        event
            .field("source", source_name(source))
            .field("tries", tries)
            .since("duration_ms", started)
            .emit();
    }

    pub(super) fn verify_retry(source: ApiSource, tries: u32, wait: Duration, error: &ApiError) {
        telemetry::event("auth.verify_retry")
            .field("source", source_name(source))
            .field("tries", tries)
            .ms("wait_ms", wait)
            .field("outcome", api_error_class(error))
            .field("status", error.status())
            .emit();
    }

    pub(super) fn verification_failed(source: ApiSource, error: &ApiError, signs_out: bool) {
        telemetry::event("auth.verification_failed")
            .field("source", source_name(source))
            .field("outcome", api_error_class(error))
            .field("status", error.status())
            .field("signs_out", signs_out)
            .emit();
    }

    pub(super) fn personal_app(enabled: bool, authorization_in_flight: bool) {
        telemetry::crumb("auth.personal_app")
            .field("enabled", enabled)
            .field("authorization_in_flight", authorization_in_flight)
            .emit();
    }

    pub(super) fn signed_out(engine: bool) {
        if !telemetry::enabled() {
            return;
        }
        if engine {
            telemetry::note_cause("engine:shutdown", "sign out");
        }
        telemetry::event("auth.signed_out")
            .field("engine", engine)
            .emit();
    }

    /// The plan is known. A Free account takes local playback down.
    pub(super) fn premium_checked(premium: Option<bool>, shuts_down: bool) {
        if !telemetry::enabled() {
            return;
        }
        if shuts_down {
            telemetry::note_cause("engine:shutdown", "not premium");
        }
        let event = if premium == Some(false) {
            telemetry::event("auth.premium_checked")
        } else {
            telemetry::crumb("auth.premium_checked")
        };
        event
            .field("premium", premium)
            .field("engine_shutdown", shuts_down)
            .emit();
    }

    // ---- receivers on the local network ---------------------------------------

    /// A browse for Connect receivers ended. The error is its outermost
    /// context only, which names no address.
    pub(super) fn receivers(started: Instant, found: Result<usize, &anyhow::Error>) {
        if !telemetry::enabled() {
            return;
        }
        let event = telemetry::event("connect.receivers").since("duration_ms", started);
        let event = match found {
            Ok(count) => event.field("outcome", "ok").field("found", count),
            Err(error) => event
                .field("outcome", "error")
                .text("error", &error.to_string()),
        };
        event.emit();
    }

    pub(super) fn receiver_activated(
        name: &str,
        started: Instant,
        result: &Result<(), String>,
        has_credentials: bool,
    ) {
        if !telemetry::enabled() {
            return;
        }
        let mut event = telemetry::event("connect.receiver_activated")
            .field("receiver", telemetry::digest(name))
            .field("has_credentials", has_credentials)
            .field("outcome", if result.is_ok() { "ok" } else { "error" })
            .since("duration_ms", started);
        if let Err(error) = result {
            event = event.text("error", error);
        }
        event.emit();
    }

    // ---- jobs, timers and the runtime ------------------------------------------

    /// A background job ended: a session read, a cache write, an update
    /// check. `requests` is how many it made.
    pub(super) fn job(name: &'static str, started: Instant, ok: bool, requests: usize) {
        if !telemetry::enabled() {
            return;
        }
        JOBS.incr();
        let elapsed = started.elapsed();
        let event = if elapsed >= SLOW_JOB && SLOW_JOB_REPORT.ready(Duration::from_secs(5)) {
            telemetry::event("backend.job_slow")
        } else {
            telemetry::crumb("backend.job")
        };
        event
            .field("job", name)
            .field("ok", ok)
            .field("requests", requests)
            .ms("duration_ms", elapsed)
            .emit();
    }

    /// A timer asked to wait `delay` from `started` has fired.
    pub(super) fn timer_late(timer: &'static str, delay: Duration, started: Instant) {
        if !telemetry::enabled() {
            return;
        }
        let late = started.elapsed().saturating_sub(delay);
        if late >= Duration::from_millis(500) {
            TIMERS_LATE.incr();
            telemetry::event("backend.timer_late")
                .field("timer", timer)
                .ms("delay_ms", delay)
                .ms("late_ms", late)
                .emit();
        }
    }

    /// A background request waited `asked` until now for one of the four
    /// background slots, which are held through Spotify's cooldowns.
    pub(super) fn permit_waited(kind: Kind, asked: Instant) {
        if !telemetry::enabled() {
            return;
        }
        let waited = asked.elapsed();
        PERMIT_WAIT_MAX_MS.raise(millis(waited));
        if waited >= Duration::from_secs(1) && PERMIT_REPORT.ready(Duration::from_secs(5)) {
            telemetry::crumb("api.permit_wait")
                .field("kind", kind)
                .ms("waited_ms", waited)
                .emit();
        }
    }

    /// Measures how late the runtime wakes a sleeping task, which delays
    /// Spotify Connect, the audio download and every Web API answer alike,
    /// and reports the runtime's load and the last half minute's requests.
    pub(super) async fn watch_runtime() {
        let runtime = tokio::runtime::Handle::current().metrics();
        let mut window = Instant::now();
        #[cfg(target_has_atomic = "64")]
        let mut busy = busy_time(&runtime);
        while telemetry::enabled() {
            let asked = Instant::now();
            tokio::time::sleep(RUNTIME_TICK).await;
            let lag = asked.elapsed().saturating_sub(RUNTIME_TICK);
            let queued = runtime.global_queue_depth();
            let tasks = runtime.num_alive_tasks();
            RUNTIME_LAG_MAX_MS.raise(millis(lag));
            RUNTIME_QUEUE_MAX.raise(i64::try_from(queued).unwrap_or(i64::MAX));
            RUNTIME_TASKS_MAX.raise(i64::try_from(tasks).unwrap_or(i64::MAX));
            if lag >= RUNTIME_LAG_ANOMALY {
                // The watch thread woke late too: the whole process was
                // throttled (App Nap) or starved, not this runtime.
                if telemetry::timer_lag_within(Duration::from_secs(10)) {
                    if LAG_SPIKE.ready(Duration::from_secs(5)) {
                        telemetry::crumb("backend.runtime_lag")
                            .ms("lag_ms", lag)
                            .field("process_throttled", true)
                            .field("global_queue_depth", queued)
                            .field("alive_tasks", tasks)
                            .emit();
                    }
                } else if LAG_REPORT.ready(Duration::from_secs(60)) {
                    telemetry::anomaly("backend.runtime_lag")
                        .ms("lag_ms", lag)
                        .field("global_queue_depth", queued)
                        .field("alive_tasks", tasks)
                        .field("workers", runtime.num_workers())
                        .emit();
                }
            } else if lag >= RUNTIME_LAG_CRUMB && LAG_SPIKE.ready(Duration::from_secs(5)) {
                telemetry::crumb("backend.runtime_lag")
                    .ms("lag_ms", lag)
                    .field("global_queue_depth", queued)
                    .field("alive_tasks", tasks)
                    .emit();
            }
            if window.elapsed() >= RUNTIME_REPORT {
                let elapsed = window.elapsed();
                window = Instant::now();
                RUNTIME_WORKERS.set(i64::try_from(runtime.num_workers()).unwrap_or(i64::MAX));
                RUNTIME_TASKS.set(i64::try_from(tasks).unwrap_or(i64::MAX));
                #[cfg(target_has_atomic = "64")]
                {
                    let now = busy_time(&runtime);
                    let capacity = elapsed.as_secs_f64() * runtime.num_workers().max(1) as f64;
                    if capacity > 0.0 {
                        let share = now.saturating_sub(busy).as_secs_f64() / capacity;
                        RUNTIME_BUSY_PCT.set((share * 100.0).round() as i64);
                    }
                    busy = now;
                }
                report_window(elapsed);
            }
        }
    }

    #[cfg(target_has_atomic = "64")]
    fn busy_time(runtime: &tokio::runtime::RuntimeMetrics) -> Duration {
        (0..runtime.num_workers())
            .map(|worker| runtime.worker_total_busy_duration(worker))
            .sum()
    }

    // ---- Web API requests ------------------------------------------------------

    /// Requests of one kind in the current window.
    #[derive(Clone, Copy, Default)]
    struct Tally {
        sent: u32,
        failed: u32,
        limited: u32,
        session: u32,
    }

    fn count(kind: Kind, update: impl FnOnce(&mut Tally)) {
        update(lock(&WINDOW).entry(kind).or_default());
    }

    /// The window's requests by kind: which features spend the quota.
    fn report_window(elapsed: Duration) {
        let window = std::mem::take(&mut *lock(&WINDOW));
        if window.is_empty() {
            return;
        }
        let mut total = Tally::default();
        let mut sent = serde_json::Map::new();
        let mut failed = serde_json::Map::new();
        for (kind, tally) in window {
            total.sent += tally.sent;
            total.failed += tally.failed;
            total.limited += tally.limited;
            total.session += tally.session;
            if tally.sent > 0 {
                sent.insert(kind.to_owned(), tally.sent.into());
            }
            if tally.failed > 0 {
                failed.insert(kind.to_owned(), tally.failed.into());
            }
        }
        let mut event = telemetry::event("backend.api_requests")
            .ms("window_ms", elapsed)
            .field("requests", total.sent)
            .field("failed", total.failed)
            .field("rate_limited", total.limited)
            .field("from_session", total.session)
            .field("by_kind", sent);
        if !failed.is_empty() {
            event = event.field("failed_by_kind", failed);
        }
        event.emit();
    }

    /// One Web API request on its way, from the command loop to its answer.
    /// Dropped unanswered (a sign-out, a superseded search), it reports
    /// itself aborted.
    pub(super) struct ApiTrack {
        id: u64,
        kind: Kind,
        operation: String,
        personal_ready: bool,
        background: bool,
        correlation: Option<u64>,
        offset: Option<u32>,
        batch: Option<usize>,
        started: Instant,
        finished: bool,
    }

    impl ApiTrack {
        pub(super) fn begin(request: &ApiRequest, api: &ApiGateway) -> Option<Self> {
            if !telemetry::enabled() {
                return None;
            }
            let kind = request.kind();
            API_DISPATCHED.incr();
            API_IN_FLIGHT_MAX.raise(IN_FLIGHT.fetch_add(1, Relaxed) + 1);
            count(kind, |tally| tally.sent += 1);
            polled(request, kind);
            repeated(request, kind);
            Some(Self {
                id: NEXT_REQUEST.fetch_add(1, Relaxed) + 1,
                kind,
                operation: format!("{:?}", super::operation_for(api, request)),
                personal_ready: api.personal_ready(),
                background: request.background(),
                correlation: correlation(request),
                offset: page_offset(request),
                batch: batch(request),
                started: Instant::now(),
                finished: false,
            })
        }

        pub(super) fn finish(mut self, response: &ApiResponse) {
            self.finished = true;
            let elapsed = self.started.elapsed();
            let error = response_error(response);
            let outcome = error.map_or("ok", api_error_class);
            if let Some(error) = error {
                API_FAILED.incr();
                let limited = matches!(error, ApiError::RateLimited | ApiError::QuotaExhausted);
                if limited {
                    API_RATE_LIMITED.incr();
                }
                count(self.kind, |tally| {
                    tally.failed += 1;
                    if limited {
                        tally.limited += 1;
                    }
                });
                if !self.background && FAILED_REQUEST_REPORT.ready(Duration::from_secs(2)) {
                    let mut event = self
                        .describe(telemetry::event("api.request_failed"), outcome, elapsed)
                        .field("status", error.status());
                    if matches!(error, ApiError::Network(_) | ApiError::Decode(_)) {
                        event = event.text("error", &error.to_string());
                    }
                    event.emit();
                }
            }
            if elapsed >= SLOW_REQUEST && SLOW_REQUEST_REPORT.ready(Duration::from_secs(5)) {
                self.describe(telemetry::event("api.request_slow"), outcome, elapsed)
                    .emit();
            }
            self.describe(telemetry::crumb("api.request"), outcome, elapsed)
                .field("status", error.and_then(ApiError::status))
                .emit();
        }

        fn describe(&self, event: Event, outcome: &'static str, elapsed: Duration) -> Event {
            event
                .field("request_id", self.id)
                .field("kind", self.kind)
                .field("operation", self.operation.as_str())
                .field("personal_ready", self.personal_ready)
                .field("background", self.background)
                .field("correlation", self.correlation)
                .field("offset", self.offset)
                .field("batch", self.batch)
                .field("outcome", outcome)
                .ms("duration_ms", elapsed)
        }
    }

    impl Drop for ApiTrack {
        fn drop(&mut self) {
            IN_FLIGHT.fetch_sub(1, Relaxed);
            if !self.finished {
                API_ABORTED.incr();
                self.describe(
                    telemetry::crumb("api.request"),
                    "aborted",
                    self.started.elapsed(),
                )
                .emit();
            }
        }
    }

    /// A poll of what is playing, the queue, or the devices, and how long
    /// after the previous one of its kind.
    fn polled(request: &ApiRequest, kind: Kind) {
        let (slot, counter) = match request {
            ApiRequest::PlaybackState { .. } => (0, &POLLS_PLAYBACK),
            ApiRequest::Queue { .. } => (1, &POLLS_QUEUE),
            ApiRequest::Devices => (2, &POLLS_DEVICES),
            _ => return,
        };
        counter.incr();
        let now = telemetry::uptime_ms().max(1);
        let last = LAST_POLL[slot].swap(now, Relaxed);
        let gap = if last == 0 {
            None
        } else {
            Some(now.saturating_sub(last))
        };
        telemetry::crumb("api.poll")
            .field("kind", kind)
            .field("gap_ms", gap)
            .emit();
    }

    /// The same read asked for again and again: a retry loop without
    /// backoff, which spends the quota on one answer.
    fn repeated(request: &ApiRequest, kind: Kind) {
        let Some(key) = repeat_key(request) else {
            return;
        };
        let mut hasher = DefaultHasher::new();
        kind.hash(&mut hasher);
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let asked = {
            let mut repeats = lock(&REPEATS);
            if repeats.len() >= 512 {
                repeats.retain(|_, (_, first)| first.elapsed() < REPEAT_WINDOW);
            }
            let entry = repeats.entry(hash).or_insert((0, Instant::now()));
            if entry.1.elapsed() >= REPEAT_WINDOW {
                *entry = (0, Instant::now());
            }
            entry.0 += 1;
            entry.0
        };
        if asked != REPEAT_LIMIT {
            return;
        }
        let event = if REPEAT_REPORT.ready(Duration::from_secs(300)) {
            telemetry::anomaly("api.repeat")
        } else {
            telemetry::crumb("api.repeat")
        };
        event
            .field("kind", kind)
            .field("key", telemetry::digest(&key))
            .field("count", asked)
            .ms("window_ms", REPEAT_WINDOW)
            .emit();
    }

    /// What a read asks for, to tell one asked twice. Never shipped as is.
    fn repeat_key(request: &ApiRequest) -> Option<String> {
        match request {
            ApiRequest::Track { id }
            | ApiRequest::Episode { id }
            | ApiRequest::Album { id }
            | ApiRequest::Artist { id }
            | ApiRequest::Show { id }
            | ApiRequest::ArtistTopTracks { id }
            | ApiRequest::RelatedArtists { id }
            | ApiRequest::Playlist { id, .. } => Some(id.clone()),
            ApiRequest::PlaylistItems { id, offset, .. }
            | ApiRequest::PlaylistSample { id, offset, .. }
            | ApiRequest::AlbumTracks { id, offset, .. }
            | ApiRequest::AlbumQueueTracks { id, offset, .. }
            | ApiRequest::ArtistAlbums { id, offset, .. }
            | ApiRequest::ShowEpisodes { id, offset } => Some(format!("{id}:{offset}")),
            ApiRequest::SavedTracks { offset, .. }
            | ApiRequest::MyPlaylists { offset, .. }
            | ApiRequest::SavedAlbums { offset }
            | ApiRequest::SavedShows { offset }
            | ApiRequest::SavedEpisodes { offset } => Some(offset.to_string()),
            ApiRequest::Contains { uris } | ApiRequest::SetSaved { uris, .. } => {
                Some(uris.join(","))
            }
            _ => None,
        }
    }

    /// The sequence, generation or serial the interface matches the answer
    /// with.
    fn correlation(request: &ApiRequest) -> Option<u64> {
        match request {
            ApiRequest::PlaybackState { seq } | ApiRequest::Queue { seq } => Some(*seq),
            ApiRequest::RecentlyPlayed { generation, .. }
            | ApiRequest::TopTracks { generation, .. }
            | ApiRequest::TopArtists { generation }
            | ApiRequest::Recommendations { generation, .. }
            | ApiRequest::Discover { generation, .. }
            | ApiRequest::MyPlaylists { generation, .. }
            | ApiRequest::Playlist { generation, .. }
            | ApiRequest::PlaylistItems { generation, .. }
            | ApiRequest::PlaylistSample { generation, .. }
            | ApiRequest::SavedTracks { generation, .. }
            | ApiRequest::AlbumTracks { generation, .. }
            | ApiRequest::HomeEpisodes { generation, .. } => Some(*generation),
            ApiRequest::Search { serial, .. }
            | ApiRequest::SearchCatalogue { serial, .. }
            | ApiRequest::SearchPlaylists { serial, .. } => Some(*serial),
            ApiRequest::UploadPlaylistCover { request, .. }
            | ApiRequest::AlbumQueueTracks { request, .. }
            | ApiRequest::AddManyToQueue { request, .. } => Some(*request),
            _ => None,
        }
    }

    fn page_offset(request: &ApiRequest) -> Option<u32> {
        match request {
            ApiRequest::TopTracks { offset, .. }
            | ApiRequest::MyPlaylists { offset, .. }
            | ApiRequest::PlaylistItems { offset, .. }
            | ApiRequest::PlaylistSample { offset, .. }
            | ApiRequest::SavedTracks { offset, .. }
            | ApiRequest::SavedAlbums { offset }
            | ApiRequest::SavedShows { offset }
            | ApiRequest::SavedEpisodes { offset }
            | ApiRequest::ArtistAlbums { offset, .. }
            | ApiRequest::AlbumTracks { offset, .. }
            | ApiRequest::AlbumQueueTracks { offset, .. }
            | ApiRequest::ShowEpisodes { offset, .. } => Some(*offset),
            _ => None,
        }
    }

    /// How many items one request carries.
    fn batch(request: &ApiRequest) -> Option<usize> {
        match request {
            ApiRequest::Contains { uris }
            | ApiRequest::SetSaved { uris, .. }
            | ApiRequest::AddToPlaylist { uris, .. }
            | ApiRequest::RemoveFromPlaylist { uris, .. }
            | ApiRequest::AddManyToQueue { uris, .. } => Some(uris.len()),
            ApiRequest::CheckPlaylistDuplicates { items, .. } => Some(items.len()),
            ApiRequest::HomeEpisodes { shows, .. } => Some(shows.len()),
            _ => None,
        }
    }

    fn response_error(response: &ApiResponse) -> Option<&ApiError> {
        match response {
            ApiResponse::Me(Err(error))
            | ApiResponse::Devices(Err(error))
            | ApiResponse::PlaylistCreated(Err(error))
            | ApiResponse::PlaybackState {
                result: Err(error), ..
            }
            | ApiResponse::Queue {
                result: Err(error), ..
            }
            | ApiResponse::RecentlyPlayed {
                result: Err(error), ..
            }
            | ApiResponse::TopTracks {
                result: Err(error), ..
            }
            | ApiResponse::TopArtists {
                result: Err(error), ..
            }
            | ApiResponse::Recommendations {
                result: Err(error), ..
            }
            | ApiResponse::Discover {
                result: Err(error), ..
            }
            | ApiResponse::MyPlaylists {
                result: Err(error), ..
            }
            | ApiResponse::Playlist {
                result: Err(error), ..
            }
            | ApiResponse::PlaylistItems {
                result: Err(error), ..
            }
            | ApiResponse::PlaylistSample {
                result: Err(error), ..
            }
            | ApiResponse::PlaylistCoverUploaded {
                result: Err(error), ..
            }
            | ApiResponse::PlaylistUpdated {
                result: Err(error), ..
            }
            | ApiResponse::PlaylistDuplicatesChecked {
                result: Err(error), ..
            }
            | ApiResponse::PlaylistItemsChanged {
                result: Err(error), ..
            }
            | ApiResponse::PlaylistFollowChanged {
                result: Err(error), ..
            }
            | ApiResponse::SavedTracks {
                result: Err(error), ..
            }
            | ApiResponse::SavedAlbums {
                result: Err(error), ..
            }
            | ApiResponse::FollowedArtists {
                result: Err(error), ..
            }
            | ApiResponse::SavedShows {
                result: Err(error), ..
            }
            | ApiResponse::SavedEpisodes {
                result: Err(error), ..
            }
            | ApiResponse::SavedChanged {
                result: Err(error), ..
            }
            | ApiResponse::Contains {
                result: Err(error), ..
            }
            | ApiResponse::Search {
                result: Err(error), ..
            }
            | ApiResponse::SearchPlaylists {
                result: Err(error), ..
            }
            | ApiResponse::Artist {
                result: Err(error), ..
            }
            | ApiResponse::ArtistTopTracks {
                result: Err(error), ..
            }
            | ApiResponse::ArtistAlbums {
                result: Err(error), ..
            }
            | ApiResponse::RelatedArtists {
                result: Err(error), ..
            }
            | ApiResponse::Album {
                result: Err(error), ..
            }
            | ApiResponse::AlbumTracks {
                result: Err(error), ..
            }
            | ApiResponse::AlbumQueueTracks {
                result: Err(error), ..
            }
            | ApiResponse::Show {
                result: Err(error), ..
            }
            | ApiResponse::ShowEpisodes {
                result: Err(error), ..
            }
            | ApiResponse::HomeEpisodes {
                result: Err(error), ..
            }
            | ApiResponse::Track {
                result: Err(error), ..
            }
            | ApiResponse::Episode {
                result: Err(error), ..
            }
            | ApiResponse::Remote {
                result: Err(error), ..
            }
            | ApiResponse::Transferred {
                result: Err(error), ..
            }
            | ApiResponse::QueueAdded {
                result: Err(error), ..
            }
            | ApiResponse::QueueBatchAdded {
                result: Err(error), ..
            } => Some(error),
            _ => None,
        }
    }

    fn api_error_class(error: &ApiError) -> &'static str {
        match error {
            ApiError::NotSignedIn => "not_signed_in",
            ApiError::Status { .. } => "status",
            ApiError::RateLimited => "rate_limited",
            ApiError::QuotaExhausted => "quota_exhausted",
            ApiError::SignInExpired { .. } => "sign_in_expired",
            ApiError::Network(_) => "network",
            ApiError::Decode(_) => "decode",
        }
    }

    // ---- where a request goes ----------------------------------------------------

    /// The streaming session answered a playlist read the Web API would
    /// otherwise have spent the shared app's quota on.
    pub(super) fn session_answered(request: &ApiRequest) {
        if telemetry::enabled() {
            count(request.kind(), |tally| tally.session += 1);
        }
    }

    /// A read the session should answer goes to the Web API instead, and why.
    pub(super) fn route(
        api: &ApiGateway,
        operation: Operation,
        engine: Option<&Engine>,
        request: &ApiRequest,
    ) {
        if !telemetry::enabled() || !api.session_serves(operation) {
            return;
        }
        let why = match engine {
            None => "no_engine",
            Some(engine)
                if !super::same_account(&engine.session().username(), api.account().as_ref()) =>
            {
                "other_account"
            }
            Some(_) => "session_failed",
        };
        telemetry::crumb("api.route")
            .field("kind", request.kind())
            .field("operation", format!("{operation:?}"))
            .field("path", "web_api")
            .field("why", why)
            .emit();
    }

    /// One read over the streaming session: `answered`, `definitive` (a
    /// refusal the Web API would repeat), or a fallback to the Web API.
    pub(super) fn session_read(request: &ApiRequest, outcome: &'static str, started: Instant) {
        if !telemetry::enabled() {
            return;
        }
        if outcome != "not_served" {
            SESSION_READS.incr();
        }
        if outcome.starts_with("fallback") {
            SESSION_FALLBACKS.incr();
        }
        telemetry::crumb("api.session_read")
            .field("kind", request.kind())
            .field("outcome", outcome)
            .field("offset", page_offset(request))
            .since("duration_ms", started)
            .emit();
    }

    /// Why a session read fell back: librespot's error kind only, since the
    /// message names the playlist.
    pub(super) fn session_retry(error: &anyhow::Error) {
        if !telemetry::enabled() {
            return;
        }
        let class = error
            .downcast_ref::<librespot_core::Error>()
            .map_or_else(|| "other".to_owned(), |error| format!("{:?}", error.kind));
        telemetry::crumb("api.session_read_retry")
            .field("class", class)
            .emit();
    }

    // ---- Spotify Connect -------------------------------------------------------

    /// This computer, another device, or none named: device ids stay here.
    fn role_of(device: Option<&str>, local: Option<&str>) -> &'static str {
        match (device, local) {
            (None, _) => "none",
            (Some(device), Some(local)) if device == local => "local",
            (Some(_), _) => "other",
        }
    }

    fn device_role(device: Option<&str>, engine: Option<&Engine>) -> &'static str {
        role_of(device, engine.map(Engine::device_id))
    }

    /// Where playback controls go, as context on every later event.
    fn note_target(target: &'static str) {
        static LAST: AtomicU8 = AtomicU8::new(0);
        let code = if target == "local" { 1 } else { 2 };
        if LAST.swap(code, Relaxed) != code {
            telemetry::set_context("playback_target", target);
        }
    }

    /// The device list, by role and kind: whether this computer is listed,
    /// which kind of device is active, and duplicated ids (two installs
    /// sharing a device name share an id).
    pub(super) fn devices(result: &ApiResult<Vec<Device>>, engine: Option<&Engine>) {
        if !telemetry::enabled() {
            return;
        }
        let Ok(devices) = result else {
            return;
        };
        let local = engine.map(Engine::device_id);
        let active = devices.iter().find(|device| device.is_active);
        let mut ids: Vec<&str> = devices
            .iter()
            .filter_map(|device| device.id.as_deref())
            .collect();
        let listed = ids.len();
        ids.sort_unstable();
        ids.dedup();
        telemetry::crumb("connect.devices")
            .field("count", devices.len())
            .field(
                "local_listed",
                local.is_some_and(|local| ids.contains(&local)),
            )
            .field(
                "active",
                active.map(|device| role_of(device.id.as_deref(), local)),
            )
            .field(
                "active_kind",
                active.map(|device| device.kind.to_lowercase()),
            )
            .field(
                "restricted",
                devices.iter().filter(|device| device.is_restricted).count(),
            )
            .field("duplicate_ids", listed - ids.len())
            .field(
                "kinds",
                devices
                    .iter()
                    .map(|device| device.kind.to_lowercase())
                    .collect::<Vec<_>>(),
            )
            .emit();
    }

    /// What the Web API says is playing where. A change of active device
    /// ships: another device taking over stops this one.
    pub(super) fn snapshot(result: &ApiResult<Option<PlaybackState>>, engine: Option<&Engine>) {
        if !telemetry::enabled() {
            return;
        }
        let Ok(state) = result else {
            return;
        };
        let local = engine.map(Engine::device_id);
        let device = state.as_ref().and_then(|state| state.device.as_ref());
        let device_id = device.and_then(|device| device.id.as_deref());
        let role = if device.is_some() {
            role_of(device_id, local)
        } else {
            "none"
        };
        let kind = device.map(|device| device.kind.to_lowercase());
        let playing = state.as_ref().map(|state| state.is_playing);
        let changed_ago = state
            .as_ref()
            .filter(|state| state.timestamp > 0)
            .map(|state| epoch_ms().saturating_sub(state.timestamp));
        telemetry::crumb("connect.snapshot")
            .field("device", role)
            .field("device_kind", kind.clone())
            .field("is_playing", playing)
            .field(
                "progress_ms",
                state.as_ref().and_then(|state| state.progress_ms),
            )
            .field("changed_ago_ms", changed_ago)
            .field("shuffle", state.as_ref().map(|state| state.shuffle_state))
            .emit();
        let digest = device_id.map(telemetry::digest);
        let previous = std::mem::replace(&mut *lock(&ACTIVE_DEVICE), digest.clone());
        if previous != digest {
            telemetry::event("connect.active_device_changed")
                .field("device", role)
                .field("device_kind", kind)
                .field("device_digest", digest)
                .field("previous_digest", previous)
                .field("is_playing", playing)
                .emit();
        }
    }

    fn epoch_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| {
                u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
            })
    }

    /// One user action that became several requests.
    pub(super) fn fanout(kind: Kind, planned: usize, issued: usize, error: Option<&ApiError>) {
        telemetry::crumb("api.fanout")
            .field("kind", kind)
            .field("planned", planned)
            .field("issued", issued)
            .field("outcome", error.map_or("ok", api_error_class))
            .emit();
    }

    /// A playback control sent through the Web API, from the request to
    /// Spotify's answer. When it targets another device no audio follows
    /// here, so the answer ends the interface's trace.
    pub(super) struct RemoteControl {
        name: &'static str,
        kind: Kind,
        trace: Option<&'static str>,
        device: &'static str,
        seq: u64,
        started: Instant,
        intent: Option<Intent>,
    }

    impl RemoteControl {
        /// Just before the request goes out. Spotify relays it to the
        /// target, this computer included, so it is noted as a cause.
        pub(super) fn send(
            name: &'static str,
            kind: Kind,
            trace: Option<&'static str>,
            intents: &[&str],
            device: Option<&str>,
            engine: Option<&Engine>,
        ) -> Option<Self> {
            if !telemetry::enabled() {
                return None;
            }
            let device = device_role(device, engine);
            telemetry::note_cause("webapi:remote_command", format!("{kind} device={device}"));
            if let Some(trace) = trace {
                telemetry::trace_mark(trace, "webapi_sent");
            }
            Some(Self {
                name,
                kind,
                trace,
                device,
                seq: REMOTE_SEQ.fetch_add(1, Relaxed) + 1,
                started: Instant::now(),
                intent: telemetry::recent_intent(intents, INTENT_WINDOW),
            })
        }

        pub(super) fn done(self, result: &ApiResult<()>) {
            REMOTE_COMMANDS.incr();
            let error = result.as_ref().err();
            if let Some(trace) = self.trace {
                telemetry::trace_mark(trace, "webapi_done");
                if error.is_some() {
                    telemetry::trace_end(trace, "failed");
                } else if self.device != "local" {
                    telemetry::trace_end(trace, "ok");
                }
            }
            with_intent(telemetry::event(self.name), self.intent.as_ref())
                .field("action", self.kind)
                .field("device", self.device)
                .field("outcome", error.map_or("ok", api_error_class))
                .field("status", error.and_then(ApiError::status))
                .since("duration_ms", self.started)
                .field("newer_sent", REMOTE_SEQ.load(Relaxed) != self.seq)
                .emit();
        }
    }

    pub(super) fn remote_kind(action: RemoteAction) -> Kind {
        match action {
            RemoteAction::Play => "Remote.Play",
            RemoteAction::Pause => "Remote.Pause",
            RemoteAction::Next => "Remote.Next",
            RemoteAction::Previous => "Remote.Previous",
            RemoteAction::Seek => "Remote.Seek",
            RemoteAction::Volume => "Remote.Volume",
            RemoteAction::Shuffle => "Remote.Shuffle",
            RemoteAction::Repeat => "Remote.Repeat",
        }
    }

    /// The trace the interface opens for a control, if any.
    pub(super) fn remote_trace(action: RemoteAction, starts_context: bool) -> Option<&'static str> {
        match action {
            RemoteAction::Next => Some("next"),
            RemoteAction::Previous => Some("previous"),
            RemoteAction::Play if starts_context => Some("play"),
            RemoteAction::Play => Some("resume"),
            RemoteAction::Seek => Some("seek"),
            RemoteAction::Pause
            | RemoteAction::Volume
            | RemoteAction::Shuffle
            | RemoteAction::Repeat => None,
        }
    }

    pub(super) fn remote_intents(action: RemoteAction) -> &'static [&'static str] {
        match action {
            RemoteAction::Play => PLAY,
            RemoteAction::Pause => PAUSE,
            RemoteAction::Next => NEXT,
            RemoteAction::Previous => PREVIOUS,
            RemoteAction::Seek => SEEK,
            RemoteAction::Volume => VOLUME,
            RemoteAction::Shuffle => SHUFFLE,
            RemoteAction::Repeat => REPEAT,
        }
    }

    fn control_intents(request: &ApiRequest) -> Option<&'static [&'static str]> {
        match request {
            ApiRequest::Remote { action, .. } => Some(remote_intents(*action)),
            ApiRequest::ShufflePlay { .. } => Some(PLAY_ITEM),
            ApiRequest::Transfer { .. } => Some(TRANSFER),
            _ => None,
        }
    }

    fn control_trace(request: &ApiRequest) -> Option<&'static str> {
        match request {
            ApiRequest::Remote { action, play, .. } => remote_trace(*action, play.is_some()),
            ApiRequest::ShufflePlay { .. } => Some("play"),
            ApiRequest::Transfer { .. } => Some("transfer"),
            _ => None,
        }
    }

    fn control_device(request: &ApiRequest) -> Option<&str> {
        match request {
            ApiRequest::Remote { device_id, .. } | ApiRequest::ShufflePlay { device_id, .. } => {
                device_id.as_deref()
            }
            ApiRequest::Transfer { device_id, .. } => Some(device_id.as_str()),
            _ => None,
        }
    }

    pub(super) fn player_kind(command: &PlayerCommand) -> Kind {
        match command {
            PlayerCommand::Toggle => "Toggle",
            PlayerCommand::Next => "Next",
            PlayerCommand::Previous => "Previous",
            PlayerCommand::ClearQueue => "ClearQueue",
            PlayerCommand::AddToQueue(_) => "AddToQueue",
            PlayerCommand::Seek(_) => "Seek",
            PlayerCommand::Volume(_) => "Volume",
            PlayerCommand::VolumePreview(_) => "VolumePreview",
            PlayerCommand::Shuffle(_) => "Shuffle",
            PlayerCommand::Repeat(_) => "Repeat",
            PlayerCommand::Load(_) => "Load",
            PlayerCommand::Transfer => "Transfer",
        }
    }

    /// The trace the interface opens for a local command, by its kind.
    fn player_trace(kind: &str) -> Option<&'static str> {
        Some(match kind {
            "Next" => "next",
            "Previous" => "previous",
            "Load" => "play",
            "Toggle" => "resume",
            "Seek" => "seek",
            "Transfer" => "transfer",
            _ => return None,
        })
    }

    fn player_intents(command: &PlayerCommand) -> Option<&'static [&'static str]> {
        Some(match command {
            PlayerCommand::Toggle => TOGGLE,
            PlayerCommand::Next => NEXT,
            PlayerCommand::Previous => PREVIOUS,
            PlayerCommand::ClearQueue => return None,
            PlayerCommand::AddToQueue(_) => QUEUE_ADD,
            PlayerCommand::Seek(_) => SEEK,
            PlayerCommand::Volume(_) | PlayerCommand::VolumePreview(_) => VOLUME,
            PlayerCommand::Shuffle(_) => SHUFFLE,
            PlayerCommand::Repeat(_) => REPEAT,
            PlayerCommand::Load(_) => PLAY_ITEM,
            PlayerCommand::Transfer => TRANSFER,
        })
    }
}

#[cfg(test)]
mod album_type_lookup_tests {
    use super::AlbumTypeLookup;

    #[test]
    fn signed_out_requests_are_dropped_and_valid_pending_work_is_deduplicated() {
        let mut lookup = AlbumTypeLookup::default();
        lookup.enqueue(false, None, vec!["signed-out".into()]);
        assert!(lookup.pending.is_empty());

        lookup.enqueue(
            true,
            None,
            vec!["first".into(), "first".into(), "second".into()],
        );
        assert_eq!(lookup.pending.len(), 2);

        let first = lookup.next().expect("first request when an engine appears");
        assert_eq!(first.uri, "first");
        assert!(lookup.next().is_none(), "only one lookup may be active");
        assert!(lookup.finish(&first));
        assert_eq!(lookup.next().expect("remaining request").uri, "second");
    }

    #[test]
    fn results_from_a_signed_out_session_are_rejected_after_a_new_session_starts() {
        let mut lookup = AlbumTypeLookup::default();
        lookup.enqueue(true, None, vec!["old".into()]);
        let old = lookup.next().expect("old session request");

        lookup.reset_session();
        assert!(!lookup.finish(&old));
        lookup.enqueue(false, None, vec!["after-logout".into()]);
        assert!(lookup.pending.is_empty());

        lookup.enqueue(true, None, vec!["new".into()]);
        let new = lookup.next().expect("new session request");
        assert_ne!(old.session_generation, new.session_generation);
        assert!(!lookup.finish(&old));
        assert!(lookup.finish(&new));
    }

    #[test]
    fn reconnect_requeues_active_work_for_the_new_engine() {
        let mut lookup = AlbumTypeLookup::default();
        lookup.enqueue(true, None, vec!["active".into(), "waiting".into()]);
        let retired = lookup.next().expect("retired engine request");

        lookup.requeue_active_for_new_engine();
        assert_eq!(lookup.pending.front().map(String::as_str), Some("active"));

        lookup.enqueue(true, None, vec!["active".into()]);
        assert_eq!(lookup.pending.len(), 2, "external duplicates stay ignored");

        let replacement = lookup.next().expect("new engine request");
        assert_eq!(replacement.uri, "active");
        assert_eq!(retired.session_generation, replacement.session_generation);
        assert_ne!(retired.engine_generation, replacement.engine_generation);
        assert!(!lookup.finish(&retired));
        assert_eq!(lookup.active.as_ref(), Some(&replacement));
        assert!(lookup.finish(&replacement));
        assert_eq!(lookup.next().expect("remaining request").uri, "waiting");
    }

    #[test]
    fn completed_failures_remain_terminal_for_the_session() {
        let mut lookup = AlbumTypeLookup::default();

        for uri in ["error", "timeout"] {
            lookup.enqueue(true, None, vec![uri.into()]);
            let request = lookup.next().expect("request before failure");
            assert!(lookup.finish(&request));

            lookup.enqueue(true, None, vec![uri.into()]);
            assert!(lookup.pending.is_empty(), "failed request must not retry");
        }
    }

    #[test]
    fn non_premium_status_clears_and_invalidates_all_engine_work() {
        let mut lookup = AlbumTypeLookup::default();
        lookup.enqueue(true, None, vec!["active".into(), "pending".into()]);
        let active = lookup.next().expect("request before account check");
        let session_generation = lookup.session_generation;

        lookup.clear_engine_work();

        assert_eq!(lookup.session_generation, session_generation);
        assert_ne!(lookup.engine_generation, active.engine_generation);
        assert!(lookup.active.is_none());
        assert!(lookup.pending.is_empty());
        assert!(lookup.seen.is_empty());
        assert!(!lookup.finish(&active));

        lookup.enqueue(true, Some(false), vec!["after-check".into()]);
        assert!(lookup.pending.is_empty());
        assert!(lookup.seen.is_empty());
    }

    #[test]
    fn unknown_and_premium_accounts_accept_album_type_work() {
        let mut lookup = AlbumTypeLookup::default();

        lookup.enqueue(true, None, vec!["unknown".into()]);
        let unknown = lookup.next().expect("unknown account request");
        assert_eq!(unknown.uri, "unknown");
        assert!(lookup.finish(&unknown));

        lookup.enqueue(true, Some(true), vec!["premium".into()]);
        let premium = lookup.next().expect("premium account request");
        assert_eq!(premium.uri, "premium");
        assert!(lookup.finish(&premium));
    }
}

#[cfg(test)]
mod playlist_cache_tests {
    use super::{
        CachedPlaylist, PlaylistCacheRows, playlist_data_path, playlist_manifest_path,
        read_cached_playlist, read_incremental_playlist_cache_file, read_playlist_cache,
        read_playlist_manifest, write_cached_playlist, write_incremental_playlist_cache_file,
    };
    use crate::api::models::{PlayableItem, PlaylistItem, Track};

    #[test]
    fn the_original_complete_cache_format_remains_readable() {
        let cached: CachedPlaylist =
            serde_json::from_str(r#"{"snapshot":"old","items":[]}"#).unwrap();

        assert_eq!(cached.snapshot, "old");
        assert_eq!(cached.total, None);
        assert_eq!(cached.next_offset, None);
    }

    #[test]
    fn incremental_checkpoints_append_only_new_rows_and_recover_from_failed_publication() {
        let root = std::env::temp_dir().join(format!(
            "spotifast-playlist-cache-incremental-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let path = root.join("playlist.json");
        let row = |uri: &str| PlaylistItem {
            item: Some(PlayableItem::Track(Track {
                uri: uri.into(),
                ..Track::default()
            })),
            ..PlaylistItem::default()
        };
        let first = vec![row("spotify:track:one"), PlaylistItem::default()];
        let added = row("spotify:track:three");
        write_incremental_playlist_cache_file(
            &path,
            "same".into(),
            PlaylistCacheRows::Replace(first.clone()),
            100,
            Some(50),
        )
        .unwrap();
        let initial = read_playlist_manifest(&path).unwrap();
        assert_eq!(initial.rows, 2);
        assert_eq!(
            initial.bytes as usize,
            serde_json::to_vec(&first).unwrap().len() + 1
        );

        let append = || PlaylistCacheRows::Append {
            previous_rows: 2,
            previous_offset: 50,
            items: vec![added.clone()],
        };
        let temporary = playlist_manifest_path(&path).with_extension("json.tmp");
        std::fs::create_dir(&temporary).unwrap();
        assert!(
            write_incremental_playlist_cache_file(&path, "same".into(), append(), 100, Some(75))
                .is_err()
        );
        assert_eq!(
            read_incremental_playlist_cache_file(&path).unwrap().items,
            first
        );
        assert_eq!(read_playlist_manifest(&path).unwrap().bytes, initial.bytes);
        std::fs::remove_dir(&temporary).unwrap();

        write_incremental_playlist_cache_file(&path, "same".into(), append(), 100, Some(75))
            .unwrap();
        let committed = read_playlist_manifest(&path).unwrap();
        assert_eq!(committed.rows, 3);
        assert_eq!(
            committed.bytes - initial.bytes,
            (serde_json::to_vec(std::slice::from_ref(&added))
                .unwrap()
                .len()
                + 1) as u64,
            "an append writes only its new block, including after a failed publication"
        );
        assert_eq!(
            std::fs::metadata(playlist_data_path(&path, committed.data_file))
                .unwrap()
                .len(),
            committed.bytes
        );
        let restored = read_incremental_playlist_cache_file(&path).unwrap();
        assert_eq!(restored.items, [first, vec![added]].concat());
        assert_eq!(restored.next_offset, Some(75));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn incremental_cache_replaces_changed_snapshot_and_keeps_legacy_reader() {
        let root = std::env::temp_dir().join(format!(
            "spotifast-playlist-cache-migration-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let path = root.join("playlist.json");
        write_cached_playlist(
            path.clone(),
            CachedPlaylist {
                snapshot: "legacy".into(),
                items: vec![PlaylistItem::default()],
                total: Some(10),
                next_offset: Some(5),
            },
        )
        .await
        .unwrap();
        let (cached, appendable) = read_playlist_cache(path.clone()).await.unwrap();
        assert_eq!(cached.snapshot, "legacy");
        assert!(!appendable);

        write_incremental_playlist_cache_file(
            &path,
            "new".into(),
            PlaylistCacheRows::Replace(vec![PlaylistItem::default(); 2]),
            10,
            Some(5),
        )
        .unwrap();
        let old_data = read_playlist_manifest(&path).unwrap().data_file;
        let (cached, appendable) = read_playlist_cache(path.clone()).await.unwrap();
        assert_eq!(cached.snapshot, "new");
        assert!(appendable);
        assert_eq!(
            read_playlist_cache(path.clone())
                .await
                .unwrap()
                .0
                .items
                .len(),
            2
        );

        std::fs::OpenOptions::new()
            .write(true)
            .open(playlist_data_path(&path, old_data))
            .unwrap()
            .set_len(0)
            .unwrap();
        let (cached, appendable) = read_playlist_cache(path.clone()).await.unwrap();
        assert_eq!(cached.snapshot, "legacy");
        assert!(!appendable);

        write_incremental_playlist_cache_file(
            &path,
            "newer".into(),
            PlaylistCacheRows::Replace(vec![PlaylistItem::default()]),
            1,
            None,
        )
        .unwrap();
        assert_eq!(
            read_playlist_cache(path.clone()).await.unwrap().0.snapshot,
            "newer"
        );
        assert!(!playlist_data_path(&path, old_data).exists());
        assert_eq!(read_cached_playlist(path).await.unwrap().snapshot, "legacy");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn reopening_a_cache_removes_rows_left_by_an_interrupted_replacement() {
        let root = std::env::temp_dir().join(format!(
            "spotifast-playlist-cache-orphan-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let path = root.join("playlist.json");
        write_incremental_playlist_cache_file(
            &path,
            "current".into(),
            PlaylistCacheRows::Replace(vec![PlaylistItem::default()]),
            1,
            None,
        )
        .unwrap();
        let current = read_playlist_manifest(&path).unwrap().data_file;
        let orphan = playlist_data_path(&path, current.wrapping_add(1));
        let blocked = playlist_data_path(&path, current.wrapping_add(2));
        let recoverable = playlist_data_path(&path, current.wrapping_add(3));
        let other_playlist = playlist_data_path(&root.join("other.json"), 1);
        std::fs::write(&orphan, b"unfinished rows").unwrap();
        std::fs::create_dir(&blocked).unwrap();
        std::fs::write(&recoverable, b"unfinished rows").unwrap();
        std::fs::write(&other_playlist, b"unrelated rows").unwrap();

        let (cached, appendable) = read_playlist_cache(path.clone()).await.unwrap();
        assert_eq!(cached.snapshot, "current");
        assert!(appendable);
        assert!(
            !orphan.exists(),
            "recovery must remove unreferenced row files"
        );
        assert!(playlist_data_path(&path, current).exists());
        assert!(
            !recoverable.exists(),
            "one failed removal must not stop cleanup"
        );
        assert!(blocked.is_dir());
        assert!(other_playlist.exists());
        std::fs::remove_dir(&blocked).unwrap();

        let before_manifest = root.join("cold.json");
        let orphan = playlist_data_path(&before_manifest, 1);
        std::fs::write(&orphan, b"unfinished first checkpoint").unwrap();
        assert!(read_playlist_cache(before_manifest).await.is_err());
        assert!(
            !orphan.exists(),
            "a crash before the first manifest is recoverable"
        );

        let orphan = playlist_data_path(&path, current.wrapping_add(1));
        std::fs::write(&orphan, b"unfinished rows").unwrap();
        write_incremental_playlist_cache_file(
            &path,
            "next".into(),
            PlaylistCacheRows::Replace(vec![PlaylistItem::default()]),
            1,
            None,
        )
        .unwrap();
        assert!(!orphan.exists());
        assert!(!playlist_data_path(&path, current).exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn the_file_reader_accepts_legacy_caches_and_ignores_unknown_fields() {
        let root = std::env::temp_dir().join(format!(
            "spotifast-playlist-cache-legacy-read-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let path = root.join("playlist.json");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            &path,
            b"{\"snapshot\":\"old\",\"items\":[],\"future_field\":true} \n\t",
        )
        .unwrap();

        let cached = read_cached_playlist(path).await.unwrap();

        assert_eq!(cached.snapshot, "old");
        assert!(cached.items.is_empty());
        assert_eq!(cached.total, None);
        assert_eq!(cached.next_offset, None);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn the_file_reader_rejects_missing_corrupt_and_trailing_data() {
        let root = std::env::temp_dir().join(format!(
            "spotifast-playlist-cache-invalid-read-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let path = root.join("playlist.json");
        std::fs::create_dir_all(&root).unwrap();
        assert!(read_cached_playlist(path.clone()).await.is_err());
        for bytes in [
            b"".as_slice(),
            b"{\"snapshot\":\"partial\",\"items\":[",
            b"{\"snapshot\":\"bad utf8: \xff\",\"items\":[]}",
            b"{\"snapshot\":\"missing items\"}",
            b"{\"snapshot\":\"ok\",\"items\":[]}{}",
            b"{\"snapshot\":\"ok\",\"items\":[]}trailing",
        ] {
            std::fs::write(&path, bytes).unwrap();

            assert!(read_cached_playlist(path.clone()).await.is_err());
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn a_new_checkpoint_atomically_replaces_the_previous_one() {
        let root = std::env::temp_dir().join(format!(
            "spotifast-playlist-cache-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let path = root.join("playlist.json");
        let cached = |snapshot: &str| CachedPlaylist {
            snapshot: snapshot.into(),
            items: Vec::new(),
            total: Some(10_000),
            next_offset: Some(500),
        };

        write_cached_playlist(path.clone(), cached("first"))
            .await
            .unwrap();
        write_cached_playlist(path.clone(), cached("second"))
            .await
            .unwrap();

        let text = tokio::fs::read_to_string(&path).await.unwrap();
        let stored: CachedPlaylist = serde_json::from_str(&text).unwrap();
        assert_eq!(stored.snapshot, "second");
        assert!(!path.with_extension("json.tmp").exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn streaming_preserves_the_cache_bytes_and_duplicate_unavailable_rows() {
        let root = std::env::temp_dir().join(format!(
            "spotifast-playlist-cache-stream-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let path = root.join("playlist.json");
        let song = PlayableItem::Track(Track {
            uri: "spotify:track:duplicate".into(),
            name: "Song with \"quotes\", newlines\nand 日本語".into(),
            is_playable: Some(false),
            ..Track::default()
        });
        let rows = [
            PlaylistItem {
                item: Some(song.clone()),
                ..PlaylistItem::default()
            },
            PlaylistItem {
                track: Some(song),
                ..PlaylistItem::default()
            },
            PlaylistItem::default(),
        ];
        let cached = CachedPlaylist {
            snapshot: "unchanged-snapshot".into(),
            items: (0..500).flat_map(|_| rows.clone()).collect(),
            total: Some(2_000),
            next_offset: Some(1_500),
        };
        let expected = serde_json::to_vec(&cached).unwrap();
        assert!(
            expected.len() > 64 * 1024,
            "exercise multiple buffer flushes"
        );

        write_cached_playlist(path.clone(), cached).await.unwrap();

        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes, expected, "existing readers see the identical format");
        let restored = read_cached_playlist(path.clone()).await.unwrap();
        assert_eq!(restored.snapshot, "unchanged-snapshot");
        assert_eq!(restored.items.len(), 1_500);
        for chunk in restored.items.chunks(3) {
            assert_eq!(chunk, rows);
        }
        assert_eq!(restored.total, Some(2_000));
        assert_eq!(restored.next_offset, Some(1_500));
        assert!(!path.with_extension("json.tmp").exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn failed_checkpoint_keeps_existing_data_and_cleans_only_its_temporary_file() {
        let root = std::env::temp_dir().join(format!(
            "spotifast-playlist-cache-failure-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let path = root.join("playlist.json");
        let temporary = path.with_extension("json.tmp");
        let cached = || CachedPlaylist {
            snapshot: "new".into(),
            items: Vec::new(),
            total: Some(0),
            next_offset: None,
        };
        std::fs::create_dir_all(&temporary).unwrap();
        let previous = br#"{"snapshot":"old","items":[]}"#;
        std::fs::write(&path, previous).unwrap();

        assert!(write_cached_playlist(path.clone(), cached()).await.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), previous);
        assert!(temporary.is_dir(), "a failed create does not own this path");

        // Force final replacement to fail after serialization and flushing.
        std::fs::remove_dir(&temporary).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("preserved"), previous).unwrap();
        assert!(write_cached_playlist(path.clone(), cached()).await.is_err());
        assert_eq!(std::fs::read(path.join("preserved")).unwrap(), previous);
        assert!(!temporary.exists(), "discard the failed checkpoint");
        std::fs::remove_dir_all(root).unwrap();
    }
}

/// Bind a streaming token to an account verified by either Web API grant.
/// A late browser result after sign-out must not start an anonymous session.
fn playback_credentials(account: Option<AccountId>, access_token: String) -> Option<Credentials> {
    let account = account.filter(|account| !account.as_str().is_empty())?;
    Some(Credentials {
        username: Some(account.as_str().to_string()),
        auth_type:
            librespot_protocol::authentication::AuthenticationType::AUTHENTICATION_SPOTIFY_TOKEN,
        auth_data: access_token.into_bytes(),
    })
}

#[cfg(test)]
mod authorization_tests {
    use super::*;

    #[test]
    fn playlist_cache_store_does_not_hold_up_the_command_loop() {
        let (runtime, mut worker, events) = worker("playlist-cache-command-loop");
        worker
            .api
            .install(ApiSource::Shared, AccountId::new("alice"))
            .unwrap();
        let path = worker
            .dirs
            .account_playlist_cache_dir("alice")
            .join("mix.json");
        let root = worker.dirs.cache.parent().unwrap().to_path_buf();
        let lock = playlist_cache_lock(&path).unwrap();
        let (commands, receiver) = mpsc::unbounded_channel();
        commands
            .send(Command::StorePlaylistCache {
                id: "mix".into(),
                generation: 1,
                snapshot: "old".into(),
                rows: PlaylistCacheRows::Replace(vec![PlaylistItem::default()]),
                total: 1,
                next_offset: None,
            })
            .unwrap();
        commands
            .send(Command::ConfigurePersonalWebApp(None))
            .unwrap();
        commands.send(Command::Shutdown).unwrap();
        let thread = std::thread::spawn(move || runtime.block_on(worker.run(receiver)));

        let deadline = Instant::now() + Duration::from_secs(2);
        let handled_next_command = loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break false;
            }
            match events.recv_timeout(remaining) {
                Ok(Event::WebApp { client_id: None }) => break true,
                Ok(_) => continue,
                Err(_) => break false,
            }
        };
        drop(lock);
        thread.join().unwrap();
        assert!(
            handled_next_command,
            "a pending disk write blocked the command loop"
        );
        assert_eq!(read_playlist_manifest(&path).unwrap().snapshot, "old");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn playlist_cache_writer_rejects_excess_snapshots_without_blocking_commands() {
        let (runtime, mut worker, events) = worker("playlist-cache-bounded-writes");
        worker
            .api
            .install(ApiSource::Shared, AccountId::new("alice"))
            .unwrap();
        let path = worker
            .dirs
            .account_playlist_cache_dir("alice")
            .join("mix.json");
        let root = worker.dirs.cache.parent().unwrap().to_path_buf();
        let lock = playlist_cache_lock(&path).unwrap();
        let (commands, receiver) = mpsc::unbounded_channel();
        for generation in 0..4 {
            commands
                .send(Command::StorePlaylistCache {
                    id: "mix".into(),
                    generation,
                    snapshot: format!("snapshot-{generation}"),
                    rows: PlaylistCacheRows::Replace(vec![PlaylistItem::default()]),
                    total: 1,
                    next_offset: None,
                })
                .unwrap();
        }
        commands
            .send(Command::ConfigurePersonalWebApp(None))
            .unwrap();
        commands.send(Command::Shutdown).unwrap();
        let thread = std::thread::spawn(move || runtime.block_on(worker.run(receiver)));

        let deadline = Instant::now() + Duration::from_secs(2);
        let mut rejected = 0;
        let handled_next_command = loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break false;
            }
            match events.recv_timeout(remaining) {
                Ok(Event::PlaylistCacheStored { success: false, .. }) => rejected += 1,
                Ok(Event::WebApp { client_id: None }) => break true,
                Ok(_) => {}
                Err(_) => break false,
            }
        };
        drop(lock);
        thread.join().unwrap();
        assert!(
            handled_next_command,
            "cache writes blocked the command loop"
        );
        assert!(rejected >= 2, "only one write may wait behind the writer");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn engine_deadline_reaches_final_access_point_after_stalled_retries() {
        let started = tokio::time::Instant::now();
        let mut attempts = Vec::new();
        let result = connect_engine_with_deadline(async {
            // Resolver and other setup work also spend the outer budget.
            tokio::time::sleep(Duration::from_secs(10)).await;
            // Model the pinned librespot cap: six APs, two five-second
            // connection attempts per AP. The final retry succeeds.
            for ap in 0..6 {
                for retry in 0..2 {
                    attempts.push((ap, retry));
                    if (ap, retry) == (5, 1) {
                        return ap;
                    }
                    assert!(
                        tokio::time::timeout(Duration::from_secs(5), std::future::pending::<()>())
                            .await
                            .is_err()
                    );
                }
            }
            unreachable!("the final fallback should connect");
        })
        .await;

        assert_eq!(result.unwrap(), 5);
        assert_eq!(
            attempts,
            (0..6).flat_map(|ap| [(ap, 0), (ap, 1)]).collect::<Vec<_>>()
        );
        assert_eq!(started.elapsed(), Duration::from_secs(65));
    }

    #[test]
    fn proxy_changes_compare_with_the_running_engine() {
        const CHILD: &str = "SPOTIFAST_PROXY_SNAPSHOT_TEST";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "backend::authorization_tests::proxy_changes_compare_with_the_running_engine",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .env("NO_PROXY", "*")
                .env("no_proxy", "*")
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }

        // The system proxy has been removed since the engine connected.
        // Resolve it in a child process so the test never alters desktop settings
        // or the environment used by concurrently running tests.
        assert_eq!(ProxyConfig::System.librespot_url(), None);
        let old_url = reqwest::Url::parse("http://127.0.0.1:7890/").unwrap();
        let (runtime, mut worker, _) = worker("proxy-snapshot");
        let _entered = runtime.enter();
        worker.engine_config.proxy = ProxyConfig::System;
        worker.engine_proxy = Some(old_url.clone());
        worker.signed_in = true;
        worker.engine_busy = true;
        worker.change_proxy(1, ProxyConfig::Off, false);
        assert!(
            worker.engine_restart_pending,
            "Off must discard an in-flight connection using the former system proxy"
        );
        assert_eq!(worker.engine_proxy, Some(old_url));
        assert_eq!(worker.engine_config.proxy, ProxyConfig::Off);

        let http = crate::settings::Settings {
            proxy_mode: crate::settings::ProxyMode::Http,
            proxy_host: "127.0.0.1".into(),
            proxy_port: "7890".into(),
            ..Default::default()
        };
        let socks = crate::settings::Settings {
            proxy_mode: crate::settings::ProxyMode::Socks,
            ..http.clone()
        };
        let other = crate::settings::Settings {
            proxy_port: "7891".into(),
            ..http.clone()
        };
        // Every choice is compared with the old connection, even after a
        // previous Apply has updated the saved policy while reconnecting.
        for (next, restart) in [
            (ProxyConfig::System, true),
            (socks.proxy_config().unwrap(), true),
            (other.proxy_config().unwrap(), true),
            (http.proxy_config().unwrap(), false),
        ] {
            assert_eq!(worker.apply_proxy(next).unwrap(), restart);
        }
        worker.engine_proxy = None;
        for (next, restart) in [
            (ProxyConfig::Off, false),
            (ProxyConfig::System, false),
            (socks.proxy_config().unwrap(), false),
            (http.proxy_config().unwrap(), true),
        ] {
            assert_eq!(worker.apply_proxy(next).unwrap(), restart);
        }
    }

    #[test]
    fn proxy_restoration_defers_work_without_blocking_shutdown() {
        let (runtime, mut worker, _) = worker("proxy-restore-barrier");
        worker.restoring_proxy = true;
        let mut audio = worker.engine_config.clone();
        audio.proxy = ProxyConfig::System;
        let (commands, receiver) = mpsc::unbounded_channel();
        commands.send(Command::RestartEngine(audio)).unwrap();
        commands.send(Command::Shutdown).unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(1), worker.run(receiver))
                .await
                .unwrap();
        });
        assert_eq!(worker.waiting_for_proxy.len(), 1);
        assert_eq!(worker.engine_config.proxy, ProxyConfig::Off);
    }

    /// A session that drops on its own comes back at the level being heard,
    /// exactly, not at the level the app launched with. Music turned down
    /// to 5% after launch used to come back at the previous session's 80%.
    #[test]
    fn a_dropped_session_comes_back_at_the_level_being_heard() {
        let (runtime, mut worker, _) = worker("reconnect-volume");
        worker.signed_in = true;
        worker.engine_config.initial_volume = crate::app::percent_to_volume(80);
        worker.heard = Some(Heard::at(crate::app::percent_to_volume(5)));
        let (commands, receiver) = mpsc::unbounded_channel();
        commands.send(Command::Reconnect).unwrap();
        commands.send(Command::Shutdown).unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(1), worker.run(receiver))
                .await
                .unwrap();
        });
        assert_eq!(
            worker.engine_config.initial_volume,
            crate::app::percent_to_volume(5),
            "the next engine must start where the last one was heard"
        );
        assert!(
            worker.heard.is_none(),
            "what was heard goes with its engine"
        );
    }

    /// Signing out takes the engine down outside the reconnect path. The
    /// engine that playback is enabled with afterwards starts where the last
    /// one was heard as well.
    #[test]
    fn signing_out_keeps_the_level_being_heard_for_the_next_engine() {
        let (runtime, mut worker, _) = worker("sign-out-volume");
        let _entered = runtime.enter();
        worker.signed_in = true;
        worker.engine_config.initial_volume = crate::app::percent_to_volume(80);
        worker.heard = Some(Heard::at(crate::app::percent_to_volume(5)));
        worker.sign_out();
        assert_eq!(
            worker.engine_config.initial_volume,
            crate::app::percent_to_volume(5)
        );
        assert!(worker.heard.is_none());
    }

    #[test]
    fn an_invalid_saved_proxy_keeps_restored_network_work_blocked() {
        let (runtime, mut worker, events) = worker("proxy-restore-invalid");
        let _entered = runtime.enter();
        let invalid = ProxyConfig::Invalid("Proxy port must be a number".into());
        worker.engine_config.proxy = invalid.clone();
        worker.restoring_proxy = true;
        worker.on_proxy_restored(
            worker.credentials.lease(CredentialSlot::Proxy),
            Ok(crate::credentials::Loaded {
                grant: None,
                warning: None,
            }),
        );
        assert!(worker.http.client().is_err());
        assert!(!worker.restoring_proxy);
        assert!(events.try_iter().any(
            |event| matches!(event, Event::ProxyRestored { config, .. } if config == invalid)
        ));
    }

    #[test]
    fn explicit_proxy_apply_can_complete_before_native_restoration() {
        let (runtime, mut worker, events) = worker("proxy-restore-apply");
        let _entered = runtime.enter();
        worker.restoring_proxy = true;
        let old = worker.credentials.lease(CredentialSlot::Proxy);
        worker.change_proxy(1, ProxyConfig::System, false);
        assert!(!worker.restoring_proxy);
        assert!(worker.http.client().is_ok());
        worker.on_proxy_restored(
            old,
            Ok(crate::credentials::Loaded {
                grant: None,
                warning: None,
            }),
        );
        assert_eq!(worker.engine_config.proxy, ProxyConfig::System);
        assert!(
            events
                .try_iter()
                .all(|event| !matches!(event, Event::ProxyRestored { .. }))
        );
    }

    #[test]
    fn audio_settings_cannot_revert_a_proxy_waiting_for_its_ui_acknowledgement() {
        let (runtime, mut worker, _) = worker("proxy-audio-restart");
        let old_audio = worker.engine_config.clone();
        let settings = crate::settings::Settings {
            proxy_mode: crate::settings::ProxyMode::Http,
            proxy_host: "127.0.0.1".into(),
            proxy_port: "8080".into(),
            ..Default::default()
        };
        let proxy = settings.proxy_config().unwrap();
        let (commands, receiver) = mpsc::unbounded_channel();
        commands
            .send(Command::ApplyProxy {
                request: 1,
                config: proxy.clone(),
            })
            .unwrap();
        commands.send(Command::RestartEngine(old_audio)).unwrap();
        commands.send(Command::Shutdown).unwrap();
        runtime.block_on(worker.run(receiver));
        assert_eq!(worker.engine_config.proxy, proxy);
    }

    #[test]
    fn a_rejected_proxy_change_does_not_replace_the_live_configuration() {
        let (runtime, mut worker, events) = worker("rejected-proxy");
        let _entered = runtime.enter();
        let original = worker.engine_config.proxy.clone();
        let bad = ProxyConfig::Invalid("Proxy port must be a number".into());
        worker.change_proxy(1, bad.clone(), false);
        assert_eq!(worker.engine_config.proxy, original);
        assert!(!worker.engine_restart_pending);
        assert!(worker.http.client().is_ok());
        let answers: Vec<_> = events.try_iter().collect();
        assert_eq!(answers.len(), 1);
        assert!(
            matches!(&answers[0], Event::ProxyApplied { config, result: Err(_), .. } if config == &bad)
        );
    }

    #[test]
    fn expired_grants_are_forgotten_and_a_new_sign_in_completes_without_restart() {
        let (runtime, mut worker, events) = worker("expired-then-sign-in");
        runtime.block_on(async {
            verify(&mut worker, ApiSource::Shared, "alice");
            let old = worker.credentials.lease(CredentialSlot::Shared);
            let grant = StoredGrant::Web(crate::auth::StoredToken {
                client_id: crate::auth::DEFAULT_WEB_CLIENT_ID.into(),
                access_token: "dummy-expired-access".into(),
                refresh_token: "dummy-rejected-refresh".into(),
                ..Default::default()
            });
            old.save(grant).await.unwrap();
            worker.on_web_verification_failed(
                ApiSource::Shared,
                ApiError::SignInExpired {
                    api_source: ApiSource::Shared,
                },
            );
            assert!(!old.current());
            assert!(!worker.signed_in);
            assert!(
                worker
                    .credentials
                    .lease(CredentialSlot::Shared)
                    .load()
                    .await
                    .unwrap()
                    .grant
                    .is_none()
            );
            let _ = events.try_iter().collect::<Vec<_>>();
            verify(&mut worker, ApiSource::Shared, "alice");
            assert!(worker.signed_in);
            assert!(
                events
                    .try_iter()
                    .any(|event| matches!(event, Event::Auth(AuthStatus::Connected { .. })))
            );
        });
    }

    #[test]
    fn old_api_and_verification_errors_cannot_sign_out_a_new_session() {
        let (runtime, mut worker, events) = worker("old-api-after-sign-in");
        let _entered = runtime.enter();
        verify(&mut worker, ApiSource::Shared, "alice");
        let generation = *worker.session.borrow();
        let shared = worker.credentials.lease(CredentialSlot::Shared);
        let personal = worker.credentials.lease(CredentialSlot::Personal);
        worker.sign_out();
        verify(&mut worker, ApiSource::Shared, "bob");
        let _ = events.try_iter().collect::<Vec<_>>();
        let (commands, receiver) = mpsc::unbounded_channel();
        commands
            .send(Command::ApiFinished {
                generation,
                response: Box::new(ApiResponse::Me(Err(ApiError::SignInExpired {
                    api_source: ApiSource::Shared,
                }))),
                expired: Some(ApiSource::Shared),
                shared_lease: shared.clone(),
                personal_lease: personal,
            })
            .unwrap();
        commands
            .send(Command::WebVerificationFailed {
                source: ApiSource::Shared,
                lease: shared,
                attempt: 0,
                error: ApiError::SignInExpired {
                    api_source: ApiSource::Shared,
                },
            })
            .unwrap();
        commands.send(Command::Shutdown).unwrap();
        runtime.block_on(worker.run(receiver));
        assert!(worker.signed_in);
        assert_eq!(worker.api.account(), Some(AccountId::new("bob")));
        assert!(events.try_iter().next().is_none());
    }

    fn worker(
        name: &str,
    ) -> (
        tokio::runtime::Runtime,
        Worker,
        std::sync::mpsc::Receiver<Event>,
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let root =
            std::env::temp_dir().join(format!("spotifast-auth-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let dirs = AppDirs {
            config: root.join("config"),
            state: root.join("state"),
            cache: root.join("cache"),
        };
        let settings = crate::settings::Settings::default();
        let config = crate::app::engine_config(
            &dirs,
            &settings,
            ProxyConfig::Off,
            crate::vis::AudioTap::new(),
            crate::eq::shared(),
        );
        let http = Http::new(reqwest::Client::new());
        let art = ArtLoader::new(http.clone(), runtime.handle().clone(), dirs.art_cache_dir());
        let (sender, events) = std::sync::mpsc::channel();
        let (commands, _) = mpsc::unbounded_channel();
        let worker = Worker::new(
            dirs,
            config,
            Some("personal".into()),
            http,
            art,
            Arc::new(NetActivity::default()),
            sender,
            commands,
            Waker::default(),
        );
        (runtime, worker, events)
    }

    #[test]
    fn cover_confirmation_checks_the_largest_image_without_spotify_credentials() {
        use std::io::{Read, Write};
        for matches in [false, true] {
            let (runtime, mut worker, events) = worker("cover-check");
            let pixels = image::RgbImage::from_pixel(24, 24, image::Rgb([20, 40, 200]));
            let mut bytes = std::io::Cursor::new(Vec::new());
            pixels
                .write_to(&mut bytes, image::ImageFormat::Png)
                .unwrap();
            let cover = crate::playlist_cover::prepare(bytes.get_ref()).unwrap();
            let returned = if matches {
                cover.jpeg.clone()
            } else {
                let earlier = image::RgbImage::from_pixel(24, 24, image::Rgb([200, 20, 40]));
                let mut bytes = std::io::Cursor::new(Vec::new());
                earlier
                    .write_to(&mut bytes, image::ImageFormat::Png)
                    .unwrap();
                crate::playlist_cover::prepare(bytes.get_ref())
                    .unwrap()
                    .jpeg
            };
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut request = Vec::new();
                let mut buffer = [0; 1024];
                while !request.windows(4).any(|part| part == b"\r\n\r\n") {
                    let count = socket.read(&mut buffer).unwrap();
                    assert!(count > 0 && request.len() < 8192);
                    request.extend_from_slice(&buffer[..count]);
                }
                let request = String::from_utf8(request).unwrap().to_lowercase();
                assert!(request.starts_with("get /full "));
                assert!(!request.contains("authorization:"));
                write!(socket,"HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",returned.len()).unwrap();
                socket.write_all(&returned).unwrap();
            });
            let images = vec![
                crate::api::models::Image {
                    url: format!("http://{address}/thumbnail"),
                    width: Some(64),
                    height: Some(64),
                },
                crate::api::models::Image {
                    url: format!("http://{address}/full"),
                    width: Some(640),
                    height: Some(640),
                },
            ];
            let (commands, receiver) = mpsc::unbounded_channel();
            commands
                .send(Command::CheckPlaylistCover {
                    id: "pl1".into(),
                    request: 7,
                    cover,
                    images: images.clone(),
                })
                .unwrap();
            runtime.block_on(async {
                let controller = async {
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                    loop {
                        if let Ok(event) = events.try_recv() {
                            match event {
                                Event::PlaylistCoverChecked {
                                    id,
                                    request,
                                    images: checked,
                                    result,
                                } => {
                                    assert_eq!(id, "pl1");
                                    assert_eq!(request, 7);
                                    assert_eq!(checked, images);
                                    assert_eq!(result, Ok(matches));
                                    commands.send(Command::Shutdown).unwrap();
                                    break;
                                }
                                _ => panic!("unexpected event during isolated cover check"),
                            }
                        }
                        assert!(
                            tokio::time::Instant::now() < deadline,
                            "cover check timed out"
                        );
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                };
                tokio::join!(worker.run(receiver), controller);
            });
            server.join().unwrap();
        }
    }

    #[test]
    fn old_protected_web_grants_wait_for_cover_consent_without_erasing_credentials() {
        for slot in [CredentialSlot::Shared, CredentialSlot::Personal] {
            let (runtime, mut worker, events) = worker("cover-consent");
            let lease = worker.credentials.lease(slot);
            let token = crate::auth::StoredToken {
                client_id: if slot == CredentialSlot::Shared {
                    crate::auth::DEFAULT_WEB_CLIENT_ID.into()
                } else {
                    "personal".into()
                },
                access_token: "dummy-access".into(),
                refresh_token: "dummy-refresh".into(),
                expires_at: u64::MAX,
                scope: crate::auth::WEB_SCOPES
                    .iter()
                    .filter(|scope| **scope != "ugc-image-upload")
                    .copied()
                    .collect::<Vec<_>>()
                    .join(" "),
            };
            runtime
                .block_on(lease.save(StoredGrant::Web(token)))
                .unwrap();
            worker.restore_pending[slot.index()] = true;
            let loaded = runtime.block_on(lease.load());
            worker.on_credentials_restored(slot, lease.clone(), loaded);
            assert!(!worker.restore_pending[slot.index()]);
            assert!(worker.web_tokens[slot.index()].is_none());
            assert!(!worker.signed_in);
            let emitted: Vec<_> = events.try_iter().collect();
            // Signing in renews the shared app; a personal app is renewed only
            // by authorizing it again in Settings (#634).
            let advice = if slot == CredentialSlot::Shared {
                "Sign in again."
            } else {
                "Authorize it again in Settings, under Account."
            };
            assert!(emitted.iter().any(|event| matches!(event,
                Event::Error(message)
                    if message.contains("permissions changed") && message.ends_with(advice))));
            assert!(
                emitted
                    .iter()
                    .any(|event| matches!(event, Event::Auth(AuthStatus::SignedOut)))
            );
            assert!(lease.current());
            assert!(matches!(runtime.block_on(lease.load()).unwrap().grant,
                Some(StoredGrant::Web(saved)) if saved.refresh_token == "dummy-refresh"));
        }
    }

    #[test]
    fn signout_rejects_late_restore_browser_verification_and_engine_results() {
        let (runtime, mut worker, events) = worker("late-authorization-results");
        let shared = worker.credentials.lease(CredentialSlot::Shared);
        let playback = worker.credentials.lease(CredentialSlot::Playback);
        let attempt = worker.authorization_attempt;
        let album_type_session = worker.album_type_lookup.session_generation;
        let token = crate::auth::StoredToken {
            client_id: crate::auth::DEFAULT_WEB_CLIENT_ID.into(),
            access_token: "dummy-access".into(),
            refresh_token: "dummy-refresh".into(),
            ..Default::default()
        };
        let (commands, receiver) = mpsc::unbounded_channel();
        commands.send(Command::SignOut).unwrap();
        commands
            .send(Command::CredentialsRestored {
                slot: CredentialSlot::Playback,
                lease: playback.clone(),
                result: Ok(crate::credentials::Loaded {
                    grant: Some(StoredGrant::Playback(Credentials::with_password(
                        "dummy-account",
                        "dummy-grant",
                    ))),
                    warning: None,
                }),
            })
            .unwrap();
        commands
            .send(Command::WebSignedIn {
                source: ApiSource::Shared,
                token: Box::new(token.clone()),
                lease: shared.clone(),
                attempt,
            })
            .unwrap();
        commands
            .send(Command::WebVerified {
                source: ApiSource::Shared,
                token: Box::new(token),
                user: Box::new(User {
                    id: "dummy-account".into(),
                    product: Some("premium".into()),
                    ..Default::default()
                }),
                lease: shared,
                attempt,
            })
            .unwrap();
        commands
            .send(Command::PlaybackAuthorized {
                access_token: "dummy-streaming-token".into(),
                lease: playback.clone(),
                attempt,
            })
            .unwrap();
        commands
            .send(Command::EngineConnected {
                session_generation: album_type_session,
                engine: Box::new(None),
                error: Some("late engine error".into()),
                lease: playback,
            })
            .unwrap();
        commands.send(Command::Shutdown).unwrap();
        runtime.block_on(worker.run(receiver));
        assert!(!worker.signed_in);
        assert!(!worker.engine_busy);
        assert!(worker.playback_grant.is_none());
        assert!(worker.web_tokens.iter().all(Option::is_none));
        assert!(worker.api.account().is_none());
        assert!(events.try_iter().all(|event| !matches!(
            event,
            Event::Auth(AuthStatus::Connected { .. })
                | Event::Playback(
                    LocalPlayback::Connecting
                        | LocalPlayback::Ready { .. }
                        | LocalPlayback::Failed(_)
                )
        )));
        let _ = std::fs::remove_dir_all(worker.dirs.state.parent().unwrap());
    }

    #[test]
    fn restored_playback_requires_the_verified_account() {
        let (runtime, mut worker, events) = worker("playback-account-mismatch");
        let _entered = runtime.enter();
        verify(&mut worker, ApiSource::Shared, "alice");
        worker.playback_grant = Some(Credentials::with_password("bob", "dummy-reusable-grant"));
        worker.resume_engine();
        assert!(!worker.engine_busy);
        assert!(worker.engine.is_none());
        assert!(!playback_account_matches(
            worker.playback_grant.as_ref().unwrap(),
            worker.api.account()
        ));
        assert!(
            events
                .try_iter()
                .any(|event| matches!(event, Event::Playback(LocalPlayback::Failed(_))))
        );
    }

    #[test]
    fn engine_cache_never_writes_a_playback_grant_file() {
        let (_runtime, worker, _) = worker("memory-playback-cache");
        let cache = worker.engine_config.open_cache().unwrap();
        let cloned = cache.clone();
        cache.save_credentials(&Credentials::with_password("dummy-account", "dummy-grant"));
        assert!(cloned.credentials().is_some());
        assert!(
            !worker
                .dirs
                .credentials_dir()
                .join("credentials.json")
                .exists()
        );
        let _ = std::fs::remove_dir_all(worker.dirs.state.parent().unwrap());
    }

    #[test]
    fn search_requests_keep_their_scope_when_personal_readiness_changes() {
        let (_runtime, worker, _) = worker("search-routing");
        for ready in [false, true] {
            worker.api.set_state(
                ApiSource::Personal,
                if ready {
                    SessionState::Ready {
                        account: AccountId::new("alice"),
                    }
                } else {
                    SessionState::Unavailable
                },
            );
            assert_eq!(
                operation_for(
                    &worker.api,
                    &ApiRequest::Search {
                        query: "q".into(),
                        serial: 1,
                    }
                ),
                Operation::PlaylistSearch
            );
            assert_eq!(
                operation_for(
                    &worker.api,
                    &ApiRequest::SearchCatalogue {
                        query: "q".into(),
                        serial: 1,
                    }
                ),
                Operation::CatalogSearch
            );
            assert_eq!(
                operation_for(
                    &worker.api,
                    &ApiRequest::SearchPlaylists {
                        query: "q".into(),
                        serial: 1,
                    }
                ),
                Operation::PlaylistSearch
            );
        }
    }

    #[test]
    fn new_empty_queries_and_signout_cancel_searches_waiting_for_shared_access() {
        let (runtime, mut worker, _) = worker("search-cancellation");
        runtime.block_on(async {
            for cancel in 0..3 {
                worker
                    .api
                    .set_state(ApiSource::Shared, SessionState::Authorizing);
                worker.search("old".into(), 1);
                tokio::task::yield_now().await;
                let old = worker.search_tasks[0].clone();
                assert!(!old.is_finished(), "old search waits for shared access");
                match cancel {
                    0 => worker.search("new".into(), 2),
                    1 => worker.search(String::new(), 2),
                    _ => worker.sign_out(),
                }
                tokio::task::yield_now().await;
                assert!(old.is_finished(), "abandoned search was cancelled");
                worker.cancel_search();
            }
        });
    }

    fn verify(worker: &mut Worker, source: ApiSource, account: &str) {
        worker.api.set_state(source, SessionState::Authorizing);
        worker.on_web_verified(
            source,
            crate::auth::StoredToken {
                client_id: if source == ApiSource::Personal {
                    "personal"
                } else {
                    crate::auth::DEFAULT_WEB_CLIENT_ID
                }
                .into(),
                ..Default::default()
            },
            User {
                id: account.into(),
                display_name: Some("Listener".into()),
                product: Some("premium".into()),
                ..Default::default()
            },
        );
    }

    #[test]
    fn personal_verification_unblocks_sign_in_while_shared_verification_waits() {
        let (runtime, mut worker, events) = worker("personal-first");
        let _entered = runtime.enter();
        worker
            .api
            .set_state(ApiSource::Shared, SessionState::Authorizing);
        verify(&mut worker, ApiSource::Personal, "alice");
        assert!(worker.signed_in);
        assert_eq!(worker.premium, Some(true));
        assert_eq!(
            worker.api.state(ApiSource::Shared),
            SessionState::Authorizing
        );
        let emitted: Vec<_> = events.try_iter().collect();
        assert!(
            emitted
                .iter()
                .any(|event| matches!(event, Event::Auth(AuthStatus::Connected { .. })))
        );
        assert!(emitted.iter().any(|event| matches!(event, Event::Api(response) if matches!(response.as_ref(), ApiResponse::Me(Ok(user)) if user.id == "alice"))));
        let credentials =
            playback_credentials(worker.api.account(), "dummy-streaming-token".into()).unwrap();
        assert_eq!(credentials.username.as_deref(), Some("alice"));
        assert_eq!(credentials.auth_data, b"dummy-streaming-token");
        verify(&mut worker, ApiSource::Shared, "alice");
        assert!(worker.signed_in);
        assert_eq!(worker.api.account(), Some(AccountId::new("alice")));
        assert!(
            !events
                .try_iter()
                .any(|event| matches!(event, Event::Auth(AuthStatus::Connected { .. })))
        );
    }

    /// The rootlist carries the edit permission for a playlist shared by
    /// invitation, and its request is sent once, when the playlist library
    /// finishes. A cached web token can finish that before the engine
    /// connects, so the request has to wait rather than be dropped.
    #[test]
    fn a_rootlist_request_before_the_engine_waits_for_it() {
        let (runtime, mut worker, events) = worker("rootlist-before-engine");
        let _entered = runtime.enter();
        assert!(worker.engine.is_none());

        worker.signed_in = true;
        worker.fetch_rootlist();
        worker.fetch_rootlist();
        assert!(worker.rootlist_pending);

        let (commands, mut results) = mpsc::unbounded_channel();
        worker.commands = commands;
        runtime.block_on(async {
            let fetched = || async {
                Ok(crate::player::Rootlist {
                    entries: vec![crate::player::RootlistEntry::Playlist(
                        "spotify:playlist:shared".into(),
                    )],
                    editable: ["spotify:playlist:shared".into()].into(),
                })
            };
            worker.start_pending_rootlist(Some(fetched()));
            worker.start_pending_rootlist(Some(fetched()));
            let Command::RootlistFinished { generation, result } = results.recv().await.unwrap()
            else {
                panic!("expected the deferred rootlist");
            };
            assert!(
                result
                    .as_ref()
                    .unwrap()
                    .editable
                    .contains("spotify:playlist:shared")
            );
            worker.on_rootlist_finished(generation, result);
            tokio::task::yield_now().await;
            assert!(
                results.try_recv().is_err(),
                "engine readiness fetches only once"
            );
            assert!(!worker.rootlist_pending);
        });
        assert!(matches!(
            events.try_recv(),
            Ok(Event::Rootlist { result: Ok(_) })
        ));
    }

    #[test]
    fn signout_cancels_pending_and_completed_rootlist_work() {
        let (runtime, mut worker, events) = worker("rootlist-signout");
        let _entered = runtime.enter();
        worker.signed_in = true;
        worker.fetch_rootlist();
        let generation = *worker.session.borrow();
        worker.sign_out();
        assert!(!worker.rootlist_pending);
        worker.fetch_rootlist();
        assert!(
            !worker.rootlist_pending,
            "signed-out work must not be deferred"
        );
        events.try_iter().for_each(drop);
        worker.signed_in = true;
        worker.on_rootlist_finished(generation, Err("previous account".into()));
        assert!(
            events.try_recv().is_err(),
            "late results cannot reach a new account"
        );
        assert!(!worker.rootlist_pending);
    }

    #[test]
    fn premium_without_local_playback_keeps_album_type_work_bounded() {
        let (runtime, mut worker, _) = worker("album-types-without-playback");
        let _entered = runtime.enter();
        verify(&mut worker, ApiSource::Shared, "alice");
        assert_eq!(worker.premium, Some(true));
        assert!(worker.playback_grant.is_none());
        assert!(worker.engine.is_none());

        worker.fetch_album_types(
            (0..MAX_PENDING_ALBUM_TYPES + 10)
                .map(|index| format!("spotify:album:{index}"))
                .collect(),
        );

        assert!(worker.album_type_lookup.active.is_none());
        assert_eq!(
            worker.album_type_lookup.pending.len(),
            MAX_PENDING_ALBUM_TYPES
        );
        assert_eq!(worker.album_type_lookup.seen.len(), MAX_PENDING_ALBUM_TYPES);
    }

    #[test]
    fn a_mismatched_grant_cannot_replace_the_verified_playback_account() {
        let (runtime, mut worker, events) = worker("mismatch");
        let _entered = runtime.enter();
        verify(&mut worker, ApiSource::Shared, "alice");
        let _ = events.try_iter().collect::<Vec<_>>();
        verify(&mut worker, ApiSource::Personal, "bob");
        assert_eq!(worker.api.account(), Some(AccountId::new("alice")));
        assert_eq!(
            worker.api.state(ApiSource::Personal),
            SessionState::Unavailable
        );
        assert!(
            events
                .try_iter()
                .any(|event| matches!(event, Event::Error(_)))
        );
    }

    #[test]
    fn a_playback_browser_result_after_sign_out_cannot_start_an_engine() {
        let (_runtime, mut worker, events) = worker("signed-out");
        worker.engine_busy = true;
        worker.on_playback_authorized("dummy-streaming-token".into());
        assert!(!worker.engine_busy);
        assert!(worker.engine.is_none());
        assert!(
            events
                .try_iter()
                .any(|event| matches!(event, Event::Playback(LocalPlayback::Failed(_))))
        );
        assert!(playback_credentials(Some(AccountId::new("")), "dummy".into()).is_none());
    }
}

fn web_slot(source: ApiSource) -> CredentialSlot {
    match source {
        ApiSource::Shared => CredentialSlot::Shared,
        ApiSource::Personal => CredentialSlot::Personal,
    }
}

fn playback_account_matches(credentials: &Credentials, account: Option<AccountId>) -> bool {
    credentials
        .username
        .as_deref()
        .filter(|name| !name.is_empty())
        .is_some_and(|name| same_account(name, account.as_ref()))
}

#[cfg(test)]
mod session_tests {
    use super::*;
    use crate::session_reads::Failure;

    /// The session answers only for the account the Web API verified.
    #[test]
    fn the_session_answers_only_for_the_web_apis_account() {
        let alice = AccountId::new("alice");
        assert!(same_account("alice", Some(&alice)));
        assert!(!same_account("bob", Some(&alice)));
        assert!(!same_account("alice", None), "nothing verified yet");
    }

    /// A header read, a page of rows or a sample at the request's offset,
    /// and nothing else: a duplicate check shares the rows' operation but
    /// stays with the Web API.
    #[test]
    fn a_request_asks_the_session_for_a_header_or_a_page_or_nothing() {
        let playlist = ApiRequest::Playlist {
            id: "pl1".into(),
            generation: 1,
        };
        assert_eq!(
            session_read(&playlist),
            Some(SessionRead::Header { id: "pl1" })
        );
        let items = ApiRequest::PlaylistItems {
            id: "pl1".into(),
            offset: 150,
            generation: 1,
        };
        assert_eq!(
            session_read(&items),
            Some(SessionRead::Rows {
                id: "pl1",
                offset: 150
            })
        );
        let sample = ApiRequest::PlaylistSample {
            id: "pl1".into(),
            offset: 150,
            generation: 1,
        };
        assert_eq!(
            session_read(&sample),
            Some(SessionRead::Sample {
                id: "pl1",
                offset: 150
            })
        );
        let duplicates = ApiRequest::CheckPlaylistDuplicates {
            playlist_id: "pl1".into(),
            playlist_name: "Mine".into(),
            items: Vec::new(),
            position: None,
        };
        assert_eq!(session_read(&duplicates), None);
        assert_eq!(session_read(&ApiRequest::Me), None);
    }

    /// A session answer reaches the app as the Web API's would: a value, a
    /// final refusal, or nothing, so the Web API is asked instead.
    #[test]
    fn a_session_answer_settles_like_a_web_api_one() {
        assert!(matches!(settle(Ok::<u8, Failure>(7)), Some(Ok(7))));
        let refused = settle(Err::<u8, Failure>(Failure::Definitive(ApiError::Status {
            status: 403,
            message: "Forbidden".into(),
        })));
        assert!(matches!(
            refused,
            Some(Err(ApiError::Status { status: 403, .. }))
        ));
        let dropped = settle(Err::<u8, Failure>(Failure::Retry(anyhow::anyhow!("gone"))));
        assert!(dropped.is_none(), "the Web API gets its turn");
    }

    /// The response carries the request's own id, offset, and generation;
    /// the app drops an answer whose generation is not the page's.
    #[test]
    fn a_session_answer_carries_the_requests_identity() {
        let rows = || SessionAnswer::Rows(Ok(Page::default()));
        let header = || SessionAnswer::Header(Ok(Playlist::default()));
        let items = ApiRequest::PlaylistItems {
            id: "pl1".into(),
            offset: 150,
            generation: 7,
        };
        assert!(matches!(
            session_response(&items, rows()),
            Some(ApiResponse::PlaylistItems { id, offset: 150, generation: 7, result: Ok(_) }) if id == "pl1"
        ));
        let sample = ApiRequest::PlaylistSample {
            id: "pl1".into(),
            offset: 150,
            generation: 7,
        };
        assert!(matches!(
            session_response(&sample, rows()),
            Some(ApiResponse::PlaylistSample { id, generation: 7, result: Ok(_) }) if id == "pl1"
        ));
        let playlist = ApiRequest::Playlist {
            id: "pl1".into(),
            generation: 7,
        };
        assert!(matches!(
            session_response(&playlist, header()),
            Some(ApiResponse::Playlist { id, generation: 7, result: Ok(_) }) if id == "pl1"
        ));
        assert!(
            session_response(&playlist, rows()).is_none(),
            "rows are no header"
        );
        assert!(
            session_response(&ApiRequest::Me, header()).is_none(),
            "nothing else is served here"
        );
    }
}

#[cfg(test)]
mod cover_routing_tests {
    use super::*;
    use crate::api::gateway::{AccountId, PlaylistAccess};

    #[test]
    fn cover_upload_uses_the_existing_playlist_mutation_route() {
        let api = ApiGateway::new(
            reqwest::Client::new(),
            std::sync::Arc::new(NetActivity::default()),
        );
        api.install(ApiSource::Shared, AccountId::new("me"))
            .unwrap();
        let request = ApiRequest::UploadPlaylistCover {
            id: "test".into(),
            request: 1,
            previous_urls: vec![],
            cover: crate::playlist_cover::Cover {
                jpeg: Vec::new().into(),
                encoded: "".into(),
                uri: String::new(),
            },
        };
        assert_eq!(
            operation_for(&api, &request),
            Operation::PlaylistMutation(PlaylistAccess::Unknown)
        );
        let mut playlist = Playlist {
            id: "test".into(),
            ..Default::default()
        };
        playlist.owner.id = Some("me".into());
        api.observe_playlist(&playlist);
        assert_eq!(
            operation_for(&api, &request),
            Operation::PlaylistMutation(PlaylistAccess::Owned)
        );
        playlist.owner.id = Some("other".into());
        playlist.collaborative = true;
        api.observe_playlist(&playlist);
        assert_eq!(
            operation_for(&api, &request),
            Operation::PlaylistMutation(PlaylistAccess::Collaborative)
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn six_session_drops_in_ten_minutes_give_up() {
        let start = Instant::now();
        let mut reconnects = Vec::new();
        for i in 0..RECONNECT_LIMIT {
            let at = start + Duration::from_secs(i as u64);
            assert!(
                !session_drops_exhausted(&mut reconnects, at),
                "attempt {i} should still reconnect"
            );
            reconnects.push(at);
        }
        assert!(session_drops_exhausted(
            &mut reconnects,
            start + Duration::from_secs(30)
        ));
    }

    #[test]
    fn session_drops_outside_the_window_do_not_count() {
        let start = Instant::now();
        let mut reconnects = vec![start; RECONNECT_LIMIT];
        assert!(!session_drops_exhausted(
            &mut reconnects,
            start + RECONNECT_WINDOW + Duration::from_secs(1)
        ));
        assert!(reconnects.is_empty());
    }

    #[test]
    fn an_engine_change_during_connect_is_deferred() {
        let mut pending = false;
        assert!(defer_engine_replace(true, &mut pending));
        assert!(pending);
        assert!(!defer_engine_replace(false, &mut pending));
        assert!(pending, "finishing the attempt owns clearing the request");
    }
}
