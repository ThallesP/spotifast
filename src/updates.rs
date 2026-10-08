//! Self-update from GitHub releases, through fastframe-update.
//!
//! The previous release's helper installs the first release built on the
//! crate, so what it relies on stays the same: `--version` prints
//! `<command> <version>`, the new app accepts `--update-receipt` and
//! `--update-error` (stripped by [`fastframe_update::intercept`] before the
//! argument parser), and the handoff and receipt files keep their format.

pub use fastframe_update::{
    CHECK_INTERVAL, DownloadState, Installation, Kind, Prepared, Release, Source, Unsupported,
    Updater,
};
use fastframe_update::{MacConfig, MacTarget, ReqwestTransport, UpdateConfig};

/// The repository whose releases this build updates from. A fork's own
/// builds set `SPOTIFAST_UPDATE_REPOSITORY` to follow that fork's releases.
const REPOSITORY: &str = match option_env!("SPOTIFAST_UPDATE_REPOSITORY") {
    Some(repository) => repository,
    None => "crmne/spotifast",
};

/// Official releases carry a universal disk image. A build made with
/// `SPOTIFAST_UPDATE_ARM64_ONLY` set expects Apple silicon images instead.
const MAC_TARGET: MacTarget = match option_env!("SPOTIFAST_UPDATE_ARM64_ONLY") {
    Some(_) => MacTarget::Arm64Only,
    None => MacTarget::Universal,
};

pub const CONFIG: UpdateConfig = UpdateConfig {
    macos: MacConfig {
        bundle_ids: &["rocks.spotifast.Spotifast"],
        executable_names: &["Spotifast"],
        legacy_bundle_names: &[],
    },
    // Releases are verified against checksums.txt alone until they are
    // signed. Only a version shipped after the first signed release may
    // carry the key: from then on an unsigned release is refused.
    publisher_key: None,
    mac_target: MAC_TARGET,
    ..UpdateConfig::new(
        REPOSITORY,
        "Spotifast",
        "spotifast",
        env!("CARGO_PKG_VERSION"),
    )
};

/// An updater on Spotifast's HTTP client, through the configured proxy.
pub fn updater(proxy: &crate::settings::ProxyConfig) -> anyhow::Result<Updater> {
    let builder = crate::http::blocking_builder(proxy).map_err(anyhow::Error::msg)?;
    Ok(Updater::new(CONFIG, ReqwestTransport::new(builder)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_config_is_valid() {
        CONFIG.validate().unwrap();
        assert_eq!(CONFIG.current_version, env!("CARGO_PKG_VERSION"));
    }
}
