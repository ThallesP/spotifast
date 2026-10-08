---
title: Privacy
description: What Spotifast stores on your computer, what it sends and to whom, and what it never collects.
nav_order: 4
---

Spotifast is a desktop app that runs entirely on your computer. It has no
account of its own, no server, no analytics, and no advertising. Its author
receives nothing about you or how you use it. Diagnostics go only to an Axiom
dataset of your own, and only when you set one up.

This page covers the Spotifast app, version 0.8.0 and later. Earlier versions
kept sign-ins in files instead of the system credential store; update to a
current release.

## What stays on your computer

- **Spotify sign-ins.** The grants Spotify issues when you sign in, and the
  reusable playback credential, are kept in the system credential store:
  Credential Manager on Windows, Keychain on macOS, and Secret Service on
  Linux. Your Spotify password never passes through Spotifast; you sign in
  on Spotify's own pages. A proxy password, if you set one, uses the same
  store.
- **Settings and history.** Settings, window positions, recent plays, the
  last session, skins, themes and MilkDrop presets live in the config
  directory.
- **Caches.** Downloaded audio, artwork, lyrics and library metadata live in
  the cache directory and can be deleted at any time.
- **Log.** `spotifast.log` records errors and diagnostics. It stays on your
  computer and never contains credentials; share it only if you choose to
  attach it to a bug report.

[Settings & Files](/settings-and-files/) lists every location and what is safe
to delete. **Sign out** in Settings removes the stored credentials.

## What is sent, and to whom

Spotifast connects only to the services below.

- **Spotify.** Sign-in, your library, search, playlists, playback and Spotify
  Connect all go to Spotify, under your account. Spotify's own
  [privacy policy](https://www.spotify.com/legal/privacy-policy/) applies to
  that data.
- **LRCLIB.** When the lyrics panel is open and Spotify has no lyrics for the
  song, Spotifast sends its artist, title, album and length to
  [lrclib.net](https://lrclib.net). Nothing identifying you is included.
- **GitHub.** Once a day, Spotifast asks GitHub for the latest release. You
  can turn automatic checks off in Settings. Downloading an update, and the
  first opening of MilkDrop, also fetch files from GitHub. No Spotify data is
  sent.
- **Your local network.** Spotifast looks for Spotify Connect speakers over
  mDNS and talks to the ones you choose.
- **Your own Axiom dataset, only if you set one up.** This build can send
  diagnostics to an [Axiom](https://axiom.co) dataset you own, to investigate
  playback, rate-limit and performance problems. It is off unless you put
  your own token in `telemetry.json`; see
  [Diagnostics you turn on](#diagnostics-you-turn-on).

Links you open from the app, such as the Winamp Skin Museum or this website,
open in your browser.

## Diagnostics you turn on

Nothing below happens unless `telemetry.json` exists in the config directory
(or `SPOTIFAST_AXIOM_TOKEN` is set) with a token for an Axiom dataset you own:

```json
{ "axiom_token": "xaat-...", "dataset": "spotifast" }
```

`SPOTIFAST_TELEMETRY=off` turns it off again without deleting the file.
Events go only to that dataset, in batches every few seconds, through the
proxy configured in Settings. An invalid proxy holds them, as it holds every
other request. Events that could not be sent are kept in
`telemetry-spool.ndjson` in the state directory and sent on the next launch.

Events record playback state changes and their causes, timings for loading and
skipping songs, Web API requests (endpoint with IDs replaced, which grant was
used, status, timing and rate-limit headers), Spotify Connect activity, audio
output problems, slow interface frames, sleep and wake, resource use, warnings
from the log, and panics. They include song, album and playlist IDs and names.
A detailed trail is kept in memory and sent only when something goes wrong.

They never contain Spotify credentials, authorization codes or responses,
access or refresh tokens, `Authorization` headers, proxy passwords, full URLs,
IP addresses, your Spotify account name, or file paths. Links, paths,
`spotify:user:` names and token-shaped text are removed from log messages, and
log lines from sign-in code are reduced to their level and source. Audio
device, speaker and network names appear only as digests keyed to your
installation, so they can be grouped but not read.

## This website

spotifast.rocks counts page visits with [Plausible](https://plausible.io/data-policy),
which uses no cookies and collects no personal data. The app itself contains
no analytics.

## Questions

Ask on [GitHub](https://github.com/crmne/spotifast/issues).
