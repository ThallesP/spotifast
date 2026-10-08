---
title: Diagnostics
description: Opt-in telemetry to your own Axiom dataset, what it records, and queries for common problems.
nav_order: 7
---

Spotifast can send detailed diagnostics to an [Axiom](https://axiom.co)
dataset you own. It is off unless you configure it, and nothing goes anywhere
else. [Privacy](/privacy/#diagnostics-you-turn-on) lists what is and is never
recorded.

## Turn it on

Create `telemetry.json` in the config directory
(`~/Library/Application Support/me.paolino.spotifast/` on macOS,
`~/.config/spotifast/` on Linux, `%APPDATA%\paolino\spotifast\config\` on
Windows) and restart Spotifast:

```json
{ "axiom_token": "xaat-...", "dataset": "spotifast" }
```

Optional keys: `endpoint` (default `https://us-east-1.aws.edge.axiom.co`, or
your organisation's edge deployment, or `https://api.axiom.co`) and `verbose`
(`true` ships every detail as it happens instead of only around problems).
`SPOTIFAST_AXIOM_TOKEN`, `SPOTIFAST_AXIOM_DATASET` and `SPOTIFAST_AXIOM_URL`
override the file, and `SPOTIFAST_TELEMETRY=off` disables it.

Use an ingest-only token limited to that dataset.

## How it is shaped

Every record has `event`, `_time`, `uptime_ms` (monotonic, stops while a Mac
sleeps), `seq` (gaps mean dropped events), `launch` (one per run), `install`,
`v`, `os`, `arch` and `thread`, plus context that applies at the time:
`engine_gen`, `connect_active`, `output_route`, `output_rate`,
`playback_target`, `personal_app` (the personal app's grant is ready),
`personal_app_configured`, and `since_wake_ms` or `since_network_change_ms`
for two minutes after a wake or network change.

- **Events** ship within about five seconds.
- **Crumbs** stay in an in-memory flight recorder. When an **anomaly** fires
  (`event == "anomaly"`, with `kind` and `anomaly_id`), the last two minutes of
  crumbs ship with it, each tagged `dump_of` with that `anomaly_id`, followed
  by an `anomaly.dump` summary. Crumbs an earlier dump already sent are not
  sent again; `earlier_through_crumb_seq` says up to where, so query by
  `launch` and `crumb_seq` to see the whole window.
- **Limits** keep a storm affordable: about 20 events a second on average
  (bursts of 600), at most six anomalies of one kind a minute, and five
  shipped log lines per source line a minute. What is over the limit stays in
  the recorder with `demoted: true`, and the heartbeat counts it
  (`events_demoted`, `log_suppressed`).
- **`heartbeat`** every 30 seconds carries counter deltas and gauges for the
  audio callback, the player, the Web API, the backend runtime and the
  interface, plus CPU, peak memory, involuntary context switches and page
  faults.
- **`intent`** records what was asked for and from where (`action`, `source`:
  ui, keyboard, media_key, menu, notch, touchbar, tray, winamp,
  remote_control, system). Repeats within 600 ms carry `repeat_of`.
- **`trace.hop`** and **`trace.done`** time an operation such as Next across
  threads: `action_applied`, `backend_received`, `spirc_sent`,
  `play_request`, `loading` or `preload_hit`, `track_changed`, and
  `first_audio`. A trace over budget raises `anomaly` `trace.slow` with the
  slowest hop.
- **`playback.state`** records each Playing, Paused and Stopped with the most
  recent `cause` (an intent, a remote command, an output failure, a session
  loss). A pause or resume with no cause raises
  `playback.pause_without_cause` or `playback.resume_without_cause`.

Events that could not be sent wait in `telemetry-spool.ndjson`, and a panic
that could not be sent in `telemetry-crash.ndjson`, in the state directory.
They ship on the next launch with `spooled: true`.

## Queries

Problems by kind, last day:

```kusto
['spotifast'] | where _time > ago(1d) and event == "anomaly"
| summarize count() by kind | order by count_ desc
```

Everything around one anomaly, in order:

```kusto
['spotifast'] | where anomaly_id == "<id>" or dump_of == "<id>"
| order by uptime_ms asc
```

Why playback paused:

```kusto
['spotifast'] | where event == "playback.state" and state in ("paused", "stopped")
| project _time, state, cause, cause_detail, cause_age_ms, output_route
```

Pause and resume within a few seconds, with what the audio output did:

```kusto
['spotifast'] | where event in ("playback.state", "audio.output.reopened",
  "audio.output.error", "audio.ran_dry", "audio.writer_stall",
  "session.lost", "system.sleep_detected", "system.network_changed")
  or (event == "anomaly" and kind startswith "audio")
| order by _time asc
```

Slow Next, by slowest step:

```kusto
['spotifast'] | where event in ("trace.done", "anomaly") and trace == "next"
| summarize count(), avg(total_ms), percentile(total_ms, 95) by slowest_hop
```

Web API rate limits, by app and endpoint:

```kusto
['spotifast'] | where event == "api.request" and status == 429
| summarize count(), make_set(retry_after_raw) by grant, path_template
```

Web API traffic per endpoint and app, per minute:

```kusto
['spotifast'] | where event == "api.request"
| summarize count() by bin(_time, 1m), grant, path_template
```

Audio health over time:

```kusto
['spotifast'] | where event == "heartbeat"
| project _time, audio_underrun_frames, audio_ran_dry, audio_writer_stalls,
  audio_write_max_gap_ms, audio_queue_min_ms, audio_cb_max_gap_us,
  backend_runtime_lag_max_ms, cpu_percent
```
