# TicketSwap STEP2 on a VPS (headed parity)

## Why headless often fails

TicketSwap frequently serves a verification shell or a stripped-down document to automated headless browsers. That HTML can look like a successful HTTP response while containing **zero** usable ticket links, which previously surfaced as empty discovery results instead of a clear signal.

## Why headed + a persistent profile

A normal Chrome session with a **manually trusted** user profile (cookies, local storage, prior verification) receives the same React app you see on your laptop. **No passwords belong in environment variables or git**: log in once with real Chrome pointed at `TICKETSWAP_PROFILE_DIR`, then reuse that directory on the server.

## `headed_vps` mode

Set:

```bash
export TICKETSWAP_BROWSER_MODE=headed_vps
export TICKETSWAP_HEADLESS=0
export TICKETSWAP_PROFILE_DIR=/opt/ticketswap/profile
export DISPLAY=:99   # or your VNC display
```

Behavior:

- Never uses headless Chrome for STEP2 prerequisites in this mode.
- On Linux, **requires** `DISPLAY` (start Xvfb or use VNC/noVNC).
- **Requires** an existing persistent profile directory (not `--anonymous-profile`).
- Enables slow timings, interaction rounds, and **debug artifact dumps** under `tmp/ticketswap_debug/<slug>/` when STEP2 fails (`STEP2_DEBUG_DUMP_ON_FAILURE`).
- Takes an exclusive file lock: `<TICKETSWAP_PROFILE_DIR>/.step2_profile.lock`. A second run exits immediately with:  
  `Profile already in use. Stop the other STEP2/Chrome process or use a different profile.`

CLI equivalent:

```bash
python -m pipeline.run_pipeline --mode discovery --headed-vps --profile-dir /opt/ticketswap/profile ...
```

Do **not** combine `--headed-vps` with `--headless`.

## One Chrome per profile

Chrome locks its user-data-dir. Two simultaneous processes on the same profile corrupt state or hang extraction. The `.step2_profile.lock` file enforces a single writer; still avoid launching manual Chrome against the same path while STEP2 runs.

## Xvfb, VNC, or SSH X forwarding

- **Xvfb**: virtual framebuffer, no physical monitor. Example: `Xvfb :99 -screen 0 1920x1080x24 &` then `export DISPLAY=:99`.
- **VNC/noVNC**: real or virtual desktop you can open in a browser; easiest for manual verification.
- **SSH `-X`**: forward X11 if your client supports it.

`scripts/bootstrap_ticketswap_vps_profile.sh` starts Xvfb on Linux when `DISPLAY` is unset (if `Xvfb` is installed).

## Bootstrap profile (manual login only)

```bash
export TICKETSWAP_PROFILE_DIR=/opt/ticketswap/profile
export TICKETSWAP_BROWSER_MODE=headed_vps
export TICKETSWAP_HEADLESS=0
export DISPLAY=:99
bash scripts/bootstrap_ticketswap_vps_profile.sh
```

Complete login or security checks in the opened Chrome window, then close it (or press Enter when prompted). **This script never asks for or saves passwords** — it only persists whatever Chrome writes into the profile directory.

## Smoke test

```bash
export TICKETSWAP_PROFILE_DIR=/opt/ticketswap/profile
export TICKETSWAP_BROWSER_MODE=headed_vps
export TICKETSWAP_HEADLESS=0
export DISPLAY=:99
bash scripts/smoke_step2_vps.sh
```

Optional: scan all 18 canonical hubs after the first URL succeeds:

```bash
bash scripts/smoke_step2_vps.sh --all-18
```

Results append to `tmp/smoke_step2_vps.jsonl`.

## Discovery: eighteen hubs in one Chrome session

Pipeline flag (or env `TICKETSWAP_VPS_EIGHTEEN=1`):

```bash
python -m pipeline.run_pipeline \
  --mode discovery \
  --headed-vps \
  --profile-dir /opt/ticketswap/profile \
  --vps-eighteen-hubs \
  --debug
```

This bypasses STEP1 listing expansion, visits each canonical hub URL **sequentially** in **one** Selenium session, upserts events/ticket types incrementally (same as normal discovery), and uses `selenium_slow_hydrate` only so the profile is not contested by a second Playwright launch.

## Failure reasons (taxonomy)

Each failed URL should map to a single reason string such as:

- `verification_blocked`
- `login_required`
- `profile_locked` (exit code **4** from the pipeline when the lock is held)
- `no_display` (Linux, missing `DISPLAY`)
- `no_ticket_urls_after_real_page`
- `extraction_error`
- `timeout`

Verification must **not** be mislabeled as “no tickets found”.

## Security policy (unchanged)

- No passwords in env vars beyond what Chrome itself needs.
- No automated login form filling.
- No exporting cookies outside the Chrome profile for “bypass” purposes.
- Only reuse a legitimate profile the operator logged into manually.
