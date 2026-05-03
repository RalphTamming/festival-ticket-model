# VPS Manual Browser Inspection

This folder contains a small, safe toolkit for visually inspecting what
TicketSwap shows on the VPS. The goal is to determine whether the current
`verification_blocked` / `events_collected = 0` failures are caused by:

1. Old code on the VPS,
2. A missing/expired browser session (cookies),
3. The persistent Chrome profile not being reused,
4. The VPS IP / datacenter being on TicketSwap's distrust list,
5. Or a code bug.

Everything here uses **a normal headed Chrome** the way a human would. We do
**not** bypass CAPTCHA, verification, or any bot detection.

## Files

- `start_vnc.sh`   - start TigerVNC on display `:1`, **bound to localhost only**.
- `stop_vnc.sh`    - stop it again.
- `manual_chrome.sh` - launch Chrome on `DISPLAY=:1` with the SAME persistent profile
                       the pipeline uses, so you can log in / verify once and
                       the cookies stick.
- `rerun_discovery_headed.sh` - rerun the two diagnostic commands headed, on
                                the VNC display, so you can watch them.

All scripts are short and read-only with respect to the codebase. They only
write to `/tmp`, `/root/.vnc`, and the log directory.

## Step-by-step (do this from your Windows laptop)

### 1. Start VNC on the VPS (one-time per boot)
```powershell
ssh root@<VPS_IP> "bash /root/start_vnc.sh"
```
You should see a line like:
```
LISTEN 0 5  127.0.0.1:5901  ...
```
**Important**: it MUST say `127.0.0.1:5901`, not `0.0.0.0:5901`. We never
expose VNC to the public internet.

### 2. Open an SSH tunnel from your laptop
In a NEW PowerShell window (leave this open while inspecting):
```powershell
ssh -N -L 5901:localhost:5901 root@<VPS_IP>
```

`-N` means "don't run a shell, just tunnel". Keep this window open.

### 3. Connect with a VNC viewer

Install [TigerVNC Viewer](https://github.com/TigerVNC/tigervnc/releases) or
[RealVNC Viewer](https://www.realvnc.com/en/connect/download/viewer/) on your
laptop.

Connect to: `localhost:5901`
Password is whatever was set with `vncpasswd` on the VPS (see
`/root/.vnc/passwd`). The default seeded by setup is `TicketSwap-VNC-2026`
- you can change it any time with `ssh root@<VPS_IP> "vncpasswd"`.

You should now see an XFCE desktop.

### 4. Open Chrome manually on the VPS

Inside the VNC desktop, open a terminal (xfce4-terminal) and run:
```bash
bash /root/manual_chrome.sh
```

Chrome will open with the **same persistent profile** the pipeline uses
(`/root/festival-ticket-model/.ticketswap_browser_profile/Default`).

Now:

1. Browse to `https://www.ticketswap.com/festival-tickets?slug=festival-tickets&location=3`
   (the script opens this by default).
2. If you see a verification screen ("Verifying you are human", etc.), wait
   for it / click through it manually like a real user would.
3. Optionally log in to your TicketSwap account.
4. Visit a couple of festival pages (Amsterdam, then click an event).
5. Verify that ticket listings actually load with prices/links.
6. **Close the Chrome window** when done. This is critical: the pipeline
   cannot use the profile while another Chrome process holds it.

### 5. Re-run the diagnostic discovery

From your laptop (or inside the VNC terminal):
```powershell
ssh root@<VPS_IP> "bash /root/rerun_discovery_headed.sh"
```

This runs - watching it live in the VNC window if you stay connected:

```
python -m discovery.step1_collect_listing_urls \
  --url "https://www.ticketswap.com/festival-tickets?slug=festival-tickets&location=3" \
  --min-events 1 --max-show-more 2

python run_pipeline.py --mode discovery --scope amsterdam_festivals --headed \
  --limit-events 1 --vps-safe-mode --require-fresh-step2 \
  --suppress-per-event-step2-alerts \
  --step2-discovery-strategy shared_listing_click
```

Logs are saved under `/root/festival-ticket-model/logs/manual_*.log`.

### 6. Stop VNC when finished

```powershell
ssh root@<VPS_IP> "bash /root/stop_vnc.sh"
```

## Interpreting the result

After re-running discovery, look at the JSON/text output:

| Symptom                                                     | Most likely cause                       | Next action                                                                     |
| ----------------------------------------------------------- | --------------------------------------- | ------------------------------------------------------------------------------- |
| STEP1 still: `total_unique_events: 0`, page shows verifying | VPS IP/datacenter trust block           | Switch to **hybrid**: discover locally, run only monitoring on VPS              |
| STEP1: events found; STEP2: `verification_blocked`          | Per-event detection / bot-fingerprint   | Try `shared_listing_click` strategy (already on); slow down; consider hybrid    |
| STEP1: events found; STEP2: events found, `ticket_urls = 0` | Parser regression                       | Re-check `extract_ticket_urls_from_loaded_selenium_page` against debug HTML     |
| In VNC, manual Chrome ALSO shows verifying that won't pass  | VPS IP is hard-blocked                  | Use hybrid (local discovery + VPS monitoring with synced URLs)                  |
| In VNC, manual Chrome works fine, but pipeline still 0      | Profile not reused, or different flags  | Confirm `apply_persistent_chrome_profile` is called; diff Chrome args           |
| Code on VPS missing newest symbols                          | Repo not pulled                         | `git pull` on VPS (we already did this)                                         |

If after a manual login + verification in VNC the pipeline STILL returns
0 events, the conclusion is: **the VPS IP itself is the blocker**, and you
should switch to a hybrid architecture (local machine does discovery on a
schedule, syncs ticket URLs to the VPS via DB; VPS does monitoring only,
which doesn't require listing pages).
