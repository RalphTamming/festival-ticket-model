# VPS Setup (Hetzner Ubuntu)

This guide deploys the project on:
- Provider: Hetzner
- VPS: `178.104.249.252` (Ubuntu)
- Recommended path: `/root/festival-ticket-model`

Never store secrets in git. Put Telegram credentials in `.env` only.

## A) SSH into server

```bash
ssh root@178.104.249.252
```

## B) Update server

```bash
apt update && apt upgrade -y
```

## C) Install system packages

```bash
apt install -y \
  python3 python3-venv python3-pip \
  git curl wget unzip \
  xvfb ca-certificates gnupg \
  fonts-liberation libnss3 libatk-bridge2.0-0 libgtk-3-0 libxss1
```

For audio library compatibility (depends on Ubuntu release):

```bash
apt install -y libasound2 || apt install -y libasound2t64
```

## D) Install Google Chrome stable (modern keyring method)

```bash
install -d -m 0755 /etc/apt/keyrings
curl -fsSL https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /etc/apt/keyrings/google-chrome.gpg
chmod a+r /etc/apt/keyrings/google-chrome.gpg
echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list
apt update
apt install -y google-chrome-stable
```

## E) Clone repo

```bash
cd /root
git clone https://github.com/RalphTamming/festival-ticket-model.git
cd festival-ticket-model
```

## F) Create virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

If Playwright is used in your flow, also install browser deps:

```bash
python -m playwright install chromium
```

## G) Create `.env`

```bash
cp .env.example .env
nano .env
```

Fill in at least:

```dotenv
TELEGRAM_BOT_TOKEN=<your actual token>
TELEGRAM_CHAT_ID=<your chat id>
```

## H) Create folders

```bash
mkdir -p logs data/outputs data/debug data/backups
```

## I) Run smoke tests

Discovery:

```bash
xvfb-run -a python run_pipeline.py --mode discovery --scope amsterdam_festivals --headed --limit-events 3 --vps-safe-mode --step2-retries 2
```

Monitoring:

```bash
xvfb-run -a python run_pipeline.py --mode monitoring --headed --limit-tickets 5
```

## J) Verification-block handling / profile priming

If TicketSwap verification blocks more often on VPS, run profile priming:

```bash
python prime_ticketswap_session.py
```

or headful under virtual display:

```bash
xvfb-run -a python prime_ticketswap_session.py
```

Alternative without xvfb (for visible desktop/VNC sessions):

```bash
python prime_ticketswap_session.py
```

Notes:
- VPS traffic can trigger verification more often than a local laptop.
- Keep persistent profile enabled and use conservative limits.
- Monitoring runs hourly from cron, but Python still decides what is truly due.
- Manual commands print to terminal; `scripts/run_discovery.sh` and `scripts/run_monitoring.sh` write to log files.

## Troubleshooting

If you see:

`ModuleNotFoundError: No module named 'distutils'`

This can happen on Python 3.12+ when compatibility shims are missing. Run:

```bash
source .venv/bin/activate
pip install setuptools
pip install -r requirements.txt
```
