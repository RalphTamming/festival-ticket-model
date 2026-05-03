# Useful VPS Commands

## SSH

```bash
ssh root@178.104.249.252
```

## Pull latest code

```bash
git pull
```

## Activate virtual environment

```bash
source .venv/bin/activate
```

## Distutils error quick fix (Python 3.12)

If you get `ModuleNotFoundError: No module named 'distutils'`:

```bash
source .venv/bin/activate
pip install setuptools
pip install -r requirements.txt
```

## Prime persistent profile (recommended on VPS)

```bash
xvfb-run -a python prime_ticketswap_session.py
```

Alternative when using a visible desktop/VNC:

```bash
python prime_ticketswap_session.py
```

Optional remote debugging tunnel:

```bash
ssh -L 9222:localhost:9222 root@178.104.249.252
```

Then open [http://localhost:9222](http://localhost:9222).

## Run discovery test

```bash
xvfb-run -a python run_pipeline.py --mode discovery --scope amsterdam_festivals --headed --limit-events 3 --vps-safe-mode --step2-retries 2
```

## Run focused STEP2 live loop test

```bash
xvfb-run -a bash scripts/test_step2_until_success.sh
```

## Run focused single-event STEP2 live test

```bash
xvfb-run -a python step2_vps_live_test.py \
  --event-url "https://www.ticketswap.com/festival-tickets/bondgenoten-festival-2026-amsterdam-lofi-2026-05-29-CZrBG4iowfg4JsxTeEx7j" \
  --browser selenium \
  --headed \
  --retries 3 \
  --verification-wait 120 \
  --wait-for-manual-verification \
  --debug
```

## Run discovery with explicit Selenium-first STEP2

```bash
xvfb-run -a python run_pipeline.py --mode discovery --scope amsterdam_festivals --headed --limit-events 3 --vps-safe-mode --step2-browser selenium --step2-retries 3 --step2-verification-wait 60 --wait-for-manual-verification --require-fresh-step2
```

## Run monitoring test

```bash
xvfb-run -a python run_pipeline.py --mode monitoring --headed --limit-tickets 5
```

## Run 1-week production test (detached)

```bash
nohup bash scripts/run_week_production_test.sh > logs/week_production_test.nohup.log 2>&1 &
```

## Check 1-week production test

```bash
tail -f logs/week_production_test.log
ps aux | grep run_week_production_test
du -h ticketswap.db
ls -lh data/exports data/outputs
```

## View logs

```bash
tail -f logs/monitoring.log
tail -f logs/discovery.log
```

Manual `run_pipeline.py` commands print directly to terminal output.
Script wrappers in `scripts/` append output to `logs/*.log`.

## Edit cron

```bash
crontab -e
```

## List cron

```bash
crontab -l
```

## Check disk

```bash
df -h
```

## Check DB

```bash
ls -lh ticketswap.db
```

## Download DB to local machine

```bash
scp root@178.104.249.252:/root/festival-ticket-model/ticketswap.db .
```
