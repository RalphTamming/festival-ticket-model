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

## Run discovery test

```bash
xvfb-run -a python run_pipeline.py --mode discovery --scope amsterdam_festivals --headed --limit-events 3 --vps-safe-mode --step2-retries 2
```

## Run monitoring test

```bash
xvfb-run -a python run_pipeline.py --mode monitoring --headed --limit-tickets 5
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
