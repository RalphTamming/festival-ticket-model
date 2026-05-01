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

## Run discovery test

```bash
xvfb-run -a python run_pipeline.py --mode discovery --scope amsterdam_festivals --headed --limit-events 3
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
