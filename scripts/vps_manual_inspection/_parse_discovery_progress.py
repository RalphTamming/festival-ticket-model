#!/usr/bin/env python3
"""Parse week_production_test.nohup.log for discovery progress heuristics."""
from __future__ import annotations

import re
import sys
from datetime import datetime
from pathlib import Path


def main() -> int:
    path = Path(sys.argv[1] if len(sys.argv) > 1 else "logs/week_production_test.nohup.log")
    text = path.read_text(encoding="utf-8", errors="ignore")
    urls = re.findall(r"Current URL \(normalized\): (.+)", text)
    starts = re.findall(r"=== DISCOVERY START (.+) ===", text)
    ends = re.findall(r"=== DISCOVERY END (.+) ===", text)
    t0 = None
    t1 = None
    if starts:
        try:
            t0 = datetime.fromisoformat(starts[0].replace("Z", "+00:00"))
        except ValueError:
            t0 = None
    lines = text.splitlines()
    last_ts = None
    for ln in reversed(lines):
        m = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+", ln)
        if m:
            try:
                last_ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                last_ts = None
            break
    uniq = list(dict.fromkeys(urls))
    print(f"log_file: {path}")
    print(f"discovery_starts: {len(starts)}")
    print(f"discovery_ends: {len(ends)}")
    print(f"current_url_log_lines: {len(urls)}")
    print(f"unique_current_urls_seen: {len(uniq)}")
    if uniq:
        print("last_unique_urls:")
        for u in uniq[-6:]:
            print(f"  {u}")
    if t0 and last_ts:
        delta = (last_ts - t0.replace(tzinfo=None)).total_seconds()
        print(f"elapsed_log_seconds_approx: {int(delta)}")
        if len(uniq) >= 2 and delta > 5:
            rate = len(uniq) / delta
            print(f"unique_urls_per_second_approx: {rate:.4f}")
            remaining_cities = 15  # western_europe_festivals_verified
            events_per_city = 30
            total_slots = remaining_cities * events_per_city
            done = len(uniq)
            # Heuristic: unique URLs ~= events touched (not perfect; hubs may repeat scans)
            if rate > 0:
                eta_sec = max(0.0, (total_slots - done) / rate)
                print(f"ETA_heuristic_hours_if_linear: {eta_sec / 3600:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
