#!/usr/bin/env python3
"""
Headed VPS STEP2 smoke: profile lock, health probe, then one shared Chrome session.

Stops after the first hub that returns fresh ticket URLs unless --all-18 is passed.
Does not handle credentials — use a manually trusted Chrome profile (TICKETSWAP_PROFILE_DIR).
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    p = argparse.ArgumentParser(description="Smoke STEP2 in headed_vps-style mode.")
    p.add_argument(
        "--all-18",
        action="store_true",
        help="After the smoke URL succeeds, scan all 18 canonical hub URLs in the same session.",
    )
    args = p.parse_args(argv)

    os.environ.setdefault("TICKETSWAP_BROWSER_MODE", "headed_vps")
    os.environ.setdefault("TICKETSWAP_HEADLESS", "0")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    import config
    from discovery import discover_urls as du
    from discovery import ticketswap_vps_mode as tvm
    from discovery.step2_discover_ticket_urls import discover_ticket_urls_from_event_selenium
    from discovery.vps_eighteen_targets import EIGHTEEN_FESTIVAL_URLS, SMOKE_FIRST_URL

    tvm.validate_headed_vps_prerequisites(profile_dir=config.ticketswap_profile_directory(), allow_anonymous=False)
    tvm.apply_headed_vps_runtime_defaults()

    out_jsonl = _REPO / "tmp" / "smoke_step2_vps.jsonl"
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)

    def append_line(obj: dict) -> None:
        with out_jsonl.open("a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False, default=str) + "\n")

    with tvm.step2_profile_lock(config.ticketswap_profile_directory(), logger=logging.getLogger("smoke_step2_vps")):
        hr = tvm.run_profile_health_probe(headed=True, logger=logging.getLogger("smoke_step2_vps"))
        setattr(config, "STEP2_LAST_PROFILE_HEALTH", hr.status.value)
        if hr.status != tvm.ProfileHealthStatus.trusted:
            append_line({"phase": "profile_health", "status": hr.status.value})
            print(f"SMOKE_FAIL PROFILE_HEALTH status={hr.status.value}", flush=True)
            return 2

        driver = du.new_driver(headless=False)
        try:
            urls: list[str]
            if args.all_18:
                urls = list(EIGHTEEN_FESTIVAL_URLS)
            else:
                # Try until we prove fresh extraction works at least once.
                urls = [SMOKE_FIRST_URL] + [u for u in EIGHTEEN_FESTIVAL_URLS if u != SMOKE_FIRST_URL]

            for idx, url in enumerate(urls):
                r = discover_ticket_urls_from_event_selenium(
                    url,
                    headed=True,
                    debug=True,
                    verification_wait_seconds=90,
                    wait_for_manual_verification=False,
                    manual_verification_press_enter=False,
                    debug_dump=True,
                    existing_driver=driver,
                )
                rec = {
                    "index": idx,
                    "url": url,
                    "step2_status": r.status,
                    "verification": r.verification,
                    "ticket_count": len(r.ticket_urls),
                    "failure_reason": r.failure_reason,
                    "result_status": getattr(r, "result_status", None),
                }
                append_line(rec)
                if r.status == "blocked" or r.verification:
                    print(f"SMOKE_BLOCKED: {rec}", flush=True)
                    if args.all_18:
                        return 4
                    # Non --all-18: keep trying until first success (or exhaustion).
                    continue
                if r.ticket_urls:
                    print(f"SMOKE_OK first success: {rec}", flush=True)
                    return 0
                # No tickets after hydration budget.
                print(f"SMOKE_NO_DATA: {rec}", flush=True)
                if args.all_18:
                    return 5
                continue
        finally:
            with contextlib.suppress(Exception):
                driver.quit()

    print("SMOKE_FAIL no hubs returned fresh URLs", flush=True)
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
