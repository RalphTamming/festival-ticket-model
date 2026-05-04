#!/usr/bin/env python3
"""
From week_production_test.nohup.log, infer targets where STEP2 logged
'Ticket URLs found (DOM): 0' for the final scan of that page (max over
repeated log lines for the same Current URL burst).
"""
from __future__ import annotations

import re
import sys
from pathlib import Path


def main() -> int:
    path = Path(sys.argv[1] if len(sys.argv) > 1 else "logs/week_production_test.nohup.log")
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()

    # Log order (gather_link_candidates_dom_first): DOM href, DOM state,
    # Ticket URLs found (DOM), Ticket URLs found (DOM state), Current URL.
    # Multiple triplets can repeat for the same page; take MAX dom-tickets
    # seen before each Current URL line, then MAX across all visits to that URL.
    per_url_max: dict[str, int] = {}

    url_re = re.compile(r"Current URL \(normalized\): (.+)")
    dom_re = re.compile(r"Ticket URLs found \(DOM\): (\d+)")

    pending_dom: int | None = None
    for ln in lines:
        dm = dom_re.search(ln)
        if dm:
            pending_dom = int(dm.group(1))
            continue
        um = url_re.search(ln)
        if um:
            url = um.group(1).strip()
            if pending_dom is not None:
                per_url_max[url] = max(per_url_max.get(url, 0), pending_dom)
            pending_dom = None

    no_fresh = sorted(u for u, mx in per_url_max.items() if mx == 0)
    print(f"log_file={path}")
    print(f"distinct_urls_with_dom_scan={len(per_url_max)}")
    print(f"no_fresh_ticket_urls_dom_max0={len(no_fresh)}")
    print("--- targets: DOM instrumentation saw 0 ticket-like hrefs (research first) ---")
    for u in no_fresh:
        print(u)

    dom_positive = sorted((u, mx) for u, mx in per_url_max.items() if mx > 0)
    print()
    print(
        f"--- targets: DOM saw ticket-like hrefs (max>0) but pipeline may still be no_data "
        f"(n={len(dom_positive)}; often hub / wrong-event filter) ---"
    )
    for u, mx in sorted(dom_positive, key=lambda x: (-x[1], x[0])):
        print(f"{mx}\t{u}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
