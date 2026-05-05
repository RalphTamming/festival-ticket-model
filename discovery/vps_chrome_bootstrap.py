"""
Optional VPS hygiene before undetected_chromedriver starts Chrome.

Controlled by TICKETSWAP_VPS_CLEAN_SLATE (default: on for Linux headed_vps) and
TICKETSWAP_VPS_ENSURE_XVFB (optional: run scripts/vps_ensure_xvfb.sh before checks).
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
_CLEAN_SLATE_SH = _SCRIPTS / "vps_chrome_clean_slate.sh"
_ENSURE_XVFB_SH = _SCRIPTS / "vps_ensure_xvfb.sh"


def _truthy(val: str) -> bool:
    return val.strip().lower() in ("1", "true", "yes", "on")


def _falsy(val: str) -> bool:
    return val.strip().lower() in ("0", "false", "no", "off")


def clean_slate_enabled() -> bool:
    raw = str(os.getenv("TICKETSWAP_VPS_CLEAN_SLATE", "")).strip()
    if raw and _falsy(raw):
        return False
    if raw and _truthy(raw):
        return True
    if not sys.platform.startswith("linux"):
        return False
    mode = str(os.getenv("TICKETSWAP_BROWSER_MODE", "")).strip().lower()
    if mode in ("headed_vps", "headed-vps"):
        return True
    return False


def ensure_xvfb_enabled() -> bool:
    raw = str(os.getenv("TICKETSWAP_VPS_ENSURE_XVFB", "")).strip()
    if not raw:
        return False
    return _truthy(raw)


def run_clean_slate_if_enabled(*, logger: Optional[logging.Logger] = None) -> bool:
    log = logger or logging.getLogger("ticketswap.vps_bootstrap")
    if not clean_slate_enabled():
        log.debug("clean_slate skipped (TICKETSWAP_VPS_CLEAN_SLATE / platform / TICKETSWAP_BROWSER_MODE)")
        return False
    if not _CLEAN_SLATE_SH.is_file():
        log.warning("clean_slate script missing: %s", _CLEAN_SLATE_SH)
        return False
    log.warning("Running VPS Chrome clean-slate (%s)", _CLEAN_SLATE_SH)
    try:
        subprocess.run(
            ["bash", str(_CLEAN_SLATE_SH)],
            check=False,
            timeout=120,
            text=True,
        )
    except Exception as exc:
        log.warning("clean_slate script failed (continuing): %s", exc)
    return True


def run_ensure_xvfb_if_enabled(*, logger: Optional[logging.Logger] = None) -> None:
    log = logger or logging.getLogger("ticketswap.vps_bootstrap")
    if not ensure_xvfb_enabled():
        return
    if not _ENSURE_XVFB_SH.is_file():
        log.warning("ensure_xvfb script missing: %s", _ENSURE_XVFB_SH)
        return
    log.warning("Running VPS Xvfb ensure (%s)", _ENSURE_XVFB_SH)
    env = os.environ.copy()
    if not str(env.get("DISPLAY", "")).strip():
        env["DISPLAY"] = ":99"
    try:
        subprocess.run(
            ["bash", str(_ENSURE_XVFB_SH)],
            check=False,
            timeout=60,
            text=True,
            env=env,
        )
    except Exception as exc:
        log.warning("ensure_xvfb script failed (continuing): %s", exc)


def run_clean_slate_after_failed_chrome_startup(*, logger: Optional[logging.Logger] = None) -> None:
    """
    After a failed uc.Chrome() attempt, optionally kill stray processes so the next
    attempt does not inherit a wedged browser/driver.
    """
    if run_clean_slate_if_enabled(logger=logger):
        time.sleep(2.0)
