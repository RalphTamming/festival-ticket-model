"""
TicketSwap STEP2 headed VPS parity: non-headless Chrome, profile lock, health check.

No passwords or automated login — trust lives only in the Chrome user-data-dir.
"""

from __future__ import annotations

import contextlib
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Iterator, Optional
from urllib.parse import urlparse

import config
from discovery import discover_urls as du

LOGGER = logging.getLogger("ticketswap.vps_mode")

LOCK_FILENAME = ".step2_profile.lock"
DEBUG_REL = Path("tmp") / "ticketswap_debug"

HEADED_VPS_MODES = frozenset({"headed_vps", "headed-vps"})


class FailureReason(str, Enum):
    verification_blocked = "verification_blocked"
    login_required = "login_required"
    profile_locked = "profile_locked"
    no_display = "no_display"
    no_ticket_urls_after_real_page = "no_ticket_urls_after_real_page"
    extraction_error = "extraction_error"
    timeout = "timeout"


class ProfileHealthStatus(str, Enum):
    trusted = "trusted"
    verification = "verification"
    login_required = "login_required"
    blocked = "blocked"
    unknown = "unknown"


@dataclass
class ProfileHealthResult:
    status: ProfileHealthStatus
    detail: str
    current_url: str = ""
    title: str = ""


def browser_mode_from_env() -> str:
    return str(os.getenv("TICKETSWAP_BROWSER_MODE", "")).strip().lower()


def is_headed_vps_browser_mode(args: Any = None) -> bool:
    if args is not None and bool(getattr(args, "headed_vps", False)):
        return True
    return browser_mode_from_env() in HEADED_VPS_MODES


def is_non_interactive_vps() -> bool:
    if str(os.getenv("TICKETSWAP_NON_INTERACTIVE", "")).strip().lower() in ("1", "true", "yes", "on"):
        return True
    if str(os.getenv("CI", "")).strip().lower() in ("1", "true", "yes"):
        return True
    if not sys.stdin.isatty():
        return True
    return False


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except (OSError, ValueError):
        return False
    except Exception:
        # Windows can raise a SystemError here depending on the environment.
        return False
    return True


def _read_lock_pid(lock_path: Path) -> Optional[int]:
    try:
        raw = lock_path.read_text(encoding="utf-8").strip()
        return int(raw.split()[0])
    except Exception:
        return None


class ProfileLockError(RuntimeError):
    pass


@contextlib.contextmanager
def step2_profile_lock(profile_dir: Path, *, logger: Optional[logging.Logger] = None) -> Iterator[None]:
    """
    Exclusive lock file under the Chrome user-data-dir.
    Fails fast if another process holds the lock (live PID).
    """
    log = logger or LOGGER
    profile_dir = profile_dir.expanduser().resolve()
    profile_dir.mkdir(parents=True, exist_ok=True)
    lock_path = profile_dir / LOCK_FILENAME
    my_pid = os.getpid()

    def _try_acquire() -> bool:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            try:
                os.write(fd, f"{my_pid}\n".encode("utf-8"))
            finally:
                os.close(fd)
            return True
        except FileExistsError:
            return False

    acquired = False
    if not _try_acquire():
        other = _read_lock_pid(lock_path)
        if other is not None and _pid_alive(other):
            raise ProfileLockError(
                "Profile already in use. Stop the other STEP2/Chrome process or use a different profile."
            )
        with contextlib.suppress(Exception):
            lock_path.unlink()
        if not _try_acquire():
            raise ProfileLockError(
                "Profile already in use. Stop the other STEP2/Chrome process or use a different profile."
            )
    acquired = True
    log.debug("Acquired STEP2 profile lock %s", lock_path)

    def _release() -> None:
        nonlocal acquired
        if not acquired:
            return
        try:
            other = _read_lock_pid(lock_path)
            if other == my_pid and lock_path.exists():
                lock_path.unlink()
        except FileNotFoundError:
            pass
        except Exception as e:
            log.warning("Could not remove profile lock %s: %s", lock_path, e)
        acquired = False

    try:
        yield
    finally:
        _release()


def require_display_for_headed_vps() -> None:
    """Linux: headed Chrome needs DISPLAY (e.g. Xvfb :99)."""
    if not sys.platform.startswith("linux"):
        return
    disp = str(os.environ.get("DISPLAY", "")).strip()
    if not disp:
        raise SystemExit(
            "headed_vps on Linux requires DISPLAY (e.g. export DISPLAY=:99 after starting Xvfb). "
            "Reason: no_display"
        )


def require_profile_directory_exists(profile_dir: Path) -> Path:
    p = profile_dir.expanduser().resolve()
    if not p.exists():
        raise SystemExit(f"Profile directory does not exist: {p}")
    return p


def validate_headed_vps_prerequisites(*, profile_dir: Path, allow_anonymous: bool = False) -> None:
    if allow_anonymous or config.STEP2_USE_ANONYMOUS_PROFILE:
        raise SystemExit(
            "headed_vps requires a persistent TICKETSWAP_PROFILE_DIR (not --anonymous-profile). "
            "Reason: extraction_error"
        )
    p = profile_dir.expanduser().resolve()
    if not str(p):
        raise SystemExit(
            "headed_vps requires TICKETSWAP_PROFILE_DIR to point to the Chrome user-data-dir. "
            "Reason: extraction_error"
        )
    require_profile_directory_exists(p)
    require_display_for_headed_vps()
    env_h = str(os.getenv("TICKETSWAP_HEADLESS", "")).strip().lower()
    if env_h in ("1", "true", "yes", "on"):
        raise SystemExit(
            "headed_vps is incompatible with TICKETSWAP_HEADLESS=1. Set TICKETSWAP_HEADLESS=0. "
            "Reason: extraction_error"
        )


def apply_headed_vps_runtime_defaults() -> None:
    """Slow + interact; never anonymous; headed implied elsewhere."""
    config.STEP2_USE_ANONYMOUS_PROFILE = False
    config.STEP2_INTERACT_ENABLED = True
    du.apply_step2_slow_timings()
    setattr(config, "STEP2_DEBUG_DUMP_ON_FAILURE", True)


def classify_profile_health(
    driver: Any,
    *,
    html: str,
    current_url: str = "",
) -> ProfileHealthResult:
    title = ""
    vis = ""
    with contextlib.suppress(Exception):
        title = str(getattr(driver, "title", "") or "")
    with contextlib.suppress(Exception):
        vis = str(driver.execute_script("return document.body && document.body.innerText") or "")
    cur = current_url or str(getattr(driver, "current_url", "") or "")
    path = (urlparse(cur).path or "").lower()
    vis_l = vis.lower()

    if "/login" in path or re.search(r"\b(sign in|log in)\b", vis_l):
        return ProfileHealthResult(ProfileHealthStatus.login_required, "login or sign-in UI", cur, title)

    if du.is_blocked_for_discovery(html, title=title, visible_text=vis[:8000], current_url=cur):
        return ProfileHealthResult(ProfileHealthStatus.verification, "verification / bot interstitial", cur, title)

    if du.looks_like_verification(html) and not du.has_ticketswap_discovery_signal(html):
        return ProfileHealthResult(ProfileHealthStatus.blocked, "blocked shell / no discovery signals", cur, title)

    if du.has_ticketswap_discovery_signal(html) or (du.has_next_data_script(html) and len(vis) > 400):
        return ProfileHealthResult(ProfileHealthStatus.trusted, "app shell / discovery signals present", cur, title)

    return ProfileHealthResult(ProfileHealthStatus.unknown, "insufficient positive signals", cur, title)


def run_profile_health_probe(
    *,
    headed: bool = True,
    logger: Optional[logging.Logger] = None,
) -> ProfileHealthResult:
    """Open Chrome with configured profile, visit TicketSwap home, return PROFILE_HEALTH."""
    log = logger or LOGGER
    require_profile_directory_exists(config.ticketswap_profile_directory())
    require_display_for_headed_vps()
    driver = du.new_driver(headless=not headed)
    try:
        driver.set_page_load_timeout(90)
        driver.get("https://www.ticketswap.com/")
        time.sleep(2.0)
        html = du.wait_for_page_content(driver, headless=not headed)
        res = classify_profile_health(driver, html=html)
        cur_out = str(getattr(driver, "current_url", "") or res.current_url)
        res = ProfileHealthResult(res.status, res.detail, cur_out, res.title)
        log.warning(
            "PROFILE_HEALTH status=%s detail=%s url=%s",
            res.status.value,
            res.detail,
            cur_out,
        )
        if res.status == ProfileHealthStatus.trusted:
            log.info("PROFILE_HEALTH status=%s", res.status.value)
        return res
    finally:
        with contextlib.suppress(Exception):
            driver.quit()


def write_vps_failure_debug(
    *,
    slug: str,
    driver: Any,
    html: str,
    browser_mode: str,
    profile_health: str,
    failure_reason: str,
    root: Optional[Path] = None,
) -> Path:
    """Persist debug bundle for a failed festival/hub URL."""
    base = (root or Path.cwd()) / DEBUG_REL / slug
    base.mkdir(parents=True, exist_ok=True)
    (base / "browser_mode.txt").write_text(browser_mode + "\n", encoding="utf-8")
    (base / "profile_health.txt").write_text(profile_health + "\n", encoding="utf-8")
    (base / "failure_reason.txt").write_text(failure_reason + "\n", encoding="utf-8")
    (base / "page.html").write_text(html or "", encoding="utf-8")
    vis = ""
    with contextlib.suppress(Exception):
        vis = str(driver.execute_script("return document.body && document.body.innerText") or "")
    (base / "body.txt").write_text(vis or "", encoding="utf-8")
    hrefs: list[str] = []
    with contextlib.suppress(Exception):
        raw = driver.execute_script(
            r"""
            const out = [];
            try {
              document.querySelectorAll('a[href]').forEach(a => {
                try { if (a.href) out.push(String(a.href)); } catch (e) {}
              });
            } catch (e) {}
            return out.sort();
            """
        )
        if isinstance(raw, list):
            hrefs = [str(x) for x in raw if x]
    (base / "hrefs.txt").write_text("\n".join(hrefs), encoding="utf-8")
    with contextlib.suppress(Exception):
        driver.save_screenshot(str(base / "screenshot.png"))
    return base


def resolve_discovery_headed_with_vps(args: Any) -> bool:
    """
    headed_vps forces headed=True regardless of TICKETSWAP_HEADLESS unless env conflicts
    (validate_headed_vps_prerequisites catches TICKETSWAP_HEADLESS=1).
    """
    if is_headed_vps_browser_mode(args):
        return True
    return du.resolve_discovery_headed(args)


_profile_lock_stack: list[Any] = []


def acquire_step2_profile_lock(profile_dir: Path, *, logger: Optional[logging.Logger] = None) -> None:
    """Enter exclusive profile lock (pair with release_step2_profile_lock in finally)."""
    cm = step2_profile_lock(profile_dir, logger=logger)
    cm.__enter__()
    _profile_lock_stack.append(cm)


def release_step2_profile_lock() -> None:
    while _profile_lock_stack:
        cm = _profile_lock_stack.pop()
        with contextlib.suppress(Exception):
            cm.__exit__(None, None, None)
