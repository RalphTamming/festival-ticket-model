"""
Discover and maintain TicketSwap ticket-type URLs in `ticketswap.db`.

This is intentionally conservative and pipeline-first:
- It always accepts deep ticket URLs as seeds (so the pipeline works even if festival discovery is blocked).
- It attempts best-effort discovery from festival/event pages by parsing hrefs and script JSON.
- It uses Selenium + undetected-chromedriver for reliability on TicketSwap.

Canonical TicketSwap festival URL shapes (example: Music On Festival):
- All festivals list: https://www.ticketswap.com/festival-tickets
- Series hub page:   https://www.ticketswap.com/festival-tickets/a/music-on-festival
- Ticket category:   https://www.ticketswap.com/festival-tickets/<event-slug>/<type>-tickets/<id>

CLI:
  python discover_urls.py
  python discover_urls.py --seed "<url>" --seed "<url2>"

Seeds may include a festivals overview URL (e.g. ``/festival-tickets?slug=festival-tickets&location=3``):
those are expanded via Amsterdam filter (when not already location=3) and repeated **Show more**,
then discovered ``/festival-tickets/a/<slug>`` hubs are scanned like explicit hub seeds.
"""

from __future__ import annotations

import argparse
import contextlib
import base64
import logging
import os
import random
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

import json
from urllib.parse import parse_qs, urljoin, urlparse, urlunparse

import config
import db as dbmod
from discovery import ticketswap_relaxed_extract as _tsx
from discovery import vps_chrome_bootstrap as _vcb

SUPPORTED_CATEGORY_PREFIXES: tuple[str, ...] = (
    "festival-tickets",
    "concert-tickets",
    "club-tickets",
    "sports-tickets",
)

_CAT_GROUP = "(?P<category>" + "|".join(re.escape(p) for p in SUPPORTED_CATEGORY_PREFIXES) + ")"

TICKET_URL_RE = re.compile(
    rf"^/(?P<category>{'|'.join(re.escape(p) for p in SUPPORTED_CATEGORY_PREFIXES)})/(?P<event_slug>[^/]+)/(?P<ticket_type_slug>[^/]+)/(?P<numeric_id>\d+)(?:[/?#].*)?$",
    re.IGNORECASE,
)

# Festival "hub page" is currently only /festival-tickets/a/<slug>
FESTIVAL_PAGE_RE = re.compile(r"^/festival-tickets/a/[^/?#]+(?:[/?#].*)?$", re.IGNORECASE)

EVENT_PAGE_RE = re.compile(
    rf"^/(?P<category>{'|'.join(re.escape(p) for p in SUPPORTED_CATEGORY_PREFIXES)})/(?!a/)(?![^/]+/[^/]+/\d+)[^/?#]+(?:[/?#].*)?$",
    re.IGNORECASE,
)

LISTING_PAGE_RE = re.compile(
    rf"^/(?P<category>{'|'.join(re.escape(p) for p in SUPPORTED_CATEGORY_PREFIXES)})/?$",
    re.IGNORECASE,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def safe_json(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)


# TicketSwap often uses /nl/, /en/, etc. before /festival-tickets/; discovery regexes expect no locale segment.
def _strip_ticketswap_locale_path(path: str) -> str:
    p = path or "/"
    m = re.match(r"^/([a-z]{2}|[a-z]{2}-[a-z]{2})(/festival-tickets(?:/.*)?)$", p, re.I)
    if m:
        return m.group(2)
    return p


def normalize_url(url: str, base: str = "https://www.ticketswap.com") -> Optional[str]:
    if not url:
        return None
    absolute = urljoin(base, url.strip())
    p = urlparse(absolute)
    if not p.netloc:
        return None
    if "ticketswap.com" not in p.netloc.lower():
        return None
    scheme = "https"
    netloc = "www.ticketswap.com"
    path = p.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    path = _strip_ticketswap_locale_path(path)
    return urlunparse((scheme, netloc, path, "", p.query or "", ""))


def is_ticket_url(url: str) -> bool:
    n = normalize_url(url) or url
    p = urlparse(n).path or ""
    if TICKET_URL_RE.match(p):
        return True
    return _tsx.path_matches_relaxed_festival_ticket(p)


def is_festival_page(url: str) -> bool:
    p = urlparse(url)
    return bool(FESTIVAL_PAGE_RE.match(p.path or ""))


def is_event_page(url: str) -> bool:
    p = urlparse(url)
    return bool(EVENT_PAGE_RE.match(p.path or ""))


def is_festival_overview_page(url: str) -> bool:
    """
    TicketSwap festivals directory: /festival-tickets with optional query (location, slug, …).
    Excludes series hubs (/festival-tickets/a/...) and single-event paths.
    """
    n = normalize_url(url)
    if not n:
        return False
    p = urlparse(n)
    path = (p.path or "/").rstrip("/") or "/"
    if path != "/festival-tickets":
        return False
    if is_festival_page(n) or is_ticket_url(n) or is_event_page(n):
        return False
    return True


def detect_category_prefix(url: str) -> Optional[str]:
    """Return category prefix like 'festival-tickets' or 'concert-tickets' from a TicketSwap URL."""
    n = normalize_url(url)
    if not n:
        return None
    path = urlparse(n).path or ""
    for p in SUPPORTED_CATEGORY_PREFIXES:
        if path.startswith(f"/{p}"):
            return p
    return None


def is_listing_page(url: str) -> bool:
    n = normalize_url(url)
    if not n:
        return False
    return bool(LISTING_PAGE_RE.match(urlparse(n).path or ""))


def is_plausible_event_page(url: str) -> bool:
    """
    TicketSwap pages sometimes leak truncated/placeholder slugs (e.g. /festival-tickets/fe, /festival-tickets/game)
    via DOM state. Those match EVENT_PAGE_RE but are not real event pages and can waste a lot of crawl budget.

    Heuristic: require an event-looking slug (contains a year or long hash-like suffix).
    """
    n = normalize_url(url)
    if not n:
        return False
    p = urlparse(n)
    path = p.path or ""
    if not is_event_page(n):
        return False
    slug = path.split("/festival-tickets/", 1)[-1].strip("/")
    if not slug or len(slug) < 12:
        return False
    if re.search(r"(19|20)\d{2}", slug):
        return True
    # Many TicketSwap event slugs end with a long code chunk like "-CUfJVG9ggm76WkYpo1Fqe"
    if re.search(r"-C[A-Za-z0-9]{12,}$", slug):
        return True
    # Fallback: at least multiple hyphenated tokens and some digits.
    if slug.count("-") >= 4 and re.search(r"\d", slug):
        return True
    return False


def event_url_from_ticket_url(ticket_url: str) -> Optional[str]:
    n = normalize_url(ticket_url)
    if not n:
        return None
    p = urlparse(n)
    path = p.path or ""
    m = TICKET_URL_RE.match(path)
    if m:
        event_slug = m.group("event_slug")
        category = m.group("category") if "category" in m.groupdict() else "festival-tickets"
        return normalize_url(f"/{category}/{event_slug}")
    eb = _tsx.event_base_path_from_relaxed_festival_ticket(path)
    if eb:
        return normalize_url(eb)
    return None


def ticket_type_from_ticket_url(ticket_url: str) -> tuple[Optional[str], Optional[str]]:
    n = normalize_url(ticket_url)
    if not n:
        return None, None
    p = urlparse(n)
    path = p.path or ""
    m = TICKET_URL_RE.match(path)
    if m:
        slug = m.group("ticket_type_slug")
        label = " ".join(w.capitalize() for w in slug.replace("-", " ").split()) if slug else None
        return slug or None, label or None
    if _tsx.path_matches_relaxed_festival_ticket(path):
        parts = [x for x in path.rstrip("/").split("/") if x]
        if len(parts) >= 4 and parts[-1].isdigit():
            slug = parts[-2]
        elif len(parts) == 3 and parts[-1].isdigit():
            slug = "tickets"
        else:
            slug = None
        label = " ".join(w.capitalize() for w in slug.replace("-", " ").split()) if slug else None
        return slug or None, label or None
    return None, None


LOGGER = logging.getLogger("ticketswap.discover")


def resolve_discovery_headed(args: Any) -> bool:
    """
    Default: headed (local STEP2). Use --headless to disable.
    TICKETSWAP_HEADLESS=1|true|yes forces headless when CLI does not pass --headed/--headless.

    TICKETSWAP_BROWSER_MODE=headed_vps (or args.headed_vps) always returns headed=True.
    """
    try:
        from discovery import ticketswap_vps_mode as tvm

        if tvm.is_headed_vps_browser_mode(args):
            return True
    except Exception:
        pass
    if bool(getattr(args, "headless", False)):
        return False
    if bool(getattr(args, "headed", False)):
        return True
    env_h = str(os.getenv("TICKETSWAP_HEADLESS", "")).strip().lower()
    if env_h in ("1", "true", "yes", "on"):
        return False
    return True


def log_step2_verification_blocked(logger: logging.Logger, *, url: str) -> None:
    logger.warning("STEP2_VERIFICATION_BLOCKED url=%s", url)


def pause_manual_verification_enter(*, logger: Optional[logging.Logger] = None) -> None:
    log = logger or LOGGER
    log.warning(
        "TicketSwap requires verification/login. Complete it in the opened browser, "
        "then press Enter to continue."
    )
    try:
        input()
    except EOFError:
        pass


def apply_step2_slow_timings() -> None:
    b = getattr(config, "_STEP2_TIMING_BACKUP", None)
    if not isinstance(b, dict):
        return
    if b:
        return
    b["PAGE_READY_TIMEOUT_SECONDS"] = float(config.PAGE_READY_TIMEOUT_SECONDS)
    b["PAGE_LOAD_SLEEP_SECONDS"] = float(config.PAGE_LOAD_SLEEP_SECONDS)
    config.PAGE_READY_TIMEOUT_SECONDS = float(config.STEP2_SLOW_PAGE_READY_SECONDS)
    config.PAGE_LOAD_SLEEP_SECONDS = float(config.STEP2_SLOW_PAGE_LOAD_SECONDS)
    config.STEP2_SLOW_MODE = True


def restore_step2_slow_timings() -> None:
    b = getattr(config, "_STEP2_TIMING_BACKUP", None)
    if not isinstance(b, dict) or not b:
        config.STEP2_SLOW_MODE = False
        return
    for k, v in b.items():
        setattr(config, k, v)
    b.clear()
    config.STEP2_SLOW_MODE = False


def _step2_interaction_sleep() -> float:
    base = float(getattr(config, "STEP2_INTERACTION_WAIT_SECONDS", 2.5))
    if getattr(config, "STEP2_SLOW_MODE", False):
        return base + random.uniform(0.4, 1.2)
    return max(0.35, min(base, 1.0))


def step2_interact_once(driver) -> None:
    """Gradual scroll + EN/NL/FR load-more labels + aria-expanded=false (best-effort)."""
    try:
        for frac in (0.2, 0.45, 0.7, 1.0, 1.0):
            with contextlib.suppress(Exception):
                driver.execute_script(
                    "window.scrollTo(0, Math.floor((document.body && document.body.scrollHeight || 0) * arguments[0]));",
                    frac,
                )
            time.sleep(_step2_interaction_sleep() * 0.35)
    except Exception:
        LOGGER.debug("step2_interact_once scroll failed", exc_info=False)
    with contextlib.suppress(Exception):
        driver.execute_script(
            r"""
            function norm(t) { return String(t || '').toLowerCase().replace(/\s+/g, ' ').trim(); }
            const needles = [
              'show more', 'load more', 'more',
              'meer', 'toon meer', 'laad meer', 'meer tonen',
              'voir plus', 'afficher plus', 'charger plus'
            ];
            const root = document.querySelector('main') || document.body;
            const els = Array.from(root.querySelectorAll('button, a, [role="button"]'));
            let clicks = 0;
            for (const el of els) {
              if (clicks >= 14) break;
              const t = norm(el.textContent || el.innerText || '');
              if (!t) continue;
              if (!needles.some(n => t.includes(n))) continue;
              try {
                el.scrollIntoView({block:'center', inline:'nearest'});
                el.click();
                clicks++;
              } catch (e) {}
            }
            const closed = Array.from(root.querySelectorAll('[aria-expanded="false"]')).slice(0, 14);
            for (const el of closed) {
              if (clicks >= 22) break;
              try {
                el.scrollIntoView({block:'center', inline:'nearest'});
                el.click();
                clicks++;
              } catch (e) {}
            }
            return clicks;
            """
        )
    time.sleep(_step2_interaction_sleep())


def run_step2_interaction_rounds(driver, *, max_rounds: Optional[int] = None) -> None:
    if not getattr(config, "STEP2_INTERACT_ENABLED", False):
        return
    n = int(max_rounds if max_rounds is not None else getattr(config, "STEP2_INTERACT_ROUNDS", 3))
    for _ in range(max(0, n)):
        step2_interact_once(driver)
        scroll_for_lazy_content(driver)


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Keep Selenium/urllib3 noise down even in verbose mode; we care about our own logs.
    for name in ("selenium", "urllib3", "undetected_chromedriver"):
        logging.getLogger(name).setLevel(logging.WARNING)


def new_driver(
    *,
    headless: bool,
    extra_args: Optional[Sequence[str]] = None,
    user_data_dir: Optional[str] = None,
    use_persistent_profile: Optional[bool] = None,
) -> Any:
    def _driver_impl() -> str:
        return str(os.getenv("TICKETSWAP_DRIVER_IMPL", "uc") or "uc").strip().lower()

    def _resolved_user_data_dir(*, anon: bool, use_prof: bool) -> Optional[str]:
        raw_override = getattr(config, "STEP2_DRIVER_USER_DATA_DIR", None)
        override_udd = (
            str(Path(str(raw_override).strip()).resolve())
            if (raw_override and str(raw_override).strip())
            else None
        )
        if anon:
            return None
        if user_data_dir:
            return str(Path(user_data_dir).resolve())
        if override_udd:
            return override_udd
        if use_prof:
            return config.persistent_browser_user_data_dir()
        return None

    def _use_persistent_profile(*, anon: bool) -> bool:
        if anon:
            return False
        if use_persistent_profile is not None:
            return bool(use_persistent_profile)
        return bool(getattr(config, "USE_PERSISTENT_BROWSER_PROFILE", True))

    def _common_options(options: Any, *, use_prof: bool) -> None:
        if use_prof:
            config.apply_persistent_chrome_profile(options)
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-infobars")
        options.add_argument("--no-first-run")
        options.add_argument("--no-default-browser-check")
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        if sys.platform.startswith("linux"):
            try:
                if os.geteuid() == 0:
                    options.add_argument("--no-sandbox")
                    options.add_argument("--disable-setuid-sandbox")
                    options.add_argument("--disable-dev-shm-usage")
            except AttributeError:
                pass
        for a in (extra_args or []):
            if a:
                options.add_argument(str(a))

    impl = _driver_impl()
    log = logging.getLogger("ticketswap.chrome")

    anon = bool(getattr(config, "STEP2_USE_ANONYMOUS_PROFILE", False))
    use_prof = _use_persistent_profile(anon=anon)
    resolved_udd = _resolved_user_data_dir(anon=anon, use_prof=use_prof)

    if impl == "selenium":
        log.warning("[Chrome] using selenium webdriver implementation")
        from selenium import webdriver  # local import: allow VPS to bypass UC entirely
        from selenium.webdriver.chrome.options import Options

        options = Options()
        _common_options(options, use_prof=use_prof)
        # Some VPS images install Chrome under /opt/... while Selenium defaults to /usr/bin/google-chrome.
        # Allow explicit override and prefer the real chrome binary when present.
        override_bin = str(os.getenv("TICKETSWAP_CHROME_BINARY", "") or "").strip()
        if override_bin:
            options.binary_location = override_bin
        else:
            opt_bin = Path("/opt/google/chrome/chrome")
            if opt_bin.is_file():
                options.binary_location = str(opt_bin)
        if resolved_udd:
            options.add_argument(f"--user-data-dir={resolved_udd}")
        # Selenium 4.6+ uses Selenium Manager when no driver path is specified.
        return webdriver.Chrome(options=options)

    # Default: undetected-chromedriver
    from undetected_chromedriver import Chrome as _UcChrome  # type: ignore
    from undetected_chromedriver import ChromeOptions as _UcChromeOptions  # type: ignore

    def _build_kw_uc() -> dict:
        options = _UcChromeOptions()
        anon = bool(getattr(config, "STEP2_USE_ANONYMOUS_PROFILE", False))
        use_prof = _use_persistent_profile(anon=anon)
        _common_options(options, use_prof=use_prof)
        kw: dict = dict(options=options, headless=headless, use_subprocess=True)
        if resolved_udd is not None:
            kw["user_data_dir"] = resolved_udd
        if config.CHROME_VERSION_MAIN is not None:
            kw["version_main"] = config.CHROME_VERSION_MAIN
        return kw

    # undetected-chromedriver can race when patching the driver binary or when stale Chrome
    # processes hold the profile / ports on VPS. Retries + optional clean-slate improve reliability.
    last_err: Optional[Exception] = None
    max_attempts = 5
    for attempt in range(max_attempts):
        log.warning(
            "[Chrome] uc.Chrome startup attempt %s/%s headless=%s",
            attempt + 1,
            max_attempts,
            headless,
        )
        try:
            return _UcChrome(**_build_kw_uc())
        except FileExistsError as e:
            last_err = e
            _vcb.run_clean_slate_after_failed_chrome_startup(logger=log)
            time.sleep(0.8)
        except Exception as e:
            # Retry only for known flaky startup conditions.
            msg = str(e).lower()
            if "session not created" in msg or "chrome not reachable" in msg:
                last_err = e
                _vcb.run_clean_slate_after_failed_chrome_startup(logger=log)
                time.sleep(1.2)
                continue
            if (
                "remote end closed connection without response" in msg
                or "connection aborted" in msg
                or "connection refused" in msg
                or "remotedisconnected" in msg.replace(" ", "")
            ):
                last_err = e
                _vcb.run_clean_slate_after_failed_chrome_startup(logger=log)
                time.sleep(1.5)
                continue
            raise
    assert last_err is not None
    raise last_err


def looks_like_verification(html: str) -> bool:
    h = (html or "").lower()
    return (
        "<title>verifying</title>" in h
        or "unable to verify" in h
        or 'meta name="ts-cv"' in h
    )


def looks_like_verification_html(
    html: str,
    *,
    current_url: str = "",
    title: str = "",
    visible_text: str = "",
) -> bool:
    """
    Broader TicketSwap / WAF / bot interstitial detection than ``looks_like_verification`` alone.
    Use together with ``has_ticketswap_discovery_signal`` to decide if discovery is blocked.
    """
    if looks_like_verification(html):
        return True
    tl = (title or "").lower()
    if any(
        x in tl
        for x in (
            "verifying",
            "just a moment",
            "attention required",
            "access denied",
            "forbidden",
            "robot check",
        )
    ):
        return True
    h = (html or "").lower()
    vi = (visible_text or "").lower()
    if any(
        x in vi
        for x in (
            "verify you are human",
            "checking your browser",
            "unusual traffic",
            "automated access",
            "enable javascript",
            "access denied",
        )
    ):
        return True
    if "cf-browser-verification" in h or "challenges.cloudflare.com" in h:
        return True
    if "captcha" in h and "ticketswap" in h and len(html or "") < 25_000:
        return True
    cur = (current_url or "").lower()
    if "interstitial" in cur or "/challenge" in cur:
        return True
    return False


def has_next_data_script(html: str) -> bool:
    return bool(html) and "__NEXT_DATA__" in html


def log_verification_blocked(
    logger: logging.Logger,
    *,
    url: str,
    title: str,
    current_url: str,
) -> None:
    logger.warning(
        "VERIFICATION_BLOCKED url=%s title=%r current_url=%r",
        url,
        (title or "")[:200],
        (current_url or "")[:500],
    )


def _deep_ticket_path_pattern() -> str:
    """Regex fragment: /(category)/…/…/digits for any supported category."""
    cats = "|".join(re.escape(p) for p in SUPPORTED_CATEGORY_PREFIXES)
    return rf"/(?:{cats})/[^\"'\s<>/]+/[^\"'\s<>/]+/\d{{5,}}"


def has_ticketswap_discovery_signal(html: str) -> bool:
    if not html:
        return False
    if "/festival-tickets/a/" in html:
        return True
    if re.search(_deep_ticket_path_pattern(), html, flags=re.I):
        return True
    if "styles_link__Jm_hk" in html:
        return True
    return False


def is_blocked_for_discovery(
    html: str,
    *,
    title: str = "",
    visible_text: str = "",
    current_url: str = "",
) -> bool:
    """True when a verification interstitial is shown and HTML lacks usable TicketSwap link signals."""
    if not (
        looks_like_verification(html)
        or looks_like_verification_html(
            html,
            current_url=current_url,
            title=title,
            visible_text=visible_text,
        )
    ):
        return False
    return not has_ticketswap_discovery_signal(html)


def hub_slug_from_festival_hub_url(url: str) -> str:
    n = normalize_url(url)
    if not n:
        return ""
    m = re.match(r"^/festival-tickets/a/([^/?#]+)", urlparse(n).path or "", re.I)
    return m.group(1) if m else ""


def scroll_for_lazy_content(driver) -> None:
    """Scroll so TicketSwap can hydrate event lists / ticket rows (infinite scroll, accordions)."""
    try:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.85)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.25)
        # TicketSwap often uses internal scroll containers; scroll them too.
        driver.execute_script(
            """
            try {
              document.querySelectorAll('div').forEach(el => {
                try {
                  if (el && el.scrollHeight && el.clientHeight && el.scrollHeight > el.clientHeight + 5) {
                    el.scrollTop = el.scrollHeight;
                  }
                } catch (e) {}
              });
            } catch (e) {}
            """
        )
        time.sleep(0.8)
        if getattr(config, "STEP2_SLOW_MODE", False):
            time.sleep(random.uniform(2.0, 4.0))
        elif getattr(config, "STEP2_INTERACT_ENABLED", False):
            time.sleep(_step2_interaction_sleep() * 0.45)
    except Exception:
        LOGGER.debug("scroll_for_lazy_content failed", exc_info=False)


def expand_main_accordions(driver, *, max_clicks: int = 24) -> None:
    """Best-effort: expand collapsed ticket-type rows on event pages (e.g. Single-Day / Weekend)."""
    try:
        from selenium.webdriver.common.by import By
    except ImportError:
        return
    selectors = (
        "main [aria-expanded='false']",
        "[role='main'] [aria-expanded='false']",
        "main button[aria-expanded='false']",
        # Radix UI accordion commonly uses data-state="closed" on triggers.
        "main [data-state='closed']",
        "[role='main'] [data-state='closed']",
        "main button[data-state='closed']",
        "main button[aria-controls][data-state='closed']",
    )
    for _ in range(max_clicks):
        clicked = False
        for sel in selectors:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
            except Exception:
                els = []
            if not els:
                continue
            try:
                driver.execute_script("arguments[0].click();", els[0])
                clicked = True
                break
            except Exception:
                continue
        if not clicked:
            break
        if getattr(config, "STEP2_SLOW_MODE", False):
            time.sleep(_step2_interaction_sleep())
        else:
            time.sleep(0.35)


def try_click_tickets_tab(driver) -> bool:
    """
    Some event pages hide the ticket category list behind a "Tickets" tab/section.
    Best-effort click to bring that section into view before scanning for deep links.
    """
    try:
        prev = len(extract_hrefs_from_dom_js(driver))
        clicked = bool(
            driver.execute_script(
                r"""
                const needles = ['tickets'];
                const norm = (s) => String(s || '').toLowerCase().replace(/\s+/g, ' ').trim();
                const score = (el) => {
                  const t = norm(el.textContent || el.innerText || '');
                  if (!t) return 0;
                  if (t === 'tickets') return 5;
                  if (t.includes('tickets')) return 2;
                  return 0;
                };
                const candidates = [];
                const root = document.querySelector('main') || document.body || document;
                const sel = 'a,button,[role="tab"],[role="button"]';
                const stripOneLocale = (pathname) => {
                  let p = String(pathname || '/').replace(/\\/+$/, '') || '/';
                  const m = p.match(/^\\/([a-z]{2}|[a-z]{2}-[a-z]{2})(\\/.*)$/i);
                  if (m) return m[2] || '/';
                  return p;
                };
                const isSitewideTicketsNav = (href) => {
                  if (!href) return false;
                  try {
                    const u = new URL(href, 'https://www.ticketswap.com');
                    if (!u.hostname.toLowerCase().includes('ticketswap')) return false;
                    const p = stripOneLocale(u.pathname);
                    return p === '/tickets';
                  } catch (e) { return false; }
                };
                root.querySelectorAll(sel).forEach(el => {
                  try {
                    // Avoid global "Tickets" → /tickets, sell/login, and other nav churn.
                    const href = (el.tagName === 'A' && el.href) ? String(el.href) : '';
                    if (href.includes('/sell') || href.includes('/login')) return;
                    if (isSitewideTicketsNav(href)) return;
                    const s = score(el);
                    if (s > 0) candidates.push([s, el]);
                  } catch (e) {}
                });
                candidates.sort((a,b) => b[0]-a[0]);
                for (const [s, el] of candidates.slice(0, 8)) {
                  try {
                    el.scrollIntoView({block:'center'});
                    el.click();
                    return true;
                  } catch (e) {}
                }
                return false;
                """
            )
            or False
        )
        if clicked:
            wait_for_dom_change(driver, prev, timeout=3.0)
            time.sleep(0.25)
        return clicked
    except Exception:
        return False


def extract_hrefs_from_dom_js(driver) -> set[str]:
    """TicketSwap category anchors via JS (sometimes differs from Selenium get_attribute after hydration)."""
    out: set[str] = set()
    try:
        anchor_sel = ",".join(f'a[href*="{p}"]' for p in SUPPORTED_CATEGORY_PREFIXES)
        hrefs = driver.execute_script(
            """
            const anchorSel = arguments[0];
            const found = new Set();
            const enqueueRoots = (root, q) => { if (root && !q.includes(root)) q.push(root); };
            const q = [];
            enqueueRoots(document, q);
            while (q.length) {
              const root = q.shift();
              try {
                const scope = root.querySelectorAll ? root : (root.documentElement || null);
                if (!scope) continue;
                scope.querySelectorAll(anchorSel).forEach(a => {
                  try {
                    const h = a.href || a.getAttribute('href');
                    if (h) found.add(String(h));
                  } catch (e) {}
                });
                scope.querySelectorAll('*').forEach(el => {
                  try {
                    if (el && el.shadowRoot) enqueueRoots(el.shadowRoot, q);
                  } catch (e) {}
                });
              } catch (e) {}
            }
            return Array.from(found);
            """,
            anchor_sel,
        )
        if not hrefs:
            return out
        for h in hrefs:
            if not h:
                continue
            n = normalize_url(h)
            if n:
                out.add(n)
    except Exception as e:
        LOGGER.debug("extract_hrefs_from_dom_js: %s", e)
    return out


def extract_ticket_urls_from_dom_state_js(driver) -> set[str]:
    """
    Extract deep TicketSwap ticket URLs from live DOM state, not just href attributes.

    Inspects href / data-* / onclick and attribute blobs for any supported ``*-tickets`` category path.
    """
    out: set[str] = set()
    try:
        prefixes = list(SUPPORTED_CATEGORY_PREFIXES)
        attr_sel = ",".join(
            f'[href*="{p}"],[data-href*="{p}"],[data-url*="{p}"],[data-to*="{p}"]'
            for p in SUPPORTED_CATEGORY_PREFIXES
        )
        vals = driver.execute_script(
            """
            const PREFIXES = arguments[0];
            const ATTR_SEL = arguments[1];
            const pathHit = (s) => {
              if (!s) return false;
              const t = String(s);
              return PREFIXES.some(p => t.includes('/' + p + '/'));
            };
            const out = new Set();
            const q = [];
            const enqueueRoots = (root) => { if (root) q.push(root); };
            enqueueRoots(document);

            const picks = (el) => {
              if (!el) return;
              try {
                const attrs = ['href','data-href','data-url','data-to','data-link','onclick'];
                attrs.forEach(k => {
                  try {
                    const v = (k === 'href' && el.href) ? el.href : (el.getAttribute && el.getAttribute(k));
                    if (v && pathHit(v)) out.add(String(v));
                  } catch (e) {}
                });
                try {
                  if (el.dataset) {
                    Object.keys(el.dataset).forEach(k => {
                      const v = el.dataset[k];
                      if (v && pathHit(v)) out.add(String(v));
                    });
                  }
                } catch (e) {}
                try {
                  if (el.attributes) {
                    for (const a of el.attributes) {
                      if (!a) continue;
                      const v = a.value;
                      if (v && pathHit(v)) out.add(String(v));
                    }
                  }
                } catch (e) {}
                try {
                  const html = el.outerHTML ? String(el.outerHTML) : '';
                  if (!html || !pathHit(html)) return;
                  const re = new RegExp('/(?:' + PREFIXES.join('|') + ')/[^"\\' + "'" + '\\s<>]+', 'gi');
                  let m;
                  let n = 0;
                  while ((m = re.exec(html)) !== null) {
                    out.add(m[0]);
                    n += 1;
                    if (n > 40) break;
                  }
                } catch (e) {}
              } catch (e) {}
            };

            const scanRoot = (root) => {
              try {
                const scope =
                  (root.querySelector ? root : null) ||
                  (root.documentElement ? root.documentElement : null) ||
                  null;
                if (!scope) return;
                const main = scope.querySelector('main') || scope.querySelector('[role="main"]') || scope;
                const sel = [
                  'a', 'button', '[role="button"]', '[role="link"]',
                  '[data-href]', '[data-url]', '[data-to]', '[data-link]',
                  '[onclick]'
                ].join(',');
                main.querySelectorAll(sel).forEach(picks);
                main.querySelectorAll(ATTR_SEL).forEach(picks);
                main.querySelectorAll('*').forEach(el => {
                  try { if (el && el.shadowRoot) enqueueRoots(el.shadowRoot); } catch (e) {}
                });
              } catch (e) {}
            };

            while (q.length) {
              const root = q.shift();
              scanRoot(root);
            }
            return Array.from(out);
            """,
            prefixes,
            attr_sel,
        )
        if not vals:
            return out
        for v in vals:
            if not v:
                continue
            n = normalize_url(str(v))
            if n and (is_ticket_url(n) or is_event_page(n) or is_festival_page(n)):
                out.add(n)
    except Exception as e:
        LOGGER.debug("extract_ticket_urls_from_dom_state_js: %s", e)
    return out


def wait_for_dom_change(driver, prev_count: int, *, timeout: float = 5.0) -> bool:
    """Poll until TicketSwap hydrates more category listing anchors."""
    start = time.time()
    prefs = list(SUPPORTED_CATEGORY_PREFIXES)
    while time.time() - start < float(timeout):
        try:
            count = int(
                driver.execute_script(
                    """
                    const prefs = arguments[0];
                    return prefs.reduce((acc, p) => acc + document.querySelectorAll('a[href*="' + p + '"]').length, 0);
                    """,
                    prefs,
                )
                or 0
            )
        except Exception:
            count = 0
        if count > int(prev_count or 0):
            return True
        time.sleep(0.3)
    return False


def gather_link_candidates_dom_first(driver, html: str, base_url: str) -> set[str]:
    """
    DOM-FIRST extraction (critical for TicketSwap): live DOM anchors first,
    then fall back to HTML href parsing + __NEXT_DATA + deep URL regexes.
    """
    dom_links = extract_hrefs_from_dom_js(driver)
    dom_state_links = extract_ticket_urls_from_dom_state_js(driver)
    html_links = extract_candidate_urls_from_html(html, base_url=base_url)
    json_links = extract_next_data_link_candidates(html, base_url=base_url)
    deep_text = extract_ticket_urls_from_page_text(html, base_url=base_url)
    hub_html = extract_festival_hub_urls_from_html(html, base_url=base_url)
    candidates: set[str] = set()
    candidates |= dom_links
    candidates |= dom_state_links
    candidates |= html_links
    candidates |= json_links
    candidates |= deep_text
    candidates |= hub_html
    candidates |= _tsx.extract_relaxed_festival_ticket_urls_from_html(html, base_url=base_url)
    cur = ""
    try:
        cur = normalize_url(getattr(driver, "current_url", "") or "") or (getattr(driver, "current_url", "") or "")
    except Exception:
        cur = ""
    LOGGER.info("DOM href count: %s", len(dom_links))
    LOGGER.info("DOM state links: %s", len(dom_state_links))
    LOGGER.info("Ticket URLs found (DOM): %s", sum(1 for u in dom_links if is_ticket_url(u)))
    LOGGER.info("Ticket URLs found (DOM state): %s", sum(1 for u in dom_state_links if is_ticket_url(u)))
    LOGGER.info("Current URL (normalized): %s", cur)
    LOGGER.debug("DOM href sample: %s", sorted(list(dom_links))[:12])
    LOGGER.debug("DOM state sample: %s", sorted(list(dom_state_links))[:12])
    return candidates


def _click_one_collapsed_in_main(driver) -> bool:
    from selenium.webdriver.common.by import By

    for sel in (
        "main [aria-expanded='false']",
        "[role='main'] [aria-expanded='false']",
        "main button[aria-expanded='false']",
        "main [data-state='closed']",
        "[role='main'] [data-state='closed']",
        "main button[data-state='closed']",
        "main button[aria-controls][data-state='closed']",
    ):
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            if els:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", els[0])
                time.sleep(0.12)
                driver.execute_script("arguments[0].click();", els[0])
                return True
        except Exception:
            continue
    return False


def click_ticket_type_rows_capture_urls(driver, base_url: str, *, max_clicks: int = 16) -> set[str]:
    """
    Some event pages don't expose deep ticket URLs as hrefs until you click rows.
    This tries a handful of clicks on main-area buttons and captures:
    - navigation to a deep ticket URL (driver.current_url)
    - new hrefs that appear after click
    """
    out: set[str] = set()
    try:
        from selenium.webdriver.common.by import By
    except ImportError:
        return out

    base_n = normalize_url(base_url) or base_url
    candidates = []
    for sel in (
        "main button",
        "[role='main'] button",
        "main [role='button']",
        "[role='main'] [role='button']",
        # Radix accordion triggers
        "main [data-state='closed']",
        "[role='main'] [data-state='closed']",
    ):
        try:
            candidates.extend(driver.find_elements(By.CSS_SELECTOR, sel))
        except Exception:
            continue
    seen_ids: set[int] = set()
    clickable = []
    for el in candidates:
        if id(el) in seen_ids:
            continue
        seen_ids.add(id(el))
        try:
            if not el.is_displayed():
                continue
        except Exception:
            continue
        # Avoid clicking generic navigation (Sell/Login/etc.) which can trap the driver on auth pages.
        try:
            href = (el.get_attribute("href") or "").lower()
        except Exception:
            href = ""
        try:
            txt = (el.text or "").strip().lower()
        except Exception:
            txt = ""
        if any(bad in href for bad in ("/sell", "/login", "redirectto=")):
            continue
        if txt in ("sell", "sell tickets") or "sell" in txt or "log in" in txt or "login" in txt:
            continue
        clickable.append(el)

    clicks = 0
    for el in clickable[: max_clicks * 3]:
        if clicks >= max_clicks:
            break
        try:
            prev_count = len(extract_hrefs_from_dom_js(driver))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.12)
            driver.execute_script("arguments[0].click();", el)
            wait_for_dom_change(driver, prev_count)
            clicks += 1
        except Exception:
            continue

        time.sleep(0.55)
        # Capture any newly materialized links in live DOM immediately after click.
        out |= extract_hrefs_from_dom_js(driver)
        out |= extract_ticket_urls_from_dom_state_js(driver)

        cur = normalize_url(getattr(driver, "current_url", "") or "") or ""
        if cur and ("/login" in cur or "/sell" in cur):
            # Bail out of auth flows quickly; go back and keep trying other elements.
            try:
                driver.back()
                time.sleep(0.8)
            except Exception:
                pass
            continue
        if cur and is_ticket_url(cur):
            out.add(cur)
        # capture any newly materialized links (fallbacks)
        html = driver.page_source or ""
        out |= gather_link_candidates_dom_first(driver, html, base_url)

        # If we navigated away from the event page, return so we can keep exploring.
        if cur and base_n and cur != base_n and not cur.startswith(base_n):
            try:
                driver.back()
                time.sleep(0.8)
            except Exception:
                pass

    return {u for u in out if is_ticket_url(u) or is_event_page(u) or is_festival_page(u)}


def reveal_event_page_deep_links(driver, base_url: str) -> set[str]:
    """
    Event pages (e.g. Dominator) often put deep ticket URLs only inside collapsed rows.
    Repeatedly expand + re-scan until few new links appear.
    """
    all_c: set[str] = set()
    prev_ticket_n = -1
    stagnant = 0
    # Try to bring the ticket category section into view early.
    try_click_tickets_tab(driver)
    scroll_for_lazy_content(driver)
    expand_main_accordions(driver)
    for i in range(30):
        html = driver.page_source or ""
        batch = gather_link_candidates_dom_first(driver, html, base_url)
        all_c |= batch
        ticket_n = sum(1 for u in all_c if is_ticket_url(u))
        if ticket_n > prev_ticket_n:
            prev_ticket_n = ticket_n
            stagnant = 0
        else:
            stagnant += 1
        if ticket_n >= 1 and stagnant >= 4:
            break
        # If we still have 0 deep URLs, try clicking ticket-type rows (Dominator-style UI).
        if ticket_n == 0 and stagnant in (4, 7, 10):
            all_c |= click_ticket_type_rows_capture_urls(driver, base_url, max_clicks=10)
            ticket_n2 = sum(1 for u in all_c if is_ticket_url(u))
            if ticket_n2 > ticket_n:
                prev_ticket_n = ticket_n2
                stagnant = 0
                ticket_n = ticket_n2

        if i >= 14 and ticket_n == 0 and stagnant >= 10:
            break
        if not _click_one_collapsed_in_main(driver):
            scroll_for_lazy_content(driver)
            stagnant += 1
            if stagnant >= 12:
                break
        time.sleep(0.42)
    return all_c


def gather_hub_page_candidates(driver, hub_url: str) -> set[str]:
    """
    Festival hub pages are not event pages. They can contain many event links but often *no* deep ticket URLs.
    We keep this fast to avoid spending ~minutes in the event-page expansion loop on a hub.
    """
    scroll_for_lazy_content(driver)
    expand_main_accordions(driver, max_clicks=10)
    html = driver.page_source or ""
    out = gather_link_candidates_dom_first(driver, html, hub_url)
    # Some hubs (Dominator-style) do embed ticket-type rows; attempt a few clicks, but keep it bounded.
    if not any(is_ticket_url(u) for u in out):
        out |= click_ticket_type_rows_capture_urls(driver, hub_url, max_clicks=4)
    return out


def collect_festival_hub_ticket_urls(
    driver,
    hub_url: str,
    *,
    max_subpages: int = 16,
    headless_for_wait: bool = True,
    out_stats: Optional[dict[str, Any]] = None,
) -> set[str]:
    """
    Hub ``/festival-tickets/a/<slug>``: expand UI, parse relaxed ticket URLs, follow linked
    dated event pages, expand again, merge ticket URLs (pattern-only).
    Skips subpage navigation when the hub (or a subpage) is verification/blocked.

    If ``out_stats`` is provided, it may receive keys:
    ``subpages_checked``, ``dated_event_count``, ``direct_hub_ticket_count``.
    """
    hub_n = normalize_url(hub_url) or hub_url
    if out_stats is not None:
        out_stats["subpages_checked"] = 0

    def _title_visible() -> tuple[str, str]:
        title = ""
        vis = ""
        with contextlib.suppress(Exception):
            title = str(getattr(driver, "title", "") or "")
        with contextlib.suppress(Exception):
            vis = str(
                driver.execute_script("return document.body && document.body.innerText") or ""
            )
        return title, vis

    def _refresh_state() -> tuple[str, str, str, str]:
        h_ = driver.page_source or ""
        c_ = str(getattr(driver, "current_url", "") or "")
        t_, v_ = _title_visible()
        return h_, c_, t_, v_

    tickets: set[str] = set()
    scroll_for_lazy_content(driver)
    with contextlib.suppress(Exception):
        try_click_tickets_tab(driver)
    expand_main_accordions(driver, max_clicks=20)
    html, cur, title, vis = _refresh_state()
    if is_blocked_for_discovery(html, title=title, visible_text=vis[:8000], current_url=cur):
        if (not headless_for_wait) and getattr(config, "STEP2_MANUAL_VERIFICATION_PRESS_ENTER", False):
            pause_manual_verification_enter(logger=LOGGER)
            with contextlib.suppress(Exception):
                driver.get(hub_n)
            wait_for_page_content(driver, headless=bool(headless_for_wait))
            html, cur, title, vis = _refresh_state()
    if is_blocked_for_discovery(html, title=title, visible_text=vis[:8000], current_url=cur):
        log_verification_blocked(LOGGER, url=hub_n, title=title, current_url=cur)
        log_step2_verification_blocked(LOGGER, url=hub_n)
        return set()

    merged = merge_link_candidates(html, driver, hub_n)
    for u in merged:
        nu = normalize_url(u)
        if nu and is_ticket_url(nu):
            tickets.add(nu)
    tickets |= _tsx.extract_relaxed_festival_ticket_urls_from_html(html, base_url=hub_n)
    subs = _tsx.extract_hub_child_event_urls_from_html(html, hub_url=hub_n, extra_urls=merged)
    subs.discard(hub_n)
    if out_stats is not None:
        out_stats["dated_event_count"] = len(subs)

    if getattr(config, "STEP2_INTERACT_ENABLED", False):
        n_rounds = int(getattr(config, "STEP2_INTERACT_ROUNDS", 3))
        for _ in range(max(0, n_rounds)):
            step2_interact_once(driver)
            time.sleep(_step2_interaction_sleep())
            scroll_for_lazy_content(driver)
            expand_main_accordions(driver, max_clicks=16)
            html = driver.page_source or ""
            merged_i = merge_link_candidates(html, driver, hub_n)
            for u in merged_i:
                nu = normalize_url(u)
                if nu and is_ticket_url(nu):
                    tickets.add(nu)
            tickets |= _tsx.extract_relaxed_festival_ticket_urls_from_html(html, base_url=hub_n)
            subs |= _tsx.extract_hub_child_event_urls_from_html(html, hub_url=hub_n, extra_urls=merged_i)
            subs.discard(hub_n)
            if out_stats is not None:
                out_stats["dated_event_count"] = len(subs)

    if out_stats is not None:
        out_stats["direct_hub_ticket_count"] = len(tickets)

    for sub in sorted(subs)[: int(max_subpages)]:
        sn = normalize_url(sub)
        if not sn or sn == hub_n:
            continue
        try:
            driver.get(sn)
            time.sleep(0.9 if not getattr(config, "STEP2_SLOW_MODE", False) else _step2_interaction_sleep())
            wait_for_page_content(driver, headless=bool(headless_for_wait))
        except Exception:
            continue
        h2, cur2, t2, v2 = _refresh_state()
        if is_blocked_for_discovery(h2, title=t2, visible_text=v2[:8000], current_url=cur2):
            log_verification_blocked(LOGGER, url=sn, title=t2, current_url=cur2)
            log_step2_verification_blocked(LOGGER, url=sn)
            continue
        if out_stats is not None:
            out_stats["subpages_checked"] = int(out_stats.get("subpages_checked", 0)) + 1
        scroll_for_lazy_content(driver)
        with contextlib.suppress(Exception):
            try_click_tickets_tab(driver)
        expand_main_accordions(driver, max_clicks=18)
        if getattr(config, "STEP2_INTERACT_ENABLED", False):
            run_step2_interaction_rounds(driver)
        revealed = reveal_event_page_deep_links(driver, sn)
        h2 = driver.page_source or ""
        m2 = merge_link_candidates(h2, driver, sn)
        for u in m2 | revealed:
            nu = normalize_url(u)
            if nu and is_ticket_url(nu):
                tickets.add(nu)
        tickets |= _tsx.extract_relaxed_festival_ticket_urls_from_html(h2, base_url=sn)
        for tu in extract_ticket_urls_from_eventtype_cache(h2, base_url=sn):
            nu = normalize_url(tu)
            if nu and is_ticket_url(nu):
                tickets.add(nu)
    try:
        driver.get(hub_n)
        time.sleep(0.35)
    except Exception:
        pass
    return {u for u in tickets if u}


def _save_discovery_debug(driver, *, label: str, url: str, html: str) -> None:
    """Persist HTML + screenshot for tricky discovery cases."""
    try:
        out_dir = Path(config.DEBUG_DIR) / "discovery"
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        safe_label = re.sub(r"[^a-zA-Z0-9_-]+", "_", label)[:60] or "debug"
        safe_path = re.sub(r"[^a-zA-Z0-9_-]+", "_", (urlparse(url).path or "").strip("/")[:70] or "root")
        html_path = out_dir / f"{ts}_{safe_label}_{safe_path}.html"
        html_path.write_text(html or "", encoding="utf-8")
        with contextlib.suppress(Exception):
            driver.save_screenshot(str(out_dir / f"{ts}_{safe_label}_{safe_path}.png"))
    except Exception:
        LOGGER.debug("_save_discovery_debug failed", exc_info=False)


def wait_for_page_content(driver, *, headless: bool) -> str:
    time.sleep(min(1.0, float(config.PAGE_LOAD_SLEEP_SECONDS)))
    deadline = time.time() + float(config.PAGE_READY_TIMEOUT_SECONDS)
    last = ""
    while time.time() < deadline:
        last = driver.page_source or ""
        if has_ticketswap_discovery_signal(last):
            break
        if not looks_like_verification(last) and len(last) > 8000:
            break
        time.sleep(float(config.PAGE_POLL_INTERVAL_SECONDS))
    if not headless and looks_like_verification(last) and int(config.MANUAL_VERIFY_WAIT_SECONDS) > 0:
        LOGGER.warning("Verification; waiting %ss for manual solve...", config.MANUAL_VERIFY_WAIT_SECONDS)
        time.sleep(float(config.MANUAL_VERIFY_WAIT_SECONDS))
        last = driver.page_source or ""
    return last


def extract_hrefs_from_dom(driver) -> set[str]:
    out: set[str] = set()
    try:
        from selenium.webdriver.common.by import By

        sel = ",".join(f"a[href*='{p}']" for p in SUPPORTED_CATEGORY_PREFIXES)
        for el in driver.find_elements(By.CSS_SELECTOR, sel):
            href = el.get_attribute("href") or ""
            n = normalize_url(href)
            if n:
                out.add(n)
    except Exception as e:
        LOGGER.debug("DOM href extraction skipped: %s", e)
    return out


def extract_ticket_urls_from_page_text(html: str, base_url: str) -> set[str]:
    """Deep ticket paths embedded in JSON (__NEXT_DATA__, etc.), not only in href attributes."""
    out: set[str] = set()
    if not html:
        return out
    pat = re.compile(rf"({_deep_ticket_path_pattern()})(?:[\"\\s<>'?#]|$)", re.I)
    for m in pat.finditer(html):
        n = normalize_url(m.group(1), base=base_url)
        if n and is_ticket_url(n):
            out.add(n)
    return out


def extract_ticket_urls_from_eventtype_cache(html: str, base_url: str) -> set[str]:
    """
    Extract deep ticket-category URLs from embedded Apollo/Next cache objects.

    TicketSwap event pages often embed EventType nodes like:
      ..."id":"RXZlbnRUeXBlOjUzMTQyMzM=","slug":"weekend-tickets",...
    where base64-decode(id) -> "EventType:5314233".

    The numeric id is not always present in any href, but can be rebuilt as:
      <event_page_url>/<slug>/<numeric_id>
    """

    out: set[str] = set()
    if not html:
        return out

    def _add_from_pair(b64: str, slug: str) -> None:
        try:
            decoded = base64.b64decode(b64).decode("utf-8", errors="ignore")
        except Exception:
            return
        num = decoded.split(":")[-1].strip()
        if not num.isdigit():
            return
        u = normalize_url(f"{base_url}/{slug}/{num}", base=base_url)
        if u and is_ticket_url(u):
            out.add(u)

    # Field order varies between pages / Apollo normalizations.
    # Most events use "<name>-tickets"; some use singular "-ticket" (e.g. "weekend-loyalty-ticket").
    slug_pat = r"(?P<slug>[a-z0-9_-]+(?:-tickets|-ticket))"
    id_pat = r'(?P<b64>RXZlbnRUeXBlOj[^"]+)"'
    for m in re.finditer(
        rf'"id"\s*:\s*"{id_pat}\s*,\s*"slug"\s*:\s*"{slug_pat}"',
        html,
        re.IGNORECASE,
    ):
        _add_from_pair(m.group("b64"), m.group("slug"))
    for m in re.finditer(
        rf'"slug"\s*:\s*"{slug_pat}"\s*,\s*"id"\s*:\s*"{id_pat}',
        html,
        re.IGNORECASE,
    ):
        _add_from_pair(m.group("b64"), m.group("slug"))

    return out


def extract_next_data_link_candidates(html: str, base_url: str) -> set[str]:
    """Pull category ticket/event paths from Next.js __NEXT_DATA__ (often JSON-escaped \\/)."""
    out: set[str] = set()
    if not html or "__NEXT_DATA__" not in html:
        return out
    m = re.search(
        r'<script[^>]*\bid\s*=\s*["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
        html,
        re.I | re.DOTALL,
    )
    if not m:
        return out
    blob = m.group(1).replace("\\/", "/")
    cats = "|".join(re.escape(p) for p in SUPPORTED_CATEGORY_PREFIXES)
    # Fast regex path grab (works if URLs are already present as strings).
    for sub in re.finditer(rf"(/(?:{cats})/[^\"'\\s<>]+)", blob, re.I):
        n = normalize_url(sub.group(1), base=base_url)
        if n:
            out.add(n)
    # Slower but more reliable: JSON-parse and traverse strings (decodes \\u002F, \\u0026, etc.).
    try:
        data = json.loads(m.group(1))

        def walk(x) -> None:
            if isinstance(x, str):
                for p in SUPPORTED_CATEGORY_PREFIXES:
                    if f"/{p}/" in x:
                        n2 = normalize_url(x, base=base_url)
                        if n2:
                            out.add(n2)
                        break
                return
            if isinstance(x, dict):
                for v in x.values():
                    walk(v)
                return
            if isinstance(x, list):
                for v in x:
                    walk(v)
                return

        walk(data)
    except Exception:
        pass
    return out


def merge_link_candidates(html: str, driver, base_url: str) -> set[str]:
    s = extract_candidate_urls_from_html(html, base_url=base_url)
    s |= extract_next_data_link_candidates(html, base_url=base_url)
    s |= extract_hrefs_from_dom(driver)
    s |= extract_hrefs_from_dom_js(driver)
    s |= extract_ticket_urls_from_page_text(html, base_url=base_url)
    s |= extract_ticket_urls_from_eventtype_cache(html, base_url=base_url)
    s |= _tsx.extract_relaxed_festival_ticket_urls_from_html(html, base_url=base_url)
    return s


def extract_candidate_urls_from_html(html: str, base_url: str) -> set[str]:
    out: set[str] = set()
    if not html:
        return out
    for m in re.finditer(r"""href\s*=\s*["']([^"']+)["']""", html, flags=re.IGNORECASE):
        n = normalize_url(m.group(1), base=base_url)
        if n:
            out.add(n)
    cats = "|".join(re.escape(p) for p in SUPPORTED_CATEGORY_PREFIXES)
    for m in re.finditer(rf"""(/(?:{cats})/[^"'\s<>]+)""", html, flags=re.IGNORECASE):
        n = normalize_url(m.group(1), base=base_url)
        if n:
            out.add(n)
    return out


def extract_festival_hub_urls_from_html(html: str, base_url: str) -> set[str]:
    """
    Find series hub URLs (/festival-tickets/a/<slug>) embedded in raw HTML or JSON.

    The overview page https://www.ticketswap.com/festival-tickets often hydrates slowly;
    hub paths may appear in __NEXT_DATA__ or inline JSON before every <a href> is visible.
    """
    out: set[str] = set()
    if not html:
        return out
    for m in re.finditer(r'(/festival-tickets/a/[^"\'\s<>?#]+)', html, re.IGNORECASE):
        n = normalize_url(m.group(1), base=base_url)
        if n and is_festival_page(n):
            out.add(n)
    return out


def _overview_hub_anchor_count(driver) -> int:
    try:
        n = driver.execute_script(
            'return document.querySelectorAll(\'a[href*="/festival-tickets/a/"]\').length'
        )
        return int(n or 0)
    except Exception:
        return 0


def _overview_hub_signal_count(driver) -> int:
    """
    Progress signal for overview expansion.
    Prefer DOM anchors, but also count raw occurrences in HTML because some pages hydrate hubs in JSON/script
    before rendering clickable <a> tags.
    """
    a = _overview_hub_anchor_count(driver)
    try:
        html = driver.page_source or ""
        b = len(re.findall(r"/festival-tickets/a/", html, flags=re.I))
    except Exception:
        b = 0
    return max(int(a), int(b))


def _url_has_amsterdam_location_param(url: str) -> bool:
    n = normalize_url(url) or url
    loc = parse_qs(urlparse(n).query).get("location", [])
    return loc == ["3"]


def try_select_amsterdam_location_filter(driver, page_url: str) -> bool:
    """
    If the listing is not already scoped to Amsterdam (location=3), click an "Amsterdam" control in main.
    Returns True when a click was performed.
    """
    if _url_has_amsterdam_location_param(page_url):
        return False
    try:
        return bool(
            driver.execute_script(
                r"""
                function norm(t) { return (t || '').replace(/\s+/g, ' ').trim(); }
                const root = document.querySelector('main') || document.body;

                // 1) Direct chip/button that says Amsterdam
                const nodes = Array.from(root.querySelectorAll('button, a, [role="button"], span[role="button"]'));
                for (const el of nodes) {
                  const t = norm(el.textContent).toLowerCase();
                  if (t !== 'amsterdam') continue;
                  if (el.getAttribute('aria-pressed') === 'true') return false;
                  if (el.getAttribute('aria-selected') === 'true') return false;
                  if (el.hasAttribute('disabled') || el.getAttribute('aria-disabled') === 'true') continue;
                  try { el.scrollIntoView({block: 'center', inline: 'nearest'}); el.click(); return true; } catch (e) {}
                }

                // 2) Open a location dropdown/control and select Amsterdam
                const openers = nodes.filter(el => {
                  const t = norm(el.textContent).toLowerCase();
                  return (
                    t.includes('location') || t.includes('area') || t.includes('plaats') || t.includes('regio') ||
                    t.includes('stad') || t.includes('city') || t.includes('where')
                  );
                });
                for (const op of openers.slice(0, 4)) {
                  try { op.scrollIntoView({block: 'center', inline: 'nearest'}); op.click(); } catch (e) {}
                  // After opening, search in document (menus/portals may render outside main)
                  const all = Array.from(document.querySelectorAll('button, a, [role="option"], [role="menuitem"], [role="button"], li'));
                  for (const el of all) {
                    const t = norm(el.textContent).toLowerCase();
                    if (t !== 'amsterdam') continue;
                    if (el.hasAttribute('disabled') || el.getAttribute('aria-disabled') === 'true') continue;
                    try { el.scrollIntoView({block: 'center', inline: 'nearest'}); el.click(); return true; } catch (e) {}
                  }
                }

                return false;
                """
            )
        )
    except Exception:
        return False


def expand_festival_overview_show_more(driver, *, max_clicks: int) -> int:
    """
    Repeatedly click TicketSwap's overview "Show more" control until:
    - button not found / disabled
    - link count stops increasing for a few cycles
    - max_clicks reached
    """
    stagnant_cycles = 0
    prev = _overview_hub_signal_count(driver)
    clicks = 0

    def _jitter(base: float = 0.55) -> None:
        time.sleep(base + random.random() * 0.65)

    while clicks < max_clicks and stagnant_cycles < 3:
        scroll_for_lazy_content(driver)
        clicked = False
        disabled_or_missing = False
        try:
            res = driver.execute_script(
                r"""
                function norm(t) { return (t || '').replace(/\s+/g, ' ').trim().toLowerCase(); }
                const root = document.querySelector('main') || document.body;
                const needles = ['show more', 'load more', 'toon meer', 'meer tonen', 'more', 'laad meer',
                  'voir plus', 'afficher plus', 'charger plus'];
                const els = Array.from(root.querySelectorAll('button, a, [role="button"]'));
                let best = null;
                for (const el of els) {
                  const t = norm(el.textContent);
                  if (!t) continue;
                  if (!needles.some(n => t.includes(n))) continue;
                  best = el;
                  if (t === 'show more' || t === 'load more' || t === 'toon meer' || t === 'voir plus') break;
                }
                if (!best) return {found:false, clicked:false, disabled:false};
                const disabled = best.hasAttribute('disabled') || best.getAttribute('aria-disabled') === 'true';
                if (disabled) return {found:true, clicked:false, disabled:true};
                try { best.scrollIntoView({block:'center', inline:'nearest'}); best.click(); return {found:true, clicked:true, disabled:false}; }
                catch (e) { return {found:true, clicked:false, disabled:false}; }
                """
            )
            if isinstance(res, dict):
                clicked = bool(res.get("clicked"))
                disabled_or_missing = bool(res.get("disabled")) or not bool(res.get("found"))
            else:
                clicked = bool(res)
        except Exception:
            clicked = False

        if not clicked:
            if disabled_or_missing:
                break
            stagnant_cycles += 1
            _jitter(0.35)
            continue

        clicks += 1
        _jitter(0.65)
        scroll_for_lazy_content(driver)
        cur = _overview_hub_signal_count(driver)
        if cur <= prev:
            stagnant_cycles += 1
        else:
            stagnant_cycles = 0
            prev = cur
    return clicks


def try_select_city_location_filter(driver, city_name: str) -> bool:
    """
    Click a location UI control labeled with city_name (e.g. "Berlin").
    Returns True only when an element matching the normalized label was clicked.
    """
    city = (city_name or "").strip().lower()
    if not city:
        return False
    try:
        return bool(
            driver.execute_script(
                r"""
                const city = arguments[0];
                function norm(t) { return (t || '').replace(/\s+/g, ' ').trim().toLowerCase(); }
                function clickEl(el) {
                  try {
                    el.scrollIntoView({block:'center', inline:'nearest'});
                    el.click();
                    return true;
                  } catch (e) {}
                  return false;
                }
                const root = document.querySelector('main') || document.body;
                const nodes = Array.from(root.querySelectorAll(
                  'button, a, [role="button"], span[role="button"], [role="option"], [role="menuitem"], li'
                ));
                for (const el of nodes) {
                  const t = norm(el.textContent);
                  if (t === city) {
                    if (el.hasAttribute('disabled') || el.getAttribute('aria-disabled') === 'true') continue;
                    if (clickEl(el)) return true;
                  }
                }
                for (const el of nodes) {
                  const t = norm(el.textContent);
                  if (t.includes(city) && t.length < 80) {
                    if (el.hasAttribute('disabled') || el.getAttribute('aria-disabled') === 'true') continue;
                    if (clickEl(el)) return true;
                  }
                }
                return false;
                """,
                city,
            )
        )
    except Exception:
        return False


def _normalize_location_text(value: str) -> str:
    t = unicodedata.normalize("NFKD", (value or ""))
    t = "".join(ch for ch in t if not unicodedata.combining(ch))
    t = re.sub(r"\s+", " ", t).strip().lower()
    return t


def _location_param_from_url(url: str) -> Optional[str]:
    try:
        q = parse_qs(urlparse(url).query)
        v = (q.get("location") or [None])[0]
        return str(v) if v is not None else None
    except Exception:
        return None


def _dismiss_page_overlays(driver: Any) -> None:
    try:
        driver.execute_script(
            r"""
            const norm = (s) => String(s || '').toLowerCase().replace(/\s+/g, ' ').trim();
            const clickMatching = (want) => {
              const nodes = Array.from(document.querySelectorAll('button, [role="button"]'));
              for (const n of nodes) {
                const t = norm(n.textContent || n.innerText || '');
                const a = norm(n.getAttribute && n.getAttribute('aria-label'));
                if (want && !(t.includes(want) || a.includes(want))) continue;
                try { n.click(); } catch (e) {}
              }
            };
            clickMatching('accept');
            clickMatching('close');
            clickMatching('not now');
            """
        )
    except Exception:
        pass


def _wait_for_city_filter(driver: Any, timeout_seconds: float = 10.0) -> bool:
    end = time.time() + max(0.5, float(timeout_seconds))
    while time.time() < end:
        try:
            if bool(driver.execute_script("return !!document.querySelector('select#city-filter')")):
                return True
        except Exception:
            pass
        time.sleep(0.25)
    return False


def _city_filter_options(driver: Any) -> list[dict[str, Any]]:
    try:
        out = driver.execute_script(
            r"""
            const sel = document.querySelector('select#city-filter');
            if (!sel) return [];
            return Array.from(sel.options || []).map(o => ({
              value: String(o.value || ''),
              text: String((o.textContent || '').trim()),
              selected: !!o.selected
            }));
            """
        )
        if isinstance(out, list):
            return [x for x in out if isinstance(x, dict)]
    except Exception:
        pass
    return []


def _selected_city_filter_text(driver: Any) -> Optional[str]:
    try:
        txt = driver.execute_script(
            r"""
            const sel = document.querySelector('select#city-filter');
            if (!sel) return '';
            const idx = sel.selectedIndex;
            if (idx < 0 || idx >= sel.options.length) return '';
            return String((sel.options[idx].textContent || '').trim());
            """
        )
        out = str(txt or "").strip()
        return out or None
    except Exception:
        return None


def selected_location_text(driver_or_page: Any) -> Optional[str]:
    return _selected_city_filter_text(driver_or_page)


def _select_city_from_native_filter(driver: Any, city: str) -> tuple[bool, Optional[str]]:
    n_city = _normalize_location_text(city)
    for opt in _city_filter_options(driver):
        txt = str(opt.get("text", ""))
        val = str(opt.get("value", ""))
        n_txt = _normalize_location_text(txt)
        n_val = _normalize_location_text(val)
        if not (n_txt == n_city or n_txt.startswith(n_city + ",") or n_val == n_city):
            continue
        try:
            driver.execute_script(
                r"""
                const v = arguments[0];
                const sel = document.querySelector('select#city-filter');
                if (!sel) return false;
                sel.value = v;
                sel.dispatchEvent(new Event('input', {bubbles:true}));
                sel.dispatchEvent(new Event('change', {bubbles:true}));
                return true;
                """,
                val,
            )
            time.sleep(1.2)
            return True, txt or val
        except Exception:
            return False, None
    return False, None


def _select_other_city_in_native_filter(driver: Any) -> bool:
    try:
        return bool(
            driver.execute_script(
                r"""
                const sel = document.querySelector('select#city-filter');
                if (!sel) return false;
                const opts = Array.from(sel.options || []);
                const m = opts.find(o => String(o.value || '').toLowerCase() === 'other')
                  || opts.find(o => String((o.textContent || '').trim()).toLowerCase() === 'other city');
                if (!m) return false;
                sel.value = String(m.value || 'other');
                sel.dispatchEvent(new Event('input', {bubbles:true}));
                sel.dispatchEvent(new Event('change', {bubbles:true}));
                return true;
                """
            )
        )
    except Exception:
        return False


def _modal_root_selector() -> str:
    return "dialog[open], [role='dialog'], .modal, [class*='modal']"


def _wait_for_other_city_modal(driver: Any, timeout_seconds: float = 10.0) -> bool:
    end = time.time() + max(0.5, float(timeout_seconds))
    while time.time() < end:
        try:
            shown = bool(
                driver.execute_script(
                    r"""
                    const roots = Array.from(document.querySelectorAll(arguments[0]));
                    const norm = (s) => String(s || '').toLowerCase().replace(/\s+/g, ' ').trim();
                    for (const r of roots) {
                      const t = norm(r.textContent || r.innerText || '');
                      if (t.includes('other city')) return true;
                    }
                    return false;
                    """,
                    _modal_root_selector(),
                )
            )
            if shown:
                return True
        except Exception:
            pass
        time.sleep(0.25)
    return False


def _type_into_modal_search(driver: Any, query: str) -> bool:
    q = (query or "").strip()
    if not q:
        return False
    try:
        from selenium.webdriver.common.by import By
    except Exception:
        By = None  # type: ignore
    if By is not None:
        try:
            inputs = driver.find_elements(
                By.CSS_SELECTOR,
                "dialog[open] input[placeholder*='Search your city'], [role='dialog'] input[placeholder*='Search your city'], dialog[open] input[type='text'], [role='dialog'] input[type='text']",
            )
            if inputs:
                inp = inputs[0]
                inp.click()
                with contextlib.suppress(Exception):
                    inp.clear()
                inp.send_keys(q)
                time.sleep(0.25)
                return True
        except Exception:
            pass
    try:
        return bool(
            driver.execute_script(
                r"""
                const q = arguments[0];
                const roots = Array.from(document.querySelectorAll(arguments[1]));
                const normalize = (s) => String(s || '').toLowerCase().replace(/\s+/g, ' ').trim();
                for (const r of roots) {
                  const rt = normalize(r.textContent || r.innerText || '');
                  if (!rt.includes('other city')) continue;
                  const inputs = Array.from(r.querySelectorAll(
                    "input[placeholder*='Search your city'], input[type='text'], dialog input, [role='dialog'] input, input"
                  ));
                  if (!inputs.length) continue;
                  const input = inputs[0];
                  try { input.focus(); } catch (e) {}
                  input.value = '';
                  input.dispatchEvent(new Event('input', {bubbles:true}));
                  input.value = q;
                  input.dispatchEvent(new Event('input', {bubbles:true}));
                  input.dispatchEvent(new Event('change', {bubbles:true}));
                  return true;
                }
                return false;
                """,
                q,
                _modal_root_selector(),
            )
        )
    except Exception:
        return False


def _collect_modal_suggestions(driver: Any) -> list[str]:
    try:
        out = driver.execute_script(
            r"""
            const roots = Array.from(document.querySelectorAll(arguments[0]));
            const norm = (s) => String(s || '').replace(/\s+/g, ' ').trim();
            const bad = new Set(['other city', 'search your city', 'close', 'x', 'cancel']);
            for (const r of roots) {
              const rt = norm(r.textContent || r.innerText || '').toLowerCase();
              if (!rt.includes('other city')) continue;
              const nodes = Array.from(r.querySelectorAll(
                "[role='option'], li, button, div, a, [class*='option'], [class*='suggest']"
              ));
              const items = [];
              for (const n of nodes) {
                const txt = norm(n.textContent || n.innerText || '');
                if (!txt || txt.length <= 2 || txt.length > 140) continue;
                if (bad.has(txt.toLowerCase())) continue;
                if ((txt.match(/,/g) || []).length > 1) continue;
                if (!txt.includes(',')) continue;
                const rect = n.getBoundingClientRect();
                if (!(rect.width > 2 && rect.height > 2)) continue;
                items.push(txt);
              }
              return Array.from(new Set(items)).slice(0, 80);
            }
            return [];
            """,
            _modal_root_selector(),
        )
        if isinstance(out, list):
            return [str(x).strip() for x in out if str(x).strip()]
    except Exception:
        pass
    return []


def _pick_suggestion_index(
    suggestions: list[str],
    *,
    city: str,
    country: str,
    expected_suggestion: Optional[str],
) -> int:
    if not suggestions:
        return -1
    n_city = _normalize_location_text(city)
    n_country = _normalize_location_text(country)
    n_expected = _normalize_location_text(expected_suggestion or "")
    for i, s in enumerate(suggestions):
        if n_expected and _normalize_location_text(s) == n_expected:
            return i
    for i, s in enumerate(suggestions):
        n = _normalize_location_text(s)
        if n.startswith(n_city + ",") and (not n_country or n_country in n):
            return i
    for i, s in enumerate(suggestions):
        if _normalize_location_text(s) == n_city:
            return i
    return -1


def _click_modal_suggestion(driver: Any, suggestion_text: str) -> bool:
    target = _normalize_location_text(suggestion_text)
    try:
        return bool(
            driver.execute_script(
                r"""
                const target = arguments[0];
                const roots = Array.from(document.querySelectorAll(arguments[1]));
                const normalize = (s) => {
                  const ascii = String(s || '').normalize('NFKD').replace(/[\u0300-\u036f]/g, '');
                  return ascii.toLowerCase().replace(/\s+/g, ' ').trim();
                };
                for (const r of roots) {
                  const rt = normalize(r.textContent || r.innerText || '');
                  if (!rt.includes('other city')) continue;
                  const nodes = Array.from(r.querySelectorAll("[role='option'], li, button, div, a"));
                  for (const n of nodes) {
                    const txt = String(n.textContent || n.innerText || '').trim();
                    if (!txt || !txt.includes(',')) continue;
                    if (normalize(txt) !== target) continue;
                    const rect = n.getBoundingClientRect();
                    if (!(rect.width > 2 && rect.height > 2)) continue;
                    try { n.scrollIntoView({block:'center'}); n.click(); return true; } catch (e) {}
                  }
                }
                return false;
                """,
                target,
                _modal_root_selector(),
            )
        )
    except Exception:
        return False


def _wait_modal_closed(driver: Any, timeout_seconds: float = 10.0) -> bool:
    end = time.time() + max(0.5, float(timeout_seconds))
    while time.time() < end:
        try:
            open_state = bool(
                driver.execute_script(
                    r"""
                    const roots = Array.from(document.querySelectorAll(arguments[0]));
                    const norm = (s) => String(s || '').toLowerCase().replace(/\s+/g, ' ').trim();
                    for (const r of roots) {
                      const txt = norm(r.textContent || r.innerText || '');
                      if (txt.includes('other city')) {
                        const rect = r.getBoundingClientRect();
                        if (rect.width > 2 && rect.height > 2) return true;
                      }
                    }
                    return false;
                    """,
                    _modal_root_selector(),
                )
            )
            if not open_state:
                return True
        except Exception:
            return True
        time.sleep(0.25)
    return False


def _save_location_debug_state(
    *,
    driver: Any,
    debug_dir: Optional[Path],
    html_name: str,
    png_name: str,
    suggestions: Optional[list[str]] = None,
) -> None:
    if debug_dir is None:
        return
    debug_dir.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(Exception):
        (debug_dir / html_name).write_text(driver.page_source or "", encoding="utf-8")
    with contextlib.suppress(Exception):
        driver.save_screenshot(str(debug_dir / png_name))
    if suggestions is not None:
        with contextlib.suppress(Exception):
            (debug_dir / "suggestions.json").write_text(
                json.dumps(suggestions, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )


def _verify_location_selection(
    *,
    city: str,
    country: str,
    selected_suggestion: Optional[str],
    selected_dropdown_text: Optional[str],
    resulting_url: str,
) -> bool:
    n_city = _normalize_location_text(city)
    n_country = _normalize_location_text(country)
    n_sug = _normalize_location_text(selected_suggestion or "")
    n_dd = _normalize_location_text(selected_dropdown_text or "")
    if n_sug and n_sug.startswith(n_city + ",") and (not n_country or n_country in n_sug):
        return True
    if n_dd and (n_dd == n_city or n_dd.startswith(n_city + ",") or n_dd.startswith(n_city + " ")):
        return True
    return _location_param_from_url(resulting_url) is not None


def select_location(
    driver_or_page: Any,
    location_name: str,
    country_hint: Optional[str] = None,
    expected_suggestion: Optional[str] = None,
    debug_dir: Optional[Path] = None,
) -> dict[str, Any]:
    city = (location_name or "").strip()
    country = (country_hint or "").strip()
    expected = (expected_suggestion or f"{city}, {country}".strip(", ")).strip()
    out = {
        "success": False,
        "city": city,
        "country": country,
        "expected_suggestion": expected,
        "selected_suggestion": None,
        "selected_dropdown_text": None,
        "resulting_url": None,
        "location_param": None,
        "strategy_used": None,
        "error_message": None,
        "suggestions_available": [],
        # backward-compatible keys
        "requested_location": city,
        "country_hint": country,
        "selected_text": None,
    }
    if not city:
        out["error_message"] = "empty_city"
        return out
    d = driver_or_page
    _dismiss_page_overlays(d)
    _save_location_debug_state(
        driver=d,
        debug_dir=debug_dir,
        html_name="before.html",
        png_name="screenshot_before.png",
    )
    if not _wait_for_city_filter(d, timeout_seconds=12):
        out["error_message"] = "city_filter_not_found"
        cur = normalize_url(getattr(d, "current_url", "") or "") or (getattr(d, "current_url", "") or "")
        out["resulting_url"] = cur
        out["location_param"] = _location_param_from_url(cur)
        return out

    ok_native, native_text = _select_city_from_native_filter(d, city)
    prefer_modal_exact = bool(expected and "," in expected and ("," not in str(native_text or "")))
    if ok_native and not prefer_modal_exact:
        cur = normalize_url(getattr(d, "current_url", "") or "") or (getattr(d, "current_url", "") or "")
        out["strategy_used"] = "native_select"
        out["selected_dropdown_text"] = _selected_city_filter_text(d) or native_text
        out["selected_text"] = out["selected_dropdown_text"]
        if out["selected_dropdown_text"] and "," in out["selected_dropdown_text"]:
            out["selected_suggestion"] = out["selected_dropdown_text"]
        out["resulting_url"] = cur
        out["location_param"] = _location_param_from_url(cur)
        out["success"] = _verify_location_selection(
            city=city,
            country=country,
            selected_suggestion=None,
            selected_dropdown_text=out["selected_dropdown_text"],
            resulting_url=cur,
        )
        if not out["success"]:
            out["error_message"] = "native_selection_not_verified"
        return out

    if not _select_other_city_in_native_filter(d):
        out["error_message"] = "other_city_option_not_found"
        cur = normalize_url(getattr(d, "current_url", "") or "") or (getattr(d, "current_url", "") or "")
        out["resulting_url"] = cur
        out["location_param"] = _location_param_from_url(cur)
        return out
    if not _wait_for_other_city_modal(d, timeout_seconds=10):
        out["error_message"] = "other_city_modal_not_found"
        return out
    _save_location_debug_state(
        driver=d,
        debug_dir=debug_dir,
        html_name="after_other_city.html",
        png_name="screenshot_modal.png",
    )
    if not _type_into_modal_search(d, city):
        out["error_message"] = "modal_search_input_not_found"
        return out
    time.sleep(1.6)
    suggestions = _collect_modal_suggestions(d)
    out["suggestions_available"] = suggestions
    _save_location_debug_state(
        driver=d,
        debug_dir=debug_dir,
        html_name="after_typing.html",
        png_name="screenshot_modal.png",
        suggestions=suggestions,
    )
    idx = _pick_suggestion_index(
        suggestions,
        city=city,
        country=country,
        expected_suggestion=expected,
    )
    if idx < 0 and country:
        if _type_into_modal_search(d, f"{city} {country}"):
            time.sleep(1.3)
            suggestions = _collect_modal_suggestions(d)
            out["suggestions_available"] = suggestions
            _save_location_debug_state(
                driver=d,
                debug_dir=debug_dir,
                html_name="after_typing.html",
                png_name="screenshot_modal.png",
                suggestions=suggestions,
            )
            idx = _pick_suggestion_index(
                suggestions,
                city=city,
                country=country,
                expected_suggestion=expected,
            )
    if idx < 0:
        out["error_message"] = "expected_suggestion_not_found"
        cur = normalize_url(getattr(d, "current_url", "") or "") or (getattr(d, "current_url", "") or "")
        out["resulting_url"] = cur
        out["location_param"] = _location_param_from_url(cur)
        return out
    chosen = suggestions[idx]
    if not _click_modal_suggestion(d, chosen):
        out["error_message"] = "click_suggestion_failed"
        return out
    _wait_modal_closed(d, timeout_seconds=10)
    time.sleep(1.4)
    cur = normalize_url(getattr(d, "current_url", "") or "") or (getattr(d, "current_url", "") or "")
    out["strategy_used"] = "modal_exact_suggestion"
    out["selected_suggestion"] = chosen
    out["selected_dropdown_text"] = _selected_city_filter_text(d) or selected_location_text(d)
    out["selected_text"] = out["selected_dropdown_text"] or out["selected_suggestion"]
    out["resulting_url"] = cur
    out["location_param"] = _location_param_from_url(cur)
    out["success"] = _verify_location_selection(
        city=city,
        country=country,
        selected_suggestion=chosen,
        selected_dropdown_text=out["selected_dropdown_text"],
        resulting_url=cur,
    )
    if not out["success"]:
        out["error_message"] = "selection_not_verified"
    _save_location_debug_state(
        driver=d,
        debug_dir=debug_dir,
        html_name="after_selection.html",
        png_name="screenshot_after_selection.png",
        suggestions=suggestions,
    )
    return out


def select_location_exact(
    driver_or_page: Any,
    city: str,
    country: str,
    expected_suggestion: Optional[str] = None,
    debug_dir: Optional[Path] = None,
) -> dict[str, Any]:
    return select_location(
        driver_or_page,
        city,
        country_hint=country,
        expected_suggestion=expected_suggestion,
        debug_dir=debug_dir,
    )

def list_event_urls_from_category_listing(
    driver,
    html: str,
    listing_url: str,
    *,
    category_prefix: str,
) -> list[str]:
    """
    Normalize + de-duplicate event page URLs appearing on a category listing
    (/concert-tickets, …), excluding hubs (/festival-tickets/a/) and ticket deep links.
    """
    base = normalize_url(listing_url) or listing_url
    cand = merge_link_candidates(html or "", driver, base_url=base)
    seen: set[str] = set()
    out: list[str] = []
    for u in sorted(cand):
        n = normalize_url(u)
        if not n or not is_event_page(n):
            continue
        if detect_category_prefix(n) != category_prefix:
            continue
        if is_ticket_url(n) or is_festival_page(n):
            continue
        if n in seen:
            continue
        seen.add(n)
        out.append(n)
    return out


def _listing_event_signal_count(driver, listing_url: str, category_prefix: str) -> int:
    """Progress heuristic for arbitrary category listings (event cards, not festival hubs)."""
    base = normalize_url(listing_url) or listing_url
    html = driver.page_source or ""
    cand = merge_link_candidates(html, driver, base_url=base)
    a = 0
    for u in cand:
        n = normalize_url(u)
        if not n or not is_event_page(n):
            continue
        if detect_category_prefix(n) != category_prefix:
            continue
        if is_ticket_url(n) or is_festival_page(n):
            continue
        a += 1
    escaped = re.escape(category_prefix)
    b = len(
        re.findall(
            rf'/{escaped}/(?!a/)(?![^"\'\\s<>?#]+/[^"\'\\s<>?#]+/\d+)[^"\'\\s<>?#]+',
            html,
            flags=re.I,
        )
    )
    return max(a, b)


def expand_category_listing_show_more(
    driver, listing_url: str, category_prefix: str, *, max_clicks: int
) -> int:
    """
    Like expand_festival_overview_show_more, but uses event-card counts on any supported category listing.
    """
    stagnant_cycles = 0
    prev = _listing_event_signal_count(driver, listing_url, category_prefix)
    clicks = 0

    def _jitter_local(base: float = 0.55) -> None:
        time.sleep(base + random.random() * 0.65)

    while clicks < max_clicks and stagnant_cycles < 3:
        scroll_for_lazy_content(driver)
        clicked = False
        disabled_or_missing = False
        try:
            res = driver.execute_script(
                r"""
                function norm(t) { return (t || '').replace(/\s+/g, ' ').trim().toLowerCase(); }
                const root = document.querySelector('main') || document.body;
                const needles = ['show more', 'load more', 'toon meer', 'meer tonen', 'more', 'laad meer',
                  'voir plus', 'afficher plus', 'charger plus'];
                const els = Array.from(root.querySelectorAll('button, a, [role="button"]'));
                let best = null;
                for (const el of els) {
                  const t = norm(el.textContent);
                  if (!t) continue;
                  if (!needles.some(n => t.includes(n))) continue;
                  best = el;
                  if (t === 'show more' || t === 'load more' || t === 'toon meer' || t === 'voir plus') break;
                }
                if (!best) return {found:false, clicked:false, disabled:false};
                const disabled = best.hasAttribute('disabled') || best.getAttribute('aria-disabled') === 'true';
                if (disabled) return {found:true, clicked:false, disabled:true};
                try { best.scrollIntoView({block:'center', inline:'nearest'}); best.click(); return {found:true, clicked:true, disabled:false}; }
                catch (e) { return {found:true, clicked:false, disabled:false}; }
                """
            )
            if isinstance(res, dict):
                clicked = bool(res.get("clicked"))
                disabled_or_missing = bool(res.get("disabled")) or not bool(res.get("found"))
            else:
                clicked = bool(res)
        except Exception:
            clicked = False

        if not clicked:
            if disabled_or_missing:
                break
            stagnant_cycles += 1
            _jitter_local(0.35)
            continue

        clicks += 1
        _jitter_local(0.65)
        scroll_for_lazy_content(driver)
        cur = _listing_event_signal_count(driver, listing_url, category_prefix)
        if cur <= prev:
            stagnant_cycles += 1
        else:
            stagnant_cycles = 0
            prev = cur
    return clicks


def _body_inner_text_lower(driver) -> str:
    try:
        t = driver.execute_script("return document.body && document.body.innerText")
        return (t or "").lower()
    except Exception:
        return ""


def discover_ticket_urls_for_event_uc(
    driver,
    event_url: str,
    *,
    headless: bool,
) -> tuple[list[str], str]:
    """
    Selenium/UC counterpart to playbook Step 2: open event page and return deep ticket URLs for that event.

    Status string: ``ok``, ``verification_blocked``, ``event_404``, ``bad_url``.
    """
    ev = normalize_url(event_url)
    if not ev:
        return [], "bad_url"
    driver.get(ev)
    html = wait_for_page_content(driver, headless=bool(headless))
    if is_blocked_for_discovery(html):
        return [], "verification_blocked"
    blob = (_body_inner_text_lower(driver) or (html or "").lower())
    if "hmm, 404" in blob or "we're a bit lost" in blob or "we’re a bit lost" in blob:
        return [], "event_404"
    revealed = reveal_event_page_deep_links(driver, ev)
    html2 = driver.page_source or html
    merged = merge_link_candidates(html2, driver, base_url=ev)
    merged |= revealed
    tickets: set[str] = set()
    for u in merged:
        nu = normalize_url(u)
        if not nu or not is_ticket_url(nu):
            continue
        eu = normalize_url(event_url_from_ticket_url(nu) or "")
        if eu == ev:
            tickets.add(nu)
    for tu in extract_ticket_urls_from_eventtype_cache(html2, base_url=ev):
        nu = normalize_url(tu)
        if nu and normalize_url(event_url_from_ticket_url(nu) or "") == ev:
            tickets.add(nu)
    return sorted(tickets), "ok"


_HUB_EVENT_FOLLOW_CAP = 3


def discover_ticket_urls_for_listing_target_uc(
    driver,
    target_url: str,
    *,
    headless: bool,
) -> tuple[list[str], str]:
    """
    Resolve ticket URLs for anything the stress runner / discovery may open:
    deep ticket URL (passthrough), festival series hub, or single-event page.

    Status: ``ok``, ``verification_blocked``, ``hub_404``, ``bad_url``, ``unsupported_target``.
    """
    n = normalize_url(target_url)
    if not n:
        return [], "bad_url"
    if is_ticket_url(n):
        return [n], "ok"
    if is_festival_page(n):
        driver.get(n)
        html = wait_for_page_content(driver, headless=bool(headless))
        if is_blocked_for_discovery(html):
            return [], "verification_blocked"
        blob = _body_inner_text_lower(driver) or (html or "").lower()
        if "hmm, 404" in blob or "we're a bit lost" in blob or "we’re a bit lost" in blob:
            return [], "hub_404"
        cands = gather_hub_page_candidates(driver, n)
        direct: set[str] = set()
        for u in cands:
            nu = normalize_url(str(u))
            if nu and is_ticket_url(nu):
                direct.add(nu)
        if direct:
            return sorted(direct), "ok"
        events: list[str] = []
        for u in sorted(cands):
            nu = normalize_url(str(u))
            if nu and is_event_page(nu) and not is_festival_page(nu):
                events.append(nu)
        acc: set[str] = set()
        for ev in events[:_HUB_EVENT_FOLLOW_CAP]:
            sub, st = discover_ticket_urls_for_event_uc(driver, ev, headless=headless)
            if st == "verification_blocked":
                return [], st
            acc.update(sub)
        return sorted(acc), "ok"
    if is_event_page(n):
        return discover_ticket_urls_for_event_uc(driver, n, headless=headless)
    return [], "unsupported_target"


def list_stress_targets_from_listing(
    driver,
    html: str,
    listing_url: str,
    *,
    category_prefix: str,
) -> tuple[list[str], str]:
    """
    URLs to exercise Step 2 + market scrape from a category listing.

    Festival **overview** pages mostly advertise series hubs; other listings advertise events.
    Returns ``(urls, kind)`` where kind is ``hubs`` or ``events``.
    """
    base = normalize_url(listing_url) or listing_url
    if category_prefix == "festival-tickets" and is_festival_overview_page(base):
        merged = merge_link_candidates(html or "", driver, base_url=base)
        hubs: set[str] = {x for x in merged if is_festival_page(x)}
        hubs |= extract_festival_hub_urls_from_html(html or "", base_url=base)
        return sorted(hubs), "hubs"
    evs = list_event_urls_from_category_listing(
        driver, html or "", base, category_prefix=category_prefix
    )
    return evs, "events"


def gather_festival_overview_hub_urls(
    driver,
    overview_url: str,
    *,
    headless: bool,
    max_show_more: Optional[int] = None,
    skip_initial_nav: bool = False,
) -> set[str]:
    """
    Load a /festival-tickets overview (optionally with query), optionally click Amsterdam,
    expand via 'Show more', and return discovered series hub URLs.
    """
    u = normalize_url(overview_url) or overview_url
    if not skip_initial_nav:
        driver.get(u)
    html = wait_for_page_content(driver, headless=bool(headless))
    if is_blocked_for_discovery(html):
        LOGGER.warning("Overview page blocked by verification; run non-headless with persistent profile. url=%s", u)
        return set()
    scroll_for_lazy_content(driver)
    if try_select_amsterdam_location_filter(driver, u):
        time.sleep(0.65)
        html = wait_for_page_content(driver, headless=bool(headless))
        scroll_for_lazy_content(driver)
    cap = int(max_show_more) if max_show_more is not None else int(getattr(config, "DISCOVERY_OVERVIEW_MAX_SHOW_MORE", 50))
    expand_festival_overview_show_more(driver, max_clicks=max(1, cap))
    html_final = driver.page_source or ""
    out = merge_link_candidates(html_final, driver, base_url=u)
    hubs = {x for x in out if is_festival_page(x)}
    hubs |= extract_festival_hub_urls_from_html(html_final, base_url=u)
    return hubs


def event_id_from_event_url(event_url: str) -> str:
    # Stable enough: use path slug as event_id.
    # Example: https://www.ticketswap.com/festival-tickets/<event-slug>
    return normalize_url(event_url).split("/festival-tickets/")[-1]


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Discover TicketSwap ticket URLs and store them in ticketswap.db")
    p.add_argument("--seed", action="append", default=[], help="Seed URL (repeatable)")
    p.add_argument(
        "--no-config-seeds",
        action="store_true",
        help="Ignore config.SEED_URLS (useful for deep-URL-only runs without opening a browser).",
    )
    p.add_argument("--headless", action="store_true", default=False)
    p.add_argument("--max-pages", type=int, default=50, help="Max festival hub pages to scan per run")
    p.add_argument(
        "--from-hubs-table",
        action="store_true",
        help="Append active hub URLs from festival_hubs table to seeds.",
    )
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(list(argv))


def run_discovery(
    conn,
    seeds: Sequence[str],
    *,
    headless: bool = False,
    max_pages: int = 50,
    skip_mark_missing: bool = False,
    verbose: bool = False,
    include_festival_hubs_from_db: bool = False,
) -> dict[str, Any]:
    """
    Run a single discovery pass into an open DB connection.
    Normalizes and de-duplicates `seeds`. Optionally skips mark_missing (for multi-seed stress tests).
    """
    setup_logging(verbose)
    norm_seeds: list[str] = []
    seen_seed: set[str] = set()
    for s in seeds:
        n = normalize_url(s)
        if n and n not in seen_seed:
            seen_seed.add(n)
            norm_seeds.append(n)

    if include_festival_hubs_from_db:
        for hu in dbmod.list_active_festival_hub_urls(conn):
            n = normalize_url(hu)
            if n and n not in seen_seed:
                seen_seed.add(n)
                norm_seeds.append(n)

    run_id = dbmod.create_discovery_run(conn, seeds_json=safe_json(norm_seeds))
    LOGGER.info("Discovery run started. run_id=%s seeds=%s", run_id, len(norm_seeds))

    events_scanned = 0
    ticket_urls_seen = 0
    new_ticket_urls = 0
    updated_ticket_urls = 0
    parse_failures = 0
    seen_ticket_urls: set[str] = set()
    hub_coverage: list[dict[str, Any]] = []

    def ingest_discovered_tickets(tus: Sequence[str], fallback_event_url: str) -> int:
        nonlocal ticket_urls_seen, new_ticket_urls, updated_ticket_urls
        n = 0
        for tu0 in tus:
            tu = normalize_url(tu0)
            if not tu or not is_ticket_url(tu):
                continue
            event_url = normalize_url(event_url_from_ticket_url(tu) or fallback_event_url) or fallback_event_url
            event_id = event_id_from_event_url(event_url)
            slug, label = ticket_type_from_ticket_url(tu)
            dbmod.upsert_event(conn, event_id=event_id, event_url=event_url, event_name=None, raw={"source": "festival_scan"})
            was_new, was_updated, _ = dbmod.upsert_ticket_url(
                conn,
                ticket_url=tu,
                event_id=event_id,
                event_url=event_url,
                ticket_type_slug=slug,
                ticket_type_label=label,
                discovery_method="href_or_script",
                discovery_run_id=run_id,
            )
            ticket_urls_seen += 1
            seen_ticket_urls.add(tu)
            new_ticket_urls += 1 if was_new else 0
            updated_ticket_urls += 1 if was_updated else 0
            n += 1
        return n

    # 1) Always ingest deep ticket URL seeds directly (pipeline-first).
    for s in norm_seeds:
        if not is_ticket_url(s):
            continue
        event_url = event_url_from_ticket_url(s)
        if not event_url:
            continue
        event_id = event_id_from_event_url(event_url)
        slug, label = ticket_type_from_ticket_url(s)
        dbmod.upsert_event(conn, event_id=event_id, event_url=event_url, event_name=None, raw={"source": "seed_ticket_url"})
        was_new, was_updated, _ = dbmod.upsert_ticket_url(
            conn,
            ticket_url=s,
            event_id=event_id,
            event_url=event_url,
            ticket_type_slug=slug,
            ticket_type_label=label,
            discovery_method="seed_ticket_url",
            discovery_run_id=run_id,
        )
        ticket_urls_seen += 1
        seen_ticket_urls.add(s)
        new_ticket_urls += 1 if was_new else 0
        updated_ticket_urls += 1 if was_updated else 0

    overview_urls = list(dict.fromkeys(s for s in norm_seeds if is_festival_overview_page(s)))
    festival_pages = list(dict.fromkeys(s for s in norm_seeds if is_festival_page(s)))
    extra_events = list(dict.fromkeys(s for s in norm_seeds if is_event_page(s) and not is_festival_page(s)))[:35]
    max_event_follow = int(config.DISCOVERY_MAX_EVENT_PAGES_PER_HUB)

    driver: Optional[uc.Chrome] = None
    try:
        if festival_pages or extra_events or overview_urls:
            driver = new_driver(headless=bool(headless))

        if overview_urls and driver:
            merged: list[str] = list(festival_pages)
            seen_hub: set[str] = set(merged)
            for ou in overview_urls:
                try:
                    found = gather_festival_overview_hub_urls(driver, ou, headless=bool(headless))
                except Exception:
                    LOGGER.exception("Overview hub gather failed for %s", ou)
                    found = set()
                for h in sorted(found):
                    if h not in seen_hub:
                        seen_hub.add(h)
                        merged.append(h)
            festival_pages = merged

        festival_pages = festival_pages[: max(0, int(max_pages))]

        for hub_url in festival_pages:
            slug = hub_slug_from_festival_hub_url(hub_url)
            cover: dict[str, Any] = {
                "hub_url": hub_url,
                "hub_slug": slug,
                "events_found": 0,
                "deep_ticket_urls_found": 0,
                "pages_blocked": 0,
                "parse_failures": 0,
            }
            assert driver is not None
            events_scanned += 1
            driver.get(hub_url)
            html = wait_for_page_content(driver, headless=bool(headless))
            if is_blocked_for_discovery(html):
                cover["pages_blocked"] += 1
                cover["parse_failures"] += 1
                parse_failures += 1
                hub_coverage.append(cover)
                if slug:
                    dbmod.update_festival_hub_discovery_stats(
                        conn,
                        hub_slug=slug,
                        events_found=0,
                        deep_ticket_urls_found=0,
                        pages_blocked=cover["pages_blocked"],
                        parse_failures=cover["parse_failures"],
                    )
                continue

            scroll_for_lazy_content(driver)
            # Hub pages should be handled quickly; then follow discovered event pages.
            candidates = gather_hub_page_candidates(driver, hub_url)
            event_pages = list(dict.fromkeys(c for c in candidates if is_plausible_event_page(c)))
            ticket_urls = [c for c in candidates if is_ticket_url(c)]
            cover["events_found"] = len(event_pages)
            deep_here = ingest_discovered_tickets(ticket_urls, hub_url)
            cover["deep_ticket_urls_found"] += deep_here

            followed_events: set[str] = set()
            for ev_url in event_pages[:max_event_follow]:
                if len(followed_events) >= max_event_follow:
                    break
                ev_n = normalize_url(ev_url)
                if not ev_n or ev_n in followed_events or not is_event_page(ev_n):
                    continue
                followed_events.add(ev_n)
                events_scanned += 1
                driver.get(ev_n)
                html_ev = wait_for_page_content(driver, headless=bool(headless))
                if is_blocked_for_discovery(html_ev):
                    cover["pages_blocked"] += 1
                    cover["parse_failures"] += 1
                    parse_failures += 1
                    continue
                ev_candidates = reveal_event_page_deep_links(driver, ev_n)
                # If we still can't see deep ticket URLs but event links exist, capture artifacts for analysis.
                if (
                    cover.get("deep_ticket_urls_found", 0) == 0
                    and not any(is_ticket_url(c) for c in ev_candidates)
                    and not bool(headless)
                ):
                    _save_discovery_debug(
                        driver,
                        label=f"no_deep_links_{slug or 'hub'}",
                        url=ev_n,
                        html=driver.page_source or html_ev or "",
                    )
                n_ev = ingest_discovered_tickets([c for c in ev_candidates if is_ticket_url(c)], ev_n)
                cover["deep_ticket_urls_found"] += n_ev
                time.sleep(0.25 + random.random() * 0.45)

            hub_coverage.append(cover)
            if slug:
                dbmod.update_festival_hub_discovery_stats(
                    conn,
                    hub_slug=slug,
                    events_found=int(cover["events_found"]),
                    deep_ticket_urls_found=int(cover["deep_ticket_urls_found"]),
                    pages_blocked=int(cover["pages_blocked"]),
                    parse_failures=int(cover["parse_failures"]),
                )
            time.sleep(0.25 + random.random() * 0.45)

        for page_url in extra_events:
            assert driver is not None
            events_scanned += 1
            driver.get(page_url)
            html = wait_for_page_content(driver, headless=bool(headless))
            if is_blocked_for_discovery(html):
                parse_failures += 1
                continue
            # Treat the seed event page like any other event page: reveal deep links via expansion/clicks.
            candidates = reveal_event_page_deep_links(driver, page_url)
            event_pages = list(dict.fromkeys(c for c in candidates if is_plausible_event_page(c)))
            ingest_discovered_tickets([c for c in candidates if is_ticket_url(c)], page_url)
            followed_events: set[str] = set()
            for ev_url in event_pages[:max_event_follow]:
                ev_n = normalize_url(ev_url)
                if not ev_n or ev_n in followed_events or not is_event_page(ev_n):
                    continue
                followed_events.add(ev_n)
                events_scanned += 1
                driver.get(ev_n)
                html_ev = wait_for_page_content(driver, headless=bool(headless))
                if is_blocked_for_discovery(html_ev):
                    parse_failures += 1
                    continue
                ev_candidates = reveal_event_page_deep_links(driver, ev_n)
                ingest_discovered_tickets([c for c in ev_candidates if is_ticket_url(c)], ev_n)
                time.sleep(0.25 + random.random() * 0.45)
            time.sleep(0.2 + random.random() * 0.35)
    finally:
        if driver is not None:
            with contextlib.suppress(Exception):
                driver.quit()

    if skip_mark_missing:
        newly_inactivated = 0
    else:
        newly_inactivated = dbmod.mark_missing_ticket_urls(
            conn, seen_urls=seen_ticket_urls, missing_runs_threshold=int(config.MISSING_RUNS_THRESHOLD)
        )

    notes = f"newly_inactivated={newly_inactivated}; hub_rows={len(hub_coverage)}"
    dbmod.finish_discovery_run(
        conn,
        run_id,
        status="ok",
        events_scanned=events_scanned,
        ticket_urls_seen=ticket_urls_seen,
        new_ticket_urls=new_ticket_urls,
        updated_ticket_urls=updated_ticket_urls,
        parse_failures=parse_failures,
        notes=notes,
    )

    return {
        "run_id": run_id,
        "events_scanned": events_scanned,
        "ticket_urls_seen": ticket_urls_seen,
        "new_ticket_urls": new_ticket_urls,
        "updated_ticket_urls": updated_ticket_urls,
        "parse_failures": parse_failures,
        "newly_inactivated": newly_inactivated,
        "norm_seeds": norm_seeds,
        "hub_coverage": hub_coverage,
    }


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)

    seeds = ([] if args.no_config_seeds else list(config.SEED_URLS)) + list(args.seed or [])
    norm_seeds: list[str] = []
    seen: set[str] = set()
    for s in seeds:
        n = normalize_url(s)
        if n and n not in seen:
            seen.add(n)
            norm_seeds.append(n)

    conn = dbmod.connect(config.DB_PATH)
    try:
        dbmod.init_db(conn)
        stats = run_discovery(
            conn,
            norm_seeds,
            headless=bool(args.headless),
            max_pages=int(args.max_pages),
            skip_mark_missing=False,
            verbose=bool(args.verbose),
            include_festival_hubs_from_db=bool(args.from_hubs_table),
        )
        print("")
        print("=== Discovery summary ===")
        print(f"run_id: {stats['run_id']}")
        print(f"pages scanned: {stats['events_scanned']}")
        print(f"ticket URLs seen: {stats['ticket_urls_seen']}")
        print(f"new ticket URLs: {stats['new_ticket_urls']}")
        print(f"updated ticket URLs: {stats['updated_ticket_urls']}")
        print(f"parse failures: {stats['parse_failures']}")
        if stats.get("hub_coverage"):
            print("")
            print("Per hub (events / deep URLs / blocked / parse_fail):")
            for h in stats["hub_coverage"][:40]:
                print(
                    f"  {h.get('hub_slug') or '?'}: events={h.get('events_found')} "
                    f"deep={h.get('deep_ticket_urls_found')} blocked={h.get('pages_blocked')} "
                    f"pf={h.get('parse_failures')}"
                )
            if len(stats["hub_coverage"]) > 40:
                print(f"  ... ({len(stats['hub_coverage'])} hubs total)")
        print(f"db: {config.DB_PATH}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    import sys

    raise SystemExit(main(sys.argv[1:]))

