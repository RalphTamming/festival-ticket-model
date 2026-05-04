"""Tests for VPS18 shared-driver dead-session heuristics."""

from pipeline.mode_runner import _is_selenium_driver_dead


def test_is_selenium_driver_dead_connection_refused() -> None:
    assert _is_selenium_driver_dead(
        RuntimeError(
            'HTTPConnection(host=\'localhost\', port=39093): Failed to establish a new connection: [Errno 111] Connection refused'
        )
    )


def test_is_selenium_driver_dead_invalid_session() -> None:
    assert _is_selenium_driver_dead(Exception("invalid session id"))


def test_is_selenium_driver_dead_chrome_not_reachable() -> None:
    assert _is_selenium_driver_dead(Exception("chrome not reachable"))


def test_is_selenium_driver_dead_unrelated() -> None:
    assert not _is_selenium_driver_dead(ValueError("no fresh urls"))
