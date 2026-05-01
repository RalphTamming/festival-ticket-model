"""Root CLI wrapper for the pipeline package entrypoint."""

from __future__ import annotations

from pipeline.run_pipeline import main


if __name__ == "__main__":
    import sys

    raise SystemExit(main(sys.argv[1:]))
