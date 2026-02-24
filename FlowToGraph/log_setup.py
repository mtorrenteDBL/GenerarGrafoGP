#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
log_setup.py
============
Centralised logging setup for the FlowToGraph pipeline.

Each invocation creates a new timestamped log file under
    log/<YYYY-MM-DD_HHMMSS>_<name>.log
while simultaneously streaming to stdout.

Usage
-----
    from log_setup import setup_logging

    log_file = setup_logging(name="my_run", level=logging.INFO)
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent / "log"

_FMT = "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s"
_DATEFMT = "%H:%M:%S"


def setup_logging(name: str = "run", level: int = logging.INFO) -> Path:
    """
    Configure the root logger with two handlers:
      - StreamHandler  → stdout, at *level*
      - FileHandler    → log/<timestamp>_<name>.log, always at DEBUG

    Safe to call once per process. Returns the Path of the log file created.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    safe_name = "".join(c if c.isalnum() or c in "-_." else "_" for c in name)
    log_file = LOG_DIR / f"{timestamp}_{safe_name}.log"

    formatter = logging.Formatter(_FMT, datefmt=_DATEFMT)

    # File handler — always at DEBUG so nothing is lost on disk
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    # Console handler — respects the requested level
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)   # handlers decide what to show/write
    root.addHandler(fh)
    root.addHandler(ch)

    logging.getLogger(__name__).info("Logging to %s", log_file)
    return log_file
