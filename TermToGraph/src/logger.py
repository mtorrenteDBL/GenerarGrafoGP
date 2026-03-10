import logging
from .config import LOGGING_LEVEL

def setup_logger(name: str) -> logging.Logger:
    """Return a named logger that propagates to the root logger.
    Logging setup (handlers, format, level) is handled centrally by the
    root main.py orchestrator. When running standalone, a basic config
    is applied if the root logger has no handlers yet.
    """
    if not logging.root.handlers:
        # Standalone usage: apply minimal logging config
        logging.basicConfig(
            level=LOGGING_LEVEL,
            format='%(asctime)s [%(levelname)-8s] %(name)s: %(message)s',
        )

    return logging.getLogger(name)
