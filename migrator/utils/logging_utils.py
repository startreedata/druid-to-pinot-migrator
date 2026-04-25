from __future__ import annotations

import logging

try:
    from rich.logging import RichHandler

    _RICH_AVAILABLE = True
except ImportError:
    _RICH_AVAILABLE = False


def get_logger(name: str = "migrator") -> logging.Logger:
    """Return a configured logger for the given name."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        if _RICH_AVAILABLE:
            handler = RichHandler(show_path=False, markup=True)
        else:
            handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
