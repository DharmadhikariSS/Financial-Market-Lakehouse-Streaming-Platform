"""Structured logger for the Data Engineering Platform."""

import logging
import sys

try:
    from rich.logging import RichHandler

    HAS_RICH = True
except ImportError:
    HAS_RICH = False


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Returns a pre-configured logger with rich or standard formatting."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    handler: logging.Handler
    if HAS_RICH:
        handler = RichHandler(
            rich_tracebacks=True,
            show_time=True,
            show_level=True,
            show_path=False,
        )
    else:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.propagate = False
    return logger
