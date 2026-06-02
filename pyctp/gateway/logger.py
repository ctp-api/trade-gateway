from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class LoggerConfig:
    level: str = "INFO"
    name: str = "pyctp"
    console: bool = True


class GatewayLogger:
    def __init__(self, name: str = "pyctp") -> None:
        self._logger = logging.getLogger(name)

    def configure(self, level: str = "INFO") -> None:
        logging.basicConfig(
            level=getattr(logging, level.upper(), logging.INFO),
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.error(msg, *args, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.exception(msg, *args, **kwargs)


_default_logger = GatewayLogger()


def configure(level: str = "INFO") -> None:
    _default_logger.configure(level)


def debug(msg: str, *args: Any, **kwargs: Any) -> None:
    _default_logger.debug(msg, *args, **kwargs)


def info(msg: str, *args: Any, **kwargs: Any) -> None:
    _default_logger.info(msg, *args, **kwargs)


def warn(msg: str, *args: Any, **kwargs: Any) -> None:
    _default_logger.warning(msg, *args, **kwargs)


def warning(msg: str, *args: Any, **kwargs: Any) -> None:
    _default_logger.warning(msg, *args, **kwargs)


def error(msg: str, *args: Any, **kwargs: Any) -> None:
    _default_logger.error(msg, *args, **kwargs)


def exception(msg: str, *args: Any, **kwargs: Any) -> None:
    _default_logger.exception(msg, *args, **kwargs)
