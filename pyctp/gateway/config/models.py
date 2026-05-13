from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class AppConfig:
    host: str = "0.0.0.0"
    port: int = 7788
    market_host: str = "0.0.0.0"
    market_port: int = 7789
    log_level: str = "INFO"
    data_dir: Path = field(default_factory=lambda: Path("./data"))
    ctp_appid: str = "simnow_client_test"
    ctp_auth_code: str = "0000000000000000"
