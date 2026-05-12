from __future__ import annotations

import json
from pathlib import Path

from pyctp.gateway.config.models import AppConfig


def load_config(path: Path) -> AppConfig:
    if not path.exists():
        return AppConfig()

    data = json.loads(path.read_text(encoding="utf-8"))
    return AppConfig(
        host=data.get("host", "0.0.0.0"),
        port=int(data.get("port", 7788)),
        log_level=str(data.get("log_level", "INFO")),
        data_dir=Path(data.get("data_dir", "./data")),
        ctp_appid=str(data.get("ctp_appid", "simnow_client_test")),
        ctp_auth_code=str(data.get("ctp_auth_code", "0000000000000000")),
    )
