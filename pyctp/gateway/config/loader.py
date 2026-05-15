from __future__ import annotations

import json
from pathlib import Path

from pyctp.gateway.config.models import AppConfig


def load_config(path: Path) -> AppConfig:
    if not path.exists():
        return AppConfig()

    data = json.loads(path.read_text(encoding="utf-8"))
    trader = data.get("trader", {}) if isinstance(data.get("trader", {}), dict) else {}
    market = data.get("market", {}) if isinstance(data.get("market", {}), dict) else {}

    host = trader.get("host", data.get("host", "0.0.0.0"))
    port = trader.get("port", data.get("port", 7788))
    market_host = market.get("host", data.get("market_host", host))
    market_port = market.get("port", data.get("market_port", 7789))
    log_level = data.get("log_level", data.get("log", {}).get("level", "INFO"))
    data_dir = data.get("data_dir", data.get("user_file_path", "./data"))
    ctp_appid = data.get("ctp_appid", data.get("ctp", {}).get("appid", "simnow_client_test"))
    ctp_auth_code = data.get("ctp_auth_code", data.get("ctp", {}).get("auth_code", "0000000000000000"))

    return AppConfig(
        host=str(host),
        port=int(port),
        market_host=str(market_host),
        market_port=int(market_port),
        log_level=str(log_level),
        data_dir=Path(data_dir),
        ctp_appid=str(ctp_appid),
        ctp_auth_code=str(ctp_auth_code),
    )
