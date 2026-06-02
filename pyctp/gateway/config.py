from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class BrokerConfig:
    name: str = ""
    type: str = ""
    is_fens: bool = False
    broker_id: str = ""
    trading_fronts: list[str] = field(default_factory=list)
    app_id: str = ""
    product_info: str = ""
    auth_code: str = ""


@dataclass(slots=True)
class LogConfig:
    level: str = "INFO"
    filename: str = ""
    console: bool = True


@dataclass(slots=True)
class CTPConfig:
    flow_path: str = "./data/"
    use_dynamic_lib: bool = False
    dynamic_lib_path: str = ""


@dataclass(slots=True)
class TqMarketFeedConfig:
    url: str = "wss://openmd.shinnytech.com/t/md/front/mobile"
    token: str = ""


@dataclass(slots=True)
class CtpMarketFeedConfig:
    front_addr: str = ""
    broker_id: str = ""
    user_id: str = ""
    password: str = ""
    flow_path: str = "./ctpmd_flow/"


@dataclass(slots=True)
class MarketFeedConfig:
    type: str = "tq"
    symbols: list[str] = field(default_factory=list)
    tq: TqMarketFeedConfig = field(default_factory=TqMarketFeedConfig)
    ctp: CtpMarketFeedConfig = field(default_factory=CtpMarketFeedConfig)


@dataclass(slots=True)
class ConditionOrderConfig:
    enabled: bool = False
    data_path: str = "./data/condition_orders"
    max_new_orders_per_day: int = 100
    max_valid_orders_total: int = 500


@dataclass(slots=True)
class Config:
    host: str = "0.0.0.0"
    port: int = 7788
    user_file_path: str = ""
    auto_confirm_settlement: bool = True
    log_price_info: bool = False
    brokers: dict[str, BrokerConfig] = field(default_factory=dict)
    broker_list_str: str = ""
    trading_day: str = ""
    log: LogConfig = field(default_factory=LogConfig)
    ctp: CTPConfig = field(default_factory=CTPConfig)
    marketfeed: MarketFeedConfig = field(default_factory=MarketFeedConfig)
    condition_order: ConditionOrderConfig = field(default_factory=ConditionOrderConfig)


_GLOBAL_CONFIG: Config | None = None


def get_config() -> Config | None:
    return _GLOBAL_CONFIG


def set_config(config: Config) -> None:
    global _GLOBAL_CONFIG
    _GLOBAL_CONFIG = config


def load(path: str | Path | None = None) -> Config:
    config_path = Path(path) if path else None
    data: dict[str, Any] = {}
    if config_path is not None:
        data = _load_from_file(config_path)
    else:
        for candidate in (Path("./config/config.json"), Path("./config/config.toml")):
            if candidate.exists():
                data = _load_from_file(candidate)
                break

    cfg = _build_config(data)
    set_config(cfg)
    return cfg


def load_from_dir(config_dir: str | Path) -> Config:
    base = Path(config_dir)
    for candidate in (base / "config.json", base / "config.toml"):
        if candidate.exists():
            cfg = _build_config(_load_from_file(candidate))
            set_config(cfg)
            return cfg
    raise FileNotFoundError(f"no config.json or config.toml found in {base}")


def get_broker(name: str) -> BrokerConfig | None:
    cfg = get_config()
    if cfg is None:
        return None
    return cfg.brokers.get(name)


def _load_from_file(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        data = json.loads(text)
    elif path.suffix.lower() == ".toml":
        try:
            import tomllib
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise RuntimeError("tomllib is required to load TOML config") from exc
        data = tomllib.loads(text)
    else:
        raise ValueError(f"unsupported config format: {path.suffix}")
    if not isinstance(data, dict):
        raise ValueError("config root must be a JSON/TOML object")
    return data


def _build_config(data: dict[str, Any]) -> Config:
    return Config(
        host=str(data.get("host", "0.0.0.0")),
        port=int(data.get("port", 7788)),
        user_file_path=str(data.get("user_file_path", "")),
        auto_confirm_settlement=bool(data.get("auto_confirm_settlement", True)),
        log_price_info=bool(data.get("log_price_info", False)),
        brokers={k: _build_broker(v) for k, v in _as_dict(data.get("brokers", {})).items()},
        broker_list_str=str(data.get("broker_list_str", "")),
        trading_day=str(data.get("trading_day", "")),
        log=_build_log(data.get("log", {})),
        ctp=_build_ctp(data.get("ctp", {})),
        marketfeed=_build_marketfeed(data.get("marketfeed", {})),
        condition_order=_build_condition_order(data.get("condition_order", {})),
    )


def _build_broker(data: Any) -> BrokerConfig:
    payload = _as_dict(data)
    return BrokerConfig(
        name=str(payload.get("name", "")),
        type=str(payload.get("type", "")),
        is_fens=bool(payload.get("is_fens", False)),
        broker_id=str(payload.get("broker_id", "")),
        trading_fronts=[str(item) for item in payload.get("trading_fronts", []) if item is not None],
        app_id=str(payload.get("app_id", "")),
        product_info=str(payload.get("product_info", "")),
        auth_code=str(payload.get("auth_code", "")),
    )


def _build_log(data: Any) -> LogConfig:
    payload = _as_dict(data)
    return LogConfig(
        level=str(payload.get("level", "INFO")),
        filename=str(payload.get("filename", "")),
        console=bool(payload.get("console", True)),
    )


def _build_ctp(data: Any) -> CTPConfig:
    payload = _as_dict(data)
    return CTPConfig(
        flow_path=str(payload.get("flow_path", "./data/")),
        use_dynamic_lib=bool(payload.get("use_dynamic_lib", False)),
        dynamic_lib_path=str(payload.get("dynamic_lib_path", "")),
    )


def _build_marketfeed(data: Any) -> MarketFeedConfig:
    payload = _as_dict(data)
    tq = _as_dict(payload.get("tq", {}))
    ctp = _as_dict(payload.get("ctp", {}))
    return MarketFeedConfig(
        type=str(payload.get("type", "tq")),
        symbols=[str(item) for item in payload.get("symbols", []) if item is not None],
        tq=TqMarketFeedConfig(
            url=str(tq.get("url", "wss://openmd.shinnytech.com/t/md/front/mobile")),
            token=str(tq.get("token", "")),
        ),
        ctp=CtpMarketFeedConfig(
            front_addr=str(ctp.get("front_addr", "")),
            broker_id=str(ctp.get("broker_id", "")),
            user_id=str(ctp.get("user_id", "")),
            password=str(ctp.get("password", "")),
            flow_path=str(ctp.get("flow_path", "./ctpmd_flow/")),
        ),
    )


def _build_condition_order(data: Any) -> ConditionOrderConfig:
    payload = _as_dict(data)
    return ConditionOrderConfig(
        enabled=bool(payload.get("enabled", False)),
        data_path=str(payload.get("data_path", "./data/condition_orders")),
        max_new_orders_per_day=int(payload.get("max_new_orders_per_day", 100)),
        max_valid_orders_total=int(payload.get("max_valid_orders_total", 500)),
    )


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}
