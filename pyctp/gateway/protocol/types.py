from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class WsRequest:
    aid: str
    conn_id: int | None = None
    request_id: int | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class WsResponse:
    aid: str
    ok: bool = True
    code: int = 0
    msg: str = "ok"
    data: dict[str, Any] = field(default_factory=dict)
    request_id: int | None = None
    conn_id: int | None = None


@dataclass(slots=True)
class LoginRequest:
    user_name: str
    password: str
    broker_id: str | None = None
    front: str | None = None
    auth_code: str = ""
    appid: str = ""


@dataclass(slots=True)
class MarketLoginRequest:
    user_name: str
    password: str
    broker_id: str | None = None
    front: str | None = None
    auth_code: str = ""
    appid: str = ""


@dataclass(slots=True)
class InsertOrderRequest:
    instrument_id: str
    exchange_id: str
    direction: str
    offset: str
    volume: int
    price: float
    price_type: str = "limit"
    close_today_prior: bool = False


@dataclass(slots=True)
class CancelOrderRequest:
    order_id: str
    exchange_id: str = ""
    instrument_id: str = ""


@dataclass(slots=True)
class AccountData:
    account_id: str
    pre_balance: float = 0.0
    deposit: float = 0.0
    withdraw: float = 0.0
    close_profit: float = 0.0
    position_profit: float = 0.0
    balance: float = 0.0
    available: float = 0.0
    frozen_margin: float = 0.0
    frozen_cash: float = 0.0
    commission: float = 0.0
    currency: str = "CNY"
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PositionData:
    symbol: str
    exchange_id: str
    direction: str
    volume: int = 0
    yd_volume: int = 0
    today_volume: int = 0
    frozen: int = 0
    price: float = 0.0
    position_cost: float = 0.0
    profit: float = 0.0
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class OrderData:
    order_ref: str
    symbol: str
    exchange_id: str
    direction: str
    offset: str
    price: float = 0.0
    volume: int = 0
    traded_volume: int = 0
    status: str = ""
    order_sys_id: str = ""
    front_id: int = 0
    session_id: int = 0
    status_msg: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TradeData:
    trade_id: str
    order_ref: str
    symbol: str
    exchange_id: str
    direction: str
    offset: str
    price: float = 0.0
    volume: int = 0
    trade_time: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class InstrumentData:
    instrument_id: str
    exchange_id: str
    product_class: str = ""
    volume_multiple: int = 1
    price_tick: float = 0.0
    raw: dict[str, Any] = field(default_factory=dict)
