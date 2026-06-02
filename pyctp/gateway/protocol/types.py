from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class MessageAid(str, Enum):
    REQ_LOGIN = "req_login"
    CHANGE_PASSWORD = "change_password"
    PEEK_MESSAGE = "peek_message"
    INSERT_ORDER = "insert_order"
    CANCEL_ORDER = "cancel_order"
    REQ_TRANSFER = "req_transfer"
    CONFIRM_SETTLEMENT = "confirm_settlement"
    QRY_SETTLEMENT_INFO = "qry_settlement_info"
    REQ_RECONNECT_TRADE = "req_reconnect_trade"
    QRY_TRANSFER_SERIAL = "qry_transfer_serial"
    QRY_ACCOUNT_INFO = "qry_account_info"
    QRY_ACCOUNT_REGISTER = "qry_account_register"
    CHANGE_TRADING_ACCOUNT_PASSWORD = "change_trading_account_password"
    REQ_START_CTP = "req_start_ctp"
    REQ_STOP_CTP = "req_stop_ctp"
    INSERT_CONDITION_ORDER = "insert_condition_order"
    CANCEL_CONDITION_ORDER = "cancel_condition_order"
    PAUSE_CONDITION_ORDER = "pause_condition_order"
    RESUME_CONDITION_ORDER = "resume_condition_order"
    QRY_CONDITION_ORDER = "qry_condition_order"
    QRY_HIS_CONDITION_ORDER = "qry_his_condition_order"
    RTN_DATA = "rtn_data"
    RTN_BROKERS = "rtn_brokers"
    NOTIFY = "notify"


class Direction(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    UNKNOWN = "UNKNOWN"


class Offset(str, Enum):
    OPEN = "OPEN"
    CLOSE = "CLOSE"
    CLOSE_TODAY = "CLOSETODAY"
    CLOSE_YESTERDAY = "CLOSEYESTERDAY"
    UNKNOWN = "UNKNOWN"


class PriceType(str, Enum):
    LIMIT = "LIMIT"
    ANY = "ANY"
    BEST = "BEST"
    FIVELEVEL = "FIVELEVEL"
    UNKNOWN = "UNKNOWN"


class VolumeCondition(str, Enum):
    ANY = "ANY"
    MIN = "MIN"
    ALL = "ALL"
    UNKNOWN = "UNKNOWN"


class TimeCondition(str, Enum):
    IOC = "IOC"
    GFS = "GFS"
    GFD = "GFD"
    GTD = "GTD"
    GTC = "GTC"
    GFA = "GFA"
    UNKNOWN = "UNKNOWN"


class HedgeFlag(str, Enum):
    SPECULATION = "SPECULATION"
    ARBITRAGE = "ARBITRAGE"
    HEDGE = "HEDGE"
    MARKETMAKER = "MARKETMAKER"
    UNKNOWN = "UNKNOWN"


class ContingentConditionType(str, Enum):
    IMMEDIATELY = "IMMEDIATELY"
    TOUCH = "TOUCH"
    TOUCH_PROFIT = "TOUCHPROFIT"
    UNKNOWN = "UNKNOWN"


class MessageLevel(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


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
    direction: Direction | str
    offset: Offset | str
    volume: int
    price: float
    price_type: PriceType | str = PriceType.LIMIT
    volume_condition: VolumeCondition | str = VolumeCondition.ANY
    time_condition: TimeCondition | str = TimeCondition.GFD
    hedge_flag: HedgeFlag | str = HedgeFlag.SPECULATION
    contingent_condition: ContingentConditionType | str = ContingentConditionType.IMMEDIATELY
    close_today_prior: bool = False
    order_id: str = ""
    user_id: str = ""
    limit_price: float = 0.0


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
    margin: float = 0.0
    frozen_margin: float = 0.0
    frozen_cash: float = 0.0
    frozen_commission: float = 0.0
    commission: float = 0.0
    currency: str = "CNY"
    risk_ratio: float = 0.0
    changed: bool = False
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class BankData:
    bank_id: str
    bank_name: str = ""
    account_id: str = ""
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
    open_cost: float = 0.0
    margin: float = 0.0
    profit: float = 0.0
    changed: bool = False
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
    changed: bool = False
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
    seq_no: int = 0
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TransferLogData:
    transfer_id: str
    bank_id: str = ""
    amount: float = 0.0
    status: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class InstrumentData:
    instrument_id: str
    exchange_id: str
    product_class: str = ""
    volume_multiple: int = 1
    price_tick: float = 0.0
    last_price: float = 0.0
    ask_price1: float = 0.0
    bid_price1: float = 0.0
    open_interest: float = 0.0
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class UserData:
    user_id: str
    trading_day: str = ""
    accounts: dict[str, AccountData] = field(default_factory=dict)
    positions: dict[str, PositionData] = field(default_factory=dict)
    orders: dict[str, OrderData] = field(default_factory=dict)
    trades: dict[str, TradeData] = field(default_factory=dict)
    transfers: dict[str, TransferLogData] = field(default_factory=dict)
    banks: dict[str, BankData] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class NotifyData:
    code: int
    msg: str
    level: MessageLevel | str = MessageLevel.INFO
    msg_type: str = "MESSAGE"


@dataclass(slots=True)
class RtnDataEnvelope:
    aid: str = MessageAid.RTN_DATA.value
    data: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class BrokerData:
    broker_name: str
    broker_id: str
    trading_fronts: list[str] = field(default_factory=list)
    app_id: str = ""
    product_info: str = ""
    auth_code: str = ""
    is_fens: bool = False
    broker_type: str = ""
    raw: dict[str, Any] = field(default_factory=dict)
