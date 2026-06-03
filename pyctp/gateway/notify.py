from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class GatewayNotifyType(str, Enum):
    MESSAGE = "MESSAGE"
    NOTIFY = "NOTIFY"
    STATE = "STATE"
    QUERY = "QUERY"
    ACCOUNT = "ACCOUNT"
    POSITION = "POSITION"
    ORDER = "ORDER"
    TRADE = "TRADE"
    TRADE_SUMMARY = "TRADE_SUMMARY"
    SETTLEMENT = "SETTLEMENT"
    TRADING_DAY = "TRADING_DAY"
    ERROR_SYSTEM = "ERROR.SYSTEM"
    ERROR_QUERY = "ERROR.QUERY"
    ERROR_ORDER = "ERROR.ORDER"
    ERROR_SETTLEMENT = "ERROR.SETTLEMENT"


class GatewayLoginStage(str, Enum):
    CONNECTING = "connecting"
    AUTHENTICATING = "authenticating"
    AUTHENTICATE_RSP = "authenticate_rsp"
    AUTHENTICATE_OK = "authenticate_ok"
    LOGIN_REQUEST_SENT = "login_request_sent"
    USER_LOGIN_RSP = "user_login_rsp"
    SETTLEMENT_QUERYING = "settlement_querying"
    CONFIRMING_SETTLEMENT = "confirming_settlement"
    READY = "ready"
    CONNECT_FAILED = "connect_failed"
    AUTHENTICATE_FAILED = "authenticate_failed"
    LOGIN_FAILED = "login_failed"
    SETTLEMENT_CONFIRM_FAILED = "settlement_confirm_failed"


@dataclass(slots=True)
class GatewayNotify:
    msg: str
    msg_type: GatewayNotifyType | str
    code: int = 0
    level: str = "INFO"
    data: dict[str, Any] = field(default_factory=dict)
    aid: str = "notify"
    ok: bool = True

    def to_payload(self) -> dict[str, Any]:
        notify_type = self.msg_type if isinstance(self.msg_type, GatewayNotifyType) else GatewayNotifyType(str(self.msg_type))
        return {
            "aid": self.aid,
            "ok": self.ok,
            "code": self.code,
            "msg": self.msg,
            "level": self.level,
            "msg_type": notify_type.value,
            "data": self.data,
        }
