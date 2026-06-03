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
