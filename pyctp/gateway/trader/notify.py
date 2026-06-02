from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class TraderNotifyType(str, Enum):
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
    NOTIFY = "NOTIFY"


@dataclass(slots=True)
class TraderNotify:
    msg: str
    msg_type: TraderNotifyType
    code: int = 0
    level: str = "INFO"
    data: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return self.build_payload(self.msg, self.msg_type, code=self.code, level=self.level, data=self.data)

    @staticmethod
    def build_payload(
        msg: str,
        msg_type: TraderNotifyType | str,
        *,
        code: int = 0,
        level: str = "INFO",
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        notify_type = msg_type if isinstance(msg_type, TraderNotifyType) else TraderNotifyType(str(msg_type))
        return {
            "aid": "notify",
            "ok": True,
            "code": code,
            "msg": msg,
            "level": level,
            "msg_type": notify_type.value,
            "data": data or {},
        }
