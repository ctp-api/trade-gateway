from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pyctp.gateway.notify import GatewayNotify, GatewayNotifyType


class TraderLoginStage(str):
    CONNECTING = "CONNECTING"
    AUTHENTICATING = "AUTHENTICATING"
    AUTHENTICATE_RSP = "AUTHENTICATE_RSP"
    AUTHENTICATE_OK = "AUTHENTICATE_OK"
    LOGIN_REQUEST_SENT = "LOGIN_REQUEST_SENT"
    USER_LOGIN_RSP = "USER_LOGIN_RSP"
    SETTLEMENT_QUERYING = "SETTLEMENT_QUERYING"
    CONFIRMING_SETTLEMENT = "CONFIRMING_SETTLEMENT"
    READY = "READY"
    CONNECT_FAILED = "CONNECT_FAILED"
    AUTHENTICATE_FAILED = "AUTHENTICATE_FAILED"
    LOGIN_FAILED = "LOGIN_FAILED"
    SETTLEMENT_CONFIRM_FAILED = "SETTLEMENT_CONFIRM_FAILED"


TraderNotifyType = GatewayNotifyType


@dataclass(slots=True)
class TraderNotify:
    msg: str
    msg_type: TraderNotifyType | str
    code: int = 0
    level: str = "INFO"
    data: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return GatewayNotify(msg=self.msg, msg_type=self.msg_type, code=self.code, level=self.level, data=self.data).to_payload()

    @staticmethod
    def build_payload(
        msg: str,
        msg_type: TraderNotifyType | str,
        *,
        code: int = 0,
        level: str = "INFO",
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return GatewayNotify(msg=msg, msg_type=msg_type, code=code, level=level, data=data or {}).to_payload()
