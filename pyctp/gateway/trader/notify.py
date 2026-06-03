from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pyctp.gateway.notify import GatewayNotify, GatewayNotifyType


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
