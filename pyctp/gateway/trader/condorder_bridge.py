from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyctp.gateway.condorder import CondOrder, CondOrderManager
from pyctp.gateway.condorder.checker import MarketTick
from pyctp.gateway.trader.notify import TraderNotifyType


@dataclass(slots=True)
class TraderCondOrderBridge:
    manager: CondOrderManager
    notify: Any

    def on_tick(self, tick: MarketTick) -> list[CondOrder]:
        triggered = self.manager.on_tick(tick)
        for order in triggered:
            self.notify(
                "condition order triggered",
                msg_type=TraderNotifyType.NOTIFY,
                data={"cond_id": order.cond_id, "name": order.name, "instrument_id": order.condition.instrument_id},
            )
        return triggered
