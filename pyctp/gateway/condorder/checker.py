from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .types import CondOrder, CondOrderConditionType


@dataclass(slots=True)
class MarketTick:
    instrument_id: str
    exchange_id: str
    last_price: float
    open_price: float = 0.0
    pre_close_price: float = 0.0
    volume: int = 0
    timestamp: float = 0.0
    extra: dict[str, Any] | None = None


class CondOrderChecker:
    def should_trigger(self, order: CondOrder, tick: MarketTick) -> bool:
        condition = order.condition
        if condition.instrument_id and condition.instrument_id != tick.instrument_id:
            return False
        if condition.exchange_id and condition.exchange_id != tick.exchange_id:
            return False
        if condition.condition_type == CondOrderConditionType.PRICE_ABOVE:
            return tick.last_price >= condition.threshold
        if condition.condition_type == CondOrderConditionType.PRICE_BELOW:
            return tick.last_price <= condition.threshold
        if condition.condition_type == CondOrderConditionType.CHANGE_PCT_ABOVE:
            base = tick.pre_close_price or tick.open_price or tick.last_price
            if base <= 0:
                return False
            return ((tick.last_price - base) / base) * 100.0 >= condition.threshold
        if condition.condition_type == CondOrderConditionType.CHANGE_PCT_BELOW:
            base = tick.pre_close_price or tick.open_price or tick.last_price
            if base <= 0:
                return False
            return ((tick.last_price - base) / base) * 100.0 <= condition.threshold
        return False
