from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from uuid import uuid4


class CondOrderStatus(str, Enum):
    PENDING = "pending"
    ACTIVE = "active"
    PAUSED = "paused"
    TRIGGERED = "triggered"
    CANCELED = "canceled"
    FAILED = "failed"
    COMPLETED = "completed"


class CondOrderActionType(str, Enum):
    BUY = "buy"
    SELL = "sell"


class CondOrderConditionType(str, Enum):
    PRICE_ABOVE = "price_above"
    PRICE_BELOW = "price_below"
    CHANGE_PCT_ABOVE = "change_pct_above"
    CHANGE_PCT_BELOW = "change_pct_below"


@dataclass(slots=True)
class CondOrderCondition:
    condition_type: CondOrderConditionType
    threshold: float
    exchange_id: str = ""
    instrument_id: str = ""
    enabled: bool = True
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CondOrderAction:
    action_type: CondOrderActionType
    price: float
    volume: int
    exchange_id: str
    instrument_id: str
    direction: str = ""
    offset: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CondOrder:
    name: str
    condition: CondOrderCondition
    action: CondOrderAction
    owner: str = ""
    status: CondOrderStatus = CondOrderStatus.PENDING
    cond_id: str = field(default_factory=lambda: uuid4().hex)
    created_at: float = 0.0
    updated_at: float = 0.0
    triggered_at: float = 0.0
    last_error: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CondOrderHistory:
    cond_id: str
    status: CondOrderStatus
    message: str = ""
    timestamp: float = 0.0
    data: dict[str, Any] = field(default_factory=dict)
