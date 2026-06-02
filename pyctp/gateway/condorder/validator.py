from __future__ import annotations

from dataclasses import replace
from typing import Any

from .errors import CondOrderError, CondOrderErrorCode
from .types import (
    CondOrder,
    CondOrderAction,
    CondOrderActionType,
    CondOrderCondition,
    CondOrderConditionType,
    CondOrderStatus,
)


class CondOrderValidator:
    def validate(self, order: CondOrder) -> CondOrder:
        self.validate_condition(order.condition)
        self.validate_action(order.action)
        if not order.name.strip():
            raise CondOrderError(CondOrderErrorCode.INVALID, "condition order name is required")
        if order.status not in {CondOrderStatus.PENDING, CondOrderStatus.ACTIVE}:
            raise CondOrderError(CondOrderErrorCode.INVALID, f"invalid initial status: {order.status.value}")
        return order

    def validate_condition(self, condition: CondOrderCondition) -> CondOrderCondition:
        if condition.threshold <= 0:
            raise CondOrderError(CondOrderErrorCode.INVALID, "condition threshold must be positive")
        if condition.condition_type not in CondOrderConditionType:
            raise CondOrderError(CondOrderErrorCode.INVALID, f"unsupported condition type: {condition.condition_type}")
        if not condition.instrument_id.strip():
            raise CondOrderError(CondOrderErrorCode.INVALID, "condition instrument_id is required")
        if not condition.exchange_id.strip():
            raise CondOrderError(CondOrderErrorCode.INVALID, "condition exchange_id is required")
        return condition

    def validate_action(self, action: CondOrderAction) -> CondOrderAction:
        if action.volume <= 0:
            raise CondOrderError(CondOrderErrorCode.INVALID, "action volume must be positive")
        if action.price < 0:
            raise CondOrderError(CondOrderErrorCode.INVALID, "action price cannot be negative")
        if not action.instrument_id.strip():
            raise CondOrderError(CondOrderErrorCode.INVALID, "action instrument_id is required")
        if not action.exchange_id.strip():
            raise CondOrderError(CondOrderErrorCode.INVALID, "action exchange_id is required")
        if action.action_type not in CondOrderActionType:
            raise CondOrderError(CondOrderErrorCode.INVALID, f"unsupported action type: {action.action_type}")
        return action

    def normalize(self, order: CondOrder) -> CondOrder:
        condition = replace(order.condition, exchange_id=order.condition.exchange_id.upper(), instrument_id=order.condition.instrument_id.strip())
        action = replace(order.action, exchange_id=order.action.exchange_id.upper(), instrument_id=order.action.instrument_id.strip())
        return replace(order, condition=condition, action=action, name=order.name.strip())
