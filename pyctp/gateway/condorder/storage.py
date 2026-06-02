from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .types import CondOrder, CondOrderAction, CondOrderActionType, CondOrderCondition, CondOrderConditionType, CondOrderHistory, CondOrderStatus


class CondOrderStorage:
    def __init__(self, storage_dir: str | Path) -> None:
        self._storage_dir = Path(storage_dir)
        self._file_path = self._storage_dir / "condorder.json"

    def save(self, orders: list[CondOrder], history: dict[str, list[CondOrderHistory]]) -> None:
        self._storage_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "orders": [self._serialize_order(order) for order in orders],
            "history": {cond_id: [self._serialize_history(item) for item in items] for cond_id, items in history.items()},
        }
        self._file_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def load(self) -> tuple[list[CondOrder], dict[str, list[CondOrderHistory]]]:
        if not self._file_path.exists():
            return [], {}
        payload = json.loads(self._file_path.read_text(encoding="utf-8"))
        orders = [self._deserialize_order(item) for item in payload.get("orders", [])]
        history = {
            cond_id: [self._deserialize_history(item) for item in items]
            for cond_id, items in (payload.get("history", {}) or {}).items()
        }
        return orders, history

    @staticmethod
    def _serialize_order(order: CondOrder) -> dict[str, Any]:
        return {
            "cond_id": order.cond_id,
            "name": order.name,
            "condition": {
                "condition_type": order.condition.condition_type.value,
                "threshold": order.condition.threshold,
                "exchange_id": order.condition.exchange_id,
                "instrument_id": order.condition.instrument_id,
                "enabled": order.condition.enabled,
                "extra": order.condition.extra,
            },
            "action": {
                "action_type": order.action.action_type.value,
                "price": order.action.price,
                "volume": order.action.volume,
                "exchange_id": order.action.exchange_id,
                "instrument_id": order.action.instrument_id,
                "direction": order.action.direction,
                "offset": order.action.offset,
                "extra": order.action.extra,
            },
            "owner": order.owner,
            "status": order.status.value,
            "created_at": order.created_at,
            "updated_at": order.updated_at,
            "triggered_at": order.triggered_at,
            "last_error": order.last_error,
            "extra": order.extra,
        }

    @staticmethod
    def _deserialize_order(item: dict[str, Any]) -> CondOrder:
        return CondOrder(
            cond_id=str(item.get("cond_id", "")),
            name=str(item.get("name", "")),
            condition=CondOrderCondition(
                condition_type=CondOrderConditionType(str((item.get("condition") or {}).get("condition_type", CondOrderConditionType.PRICE_ABOVE.value))),
                threshold=float((item.get("condition") or {}).get("threshold", 0.0) or 0.0),
                exchange_id=str((item.get("condition") or {}).get("exchange_id", "")),
                instrument_id=str((item.get("condition") or {}).get("instrument_id", "")),
                enabled=bool((item.get("condition") or {}).get("enabled", True)),
                extra=dict((item.get("condition") or {}).get("extra", {}) or {}),
            ),
            action=CondOrderAction(
                action_type=CondOrderActionType(str((item.get("action") or {}).get("action_type", CondOrderActionType.BUY.value))),
                price=float((item.get("action") or {}).get("price", 0.0) or 0.0),
                volume=int((item.get("action") or {}).get("volume", 0) or 0),
                exchange_id=str((item.get("action") or {}).get("exchange_id", "")),
                instrument_id=str((item.get("action") or {}).get("instrument_id", "")),
                direction=str((item.get("action") or {}).get("direction", "")),
                offset=str((item.get("action") or {}).get("offset", "")),
                extra=dict((item.get("action") or {}).get("extra", {}) or {}),
            ),
            owner=str(item.get("owner", "")),
            status=CondOrderStatus(str(item.get("status", CondOrderStatus.PENDING.value))),
            created_at=float(item.get("created_at", 0.0) or 0.0),
            updated_at=float(item.get("updated_at", 0.0) or 0.0),
            triggered_at=float(item.get("triggered_at", 0.0) or 0.0),
            last_error=str(item.get("last_error", "")),
            extra=dict(item.get("extra", {}) or {}),
        )

    @staticmethod
    def _serialize_history(item: CondOrderHistory) -> dict[str, Any]:
        return {
            "cond_id": item.cond_id,
            "status": item.status.value,
            "message": item.message,
            "timestamp": item.timestamp,
            "data": item.data,
        }

    @staticmethod
    def _deserialize_history(item: dict[str, Any]) -> CondOrderHistory:
        return CondOrderHistory(
            cond_id=str(item.get("cond_id", "")),
            status=CondOrderStatus(str(item.get("status", CondOrderStatus.PENDING.value))),
            message=str(item.get("message", "")),
            timestamp=float(item.get("timestamp", 0.0) or 0.0),
            data=dict(item.get("data", {}) or {}),
        )
