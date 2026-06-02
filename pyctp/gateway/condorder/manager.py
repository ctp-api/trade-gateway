from __future__ import annotations

from pathlib import Path
from time import time
from typing import Any, Callable

from .checker import CondOrderChecker, MarketTick
from .errors import CondOrderError, CondOrderErrorCode
from .index import CondOrderIndex
from .storage import CondOrderStorage
from .types import CondOrder, CondOrderHistory, CondOrderStatus
from .validator import CondOrderValidator


class CondOrderManager:
    def __init__(self, storage_dir: str | Path | None = None) -> None:
        self._orders: dict[str, CondOrder] = {}
        self._history: dict[str, list[CondOrderHistory]] = {}
        self._index = CondOrderIndex()
        self._storage = CondOrderStorage(storage_dir) if storage_dir is not None else None
        self._validator = CondOrderValidator()
        self._checker = CondOrderChecker()
        self._callbacks: list[Callable[[CondOrder, MarketTick], None]] = []

    def register_callback(self, callback: Callable[[CondOrder, MarketTick], None]) -> None:
        self._callbacks.append(callback)

    def unregister_callback(self, callback: Callable[[CondOrder, MarketTick], None]) -> None:
        self._callbacks = [item for item in self._callbacks if item is not callback]

    def _notify_callbacks(self, order: CondOrder, tick: MarketTick) -> None:
        for callback in self._callbacks:
            callback(order, tick)

    def create(self, order: CondOrder) -> CondOrder:
        order = self._validator.normalize(order)
        order = self._validator.validate(order)
        if order.cond_id in self._orders:
            raise CondOrderError(CondOrderErrorCode.DUPLICATE, f"condition order exists: {order.cond_id}")
        now = time()
        order.created_at = order.created_at or now
        order.updated_at = now
        order.status = CondOrderStatus.ACTIVE
        self._orders[order.cond_id] = order
        self._index.upsert(order)
        self._append_history(order.cond_id, CondOrderStatus.ACTIVE, "created", {"cond_id": order.cond_id})
        return order

    def get(self, cond_id: str) -> CondOrder:
        try:
            return self._orders[cond_id]
        except KeyError as exc:
            raise CondOrderError(CondOrderErrorCode.NOT_FOUND, f"condition order not found: {cond_id}") from exc

    def list(self) -> list[CondOrder]:
        return list(self._orders.values())

    def by_owner(self, owner: str) -> list[CondOrder]:
        return self._index.by_owner(owner)

    def by_instrument(self, instrument_id: str) -> list[CondOrder]:
        return self._index.by_instrument(instrument_id)

    def by_status(self, status: CondOrderStatus) -> list[CondOrder]:
        return self._index.by_status(status)

    def pause(self, cond_id: str) -> CondOrder:
        order = self.get(cond_id)
        if order.status not in {CondOrderStatus.ACTIVE, CondOrderStatus.PENDING}:
            raise CondOrderError(CondOrderErrorCode.NOT_ACTIVE, f"condition order not pausable: {order.status.value}")
        order.status = CondOrderStatus.PAUSED
        order.updated_at = time()
        self._index.update_status(cond_id, CondOrderStatus.PAUSED)
        self._append_history(cond_id, CondOrderStatus.PAUSED, "paused")
        return order

    def resume(self, cond_id: str) -> CondOrder:
        order = self.get(cond_id)
        if order.status != CondOrderStatus.PAUSED:
            raise CondOrderError(CondOrderErrorCode.NOT_ACTIVE, f"condition order not resumable: {order.status.value}")
        order.status = CondOrderStatus.ACTIVE
        order.updated_at = time()
        self._index.update_status(cond_id, CondOrderStatus.ACTIVE)
        self._append_history(cond_id, CondOrderStatus.ACTIVE, "resumed")
        return order

    def cancel(self, cond_id: str) -> CondOrder:
        order = self.get(cond_id)
        if order.status in {CondOrderStatus.CANCELED, CondOrderStatus.COMPLETED}:
            raise CondOrderError(CondOrderErrorCode.NOT_ACTIVE, f"condition order already finalized: {order.status.value}")
        order.status = CondOrderStatus.CANCELED
        order.updated_at = time()
        self._index.update_status(cond_id, CondOrderStatus.CANCELED)
        self._append_history(cond_id, CondOrderStatus.CANCELED, "canceled")
        return order

    def trigger(self, cond_id: str, context: dict[str, Any] | None = None) -> CondOrder:
        order = self.get(cond_id)
        if order.status != CondOrderStatus.ACTIVE:
            raise CondOrderError(CondOrderErrorCode.NOT_ACTIVE, f"condition order not triggerable: {order.status.value}")
        order.status = CondOrderStatus.TRIGGERED
        order.triggered_at = time()
        order.updated_at = order.triggered_at
        self._index.update_status(cond_id, CondOrderStatus.TRIGGERED)
        self._append_history(cond_id, CondOrderStatus.TRIGGERED, "triggered", context or {})
        return order

    def complete(self, cond_id: str, message: str = "completed", context: dict[str, Any] | None = None) -> CondOrder:
        order = self.get(cond_id)
        order.status = CondOrderStatus.COMPLETED
        order.updated_at = time()
        self._index.update_status(cond_id, CondOrderStatus.COMPLETED)
        self._append_history(cond_id, CondOrderStatus.COMPLETED, message, context or {})
        return order

    def history(self, cond_id: str) -> list[CondOrderHistory]:
        return list(self._history.get(cond_id, []))

    def save(self) -> None:
        if self._storage is None:
            return
        self._storage.save(self.list(), self._history)

    def load(self) -> None:
        if self._storage is None:
            return
        orders, history = self._storage.load()
        self._orders = {order.cond_id: order for order in orders}
        self._history = history
        self._index.rebuild(orders)

    def on_tick(self, tick: MarketTick) -> list[CondOrder]:
        triggered: list[CondOrder] = []
        for order in self._orders.values():
            if order.status != CondOrderStatus.ACTIVE:
                continue
            if self._checker.should_trigger(order, tick):
                try:
                    triggered_order = self.trigger(order.cond_id, {"tick": tick.__dict__})
                    triggered.append(triggered_order)
                    self._notify_callbacks(triggered_order, tick)
                except CondOrderError as exc:
                    order.last_error = exc.message
                    order.updated_at = time()
                    self._append_history(order.cond_id, CondOrderStatus.FAILED, exc.message, {"tick": tick.__dict__})
        return triggered

    def _append_history(self, cond_id: str, status: CondOrderStatus, message: str, data: dict[str, Any] | None = None) -> None:
        self._history.setdefault(cond_id, []).append(
            CondOrderHistory(cond_id=cond_id, status=status, message=message, timestamp=time(), data=data or {})
        )
