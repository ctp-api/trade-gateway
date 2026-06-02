from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from typing import Any

from .types import CondOrder, CondOrderStatus


class CondOrderIndex:
    def __init__(self) -> None:
        self._by_id: dict[str, CondOrder] = {}
        self._by_owner: dict[str, set[str]] = defaultdict(set)
        self._by_instrument: dict[str, set[str]] = defaultdict(set)
        self._by_status: dict[str, set[str]] = defaultdict(set)

    def upsert(self, order: CondOrder) -> None:
        self.remove(order.cond_id)
        self._by_id[order.cond_id] = order
        if order.owner:
            self._by_owner[order.owner].add(order.cond_id)
        if order.condition.instrument_id:
            self._by_instrument[order.condition.instrument_id].add(order.cond_id)
        self._by_status[order.status.value].add(order.cond_id)

    def remove(self, cond_id: str) -> None:
        order = self._by_id.pop(cond_id, None)
        if order is None:
            return
        if order.owner in self._by_owner:
            self._by_owner[order.owner].discard(cond_id)
            if not self._by_owner[order.owner]:
                self._by_owner.pop(order.owner, None)
        if order.condition.instrument_id in self._by_instrument:
            self._by_instrument[order.condition.instrument_id].discard(cond_id)
            if not self._by_instrument[order.condition.instrument_id]:
                self._by_instrument.pop(order.condition.instrument_id, None)
        if order.status.value in self._by_status:
            self._by_status[order.status.value].discard(cond_id)
            if not self._by_status[order.status.value]:
                self._by_status.pop(order.status.value, None)

    def update_status(self, cond_id: str, status: CondOrderStatus) -> None:
        order = self._by_id.get(cond_id)
        if order is None:
            return
        if order.status.value in self._by_status:
            self._by_status[order.status.value].discard(cond_id)
            if not self._by_status[order.status.value]:
                self._by_status.pop(order.status.value, None)
        order.status = status
        self._by_status[status.value].add(cond_id)

    def get(self, cond_id: str) -> CondOrder | None:
        return self._by_id.get(cond_id)

    def by_owner(self, owner: str) -> list[CondOrder]:
        return [self._by_id[cond_id] for cond_id in self._by_owner.get(owner, set()) if cond_id in self._by_id]

    def by_instrument(self, instrument_id: str) -> list[CondOrder]:
        return [self._by_id[cond_id] for cond_id in self._by_instrument.get(instrument_id, set()) if cond_id in self._by_id]

    def by_status(self, status: CondOrderStatus) -> list[CondOrder]:
        return [self._by_id[cond_id] for cond_id in self._by_status.get(status.value, set()) if cond_id in self._by_id]

    def all(self) -> list[CondOrder]:
        return list(self._by_id.values())

    def rebuild(self, orders: list[CondOrder]) -> None:
        self._by_id.clear()
        self._by_owner.clear()
        self._by_instrument.clear()
        self._by_status.clear()
        for order in orders:
            self.upsert(order)
