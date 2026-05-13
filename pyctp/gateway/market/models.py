from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class MarketState(str, Enum):
    INIT = "init"
    CONNECTING = "connecting"
    LOGGING_IN = "logging_in"
    READY = "ready"
    SUBSCRIBING = "subscribing"
    STOPPING = "stopping"
    STOPPED = "stopped"


@dataclass(slots=True)
class Quote:
    symbol: str
    exchange_id: str
    instrument_id: str
    last_price: float
    pre_close: float
    open_price: float
    highest_price: float
    lowest_price: float
    bid_price1: float
    bid_volume1: int
    ask_price1: float
    ask_volume1: int
    volume: int
    open_interest: float
    settlement_price: float
    pre_settlement_price: float
    upper_limit_price: float
    lower_limit_price: float
    action_day: str
    trading_day: str
    update_time: str
    update_millisec: int
    raw: dict[str, Any] | None = None


class MarketStateMachine:
    def __init__(self) -> None:
        self._state = MarketState.INIT

    def transition_to(self, state: MarketState) -> None:
        self._state = state

    def get_state(self) -> MarketState:
        return self._state

    def can_accept_login(self) -> bool:
        return self._state in {MarketState.INIT, MarketState.STOPPED}

    def can_accept_subscribe(self) -> bool:
        return self._state in {MarketState.READY, MarketState.SUBSCRIBING}

    def can_accept_unsubscribe(self) -> bool:
        return self._state in {MarketState.READY, MarketState.SUBSCRIBING}

    def is_ready(self) -> bool:
        return self._state == MarketState.READY


@dataclass(slots=True)
class QuoteStore:
    _quotes: dict[str, Quote] = field(default_factory=dict)

    def update(self, quote: Quote) -> None:
        self._quotes[quote.symbol] = quote

    def get(self, symbol: str) -> Quote | None:
        return self._quotes.get(symbol)

    def get_many(self, symbols: list[str]) -> list[Quote]:
        return [quote for symbol in symbols if (quote := self._quotes.get(symbol)) is not None]

    def all(self) -> list[Quote]:
        return list(self._quotes.values())

    def clear(self) -> None:
        self._quotes.clear()
