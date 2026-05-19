from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import logging
from typing import Any, Protocol

from pyctp.gateway.eventbus.bus import Event, EventBus

logger = logging.getLogger(__name__)

try:
    from pyctp.ctpmd import MdApi  # type: ignore
except Exception:  # pragma: no cover
    MdApi = None  # type: ignore


@dataclass(slots=True)
class MarketLoginResult:
    ok: bool
    message: str = ""
    data: dict[str, Any] | None = None


@dataclass(slots=True)
class SubscribeResult:
    ok: bool
    message: str = ""
    data: dict[str, Any] | None = None


class MarketApiPort(Protocol):
    def connect(self, address: str, userid: str, password: str, broker_id: str, auth_code: str = "", appid: str = "") -> None: ...
    def login(self) -> None: ...
    def subscribe(self, instrument_ids: list[str]) -> None: ...
    def unsubscribe(self, instrument_ids: list[str]) -> None: ...
    def close(self) -> None: ...
    def get_quote(self, symbol: str) -> dict[str, Any] | None: ...
    def get_quotes(self, symbols: list[str]) -> list[dict[str, Any]]: ...
    def is_subscribed(self, symbol: str) -> bool: ...


if MdApi is not None:
    MdSpiBase = MdApi
else:
    class MdSpiBase:
        pass


class MdSpiBridge(MdSpiBase):
    def __init__(self, bus: EventBus) -> None:
        if MdApi is not None:
            super().__init__()
        self.bus = bus
        self.front = ""
        self.userid = ""
        self.password = ""
        self.broker_id = ""
        self.auth_code = ""
        self.appid = ""
        self.connect_status = False
        self.login_status = False
        self.reqid = 0
        self._subscribed: set[str] = set()
        self._quote_cache: dict[str, dict[str, Any]] = {}
        self._api_created = False

    def connect(self, address: str, userid: str, password: str, broker_id: str, auth_code: str = "", appid: str = "") -> None:
        self.front = address
        self.userid = userid
        self.password = password
        self.broker_id = broker_id
        self.auth_code = auth_code
        self.appid = appid
        if MdApi is None:
            raise RuntimeError("ctpmd MdApi is not available")
        try:
            ctp_con_dir: Path = Path.cwd().joinpath("con")
            if not ctp_con_dir.exists():
                ctp_con_dir.mkdir(parents=True, exist_ok=True)
            api_path_str = str(ctp_con_dir / "md")
            self.bus.publish_threadsafe(Event(type="market.connect_start", source="market", payload={"front": address, "api_path": api_path_str}))
            if hasattr(self, "createFtdcMdApi"):
                self.bus.publish_threadsafe(Event(type="market.connect_step", source="market", payload={"step": "createFtdcMdApi.start", "api_path": api_path_str}))
                self.createFtdcMdApi(api_path_str.encode("GBK").decode("utf-8"), False, False, True)
                self._api_created = True
                self.bus.publish_threadsafe(Event(type="market.connect_step", source="market", payload={"step": "createFtdcMdApi.done", "api_created": True}))
            else:
                self.bus.publish_threadsafe(Event(type="market.connect_step", source="market", payload={"step": "createFtdcMdApi.skip", "reason": "not available"}))
            self.bus.publish_threadsafe(Event(type="market.connect_step", source="market", payload={"step": "registerFront.start", "front": address}))
            self.registerFront(address)
            self.bus.publish_threadsafe(Event(type="market.connect_step", source="market", payload={"step": "registerFront.done", "front": address}))
            self.bus.publish_threadsafe(Event(type="market.connect_step", source="market", payload={"step": "init.start", "front": address}))
            self.init()
            self.bus.publish_threadsafe(Event(type="market.connect_step", source="market", payload={"step": "init.done", "front": address}))
            self.connect_status = True
            self.bus.publish_threadsafe(Event(type="market.connect_succeeded", source="market", payload={"front": address}))
        except Exception as exc:
            self.connect_status = False
            self.bus.publish_threadsafe(Event(type="market.connect_failed", source="market", payload={"front": address, "error": str(exc)}))
            raise

    def login(self) -> None:
        if MdApi is None:
            raise RuntimeError("ctpmd MdApi is not available")
        self.reqid += 1
        req = {"BrokerID": self.broker_id, "UserID": self.userid, "Password": self.password}
        ret = self.reqUserLogin(req, self.reqid)
        if ret != 0:
            self.bus.publish_threadsafe(Event(type="market.login_failed", source="market", payload={"ret_code": ret, "reqid": self.reqid}))
            raise RuntimeError(f"reqUserLogin failed ret_code={ret}")
        self.bus.publish_threadsafe(Event(type="market.login_request_sent", source="market", payload={"reqid": self.reqid}))

    def subscribe(self, instrument_ids: list[str]) -> None:
        items = [self._normalize_symbol(symbol) for symbol in instrument_ids if symbol]
        if not items:
            return
        self._subscribed.update(items)
        if MdApi is None:
            raise RuntimeError("ctpmd MdApi is not available")
        results: list[dict[str, Any]] = []
        for symbol in items:
            ctp_symbol = self._to_ctp_instrument_id(symbol)
            ret = self.subscribeMarketData(ctp_symbol)
            results.append({"symbol": symbol, "instrument_id": ctp_symbol, "ret_code": ret})
            if ret != 0:
                self.bus.publish_threadsafe(Event(type="market.subscribe.accepted", source="market", payload={"instrument_ids": items, "subscribed": sorted(self._subscribed), "results": results}))
                raise RuntimeError(f"subscribeMarketData failed symbol={symbol} instrument_id={ctp_symbol} ret_code={ret}")
        self.bus.publish_threadsafe(Event(type="market.subscribe.accepted", source="market", payload={"instrument_ids": items, "subscribed": sorted(self._subscribed), "results": results}))

    def unsubscribe(self, instrument_ids: list[str]) -> None:
        items = [symbol for symbol in instrument_ids if symbol]
        if not items:
            return
        self._subscribed.difference_update(items)
        if MdApi is None:
            raise RuntimeError("ctpmd MdApi is not available")
        results: list[dict[str, Any]] = []
        for symbol in items:
            ctp_symbol = self._to_ctp_instrument_id(symbol)
            ret = self.unSubscribeMarketData(ctp_symbol)
            results.append({"symbol": symbol, "instrument_id": ctp_symbol, "ret_code": ret})
            if ret != 0:
                self.bus.publish_threadsafe(Event(type="market.unsubscribe.accepted", source="market", payload={"instrument_ids": items, "subscribed": sorted(self._subscribed), "results": results}))
                raise RuntimeError(f"unSubscribeMarketData failed symbol={symbol} instrument_id={ctp_symbol} ret_code={ret}")
        self.bus.publish_threadsafe(Event(type="market.unsubscribe.accepted", source="market", payload={"instrument_ids": items, "subscribed": sorted(self._subscribed), "results": results}))

    def onFrontConnected(self) -> None:
        self.connect_status = True
        self.bus.publish_threadsafe(Event(type="market.front_connected", source="market", payload={"front": self.front}))
        if self.userid and self.password and self.broker_id:
            self.login()

    def onFrontDisconnected(self, reason: int) -> None:
        self.connect_status = False
        self.login_status = False
        self.bus.publish_threadsafe(Event(type="market.front_disconnected", source="market", payload={"reason": int(reason)}))

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        self.login_status = int(error.get("ErrorID", 0)) == 0
        self.bus.publish_threadsafe(Event(type="market.login_rsp", source="market", request_id=int(reqid), payload={"data": data, "error": error, "last": bool(last), "front": self.front}))

    def onRspSubMarketData(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        instrument_id = str(data.get("InstrumentID", "")) if isinstance(data, dict) else ""
        exchange_id = str(data.get("ExchangeID", "")) if isinstance(data, dict) else ""
        logger.info(
            "market onRspSubMarketData reqid=%s last=%s instrument_id=%s exchange_id=%s error=%s data=%s",
            reqid,
            last,
            instrument_id,
            exchange_id,
            error,
            data,
        )
        self.bus.publish_threadsafe(Event(type="market.subscribe.finished", source="market", request_id=int(reqid), payload={"data": data, "error": error, "last": bool(last)}))

    def onRspUnSubMarketData(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        instrument_id = str(data.get("InstrumentID", "")) if isinstance(data, dict) else ""
        exchange_id = str(data.get("ExchangeID", "")) if isinstance(data, dict) else ""
        logger.info(
            "market onRspUnSubMarketData reqid=%s last=%s instrument_id=%s exchange_id=%s error=%s data=%s",
            reqid,
            last,
            instrument_id,
            exchange_id,
            error,
            data,
        )
        self.bus.publish_threadsafe(Event(type="market.unsubscribe.finished", source="market", request_id=int(reqid), payload={"data": data, "error": error, "last": bool(last)}))

    def onRtnDepthMarketData(self, data: dict[str, Any]) -> None:
        instrument_id = str(data.get("InstrumentID", ""))
        exchange_id = str(data.get("ExchangeID", ""))
        update_time = str(data.get("UpdateTime", ""))
        update_millisec = int(data.get("UpdateMillisec", 0) or 0)
        last_price = data.get("LastPrice", None)
        logger.info(
            "market onRtnDepthMarketData instrument_id=%s exchange_id=%s update_time=%s update_millisec=%s last_price=%s",
            instrument_id,
            exchange_id,
            update_time,
            update_millisec,
            last_price,
        )
        if not instrument_id:
            logger.info("market onRtnDepthMarketData skipped empty instrument_id data=%s", data)
            return
        symbol = self._resolve_quote_symbol(exchange_id, instrument_id)
        quote = self._normalize_quote(symbol, exchange_id, instrument_id, data)
        self._quote_cache[symbol] = quote
        logger.info("market quote cached symbol=%s trading_day=%s update_time=%s last_price=%s", symbol, quote.get("trading_day"), quote.get("update_time"), quote.get("last_price"))
        self.bus.publish_threadsafe(Event(type="market.quote.update", source="market", payload={"quote": quote, "symbol": symbol}))

    def get_quote(self, symbol: str) -> dict[str, Any] | None:
        return self._quote_cache.get(symbol)

    def get_quotes(self, symbols: list[str]) -> list[dict[str, Any]]:
        return [self._quote_cache[s] for s in symbols if s in self._quote_cache]

    def is_subscribed(self, symbol: str) -> bool:
        return self._normalize_symbol(symbol) in self._subscribed

    @staticmethod
    def _to_ctp_instrument_id(symbol: str) -> str:
        return symbol.split(".", 1)[1] if "." in symbol else symbol

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        symbol = symbol.strip()
        if "." not in symbol:
            return symbol.lower() if symbol[:2].isalpha() else symbol
        exchange_id, instrument_id = symbol.split(".", 1)
        exchange_id = exchange_id.strip().upper()
        instrument_id = instrument_id.strip()
        if exchange_id == "SHFE":
            instrument_id = instrument_id.lower()
        return f"{exchange_id}.{instrument_id}"

    @staticmethod
    def _resolve_quote_symbol(exchange_id: str, instrument_id: str) -> str:
        exchange_id = exchange_id.strip().upper()
        instrument_id = instrument_id.strip()
        if exchange_id == "SHFE":
            instrument_id = instrument_id.lower()
        return f"{exchange_id}.{instrument_id}" if exchange_id else instrument_id

    @staticmethod
    def _normalize_quote(symbol: str, exchange_id: str, instrument_id: str, data: dict[str, Any]) -> dict[str, Any]:
        return {
            "symbol": symbol,
            "exchange_id": exchange_id,
            "instrument_id": instrument_id,
            "last_price": float(data.get("LastPrice", 0.0) or 0.0),
            "pre_close": float(data.get("PreClosePrice", 0.0) or 0.0),
            "open_price": float(data.get("OpenPrice", 0.0) or 0.0),
            "highest_price": float(data.get("HighestPrice", 0.0) or 0.0),
            "lowest_price": float(data.get("LowestPrice", 0.0) or 0.0),
            "bid_price1": float(data.get("BidPrice1", 0.0) or 0.0),
            "bid_volume1": int(data.get("BidVolume1", 0) or 0),
            "ask_price1": float(data.get("AskPrice1", 0.0) or 0.0),
            "ask_volume1": int(data.get("AskVolume1", 0) or 0),
            "volume": int(data.get("Volume", 0) or 0),
            "open_interest": float(data.get("OpenInterest", 0.0) or 0.0),
            "settlement_price": float(data.get("SettlementPrice", 0.0) or 0.0),
            "pre_settlement_price": float(data.get("PreSettlementPrice", 0.0) or 0.0),
            "upper_limit_price": float(data.get("UpperLimitPrice", 0.0) or 0.0),
            "lower_limit_price": float(data.get("LowerLimitPrice", 0.0) or 0.0),
            "action_day": str(data.get("ActionDay", "")),
            "trading_day": str(data.get("TradingDay", "")),
            "update_time": str(data.get("UpdateTime", "")),
            "update_millisec": int(data.get("UpdateMillisec", 0) or 0),
            "raw": data,
        }


class PybindMdApiAdapter:
    def __init__(self, api: Any | None = None, bus: EventBus | None = None) -> None:
        self.bus = bus or EventBus()
        self.api = api or MdSpiBridge(self.bus)

    def connect(self, address: str, userid: str, password: str, broker_id: str, auth_code: str = "", appid: str = "") -> None:
        self._require_api().connect(address, userid, password, broker_id, auth_code, appid)

    def login(self) -> None:
        self._require_api().login()

    def subscribe(self, instrument_ids: list[str]) -> None:
        self._require_api().subscribe(instrument_ids)

    def unsubscribe(self, instrument_ids: list[str]) -> None:
        self._require_api().unsubscribe(instrument_ids)

    def close(self) -> None:
        api = self._require_api()
        if hasattr(api, "close"):
            api.close()

    def get_quote(self, symbol: str) -> dict[str, Any] | None:
        api = self._require_api()
        if hasattr(api, "get_quote"):
            return getattr(api, "get_quote")(symbol)
        return None

    def get_quotes(self, symbols: list[str]) -> list[dict[str, Any]]:
        api = self._require_api()
        if hasattr(api, "get_quotes"):
            return list(getattr(api, "get_quotes")(symbols))
        return []

    def is_subscribed(self, symbol: str) -> bool:
        api = self._require_api()
        if hasattr(api, "is_subscribed"):
            return bool(getattr(api, "is_subscribed")(symbol))
        return False

    def _require_api(self) -> MarketApiPort:
        if self.api is None:
            raise RuntimeError("CTP market api is not attached")
        return self.api


class MarketFeedAdapter:
    def __init__(self, api: MarketApiPort | None = None, bus: EventBus | None = None) -> None:
        self.api = api
        self.bus = bus

    def attach(self, api: MarketApiPort) -> None:
        self.api = api

    def connect(self, address: str, userid: str, password: str, broker_id: str, auth_code: str = "", appid: str = "") -> None:
        self._require_api().connect(address, userid, password, broker_id, auth_code, appid)

    def login(self) -> None:
        self._require_api().login()

    def subscribe(self, instrument_ids: list[str]) -> None:
        self._require_api().subscribe(instrument_ids)

    def unsubscribe(self, instrument_ids: list[str]) -> None:
        self._require_api().unsubscribe(instrument_ids)

    def close(self) -> None:
        self._require_api().close()

    def get_quote(self, symbol: str) -> dict[str, Any] | None:
        api = self._require_api()
        if hasattr(api, "get_quote"):
            return getattr(api, "get_quote")(symbol)
        return None

    def get_quotes(self, symbols: list[str]) -> list[dict[str, Any]]:
        api = self._require_api()
        if hasattr(api, "get_quotes"):
            return list(getattr(api, "get_quotes")(symbols))
        return []

    def is_subscribed(self, symbol: str) -> bool:
        api = self._require_api()
        if hasattr(api, "is_subscribed"):
            return bool(getattr(api, "is_subscribed")(symbol))
        return False

    def _require_api(self) -> MarketApiPort:
        if self.api is None:
            raise RuntimeError("CTP market api is not attached")
        return self.api
