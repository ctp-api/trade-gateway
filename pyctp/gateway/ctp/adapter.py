from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from pyctp.ctp_constant import (
    THOST_FTDC_AF_Delete,
    THOST_FTDC_CC_Immediately,
    THOST_FTDC_D_Buy,
    THOST_FTDC_D_Sell,
    THOST_FTDC_FCC_NotForceClose,
    THOST_FTDC_HF_Speculation,
    THOST_FTDC_OPT_AnyPrice,
    THOST_FTDC_OPT_LimitPrice,
    THOST_FTDC_OF_Close,
    THOST_FTDC_OF_CloseToday,
    THOST_FTDC_OF_Open,
    THOST_FTDC_TC_GFD,
    THOST_FTDC_TC_IOC,
    THOST_FTDC_VC_AV,
    THOST_FTDC_VC_CV,
)
from pyctp.ctptd import TdApi
from pyctp.gateway.eventbus.bus import Event, EventBus
from pyctp.util import prepare_address


@dataclass(slots=True)
class TraderLoginResult:
    ok: bool
    message: str = ""
    data: dict[str, Any] | None = None


@dataclass(slots=True)
class OrderInsertResult:
    ok: bool
    order_ref: str = ""
    message: str = ""
    data: dict[str, Any] | None = None


@dataclass(slots=True)
class OrderCancelResult:
    ok: bool
    message: str = ""
    data: dict[str, Any] | None = None


@dataclass(slots=True)
class QueryResult:
    ok: bool
    data: list[dict[str, Any]]
    message: str = ""
    raw: list[dict[str, Any]] | None = None


class TraderApiPort(Protocol):
    def connect(self, address: str, userid: str, password: str, broker_id: str, auth_code: str, appid: str) -> None: ...
    def authenticate(self) -> None: ...
    def login(self) -> None: ...
    def send_order(self, symbol: str, direction: str, price: float, volume: int) -> str: ...
    def cancel_order(self, order_ref: str, exchange_id: str = "", instrument_id: str = "") -> None: ...
    def close(self) -> None: ...


class TdSpiBridge(TdApi):
    def __init__(self, bus: EventBus) -> None:
        super().__init__()
        self.bus = bus
        self.userid = ""
        self.password = ""
        self.broker_id = ""
        self.auth_code = ""
        self.appid = ""
        self.front = ""
        self.connect_status = False
        self.login_status = False
        self.auth_status = False
        self.auth_failed = False
        self.login_failed = False
        self.reqid = 0
        self.order_ref = 0
        self.front_id = 0
        self.session_id = 0
        self._production_mode = True
        self.pending_order_map: dict[str, dict[str, str]] = {}
        self.order_status_map: dict[str, str] = {}
        self._query_rows: dict[str, list[dict[str, Any]]] = {
            "account": [],
            "position": [],
            "order": [],
            "trade": [],
            "instrument": [],
        }
        self._query_context: str = ""

    def connect(self, address: str, userid: str, password: str, broker_id: str, auth_code: str = "", appid: str = "") -> None:
        self.userid = userid
        self.password = password
        self.broker_id = broker_id
        self.auth_code = auth_code
        self.appid = appid
        self.front = prepare_address(address)

        if not self.connect_status:
            ctp_con_dir = Path.cwd() / "con"
            ctp_con_dir.mkdir(exist_ok=True)
            api_path_str = str(ctp_con_dir / "td")
            self.createFtdcTraderApi(api_path_str, self._production_mode)
            self.subscribePrivateTopic(0)
            self.subscribePublicTopic(0)
            self.registerFront(self.front)
            self.init()
            self.connect_status = True
        else:
            self.authenticate_or_login()

    def authenticate_or_login(self) -> None:
        if self.auth_code:
            self.authenticate()
        else:
            self.login()

    def onFrontConnected(self) -> None:
        self.bus.publish_threadsafe(Event(type="ctp.front_connected", source="ctp", payload={}))
        self.authenticate_or_login()

    def onFrontDisconnected(self, reason: int) -> None:
        self.bus.publish_threadsafe(Event(type="ctp.front_disconnected", source="ctp", payload={"reason": int(reason)}))
        self.connect_status = False
        self.login_status = False

    def onRspAuthenticate(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        self.bus.publish_threadsafe(Event(type="ctp.rsp_authenticate", source="ctp", request_id=int(reqid), payload={"data": data, "error": error, "last": bool(last)}))
        if int(error.get("ErrorID", 0)) == 0:
            self.auth_status = True
            self.auth_failed = False
            self.login()
        else:
            self.auth_failed = True

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        self.bus.publish_threadsafe(Event(type="ctp.rsp_user_login", source="ctp", request_id=int(reqid), payload={"data": data, "error": error, "last": bool(last)}))
        if int(error.get("ErrorID", 0)) == 0:
            self.login_status = True
            self.login_failed = False
            self.front_id = int(data.get("FrontID", 0))
            self.session_id = int(data.get("SessionID", 0))
            self.reqid += 1
            self.reqSettlementInfoConfirm({"BrokerID": self.broker_id, "InvestorID": self.userid}, self.reqid)
        else:
            self.login_failed = True

    def onRspSettlementInfoConfirm(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        self.bus.publish_threadsafe(Event(type="ctp.rsp_settlement_info_confirm", source="ctp", request_id=int(reqid), payload={"data": data, "error": error, "last": bool(last)}))

    def onRspOrderInsert(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        self.bus.publish_threadsafe(Event(type="ctp.rsp_order_insert", source="ctp", request_id=int(reqid), payload={"data": data, "error": error, "last": bool(last)}))
        order_ref = str(data.get("OrderRef", "") or data.get("order_ref", ""))
        if order_ref and int(error.get("ErrorID", 0)) == 0:
            self.order_status_map[order_ref] = "insert_submitted"

    def onErrRtnOrderInsert(self, data: dict, error: dict) -> None:
        self.bus.publish_threadsafe(Event(type="ctp.err_order_insert", source="ctp", payload={"data": data, "error": error}))
        order_ref = str(data.get("OrderRef", "") or data.get("order_ref", ""))
        if order_ref:
            self.order_status_map[order_ref] = "insert_rejected"

    def onRtnOrder(self, data: dict) -> None:
        self.bus.publish_threadsafe(Event(type="ctp.rtn_order", source="ctp", payload={"data": data}))
        order_ref = str(data.get("OrderRef", "") or data.get("order_ref", ""))
        if order_ref:
            self.order_status_map[order_ref] = str(data.get("OrderStatus", self.order_status_map.get(order_ref, "")))

    def onRtnTrade(self, data: dict) -> None:
        self.bus.publish_threadsafe(Event(type="ctp.rtn_trade", source="ctp", payload={"data": data}))

    def onRspOrderAction(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        self.bus.publish_threadsafe(Event(type="ctp.rsp_order_action", source="ctp", request_id=int(reqid), payload={"data": data, "error": error, "last": bool(last)}))
        order_ref = str(data.get("OrderRef", "") or data.get("order_ref", ""))
        if order_ref and int(error.get("ErrorID", 0)) == 0:
            self.order_status_map[order_ref] = "cancel_submitted"
        elif order_ref:
            self.order_status_map[order_ref] = "cancel_rejected"

    def onRspQryTradingAccount(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        self.bus.publish_threadsafe(Event(type="ctp.rsp_qry_trading_account", source="ctp", request_id=int(reqid), payload={"data": data, "error": error, "last": bool(last)}))
        if int(error.get("ErrorID", 0)) == 0:
            self._query_rows["account"].append(data)

    def onRspQryInvestorPosition(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        self.bus.publish_threadsafe(Event(type="ctp.rsp_qry_investor_position", source="ctp", request_id=int(reqid), payload={"data": data, "error": error, "last": bool(last)}))
        if int(error.get("ErrorID", 0)) == 0:
            self._query_rows["position"].append(data)

    def onRspQryOrder(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        self.bus.publish_threadsafe(Event(type="ctp.rsp_qry_order", source="ctp", request_id=int(reqid), payload={"data": data, "error": error, "last": bool(last)}))
        if int(error.get("ErrorID", 0)) == 0:
            self._query_rows["order"].append(data)

    def onRspQryTrade(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        self.bus.publish_threadsafe(Event(type="ctp.rsp_qry_trade", source="ctp", request_id=int(reqid), payload={"data": data, "error": error, "last": bool(last)}))
        if int(error.get("ErrorID", 0)) == 0:
            self._query_rows["trade"].append(data)

    def onRspQryInstrument(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        self.bus.publish_threadsafe(Event(type="ctp.rsp_qry_instrument", source="ctp", request_id=int(reqid), payload={"data": data, "error": error, "last": bool(last)}))
        if int(error.get("ErrorID", 0)) == 0:
            self._query_rows["instrument"].append(data)

    def authenticate(self) -> None:
        if self.auth_failed or self.auth_status:
            return
        ctp_req: dict = {"UserID": self.userid, "BrokerID": self.broker_id, "AuthCode": self.auth_code, "AppID": self.appid}
        self.reqid += 1
        self.reqAuthenticate(ctp_req, self.reqid)

    def login(self) -> None:
        if self.login_failed or self.login_status:
            return
        ctp_req: dict = {"BrokerID": self.broker_id, "UserID": self.userid, "Password": self.password}
        self.reqid += 1
        self.reqUserLogin(ctp_req, self.reqid)

    def send_order(self, symbol: str, direction: str, price: float, volume: int) -> str:
        self.order_ref += 1
        exchange_id, instrument_id = self._split_symbol(symbol)
        order_ref = str(self.order_ref)
        direction_field, offset_flag, price_type, time_condition, volume_condition = self._map_order_fields(direction)
        req = {
            "BrokerID": self.broker_id,
            "InvestorID": self.userid,
            "InstrumentID": instrument_id,
            "OrderRef": order_ref,
            "UserID": self.userid,
            "ExchangeID": exchange_id,
            "VolumeTotalOriginal": int(volume),
            "OrderPriceType": price_type,
            "Direction": direction_field,
            "CombOffsetFlag": offset_flag,
            "CombHedgeFlag": THOST_FTDC_HF_Speculation,
            "ContingentCondition": THOST_FTDC_CC_Immediately,
            "ForceCloseReason": THOST_FTDC_FCC_NotForceClose,
            "IsAutoSuspend": 0,
            "MinVolume": 1,
            "GTDDate": "",
            "LimitPrice": float(price),
            "StopPrice": 0,
            "UserForceClose": 0,
            "TimeCondition": time_condition,
            "VolumeCondition": volume_condition,
            "RequestID": self.reqid + 1,
            "IsSwapOrder": 0,
        }
        self.pending_order_map[order_ref] = {"exchange_id": exchange_id, "instrument_id": instrument_id}
        self.reqid += 1
        self.reqOrderInsert(req, self.reqid)
        return order_ref

    def cancel_order(self, order_ref: str, exchange_id: str = "", instrument_id: str = "") -> None:
        meta = self.pending_order_map.get(order_ref, {})
        req = {
            "BrokerID": self.broker_id,
            "InvestorID": self.userid,
            "OrderRef": order_ref,
            "ExchangeID": exchange_id or meta.get("exchange_id", ""),
            "UserID": self.userid,
            "InstrumentID": instrument_id or meta.get("instrument_id", ""),
            "FrontID": self.front_id,
            "SessionID": self.session_id,
            "ActionFlag": THOST_FTDC_AF_Delete,
        }
        self.reqid += 1
        self.reqOrderAction(req, self.reqid)

    def query_trading_account(self) -> None:
        self._begin_query("account")
        self.reqid += 1
        self.reqQryTradingAccount({"BrokerID": self.broker_id, "InvestorID": self.userid}, self.reqid)

    def query_investor_position(self) -> None:
        self._begin_query("position")
        self.reqid += 1
        self.reqQryInvestorPosition({"BrokerID": self.broker_id, "InvestorID": self.userid}, self.reqid)

    def query_order(self) -> None:
        self._begin_query("order")
        self.reqid += 1
        self.reqQryOrder({"BrokerID": self.broker_id, "InvestorID": self.userid}, self.reqid)

    def query_trade(self) -> None:
        self._begin_query("trade")
        self.reqid += 1
        self.reqQryTrade({"BrokerID": self.broker_id, "InvestorID": self.userid}, self.reqid)

    def query_instrument(self, instrument_id: str = "") -> None:
        self._begin_query("instrument")
        self.reqid += 1
        req: dict[str, Any] = {}
        if instrument_id:
            req["InstrumentID"] = instrument_id
        self.reqQryInstrument(req, self.reqid)

    def begin_query(self, context: str) -> None:
        self._begin_query(context)

    def query_rows(self, context: str) -> list[dict[str, Any]]:
        return list(self._query_rows.get(context, []))

    def clear_query_rows(self, context: str | None = None) -> None:
        if context is None:
            for rows in self._query_rows.values():
                rows.clear()
            return
        self._query_rows.setdefault(context, []).clear()

    def _begin_query(self, context: str) -> None:
        self._query_context = context
        self.clear_query_rows(context)

    def _map_order_fields(self, direction: str) -> tuple[str, str, str, str, str]:
        d = direction.upper()
        if d == "BUY_OPEN":
            return THOST_FTDC_D_Buy, THOST_FTDC_OF_Open, THOST_FTDC_OPT_LimitPrice, THOST_FTDC_TC_GFD, THOST_FTDC_VC_AV
        if d == "BUY_CLOSE":
            return THOST_FTDC_D_Buy, THOST_FTDC_OF_Close, THOST_FTDC_OPT_LimitPrice, THOST_FTDC_TC_GFD, THOST_FTDC_VC_AV
        if d == "BUY_CLOSE_TODAY":
            return THOST_FTDC_D_Buy, THOST_FTDC_OF_CloseToday, THOST_FTDC_OPT_LimitPrice, THOST_FTDC_TC_GFD, THOST_FTDC_VC_AV
        if d == "SELL_OPEN":
            return THOST_FTDC_D_Sell, THOST_FTDC_OF_Open, THOST_FTDC_OPT_LimitPrice, THOST_FTDC_TC_GFD, THOST_FTDC_VC_AV
        if d == "SELL_CLOSE":
            return THOST_FTDC_D_Sell, THOST_FTDC_OF_Close, THOST_FTDC_OPT_LimitPrice, THOST_FTDC_TC_GFD, THOST_FTDC_VC_AV
        if d == "SELL_CLOSE_TODAY":
            return THOST_FTDC_D_Sell, THOST_FTDC_OF_CloseToday, THOST_FTDC_OPT_LimitPrice, THOST_FTDC_TC_GFD, THOST_FTDC_VC_AV
        if d == "BUY_OPEN_FAQ":
            return THOST_FTDC_D_Buy, THOST_FTDC_OF_Open, THOST_FTDC_OPT_AnyPrice, THOST_FTDC_TC_IOC, THOST_FTDC_VC_CV
        raise ValueError(f"unsupported direction: {direction}")

    @staticmethod
    def _split_symbol(symbol: str) -> tuple[str, str]:
        if "." in symbol:
            exchange_id, instrument_id = symbol.split(".", 1)
            return exchange_id, instrument_id
        return "", symbol

    def close(self) -> None:
        self.exit()


class PybindTdApiAdapter:
    def __init__(self, api: TdApi | None = None, bus: EventBus | None = None) -> None:
        self.bus = bus
        self.api = api or (TdSpiBridge(bus) if bus is not None else TdApi())

    def connect(self, address: str, userid: str, password: str, broker_id: str, auth_code: str = "", appid: str = "") -> None:
        self._require_api().connect(address, userid, password, broker_id, auth_code, appid)

    def authenticate(self) -> None:
        self._require_api().authenticate()

    def login(self) -> None:
        self._require_api().login()

    def send_order(self, symbol: str, direction: str, price: float, volume: int) -> str:
        return self._require_api().send_order(symbol, direction, price, volume)

    def cancel_order(self, order_ref: str, exchange_id: str = "", instrument_id: str = "") -> None:
        self._require_api().cancel_order(order_ref, exchange_id, instrument_id)

    def query_trading_account(self) -> None:
        self._require_api().begin_query("account")
        self._require_api().query_trading_account()

    def query_investor_position(self) -> None:
        self._require_api().begin_query("position")
        self._require_api().query_investor_position()

    def query_order(self) -> None:
        self._require_api().begin_query("order")
        self._require_api().query_order()

    def query_trade(self) -> None:
        self._require_api().begin_query("trade")
        self._require_api().query_trade()

    def query_instrument(self, instrument_id: str = "") -> None:
        self._require_api().begin_query("instrument")
        self._require_api().query_instrument(instrument_id)

    def query_rows(self, context: str) -> list[dict[str, Any]]:
        api = self._require_api()
        if isinstance(api, TdSpiBridge):
            return api.query_rows(context)
        return []

    def clear_query_rows(self, context: str | None = None) -> None:
        api = self._require_api()
        if isinstance(api, TdSpiBridge):
            api.clear_query_rows(context)

    def close(self) -> None:
        self._require_api().close()

    def _require_api(self) -> TraderApiPort:
        if self.api is None:
            raise RuntimeError("CTP trader api is not attached")
        return self.api


class CtpTraderAdapter:
    def __init__(self, api: TraderApiPort | None = None) -> None:
        self.api = api

    def attach(self, api: TraderApiPort) -> None:
        self.api = api

    def connect(self, address: str, userid: str, password: str, broker_id: str, auth_code: str = "", appid: str = "") -> None:
        self._require_api().connect(address, userid, password, broker_id, auth_code, appid)

    def authenticate(self) -> None:
        self._require_api().authenticate()

    def login(self) -> None:
        self._require_api().login()

    def insert_order(self, symbol: str, direction: str, price: float, volume: int) -> OrderInsertResult:
        order_ref = self._require_api().send_order(symbol, direction, price, volume)
        return OrderInsertResult(ok=bool(order_ref), order_ref=order_ref, message="sent" if order_ref else "failed")

    def cancel_order(self, order_ref: str, exchange_id: str = "", instrument_id: str = "") -> OrderCancelResult:
        self._require_api().cancel_order(order_ref, exchange_id, instrument_id)
        return OrderCancelResult(ok=True, message="cancel requested", data={"order_ref": order_ref, "exchange_id": exchange_id, "instrument_id": instrument_id})

    def query_trading_account(self) -> None:
        self._require_api().query_trading_account()

    def query_investor_position(self) -> None:
        self._require_api().query_investor_position()

    def query_order(self) -> None:
        self._require_api().query_order()

    def query_trade(self) -> None:
        self._require_api().query_trade()

    def query_instrument(self, instrument_id: str = "") -> None:
        self._require_api().query_instrument(instrument_id)

    def query_rows(self, context: str) -> list[dict[str, Any]]:
        api = self._require_api()
        if hasattr(api, "query_rows"):
            return list(getattr(api, "query_rows")(context))
        return []

    def clear_query_rows(self, context: str | None = None) -> None:
        api = self._require_api()
        if hasattr(api, "clear_query_rows"):
            getattr(api, "clear_query_rows")(context)

    def close(self) -> None:
        self._require_api().close()

    def _require_api(self) -> TraderApiPort:
        if self.api is None:
            raise RuntimeError("CTP trader api is not attached")
        return self.api
