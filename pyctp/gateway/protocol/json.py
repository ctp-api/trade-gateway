from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from typing import Any

from pyctp.gateway.protocol.types import (
    CancelOrderRequest,
    InsertOrderRequest,
    LoginRequest,
    MarketLoginRequest,
    WsRequest,
    WsResponse,
)


class ProtocolCodec:
    def dumps(self, obj: Any) -> str:
        if is_dataclass(obj):
            obj = asdict(obj)
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

    def loads(self, raw: str) -> dict[str, Any]:
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("message must be a JSON object")
        return data

    def parse_request(self, raw: str, conn_id: int | None = None) -> WsRequest:
        data = self.loads(raw)
        aid = str(data.get("aid", ""))
        if not aid:
            raise ValueError("missing aid")
        return WsRequest(aid=aid, conn_id=conn_id, request_id=None, raw=data)

    @staticmethod
    def _payload(req: WsRequest) -> dict[str, Any]:
        payload = req.raw.get("data")
        if not isinstance(payload, dict):
            raise ValueError("missing data payload")
        return payload

    def parse_login(self, req: WsRequest) -> LoginRequest:
        data = self._payload(req)
        return LoginRequest(
            user_name=str(data.get("user_name", "")),
            password=str(data.get("password", "")),
            broker_id=(str(data["broker_id"]) if data.get("broker_id") is not None else None),
            front=(str(data["front"]) if data.get("front") is not None else None),
            auth_code=str(data.get("auth_code", "")),
            appid=str(data.get("appid", "")),
        )

    def parse_market_login(self, req: WsRequest) -> MarketLoginRequest:
        data = self._payload(req)
        return MarketLoginRequest(
            user_name=str(data.get("user_name", "")),
            password=str(data.get("password", "")),
            broker_id=(str(data["broker_id"]) if data.get("broker_id") is not None else None),
            front=(str(data["front"]) if data.get("front") is not None else None),
            auth_code=str(data.get("auth_code", "")),
            appid=str(data.get("appid", "")),
        )

    def parse_insert_order(self, req: WsRequest) -> InsertOrderRequest:
        data = self._payload(req)
        return InsertOrderRequest(
            instrument_id=str(data.get("instrument_id", "")),
            exchange_id=str(data.get("exchange_id", "")),
            direction=str(data.get("direction", "")),
            offset=str(data.get("offset", "")),
            volume=int(data.get("volume", 0)),
            price=float(data.get("price", 0.0)),
            price_type=str(data.get("price_type", "limit")),
            close_today_prior=bool(data.get("close_today_prior", False)),
        )

    def parse_cancel_order(self, req: WsRequest) -> CancelOrderRequest:
        data = self._payload(req)
        return CancelOrderRequest(
            order_id=str(data.get("order_id", "")),
            exchange_id=str(data.get("exchange_id", "")),
            instrument_id=str(data.get("instrument_id", "")),
        )

    def build_response(self, response: WsResponse) -> str:
        return self.dumps(response)

    def build_notify(self, code: int, msg: str, level: str = "INFO", msg_type: str = "NOTIFY") -> str:
        return self.dumps({
            "aid": "notify",
            "code": code,
            "msg": msg,
            "level": level,
            "msg_type": msg_type,
        })
