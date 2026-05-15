from __future__ import annotations

import asyncio
from pathlib import Path

from pyctp.gateway.config.loader import load_config
from pyctp.gateway.config.models import AppConfig
from pyctp.gateway.eventbus.bus import EventBus
from pyctp.gateway.market.adapter import MarketFeedAdapter, PybindMdApiAdapter
from pyctp.gateway.market.engine import MarketConfig, MarketEngine
from pyctp.gateway.trader.engine import TraderConfig, TraderEngine
from pyctp.gateway.websocket import WebSocketServer
from pyctp.gateway.utils.logger import setup_logging


async def run() -> int | None:
    root = Path(__file__).resolve().parent.parent
    config = load_config(root / "config" / "config.json")
    setup_logging(config.log_level)

    trader_bus = EventBus()
    market_bus = EventBus()
    trader_config = TraderConfig(
        host=config.host,
        port=config.port,
        log_level=config.log_level,
        data_dir=config.data_dir,
        ctp_appid=getattr(config, "ctp_appid", "simnow_client_test"),
        ctp_auth_code=getattr(config, "ctp_auth_code", "0000000000000000"),
    )
    trader = TraderEngine(bus=trader_bus, config=trader_config)
    market_ws = WebSocketServer(getattr(config, "market_host", config.host), getattr(config, "market_port", 7789), market_bus)
    market = MarketEngine(
        bus=market_bus,
        feed=MarketFeedAdapter(PybindMdApiAdapter(bus=market_bus), market_bus),
        config=MarketConfig(data_dir=config.data_dir, log_level=config.log_level, md_front=getattr(config, "md_front", ""), broker_id=getattr(config, "broker_id", ""), user_name=getattr(config, "user_name", ""), password=getattr(config, "password", ""), auth_code=getattr(config, "auth_code", ""), appid=getattr(config, "appid", ""), host=getattr(config, "market_host", getattr(config, "host", "0.0.0.0")), port=getattr(config, "market_port", 7789)),
        ws=market_ws,
    )

    await trader.start()
    await market.start()

    try:
        await trader.run_forever()
    finally:
        await market.stop()
        await trader.stop()
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(run()))


if __name__ == "__main__":
    main()
