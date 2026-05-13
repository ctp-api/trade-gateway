from __future__ import annotations

import asyncio
from pathlib import Path

from pyctp.gateway.config.loader import load_config
from pyctp.gateway.eventbus.bus import EventBus
from pyctp.gateway.market.adapter import MarketFeedAdapter, PybindMdApiAdapter
from pyctp.gateway.market.engine import MarketConfig, MarketEngine
from pyctp.gateway.websocket import WebSocketServer
from pyctp.gateway.trader.engine import TraderEngine
from pyctp.gateway.utils.logger import setup_logging


async def run() -> int | None:
    root = Path(__file__).resolve().parent.parent
    config = load_config(root / "config" / "config.json")
    setup_logging(config.log_level)

    bus = EventBus()
    trader = TraderEngine(bus=bus, config=config)
    market_ws = WebSocketServer(config.market_host if hasattr(config, "market_host") else config.host, getattr(config, "market_port", 7789), bus)
    market = MarketEngine(
        bus=bus,
        feed=MarketFeedAdapter(PybindMdApiAdapter(bus=bus), bus=bus),
        config=MarketConfig(data_dir=config.data_dir, log_level=config.log_level, md_front=getattr(config, "md_front", ""), broker_id=getattr(config, "broker_id", ""), user_name=getattr(config, "user_name", ""), password=getattr(config, "password", ""), auth_code=getattr(config, "auth_code", ""), appid=getattr(config, "appid", "")),
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
