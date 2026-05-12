from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Coroutine

from pyctp.gateway.config.loader import load_config
from pyctp.gateway.eventbus.bus import EventBus
from pyctp.gateway.trader.engine import TraderEngine
from pyctp.gateway.utils.logger import setup_logging


async def run() -> int | None:
    root = Path(__file__).resolve().parent.parent
    config = load_config(root / "config" / "config.json")
    setup_logging(config.log_level)

    bus = EventBus()
    trader = TraderEngine(bus=bus, config=config)
    await trader.start()

    try:
        await trader.run_forever()
    finally:
        await trader.stop()
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(run()))


if __name__ == "__main__":
    main()
