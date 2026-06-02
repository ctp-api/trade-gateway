from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class TraderPersistenceData:
    user_name: str = ""
    trading_day: str = ""
    order_conn_map: dict[str, int] = None  # type: ignore[assignment]
    query_pending: dict[str, dict[str, Any]] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.order_conn_map is None:
            self.order_conn_map = {}
        if self.query_pending is None:
            self.query_pending = {}


class TraderPersistence:
    def __init__(self, base_dir: str | Path) -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def path_for(self, user_name: str, trading_day: str = "") -> Path:
        safe_user = user_name or "default"
        safe_day = trading_day or "unknown"
        return self.base_dir / f"trader_{safe_user}_{safe_day}.json"

    def load(self, user_name: str, trading_day: str = "") -> TraderPersistenceData:
        path = self.path_for(user_name, trading_day)
        if not path.exists():
            return TraderPersistenceData(user_name=user_name, trading_day=trading_day)
        data = json.loads(path.read_text(encoding="utf-8"))
        return TraderPersistenceData(
            user_name=str(data.get("user_name", user_name)),
            trading_day=str(data.get("trading_day", trading_day)),
            order_conn_map={str(k): int(v) for k, v in dict(data.get("order_conn_map", {})).items()},
            query_pending=dict(data.get("query_pending", {})),
        )

    def save(self, data: TraderPersistenceData) -> Path:
        path = self.path_for(data.user_name, data.trading_day)
        payload = asdict(data)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def clear(self, user_name: str, trading_day: str = "") -> None:
        path = self.path_for(user_name, trading_day)
        if path.exists():
            path.unlink()
