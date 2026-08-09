"""Upstox Market Data Feed V3 — single process-level WebSocket manager."""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from market.instrument_master import InstrumentMaster, UPSTOX_INDEX_KEYS
from market.market_state import get_market_state
from market.subscription_tiers import SubscriptionTierManager

log = logging.getLogger(__name__)

# API versions documented for audit trail
UPSTOX_API_VERSIONS = {
    "profile": "GET /v2/user/profile",
    "quote": "GET /v2/market-quote/quotes",
    "funds": "GET /v2/user/get-funds-and-margin",
    "holdings": "GET /v2/portfolio/long-term-holdings",
    "positions": "GET /v2/portfolio/short-term-positions",
    "orders": "GET /v2/order/retrieve-all",
    "market_feed": "WebSocket Market Data Feed V3 (protobuf via upstox-python-sdk MarketDataStreamerV3)",
}


class LatencyTracker:
    def __init__(self, max_samples: int = 5000):
        self.max_samples = max_samples
        self.exchange_to_received: list[float] = []
        self.received_to_published: list[float] = []
        self.lock = threading.Lock()

    def record_exchange_to_received(self, ms: float) -> None:
        with self.lock:
            self.exchange_to_received.append(ms)
            if len(self.exchange_to_received) > self.max_samples:
                self.exchange_to_received = self.exchange_to_received[-self.max_samples :]

    def record_received_to_published(self, ms: float) -> None:
        with self.lock:
            self.received_to_published.append(ms)
            if len(self.received_to_published) > self.max_samples:
                self.received_to_published = self.received_to_published[-self.max_samples :]

    def summary(self) -> dict[str, Any]:
        def stats(values: list[float]) -> dict[str, float | None]:
            if not values:
                return {"p50": None, "p95": None, "max": None}
            s = sorted(values)
            p50 = s[len(s) // 2]
            p95 = s[int(len(s) * 0.95) - 1]
            return {"p50": round(p50, 2), "p95": round(p95, 2), "max": round(max(s), 2)}

        with self.lock:
            return {
                "exchange_to_received_ms": stats(self.exchange_to_received),
                "received_to_published_ms": stats(self.received_to_published),
            }


class UpstoxV3FeedManager:
    """One WebSocket worker for the entire process. Streamlit reruns attach here."""

    _instance: "UpstoxV3FeedManager | None" = None
    _lock = threading.Lock()

    def __init__(self):
        self.worker_id = f"upstox-v3-{uuid.uuid4().hex[:12]}"
        self.state = get_market_state()
        self.tiers = SubscriptionTierManager()
        self.instruments = InstrumentMaster()
        self.latency = LatencyTracker()
        self.lock = threading.RLock()
        self._streamer = None
        self._profile: dict[str, Any] = {}
        self._connected = False
        self._last_tick_at: str | None = None
        self._last_heartbeat_at: str | None = None
        self._reconnect_count = 0
        self._mode = "ltpc"
        self._replay_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._entry_latency_cb: Callable[[float], None] | None = None

    @classmethod
    def instance(cls) -> "UpstoxV3FeedManager":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def health(self) -> dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "connected": self._connected,
            "last_tick_at": self._last_tick_at,
            "last_heartbeat_at": self._last_heartbeat_at,
            "reconnect_count": self._reconnect_count,
            "subscriptions": self.tiers.snapshot(),
            "latency": self.latency.summary(),
            "api_versions": UPSTOX_API_VERSIONS,
        }

    def set_entry_latency_callback(self, cb: Callable[[float], None]) -> None:
        self._entry_latency_cb = cb

    def start(self, profile: dict[str, Any], *, replay_path: Path | None = None) -> bool:
        with self.lock:
            self._profile = {k: v for k, v in profile.items() if k not in {"api_secret", "refresh_token", "totp"}}
            token = str(self._profile.get("access_token") or "").strip()
            if replay_path and replay_path.exists():
                return self._start_replay(replay_path)
            if not token:
                log.warning("Upstox V3 feed: no access token")
                return False
            if self._streamer is not None:
                return False  # already running — duplicate prevention
            return self._start_websocket(token)

    def stop(self) -> None:
        with self.lock:
            self._stop_event.set()
            if self._streamer is not None:
                try:
                    self._streamer.disconnect()
                except Exception:
                    pass
                self._streamer = None
            self._connected = False

    def subscribe_tiers(self, *, active: list[str] | None = None, focus: list[str] | None = None, hot: list[str] | None = None) -> None:
        if active:
            self.tiers.set_active(active)
        if focus:
            self.tiers.set_focus(focus)
        if hot:
            self.tiers.set_hot(hot)
        keys = self._instrument_keys_for_tiers()
        if self._streamer is not None and keys:
            try:
                self._streamer.subscribe(keys, self._mode)
            except Exception as exc:
                log.warning("Subscribe failed: %s", type(exc).__name__)

    def _instrument_keys_for_tiers(self) -> list[str]:
        keys: set[str] = set(UPSTOX_INDEX_KEYS.values())
        for sym in self.tiers.hot | self.tiers.focus | self.tiers.active:
            key = self.instruments.resolve_upstox_key(sym)
            if key:
                keys.add(key)
        return sorted(keys)

    def _start_websocket(self, token: str) -> bool:
        try:
            import upstox_client

            conf = upstox_client.Configuration()
            conf.access_token = token
            api_client = upstox_client.ApiClient(conf)
            index_keys = list(UPSTOX_INDEX_KEYS.values())
            self._streamer = upstox_client.MarketDataStreamerV3(
                api_client, instrumentKeys=index_keys, mode=self._mode
            )
            self._streamer.on("open", self._on_open)
            self._streamer.on("message", self._on_message)
            self._streamer.on("error", self._on_error)
            self._streamer.on("close", self._on_close)
            self._streamer.auto_reconnect(True, 5, 50)
            self._streamer.connect()
            self.tiers.set_master(list(UPSTOX_INDEX_KEYS.keys()))
            return True
        except Exception as exc:
            log.exception("Upstox V3 WebSocket start failed: %s", exc)
            return False

    def _on_open(self, *_args) -> None:
        self._connected = True
        self._last_heartbeat_at = datetime.now(timezone.utc).isoformat()
        self.state.update(health_status="LIVE_DATA_CONNECTED", broker_name="UPSTOX")

    def _on_close(self, *_args) -> None:
        self._connected = False
        self._reconnect_count += 1

    def _on_error(self, error, *_args) -> None:
        log.warning("Upstox V3 feed error: %s", error)
        self._reconnect_count += 1

    def _on_message(self, msg: dict[str, Any]) -> None:
        received = datetime.now(timezone.utc)
        self._last_tick_at = received.isoformat()
        self._last_heartbeat_at = received.isoformat()
        feed_type = msg.get("type") or msg.get("type_")
        if feed_type == "market_info":
            return
        feeds = msg.get("feeds") or {}
        if not feeds:
            return
        normalized: dict[str, dict[str, Any]] = {}
        key_to_symbol = {v: k for k, v in UPSTOX_INDEX_KEYS.items()}
        for instrument_key, feed in feeds.items():
            symbol = key_to_symbol.get(instrument_key)
            if not symbol:
                bare = instrument_key.split("|")[-1].replace(" ", "").upper()
                symbol = bare
            quote = self._normalize_feed(instrument_key, feed, received)
            if quote.get("ltp") is not None:
                normalized[symbol] = quote
                exch_ts = quote.get("exchange_timestamp")
                if exch_ts:
                    try:
                        exch_dt = pd_timestamp(exch_ts)
                        self.latency.record_exchange_to_received(max(0.0, (received - exch_dt).total_seconds() * 1000))
                    except Exception:
                        pass
        if normalized:
            t0 = time.perf_counter()
            self.state.publish_quotes(normalized, "BROKER_LIVE")
            pub_ms = (time.perf_counter() - t0) * 1000
            self.latency.record_received_to_published(pub_ms)
            if self._entry_latency_cb:
                self._entry_latency_cb(pub_ms)

    def _normalize_feed(self, instrument_key: str, feed: dict[str, Any], received: datetime) -> dict[str, Any]:
        ltpc = feed.get("ltpc") or {}
        full = feed.get("fullFeed") or {}
        idx = full.get("indexFF") or full.get("marketFF") or {}
        ltpc = ltpc or idx.get("ltpc") or full.get("ltpc") or {}
        ltp = ltpc.get("ltp")
        previous = ltpc.get("cp")
        ltt = ltpc.get("ltt")
        ohlc_list = (idx.get("marketOHLC") or full.get("marketOHLC") or {}).get("ohlc") or []
        ohlc = ohlc_list[0] if ohlc_list else {}
        return {
            "broker": "UPSTOX",
            "instrument_key": instrument_key,
            "exchange": "NSE",
            "ltp": float(ltp) if ltp is not None else None,
            "previous_close": float(previous) if previous not in (None, 0, "0") else None,
            "previous_close_source": "BROKER_FEED" if previous not in (None, 0, "0") else "UNAVAILABLE",
            "open": ohlc.get("open"),
            "high": ohlc.get("high"),
            "low": ohlc.get("low"),
            "volume": (full.get("marketFF") or {}).get("vtt") or ohlc.get("vol"),
            "timestamp": ltt,
            "exchange_timestamp": ltt,
            "received_at": received,
        }

    def _start_replay(self, path: Path) -> bool:
        if self._replay_thread and self._replay_thread.is_alive():
            return False
        self._stop_event.clear()
        self._replay_thread = threading.Thread(
            target=self._run_replay, args=(path,), name="upstox-v3-replay", daemon=True
        )
        self._replay_thread.start()
        self._connected = True
        return True

    def _run_replay(self, path: Path) -> None:
        events = json.loads(path.read_text(encoding="utf-8"))
        for event in events:
            if self._stop_event.is_set():
                break
            if event.get("type") == "message":
                self._on_message(event.get("payload", {}))
            time.sleep(float(event.get("delay_s", 0.05)))
        self.state.update(data_source="BROKER_LIVE", health_status="REPLAY_VERIFIED")


def pd_timestamp(value: Any) -> datetime:
    import pandas as pd

    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    ts = pd.to_datetime(value, unit="ms", utc=True, errors="coerce")
    if pd.isna(ts):
        ts = pd.to_datetime(value, utc=True, errors="coerce")
    return ts.to_pydatetime()


# Backward-compatible alias
UpstoxFeedWorker = UpstoxV3FeedManager
