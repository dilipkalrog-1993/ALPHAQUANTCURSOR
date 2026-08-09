"""Upstox BrokerAdapter — REST validation + V3 feed coordination."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import requests

from broker.adapter import BrokerAdapter, BrokerAdapterStatus
from broker.models import BrokerFunds, BrokerHolding, BrokerOrder, BrokerPosition
from market.upstox_v3_feed import UPSTOX_API_VERSIONS
from market.instrument_master import UPSTOX_INDEX_KEYS

BASE = "https://api.upstox.com/v2"


class UpstoxAdapter(BrokerAdapter):
    name = "UPSTOX"

    def __init__(self):
        self._credentials: dict[str, str] = {}
        self._status = BrokerAdapterStatus(broker="UPSTOX")
        self._profile: dict[str, Any] = {}
        self._checks: dict[str, bool] = {}

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._credentials.get('access_token', '')}",
            "Accept": "application/json",
        }

    def _check_token_expiry(self) -> tuple[bool, str]:
        raw = self._credentials.get("token_expiry_date") or self._credentials.get("token_expiry")
        if not raw:
            return True, ""
        try:
            expiry = datetime.fromisoformat(str(raw)).date()
            if expiry < datetime.now(timezone.utc).date():
                return False, "TOKEN EXPIRED"
        except (TypeError, ValueError):
            pass
        return True, ""

    def authenticate(self, credentials: dict[str, str]) -> BrokerAdapterStatus:
        self._credentials = {k: str(v or "").strip() for k, v in credentials.items()}
        started = time.perf_counter()
        checks = {
            "authentication": False,
            "profile": False,
            "quote_api": False,
            "funds": False,
            "holdings": False,
            "positions": False,
            "orders": False,
        }
        messages: list[str] = []
        token = self._credentials.get("access_token", "")
        if not token:
            return self._finalize(checks, "INVALID CREDENTIALS", "Access token is required", started)
        ok_exp, exp_msg = self._check_token_expiry()
        if not ok_exp:
            return self._finalize(checks, exp_msg, "Renew access token and reconnect", started)

        try:
            r = requests.get(f"{BASE}/user/profile", headers=self._headers(), timeout=15)
            if r.status_code in {401, 403}:
                return self._finalize(checks, "TOKEN EXPIRED", "Access token is invalid or expired", started)
            r.raise_for_status()
            self._profile = r.json().get("data") or {}
            checks["authentication"] = True
            checks["profile"] = bool(self._profile)
            if not checks["profile"]:
                messages.append("PROFILE ACCESS FAILED")
        except requests.ConnectionError:
            return self._finalize(checks, "NETWORK ERROR", "Check internet connection", started)
        except requests.Timeout:
            return self._finalize(checks, "NETWORK ERROR", "Broker request timed out", started)
        except requests.HTTPError:
            return self._finalize(checks, "PROFILE ACCESS FAILED", "Verify API credentials", started)

        try:
            qr = requests.get(
                f"{BASE}/market-quote/quotes",
                headers=self._headers(),
                params={"instrument_key": UPSTOX_INDEX_KEYS["NIFTY 50"]},
                timeout=15,
            )
            qr.raise_for_status()
            checks["quote_api"] = bool(qr.json().get("data"))
            if not checks["quote_api"]:
                messages.append("QUOTE ACCESS FAILED")
        except Exception:
            messages.append("QUOTE ACCESS FAILED")

        for key, path, attr in [
            ("funds", "/user/get-funds-and-margin", "funds"),
            ("holdings", "/portfolio/long-term-holdings", "holdings"),
            ("positions", "/portfolio/short-term-positions", "positions"),
            ("orders", "/order/retrieve-all", "orders"),
        ]:
            try:
                resp = requests.get(f"{BASE}{path}", headers=self._headers(), timeout=15)
                if resp.status_code == 403:
                    messages.append(f"{attr.upper()} ACCESS FAILED")
                    continue
                resp.raise_for_status()
                data = resp.json().get("data")
                checks[key] = data is not None
            except Exception:
                messages.append(f"{attr.upper()} ACCESS FAILED")

        if not checks["quote_api"]:
            return self._finalize(checks, "QUOTE ACCESS FAILED", "Enable market-quote permission", started)
        if messages:
            # Non-fatal read API failures still allow CONNECTED if auth+quote pass
            pass

        required = checks["authentication"] and checks["profile"] and checks["quote_api"]
        status_msg = "CONNECTED" if required else "CONNECTION FAILED"
        reason = messages[0] if messages and not required else ""
        return self._finalize(checks, status_msg if required else (reason or "CONNECTION FAILED"), "", started, connected=required)

    def _finalize(
        self,
        checks: dict[str, bool],
        status: str,
        action: str,
        started: float,
        connected: bool = False,
    ) -> BrokerAdapterStatus:
        self._checks = checks
        st = BrokerAdapterStatus(
            broker="UPSTOX",
            connected=connected,
            status="CONNECTED" if connected else "CONNECTION FAILED",
            message=status if connected else f"{status}. {action}".strip(),
            last_validated=datetime.now(timezone.utc).isoformat(),
            token_expiry=self._credentials.get("token_expiry_date"),
            capabilities=checks,
        )
        self._status = st
        return st

    def disconnect(self) -> None:
        self._credentials = {}
        self._status = BrokerAdapterStatus(broker="UPSTOX", connected=False, status="DISCONNECTED")

    def health(self) -> BrokerAdapterStatus:
        if not self._credentials.get("access_token"):
            return BrokerAdapterStatus(broker="UPSTOX", connected=False, status="NOT_CONNECTED")
        return self.authenticate(self._credentials)

    def get_profile(self) -> dict[str, Any]:
        return {"broker": "UPSTOX", "profile": self._profile or "N/A", "api_versions": UPSTOX_API_VERSIONS}

    def get_funds(self) -> BrokerFunds:
        try:
            resp = requests.get(f"{BASE}/user/get-funds-and-margin", headers=self._headers(), timeout=15)
            resp.raise_for_status()
            data = resp.json().get("data") or {}
            equity = data.get("equity") or {}
            return BrokerFunds(
                broker="UPSTOX",
                available_cash=equity.get("available_margin", "N/A"),
                used_margin=equity.get("used_margin", "N/A"),
                available_margin=equity.get("available_margin", "N/A"),
                collateral=equity.get("collateral", "N/A"),
            )
        except Exception:
            return BrokerFunds(broker="UPSTOX")

    def get_holdings(self) -> list[BrokerHolding]:
        try:
            resp = requests.get(f"{BASE}/portfolio/long-term-holdings", headers=self._headers(), timeout=15)
            resp.raise_for_status()
            return [
                BrokerHolding(
                    broker="UPSTOX",
                    symbol=item.get("tradingsymbol", "N/A"),
                    quantity=item.get("quantity", "N/A"),
                    average_price=item.get("average_price", "N/A"),
                    ltp=item.get("last_price", "N/A"),
                    pnl=item.get("pnl", "N/A"),
                )
                for item in (resp.json().get("data") or [])
            ]
        except Exception:
            return []

    def get_positions(self) -> list[BrokerPosition]:
        try:
            resp = requests.get(f"{BASE}/portfolio/short-term-positions", headers=self._headers(), timeout=15)
            resp.raise_for_status()
            return [
                BrokerPosition(
                    broker="UPSTOX",
                    symbol=item.get("tradingsymbol", "N/A"),
                    quantity=item.get("quantity", "N/A"),
                    average_price=item.get("average_price", "N/A"),
                    ltp=item.get("last_price", "N/A"),
                    pnl=item.get("pnl", "N/A"),
                    product=item.get("product", "N/A"),
                )
                for item in (resp.json().get("data") or [])
            ]
        except Exception:
            return []

    def get_orders(self) -> list[BrokerOrder]:
        try:
            resp = requests.get(f"{BASE}/order/retrieve-all", headers=self._headers(), timeout=15)
            resp.raise_for_status()
            return [
                BrokerOrder(
                    broker="UPSTOX",
                    order_id=str(item.get("order_id", "N/A")),
                    symbol=item.get("tradingsymbol", "N/A"),
                    side=item.get("transaction_type", "N/A"),
                    quantity=item.get("quantity", "N/A"),
                    price=item.get("price", "N/A"),
                    order_type=item.get("order_type", "N/A"),
                    status=item.get("status", "N/A"),
                )
                for item in (resp.json().get("data") or [])
            ]
        except Exception:
            return []

    def get_quote(self, symbol: str) -> dict[str, Any]:
        key = UPSTOX_INDEX_KEYS.get(symbol.upper().replace(".NS", ""))
        if not key:
            return {"symbol": symbol, "status": "INSTRUMENT_UNKNOWN"}
        try:
            resp = requests.get(
                f"{BASE}/market-quote/quotes",
                headers=self._headers(),
                params={"instrument_key": key},
                timeout=15,
            )
            resp.raise_for_status()
            raw = (resp.json().get("data") or {}).get(key) or {}
            ltp = raw.get("last_price") or raw.get("ltp")
            previous = (raw.get("ohlc") or {}).get("close")
            change = float(ltp) - float(previous) if ltp is not None and previous else None
            return {
                "broker": "UPSTOX",
                "symbol": symbol,
                "instrument_key": key,
                "ltp": ltp,
                "previous_close": previous,
                "change": change,
                "change_percent": (change / float(previous) * 100) if change is not None and previous else None,
                "source": "BROKER_SNAPSHOT",
                "received_at": datetime.now(timezone.utc).isoformat(),
            }
        except Exception as exc:
            return {"symbol": symbol, "status": "ERROR", "message": str(type(exc).__name__)}

    def place_order(self, order: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError("External live order placement is locked in this phase")
