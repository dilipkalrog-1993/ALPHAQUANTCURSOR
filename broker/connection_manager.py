"""Multi-broker connection manager."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from broker.adapter import BrokerAdapter, BrokerAdapterStatus
from broker.credential_store import CredentialStore
from broker.registry import BROKER_REGISTRY, get_adapter_class

_DEFAULT_PATH = Path(__file__).resolve().parent.parent / "data" / "broker_connections.json"


class BrokerConnectionManager:
    """Manage multiple broker profiles, defaults, and validation state."""

    def __init__(self, storage_path: Path | None = None):
        self.storage_path = storage_path or _DEFAULT_PATH
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self.credentials = CredentialStore()
        self._adapters: dict[str, BrokerAdapter] = {}
        self._connections = self._load()

    def _load(self) -> dict[str, Any]:
        if not self.storage_path.exists():
            return {"profiles": {}, "default_market_data_broker": None, "default_execution_broker": None}
        try:
            return json.loads(self.storage_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return {"profiles": {}, "default_market_data_broker": None, "default_execution_broker": None}

    def _save(self) -> None:
        temp = self.storage_path.with_suffix(".tmp")
        temp.write_text(json.dumps(self._connections, indent=2, default=str), encoding="utf-8")
        temp.replace(self.storage_path)

    def list_profiles(self) -> dict[str, dict[str, Any]]:
        profiles = {}
        for name, profile in (self._connections.get("profiles") or {}).items():
            profiles[name] = self.credentials.masked_profile(profile)
        return profiles

    def save_profile(self, profile: dict[str, Any]) -> dict[str, Any]:
        name = profile.get("name") or profile.get("profile_name") or "default"
        profile = dict(profile)
        profile["name"] = name
        profile["updated_at"] = datetime.now(timezone.utc).isoformat()
        public = self.credentials.store_profile_secrets(name, profile)
        self._connections.setdefault("profiles", {})[name] = public
        self._save()
        return public

    def get_profile(self, name: str, *, include_secrets: bool = False) -> dict[str, Any] | None:
        profile = (self._connections.get("profiles") or {}).get(name)
        if not profile:
            return None
        if include_secrets:
            return self.credentials.merge_secrets(name, profile)
        return self.credentials.masked_profile(profile)

    def delete_profile(self, name: str) -> bool:
        removed = (self._connections.get("profiles") or {}).pop(name, None)
        self._adapters.pop(name, None)
        self._save()
        return removed is not None

    def set_defaults(self, *, market_data: str | None = None, execution: str | None = None) -> None:
        if market_data is not None:
            self._connections["default_market_data_broker"] = market_data
        if execution is not None:
            self._connections["default_execution_broker"] = execution
        self._save()

    def get_adapter(self, profile_name: str) -> BrokerAdapter | None:
        if profile_name in self._adapters:
            return self._adapters[profile_name]
        profile = self.get_profile(profile_name, include_secrets=True)
        if not profile:
            return None
        cls = get_adapter_class(profile.get("broker_name", ""))
        if cls is None:
            return None
        adapter = cls() if cls.__name__ != "NotImplementedBrokerAdapter" else cls(profile.get("broker_name", "BROKER"))
        self._adapters[profile_name] = adapter
        return adapter

    def connect(self, profile_name: str) -> BrokerAdapterStatus:
        adapter = self.get_adapter(profile_name)
        profile = self.get_profile(profile_name, include_secrets=True) or {}
        if adapter is None:
            status = BrokerAdapterStatus(
                broker=profile.get("broker_name", profile_name),
                connected=False,
                status="NOT_IMPLEMENTED" if profile.get("broker_name", "").upper() not in {"UPSTOX"} else "PROFILE_NOT_FOUND",
                message="Broker adapter unavailable",
            )
            self._record_validation(profile_name, status)
            return status
        status = adapter.authenticate(profile)
        self._record_validation(profile_name, status)
        return status

    def disconnect(self, profile_name: str) -> None:
        adapter = self._adapters.pop(profile_name, None)
        if adapter:
            adapter.disconnect()
        profile = (self._connections.get("profiles") or {}).get(profile_name)
        if profile:
            profile["connection_status"] = "DISCONNECTED"
            profile["disconnected_at"] = datetime.now(timezone.utc).isoformat()
            self._save()

    def _record_validation(self, profile_name: str, status: BrokerAdapterStatus) -> None:
        profile = self._connections.setdefault("profiles", {}).setdefault(profile_name, {})
        now = datetime.now(timezone.utc).isoformat()
        profile["connection_status"] = status.status
        profile["connected"] = status.connected
        profile["last_message"] = status.message
        if status.connected:
            profile["last_successful_validation"] = now
        else:
            profile["last_failed_validation"] = now
        self._save()

    def connection_summary(self) -> list[dict[str, Any]]:
        rows = []
        for broker_key, meta in BROKER_REGISTRY.items():
            matching = [
                (name, prof)
                for name, prof in (self._connections.get("profiles") or {}).items()
                if str(prof.get("broker_name", "")).upper().startswith(broker_key.split()[0])
            ]
            if matching:
                for name, prof in matching:
                    rows.append({
                        "broker": meta["display"],
                        "profile": name,
                        "implementation": meta["status"],
                        "status": prof.get("connection_status", "NOT_CONNECTED"),
                        "connected": bool(prof.get("connected")),
                        "last_successful_validation": prof.get("last_successful_validation"),
                        "last_failed_validation": prof.get("last_failed_validation"),
                        "default_market_data": self._connections.get("default_market_data_broker") == name,
                        "default_execution": self._connections.get("default_execution_broker") == name,
                    })
            else:
                rows.append({
                    "broker": meta["display"],
                    "profile": None,
                    "implementation": meta["status"],
                    "status": "NOT_CONNECTED",
                    "connected": False,
                })
        return rows

    def save_and_connect(self, profile: dict[str, Any]) -> BrokerAdapterStatus:
        saved = self.save_profile(profile)
        return self.connect(saved["name"])
