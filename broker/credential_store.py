"""Secure local credential storage — secrets never returned in UI snapshots."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

_DEFAULT_DIR = Path(os.environ.get("ALPHAQUANT_DATA_DIR", Path(__file__).resolve().parent.parent / "data"))


class CredentialStore:
    """Store broker secrets outside workspace preferences JSON."""

    def __init__(self, path: Path | None = None):
        self.path = path or (_DEFAULT_DIR / "broker_secrets.json")
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _load(self) -> dict[str, dict[str, str]]:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return {}

    def _save(self, payload: dict[str, dict[str, str]]) -> None:
        temp = self.path.with_suffix(".tmp")
        temp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        os.chmod(temp, 0o600)
        temp.replace(self.path)
        os.chmod(self.path, 0o600)

    SECRET_KEYS = ("api_key", "api_secret", "access_token", "refresh_token", "totp")

    def store_profile_secrets(self, profile_name: str, profile: dict[str, Any]) -> dict[str, Any]:
        secrets = self._load()
        bucket = secrets.setdefault(profile_name, {})
        public = dict(profile)
        for key in self.SECRET_KEYS:
            value = str(profile.get(key) or "").strip()
            if value:
                bucket[key] = value
            public[key] = self.mask(value) if value else ""
        self._save(secrets)
        return public

    def load_profile_secrets(self, profile_name: str) -> dict[str, str]:
        return dict(self._load().get(profile_name, {}))

    def merge_secrets(self, profile_name: str, public_profile: dict[str, Any]) -> dict[str, Any]:
        merged = dict(public_profile)
        secrets = self.load_profile_secrets(profile_name)
        for key in self.SECRET_KEYS:
            masked = str(public_profile.get(key) or "")
            if masked and not masked.startswith("****") and masked:
                secrets[key] = masked
            if secrets.get(key):
                merged[key] = secrets[key]
            elif masked.startswith("****"):
                merged[key] = secrets.get(key, "")
        return merged

    @staticmethod
    def mask(value: str) -> str:
        value = str(value or "")
        if not value:
            return ""
        if len(value) <= 4:
            return "****"
        return f"****{value[-4:]}"

    def masked_profile(self, profile: dict[str, Any]) -> dict[str, Any]:
        out = dict(profile)
        for key in self.SECRET_KEYS:
            if out.get(key):
                out[key] = self.mask(str(out[key]))
        return out
