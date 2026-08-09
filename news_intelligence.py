"""Isolated news intelligence service for AlphaQuant."""

from __future__ import annotations

import json
import logging
import threading
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

log = logging.getLogger("alphaquant.news")


class NewsManager:
    """Fetch, cache, and score market news without blocking trading."""

    RSS_FEEDS = (
        ("Moneycontrol Markets", "https://www.moneycontrol.com/rss/marketreports.xml"),
        ("Economic Times Markets", "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"),
    )

    def __init__(self, cache_path: Path):
        self.cache_path = Path(cache_path)
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.settings: dict[str, Any] = {"enabled": False, "provider": "RSS"}
        self.state: dict[str, Any] = self._load()
        self._last_critical_alert: datetime | None = None

    def _load(self) -> dict[str, Any]:
        default = {
            "articles": [],
            "clusters": [],
            "briefing_history": [],
            "provider_status": "IDLE",
            "last_error": None,
            "reason": None,
            "stale": True,
            "last_successful_fetch": None,
            "alerted_ids": [],
        }
        try:
            if self.cache_path.exists():
                saved = json.loads(self.cache_path.read_text(encoding="utf-8"))
                if isinstance(saved, dict):
                    default.update(saved)
        except (OSError, ValueError, TypeError) as exc:
            default["last_error"] = str(exc)
        return default

    def _persist(self) -> None:
        temp = self.cache_path.with_suffix(".tmp")
        temp.write_text(json.dumps(self.state, default=str, indent=2), encoding="utf-8")
        temp.replace(self.cache_path)

    def configure(self, **changes: Any) -> None:
        with self.lock:
            self.settings.update({k: v for k, v in changes.items() if k != "api_key"})
            self._api_key = changes.get("api_key", getattr(self, "_api_key", ""))

    def request_refresh(self) -> None:
        if not self.settings.get("enabled", False):
            return
        threading.Thread(target=self._refresh, name="news-refresh", daemon=True).start()

    def _refresh(self) -> None:
        with self.lock:
            self.state["provider_status"] = "FETCHING"
        try:
            articles = self._fetch_rss_articles()
            if not articles:
                raise RuntimeError("All configured news providers returned no usable articles")
            with self.lock:
                self.state["articles"] = articles
                self.state["provider_status"] = "OK"
                self.state["stale"] = False
                self.state["last_error"] = None
                self.state["reason"] = None
                self.state["last_successful_fetch"] = datetime.now(timezone.utc).isoformat()
                self._persist()
        except Exception as exc:
            log.exception("News refresh failed")
            with self.lock:
                self.state["provider_status"] = "DEGRADED"
                self.state["reason"] = "PROVIDER_FAILURE"
                self.state["last_error"] = f"{type(exc).__name__}: {exc}"
                self.state["stale"] = True
                self._persist()

    def _fetch_rss_articles(self) -> list[dict[str, Any]]:
        articles: list[dict[str, Any]] = []
        headers = {"User-Agent": "AlphaQuant/3.0 (+https://github.com/dilipkalro-hash/AlphaQuant)"}
        for source, url in self.RSS_FEEDS:
            try:
                response = requests.get(url, headers=headers, timeout=12)
                response.raise_for_status()
                root = ET.fromstring(response.content)
                for item in root.findall(".//item")[:15]:
                    title = (item.findtext("title") or "").strip()
                    link = (item.findtext("link") or "").strip()
                    description = (item.findtext("description") or "").strip()
                    pub = item.findtext("pubDate") or datetime.now(timezone.utc).isoformat()
                    symbols = self._extract_symbols(f"{title} {description}")
                    articles.append(self._score_article(title, description, link, pub, source, symbols))
            except Exception as exc:
                log.warning("RSS feed failed %s: %s", source, exc)
        # A provider may syndicate the same story under several feeds.  Stable
        # URL/headline keys prevent duplicated sentiment weight.
        unique: dict[str, dict[str, Any]] = {}
        for article in articles:
            key = (article.get("url") or article.get("headline") or "").strip().lower()
            if key:
                unique.setdefault(key, article)
        return list(unique.values())[:40]

    @staticmethod
    def _extract_symbols(text: str) -> list[str]:
        known = {
            "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN", "NIFTY", "BANKNIFTY",
            "TATAMOTORS", "MARUTI", "ITC", "HINDUNILVR", "SUNPHARMA", "WIPRO", "AXISBANK",
        }
        upper = text.upper()
        return sorted({sym for sym in known if sym in upper})

    def _score_article(
        self,
        headline: str,
        description: str,
        url: str,
        published_at: str,
        source: str,
        symbols: list[str],
    ) -> dict[str, Any]:
        text = f"{headline} {description}".lower()
        urgency = "CRITICAL" if any(w in text for w in ("crash", "halt", "fraud", "default", "sebi ban")) else "LOW"
        if urgency != "CRITICAL" and any(w in text for w in ("surge", "plunge", "record", "policy", "rate cut")):
            urgency = "MEDIUM"
        sentiment = "NEGATIVE" if any(w in text for w in ("fall", "drop", "weak", "loss", "down")) else (
            "POSITIVE" if any(w in text for w in ("gain", "rise", "strong", "beat", "up")) else "NEUTRAL"
        )
        relevance = min(100, 20 + 15 * len(symbols))
        risk = 80 if urgency == "CRITICAL" else 40 if sentiment == "NEGATIVE" else 10
        try:
            parsed = parsedate_to_datetime(published_at)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            published_at = parsed.astimezone(timezone.utc).isoformat()
        except (TypeError, ValueError, OverflowError):
            try:
                published_at = datetime.fromisoformat(str(published_at).replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
            except (TypeError, ValueError):
                published_at = datetime.now(timezone.utc).isoformat()
        return {
            "headline": headline,
            "description": description[:500],
            "url": url or f"https://www.google.com/search?q={quote(headline)}",
            "published_at": published_at,
            "source": source,
            "category": "MARKET",
            "urgency": urgency,
            "sentiment": sentiment,
            "related_symbols": symbols,
            "related_sectors": [],
            "news_relevance_score": relevance,
            "news_risk_score": risk,
            "score_reasons": [f"urgency={urgency}", f"sentiment={sentiment}"],
        }

    def candidate_effect(self, symbol: str) -> dict[str, Any]:
        base = str(symbol).replace(".NS", "").upper()
        with self.lock:
            articles = list(self.state.get("articles", []))
        related = [a for a in articles if base in (a.get("related_symbols") or [])]
        if not related and not self.settings.get("enabled", False):
            return {
                "news_status": "DISABLED",
                "news_relevance": 0,
                "news_sentiment": 0,
                "news_risk": 0,
                "news_summary": "",
                "news_timestamp": None,
                "news_sources": [],
                "news_effect_on_confidence": 0,
                "news_veto_reason": None,
            }
        if not related:
            return {
                "news_status": "NO_NEWS",
                "news_relevance": 0,
                "news_sentiment": 0,
                "news_risk": 0,
                "news_summary": "",
                "news_timestamp": None,
                "news_sources": [],
                "news_effect_on_confidence": 0,
                "news_veto_reason": None,
            }
        top = max(related, key=lambda a: a.get("news_relevance_score", 0))
        relevance = int(top.get("news_relevance_score", 0))
        risk = int(top.get("news_risk_score", 0))
        sentiment = top.get("sentiment", "NEUTRAL")
        effect = 5 if sentiment == "POSITIVE" else -5 if sentiment == "NEGATIVE" else 0
        veto = None
        if top.get("urgency") == "CRITICAL" and risk >= 70:
            veto = f"News risk veto: {top.get('headline', 'critical headline')}"
        return {
            "news_status": "ACTIVE",
            "news_relevance": relevance,
            "news_sentiment": {"POSITIVE": 1, "NEGATIVE": -1, "NEUTRAL": 0}.get(sentiment, 0),
            "news_risk": risk,
            "news_summary": top.get("description", "")[:200],
            "news_timestamp": top.get("published_at"),
            "news_sources": [top.get("source", "")],
            "news_effect_on_confidence": effect,
            "news_veto_reason": veto,
        }

    def briefing(self, kind: str = "INTRADAY") -> str:
        with self.lock:
            articles = list(self.state.get("articles", []))
        prefix = kind.upper()
        if not self.settings.get("enabled", False):
            return f"{prefix} BRIEF. News Intelligence is disabled in settings."
        if not articles:
            return (
                f"{prefix} BRIEF. No cached headlines yet. "
                "Use REFRESH NEWS on the Market → News tab."
            )
        critical = [a for a in articles if a.get("urgency") == "CRITICAL"]
        lead = critical[0]["headline"] if critical else articles[0]["headline"]
        return f"{prefix} BRIEF. Lead headline: {lead}. {len(articles)} articles cached."

    def claim_critical_alert(self, cooldown_minutes: int = 30) -> str | None:
        now = datetime.now(timezone.utc)
        with self.lock:
            if self._last_critical_alert:
                elapsed = (now - self._last_critical_alert).total_seconds() / 60
                if elapsed < cooldown_minutes:
                    return None
            for article in self.state.get("articles", []):
                if article.get("urgency") != "CRITICAL":
                    continue
                article_id = article.get("url") or article.get("headline")
                if article_id in self.state.setdefault("alerted_ids", []):
                    continue
                self.state["alerted_ids"].append(article_id)
                self._last_critical_alert = now
                self._persist()
                return f"Critical alert. {article.get('headline', '')}"
        return None

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return json.loads(json.dumps(self.state, default=str))
