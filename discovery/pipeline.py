"""Discovery scan orchestrator."""

from __future__ import annotations

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable

from discovery.eligibility import EligibilityAudit, filter_eligible
from discovery.focus_universe import build_focus_universe, focus_limit_for_mode
from discovery.streamlit_focus_adapter import mandatory_symbols
from discovery.opportunity_ranker import rank_eligible
from discovery.structure_cache import get_cached_structure, store_structure_cache
from discovery.symbol_context import SymbolAnalysisContext
from discovery.strategy_gates import should_run_strategy


@dataclass
class DiscoveryTimings:
    history_cache: float = 0.0
    eligibility: float = 0.0
    opportunity_ranking: float = 0.0
    indicator_preparation: float = 0.0
    structure_preparation: float = 0.0
    full_strategies: float = 0.0
    scoring_v2: float = 0.0
    risk: float = 0.0
    total: float = 0.0

    def as_dict(self) -> dict[str, float]:
        return {
            "history_cache": round(self.history_cache, 3),
            "eligibility": round(self.eligibility, 3),
            "opportunity_ranking": round(self.opportunity_ranking, 3),
            "indicator_preparation": round(self.indicator_preparation, 3),
            "structure_preparation": round(self.structure_preparation, 3),
            "full_strategies": round(self.full_strategies, 3),
            "scoring_v2": round(self.scoring_v2, 3),
            "risk": round(self.risk, 3),
            "total": round(self.total, 3),
        }


@dataclass
class DiscoveryResult:
    eligible_count: int = 0
    focus_count: int = 0
    strategy_evaluated: int = 0
    strategy_signals: int = 0
    candidates: int = 0
    eligibility_audit: EligibilityAudit = field(default_factory=EligibilityAudit)
    focus_meta: dict[str, Any] = field(default_factory=dict)
    timings: DiscoveryTimings = field(default_factory=DiscoveryTimings)
    worker_count: int = 1


class DiscoveryPipeline:
    def __init__(self, app_module: Any):
        self.aq = app_module

    def _worker_count(self) -> int:
        cpu = os.cpu_count() or 2
        preferred = min(4, max(1, cpu // 2))
        return int(self.aq.WORKSPACE.preferences.get("discovery_worker_count") or preferred)

    def run(
        self,
        market_data: dict[str, Any],
        *,
        reject_fn: Callable[[str, str, str], None] | None = None,
        stage_counts: dict[str, int] | None = None,
    ) -> DiscoveryResult:
        t0 = time.perf_counter()
        result = DiscoveryResult()
        prefs = self.aq.WORKSPACE.preferences
        filters = prefs.get("filters", {}) or {}
        min_price = float(filters.get("price_range", [self.aq.CONFIG.get("MIN_PRICE", 20), 100000])[0])
        max_price = float(filters.get("price_range", [20, 100000])[1])
        min_vol = float(filters.get("minimum_volume", self.aq.CONFIG.get("MIN_AVG_VOLUME", 100000)))

        t1 = time.perf_counter()
        eligible, audit = filter_eligible(
            market_data, min_price=min_price, max_price=max_price, min_avg_volume=min_vol,
        )
        result.eligibility_audit = audit
        result.eligible_count = audit.eligible_count
        result.timings.eligibility = time.perf_counter() - t1

        t2 = time.perf_counter()
        ranked = rank_eligible(eligible)
        result.timings.opportunity_ranking = time.perf_counter() - t2

        mandatory = mandatory_symbols(self.aq.st.session_state, prefs)
        limit = focus_limit_for_mode(prefs)
        if limit and limit >= 9999:
            limit = None
        focus_rows, focus_meta = build_focus_universe(
            ranked, limit=limit, mandatory=mandatory,
            min_opportunity_score=float(prefs.get("discovery_min_opportunity_score") or 8.0),
        )
        result.focus_count = len(focus_rows)
        result.focus_meta = focus_meta

        workers = self._worker_count()
        result.worker_count = workers
        strategy_signals = 0
        t3 = time.perf_counter()

        # Evaluation mutates the UI adapter's session state, so it must remain
        # on Streamlit's main script thread. Workers are for pure data tasks.
        for row in focus_rows:
            if self._evaluate_symbol(row, reject_fn, stage_counts):
                strategy_signals += 1

        result.strategy_evaluated = len(focus_rows)
        result.strategy_signals = strategy_signals
        result.candidates = len(self.aq.st.session_state.get("trade_candidates", {}))
        result.timings.full_strategies = time.perf_counter() - t3
        result.timings.total = time.perf_counter() - t0
        return result

    def _evaluate_symbol(
        self,
        row: dict[str, Any],
        reject_fn: Callable[[str, str, str], None] | None,
        stage_counts: dict[str, int] | None,
    ) -> bool:
        sym = row["symbol"]
        df = row["dataframe"]
        ss = self.aq.st.session_state
        cache = ss.setdefault("indicator_frame_cache", {})
        signature = (len(df), str(df.index[-1]) if len(df) else "", float(df.iloc[-1]["Close"]))
        cached = cache.get(sym)
        if cached and cached[0] == signature:
            df = cached[1].copy()
        elif "EMA20" not in df.columns:
            df = self.aq.calculate_indicators(df) or df
            if df is not None:
                cache[sym] = (signature, df.copy())

        ctx = SymbolAnalysisContext.from_dataframe(sym, df)
        stock = self.aq.get_stock(sym)
        stock.set_dataframe(df)
        ctx.attach_to_stock(stock)

        cached_struct = get_cached_structure(sym, df)
        self.aq.calculate_trade_quality(stock)
        if cached_struct:
            stock.market.update(cached_struct.get("market", {}))
            stock.patterns.update(cached_struct.get("patterns", {}))
        else:
            self.aq.update_market_structure(stock)
            store_structure_cache(sym, df, {"market": dict(stock.market), "patterns": dict(stock.patterns)})

        if stage_counts is not None:
            stage_counts["Passed fast screen"] = stage_counts.get("Passed fast screen", 0) + 1
            stage_counts["Strategy evaluated"] = stage_counts.get("Strategy evaluated", 0) + 1

        self.aq.assign_sector(stock)
        before = {k for k, v in ss.trade_candidates.items() if v.symbol == sym}
        try:
            for strategy in ss.strategy_registry:
                if not strategy.enabled:
                    continue
                if not should_run_strategy(strategy.name, stock):
                    continue
                strategy.function(stock)
            after = {k for k, v in ss.trade_candidates.items() if v.symbol == sym}
            if after - before:
                self.aq.run_batch1_signal_engines(stock)
                self.aq.run_batch2_signal_engines(stock)
                if stage_counts is not None:
                    stage_counts["Strategy signalled"] = stage_counts.get("Strategy signalled", 0) + 1
                for trade in list(ss.trade_candidates.values()):
                    if trade.symbol != sym:
                        continue
                    self.aq.validate_trade_candidate(stock, trade)
                    self.aq.apply_sector_bonus(stock, trade)
                    self.aq.calculate_position_size(trade)
                return True
        except Exception as exc:
            if reject_fn:
                reject_fn(sym, "STRATEGY_ERROR", f"{type(exc).__name__}: {exc}")
            return False
        if reject_fn:
            reject_fn(sym, "NO_STRATEGY_SIGNAL", "No enabled strategy setup triggered")
        return False


def run_discovery_scan(app_module: Any, **kwargs: Any) -> DiscoveryResult:
    market_data = app_module.st.session_state.market_data
    return DiscoveryPipeline(app_module).run(market_data, **kwargs)
