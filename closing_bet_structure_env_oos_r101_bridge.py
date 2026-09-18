#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
R1.0.1 input bridge for closing_bet_structure_env_oos_r1.py
Research-only. Does not modify production search/score/rank/order.
Adapts current v49.76 global-performance artifact naming/schema to R1 reader.
"""
from __future__ import annotations
import sys
import pandas as pd
import numpy as np
import closing_bet_structure_env_oos_r1 as r1

_orig_find = r1.find
_orig_prep_evt = r1.prep_evt

def _find_bridge(root, name):
    p = _orig_find(root, name)
    if p is not None:
        return p
    if name == "v73_backtest_event_master.csv":
        for alt in (
            "v49_76_selected_enriched_outcomes.csv",
            "v49_76_global_canonical.csv",
            "v49_76_search_intent_enriched.csv",
        ):
            q = _orig_find(root, alt)
            if q is not None:
                print(f"R101_EVENT_SOURCE_BRIDGE {name} -> {alt}")
                return q
    return None

def _copy_if_missing(q: pd.DataFrame, target: str, sources: list[str]):
    if target in q.columns and q[target].notna().any():
        return
    for s in sources:
        if s in q.columns:
            q[target] = q[s]
            return
    if target not in q.columns:
        q[target] = np.nan

def _prep_evt_bridge(df: pd.DataFrame) -> pd.DataFrame:
    q = df.copy()

    # Current v49.76 outcome schema -> R1 canonical outcome aliases.
    _copy_if_missing(q, "ret1", ["ret_close_1d", "ret_next_close"])
    _copy_if_missing(q, "ret3", ["ret_close_3d"])
    _copy_if_missing(q, "ret5", ["ret_close_5d", "ret_close_hd"])
    _copy_if_missing(q, "ret10", ["ret_close_10d"])
    _copy_if_missing(q, "mfe", ["ret_max_high_5d", "ret_max_high_hd", "path_max_high_ret"])
    _copy_if_missing(q, "mae", ["ret_min_low_5d", "ret_min_low_hd", "path_min_low_ret"])
    _copy_if_missing(q, "evaluation_ret", ["ret_close_5d", "ret_close_hd", "rule35_pnl"])

    # Pattern / overlap identity.
    _copy_if_missing(q, "primary_formula", ["primary_strategy", "strategy", "mode"])
    _copy_if_missing(q, "formula_list", ["all_matched_strategies", "primary_strategy", "strategy"])
    _copy_if_missing(q, "formula_count", ["matched_strategy_count"])

    # Existing causal-ish context available in current v49.76 enriched outcome.
    # Keep descriptive only; never create a production gate.
    _copy_if_missing(q, "market_state", ["market_down_context", "i_mkt_regime", "market_m5_state_calc_t1", "market_m5_state_calc"])
    _copy_if_missing(q, "sector_state", ["sector_label"])
    _copy_if_missing(q, "context_alignment", ["event_theme_bucket"])
    _copy_if_missing(q, "market_turnover_ratio", ["entry_amount20_ratio"])
    _copy_if_missing(q, "sector_turnover_ratio", ["sector_peer_amount_b"])
    _copy_if_missing(q, "sector_breadth", ["sector_peer_positive_pct"])

    # Preserve unknown catalyst rather than infer one from hindsight.
    if "catalyst_state" not in q.columns:
        q["catalyst_state"] = "UNKNOWN_CURRENT_V4976_ARTIFACT"

    return _orig_prep_evt(q)

r1.find = _find_bridge
r1.prep_evt = _prep_evt_bridge

if __name__ == "__main__":
    print("CLOSEBET_STRUCTURE_ENV_OOS_R101_INPUT_BRIDGE active")
    r1.main()
