#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import json, sys
from pathlib import Path
import closing_bet_live_shadow_board_r161 as base

VERSION="CLOSEBET_LIVE_SHADOW_BOARD_R162_AMBIGUOUS_PROX_EXCLUSION_20260920"
EXCLUDED_PROXIMITY_FAMILIES={"AMBIGUOUS_OR_PENDING"}

_base_routes_for=base.routes_for
_base_board_for_row=base.board_for_row

def routes_for_without_ambiguous(evidence, pat):
    routes=_base_routes_for(evidence, pat)
    return {k:v for k,v in routes.items() if k not in EXCLUDED_PROXIMITY_FAMILIES}

def board_for_row_with_guard(r, evidence, response_map, macro):
    txt=_base_board_for_row(r, evidence, response_map, macro)
    old="※ n<3 비교 제외 · 3≤n<10 LOW CONFIDENCE · 종합 경로 판정/확률 아님"
    new=("※ proximity 비교: Ambiguous/Pending 제외 · n<3 제외 · "
         "3≤n<10 LOW CONFIDENCE · 종합 경로 판정/확률 아님")
    return txt.replace(old,new)

def _arg_value(flag, default):
    try:
        i=sys.argv.index(flag)
        return sys.argv[i+1]
    except Exception:
        return default

def main():
    base.VERSION=VERSION
    base.routes_for=routes_for_without_ambiguous
    base.board_for_row=board_for_row_with_guard
    base.main()

    out=Path(_arg_value("--output-dir","reports/live_shadow_board_r162"))
    meta_path=out/"meta.json"
    if meta_path.exists():
        m=json.loads(meta_path.read_text(encoding="utf-8"))
        m["version"]=VERSION
        m["ambiguous_pending_excluded_from_route_proximity"]=True
        m["excluded_route_proximity_families"]=sorted(EXCLUDED_PROXIMITY_FAMILIES)
        m["historical_path_counts_still_include_ambiguous_pending"]=True
        m["display_only_change"]=True
        m["candidate_membership_changed"]=False
        m["candidate_order_changed"]=False
        m["score_rank_changed"]=False
        m["candidate_filter_created"]=False
        m["production_logic_changed"]=False
        m["aggregate_route_score_created"]=False
        m["aggregate_route_classification_created"]=False
        meta_path.write_text(json.dumps(m,ensure_ascii=False,indent=2),encoding="utf-8")

if __name__=="__main__":
    main()
