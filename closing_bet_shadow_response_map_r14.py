#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

VERSION="CLOSEBET_SHADOW_RESPONSE_MAP_R14_20260919"

def read_csv(p):
    for enc in ("utf-8-sig","utf-8","cp949"):
        try:return pd.read_csv(p,encoding=enc,low_memory=False)
        except Exception:pass
    return pd.DataFrame()

def safe_num(s):
    return pd.to_numeric(s,errors="coerce")

def med(g,c):
    if c not in g:return np.nan
    x=safe_num(g[c]).dropna()
    return float(x.median()) if len(x) else np.nan

def count_family(df,pat,fam):
    return int(((df["pattern"]==pat)&(df["refined_family_r13"]==fam)).sum())

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--source-root",default="source_r13")
    ap.add_argument("--output-dir",default="reports/shadow_response_map_r14")
    ap.add_argument("--source-r13-run-id",default="")
    a=ap.parse_args()

    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)
    xs=list(Path(a.source_root).rglob("shadow_outcome_refined_events.csv"))
    if not xs: raise SystemExit("R13_EVENTS_NOT_FOUND")
    df=read_csv(xs[0])
    if df.empty: raise SystemExit("R13_EVENTS_EMPTY")

    # ATR provenance audit:
    # R1.2 used alias fallback ['entry_atr_pct','atr_pct','atr'].
    # Generic raw 'atr' is price-unit ATR, not a percent. Since provenance was lost in the
    # transformed R1.2 file, R1.4 MUST NOT use entry_atr_pct as evidence unless it is known-safe.
    # In this replay, B2 values are on the percent-like scale while most other patterns clearly
    # contain price-unit leakage. We avoid any pattern-specific recovery heuristic and exclude ATR
    # from ALL response-map evidence until source-level provenance is re-extracted.
    df["entry_atr_pct_r14_status"]="EXCLUDED_PROVENANCE_AMBIGUOUS"
    df["entry_atr_pct_r14"]=np.nan

    # Evidence map is descriptive only. No score, rank, filtering or thresholds.
    evidence=[]
    for pat in ["C","B1","B2","I"]:
        pg=df[df["pattern"]==pat].copy()
        if pg.empty: continue
        for fam in ["EARLY_WIN_HELD","EARLY_WIN_GIVEBACK","SHAKEOUT_THEN_RECOVERY","EARLY_STOP_SLOW_OR_NO_RECOVERY"]:
            g=pg[pg["refined_family_r13"]==fam]
            if g.empty: continue
            evidence.append({
                "pattern":pat,
                "response_family":fam,
                "n":len(g),
                "independent_signal_dates":g["signal_date"].nunique(),
                "close_loc_median":med(g,"entry_close_loc_pct"),
                "vol20_median":med(g,"entry_vol20_ratio"),
                "amount20_median":med(g,"entry_amount20_ratio"),
                "ma20_median":med(g,"entry_ma20_dist_pct"),
                "ma60_median":med(g,"entry_ma60_dist_pct"),
                "ma224_median":med(g,"entry_ma224_dist_pct"),
                "wick_median":med(g,"entry_upper_wick_pct"),
                "ret5_median":med(g,"entry_ret5_pct"),
                "ret10max_median":med(g,"ret_max_high_10d"),
                "ret10close_median":med(g,"ret_close_10d"),
                "atr_status":"EXCLUDED_PROVENANCE_AMBIGUOUS",
            })
    ev=pd.DataFrame(evidence)
    ev.to_csv(out/"response_map_evidence.csv",index=False,encoding="utf-8-sig")

    # Predeclared response-language map based on R1.3 observed route families.
    # These are watchpoints, not trade instructions.
    response_rows=[]
    definitions={
        "C":{
            "dominant":"EARLY_WIN_GIVEBACK / SHAKEOUT_THEN_RECOVERY",
            "watch":[
                "초기 +5% 도달 자체보다 D3~D5 유지 여부를 별도로 본다",
                "초기 흔들림이 있어도 MA20/MA60 구조와 유동성 소멸 여부를 계속 관찰한다",
                "최근 5일 모멘텀과 MA20 위치는 OOS에서 상대적으로 구분력이 유지됐는지 함께 표시한다",
                "빠른 상승 뒤 종가 약화·윗꼬리 확대는 giveback 경로와 분리해 기록한다",
            ],
        },
        "B1":{
            "dominant":"EARLY_WIN_GIVEBACK / SHAKEOUT_THEN_RECOVERY",
            "watch":[
                "초기 반등이 나와도 D5까지 유지되는지 따로 본다",
                "초기 stop 자체를 즉시 구조 실패로 동일시하지 않는다",
                "MA60/224 훼손의 회복 여부와 유동성 재진입을 함께 기록한다",
                "기존 MA60 Winner-reference는 R1.2 OOS에서 역전 관찰이 있어 단독 근거로 쓰지 않는다",
            ],
        },
        "B2":{
            "dominant":"EARLY_WIN_GIVEBACK",
            "watch":[
                "빠른 반등 후 D5 giveback이 많은 패턴으로 표시한다",
                "반등 강도보다 이후 유지력과 거래대금 지속 여부를 별도로 관찰한다",
                "초기 stop 뒤 회복 사례도 있으나 표본이 작아 보조 정보로만 표시한다",
                "ATR은 provenance 수정 전까지 Response Map에서 제외한다",
            ],
        },
        "I":{
            "dominant":"SHAKEOUT_THEN_RECOVERY",
            "watch":[
                "초기 stop을 곧바로 최종 실패로 해석하지 않는다",
                "MA60 복구/유지와 균형 잡힌 유동성을 함께 관찰한다",
                "당일 종가 강도 하나만으로 후속 경로를 판단하지 않는다",
                "과도한 유동성·이격보다 조용한 재시동 여부를 설명형 태그로 표시한다",
            ],
        },
    }

    for pat,cfg in definitions.items():
        pg=df[df["pattern"]==pat]
        if pg.empty: continue
        for fam in ["EARLY_WIN_HELD","EARLY_WIN_GIVEBACK","SHAKEOUT_THEN_RECOVERY","EARLY_STOP_SLOW_OR_NO_RECOVERY","AMBIGUOUS_OR_PENDING"]:
            response_rows.append({
                "pattern":pat,
                "response_family":fam,
                "pattern_n":len(pg),
                "family_n":count_family(df,pat,fam),
                "dominant_observed_routes":cfg["dominant"],
                "response_watchlist":" / ".join(cfg["watch"]),
                "atr_evidence":"EXCLUDED_PROVENANCE_AMBIGUOUS",
                "use":"SHADOW_DESCRIPTION_ONLY",
            })
    rm=pd.DataFrame(response_rows)
    rm.to_csv(out/"pattern_response_map.csv",index=False,encoding="utf-8-sig")

    # Event-level observer enrichment, preserving membership and order.
    enriched=df.copy()
    enrich_map={p:" / ".join(v["watch"]) for p,v in definitions.items()}
    enriched["response_map_r14"]=enriched["pattern"].map(enrich_map).fillna("GENERIC_STRUCTURE_ONLY")
    enriched["atr_response_evidence_r14"]="EXCLUDED_PROVENANCE_AMBIGUOUS"
    enriched.to_csv(out/"shadow_response_map_events.csv",index=False,encoding="utf-8-sig")

    # Human-readable board.
    lines=[
        "# Closing Bet SHADOW Response Map R1.4",
        "",
        f"- source R1.3 run: `{a.source_r13_run_id}`",
        f"- events: {len(df)}",
        f"- independent signal dates: {df['signal_date'].nunique()}",
        "",
        "## ATR provenance correction",
        "- R1.2 allowed generic `atr` as a fallback for `entry_atr_pct`.",
        "- Generic `atr` is price-unit ATR and cannot be treated as a percentage.",
        "- Because R1.2 no longer retains which alias supplied each row, R1.4 excludes ATR from ALL response evidence rather than guessing a conversion.",
        "- A later source-level recapture may restore ATR% after provenance is explicit.",
        "",
        "## Pattern response maps",
    ]
    for pat,cfg in definitions.items():
        pg=df[df["pattern"]==pat]
        if pg.empty: continue
        lines.append(f"### {pat} (n={len(pg)})")
        fam=pg["refined_family_r13"].value_counts()
        for k,v in fam.items():
            lines.append(f"- {k}: {int(v)}")
        lines.append(f"- dominant observed routes: {cfg['dominant']}")
        for w in cfg["watch"]:
            lines.append(f"- WATCH: {w}")
        lines.append("")
    lines += [
        "## Guardrails",
        "- Research-only SHADOW observer.",
        "- No candidate filtering.",
        "- No score/rank change.",
        "- No production order change.",
        "- No new optimized thresholds.",
        "- Outcome groups remain retrospective descriptions, not probabilities.",
        "- ATR is deliberately unavailable in R1.4 until provenance is repaired at source.",
    ]
    (out/"REPORT.md").write_text("\n".join(lines),encoding="utf-8")

    meta={
        "version":VERSION,"status":"PASS","research_only":True,
        "source_r13_run_id":a.source_r13_run_id,
        "events":len(df),"independent_signal_dates":int(df["signal_date"].nunique()),
        "atr_provenance_issue_confirmed":True,
        "atr_generic_raw_alias_forbidden":True,
        "atr_used_in_response_map":False,
        "atr_fix_semantics":"EXCLUDE_UNTIL_SOURCE_PROVENANCE_EXPLICIT",
        "candidate_membership_changed":False,
        "candidate_order_changed":False,
        "score_rank_changed":False,
        "candidate_filter_created":False,
        "production_logic_changed":False,
        "new_threshold_optimization":False,
        "same_sample_tuning":False,
        "response_map_patterns":["C","B1","B2","I"],
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps(meta,ensure_ascii=False,indent=2))
    print(ev.to_string(index=False))

if __name__=="__main__":
    main()
