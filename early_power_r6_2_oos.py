#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EARLY POWER R6.2 — DATA_PENDING / exact-date backfill infrastructure.

IMPORTANT:
- Imports and preserves R6_FROZEN_V1 exactly from early_power_r6_oos.py.
- Does not change eligibility, ranking, production logic, or order logic.
- If authoritative marcap has not reached the REAL_FULL signal date:
    * ORIGINAL_TOP3 cohort is preserved immediately.
    * EARLY_POWER status is DATA_PENDING, not silently backdated.
- When marcap later contains that exact signal date:
    * pending EARLY_POWER cohort is reconstructed for that exact historical date.
    * no future outcome is used in selection.
"""
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd
import early_power_r6_oos as base

REVISION="EARLY_POWER_R6_2_PENDING_BACKFILL_20261005"
INFRA_REVISION="R6_2_EXACT_DATE_PENDING_BACKFILL"

def read_optional(path, dtype=None):
    if path and Path(path).exists():
        return pd.read_csv(path, dtype=dtype, low_memory=False)
    return pd.DataFrame()

def iso(v):
    return pd.Timestamp(v).normalize().date().isoformat()

def add_original_rows(orig, revision):
    q=orig.copy()
    if q.empty:return q
    q["revision"]=revision
    q["research_only"]=True
    q["cohort_key"]=q.signal_date.astype(str)+"|"+q.lane.astype(str)+"|"+q.code.astype(str)
    q["price_calendar_has_signal_date"]=False
    return q

def early_rows(px, sd):
    q=base.build_early_power(px, pd.Timestamp(sd).normalize())
    if q is None or q.empty:
        return pd.DataFrame()
    q=q.copy()
    q["revision"]=REVISION
    q["research_only"]=True
    q["cohort_key"]=q.signal_date.astype(str)+"|"+q.lane.astype(str)+"|"+q.code.astype(str)
    q["price_calendar_has_signal_date"]=True
    return q

def build_summary(ledger):
    rows=[]
    for h in base.HORIZONS:
        col=f"d{h}_ret_pct"
        for lane,g in ledger.groupby("lane"):
            if lane not in ["ORIGINAL_TOP3","EARLY_POWER"]: continue
            x=pd.to_numeric(g.get(col),errors="coerce")
            ok=x.notna()
            rows.append({
                "horizon":f"D{h}","lane":lane,"n":int(ok.sum()),
                "signal_days":int(g.loc[ok,"signal_date"].nunique()) if ok.any() else 0,
                "mean_ret_pct":float(x[ok].mean()) if ok.any() else np.nan,
                "median_ret_pct":float(x[ok].median()) if ok.any() else np.nan,
                "positive_pct":float((x[ok]>0).mean()*100) if ok.any() else np.nan,
                "mfe_median_pct":float(pd.to_numeric(g.loc[ok,f"mfe{h}_pct"],errors="coerce").median()) if ok.any() else np.nan,
                "mae_median_pct":float(pd.to_numeric(g.loc[ok,f"mae{h}_pct"],errors="coerce").median()) if ok.any() else np.nan,
            })
    return pd.DataFrame(rows)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--marcap",nargs="+",required=True)
    ap.add_argument("--original-source",required=True)
    ap.add_argument("--previous-ledger",default="")
    ap.add_argument("--previous-cohorts",default="")
    ap.add_argument("--out",default="reports/early_power_r6")
    args=ap.parse_args()
    out=Path(args.out); out.mkdir(parents=True,exist_ok=True)

    px=base.load_marcap(args.marcap)
    price_dates={iso(x) for x in px.Date.dropna().unique()}
    price_last=iso(px.Date.max()) if len(px) else ""

    source_date,orig=base.load_original_top3(args.original_source)
    source_iso=iso(source_date)

    prior=read_optional(args.previous_ledger,dtype={"code":str})
    cohorts=read_optional(args.previous_cohorts)

    # Cohort authority: one row per REAL_FULL signal date.
    if cohorts.empty:
        cohorts=pd.DataFrame(columns=[
            "signal_date","frozen_rule_id","original_status","original_rows",
            "early_power_status","early_power_rows","price_calendar_has_signal_date",
            "first_seen_revision","last_update_revision"
        ])

    # Preserve current ORIGINAL cohort even if all-market price calendar is lagging.
    orig_new=add_original_rows(orig,REVISION)
    current_has_price = source_iso in price_dates
    orig_new["price_calendar_has_signal_date"]=current_has_price

    new_rows=[orig_new]
    if current_has_price:
        ep=early_rows(px,source_iso)
        new_rows.append(ep)
        ep_status="READY"
        ep_n=len(ep)
    else:
        ep=pd.DataFrame()
        ep_status="DATA_PENDING"
        ep_n=0

    # Upsert current cohort row.
    current_cohort={
        "signal_date":source_iso,
        "frozen_rule_id":base.FROZEN_RULE_ID,
        "original_status":"CAPTURED",
        "original_rows":int(len(orig)),
        "early_power_status":ep_status,
        "early_power_rows":int(ep_n),
        "price_calendar_has_signal_date":bool(current_has_price),
        "first_seen_revision":REVISION,
        "last_update_revision":REVISION,
    }
    if not cohorts.empty and cohorts.signal_date.astype(str).eq(source_iso).any():
        ix=cohorts.index[cohorts.signal_date.astype(str).eq(source_iso)][0]
        for k,v in current_cohort.items():
            if k=="first_seen_revision" and pd.notna(cohorts.loc[ix,k]): continue
            cohorts.loc[ix,k]=v
    else:
        cohorts=pd.concat([cohorts,pd.DataFrame([current_cohort])],ignore_index=True)

    # Exact-date backfill for every previous DATA_PENDING cohort whose date is now present.
    pending_dates=[]
    if not cohorts.empty:
        for _,cr in cohorts.iterrows():
            sd=str(cr.get("signal_date",""))[:10]
            if str(cr.get("early_power_status",""))=="DATA_PENDING" and sd in price_dates:
                pending_dates.append(sd)

    backfilled=[]
    for sd in sorted(set(pending_dates)):
        # Do not duplicate if candidate lane already materialized.
        prior_ep = (not prior.empty and
                    prior.signal_date.astype(str).eq(sd).any() and
                    prior.loc[prior.signal_date.astype(str).eq(sd),"lane"].astype(str).eq("EARLY_POWER").any())
        if not prior_ep:
            q=early_rows(px,sd)
            new_rows.append(q)
            ep_n=len(q)
        else:
            ep_n=int(((prior.signal_date.astype(str)==sd)&(prior.lane.astype(str)=="EARLY_POWER")).sum())
        ix=cohorts.index[cohorts.signal_date.astype(str).eq(sd)][0]
        cohorts.loc[ix,"early_power_status"]="BACKFILLED_READY"
        cohorts.loc[ix,"early_power_rows"]=ep_n
        cohorts.loc[ix,"price_calendar_has_signal_date"]=True
        cohorts.loc[ix,"last_update_revision"]=REVISION
        backfilled.append(sd)

    ledger=pd.concat([prior]+new_rows,ignore_index=True,sort=False)
    if ledger.empty:
        raise SystemExit("R6_2_LEDGER_EMPTY")
    ledger["code"]=ledger.code.map(base.norm_code)
    ledger=ledger.drop_duplicates(["signal_date","lane","code"],keep="first")
    ledger=base.update_outcomes(ledger,px)
    ledger=ledger.sort_values(["signal_date","lane","shadow_rank","code"],kind="stable").reset_index(drop=True)

    cohorts=cohorts.sort_values("signal_date",kind="stable").drop_duplicates("signal_date",keep="last").reset_index(drop=True)

    ledger.to_csv(out/"r6_oos_candidate_ledger.csv",index=False,encoding="utf-8-sig")
    cohorts.to_csv(out/"r6_oos_cohort_ledger.csv",index=False,encoding="utf-8-sig")

    today=ledger[ledger.signal_date.astype(str).eq(source_iso)].copy()
    today.to_csv(out/"r6_today_shadow_board.csv",index=False,encoding="utf-8-sig")
    cohorts[cohorts.signal_date.astype(str).eq(source_iso)].to_csv(
        out/"r6_today_cohort_status.csv",index=False,encoding="utf-8-sig"
    )

    summary=build_summary(ledger)
    summary.to_csv(out/"r6_oos_summary.csv",index=False,encoding="utf-8-sig")

    meta={
      "revision":REVISION,
      "base_model_revision":base.REVISION,
      "infrastructure_revision":INFRA_REVISION,
      "research_only":True,
      "production_changed":False,
      "production_search_changed":False,
      "production_score_changed":False,
      "production_rank_changed":False,
      "production_order_changed":False,
      "automatic_orders":False,
      "same_sample_retuning":False,
      "prospective_only":True,
      "frozen_rule_id":base.FROZEN_RULE_ID,
      "comparison_date":source_iso,
      "price_data_last_date":price_last,
      "price_calendar_has_comparison_date":bool(current_has_price),
      "current_early_power_status":ep_status,
      "current_original_top3_rows":int(len(orig)),
      "current_early_power_rows":int(ep_n if current_has_price else 0),
      "backfilled_dates":backfilled,
      "ledger_rows":int(len(ledger)),
      "cohort_rows":int(len(cohorts)),
      "note":"R6_FROZEN_V1 unchanged. Missing exact-date marcap creates DATA_PENDING; no backdating/substitution. Exact-date backfill occurs only when that date becomes available."
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    report=[
      "# EARLY POWER R6.2 — exact-date pending/backfill",
      "",
      json.dumps(meta,ensure_ascii=False,indent=2),
      "",
      "## Today cohort",
      cohorts[cohorts.signal_date.astype(str).eq(source_iso)].to_string(index=False),
      "",
      "## Today candidates",
      today.to_string(index=False),
      "",
      "## Matured summary",
      summary.to_string(index=False)
    ]
    (out/"REPORT.txt").write_text("\n".join(report),encoding="utf-8")
    print("EARLY_POWER_R6_2_PENDING_BACKFILL_PASS")
    print(json.dumps(meta,ensure_ascii=False))
    print(cohorts[cohorts.signal_date.astype(str).eq(source_iso)].to_string(index=False))

if __name__=="__main__":
    main()
