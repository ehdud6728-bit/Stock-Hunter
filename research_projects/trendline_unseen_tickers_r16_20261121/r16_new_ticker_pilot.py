#!/usr/bin/env python3
"""R16: ticker-disjoint R116/R14/R15 pilot with a price-source audit gate.

RESEARCH_ONLY. Not representative full-market OOS: the frozen candidate-code
roster was originally produced by another as-of BB-event study. R16 uses ONLY
the set of codes and a preregistered SHA rank, never that study's future return
or selected signal dates. Public provider is not independently PIT certified.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import importlib.util
import json
import math
import statistics
import time
from pathlib import Path

import pandas as pd

ROOT=Path(__file__).resolve().parents[2]
ROSTER_REL="research_inputs/R1D27D_ALLMARKET_85576_TICKER_DATES_SOURCE_QUEUE.csv.gz"
ROSTER_SHA256="a63698a574d0770653c37df3737c4b1e3dc190761adeb75d1b76a2e66a5f3949"
R15_REL="research_projects/trendline_exit_capture_r15_20261121/r15_capture_regret.py"
R15_SHA256="03f6003bcc9a553cbe1164ddce7db8e365cef9395a94fd789e66813ced035720"
SELECTION_SALT="R16_UNSEEN_CODES_V1_20261121"
EXCLUDED_FIVE=frozenset(("006400","039840","071840","053800","220260"))
PREDECLARED_CODES=(
    "093320","016385","332290","125210","062970","068760",
    "250000","004380","042370","015710","057540","377480",
    "054050","131090","012610","044450","100120","002450",
    "066790","177830","039240","353200","226320","129260",
    "053350","127120","001720","052020","072870","065500",
)
FETCH_START="20200101"
FETCH_END="20251231"
MIN_HISTORY_BARS=280
MIN_VALID_TICKERS_FOR_EVALUATION=10
PREDECLARED_NO_MORE_THAN=30


def _hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _module(repo: Path, relative: str, sha: str, name: str):
    p=repo/relative
    if _hash(p.read_bytes())!=sha:
        raise ValueError("FROZEN_PARENT_SOURCE_CHANGED "+relative)
    spec=importlib.util.spec_from_file_location(name,p)
    mod=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def selected_codes(repo: Path=ROOT) -> list[str]:
    source=repo/ROSTER_REL
    if _hash(source.read_bytes())!=ROSTER_SHA256:
        raise ValueError("FROZEN_UNIVERSE_ROSTER_SHA_CHANGED")
    with gzip.open(source,"rt",encoding="utf-8-sig",newline="") as f:
        reader=csv.DictReader(f)
        if not {"date","code","future_outcomes_used"}.issubset(reader.fieldnames or []):
            raise ValueError("ROSTER_MISSING_REQUIRED_COLUMNS")
        codes=set()
        for r in reader:
            code=r["code"].strip()
            # KRX preferred share tickers can end in K (e.g. 00680K).
            if not (len(code)==6 and code.isascii() and code.isalnum()):
                raise ValueError("ROSTER_INVALID_CODE")
            if r["future_outcomes_used"].strip().lower() not in ("false","0"):
                raise ValueError("ROSTER_MAY_CONTAIN_FUTURE_OUTCOMES")
            codes.add(code)
    ranked=sorted(codes-EXCLUDED_FIVE,
                  key=lambda c:(_hash((SELECTION_SALT+"|"+c).encode()),c))
    result=ranked[:PREDECLARED_NO_MORE_THAN]
    if tuple(result)!=PREDECLARED_CODES or len(result)!=len(set(result)):
        raise AssertionError("PREREGISTERED_HOLDOUT_CODES_DRIFT")
    return result


def validate_and_normalize(raw: pd.DataFrame, code: str) -> pd.DataFrame:
    """Do not heal bad bars or quietly fill suspended-day prices."""
    names={"시가":"open","고가":"high","저가":"low","종가":"close",
           "거래량":"volume","거래대금":"amount"}
    if raw is None or raw.empty:
        raise ValueError("NO_PROVIDER_PRICE_ROWS")
    if not set(names).issubset(raw.columns) or not raw.index.is_unique or raw.index.hasnans:
        raise ValueError("INCOMPLETE_PROVIDER_PRICE_SCHEMA")
    out=raw.rename(columns=names)[list(names.values())].copy()
    out["date"]=pd.to_datetime(out.index)
    out=out.sort_values("date").reset_index(drop=True)
    for k in names.values():
        out[k]=pd.to_numeric(out[k],errors="coerce")
    if out[list(names.values())].isna().any().any():
        raise ValueError("MISSING_PRICE_OR_ACTIVITY")
    if (out[["open","high","low","close"]]<=0).any().any():
        raise ValueError("ZERO_OR_NEGATIVE_TRADE_PRICE_OR_HALT")
    if (out[["volume","amount"]]<0).any().any():
        raise ValueError("NEGATIVE_TRADE_ACTIVITY")
    if ((out["low"]>out[["open","close","high"]].min(axis=1))|
            (out["high"]<out[["open","close","low"]].max(axis=1))).any():
        raise ValueError("OHLC_ORDER_INCONSISTENT")
    if len(out)<MIN_HISTORY_BARS:
        raise ValueError("INSUFFICIENT_HISTORICAL_BARS")
    if (out.date.dt.strftime("%Y%m%d")<FETCH_START).any() or (out.date.dt.strftime("%Y%m%d")>FETCH_END).any():
        raise ValueError("PROVIDER_RETURNED_OUT_OF_REQUESTED_RANGE")
    # Extreme jumps may be splits, corporate actions, delist/relist or genuine
    # moves; no attempt to infer which one is correct using future knowledge.
    gap=(out.open/out.close.shift(1)-1).abs()
    if bool((gap>.35).any()):
        raise ValueError("PRICE_ACTION_OR_EXTREME_GAP_GT35PCT_NOT_CERTIFIED")
    out["code"]=code
    out["date"]=out.date.dt.strftime("%Y-%m-%d")
    return out[["date","code","open","high","low","close","volume","amount"]]


def fetch_pykrx(code: str, *, delay_seconds: float=1.0) -> pd.DataFrame:
    from pykrx import stock
    # A single public request per code, no need to access KIS keys or secrets.
    try:
        return stock.get_market_ohlcv_by_date(FETCH_START,FETCH_END,code,adjusted=False)
    finally:
        if delay_seconds>0:
            time.sleep(delay_seconds)


def collect(repo: Path, out: Path, fetcher=fetch_pykrx) -> dict:
    codes=selected_codes(repo)
    out.mkdir(parents=True,exist_ok=True)
    rows=[];audit=[]
    for n,code in enumerate(codes,1):
        status="VALID";frame=None
        try:
            frame=validate_and_normalize(fetcher(code),code)
        except Exception as exc:
            response=getattr(exc,"response",None)
            http_status=getattr(response,"status_code",None)
            if isinstance(exc,PermissionError) or http_status in (401,403):
                raise PermissionError("PUBLIC_PROVIDER_PERMISSION_DENIED_STOP_COLLECTION") from exc
            # Do not swap a missing/invalid ticker for a more successful one.
            status="UNKNOWN_PROVIDER_OR_PROVENANCE_"+type(exc).__name__
            if isinstance(exc,ValueError):
                status="REJECT_SOURCE_"+str(exc)
            frame=None
        if frame is not None:
            rows.extend(frame.to_dict("records"))
        audit.append({"code":code,"status":status,"bars":len(frame) if frame is not None else 0,
                      "first_trade_date":frame.iloc[0].date if frame is not None else None,
                      "last_trade_date":frame.iloc[-1].date if frame is not None else None})
        print("R16_COLLECTION",n,"of",len(codes),code,status,flush=True)
    source=out/"R16_FROZEN_HOLDOUT_UNADJUSTED_OHLCV.csv.gz"
    with gzip.open(source,"wt",encoding="utf-8",newline="") as f:
        writer=csv.DictWriter(f,fieldnames=["date","code","open","high","low","close","volume","amount"])
        writer.writeheader();writer.writerows(rows)
    valid_codes=[r["code"] for r in audit if r["status"]=="VALID"]
    manifest={
        "revision":"R16_FIXED_30_CODES_PRICE_COLLECTION_RESEARCH_ONLY",
        "roster_sha256":ROSTER_SHA256,"selection_salt":SELECTION_SALT,
        "selected_codes":codes,"n_selected":len(codes),
        "n_valid_tickers":len(valid_codes),"n_invalid_or_unknown":len(codes)-len(valid_codes),
        "valid_codes":valid_codes,"source_row_count":len(rows),
        "raw_archive_sha256":_hash(source.read_bytes()),
        "fetch_start":FETCH_START,"fetch_end":FETCH_END,
        "data_provider":"pykrx_get_market_ohlcv_by_date_adjusted_FALSE",
        "provider_point_in_time_history_certified":False,
        "corporate_action_history_independently_certified":False,
        "roster_is_representative_full_market_random_sample":False,
        "code_replacement_on_failure":False,
        "live_changed":False,"orders_placed":0,"can_publish_trade_edge":False,
        "audit":audit,
    }
    (out/"R16_SOURCE_MANIFEST.json").write_text(
        json.dumps(manifest,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    return manifest


def evaluate(repo: Path, out: Path) -> dict:
    manifest=json.loads((out/"R16_SOURCE_MANIFEST.json").read_text(encoding="utf-8"))
    codes=selected_codes(repo)
    if manifest["selected_codes"]!=codes:
        raise ValueError("HOLDOUT_SELECTION_CHANGED_AFTER_FETCH")
    source=out/"R16_FROZEN_HOLDOUT_UNADJUSTED_OHLCV.csv.gz"
    if _hash(source.read_bytes())!=manifest["raw_archive_sha256"]:
        raise ValueError("FROZEN_PRICE_ARCHIVE_HASH_CHANGED")
    if manifest["n_valid_tickers"]<MIN_VALID_TICKERS_FOR_EVALUATION:
        raise ValueError("INSUFFICIENT_VALID_HOLDOUT_PRICE_TICKERS_DO_NOT_EVALUATE")
    r15=_module(repo,R15_REL,R15_SHA256,"r15_fixed_capture_diagnostics")
    r14=r15.r14_module(repo)
    r12,_r7=r14.parent_modules(repo)
    r5=r12.r5_module(repo)
    prepare=r5._modules(repo)[0]
    table=pd.read_csv(source,dtype={"code":str,"date":str})
    if len(table)!=manifest["source_row_count"]:
        raise ValueError("FROZEN_SOURCE_ROW_COUNT_CHANGED")
    if set(table.code)!=set(manifest["valid_codes"]):
        raise ValueError("FROZEN_SOURCE_VALID_CODES_CHANGED")
    records=[];summaries=[]
    for code in manifest["valid_codes"]:
        raw=table.loc[table.code==code].copy()
        prices=prepare(raw)
        unique=r12.primary_signals([
            x for x in r5.scan_stock(raw,repo) if "2021-01-01"<=x["date"]<="2025-12-31"
        ])
        selected=r12.nonoverlap_signals(unique,bars=60)
        scanned=[r15.event_diagnostics(e,prices,r14) for e in selected]
        summaries.append({"code":code,"n_unique_events":len(unique),
                          "n_nonoverlap_60":len(selected),
                          "summary":r15.cohort_summary(scanned,r14)})
        records.extend(scanned)
    combined=r15.cohort_summary(records,r14)
    result={
        "revision":"R16_TICKER_DISJOINT_R116_R14_R15_PILOT_RESEARCH_ONLY",
        "frozen_roster_sha256":ROSTER_SHA256,"frozen_r15_sha256":R15_SHA256,
        "holdout_tickers":codes,"n_valid_price_tickers":manifest["n_valid_tickers"],
        "price_archive_sha256":manifest["raw_archive_sha256"],
        "original_5_tick_codes_excluded":sorted(EXCLUDED_FIVE),
        "holdout_is_representative_full_market":False,
        "independent_prospective_OOS":False,
        "selection_convenience_bias":"TICKERS_FROM_HISTORICAL_BB_EVENT_ROSTER_NOT_FULL_MARKET_RANDOM",
        "retrospective_source_PIT_certified":False,
        "no_percent_threshold_tuning":True,
        "r14_structure_exit_policy_unmodified":True,
        "can_modify_live_search_watch":False,
        "can_publish_trade_edge":False,
        "combined":combined,"by_ticker":summaries,"records":records,
    }
    result["research_event_count_sufficient_for_descriptive_pilot"]=(
        combined["n_complete_same_events_D20_and_D60"]>=20)
    (out/"R16_HOLDOUT_FULL_AUDIT.json").write_text(
        json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    (out/"R16_HOLDOUT_SUMMARY.json").write_text(
        json.dumps({k:v for k,v in result.items() if k not in ("records","by_ticker")},
                   ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    return result


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo",default=".")
    p.add_argument("--out",required=True)
    p.add_argument("--phase",choices=("selection","collect","evaluate","all"),default="selection")
    p.add_argument("--allow-public-network-fetch",action="store_true")
    a=p.parse_args()
    repo=Path(a.repo).resolve();out=Path(a.out).resolve()
    if a.phase=="selection":
        print(json.dumps({"selected_codes":selected_codes(repo),"source_sha256":ROSTER_SHA256},indent=2))
    else:
        if a.phase in ("collect","all"):
            if not a.allow_public_network_fetch:
                p.error("COLLECTION_REQUIRES_EXPLICIT_PUBLIC_FETCH_FLAG")
            m=collect(repo,out)
            print("R16_SOURCE_STATUS",json.dumps({"valid":m["n_valid_tickers"],
                                                  "invalid":m["n_invalid_or_unknown"]}))
        if a.phase in ("evaluate","all"):
            result=evaluate(repo,out)
            print("R16_EVALUATION",json.dumps({"n_valid_tickers":result["n_valid_price_tickers"],
                                             "n_complete":result["combined"]["n_complete_same_events_D20_and_D60"]}))
            if not result["research_event_count_sufficient_for_descriptive_pilot"]:
                raise SystemExit("R16_INSUFFICIENT_EVENTS_DO_NOT_CLAIM_VALIDATION")
