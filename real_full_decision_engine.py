#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import math, re
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd

POLICY_ID = "REAL_FULL_DECISION_STATE_R1"
POLICY_REVISION = "REAL_FULL_DECISION_STATE_R1_2_CANONICAL_MATERIALIZED_FIELDS"
CONFIRM_MAX_BARS = 3
PULLBACK_MAX_BARS = 5
TERMINAL = {"READY","INVALID_STOP","INVALID_REFERENCE","INVALIDATED","EXPIRED","NO_CHASE"}

def code(v: Any) -> str:
    s=re.sub(r"\D","",str(v or ""))
    return s[-6:].zfill(6) if s else ""

def text(v: Any) -> str:
    if v is None: return ""
    try:
        if pd.isna(v): return ""
    except Exception:
        pass
    s=str(v).strip()
    return "" if s.lower() in {"","nan","none","nat"} else s

def num(v: Any) -> float:
    try:
        x=float(v)
        return x if math.isfinite(x) else np.nan
    except Exception:
        return np.nan

def boolv(v: Any) -> bool:
    if isinstance(v,bool): return v
    if isinstance(v,(int,np.integer)): return int(v)!=0
    if isinstance(v,(float,np.floating)) and math.isfinite(float(v)): return float(v)!=0
    return str(v).strip().lower() in {"1","true","t","yes","y","pass","ok"}


def first_num(r: Dict[str,Any], *keys: str) -> float:
    for k in keys:
        if k in r:
            x=num(r.get(k))
            if math.isfinite(x):
                return x
    return np.nan

def first_text(r: Dict[str,Any], *keys: str) -> str:
    for k in keys:
        if k in r:
            x=text(r.get(k))
            if x:
                return x
    return ""

def first_bool(r: Dict[str,Any], *keys: str) -> bool:
    for k in keys:
        if k in r and not pd.isna(r.get(k)):
            return boolv(r.get(k))
    return False

def price_fmt(v: Any) -> str:
    x=num(v)
    return "-" if not math.isfinite(x) else f"{int(round(x)):,}"

def in_zone(price: float, lo: float, hi: float) -> Optional[bool]:
    if not all(math.isfinite(x) for x in (price,lo,hi)) or lo>hi: return None
    return lo <= price <= hi

def extract_row(row: Any) -> Dict[str,Any]:
    if isinstance(row,pd.Series): r=row.to_dict()
    elif isinstance(row,dict): r=dict(row)
    else: r={}

    # LIVE Korean output and V23 materialized canonical payloads are both
    # authorities for their own lane. Never silently discard the canonical
    # historical field just because its display label differs from LIVE.
    current_price=first_num(r,"현재가","snapshot_price","entry_price")
    stop_price=first_num(r,"🚨손절가","signal_stop_price","v72_stop_line")
    blue_line=first_num(r,"파란점선기준가","v72_entry_line")
    blue_state=first_text(r,"파란점선상태","cross_blue_state")

    live_wave=first_text(r,"파동타점상태")
    canonical_phase=first_text(r,"phase")
    wave_state=live_wave or canonical_phase

    live_pattern=first_text(r,"검색식대표","검색패턴")
    canonical_pattern=first_text(r,"search_pattern_primary","strategy","pattern")
    search_pattern=live_pattern or canonical_pattern
    search_matches=first_text(r,"검색식매칭","검색패턴매칭","search_pattern_matches")

    # Do not pretend canonical English fields are identical to LIVE Korean
    # labels. Preserve both explicit historical dimensions below.
    return {
        "code":code(r.get("code") or r.get("종목코드") or r.get("Code")),
        "name":first_text(r,"종목명","name","Name"),
        "current_price":current_price,
        "stop_price":stop_price,
        "blue_line":blue_line,
        "blue_state":blue_state,
        "optimal_low":first_num(r,"파동최적하단"),
        "optimal_high":first_num(r,"파동최적상단"),
        "wave_state":wave_state,
        "wave_opinion":first_text(r,"파동실행의견","v72_pullback_restart_reason"),
        "watermelon_state":first_text(r,"수박상태명","수박최종상태"),
        "recommendation_stage":first_text(r,"추천단계","v1097_gate_group"),
        "stage_status":first_text(r,"단계상태"),
        "cloud_state":first_text(r,"저항구름상태"),
        "refine_state":first_text(r,"수박정제태그","fake_severity"),
        "five_day_ok":first_bool(r,"5일재안착"),
        "volume_ok":first_bool(r,"수박정제_vol_ok"),
        "candle_ok":first_bool(r,"수박정제_candle_ok"),
        "wick_ok":first_bool(r,"수박정제_wick_ok"),
        "cloud_ok":first_bool(r,"수박정제_cloud_ok"),
        "obv_ok":first_bool(r,"수박정제_obv_ok"),
        "top15_sort_score":first_num(r,"TOP15정렬점수"),
        "recommend_sort_score":first_num(r,"추천정렬점수"),
        "safe_score":first_num(r,"안전점수","safe_score"),
        "n_score":first_num(r,"N점수","n_score"),
        "raw_score":first_num(r,"점수"),
        "search_pattern":search_pattern,
        "search_matches":search_matches,

        # LIVE chart-context fields where present.
        "small_wave_direction":first_text(r,"소파동박스방향"),
        "small_wave_position":first_text(r,"소파동위치권"),
        "small_wave_angle":first_num(r,"소파동각도"),
        "medium_wave_direction":first_text(r,"중파동박스방향"),
        "medium_wave_position":first_text(r,"중파동위치권"),
        "medium_wave_angle":first_num(r,"중파동각도"),
        "recent_drawdown_pct":first_num(r,"최근고점대비조정률"),
        "correction_label":first_text(r,"조정률라벨"),
        "place_label":first_text(r,"자리평가라벨"),
        "long_pullback_label":first_text(r,"장기눌림라벨"),
        "overheat_label":first_text(r,"과열라벨"),
        "source_rr_ratio":first_num(r,"RR비율"),
        "rsi":first_num(r,"RSI","rsi"),
        "bb40":first_num(r,"BB40","bb40"),
        "ma_convergence":first_num(r,"MA수렴"),
        "obv_slope":first_num(r,"OBV기울기","obv_slope"),

        # Canonical V23 historical context — preserved under its OWN names.
        "canonical_phase":canonical_phase,
        "canonical_strategy":first_text(r,"strategy"),
        "canonical_pattern":first_text(r,"pattern"),
        "canonical_td_label":first_text(r,"td_label"),
        "canonical_td_exec_bucket":first_text(r,"td_exec_bucket"),
        "canonical_td_core_quality":first_text(r,"td_core_quality"),
        "canonical_v1097_gate_group":first_text(r,"v1097_gate_group"),
        "canonical_v1096_gate_group":first_text(r,"v1096_gate_group"),
        "canonical_pullback_grade":first_text(r,"v72_pullback_restart_grade"),
        "canonical_pullback_restart":first_bool(r,"v72_pullback_restart"),
        "canonical_impulse_pct":first_num(r,"v72_impulse_pct"),
        "canonical_pullback_days":first_num(r,"v72_pullback_days"),
        "canonical_support_count":first_num(r,"v72_support_count"),
        "canonical_volume_ratio20":first_num(r,"v72_volume_ratio20"),
        "canonical_headroom_pct":first_num(r,"v72_headroom_pct"),
        "canonical_stop_distance_pct":first_num(r,"v72_stop_distance_pct"),
        "canonical_pattern_exact_combo":first_text(r,"pattern_exact_combo"),
        "canonical_pattern_overlap_count":first_num(r,"pattern_overlap_count"),
        "canonical_context_status":first_text(r,"v1104_context_status"),
        "canonical_context_polarity":first_text(r,"v1105_context_polarity"),
        "canonical_rotation_bucket":first_text(r,"v1103_rotation_bucket"),
        "canonical_signal_stop_source":first_text(r,"signal_stop_source"),
        "canonical_signal_stop_pct":first_num(r,"signal_stop_pct"),
    }

def initial_action(x: Dict[str,Any]) -> Tuple[str,int,str]:
    p,stop,blue,lo,hi=x["current_price"],x["stop_price"],x["blue_line"],x["optimal_low"],x["optimal_high"]
    if not math.isfinite(p) or not math.isfinite(stop) or stop<=0 or stop>=p:
        return "INVALID_STOP",0,"유효한 기존 손절가가 없음"
    wm=x["watermelon_state"]; bs=x["blue_state"]; wave=x["wave_state"]
    if "후행" in wm or bs=="과이격":
        return "NO_CHASE",0,"후행/과이격"
    if "1파발생" in wave:
        if not (math.isfinite(lo) and math.isfinite(hi) and lo<=hi):
            return "INVALID_REFERENCE",0,"첫눌림 대기용 최적구간 없음"
        return "WAIT_FIRST_PULLBACK",PULLBACK_MAX_BARS,"1파 발생 후 첫눌림 대기"
    z=in_zone(p,lo,hi)
    if bs=="위확장" or z is False and math.isfinite(hi) and p>hi:
        if not (math.isfinite(lo) and math.isfinite(hi) and lo<=hi):
            return "INVALID_REFERENCE",0,"눌림 대기용 최적구간 없음"
        return "WAIT_PULLBACK",PULLBACK_MAX_BARS,"최적 눌림구간보다 위"
    if bs=="아래" or (math.isfinite(blue) and p<blue):
        if not math.isfinite(blue):
            return "INVALID_REFERENCE",0,"회복 기준선 없음"
        return "WAIT_RECLAIM",CONFIRM_MAX_BARS,"파란점선 아래"
    if bs=="접촉":
        if not math.isfinite(blue):
            return "INVALID_REFERENCE",0,"지지 기준선 없음"
        return "WAIT_HOLD",CONFIRM_MAX_BARS,"파란점선 접촉"
    core=x["five_day_ok"] and x["volume_ok"] and x["candle_ok"]
    if z is True and bs=="위안착" and core:
        return "READY",0,"가격위치+5일선+양봉+거래량 완료"
    return "WAIT_CONFIRM",CONFIRM_MAX_BARS,"핵심 확인조건 미완료"

def make_event(row: Any, signal_date: str, rank: int) -> Dict[str,Any]:
    x=extract_row(row)
    act,maxbars,why=initial_action(x)
    event_id=f"{signal_date}|{x['code']}"
    out={
        "policy_id":POLICY_ID,"policy_revision":POLICY_REVISION,
        "event_id":event_id,"origin_date":signal_date,"origin_rank":int(rank),
        "code":x["code"],"name":x["name"],
        "origin_action":act,"max_wait_bars":int(maxbars),"origin_reason":why,
        "frozen_stop":x["stop_price"],"frozen_blue":x["blue_line"],
        "frozen_optimal_low":x["optimal_low"],"frozen_optimal_high":x["optimal_high"],
        "origin_price":x["current_price"],
        "origin_wave_state":x["wave_state"],"origin_wave_opinion":x["wave_opinion"],
        "origin_watermelon_state":x["watermelon_state"],
        "origin_recommendation_stage":x["recommendation_stage"],
        "origin_stage_status":x["stage_status"],"origin_cloud_state":x["cloud_state"],
        "origin_refine_state":x["refine_state"],"origin_search_pattern":x["search_pattern"],
        "origin_search_matches":x["search_matches"],
        "origin_small_wave_direction":x["small_wave_direction"],
        "origin_small_wave_position":x["small_wave_position"],
        "origin_small_wave_angle":x["small_wave_angle"],
        "origin_medium_wave_direction":x["medium_wave_direction"],
        "origin_medium_wave_position":x["medium_wave_position"],
        "origin_medium_wave_angle":x["medium_wave_angle"],
        "origin_recent_drawdown_pct":x["recent_drawdown_pct"],
        "origin_correction_label":x["correction_label"],
        "origin_place_label":x["place_label"],
        "origin_long_pullback_label":x["long_pullback_label"],
        "origin_overheat_label":x["overheat_label"],
        "origin_source_rr_ratio":x["source_rr_ratio"],
        "origin_rsi":x["rsi"],
        "origin_bb40":x["bb40"],
        "origin_ma_convergence":x["ma_convergence"],
        "origin_obv_slope":x["obv_slope"],
        "origin_canonical_phase":x["canonical_phase"],
        "origin_canonical_strategy":x["canonical_strategy"],
        "origin_canonical_pattern":x["canonical_pattern"],
        "origin_td_label":x["canonical_td_label"],
        "origin_td_exec_bucket":x["canonical_td_exec_bucket"],
        "origin_td_core_quality":x["canonical_td_core_quality"],
        "origin_v1097_gate_group":x["canonical_v1097_gate_group"],
        "origin_v1096_gate_group":x["canonical_v1096_gate_group"],
        "origin_pullback_grade":x["canonical_pullback_grade"],
        "origin_pullback_restart":int(x["canonical_pullback_restart"]),
        "origin_impulse_pct":x["canonical_impulse_pct"],
        "origin_pullback_days":x["canonical_pullback_days"],
        "origin_support_count":x["canonical_support_count"],
        "origin_volume_ratio20":x["canonical_volume_ratio20"],
        "origin_headroom_pct":x["canonical_headroom_pct"],
        "origin_stop_distance_pct":x["canonical_stop_distance_pct"],
        "origin_pattern_exact_combo":x["canonical_pattern_exact_combo"],
        "origin_pattern_overlap_count":x["canonical_pattern_overlap_count"],
        "origin_market_context_status":x["canonical_context_status"],
        "origin_market_context_polarity":x["canonical_context_polarity"],
        "origin_rotation_bucket":x["canonical_rotation_bucket"],
        "origin_signal_stop_source":x["canonical_signal_stop_source"],
        "origin_signal_stop_pct":x["canonical_signal_stop_pct"],
        "origin_top15_sort_score":x["top15_sort_score"],
        "origin_recommend_sort_score":x["recommend_sort_score"],
        "origin_safe_score":x["safe_score"],"origin_n_score":x["n_score"],"origin_raw_score":x["raw_score"],
        "state":"READY" if act=="READY" else act if act in TERMINAL else "WAIT",
        "wait_age":0,"days_left":int(maxbars),
        "missing_conditions":"","met_conditions":"","last_observed_date":signal_date,
        "last_price":x["current_price"],"ready_date":signal_date if act=="READY" else "",
        "ready_price":x["current_price"] if act=="READY" else np.nan,
        "terminal_date":signal_date if act in TERMINAL else "",
        "manual_review_required":1 if act=="READY" else 0,
        "auto_order_allowed":0,
        "ever_near_ready":0,
    }
    return out

def requirements(event: Dict[str,Any], current: Optional[Dict[str,Any]], current_price: float) -> Dict[str,Optional[bool]]:
    act=text(event.get("origin_action"))
    blue=num(event.get("frozen_blue")); lo=num(event.get("frozen_optimal_low")); hi=num(event.get("frozen_optimal_high"))
    req={}
    if act in {"WAIT_PULLBACK","WAIT_FIRST_PULLBACK"}:
        req["눌림가격구간"]=in_zone(current_price,lo,hi)
    elif act in {"WAIT_RECLAIM","WAIT_HOLD","WAIT_CONFIRM"}:
        req["파란점선회복/지지"]=(current_price>=blue) if math.isfinite(current_price) and math.isfinite(blue) else None
    if act in {"WAIT_PULLBACK","WAIT_FIRST_PULLBACK","WAIT_RECLAIM","WAIT_HOLD","WAIT_CONFIRM"}:
        req["5일선재안착"]=current.get("five_day_ok") if current else None
        req["양봉/종가유지"]=current.get("candle_ok") if current else None
        req["거래량보강"]=current.get("volume_ok") if current else None
    return req

def evaluate(event: Dict[str,Any], current_row: Any, current_price: Any, wait_age: int, current_date: str) -> Dict[str,Any]:
    out=dict(event)
    if text(out.get("state")) in TERMINAL:
        return out
    p=num(current_price)
    stop=num(out.get("frozen_stop"))
    if math.isfinite(p) and math.isfinite(stop) and p<=stop:
        out.update(state="INVALIDATED",terminal_date=current_date,last_observed_date=current_date,last_price=p,
                   missing_conditions="",met_conditions="",days_left=0,manual_review_required=0)
        return out
    cur=extract_row(current_row) if current_row is not None else None
    req=requirements(out,cur,p)
    met=[k for k,v in req.items() if v is True]
    missing=[k for k,v in req.items() if v is not True]
    out["wait_age"]=int(wait_age)
    out["days_left"]=max(0,int(out.get("max_wait_bars",0))-int(wait_age))
    out["last_observed_date"]=current_date
    out["last_price"]=p
    out["met_conditions"]=" | ".join(met)
    out["missing_conditions"]=" | ".join(missing)
    if req and not missing:
        out.update(state="READY",ready_date=current_date,ready_price=p,terminal_date=current_date,
                   manual_review_required=1,days_left=0)
        return out
    if int(wait_age) >= int(out.get("max_wait_bars",0)):
        out.update(state="EXPIRED",terminal_date=current_date,manual_review_required=0,days_left=0)
        return out
    if len(missing)==1:
        out["state"]="NEAR_READY"; out["ever_near_ready"]=1
    else:
        out["state"]="WAIT"
    out["manual_review_required"]=0
    return out

def condition_summary(event: Dict[str,Any]) -> str:
    st=text(event.get("state"))
    if st=="READY": return "필수조건 완료 → 수동 실전검토"
    if st=="NEAR_READY": return f"1조건 남음: {text(event.get('missing_conditions'))}"
    if st=="WAIT": return f"남은조건: {text(event.get('missing_conditions')) or '-'}"
    if st=="INVALIDATED": return "손절/무효가격 선이탈"
    if st=="EXPIRED": return "고정 대기기한 초과 · 추격금지"
    if st=="NO_CHASE": return "후행/과이격 · 새 눌림 사건 전까지 종료"
    return text(event.get("origin_reason")) or st

def mature_from_ready(event: Dict[str,Any], frame: Optional[pd.DataFrame]) -> Dict[str,Any]:
    out={}
    for h in (1,3,5,10):
        out.update({f"ready_d{h}_complete":0,f"ready_d{h}_close_ret_pct":np.nan,
                    f"ready_d{h}_mfe_pct":np.nan,f"ready_d{h}_mae_pct":np.nan,
                    f"ready_d{h}_hit_plus3":0,f"ready_d{h}_hit_plus5":0})
    if frame is None or frame.empty or text(event.get("ready_date"))=="":
        return out
    price=num(event.get("ready_price"))
    if not math.isfinite(price) or price<=0: return out
    d=pd.Timestamp(event["ready_date"]).normalize()
    f=frame.copy()
    f["date"]=pd.to_datetime(f["date"],errors="coerce").dt.normalize()
    fut=f[f["date"].gt(d)].sort_values("date")
    for h in (1,3,5,10):
        if len(fut)<h: continue
        path=fut.iloc[:h]
        close=num(path.iloc[h-1]["close"])
        hi=pd.to_numeric(path["high"],errors="coerce").max()
        lo=pd.to_numeric(path["low"],errors="coerce").min()
        out[f"ready_d{h}_complete"]=1
        out[f"ready_d{h}_close_ret_pct"]=(close/price-1)*100
        out[f"ready_d{h}_mfe_pct"]=(float(hi)/price-1)*100
        out[f"ready_d{h}_mae_pct"]=(float(lo)/price-1)*100
        out[f"ready_d{h}_hit_plus3"]=int(float(hi)>=price*1.03)
        out[f"ready_d{h}_hit_plus5"]=int(float(hi)>=price*1.05)
    return out
