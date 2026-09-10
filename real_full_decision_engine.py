#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import math, re
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd

POLICY_ID = "REAL_FULL_DECISION_STATE_R1"
POLICY_REVISION = "REAL_FULL_DECISION_STATE_R1_1_FIXED_3_5_BAR"
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
    return {
        "code":code(r.get("code") or r.get("종목코드") or r.get("Code")),
        "name":text(r.get("종목명") or r.get("name") or r.get("Name")),
        "current_price":num(r.get("현재가") if "현재가" in r else r.get("snapshot_price")),
        "stop_price":num(r.get("🚨손절가")),
        "blue_line":num(r.get("파란점선기준가")),
        "blue_state":text(r.get("파란점선상태")),
        "optimal_low":num(r.get("파동최적하단")),
        "optimal_high":num(r.get("파동최적상단")),
        "wave_state":text(r.get("파동타점상태")),
        "wave_opinion":text(r.get("파동실행의견")),
        "wave_invalidation":num(r.get("파동무효화")),
        "watermelon_state":text(r.get("수박상태명") or r.get("수박최종상태")),
        "recommendation_stage":text(r.get("추천단계")),
        "stage_status":text(r.get("단계상태")),
        "cloud_state":text(r.get("저항구름상태")),
        "refine_state":text(r.get("수박정제태그")),
        "five_day_ok":boolv(r.get("5일재안착")),
        "volume_ok":boolv(r.get("수박정제_vol_ok")),
        "candle_ok":boolv(r.get("수박정제_candle_ok")),
        "wick_ok":boolv(r.get("수박정제_wick_ok")),
        "cloud_ok":boolv(r.get("수박정제_cloud_ok")),
        "obv_ok":boolv(r.get("수박정제_obv_ok")),
        "top15_sort_score":num(r.get("TOP15정렬점수")),
        "recommend_sort_score":num(r.get("추천정렬점수")),
        "safe_score":num(r.get("안전점수")),
        "n_score":num(r.get("N점수")),
        "raw_score":num(r.get("점수")),
        "search_pattern":text(r.get("검색식대표") or r.get("검색패턴")),
        "search_matches":text(r.get("검색식매칭") or r.get("검색패턴매칭")),
        # Existing descriptive context fields. These are analysis dimensions only;
        # they do not change selection, rank, or READY logic.
        "small_wave_direction":text(r.get("소파동박스방향")),
        "small_wave_position":text(r.get("소파동위치권")),
        "small_wave_angle":num(r.get("소파동각도")),
        "medium_wave_direction":text(r.get("중파동박스방향")),
        "medium_wave_position":text(r.get("중파동위치권")),
        "medium_wave_angle":num(r.get("중파동각도")),
        "recent_drawdown_pct":num(r.get("최근고점대비조정률")),
        "correction_label":text(r.get("조정률라벨")),
        "place_label":text(r.get("자리평가라벨")),
        "long_pullback_label":text(r.get("장기눌림라벨")),
        "overheat_label":text(r.get("과열라벨")),
        "source_rr_ratio":num(r.get("RR비율")),
        "rsi":num(r.get("RSI")),
        "bb40":num(r.get("BB40")),
        "ma_convergence":num(r.get("MA수렴")),
        "obv_slope":num(r.get("OBV기울기")),
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
