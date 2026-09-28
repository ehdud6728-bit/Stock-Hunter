#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import numpy as np
import pandas as pd

REV="ODOLI_INDEPENDENT_DAILY_R1_2_PYKRX_AUGMENT_20260929"
DEFINITION="STRICT_ODOLI_R1"
AMOUNT_Q2_UPPER=0.7487214470811911
VOLUME_Q2_UPPER=0.7562848676975253
KST=ZoneInfo("Asia/Seoul")
MIN_DAILY_ROWS=1200

def norm_code(v):
    s=re.sub(r"\D","",str(v or ""))
    return s[-6:].zfill(6) if s else ""

def fnum(v):
    try:
        x=float(v)
        return x if math.isfinite(x) else np.nan
    except Exception:
        return np.nan

def truth(v):
    return str(v).strip().lower() in {"1","true","yes","y"}

def load_csv(p):
    p=Path(p)
    if not p.exists() or p.stat().st_size==0:
        return pd.DataFrame()
    return pd.read_csv(p,dtype=str,low_memory=False)

def load_marcap(root):
    parts=[]
    for p in sorted(Path(root).glob("data/marcap-*.parquet")):
        q=pd.read_parquet(p)
        if "Date" not in q.columns:
            q=q.reset_index()
        if "Date" not in q.columns:
            continue
        q["Date"]=pd.to_datetime(q["Date"],errors="coerce").dt.normalize()
        q["Code"]=q["Code"].map(norm_code)
        if "Market" in q.columns:
            q=q[q["Market"].astype(str).str.upper().isin(["KOSPI","KOSDAQ"])]
        for c in ["Open","High","Low","Close","Volume","Amount"]:
            if c in q.columns:
                q[c]=pd.to_numeric(q[c],errors="coerce")
        parts.append(q)
    if not parts:
        raise SystemExit("NO_MARCAP_DATA")
    q=pd.concat(parts,ignore_index=True)
    q=q.dropna(subset=["Date"])
    return q.sort_values(["Code","Date"]).drop_duplicates(["Code","Date"],keep="last")

def expected_krx_session(now_kst=None):
    now_kst=now_kst or datetime.now(KST)
    cal=xcals.get_calendar("XKRX")
    day=pd.Timestamp(now_kst.date())
    if now_kst.hour < 16:
        day=day-pd.Timedelta(days=1)
    session=cal.date_to_session(day,direction="previous")
    return pd.Timestamp(session).tz_localize(None).normalize()

def missing_sessions(last_date, expected):
    cal=xcals.get_calendar("XKRX")
    if pd.Timestamp(last_date).normalize() >= pd.Timestamp(expected).normalize():
        return []
    start=pd.Timestamp(last_date).normalize()+pd.Timedelta(days=1)
    sessions=cal.sessions_in_range(start,expected)
    return [pd.Timestamp(x).tz_localize(None).normalize() for x in sessions]

def _pick_col(df,names):
    by={str(c).strip().lower():c for c in df.columns}
    for n in names:
        if str(n).strip().lower() in by:
            return by[str(n).strip().lower()]
    return None

def _fetch_pykrx_one_session(day, name_map):
    from pykrx import stock
    ymd=pd.Timestamp(day).strftime("%Y%m%d")
    parts=[]
    diag={"date":pd.Timestamp(day).strftime("%Y-%m-%d"),"markets":[],"rows":0,"status":"FAIL","errors":[]}
    for market in ["KOSPI","KOSDAQ"]:
        got=pd.DataFrame()
        used=""
        for fn_name in ["get_market_ohlcv_by_ticker","get_market_ohlcv"]:
            fn=getattr(stock,fn_name,None)
            if not callable(fn):
                continue
            attempts=[((ymd,),{"market":market}),((ymd,market),{})]
            for args,kwargs in attempts:
                try:
                    z=fn(*args,**kwargs)
                    if isinstance(z,pd.DataFrame) and not z.empty:
                        got=z.reset_index()
                        used=fn_name
                        break
                except Exception as e:
                    diag["errors"].append(f"{market}:{fn_name}:{type(e).__name__}:{e}"[:240])
            if not got.empty:
                break

        if got.empty:
            diag["markets"].append({"market":market,"rows":0,"method":used or "NONE"})
            continue

        cc=_pick_col(got,["Code","code","티커","Ticker","종목코드","index"])
        if cc is None:
            diag["errors"].append(f"{market}:CODE_COLUMN_MISSING:{list(map(str,got.columns))[:12]}")
            diag["markets"].append({"market":market,"rows":0,"method":used})
            continue

        mapping={
            "Open":["Open","open","시가"],
            "High":["High","high","고가"],
            "Low":["Low","low","저가"],
            "Close":["Close","close","종가","현재가"],
            "Volume":["Volume","volume","거래량"],
            "Amount":["Amount","amount","거래대금","Turnover"],
        }
        out=pd.DataFrame()
        out["Code"]=got[cc].map(norm_code)
        out["Date"]=pd.Timestamp(day).normalize()
        out["Market"]=market
        for dst,names in mapping.items():
            c=_pick_col(got,names)
            out[dst]=pd.to_numeric(got[c],errors="coerce") if c else np.nan

        # If Amount is absent in OHLCV, try market-cap cross section for actual same-day Amount.
        if out["Amount"].isna().all() or out["Amount"].fillna(0).le(0).all():
            cap=pd.DataFrame()
            for fn_name in ["get_market_cap_by_ticker","get_market_cap"]:
                fn=getattr(stock,fn_name,None)
                if not callable(fn):
                    continue
                for args,kwargs in [((ymd,),{"market":market}),((ymd,market),{})]:
                    try:
                        z=fn(*args,**kwargs)
                        if isinstance(z,pd.DataFrame) and not z.empty:
                            cap=z.reset_index()
                            break
                    except Exception as e:
                        diag["errors"].append(f"{market}:cap:{fn_name}:{type(e).__name__}:{e}"[:240])
                if not cap.empty:
                    break
            if not cap.empty:
                ccc=_pick_col(cap,["Code","code","티커","Ticker","종목코드","index"])
                ac=_pick_col(cap,["Amount","amount","거래대금","Turnover"])
                if ccc and ac:
                    am=pd.DataFrame({
                        "Code":cap[ccc].map(norm_code),
                        "_amount":pd.to_numeric(cap[ac],errors="coerce")
                    }).drop_duplicates("Code",keep="last")
                    out=out.merge(am,on="Code",how="left")
                    out["Amount"]=out["Amount"].where(out["Amount"].gt(0),out["_amount"])
                    out=out.drop(columns=["_amount"],errors="ignore")

        req=out[["Open","High","Low","Close","Volume"]].apply(pd.to_numeric,errors="coerce")
        valid=req.notna().all(axis=1) & req[["Open","High","Low","Close"]].gt(0).all(axis=1)
        out=out[valid & out["Code"].ne("")].copy()
        out["Name"]=out["Code"].map(name_map).fillna("")
        out["Source"]="PYKRX_DAILY_CROSS_SECTION"
        parts.append(out)
        diag["markets"].append({"market":market,"rows":len(out),"method":used})

    allq=pd.concat(parts,ignore_index=True,sort=False) if parts else pd.DataFrame()
    diag["rows"]=len(allq)
    diag["status"]="PASS" if len(allq)>=MIN_DAILY_ROWS else "INSUFFICIENT_ROWS"
    return allq,diag

def augment_with_pykrx(mar, expected, out_dir):
    base_last=pd.Timestamp(mar["Date"].max()).normalize()
    miss=missing_sessions(base_last,expected)
    diag={
        "base_last_date":str(base_last.date()),
        "expected_krx_session":str(pd.Timestamp(expected).date()),
        "missing_sessions":[str(x.date()) for x in miss],
        "session_results":[],
        "all_missing_sessions_fetched":True,
        "augmented_rows":0,
    }
    if not miss:
        diag["source_authority"]="MARCAP_PARQUET_CURRENT"
        return mar,diag

    name_map={}
    if "Name" in mar.columns:
        n=mar[["Code","Name"]].dropna().drop_duplicates("Code",keep="last")
        name_map=dict(zip(n["Code"].map(norm_code),n["Name"].astype(str)))

    additions=[]
    for d in miss:
        q,one=_fetch_pykrx_one_session(d,name_map)
        diag["session_results"].append(one)
        if one["status"]!="PASS":
            diag["all_missing_sessions_fetched"]=False
            continue
        additions.append(q)

    if diag["all_missing_sessions_fetched"] and len(additions)==len(miss):
        add=pd.concat(additions,ignore_index=True,sort=False)
        diag["augmented_rows"]=len(add)
        merged=pd.concat([mar,add],ignore_index=True,sort=False)
        merged["Date"]=pd.to_datetime(merged["Date"],errors="coerce").dt.normalize()
        merged["Code"]=merged["Code"].map(norm_code)
        merged=merged.sort_values(["Code","Date"]).drop_duplicates(["Code","Date"],keep="last")
        diag["source_authority"]="MARCAP_PLUS_PYKRX_DAILY_CROSS_SECTION"
        pd.DataFrame(add).to_csv(Path(out_dir)/"pykrx_augmented_daily_rows.csv",index=False,encoding="utf-8-sig")
        return merged,diag

    diag["source_authority"]="MARCAP_PARQUET_STALE_PYKRX_AUGMENT_FAILED"
    return mar,diag

def add_features(g):
    g=g.sort_values("Date").reset_index(drop=True).copy()
    c=pd.to_numeric(g["Close"],errors="coerce")
    g["ma5"]=c.rolling(5,min_periods=5).mean()
    g["ma10"]=c.rolling(10,min_periods=10).mean()
    g["prev_close"]=c.shift(1)
    g["prev_ma5"]=g["ma5"].shift(1)
    g["prev2_close"]=c.shift(2)
    g["prev2_ma5"]=g["ma5"].shift(2)
    g["prev3_close"]=c.shift(3)
    g["prev3_ma5"]=g["ma5"].shift(3)
    g["pre3_below_ma5_n"]=(
        (g["prev_close"]<g["prev_ma5"]).astype(float)
        +(g["prev2_close"]<g["prev2_ma5"]).astype(float)
        +(g["prev3_close"]<g["prev3_ma5"]).astype(float)
    )
    g["ma5_slope_1d_pct"]=(g["ma5"]/g["prev_ma5"]-1)*100
    g["strict_odoli_r1"]=(
        (g["Close"]>g["Open"])
        & (g["prev_close"]<g["prev_ma5"])
        & (g["Close"]>g["ma5"])
        & (g["ma5_slope_1d_pct"]>0)
        & (g["pre3_below_ma5_n"]>=2)
    )
    return g

def descriptor(g,i):
    if i<20:
        return {}
    r=g.iloc[i]
    pre20=g.iloc[max(0,i-20):i]
    av=pd.to_numeric(pre20["Amount"],errors="coerce").mean()
    vv=pd.to_numeric(pre20["Volume"],errors="coerce").mean()
    amount_x=fnum(r.get("Amount"))/av if pd.notna(av) and av>0 else np.nan
    volume_x=fnum(r.get("Volume"))/vv if pd.notna(vv) and vv>0 else np.nan

    pre10=g.iloc[max(0,i-10):i].copy()
    pre10["ret"]=pd.to_numeric(pre10["Close"],errors="coerce").pct_change()
    down=pre10[pre10["ret"]<0]
    up=pre10[pre10["ret"]>0]
    da=pd.to_numeric(down["Amount"],errors="coerce").mean()
    ua=pd.to_numeric(up["Amount"],errors="coerce").mean()
    dv=pd.to_numeric(down["Volume"],errors="coerce").mean()
    uv=pd.to_numeric(up["Volume"],errors="coerce").mean()
    ar=da/ua if pd.notna(da) and pd.notna(ua) and ua>0 else np.nan
    vr=dv/uv if pd.notna(dv) and pd.notna(uv) and uv>0 else np.nan

    ma5=fnum(r.get("ma5")); ma10=fnum(r.get("ma10"))
    core_tag=bool(
        pd.notna(ma5) and pd.notna(ma10) and ma5>ma10
        and ((pd.notna(ar) and ar<=AMOUNT_Q2_UPPER) or (pd.notna(vr) and vr<=VOLUME_Q2_UPPER))
    )
    return {
        "signal_amount_ratio20":amount_x,
        "signal_volume_ratio20":volume_x,
        "down_up_amount_ratio_10d":ar,
        "down_up_volume_ratio_10d":vr,
        "core_overlap_tag":core_tag,
    }

def current_signals(bycode,asof):
    rows=[]
    for code,g in bycode.items():
        hit=g.index[g["Date"].eq(asof)]
        if len(hit)!=1:
            continue
        i=int(hit[0]); r=g.iloc[i]
        if not bool(r.get("strict_odoli_r1",False)):
            continue
        d=descriptor(g,i)
        rows.append({
            "signal_date":asof,
            "code":code,
            "name":r.get("Name",""),
            "market":r.get("Market",""),
            "close":r.get("Close"),
            "signal_low":r.get("Low"),
            "definition":DEFINITION,
            "source":r.get("Source","MARCAP_OR_PYKRX"),
            **d,
        })
    return pd.DataFrame(rows)

def migrate_pre_freshness_state(state):
    marker=state/"r11_freshness_migrated.flag"
    if marker.exists():
        return 0
    moved=0
    for name in ["events.csv","observations.csv"]:
        p=state/name
        if p.exists() and p.stat().st_size:
            target=state/f"bootstrap_pre_freshness_{name}"
            if target.exists():
                target.unlink()
            p.replace(target)
            moved+=1
    marker.write_text(
        "ODOLI R1.1 freshness migration: prior prospective state quarantined as BOOTSTRAP_PRE_FRESHNESS.\n",
        encoding="utf-8"
    )
    return moved

def append_events(signals,events):
    cols=["signal_date","code","name","market","close","signal_low","definition","source",
          "signal_amount_ratio20","signal_volume_ratio20",
          "down_up_amount_ratio_10d","down_up_volume_ratio_10d","core_overlap_tag"]
    if events.empty:
        events=pd.DataFrame(columns=cols)
    if len(events):
        events["signal_date"]=pd.to_datetime(events["signal_date"],errors="coerce").dt.normalize()
        events["code"]=events["code"].map(norm_code)
    existing=set(zip(events.get("signal_date",pd.Series(dtype="datetime64[ns]")),
                     events.get("code",pd.Series(dtype=str))))
    add=[]
    for _,r in signals.iterrows():
        key=(pd.Timestamp(r["signal_date"]).normalize(),norm_code(r["code"]))
        if key in existing:
            continue
        add.append(r.to_dict())
        existing.add(key)
    if add:
        events=pd.concat([events,pd.DataFrame(add)],ignore_index=True)
    return events,len(add)

def event_status(row,g):
    sd=pd.Timestamp(row["signal_date"]).normalize()
    hit=g.index[g["Date"].eq(sd)]
    if len(hit)!=1:
        return None
    i=int(hit[0])
    cur_i=len(g)-1
    sig=float(g.iloc[i]["Close"])
    sig_low=float(g.iloc[i]["Low"])
    ma5=pd.to_numeric(g["Close"],errors="coerce").rolling(5).mean()
    day=max(0,min(cur_i-i,60))

    q=g.iloc[i+1:cur_i+1].copy()
    mfe=((pd.to_numeric(q["High"],errors="coerce")/sig-1)*100).max() if len(q) else np.nan
    mae=((pd.to_numeric(q["Low"],errors="coerce")/sig-1)*100).min() if len(q) else np.nan
    cur_ret=(float(g.iloc[cur_i]["Close"])/sig-1)*100 if cur_i>=i else np.nan

    d1d2_hl=np.nan; d2_ma5=np.nan; all3_ma5=np.nan; low_survive=np.nan
    if cur_i>=i+2:
        d1d2_hl=bool(float(g.iloc[i+2]["Low"])>float(g.iloc[i+1]["Low"]))
        d2_ma5=bool(float(g.iloc[i+2]["Close"])>float(ma5.iloc[i+2]))
    if cur_i>=i+3:
        all3_ma5=bool(all(float(g.iloc[i+k]["Close"])>float(ma5.iloc[i+k]) for k in [1,2,3]))
    if len(q):
        low_survive=bool(pd.to_numeric(q["Low"],errors="coerce").min()>=sig_low)

    c2=c3=c4=False
    c2d=c3d=c4d=np.nan
    amount=pd.to_numeric(g["Amount"],errors="coerce")
    for j in range(i+4,min(cur_i,i+10)+1):
        close=float(g.iloc[j]["Close"])
        low=float(g.iloc[j]["Low"])
        prev_low=float(g.iloc[j-1]["Low"])
        above=bool(pd.notna(ma5.iloc[j]) and close>float(ma5.iloc[j]))
        hl=bool(low>prev_low)
        pre20=amount.iloc[max(0,j-20):j]
        ama=float(pre20.mean()) if len(pre20) else np.nan
        ar=float(amount.iloc[j]/ama) if pd.notna(ama) and ama>0 else np.nan
        hist=g.iloc[i+1:j+1]
        survive=bool(pd.to_numeric(hist["Low"],errors="coerce").min()>=sig_low) if len(hist) else False
        if not c2 and above and hl and survive:
            c2=True; c2d=j-i
        if not c3 and above and pd.notna(ar) and ar>=1.0 and survive:
            c3=True; c3d=j-i
        if not c4 and above and hl and pd.notna(ar) and ar>=1.0 and survive:
            c4=True; c4d=j-i

    if day==0:
        flow="🆕 D0 신규"
    elif day<=3 and d1d2_hl is True and d2_ma5 is True:
        flow="⚡ FAST 흐름 관찰"
    elif 4<=day<=10 and c4:
        flow="🌱 SLOW C4 확인"
    elif 4<=day<=10 and (c2 or c3):
        flow="🌱 SLOW C2/C3 확인"
    elif low_survive is False:
        flow="⚠️ 구조 약화 관찰"
    else:
        flow="👀 진행 관찰"

    return {
        "asof_date":g.iloc[cur_i]["Date"],
        "signal_date":sd,
        "code":norm_code(row.get("code")),
        "name":row.get("name",""),
        "day":day,
        "mfe_pct":mfe,
        "mae_pct":mae,
        "current_close_ret_pct":cur_ret,
        "d1_to_d2_higher_low":d1d2_hl,
        "d2_above_ma5":d2_ma5,
        "d1_d3_all_above_ma5":all3_ma5,
        "signal_low_survived":low_survive,
        "C2":c2,"C3":c3,"C4":c4,
        "C2_day":c2d,"C3_day":c3d,"C4_day":c4d,
        "flow":flow,
        "core_overlap_tag":row.get("core_overlap_tag",False),
    }

def xpct(v):
    return "-" if pd.isna(v) else f"{float(v):+.1f}%"

def xx(v):
    return "-" if pd.isna(v) else f"{float(v):.2f}x"

def make_message(signals,status,new_n,asof,expected,fresh,authority):
    if not fresh:
        return "\n".join([
            "⚠️ [ODOLI 독립 관찰 · SOURCE STALE]",
            f"KRX 기대 최신 거래일 {expected.date()}",
            f"최종 확보 거래일 {asof.date()}",
            f"source {authority}",
            "신규 prospective frozen 0건",
            "marcap + pykrx 보강으로도 최신 거래일을 완성하지 못했습니다.",
            "※ 기존 production 검색/점수/순위/주문에는 영향 없음",
        ])

    lines=[
        "🧪 [ODOLI 독립 관찰 · 전체시장]",
        f"기준일 {pd.Timestamp(asof).date()} · {DEFINITION}",
        f"source {authority}",
        f"오늘 전체시장 ODOLI {len(signals)} · 신규 frozen {new_n}",
        "REAL_FULL/A/CORE는 선별 조건으로 사용하지 않음",
    ]
    if len(signals):
        lines+=["","🆕 오늘 신규 ODOLI"]
        for _,r in signals.head(12).iterrows():
            core="CORE겹침" if truth(r.get("core_overlap_tag")) else "CORE비겹침"
            name=str(r.get("name","") or "").strip()
            label=f"{name}({r.get('code','')})" if name else str(r.get("code",""))
            lines.append(
                f"• {label} · 거래대금 {xx(r.get('signal_amount_ratio20'))} · "
                f"거래량 {xx(r.get('signal_volume_ratio20'))} · {core}"
            )
    active=status[pd.to_numeric(status.get("day",pd.Series(dtype=float)),errors="coerce").between(1,10)] if len(status) else status
    if len(active):
        lines+=["","👀 D1~D10 추적"]
        active=active.sort_values(["day","signal_date"],ascending=[False,True])
        for _,r in active.head(12).iterrows():
            tags=[x for x in ["C2","C3","C4"] if truth(r.get(x))]
            label=str(r.get("name","") or "").strip() or str(r.get("code",""))
            lines.append(
                f"• {label} D{int(float(r['day']))} · {r.get('flow','')} · "
                f"MFE {xpct(r.get('mfe_pct'))} / MAE {xpct(r.get('mae_pct'))} · "
                f"{'/'.join(tags) if tags else '-'}"
            )
    lines += [
        "",
        "📌 연구 해석",
        "FAST: D1→D2 higher-low + D2 MA5 회복, D1~D3 MA5 유지 여부 확인",
        "SLOW: D4~D10 signal-low 생존 + C2/C3/C4 확인",
        "겹침 태그는 성과 비교용일 뿐 가산점·매수 조건이 아님",
        "※ 연구 전용. production 검색/점수/순위/주문 변경 없음",
    ]
    return "\n".join(lines)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--marcap-root",required=True)
    ap.add_argument("--state-dir",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()

    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)
    state=Path(a.state_dir); state.mkdir(parents=True,exist_ok=True)

    migrated=migrate_pre_freshness_state(state)

    mar=load_marcap(a.marcap_root)
    base_last=pd.Timestamp(mar["Date"].max()).normalize()
    expected=expected_krx_session()

    mar,augdiag=augment_with_pykrx(mar,expected,out)
    (out/"source_augmentation_diagnostics.json").write_text(
        json.dumps(augdiag,ensure_ascii=False,indent=2,default=str),encoding="utf-8"
    )

    asof=pd.Timestamp(mar["Date"].max()).normalize()
    fresh=(asof==expected and bool(augdiag.get("all_missing_sessions_fetched",True)))
    source_status="FRESH" if fresh else "STALE_SOURCE"
    authority=str(augdiag.get("source_authority","UNKNOWN"))

    bycode={c:add_features(g) for c,g in mar.groupby("Code",sort=False)}

    signals=current_signals(bycode,asof) if fresh else pd.DataFrame()
    signals.to_csv(out/"odoli_current_signals.csv",index=False,encoding="utf-8-sig")

    events=load_csv(state/"events.csv")
    if fresh:
        events,new_n=append_events(signals,events)
    else:
        new_n=0
    events.to_csv(state/"events.csv",index=False,encoding="utf-8-sig")
    events.to_csv(out/"odoli_events.csv",index=False,encoding="utf-8-sig")

    rows=[]
    for _,r in events.iterrows():
        g=bycode.get(norm_code(r.get("code")))
        if g is None:
            continue
        x=event_status(r,g)
        if x:
            rows.append(x)
    status=pd.DataFrame(rows)
    status.to_csv(out/"odoli_current_status.csv",index=False,encoding="utf-8-sig")

    obs=load_csv(state/"observations.csv")
    if fresh and len(status):
        cur=status.copy()
        cur["asof_date"]=pd.to_datetime(cur["asof_date"],errors="coerce").dt.normalize()
        cur["signal_date"]=pd.to_datetime(cur["signal_date"],errors="coerce").dt.normalize()
        if len(obs):
            obs["asof_date"]=pd.to_datetime(obs["asof_date"],errors="coerce").dt.normalize()
            obs["signal_date"]=pd.to_datetime(obs["signal_date"],errors="coerce").dt.normalize()
            obs["code"]=obs["code"].map(norm_code)
            keys=set(zip(obs["asof_date"],obs["signal_date"],obs["code"]))
            cur=cur[[
                (pd.Timestamp(r.asof_date).normalize(),pd.Timestamp(r.signal_date).normalize(),norm_code(r.code)) not in keys
                for r in cur.itertuples()
            ]]
        if len(cur):
            obs=pd.concat([obs,cur],ignore_index=True)
    obs.to_csv(state/"observations.csv",index=False,encoding="utf-8-sig")

    msg=make_message(signals,status,new_n,asof,expected,fresh,authority)
    fp=hashlib.sha256(msg.encode("utf-8")).hexdigest()
    fpfile=state/"last_message_fingerprint.txt"
    old=fpfile.read_text(encoding="utf-8").strip() if fpfile.exists() else ""
    should_send=(fp!=old)
    if should_send:
        fpfile.write_text(fp,encoding="utf-8")
    (out/"telegram_message.txt").write_text(msg,encoding="utf-8")

    meta={
        "revision":REV,
        "definition":DEFINITION,
        "marcap_base_last_date":str(base_last.date()),
        "asof_date":str(asof.date()),
        "expected_krx_session":str(expected.date()),
        "source_status":source_status,
        "source_authority":authority,
        "fresh_source":fresh,
        "missing_sessions_requested":augdiag.get("missing_sessions",[]),
        "all_missing_sessions_fetched":bool(augdiag.get("all_missing_sessions_fetched",True)),
        "pykrx_augmented_rows":int(augdiag.get("augmented_rows",0) or 0),
        "research_only":True,
        "independent_all_market_lane":True,
        "real_full_source_required":False,
        "a_required":False,
        "core_required":False,
        "production_search_changed":False,
        "production_score_changed":False,
        "production_rank_changed":False,
        "production_order_changed":False,
        "automatic_ordering":False,
        "bootstrap_state_files_quarantined":migrated,
        "current_signals":len(signals),
        "frozen_events":len(events),
        "new_frozen_events":new_n,
        "telegram_route":"TELEGRAM_DYUL_CHAT_ID",
        "telegram_should_send":should_send,
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
    print(json.dumps(meta,ensure_ascii=False))

if __name__=="__main__":
    main()
