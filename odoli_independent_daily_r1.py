#!/usr/bin/env python3
from __future__ import annotations
import argparse, hashlib, json, math, re
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_INDEPENDENT_DAILY_R1_20260928"
DEFINITION="STRICT_ODOLI_R1"
AMOUNT_Q2_UPPER=0.7487214470811911
VOLUME_Q2_UPPER=0.7562848676975253

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
            "source":"ALL_MARKET_MARCAP_PIT",
            **d,
        })
    return pd.DataFrame(rows)

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
        add.append(r.to_dict()); existing.add(key)
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

def make_message(signals,status,new_n,asof):
    lines=[
        "🧪 [ODOLI 독립 관찰 · 전체시장]",
        f"기준일 {pd.Timestamp(asof).date()} · {DEFINITION}",
        f"오늘 전체시장 ODOLI {len(signals)} · 신규 frozen {new_n}",
        "REAL_FULL/A/CORE는 선별 조건으로 사용하지 않음",
    ]
    if len(signals):
        lines+=["","🆕 오늘 신규 ODOLI"]
        for _,r in signals.head(12).iterrows():
            core="CORE겹침" if truth(r.get("core_overlap_tag")) else "CORE비겹침"
            lines.append(
                f"• {r.get('name','')}({r.get('code','')}) · "
                f"거래대금 {xx(r.get('signal_amount_ratio20'))} · "
                f"거래량 {xx(r.get('signal_volume_ratio20'))} · {core}"
            )
    active=status[pd.to_numeric(status.get("day",pd.Series(dtype=float)),errors="coerce").between(1,10)] if len(status) else status
    if len(active):
        lines+=["","👀 D1~D10 추적"]
        active=active.sort_values(["day","signal_date"],ascending=[False,True])
        for _,r in active.head(12).iterrows():
            tags=[x for x in ["C2","C3","C4"] if truth(r.get(x))]
            lines.append(
                f"• {r.get('name','')} D{int(float(r['day']))} · {r.get('flow','')} · "
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

    mar=load_marcap(a.marcap_root)
    asof=pd.Timestamp(mar["Date"].max()).normalize()
    bycode={c:add_features(g) for c,g in mar.groupby("Code",sort=False)}

    signals=current_signals(bycode,asof)
    signals.to_csv(out/"odoli_current_signals.csv",index=False,encoding="utf-8-sig")

    events=load_csv(state/"events.csv")
    events,new_n=append_events(signals,events)
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
    if len(status):
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

    msg=make_message(signals,status,new_n,asof)
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
        "asof_date":str(asof.date()),
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
        "current_signals":len(signals),
        "frozen_events":len(events),
        "new_frozen_events":new_n,
        "telegram_should_send":should_send,
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
    print(json.dumps(meta,ensure_ascii=False))

if __name__=="__main__":
    main()
