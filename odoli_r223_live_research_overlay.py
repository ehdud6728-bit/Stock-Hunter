#!/usr/bin/env python3
from __future__ import annotations

import argparse, hashlib, json, math, re
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_R2_23_LIVE_RESEARCH_OVERLAY_V1_20260928"
AMOUNT_Q2_UPPER=0.7487214470811911
VOLUME_Q2_UPPER=0.7562848676975253

def norm_code(v):
    s=re.sub(r"\D","",str(v or ""))
    return s[-6:].zfill(6) if s else ""

def safe_float(v):
    try:
        x=float(v)
        return x if math.isfinite(x) else np.nan
    except Exception:
        return np.nan

def truth(v):
    return str(v).strip().lower() in {"1","true","yes","y"}

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
    if i<25:
        return {}
    hist=g.iloc[:i+1]
    pre10=g.iloc[max(0,i-10):i].copy()
    p10=pre10.copy()
    p10["ret"]=pd.to_numeric(p10["Close"],errors="coerce").pct_change()
    down=p10[p10["ret"]<0]
    up=p10[p10["ret"]>0]
    da=pd.to_numeric(down["Amount"],errors="coerce").mean()
    ua=pd.to_numeric(up["Amount"],errors="coerce").mean()
    dv=pd.to_numeric(down["Volume"],errors="coerce").mean()
    uv=pd.to_numeric(up["Volume"],errors="coerce").mean()
    ar=da/ua if pd.notna(da) and pd.notna(ua) and ua>0 else np.nan
    vr=dv/uv if pd.notna(dv) and pd.notna(uv) and uv>0 else np.nan
    r=g.iloc[i]
    ma5=safe_float(r.get("ma5"))
    ma10=safe_float(r.get("ma10"))
    core=bool(
        pd.notna(ma5) and pd.notna(ma10) and ma5>ma10
        and (
            (pd.notna(ar) and ar<=AMOUNT_Q2_UPPER)
            or (pd.notna(vr) and vr<=VOLUME_Q2_UPPER)
        )
    )
    pre20=g.iloc[max(0,i-20):i]
    av=pd.to_numeric(pre20["Amount"],errors="coerce").mean()
    vv=pd.to_numeric(pre20["Volume"],errors="coerce").mean()
    amount_x=safe_float(r.get("Amount"))/av if pd.notna(av) and av>0 else np.nan
    volume_x=safe_float(r.get("Volume"))/vv if pd.notna(vv) and vv>0 else np.nan
    return {
        "ma5_gt_ma10":bool(pd.notna(ma5) and pd.notna(ma10) and ma5>ma10),
        "down_up_amount_ratio_10d":ar,
        "down_up_volume_ratio_10d":vr,
        "weak_down_participation":bool(
            (pd.notna(ar) and ar<=AMOUNT_Q2_UPPER)
            or (pd.notna(vr) and vr<=VOLUME_Q2_UPPER)
        ),
        "is_CORE":core,
        "signal_amount_ratio20":amount_x,
        "signal_volume_ratio20":volume_x,
    }

def strategy_is_a(r):
    for c in ["primary_strategy","strategy","mode","primary_mode"]:
        if c in r.index:
            v=str(r.get(c,"")).strip().upper()
            if v and v not in {"NAN","NONE"}:
                return v=="A"
    # REAL_FULL trust source historically represents the selected search surface,
    # but without explicit A authority we do not silently relabel it A.
    return False

def find_col(df, names):
    return next((c for c in names if c in df.columns),None)

def current_candidates(source_path, bycode, source_run):
    s=pd.read_csv(source_path,dtype=str,low_memory=False)
    codecol=find_col(s,["code","종목코드","Code"])
    namecol=find_col(s,["name","종목명","Name"])
    datecol=find_col(s,["signal_date","date","날짜","snapshot_date"])
    rankcol=find_col(s,["rank","origin_rank","순위","cross_top15_rank"])
    if not codecol:
        raise SystemExit("SOURCE_CODE_COLUMN_MISSING")

    rows=[]
    for _,r in s.head(30).iterrows():
        c=norm_code(r.get(codecol))
        if not c or c not in bycode:
            continue
        g=bycode[c]
        if datecol:
            sd=pd.to_datetime(r.get(datecol),errors="coerce")
            sd=sd.normalize() if pd.notna(sd) else g["Date"].max()
        else:
            sd=g["Date"].max()
        hit=g.index[g["Date"].eq(sd)]
        if len(hit)!=1:
            continue
        i=int(hit[0])
        rr=g.iloc[i]
        d=descriptor(g,i)
        is_odoli=bool(rr.get("strict_odoli_r1",False))
        is_a=strategy_is_a(r)
        rank=safe_float(r.get(rankcol)) if rankcol else np.nan
        rows.append({
            "signal_date":sd,
            "code":c,
            "name":str(r.get(namecol,c)) if namecol else c,
            "rank":rank,
            "is_A":is_a,
            "is_CORE":bool(d.get("is_CORE",False)),
            "is_ODOLI":is_odoli,
            "source_run":str(source_run),
            **d,
        })
    return pd.DataFrame(rows)

def ensure_state_dir(p):
    p=Path(p); p.mkdir(parents=True,exist_ok=True)
    return p

def load_csv(path, cols=None):
    p=Path(path)
    if not p.exists() or p.stat().st_size==0:
        return pd.DataFrame(columns=cols or [])
    return pd.read_csv(p,dtype={"code":str},low_memory=False)

def append_new_events(cands, events):
    if events.empty:
        events=pd.DataFrame(columns=[
            "signal_date","code","name","is_A","is_CORE","is_ODOLI","source_run",
            "notes","signal_amount_ratio20","signal_volume_ratio20",
            "down_up_amount_ratio_10d","down_up_volume_ratio_10d"
        ])
    if len(events):
        events["signal_date"]=pd.to_datetime(events["signal_date"],errors="coerce").dt.normalize()
        events["code"]=events["code"].map(norm_code)
    existing=set(zip(events.get("signal_date",pd.Series(dtype="datetime64[ns]")),
                     events.get("code",pd.Series(dtype=str))))
    add=[]
    # Primary frozen prospective lane: explicit A authority + CORE + ODOLI only.
    for _,r in cands.iterrows():
        if not (truth(r["is_A"]) and truth(r["is_CORE"]) and truth(r["is_ODOLI"])):
            continue
        key=(pd.Timestamp(r["signal_date"]).normalize(),norm_code(r["code"]))
        if key in existing:
            continue
        add.append({
            "signal_date":key[0],
            "code":key[1],
            "name":r["name"],
            "is_A":True,
            "is_CORE":True,
            "is_ODOLI":True,
            "source_run":r["source_run"],
            "notes":"R223_AUTO_CAPTURE_FROZEN_CORE_ODOLI",
            "signal_amount_ratio20":r.get("signal_amount_ratio20"),
            "signal_volume_ratio20":r.get("signal_volume_ratio20"),
            "down_up_amount_ratio_10d":r.get("down_up_amount_ratio_10d"),
            "down_up_volume_ratio_10d":r.get("down_up_volume_ratio_10d"),
        })
        existing.add(key)
    if add:
        events=pd.concat([events,pd.DataFrame(add)],ignore_index=True)
    return events, len(add)

def event_status(row, g):
    sd=pd.Timestamp(row["signal_date"]).normalize()
    hit=g.index[g["Date"].eq(sd)]
    if len(hit)!=1:
        return None
    i=int(hit[0])
    sig=float(g.iloc[i]["Close"])
    sig_low=float(g.iloc[i]["Low"])
    ma5=pd.to_numeric(g["Close"],errors="coerce").rolling(5).mean()
    cur_i=len(g)-1
    if cur_i<=i:
        day=0
    else:
        day=min(cur_i-i,60)

    q=g.iloc[i+1:cur_i+1].copy()
    mfe=((pd.to_numeric(q["High"],errors="coerce")/sig-1)*100).max() if len(q) else np.nan
    mae=((pd.to_numeric(q["Low"],errors="coerce")/sig-1)*100).min() if len(q) else np.nan
    current_close_ret=(float(g.iloc[cur_i]["Close"])/sig-1)*100 if cur_i>=i else np.nan

    d1d2_hl=np.nan; d2_ma5=np.nan; all3_ma5=np.nan; low_survive=np.nan
    if cur_i>=i+2:
        d1d2_hl=bool(float(g.iloc[i+2]["Low"])>float(g.iloc[i+1]["Low"]))
        d2_ma5=bool(float(g.iloc[i+2]["Close"])>float(ma5.iloc[i+2]))
    if cur_i>=i+3:
        all3_ma5=bool(all(float(g.iloc[i+k]["Close"])>float(ma5.iloc[i+k]) for k in [1,2,3]))
    if len(q):
        low_survive=bool(pd.to_numeric(q["Low"],errors="coerce").min()>=sig_low)

    # Frozen C2/C3/C4 prospective observers, D4-D10 only.
    c2=c3=c4=False
    confirm_day_c2=confirm_day_c3=confirm_day_c4=np.nan
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
        if (not c2) and above and hl and survive:
            c2=True; confirm_day_c2=j-i
        if (not c3) and above and pd.notna(ar) and ar>=1.0 and survive:
            c3=True; confirm_day_c3=j-i
        if (not c4) and above and hl and pd.notna(ar) and ar>=1.0 and survive:
            c4=True; confirm_day_c4=j-i

    if day==0:
        flow="🆕 D0 신규"
    elif day<=3 and bool(d1d2_hl is True) and bool(d2_ma5 is True):
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
        "code":norm_code(row["code"]),
        "name":row.get("name",""),
        "day":day,
        "mfe_pct":mfe,
        "mae_pct":mae,
        "current_close_ret_pct":current_close_ret,
        "d1_to_d2_higher_low":d1d2_hl,
        "d2_above_ma5":d2_ma5,
        "d1_d3_all_above_ma5":all3_ma5,
        "signal_low_survived":low_survive,
        "C2":c2,"C3":c3,"C4":c4,
        "C2_day":confirm_day_c2,"C3_day":confirm_day_c3,"C4_day":confirm_day_c4,
        "flow":flow,
    }

def fmt_x(v):
    return "-" if pd.isna(v) else f"{float(v):.2f}x"

def fmt_pct(v):
    return "-" if pd.isna(v) else f"{float(v):+.1f}%"

def make_message(cands, events, status, new_n, source_run):
    coreod=cands[
        cands["is_CORE"].astype(bool) & cands["is_ODOLI"].astype(bool)
    ].copy() if len(cands) else pd.DataFrame()
    od=cands[cands["is_ODOLI"].astype(bool)].copy() if len(cands) else pd.DataFrame()

    lines=[
        "🧪 [ODOLI 연구 오버레이 · 실전관찰]",
        f"source run {source_run} · production 검색/점수/순위/주문 변경 없음",
        f"오늘 후보 ODOLI {len(od)} · CORE+ODOLI {len(coreod)} · 신규 frozen A×CORE×ODOLI {new_n}",
    ]

    if len(coreod):
        lines.append("")
        lines.append("🔎 오늘 CORE+ODOLI")
        for _,r in coreod.head(5).iterrows():
            af="A확인" if truth(r.get("is_A")) else "A권한 미확인"
            lines.append(
                f"• {r['name']}({r['code']}) · {af} · "
                f"신호 거래대금 {fmt_x(r.get('signal_amount_ratio20'))} · "
                f"하락/상승 거래대금 {fmt_x(r.get('down_up_amount_ratio_10d'))}"
            )

    active=status[(pd.to_numeric(status["day"],errors="coerce")<=10)] if len(status) else status
    if len(active):
        lines.append("")
        lines.append("👀 D1~D10 추적")
        active=active.sort_values(["day","signal_date"],ascending=[False,True])
        for _,r in active.head(8).iterrows():
            tags=[]
            if truth(r.get("C2")): tags.append("C2")
            if truth(r.get("C3")): tags.append("C3")
            if truth(r.get("C4")): tags.append("C4")
            tag="/".join(tags) if tags else "-"
            lines.append(
                f"• {r['name']} D{int(r['day'])} · {r['flow']} · "
                f"MFE {fmt_pct(r['mfe_pct'])} / MAE {fmt_pct(r['mae_pct'])} · "
                f"{tag}"
            )

    lines += [
        "",
        "📌 해석 기준(연구 전용)",
        "FAST 관찰: D0~D3에서 higher-low·MA5 유지가 빠르게 확인되는 흐름",
        "SLOW 관찰: D4~D10 저점 생존 후 C2/C3/C4 확인 여부",
        "※ 매수/매도 신호가 아니라 지금까지 연구결과를 현재 후보에 겹쳐 보는 sidecar입니다.",
    ]
    return "\n".join(lines)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--source",required=True)
    ap.add_argument("--source-run",required=True)
    ap.add_argument("--marcap-root",required=True)
    ap.add_argument("--state-dir",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()

    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)
    state=ensure_state_dir(a.state_dir)
    mar=load_marcap(a.marcap_root)
    bycode={c:add_features(g) for c,g in mar.groupby("Code",sort=False)}

    cands=current_candidates(a.source,bycode,a.source_run)
    cands.to_csv(out/"r223_current_candidates.csv",index=False,encoding="utf-8-sig")

    events=load_csv(state/"events.csv")
    events,new_n=append_new_events(cands,events)
    events.to_csv(state/"events.csv",index=False,encoding="utf-8-sig")
    events.to_csv(out/"r223_events.csv",index=False,encoding="utf-8-sig")

    srows=[]
    for _,r in events.iterrows():
        c=norm_code(r.get("code"))
        g=bycode.get(c)
        if g is None: continue
        x=event_status(r,g)
        if x: srows.append(x)
    status=pd.DataFrame(srows)
    status.to_csv(out/"r223_current_status.csv",index=False,encoding="utf-8-sig")

    # Append daily observations without rewriting historical rows.
    obs=load_csv(state/"observations.csv")
    if len(status):
        cur=status.copy()
        cur["asof_date"]=pd.to_datetime(cur["asof_date"],errors="coerce").dt.normalize()
        if len(obs):
            obs["asof_date"]=pd.to_datetime(obs["asof_date"],errors="coerce").dt.normalize()
            obs["signal_date"]=pd.to_datetime(obs["signal_date"],errors="coerce").dt.normalize()
            keys=set(zip(obs["asof_date"],obs["signal_date"],obs["code"].map(norm_code)))
            cur=cur[[
                (pd.Timestamp(r.asof_date).normalize(),pd.Timestamp(r.signal_date).normalize(),norm_code(r.code)) not in keys
                for r in cur.itertuples()
            ]]
        if len(cur):
            obs=pd.concat([obs,cur],ignore_index=True)
    obs.to_csv(state/"observations.csv",index=False,encoding="utf-8-sig")

    msg=make_message(cands,events,status,new_n,a.source_run)
    fingerprint=hashlib.sha256(msg.encode("utf-8")).hexdigest()
    fpfile=state/"last_message_fingerprint.txt"
    prior=fpfile.read_text(encoding="utf-8").strip() if fpfile.exists() else ""
    should_send=(fingerprint!=prior)
    if should_send:
        fpfile.write_text(fingerprint,encoding="utf-8")
    (out/"telegram_message.txt").write_text(msg,encoding="utf-8")

    meta={
        "revision":REV,
        "research_only":True,
        "production_search_changed":False,
        "production_score_changed":False,
        "production_rank_changed":False,
        "production_order_changed":False,
        "automatic_ordering":False,
        "strict_odoli_definition":"STRICT_ODOLI_R1",
        "core_semantics":"FROZEN_R23_Q2_UPPER_BOUNDARY",
        "amount_q2_upper":AMOUNT_Q2_UPPER,
        "volume_q2_upper":VOLUME_Q2_UPPER,
        "source_run":str(a.source_run),
        "current_candidate_rows":len(cands),
        "current_odoli_rows":int(cands["is_ODOLI"].sum()) if len(cands) else 0,
        "current_core_odoli_rows":int((cands["is_CORE"] & cands["is_ODOLI"]).sum()) if len(cands) else 0,
        "frozen_prospective_events":len(events),
        "new_frozen_events":new_n,
        "telegram_should_send":should_send,
        "duplicate_message_suppressed":not should_send,
        "a_authority_policy":"append only when source explicitly identifies strategy A; never infer A when absent",
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
    print(json.dumps(meta,ensure_ascii=False))

if __name__=="__main__":
    main()
