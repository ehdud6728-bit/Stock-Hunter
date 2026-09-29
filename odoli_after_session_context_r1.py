#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup

REV="ODOLI_AFTER_SESSION_CONTEXT_R1_20260929"
KST=ZoneInfo("Asia/Seoul")
HEADERS={
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer":"https://finance.naver.com/",
}

def norm_code(v):
    s=re.sub(r"\D","",str(v or ""))
    return s[-6:].zfill(6) if s else ""

def fnum(v):
    try:
        x=float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None

def load_csv(p):
    p=Path(p)
    if not p.exists() or p.stat().st_size==0:
        return pd.DataFrame()
    try:
        return pd.read_csv(p,dtype=str,low_memory=False)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()

def fetch_after_info(code, krx_close):
    out={
        "after_price":None,
        "after_volume":None,
        "after_ret_from_krx_close_pct":None,
        "after_has_data":False,
        "after_source":"NAVER_AFTER_INFO_OBSERVATIONAL",
        "after_error":"",
    }
    try:
        url=f"https://finance.naver.com/item/main.naver?code={code}"
        r=requests.get(url,headers=HEADERS,timeout=8)
        r.raise_for_status()
        r.encoding="euc-kr"
        soup=BeautifulSoup(r.text,"html.parser")
        box=soup.select_one(".after_info")
        if box is None:
            out["after_error"]="AFTER_INFO_NOT_FOUND"
            return out
        nums=box.select(".num")
        if not nums:
            out["after_error"]="AFTER_PRICE_NOT_FOUND"
            return out
        price_txt=re.sub(r"[^0-9.]","",nums[0].get_text(" ",strip=True).replace(",",""))
        price=fnum(price_txt)
        if price is None or price<=0 or krx_close<=0:
            out["after_error"]="AFTER_PRICE_INVALID"
            return out
        vol=None
        if len(nums)>1:
            vol_txt=re.sub(r"[^0-9]","",nums[1].get_text(" ",strip=True).replace(",",""))
            vol=fnum(vol_txt)
        out.update({
            "after_price":price,
            "after_volume":vol,
            "after_ret_from_krx_close_pct":(price/krx_close-1.0)*100.0,
            "after_has_data":True,
            "after_error":"",
        })
        return out
    except Exception as e:
        out["after_error"]=f"{type(e).__name__}:{e}"[:180]
        return out

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--odoli-state-dir",required=True)
    ap.add_argument("--after-state-dir",required=True)
    ap.add_argument("--output-dir",required=True)
    ap.add_argument("--max-workers",type=int,default=12)
    a=ap.parse_args()

    odoli_state=Path(a.odoli_state_dir)
    after_state=Path(a.after_state_dir)
    out=Path(a.output_dir)
    after_state.mkdir(parents=True,exist_ok=True)
    out.mkdir(parents=True,exist_ok=True)

    events=load_csv(odoli_state/"events.csv")
    captured_at=datetime.now(KST).isoformat(timespec="seconds")
    meta={
        "revision":REV,
        "research_only":True,
        "signal_close_authority":"KRX_1530_CLOSE_FROZEN",
        "after_session_used_as_gate":False,
        "real_full_changed":False,
        "core224_changed":False,
        "production_changed":False,
        "captured_at_kst":captured_at,
        "source":"NAVER_AFTER_INFO_OBSERVATIONAL",
        "source_is_observational_only":True,
    }

    if events.empty:
        pd.DataFrame().to_csv(out/"odoli_after_session_context.csv",index=False)
        meta.update({"status":"NO_ODOLI_STATE","signal_date":None,"cohort_n":0,"after_data_n":0,"coverage_pct":0.0})
    else:
        events["signal_date"]=pd.to_datetime(events["signal_date"],errors="coerce").dt.normalize()
        events["code"]=events["code"].map(norm_code)
        latest=events["signal_date"].max()
        cohort=events[events["signal_date"].eq(latest)].copy()
        cohort["krx_close"]=pd.to_numeric(cohort.get("close"),errors="coerce")
        cohort=cohort[cohort["code"].ne("") & cohort["krx_close"].gt(0)].copy()

        rows=[r for _,r in cohort.iterrows()]
        def one(r):
            d={
                "signal_date":pd.Timestamp(r["signal_date"]).date().isoformat(),
                "code":r["code"],
                "name":r.get("name",""),
                "market":r.get("market",""),
                "krx_close":float(r["krx_close"]),
                "captured_at_kst":captured_at,
            }
            d.update(fetch_after_info(r["code"],float(r["krx_close"])))
            return d

        with ThreadPoolExecutor(max_workers=max(1,a.max_workers)) as ex:
            result=list(ex.map(one,rows))
        cur=pd.DataFrame(result)
        cur.to_csv(out/"odoli_after_session_context.csv",index=False,encoding="utf-8-sig")

        hist=load_csv(after_state/"after_session_history.csv")
        if not hist.empty:
            hist["signal_date"]=pd.to_datetime(hist["signal_date"],errors="coerce").dt.strftime("%Y-%m-%d")
            hist["code"]=hist["code"].map(norm_code)
            keys=set(zip(cur["signal_date"],cur["code"]))
            hist=hist[[ (str(r.signal_date),norm_code(r.code)) not in keys for r in hist.itertuples() ]]
        hist=pd.concat([hist,cur],ignore_index=True,sort=False)
        hist.to_csv(after_state/"after_session_history.csv",index=False,encoding="utf-8-sig")
        hist.to_csv(out/"odoli_after_session_history.csv",index=False,encoding="utf-8-sig")

        have=int(cur["after_has_data"].astype(str).str.lower().isin(["true","1"]).sum()) if len(cur) else 0
        coverage=(100.0*have/len(cur)) if len(cur) else 0.0
        meta.update({
            "status":"PASS" if have else "NO_AFTER_DATA",
            "signal_date":pd.Timestamp(latest).date().isoformat(),
            "cohort_n":int(len(cur)),
            "after_data_n":have,
            "coverage_pct":coverage,
        })

        good=cur[cur["after_has_data"].eq(True)].copy() if "after_has_data" in cur else pd.DataFrame()
        lines=[
            "🌙 [ODOLI AFTER_SESSION_CONTEXT · 연구전용]",
            f"D0 {meta['signal_date']} · cohort {meta['cohort_n']} · AFTER 확보 {meta['after_data_n']} ({meta['coverage_pct']:.1f}%)",
            "신호 종가 authority = KRX 15:30 고정",
            "AFTER 데이터는 FAST/SLOW 설명변수일 뿐 gate/점수/랭킹에 사용하지 않음",
        ]
        if len(good):
            good["after_ret_from_krx_close_pct"]=pd.to_numeric(good["after_ret_from_krx_close_pct"],errors="coerce")
            top=good.sort_values("after_ret_from_krx_close_pct",ascending=False).head(5)
            bot=good.sort_values("after_ret_from_krx_close_pct",ascending=True).head(5)
            lines+=["","📈 AFTER 상대강도 상위"]
            for _,r in top.iterrows():
                lines.append(f"• {r.get('name','') or r['code']} · {r['after_ret_from_krx_close_pct']:+.2f}%")
            lines+=["","📉 AFTER 상대약도 하위"]
            for _,r in bot.iterrows():
                lines.append(f"• {r.get('name','') or r['code']} · {r['after_ret_from_krx_close_pct']:+.2f}%")
        msg="\n".join(lines)
        (out/"telegram_message.txt").write_text(msg,encoding="utf-8")
        fp=hashlib.sha256(msg.encode("utf-8")).hexdigest()
        fpfile=after_state/"last_message_fingerprint.txt"
        old=fpfile.read_text(encoding="utf-8").strip() if fpfile.exists() else ""
        meta["telegram_should_send"]=bool(fp!=old)
        if meta["telegram_should_send"]:
            fpfile.write_text(fp,encoding="utf-8")

    meta.setdefault("telegram_should_send",False)
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps(meta,ensure_ascii=False))

if __name__=="__main__":
    main()
