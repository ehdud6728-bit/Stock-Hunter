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

REV="ODOLI_AFTER_SESSION_CONTEXT_R2_NXT_JSON_PRIMARY_20261012"
KST=ZoneInfo("Asia/Seoul")

HEADERS={
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer":"https://finance.naver.com/",
    "Accept":"application/json,text/plain,*/*",
}

def norm_code(v):
    s=re.sub(r"\D","",str(v or ""))
    return s[-6:].zfill(6) if s else ""

def fnum(v):
    try:
        if isinstance(v,str):
            v=v.replace(",","").strip()
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

def _walk_dicts(obj):
    if isinstance(obj,dict):
        yield obj
        for v in obj.values():
            yield from _walk_dicts(v)
    elif isinstance(obj,list):
        for v in obj:
            yield from _walk_dicts(v)

def _pick_nxt_payload(payload, code):
    """
    Naver realtime JSON can be wrapped in result/areas/datas.
    Search recursively and prefer the row matching the requested item code.
    """
    code=norm_code(code)
    candidates=[]
    for d in _walk_dicts(payload):
        info=d.get("nxtOverMarketPriceInfo")
        if isinstance(info,dict):
            row_code=norm_code(
                d.get("cd") or d.get("itemCode") or d.get("code") or d.get("symbolCode") or ""
            )
            score=2 if row_code==code else 1
            candidates.append((score,d,info))
    if not candidates:
        return None,None
    candidates.sort(key=lambda x:x[0],reverse=True)
    _,row,info=candidates[0]
    return row,info

def fetch_nxt_json(code, krx_close):
    out={
        "after_price":None,
        "after_volume":None,
        "after_value":None,
        "after_ret_from_krx_close_pct":None,
        "after_has_data":False,
        "after_source":"NAVER_REALTIME_NXT_OVERMARKET",
        "after_session_type":"",
        "after_market_status":"",
        "after_local_traded_at":"",
        "after_error":"",
    }
    try:
        url="https://polling.finance.naver.com/api/realtime"
        params={"query":f"SERVICE_ITEM:{norm_code(code)}"}
        r=requests.get(url,params=params,headers=HEADERS,timeout=10)
        r.raise_for_status()

        # Endpoint has historically returned JSON with varying encoding headers.
        try:
            payload=r.json()
        except Exception:
            txt=r.content.decode("utf-8","replace")
            payload=json.loads(txt)

        row,info=_pick_nxt_payload(payload,code)
        if not isinstance(info,dict):
            out["after_error"]="NXT_OVERMARKET_INFO_NOT_FOUND"
            return out

        price=fnum(
            info.get("overPrice")
            or info.get("closePrice")
            or info.get("currentPrice")
            or info.get("price")
        )
        vol=fnum(
            info.get("accumulatedTradingVolume")
            or info.get("accTradeVolume")
            or info.get("tradingVolume")
        )
        val=fnum(
            info.get("accumulatedTradingValue")
            or info.get("accTradeValue")
            or info.get("tradingValue")
        )

        session=str(info.get("tradingSessionType") or "")
        status=str(info.get("overMarketStatus") or "")
        traded_at=str(info.get("localTradedAt") or "")

        if price is None or price<=0 or krx_close<=0:
            out.update({
                "after_session_type":session,
                "after_market_status":status,
                "after_local_traded_at":traded_at,
                "after_error":"NXT_OVERMARKET_PRICE_INVALID",
            })
            return out

        out.update({
            "after_price":price,
            "after_volume":vol,
            "after_value":val,
            "after_ret_from_krx_close_pct":(price/krx_close-1.0)*100.0,
            "after_has_data":True,
            "after_session_type":session,
            "after_market_status":status,
            "after_local_traded_at":traded_at,
            "after_error":"",
        })
        return out
    except Exception as e:
        out["after_error"]=f"NXT_JSON:{type(e).__name__}:{e}"[:180]
        return out

def fetch_legacy_html(code, krx_close):
    out={
        "after_price":None,
        "after_volume":None,
        "after_value":None,
        "after_ret_from_krx_close_pct":None,
        "after_has_data":False,
        "after_source":"NAVER_AFTER_INFO_HTML_FALLBACK",
        "after_session_type":"",
        "after_market_status":"",
        "after_local_traded_at":"",
        "after_error":"",
    }
    try:
        url=f"https://finance.naver.com/item/main.naver?code={norm_code(code)}"
        r=requests.get(url,headers=HEADERS,timeout=8)
        r.raise_for_status()
        r.encoding=r.apparent_encoding or "euc-kr"
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
        out["after_error"]=f"HTML:{type(e).__name__}:{e}"[:180]
        return out

def fetch_after_info(code, krx_close):
    """
    Primary: Naver realtime JSON NXT over-market payload.
    Fallback: legacy HTML .after_info only if JSON has no usable NXT quote.
    AFTER remains observational and never enters gate/score/rank/order.
    """
    primary=fetch_nxt_json(code,krx_close)
    if primary.get("after_has_data"):
        return primary

    fallback=fetch_legacy_html(code,krx_close)
    if fallback.get("after_has_data"):
        fallback["after_error_primary"]=primary.get("after_error","")
        return fallback

    # Preserve both diagnostics so source failure can be audited.
    primary["after_error_fallback"]=fallback.get("after_error","")
    return primary

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
        "source":"NAVER_REALTIME_NXT_OVERMARKET_PRIMARY_WITH_HTML_FALLBACK",
        "source_is_observational_only":True,
    }

    if events.empty:
        pd.DataFrame().to_csv(out/"odoli_after_session_context.csv",index=False)
        meta.update({
            "status":"NO_ODOLI_STATE",
            "signal_date":None,
            "cohort_n":0,
            "after_data_n":0,
            "coverage_pct":0.0,
        })
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
            hist=hist[[
                (str(r.signal_date),norm_code(r.code)) not in keys
                for r in hist.itertuples()
            ]]

        hist=pd.concat([hist,cur],ignore_index=True,sort=False)
        hist.to_csv(after_state/"after_session_history.csv",index=False,encoding="utf-8-sig")
        hist.to_csv(out/"odoli_after_session_history.csv",index=False,encoding="utf-8-sig")

        have=int(
            cur["after_has_data"].astype(str).str.lower().isin(["true","1"]).sum()
        ) if len(cur) else 0
        coverage=(100.0*have/len(cur)) if len(cur) else 0.0

        source_counts={}
        error_counts={}
        if len(cur):
            if "after_source" in cur:
                source_counts=cur["after_source"].fillna("").value_counts().to_dict()
            if "after_error" in cur:
                error_counts=cur["after_error"].fillna("").replace("", "NONE").value_counts().head(10).to_dict()

        meta.update({
            "status":"PASS" if have else "NO_AFTER_DATA",
            "signal_date":pd.Timestamp(latest).date().isoformat(),
            "cohort_n":int(len(cur)),
            "after_data_n":have,
            "coverage_pct":coverage,
            "source_counts":source_counts,
            "top_error_counts":error_counts,
        })

        good=cur[cur["after_has_data"].astype(str).str.lower().isin(["true","1"])].copy() if "after_has_data" in cur else pd.DataFrame()

        lines=[
            "🌙 [ODOLI AFTER_SESSION_CONTEXT · 연구전용]",
            f"D0 {meta['signal_date']} · cohort {meta['cohort_n']} · AFTER 확보 {meta['after_data_n']} ({meta['coverage_pct']:.1f}%)",
            "신호 종가 authority = KRX 15:30 고정",
            "AFTER = NXT over-market 관찰값 · FAST/SLOW 설명변수 전용",
            "gate/점수/랭킹/주문에는 사용하지 않음",
        ]

        if len(good):
            good["after_ret_from_krx_close_pct"]=pd.to_numeric(
                good["after_ret_from_krx_close_pct"],errors="coerce"
            )
            top=good.sort_values("after_ret_from_krx_close_pct",ascending=False).head(5)
            bot=good.sort_values("after_ret_from_krx_close_pct",ascending=True).head(5)

            lines+=["","📈 AFTER 상대강도 상위"]
            for _,r in top.iterrows():
                lines.append(f"• {r.get('name','') or r['code']} · {r['after_ret_from_krx_close_pct']:+.2f}%")

            lines+=["","📉 AFTER 상대약도 하위"]
            for _,r in bot.iterrows():
                lines.append(f"• {r.get('name','') or r['code']} · {r['after_ret_from_krx_close_pct']:+.2f}%")
        else:
            lines+=["","⚠️ AFTER quote 0건 · source/error diagnostics 확인 필요"]

        msg="\n".join(lines)
        (out/"telegram_message.txt").write_text(msg,encoding="utf-8")

        fp=hashlib.sha256(msg.encode("utf-8")).hexdigest()
        fpfile=after_state/"last_message_fingerprint.txt"
        old=fpfile.read_text(encoding="utf-8").strip() if fpfile.exists() else ""
        meta["telegram_should_send"]=bool(fp!=old)
        if meta["telegram_should_send"]:
            fpfile.write_text(fp,encoding="utf-8")

    meta.setdefault("telegram_should_send",False)
    (out/"meta.json").write_text(
        json.dumps(meta,ensure_ascii=False,indent=2),
        encoding="utf-8"
    )
    print(json.dumps(meta,ensure_ascii=False))

if __name__=="__main__":
    main()
