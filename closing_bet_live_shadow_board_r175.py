#!/usr/bin/env python3
from __future__ import annotations
import re
import closing_bet_live_shadow_board_r17 as r17
import closing_bet_live_shadow_board_r173 as r173

VERSION="CLOSEBET_LIVE_SHADOW_BOARD_R175_COMPACT_TYPE_EXPECTATION_20260921"
_orig_split=r17._build_telegram_parts

def _short(s):
    for a,b in {
        "Shakeout→Recovery":"회복",
        "Giveback":"Giveback",
        "Slow/No recovery":"Slow/No",
        "Ambiguous/Pending":"불확실",
        "CloseLoc":"Close",
        "Amount20":"Amt",
        "UpperWick(body-relative)":"Wick",
    }.items():
        s=s.replace(a,b)
    return re.sub(r"\s+"," ",s).strip()

def _num_from_current(lines, key):
    for ln in lines:
        if ln.startswith("CURRENT |"):
            m=re.search(rf"{re.escape(key)}\s+(-?[\d.]+)",ln)
            if m:
                try:return float(m.group(1))
                except:return None
    return None

def _type_and_expectation(title, lines):
    pat=title.rsplit(" · ",1)[-1] if " · " in title else ""

    close=_num_from_current(lines,"CloseLoc")
    vol=_num_from_current(lines,"Vol20")
    amt=_num_from_current(lines,"Amount20")
    ma20=_num_from_current(lines,"MA20")
    ma60=_num_from_current(lines,"MA60")
    ret5=_num_from_current(lines,"Ret5")

    # Descriptive type labels only; no score/ranking/filter.
    if pat=="A":
        if (vol is not None and vol>=2.0) and (ret5 is not None and ret5>=5):
            typ="강한 단기 모멘텀 + 거래대금 유입형"
        elif (ma20 is not None and ma20>=7):
            typ="단기 이격 확장형"
        else:
            typ="A형 모멘텀 재시동 후보"

        exp="과거 A 표본에서는 흔들림 후 회복과 초기상승 후 Giveback이 주된 두 경로"
        if ma20 is not None and ma20>=10:
            exp+=" · MA20 과이격이면 되밀림 여부 특히 관찰"
        elif vol is not None and vol>=2:
            exp+=" · 유동성 유지 시 회복 경로 여부 관찰"
        return typ,exp

    if pat=="B1":
        if (ma20 is not None and ma20<=-8) and (ma60 is not None and ma60<=-10):
            typ="깊은 눌림 + 이평 훼손 후 재진입 관찰형"
        elif vol is not None and vol<1:
            typ="저유동성 눌림 재시동 관찰형"
        else:
            typ="B1 눌림 후 재진입 관찰형"

        exp="과거 B1 표본에서는 Giveback과 Shakeout→Recovery가 주된 경로"
        if vol is not None and vol<1:
            exp+=" · 유동성 재유입이 핵심"
        if ma60 is not None and ma60<0:
            exp+=" · MA60 회복 여부 확인"
        return typ,exp

    return f"{pat or 'UNKNOWN'}형 구조", "과거 경로와 현재 구조의 지속 여부를 관찰"

def _current(line):
    body=line.split("|",1)[1].strip()
    body=body.replace("CloseLoc ","Close ").replace("Vol20 ","Vol ").replace("Amount20 ","Amt ")
    parts=[x.strip() for x in body.split("·")]
    return " · ".join(x for x in parts if x.startswith(("Close ","Vol ","Amt ","MA20 ","MA60 ","Ret5 ")))

def _path(line):
    body=line.split("|",1)[1].strip()
    out=[]
    for x in [z.strip() for z in body.split("·") if z.strip()]:
        m=re.match(r"(.+?)\s+\d+/\d+\s+\(([\d.]+%)\)",x)
        if not m:continue
        name,pct=_short(m.group(1)),m.group(2)
        if name=="Held":
            try:
                if float(pct.rstrip("%"))<3:continue
            except:pass
        out.append(f"{name} {pct}")
    return " · ".join(out[:4])

def _prox(line):
    body=line.split("|",1)[1].strip().replace(" · LOW CONFIDENCE"," LOW")
    chunks=re.split(r"\s+·\s+(?=[A-Za-z])",body)
    out=[]
    for c in chunks[:6]:
        c=_short(c)
        c=re.sub(r"\s+median\(n=(\d+)\)",r"(n=\1)",c)
        out.append(c)
    return " · ".join(out)

def compact(full):
    historical="HISTORICAL REPLAY FORMAT TEST" in full
    blocks=[b.strip() for b in full.split("\n\n") if b.strip()]
    candidates=[]; market=""; pats=[]

    for b in blocks:
        if not b.startswith("🔬 "):continue
        lines=b.splitlines()
        title=lines[0]
        pat=title.rsplit(" · ",1)[-1] if " · " in title else ""
        if pat and pat not in pats:pats.append(pat)

        typ,exp=_type_and_expectation(title,lines)
        cur=path=prox=""
        for ln in lines[1:]:
            if ln.startswith("CURRENT |"):cur=_current(ln)
            elif ln.startswith("HISTORICAL PATH |"):path=_path(ln)
            elif ln.startswith("FEATURE ROUTE PROXIMITY |"):prox=_prox(ln)
            elif ln.startswith("MARKET |") and not market:market=ln.split("|",1)[1].strip()

        c=[title,f"유형 | {typ}",f"관찰 기대 | {exp}"]
        if cur:c.append("현재 | "+cur)
        if path:c.append("경로 | "+path)
        if prox:c.append("근접 | "+prox)
        candidates.append("\n".join(c))

    head=["🧪 SHADOW · RESEARCH ONLY · COMPACT"]
    if historical:
        head.append("⚠️ HISTORICAL REPLAY FORMAT TEST · NOT LIVE")
    head.append("※ '관찰 기대'는 과거 OOS 경로 설명이며 예측 확률/매매판정 아님")
    if market:head.append("시장 | "+market)

    notes=[]
    if "A" in pats:
        notes.append("A 공통 | D3~D5 유동성 · MA20 과이격 · 종가/이평 재회복")
    if "B1" in pats:
        notes.append("B1 공통 | 초기 stop≠즉시 실패 · D5 유지 · MA60/224 회복 · 유동성 재진입")
    notes.append("공통 | proximity는 항목별 과거 중앙값 근접도 · 종합 판정/확률 아님 · n<3 제외 · n<10 LOW")
    notes.append("Catalyst | 현재 UNKNOWN · forward capture 필요")
    return "\n".join(head)+"\n\n"+"\n\n".join(candidates)+"\n\n"+"\n".join(notes)

def _compact_split(text,safe_limit=3500):
    return _orig_split(compact(text),safe_limit=safe_limit)

def main():
    r17.VERSION=VERSION
    r17._send_parts=r173._send_parts
    r17._build_telegram_parts=_compact_split
    r17.main()

if __name__=="__main__":
    main()
