#!/usr/bin/env python3
from __future__ import annotations
import json, math, os, re, sys
from pathlib import Path
import numpy as np
import pandas as pd

import closing_bet_live_shadow_board_r17 as r17
import closing_bet_live_shadow_board_r173 as r173

# Keep the R1.7.9 version string for workflow compatibility.
VERSION="CLOSEBET_LIVE_SHADOW_BOARD_R179_OUTCOME_PROBABILITY_20260922"
STRUCTURE_READOUT_REVISION="R179_FIX2_STRUCTURE_READOUT_20260924"
_TEST_MODE=False
_R13=pd.DataFrame()

_orig_find_lane=r17._find_lane
_orig_lane_candidate=r17._lane_is_candidate_generating
_orig_split=r17._build_telegram_parts

FAMS=[
    "EARLY_WIN_HELD",
    "SHAKEOUT_THEN_RECOVERY",
    "EARLY_WIN_GIVEBACK",
    "EARLY_STOP_SLOW_OR_NO_RECOVERY",
]
GOOD={"EARLY_WIN_HELD","SHAKEOUT_THEN_RECOVERY"}
RISK={"EARLY_WIN_GIVEBACK","EARLY_STOP_SLOW_OR_NO_RECOVERY"}

FEATURES=[
    ("CloseLoc","entry_close_loc_pct"),
    ("Vol20","entry_vol20_ratio"),
    ("Amount20","entry_amount20_ratio"),
    ("MA20","entry_ma20_dist_pct"),
    ("MA60","entry_ma60_dist_pct"),
    ("Ret5","entry_ret5_pct"),
]

PRIOR_STRENGTH=10.0
MIN_MODEL_N=20
MIN_FEATURES=3

def _read_json(p):
    try:return json.loads(Path(p).read_text(encoding="utf-8"))
    except Exception:return {}

def _read_csv(p):
    for enc in ("utf-8-sig","utf-8","cp949"):
        try:return pd.read_csv(p,encoding=enc,low_memory=False)
        except Exception:pass
    return pd.DataFrame()

def _latest_source_meta(root):
    xs=sorted(Path(root).rglob("live_shadow_source_meta_*.json"))
    for p in reversed(xs):
        d=_read_json(p)
        if d:return d
    return {}

def _arg_value(flag,default=""):
    try:
        i=sys.argv.index(flag); return sys.argv[i+1]
    except Exception:return default

def _patched_find_lane(root):
    d=_latest_source_meta(root)
    if bool(d.get("test_only")) and bool(d.get("manual_test_authorized")):
        return "MANUAL_TEST_CAPTURE"
    return _orig_find_lane(root)

def _patched_lane_candidate(lane):
    if str(lane or "").upper()=="MANUAL_TEST_CAPTURE": return True
    return _orig_lane_candidate(lane)

def _load_r13(root):
    xs=sorted(Path(root).rglob("shadow_outcome_refined_events.csv"))
    if not xs:return pd.DataFrame()
    return _read_csv(xs[0])

def _num_from_lines(lines,key):
    for ln in lines:
        if ln.startswith("CURRENT |"):
            m=re.search(rf"{re.escape(key)}\s+(-?[\d.]+)",ln)
            if m:
                try:return float(m.group(1))
                except Exception:return None
    return None

def _candidate_x(lines):
    x={}
    for label,col in FEATURES:
        v=_num_from_lines(lines,label)
        if v is not None and math.isfinite(v): x[col]=float(v)
    return x

def _robust_scale(s):
    x=pd.to_numeric(s,errors="coerce").dropna().astype(float)
    if len(x)<3:return np.nan
    med=float(x.median())
    mad=float((x-med).abs().median())
    scale=1.4826*mad
    if not math.isfinite(scale) or scale<=1e-12:
        q1,q3=x.quantile([.25,.75])
        scale=float(q3-q1)/1.349 if pd.notna(q3) and pd.notna(q1) else np.nan
    if not math.isfinite(scale) or scale<=1e-12:
        scale=float(x.std(ddof=0))
    return scale if math.isfinite(scale) and scale>1e-12 else np.nan

def _model_frame(pattern):
    if _R13.empty:return pd.DataFrame(),pd.DataFrame()
    q=_R13[_R13["pattern"].astype(str).str.upper().eq(str(pattern).upper())].copy()
    if q.empty:return q,q
    allq=q.copy()
    q=q[q["refined_family_r13"].isin(FAMS)].copy()
    return q,allq

def _distance_weights(train,x,exclude_index=None):
    cols=[c for _,c in FEATURES if c in x and c in train.columns]
    if len(cols)<MIN_FEATURES:return None,None,0
    scales={}
    for c in cols:
        vals=pd.to_numeric(train[c],errors="coerce")
        scales[c]=_robust_scale(vals)
    usable=[c for c in cols if math.isfinite(scales.get(c,np.nan))]
    if len(usable)<MIN_FEATURES:return None,None,len(usable)

    d2=[]; idx=[]
    for i,r in train.iterrows():
        if exclude_index is not None and i==exclude_index:continue
        zs=[]
        for c in usable:
            rv=pd.to_numeric(pd.Series([r.get(c)]),errors="coerce").iloc[0]
            if pd.isna(rv):continue
            zs.append(((float(x[c])-float(rv))/scales[c])**2)
        if len(zs)<MIN_FEATURES:continue
        d2.append(float(np.mean(zs))); idx.append(i)
    if not d2:return None,None,len(usable)
    d2=np.asarray(d2,float)
    w=np.exp(-0.5*d2)
    return np.asarray(idx),w,len(usable)

def _estimate(pattern,x,exclude_index=None):
    train,allq=_model_frame(pattern)
    if len(train)<MIN_MODEL_N:
        return {"status":"INSUFFICIENT","n":len(train),"all_n":len(allq)}

    idx,w,kfeat=_distance_weights(train,x,exclude_index=exclude_index)
    if idx is None or len(idx)==0 or float(w.sum())<=0:
        return {"status":"INSUFFICIENT_FEATURES","n":len(train),"features":kfeat,"all_n":len(allq)}

    tr=train.loc[idx]
    base_counts=train["refined_family_r13"].value_counts()
    base_n=float(sum(base_counts.get(f,0) for f in FAMS))
    base={f:(float(base_counts.get(f,0))/base_n if base_n else 0.0) for f in FAMS}

    local={}
    for f in FAMS:
        mask=tr["refined_family_r13"].astype(str).eq(f).to_numpy()
        local[f]=float(w[mask].sum())

    denom=float(w.sum())+PRIOR_STRENGTH
    probs={f:(local[f]+PRIOR_STRENGTH*base[f])/denom for f in FAMS}
    s=sum(probs.values()) or 1.0
    probs={f:v/s for f,v in probs.items()}

    ess=(float(w.sum())**2/float((w*w).sum())) if float((w*w).sum())>0 else 0.0
    good=sum(probs[f] for f in GOOD)
    risk=sum(probs[f] for f in RISK)
    base_good=sum(base[f] for f in GOOD)

    amb=0.0
    if len(allq):
        amb=float(allq["refined_family_r13"].astype(str).eq("AMBIGUOUS_OR_PENDING").mean())

    return {
        "status":"OK","n":len(train),"all_n":len(allq),"features":kfeat,
        "ess":ess,"probs":probs,"good":good,"risk":risk,
        "base_good":base_good,"ambiguous_share":amb,
    }

def _loo_brier(pattern):
    train,_=_model_frame(pattern)
    if len(train)<MIN_MODEL_N:return np.nan
    preds=[]; ys=[]
    for i,r in train.iterrows():
        x={}
        for _,c in FEATURES:
            v=pd.to_numeric(pd.Series([r.get(c)]),errors="coerce").iloc[0]
            if pd.notna(v):x[c]=float(v)
        est=_estimate(pattern,x,exclude_index=i)
        if est.get("status")!="OK":continue
        preds.append(float(est["good"]))
        ys.append(1.0 if str(r["refined_family_r13"]) in GOOD else 0.0)
    if not preds:return np.nan
    p=np.asarray(preds); y=np.asarray(ys)
    return float(np.mean((p-y)**2))

def _group_median_reasons(pattern,x):
    train,_=_model_frame(pattern)
    if train.empty:return [],[]
    g=train[train["refined_family_r13"].isin(GOOD)]
    r=train[train["refined_family_r13"].isin(RISK)]
    pos=[]; neg=[]
    for label,c in FEATURES:
        if c not in x or c not in train.columns:continue
        scale=_robust_scale(train[c])
        if not math.isfinite(scale):continue
        gm=pd.to_numeric(g[c],errors="coerce").median()
        rm=pd.to_numeric(r[c],errors="coerce").median()
        if pd.isna(gm) or pd.isna(rm):continue
        dg=abs(x[c]-float(gm))/scale
        dr=abs(x[c]-float(rm))/scale
        delta=dr-dg
        if delta>0:pos.append((delta,label))
        elif delta<0:neg.append((-delta,label))
    pos.sort(reverse=True);neg.sort(reverse=True)
    return [x[1] for x in pos[:2]],[x[1] for x in neg[:2]]

def _confidence(est,brier):
    # Forward calibration is incomplete, so HIGH is intentionally unavailable.
    if est.get("status")!="OK":return "LOW"
    n=int(est.get("n",0)); ess=float(est.get("ess",0)); f=int(est.get("features",0))
    if n>=60 and ess>=15 and f>=5 and math.isfinite(brier) and brier<=0.25:
        return "MEDIUM"
    return "LOW"

def _structure_readout(good,base,conf,pattern):
    """
    Human-readable relative structure.
    This is descriptive only and never a production score/gate.
    The key comparison is candidate probability vs SAME-PATTERN base rate.
    """
    delta=good-base

    if delta>=8:
        label="성공형 쪽 뚜렷" if conf=="MEDIUM" else "성공형 쪽 기울기 관찰"
    elif delta>=3:
        label="약한 성공형 기울기" if conf=="MEDIUM" else "약한 성공형 기울기 관찰"
    elif delta>-3:
        label="기본형에 가까움 · 혼합"
    elif delta>-8:
        label="약한 실패형 기울기" if conf=="MEDIUM" else "약한 실패형 기울기 관찰"
    else:
        label="실패형 쪽 뚜렷" if conf=="MEDIUM" else "실패형 쪽 기울기 관찰"

    sign="+" if delta>=0 else ""
    return [
        f"🧭 구조 판독 | {label}",
        f"   ↳ 성공형(유지/회복) {good:.0f} : 실패형(Giveback/부진) {100-good:.0f}",
        f"   ↳ {pattern}형 기본 성공률 {base:.0f}% 대비 {sign}{delta:.0f}%p · 신뢰도 {conf}",
    ]

def _type_text(pat,lines):
    vol=_num_from_lines(lines,"Vol20")
    ma20=_num_from_lines(lines,"MA20")
    ma60=_num_from_lines(lines,"MA60")
    ret5=_num_from_lines(lines,"Ret5")
    if pat=="A":
        if vol is not None and vol>=2 and ret5 is not None and ret5>=5:
            return "강한 모멘텀 + 거래대금 유입형"
        if ma20 is not None and ma20>=7:return "단기 이격 확장형"
        return "A형 모멘텀 재시동 관찰형"
    if pat=="B1":
        if ma20 is not None and ma20<=-8 and ma60 is not None and ma60<=-10:
            return "깊은 눌림 + 이평 훼손 회복 관찰형"
        if vol is not None and vol<1:return "저유동성 눌림 재시동 관찰형"
        return "B1 눌림 후 재진입 관찰형"
    if pat=="B2":return "눌림 후 유동성 재유입 관찰형"
    if pat=="C":return "1차 파동 후 눌림·재시동 관찰형"
    if pat=="I":return "조용한 눌림 후 재시동 관찰형"
    return f"{pat or 'UNKNOWN'}형 구조"

def _current_line(lines):
    vals=[]
    for key,label,suf,dec in [
        ("CloseLoc","CloseLoc","%",0),("Vol20","Vol20","x",2),("Amount20","Amount20","x",2),
        ("MA20","MA20","%",1),("MA60","MA60","%",1),("Ret5","Ret5","%",1),
    ]:
        v=_num_from_lines(lines,key)
        if v is None:continue
        if suf=="%":
            # CloseLoc is a 0~100 location metric, not a signed return.
            if key=="CloseLoc":
                vals.append(f"{label} {v:.{dec}f}%")
            else:
                vals.append(f"{label} {v:+.{dec}f}%")
        else:
            vals.append(f"{label} {v:.{dec}f}x")
    return " · ".join(vals)

def _checkpoints(pat,lines):
    vol=_num_from_lines(lines,"Vol20"); ma20=_num_from_lines(lines,"MA20")
    if pat=="A":
        x=["첫 눌림 거래감소","전일 종가/고가 재회복","재상승 거래대금 유지"]
        if ma20 is not None and ma20>=10:x[0]="MA20 과이격 되밀림 여부"
        return " · ".join(x)
    if pat=="B1":
        x=["초반 저점 회복","MA60 방향 복구","거래량·거래대금 재유입"]
        if vol is not None and vol<1:x[2]="Vol/Amt 1x 부근 재진입"
        return " · ".join(x)
    if pat=="C":return "D3~D5 구조 유지 · MA20/60 지지·회복 · 유동성 소멸 여부"
    if pat=="I":return "MA20 근처 재시동 · 과도한 거래폭발 여부 · 종가강도 악화 여부"
    return "거래대금 지속 · 이평 구조 유지 · 첫 눌림 후 재회복"

def _prob_text(pattern,lines):
    x=_candidate_x(lines)
    est=_estimate(pattern,x)
    if est.get("status")!="OK":
        return [
            "🧭 구조 판독 | 판독 보류 · 동일패턴 표본/feature 부족",
            f"확률 추정 | 표본/feature 부족 · status={est.get('status')} · n={est.get('n',0)}"
        ]

    brier=_loo_brier(pattern)
    conf=_confidence(est,brier)
    p=est["probs"]
    good=100*est["good"]; risk=100*est["risk"]; base=100*est["base_good"]
    amb=100*est["ambiguous_share"]
    tilt="유지/회복 쪽 기울음" if good-risk>=10 else ("Giveback/부진 쪽 기울음" if risk-good>=10 else "혼합")
    pos,neg=_group_median_reasons(pattern,x)

    out=[
        f"📊 outcome 추정 | 유지/회복 {good:.0f}% · Giveback/부진 {risk:.0f}% · {tilt}",
    ]
    out.extend(_structure_readout(good,base,conf,pattern))
    out.extend([
        f"패턴 기본률 | 유지/회복 {base:.0f}% · 과거 불확실/진행중 {amb:.0f}%",
        ("4경로 | "
         f"유지 {100*p['EARLY_WIN_HELD']:.0f}% · "
         f"흔들림후회복 {100*p['SHAKEOUT_THEN_RECOVERY']:.0f}% · "
         f"Giveback {100*p['EARLY_WIN_GIVEBACK']:.0f}% · "
         f"회복부진 {100*p['EARLY_STOP_SLOW_OR_NO_RECOVERY']:.0f}%"),
        f"신뢰도 | {conf} · OOS pattern n={est['all_n']} · 비모호 n={est['n']} · 유효feature={est['features']} · 유사표본 ESS={est['ess']:.1f}"
               + (f" · LOO Brier={brier:.3f}" if math.isfinite(brier) else ""),
    ])
    if pos:out.append("성공/회복 쪽 근거 | "+" · ".join(pos)+"가 과거 유지/회복군 중앙값에 상대적으로 가까움")
    if neg:out.append("주의 쪽 근거 | "+" · ".join(neg)+"가 Giveback/부진군 중앙값에 상대적으로 가까움")
    return out

def briefing(full):
    blocks=[b.strip() for b in full.split("\n\n") if b.strip()]
    candidates=[]; market=""; pats={}
    for b in blocks:
        if not b.startswith("🔬 "):continue
        lines=b.splitlines()
        name_pat=lines[0][2:].strip()
        if " · " in name_pat:name,pat=name_pat.rsplit(" · ",1)
        else:name,pat=name_pat,"UNKNOWN"
        pat=pat.strip().upper(); name=name.strip()
        pats[pat]=pats.get(pat,0)+1
        ml=next((x for x in lines if x.startswith("MARKET |")),"")
        if ml and not market:market=ml.split("|",1)[1].strip()
        candidates.append((name,pat,lines))

    if _TEST_MODE:
        head=["⚠️ MANUAL SHADOW TEST · RESEARCH ONLY · NOT LIVE",
              "실전 authority/성과표본 제외 · 형식/전송 검증용"]
    else:
        head=["🧪 SHADOW · RESEARCH ONLY"]

    head += [
        f"📌 분석 대상: {len(candidates)}종목 | "+" · ".join(f"{k} {v}" for k,v in pats.items()),
        "종목 | "+" · ".join(x[0] for x in candidates),
        "※ 구조 판독은 후보 확률을 같은 패턴 기본률과 비교한 연구용 상대판독 · 매매판정/production gate 아님",
        "※ 확률은 R1.3 OOS 과거표본 기반 경험적 추정치 · forward calibration 미완료",
        "※ Ambiguous/Pending은 확률 분모에서 제외하고 과거 비중만 별도 표시",
    ]
    if market:head.append("시장 | "+market)

    body=[]
    for i,(name,pat,lines) in enumerate(candidates,1):
        sec=[f"{i}) 🔬 {name} · {pat}",f"유형 | {_type_text(pat,lines)}"]
        cur=_current_line(lines)
        if cur:sec.append("현재 | "+cur)
        sec.extend(_prob_text(pat,lines))
        sec.append("다음 체크 | "+_checkpoints(pat,lines))
        body.append("\n".join(sec))

    foot=[
        "────────────────",
        "구조 판독 | 같은 패턴 기본률 대비 +8%p 이상 성공형, +3~8 약한 성공형, ±3 기본형/혼합, -3~-8 약한 실패형, -8 이하 실패형",
        "주의 | LOW 신뢰도에서는 강한 분류 대신 '기울기 관찰'로 완화",
        "확률 방법 | 동일 패턴 OOS 사건의 6개 구조 feature를 robust-z 거리로 비교 → Gaussian 유사도 가중 → 패턴 기본률로 10 pseudo-observation 수축",
        "검증 | leave-one-out Brier는 내부 진단일 뿐 새로운 forward 검증을 대체하지 않음",
        "분류 | 성공형=EARLY_WIN_HELD+SHAKEOUT_THEN_RECOVERY · 실패형=EARLY_WIN_GIVEBACK+SLOW/NO",
        "중요 | Giveback은 손실 확정 의미가 아니라 초반 +5% 이후 D5 기준 이익반납 경로",
        "production | 검색식·점수·랭킹·후보순서·주문 로직 변경 0",
    ]
    return "\n".join(head)+"\n\n"+"\n\n".join(body)+"\n\n"+"\n".join(foot)

def _prob_split(text,safe_limit=3500):
    return _orig_split(briefing(text),safe_limit=safe_limit)

def main():
    global _TEST_MODE,_R13
    root=_arg_value("--source-root","source_run")
    sm=_latest_source_meta(root)
    _TEST_MODE=bool(sm.get("test_only")) and bool(sm.get("manual_test_authorized"))
    _R13=_load_r13(os.environ.get("CLOSEBET_R13_ROOT","source_r13"))

    r17.VERSION=VERSION
    r17._find_lane=_patched_find_lane
    r17._lane_is_candidate_generating=_patched_lane_candidate
    r17._send_parts=r173._send_parts
    r17._build_telegram_parts=_prob_split
    r17.main()

    try:
        out=Path(_arg_value("--output-dir","reports/live_shadow_board_r17"))
        mp=out/"meta.json"
        if mp.exists():
            m=_read_json(mp)
            m.update({
                "version":VERSION,
                "structural_readout_revision":STRUCTURE_READOUT_REVISION,
                "structural_readout_enabled":True,
                "structural_readout_relative_to_same_pattern_base":True,
                "structural_readout_thresholds_pp":{"strong":8.0,"weak":3.0},
                "structural_readout_low_confidence_softened":True,
                "outcome_probability_enabled":True,
                "probability_semantics":"EXPERIMENTAL_OOS_EMPIRICAL_ESTIMATE_NOT_FORWARD_CALIBRATED",
                "probability_source_r13":"35443265652",
                "probability_features":[c for _,c in FEATURES],
                "probability_atr_used":False,
                "probability_ma224_used":False,
                "probability_prior_strength":PRIOR_STRENGTH,
                "probability_kernel":"GAUSSIAN_ROBUST_Z_FIXED_BANDWIDTH",
                "probability_ambiguous_excluded_from_denominator":True,
                "probability_candidate_order_changed":False,
                "probability_score_rank_changed":False,
                "production_logic_changed":False,
                "manual_test_capture":bool(_TEST_MODE),
                "i_pattern_held_zero_verified_from_frozen_r13":True,
                "i_pattern_frozen_counts":{
                    "all_n":111,
                    "non_ambiguous_n":79,
                    "EARLY_WIN_HELD":0,
                    "SHAKEOUT_THEN_RECOVERY":52,
                    "EARLY_WIN_GIVEBACK":25,
                    "EARLY_STOP_SLOW_OR_NO_RECOVERY":2,
                    "AMBIGUOUS_OR_PENDING":32,
                },
            })
            mp.write_text(json.dumps(m,ensure_ascii=False,indent=2),encoding="utf-8")
        dp=out/"telegram_delivery.json"
        if dp.exists():
            d=_read_json(dp)
            if _TEST_MODE:d["policy_reason"]="AUTO_ELIGIBLE_MANUAL_TEST_CAPTURE"
            d["presentation"]="HUMAN_BRIEFING_WITH_OUTCOME_PROBABILITY_AND_STRUCTURE_READOUT"
            d["probability_research_only"]=True
            d["structural_readout_research_only"]=True
            dp.write_text(json.dumps(d,ensure_ascii=False,indent=2),encoding="utf-8")
    except Exception:
        pass

if __name__=="__main__":
    main()
