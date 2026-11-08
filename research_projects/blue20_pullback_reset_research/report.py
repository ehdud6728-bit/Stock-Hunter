from pathlib import Path
from html import escape
import argparse,json,os
import numpy as np
import pandas as pd
from study import HERE,load_prices,indicators

CASES={('028300','2023-02-20'):'HLB',('145990','2023-05-08'):'삼양사',('383220','2024-07-17'):'F&F',('180640','2023-09-14'):'한진칼',('006220','2024-01-24'):'제주은행',('222080','2024-03-05'):'씨아이에스',('038500','2024-06-05'):'삼표시멘트',('034120','2023-02-07'):'SBS'}
def table(d):
    lines=['|'+'|'.join(d.columns)+'|','|'+'|'.join(['---']*len(d.columns))+'|']
    for row in d.itertuples(index=False,name=None):lines.append('|'+'|'.join('—' if pd.isna(v) else f'{v:.3f}' if isinstance(v,(float,np.floating)) else str(v) for v in row)+'|')
    return '\n'.join(lines)

def summarize(g):
    complete=g[g.opportunity_complete];a=complete[complete.closed]
    return dict(opportunities=len(g),complete_opportunities=len(complete),entries=int(g.entered.sum()),closed=int(g.closed.sum()),entry_pct=100*g.entered.mean(),
        mean_net_closed=a.net_pct.mean(),median_net_closed=a.net_pct.median(),mean_opportunity_net=complete.opportunity_net_pct.mean(),unknown_nets=int(complete.opportunity_net_pct.isna().sum()),
        stop_pct=100*a.reason.str.startswith('SL').mean(),tp_pct=100*a.reason.str.startswith('TP').mean(),mean_distance=a.distance_pct.mean(),median_distance=a.distance_pct.median(),
        mean_R=a.planned_R.mean(),mean_close_mdd=a.close_mdd_pct.mean(),worst_net=a.net_pct.min(),mean_risk_scaled=a.example_risk_scaled_net.mean(),missed10=int(g.missed10.sum()),missed20=int(g.missed20.sum()))

def cohorts(o):
    known=o.context_status.eq('OBSERVED_UNVERIFIED_SOURCE')
    return {'ALL':pd.Series(True,index=o.index),'STRONG_PRICE':known&o.strong_active.fillna(False),'STRONG_BLOCKING10':known&o.strong_blocking10.fillna(False),
            'STRONG_BODY':known&o.strong_body.fillna(False),'STRONG_RELEVANT':known&o.strong_relevant.fillna(False),'ANY_BODY':known&o.any_body.fillna(False),
            'REPEAT_PRICE':known&o.repeat_active.fillna(False),'VOLUME_MODEL':known&o.stable_volume_active.fillna(False),
            'ANY_EVIDENCE':known&o.any_active.fillna(False),'NONE_DETECTED':known&~o.any_active.fillna(False)}

def gallery(manifest,o,t,conf,zones,out):
    frames=load_prices(Path(manifest));selected=[];cal=pd.Index(sorted(set(v for dd in frames.values() for v in dd.date)))
    for (code,date),name in CASES.items():
        row=o[o.universe.eq('BASELINE_1804')&o.code.eq(code)&o.date.eq(date)&o.band.eq('UPPER')].iloc[0]
        selected.append((row.opportunity_id,name+' / 지정 기준점'))
    # Explanatory counterexamples, selected by a fixed date-first rule, not parameter fitting.
    fresh=t[t.horizon.eq('FRESH20')&t.policy.eq('B_RECOVERY')&t.opportunity_complete&t.closed]
    p=fresh.pivot(index='opportunity_id',columns='stop_mode',values='net_pct').dropna()
    for year in [2023,2024]:
        for label,mask in [('새저점 유리',p.PULLBACK_LOW>p.FIXED5),('새저점 불리',p.PULLBACK_LOW<p.FIXED5)]:
            ids=p.index[mask];a=o[o.opportunity_id.isin(ids)&o.year.eq(year)&o.strong_active].sort_values(['date','code','universe'])
            if len(a):selected.append((a.opportunity_id.iloc[0],f'{year} {label} / 날짜순 첫 강한저항 예시'))
    selected=list(dict.fromkeys(selected));page=['<!doctype html><html lang="ko"><meta charset="utf-8"><title>눌림 후 진입·새 손절선</title><style>body{font:16px system-ui;max-width:1380px;margin:auto;padding:24px;color:#19263c}svg{width:100%;border:1px solid #ddd;background:#fafbfc}table{border-collapse:collapse;font-size:12px}th,td{border:1px solid #ddd;padding:5px}section{margin:48px 0}</style>',
    '<h1>첫 신호 대기→후속 타점→새 손절선 비교</h1><p>검은 세로선은 최초 D0, 오른쪽 음영은 당시 아직 몰랐던 후속 가격입니다. 주황 존은 D−1에 고정한 과거 가격 저항이며 이후 다시 선택하지 않았습니다. 매물대 후보·실제 매도 잔량은 다릅니다. 확인일 뒤 다음 시가에 진입하며 손절선은 진입 뒤 내리지 않습니다.</p>',
    '<p>파랑: 고정−5%, 초록: 관측한 새 눌림 저점, 보라: 저점−0.25ATR. E는 진입, X는 청산. 같은 후속 타점에서 세 선을 비교합니다. 회색 영역의 새 저점을 과거 판단에 사용하지 않았습니다.</p>'];case_rows=[]
    for oid,label in selected:
        r=o[o.opportunity_id.eq(oid)].iloc[0];d=frames[r.code];j=int(d.index[d.date.eq(r.date)][0]);limit=cal[min(cal.get_loc(r.date)+60,len(cal)-1)]
        w=d.iloc[max(0,j-30):int(d.date.searchsorted(limit,side='right'))].reset_index(drop=True);start=max(0,j-30)
        tr=t[t.opportunity_id.eq(oid)&t.horizon.eq('FRESH20')]
        focus=next((name for name in ['B_RECOVERY','C_BROKEN_BODY','C_RETEST','D_NEW_BLUE','D_BLUE_RSI','R0_P3'] if tr[tr.policy.eq(name)].entered.any()),'A_FIRST')
        a=tr[tr.policy.eq(focus)];zz=zones[zones.opportunity_id.eq(oid)&zones.kind.eq('PRICE_REJECTION')]
        lo=w.low.min();hi=w.high.max()
        if len(zz):lo=min(lo,zz.low.min());hi=max(hi,zz.high.max())
        lo=min(lo,a.stop_price.min() if a.stop_price.notna().any() else lo);span=hi-lo
        x=lambda k:65+1130*k/(len(w)-1);y=lambda p:365-310*(p-lo)/span
        svg=[f'<svg viewBox="0 0 1300 520"><rect x="{x(j-start)}" y="30" width="{1195-x(j-start)}" height="440" fill="#e3e5e9" opacity=".5"/><text x="{x(j-start)+4}" y="23">D0 이후 관측 영역</text>']
        for q in zz.itertuples():
            color='#df9d39' if q.position=='ABOVE' else '#3c9c70' if q.position=='BELOW' else '#a76ac9'
            svg.append(f'<rect x="65" y="{y(q.high)}" width="1130" height="{y(q.low)-y(q.high)}" fill="{color}" opacity="{.16 if q.strong else .06}"><title>{q.zone_id} {q.low:.1f}~{q.high:.1f}, {q.count}회, {q.position}, D−1 고정</title></rect>')
        for k,b in w.iterrows():
            xx=x(k);color='#c44' if b.close>=b.open else '#2674b5'
            svg.append(f'<g><title>{b.date} O={b.open} H={b.high} L={b.low} C={b.close} V={b.volume}</title><path d="M{xx} {y(b.high)}V{y(b.low)}" stroke="{color}"/><rect x="{xx-2}" y="{y(max(b.open,b.close))}" width="4" height="{max(1,abs(y(b.open)-y(b.close)))}" fill="{color}"/></g><rect x="{xx-2}" y="{465-65*b.volume/w.volume.max()}" width="4" height="{65*b.volume/w.volume.max()}" fill="{color}" opacity=".6"/>')
        for q in conf[conf.opportunity_id.eq(oid)].itertuples():
            if q.confirm_date in set(w.date):
                k=int(w.index[w.date.eq(q.confirm_date)][0]);svg.append(f'<circle cx="{x(k)}" cy="{y(w.high.iloc[k])-8}" r="3" fill="#222"><title>{q.policy} 확인 {q.confirm_date}, 저점 {q.stop_low}, 저점관측 {q.low_date}</title></circle>')
        for q in a[a.entered].itertuples():
            color={'FIXED5':'#256bc5','PULLBACK_LOW':'#1b955d','LOW_MINUS_025ATR':'#9348ae'}[q.stop_mode]
            if q.entry_date not in set(w.date):continue
            k=int(w.index[w.date.eq(q.entry_date)][0]);end=int(w.index[w.date.eq(q.exit_date)][0]) if q.closed and q.exit_date in set(w.date) else len(w)-1
            svg.append(f'<path d="M{x(k)} {y(q.stop_price)}H{x(end)}" stroke="{color}" stroke-dasharray="5 3"/><circle cx="{x(k)}" cy="{y(q.entry_price)}" r="4" fill="{color}"><title>{q.stop_mode} E {q.entry_date} {q.entry_price}, stop {q.stop_price}</title></circle>')
            if q.closed and q.exit_date in set(w.date):svg.append(f'<text x="{x(end)}" y="{y(q.exit_price)}" fill="{color}" font-size="13">X<title>{q.stop_mode} {q.exit_date} {q.reason} {q.net_pct:.3f}%</title></text>')
        svg.append(f'<path d="M{x(j-start)} 30V470" stroke="black"/><text x="65" y="495">{w.date.iloc[0]} → {w.date.iloc[-1]}</text></svg>')
        page.extend([f'<section><h2>{escape(label)} {r.code} {r.date}</h2><p>그룹 {r.universe} / 강한 가격 저항 {r.strong_active} / 반복2회 후보 {r.repeat_active} / 일봉모델 후보 {r.stable_volume_active}. 초점 진입: {focus}</p>',''.join(svg)])
        cols=['policy','stop_mode','confirm_date','entry_date','low_date','stop_price','distance_pct','status','exit_date','reason','net_pct']
        page.append(tr[cols].to_html(index=False,float_format=lambda v:f'{v:.3f}'));page.append('</section>')
        for q in tr.to_dict('records'):case_rows.append({'case_label':label,**q})
    page.append('</html>');(out/'GALLERY.html').write_text('\n'.join(page),encoding='utf-8');pd.DataFrame(case_rows).to_csv(out/'CASES.csv',index=False)
    return len(selected)

def main(manifest):
    out=HERE/'results';o=pd.read_csv(out/'OPPORTUNITIES.csv.gz',dtype={'code':str});t=pd.read_csv(out/'TRADES.csv.gz',dtype={'code':str});conf=pd.read_csv(out/'CONFIRMATIONS.csv.gz',dtype={'code':str});z=pd.read_csv(out/'FROZEN_ZONES.csv.gz',dtype={'code':str})
    masks=cohorts(o);t=t.merge(o[['opportunity_id','context_status','strong_active','strong_blocking10','repeat_active','stable_volume_active','any_active','strong_body','strong_relevant','any_body']],on='opportunity_id',validate='many_to_one')
    summaries=[];cohort_counts=[]
    for cohort,mask in masks.items():
        ids=set(o.loc[mask,'opportunity_id']);part=t[t.opportunity_id.isin(ids)]
        for (universe,year),g in o[mask].groupby(['universe','year']):cohort_counts.append(dict(cohort=cohort,universe=universe,year=year,n=len(g)))
        for subset,a in [('ALL',part),('NONOVERLAP60',part[part.nonoverlap60]),('CLEAN_SOURCE',part[~part.known_gap_lookback_through60&~part.known_issue_initial])]:
            for key,g in a.groupby(['universe','year','policy','stop_mode','horizon']):summaries.append(dict(cohort=cohort,subset=subset,**dict(zip(['universe','year','policy','stop_mode','horizon'],key)),**summarize(g)))
    summary=pd.DataFrame(summaries);summary.to_csv(out/'SUMMARY.csv',index=False);pd.DataFrame(cohort_counts).to_csv(out/'COHORT_COUNTS.csv',index=False)
    byband=t.groupby(['universe','year','band','gc_offset','policy','stop_mode','horizon'],dropna=False)
    pd.DataFrame([{**dict(zip(['universe','year','band','gc_offset','policy','stop_mode','horizon'],key)),**summarize(g)} for key,g in byband]).to_csv(out/'BAND_RSI_SUMMARY.csv',index=False)
    rsi=t[t.policy.eq('D_BLUE_RSI')].merge(conf[conf.policy.eq('D_BLUE_RSI')][['opportunity_id','followup_gc_offset']],on='opportunity_id',how='left',validate='many_to_one')
    rsi['followup_rsi_group']=np.where(rsi.followup_gc_offset.isna(),'NO_CONFIRMED_GC',np.where(rsi.followup_gc_offset.eq(0),'GC0','GC_DELAY_1_10'))
    pd.DataFrame([{**dict(zip(['universe','year','followup_rsi_group','stop_mode','horizon'],key)),**summarize(g)} for key,g in rsi.groupby(['universe','year','followup_rsi_group','stop_mode','horizon'])]).to_csv(out/'FOLLOWUP_RSI_SUMMARY.csv',index=False)
    pairs=[]
    for cohort,mask in masks.items():
        ids=set(o.loc[mask,'opportunity_id']);part=t[t.opportunity_id.isin(ids)&t.opportunity_complete&t.closed&~t.policy.eq('A_FIRST')]
        complete=part.groupby(['opportunity_id','policy','horizon']).stop_mode.nunique();valid=set(complete[complete.eq(3)].index)
        part=part.loc[[tuple(v) in valid for v in part[['opportunity_id','policy','horizon']].to_numpy()]]
        base=part[part.stop_mode.eq('FIXED5')][['opportunity_id','policy','horizon','net_pct','reason','distance_pct','close_mdd_pct']]
        for mode in ['PULLBACK_LOW','LOW_MINUS_025ATR']:
            a=part[part.stop_mode.eq(mode)].merge(base,on=['opportunity_id','policy','horizon'],validate='one_to_one',suffixes=('','_fixed'))
            for key,g in a.groupby(['universe','year','policy','horizon']):pairs.append(dict(cohort=cohort,stop_mode=mode,**dict(zip(['universe','year','policy','horizon'],key)),n=len(g),fixed_mean=g.net_pct_fixed.mean(),new_mean=g.net_pct.mean(),delta=(g.net_pct-g.net_pct_fixed).mean(),new_distance=g.distance_pct.mean(),fixed_distance=g.distance_pct_fixed.mean(),new_mdd=g.close_mdd_pct.mean(),fixed_mdd=g.close_mdd_pct_fixed.mean(),beats_fixed_pct=100*g.net_pct.gt(g.net_pct_fixed).mean(),new_stop_fixed_tp=int((g.reason.str.startswith('SL')&g.reason_fixed.str.startswith('TP')).sum()),new_tp_fixed_stop=int((g.reason.str.startswith('TP')&g.reason_fixed.str.startswith('SL')).sum())))
    paired=pd.DataFrame(pairs);paired.to_csv(out/'PAIRED_STOP_EFFECT.csv',index=False)
    # Conditional switch uses only the initial frozen context; no future quality flags in its decision.
    cond_rows=[];a=t[t.policy.eq('A_FIRST')&t.horizon.eq('FRESH20')].set_index('opportunity_id')
    take=['status','entered','closed','net_pct','opportunity_net_pct','opportunity_complete','reason','distance_pct','planned_R','close_mdd_pct','example_risk_scaled_net','missed10','missed20']
    for rule,flag in [('STRONG_ACTIVE','strong_active'),('STRONG_BLOCKING10','strong_blocking10'),('ANY_EVIDENCE','any_active'),('STRONG_BODY','strong_body'),('STRONG_RELEVANT','strong_relevant')]:
        for (policy,mode),g in t[~t.policy.eq('A_FIRST')&t.horizon.eq('FRESH20')].groupby(['policy','stop_mode']):
            g=g.set_index('opportunity_id');first=a.reindex(g.index);wait=g[flag].fillna(False);valid_context=g.context_status.eq('OBSERVED_UNVERIFIED_SOURCE')
            mixed=g[['universe','year','nonoverlap60','known_gap_lookback_through60','known_issue_initial']].copy();mixed['switch_rule']=rule;mixed['followup_policy']=policy;mixed['followup_stop']=mode;mixed['wait_for_resistance']=wait;mixed['actual_policy']=np.where(wait,policy,'A_FIRST')
            for col in take:mixed[col]=g[col].where(wait,first[col])
            mixed.loc[~valid_context,['opportunity_net_pct','net_pct']]=np.nan;mixed.loc[~valid_context,'status']='UNKNOWN_INITIAL_CONTEXT'
            mixed.loc[~valid_context,['entered','closed','missed10','missed20']]=False
            cond_rows.append(mixed.reset_index())
    conditional=pd.concat(cond_rows,ignore_index=True);conditional.to_csv(out/'CONDITIONAL_POLICIES.csv.gz',index=False);cs=[]
    for key,g in conditional.groupby(['switch_rule','universe','year','followup_policy','followup_stop']):cs.append(dict(zip(['switch_rule','universe','year','followup_policy','followup_stop'],key),opportunities=len(g),waited=int(g.wait_for_resistance.sum()),entry_pct=100*g.entered.mean(),complete=int(g.opportunity_complete.sum()),mean_opportunity_net=g.loc[g.opportunity_complete,'opportunity_net_pct'].mean(),missed10=int(g.missed10.sum()),missed20=int(g.missed20.sum())))
    pd.DataFrame(cs).to_csv(out/'CONDITIONAL_SUMMARY.csv',index=False)
    retry=pd.read_csv(out/'RETRY_MAX2.csv.gz',dtype={'code':str});retry_summary=[]
    for key,g in retry.groupby(['universe','year','stop_mode']):
        c=g[g.opportunity_complete];retry_summary.append(dict(zip(['universe','year','stop_mode'],key),opportunities=len(g),retried=int(g.attempts.gt(1).sum()),total_roundtrips=int(g.roundtrips.sum()),total_cost_pct_points=g.total_cost_pct_points.sum(),mean_opportunity_net=c.net_compounded_pct.mean(),unknown=int(c.net_compounded_pct.isna().sum())))
    pd.DataFrame(retry_summary).to_csv(out/'RETRY_SUMMARY.csv',index=False)
    charts=gallery(manifest,o,t,conf,z,out);status=json.loads((out/'STATUS.json').read_text())
    main_table=summary[summary.cohort.isin(['STRONG_PRICE','STRONG_BODY'])&summary.subset.eq('ALL')&summary.horizon.eq('FRESH20')&summary.policy.isin(['A_FIRST','B_RECOVERY','C_RETEST','C_BROKEN_BODY','D_NEW_BLUE'])]
    main_pairs=paired[paired.cohort.isin(['STRONG_PRICE','STRONG_BODY'])&paired.horizon.eq('FRESH20')&paired.policy.isin(['B_RECOVERY','C_RETEST','C_BROKEN_BODY','D_NEW_BLUE'])]
    lines=['# 저항 앞 대기·후속 진입·새 눌림 손절 비교 R0',
    '이번에는 첫 신호를 버리는 정책이 아니라, 당시 저항을 고정해 다음 타점을 기다리는 정책을 연구했다. 후속 타점의 효과와 같은 타점에서 손절만 바꾼 효과를 분리했다. 기존 LIVE/SEARCH/WATCH·R1-D118·기존 연구를 수정하지 않았다. 임계값과 집단 정의는 PROTOCOL_KO.md에 사전 고정했다.',
    '## 모집단·보존',json.dumps(status,ensure_ascii=False,indent=2),
    'BASELINE_1804와 ALL_DAYS_P1은 서로 다른 집단이며 일부 종목·날짜가 겹친다. 합산해 표본을 늘려 주장하지 않는다. 기존1,804의 원래 RSI 진입과 D0+20 만기를 전수 재현했다. 별도 P1 2,178의 A_FIRST도 선행 실제 실행의 고정5% 결과와 대조한다. 기존1,804는 미래 RSI관측으로 선택됐으므로 전체 최초신호 모집단으로 일반화하지 않는다.',
    '## 첫 신호의 저항 고정',table(pd.DataFrame(cohort_counts)),
    'STRONG_PRICE는 최소3회 반락·중앙1ATR 이상인 가격 저항이 현재 종가 위 또는 존 안에 남은 집단이다. REPEAT_PRICE는2회 후보다. VOLUME_MODEL은 일봉 대리량의 모델 일치이며 실제 체결 분포가 아니다. NONE_DETECTED는 부재 인증이 아니다. 초기 신호때의 존을 그대로 유지하며 후속 성공 여부로 첫 저항을 재분류하지 않았다.',
    'STRONG_BODY는 신호 전일 종가가 존 상단 이하이고 D0 몸통으로 그 존을 통과한 별도 집합이다. 이는 이미 돌파한 과거 존/지지 후보이며 미돌파 상단 저항이 아니다. C_BROKEN_BODY는 이 동결한 존의 재시험을 기다린다. 활성 상단 C_RETEST와 구분하고 STRONG_RELEVANT 합집합도 별도로 제공한다.',
    '## 강한 가격 저항 집합: 후속 정책',table(main_table[['cohort','universe','year','policy','stop_mode','opportunities','complete_opportunities','entry_pct','mean_net_closed','mean_opportunity_net','mean_distance','mean_R','mean_close_mdd','missed10','missed20']]),
    '주 관찰창은 첫 신호+60일, 진입 확인 대기는40일, 각 확인일+20일 만기다. 늦은 타점에도 관측기간을 주지만 실제 달력 노출은 정책마다 달라진다. OLD20 표는 기존 첫 신호+20 만기를 유지해 별도로 제공한다. 기회평균은 공통60일 관측 완료 집합에서 미진입 현금0을 포함하고, 누락·미완료를0으로 채우지 않는다. 거래당 평균은 그 집합의 청산한 거래만이므로 진입률과 함께 봐야 한다. 최고가 경로의 놓친10/20은 실현 가능 수익률이 아니다.',
    '## 같은 진입·같은 만기: 손절선만 변경',table(main_pairs[['cohort','universe','year','policy','stop_mode','n','fixed_mean','new_mean','delta','new_distance','new_mdd','fixed_mdd','new_stop_fixed_tp','new_tp_fixed_stop']]),
    '세 선 모두 유효하고 같은 시가에 진입한 공통표본이다. 구조선이 진입 시가 이상이면 전체 기회표에는 미진입으로 남고 이 쌍 비교에서는 제외된다. PULLBACK_LOW는 후속 확인일까지 실제 눌림에서 관측한 최저가이며 최종 바닥 인증이 아니다. 후속의 더 높은 저점을 임의로 고르는 전략과도 다르다. 진입 후 더 낮은 저점으로 손절선을 내리지 않았다. 완충0.25ATR은 확인일 직전 ATR20을 사용했다. 결과를 보고 완충 폭을 바꾸지 않았다.',
    'PAIRED_STOP_EFFECT는 같은 타점의 순수 손절 차이를, SUMMARY는 진입률·미진입까지의 전체 효과를 보여준다. 둘의 표본을 섞어 해석하지 않는다. 종가MDD는 거래의 종가와 실제 청산가 경로이며 장중 최대손실·계좌MDD가 아니다. R은 순익/예정 손절거리라서 갭 손실에서−1R보다 나쁠 수 있다. 계좌위험1%·무차입 비중 예시는 실제 포트폴리오 결과가 아니다.',
    '## 조건부 조합과 반복 진입',
    'CONDITIONAL_SUMMARY는 처음 강한 저항이 있으면 후속타점을 기다리고, 미검출이면 A_FIRST를 택하는 조합이다. STRONG_ACTIVE/STRONG_BLOCKING10/ANY_EVIDENCE 세 기준을 모두 제공하며 성과로 유리한 것을 선택하지 않았다. 정확한 저항 실제 부재나 매수 안전성을 인증하지 않는다.',
    'RETRY_SUMMARY는 D_NEW_BLUE 첫 거래가 손절된 뒤 최대1회, 손절 이후 새로 생긴 확인저점과 새 BLUE20에서 재진입하는 별도 보조정책이다. 각 왕복0.25%p를 반영했고 결과는 순차 복리다. 손절 방식에 따라 첫 청산일이 달라 다음 재진입도 달라질 수 있어 이 표를 같은 진입의 손절 효과로 부르지 않는다. 무제한 재진입·물타기는 구현하지 않았다.',table(pd.DataFrame(retry_summary)),
    '## 차트와 해석 한계',
    f'실제 과거 일봉 차트 {charts}개를 생성했다. 최초D0·동결 저항·후속 확인·새 저점 관측일·진입·세 손절선·청산을 표시했다. 이후 가격 영역은 첫 신호 당시 몰랐던 정보로 구분했다. 지정8사례에서 강한3회 후보로 확인되지 않은 경우에도2회 후보·일봉모델 후보를 따로 표시했으며 사례에 맞춰 강도 문턱을 내리지 않았다. 후속기회가 없었던 사례도 CASES.csv에 남겼다.',
    '수익률은 기업행사·수정주가·보유수량 및 거래대금 세션 범위가 미검증인 원천 재현용 탐색값이다. CLEAN_SOURCE는 알려진 단절/기준가 단서 제외일 뿐 전체 인증이 아니다. 신호 이후 단절로 제외한 표는 품질 민감도이며 미래 품질 정보를 쓰는 매수 필터가 아니다. 실제 분봉/틱·HTS/TradingView 일치는 미검증이며 매도 잔량을 추정했다고 주장하지 않는다. 2025~2026년 자료는 읽지 않았다.',
    '## 실행 구분']
    if os.environ.get('GITHUB_ACTIONS')=='true':lines.append(f'현재 실행에서 생성: https://github.com/{os.environ.get("GITHUB_REPOSITORY")}/actions/runs/{os.environ.get("GITHUB_RUN_ID")}. 전체 성공 여부는 최종 작업 상태를 확인한다.')
    else:lines.append('이번 신규 연구는 로컬에서 계산했다. 아직 신규 GitHub Actions를 실행하지 않았다. 선행#42419112528은 재사용한 고정 대조군의 실행이다. 독립 패치ZIP과 루트YML 사본을 제공하며 실전 통합은 하지 않았다.')
    (out/'REPORT_KO.md').write_text('\n\n'.join(lines),encoding='utf-8');print(json.dumps({'reports':'COMPLETE','charts':charts,'paired_rows':len(paired),'conditional_rows':len(conditional)}))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--manifest',required=True);main(p.parse_args().manifest)
