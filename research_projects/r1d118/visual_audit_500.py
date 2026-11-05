#!/usr/bin/env python3
"""R118 500-cohort research-only audit. Read canonical R118 output and SHA-pinned actual OHLCV.

No recalibration of signal rules, no calls to LIVE/SEARCH/WATCH. Historical outcome
charts are *ex-post diagnostics*, not forecasts. Corporate actions are NOT
certified by price heuristics. This file does not change the existing R118 engine.
"""
from __future__ import annotations
import argparse,csv,hashlib,json,math,sys
from pathlib import Path
import numpy as np
import pandas as pd

from research_r1d118 import load_data,features,lower_events,dedup_events,COST_PCT,market_context

REPORT_COLUMNS=['cohort','market_condition','events','stocks','dates','winners','losers_or_flat',
                'win_rate_pct','mean_d5_net_pct','median_d5_net_pct',
                'date_equal_weight_mean_pct','stock_equal_weight_mean_pct','research_status']


def sha256(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def load_manifest(manifest):
    p=Path(manifest).resolve()
    x=pd.read_csv(p,dtype=str).fillna('')
    if not {'code','path','sha256'}.issubset(x.columns):raise ValueError('manifest requires code,path,sha256')
    if x.code.duplicated().any() or not x.code.str.fullmatch(r'\d{6}').all():
        raise ValueError('invalid cohort manifest codes')
    files={}
    for q in x.to_dict('records'):
        f=Path(q['path']);f=f if f.is_absolute() else p.parent/f
        if sha256(f).lower()!=q['sha256'].lower():raise ValueError('OHLCV_SHA256_MISMATCH '+q['code'])
        files[q['code']]=f
    return files


def check_500_cohort(files,audit,baseline_200):
    if len(files)!=500:raise ValueError('NOT_500_STOCKS '+str(len(files)))
    if audit.get('selected_count')!=500 or set(audit.get('selected_codes',[]))!=set(files):
        raise ValueError('500_COHORT_IDENTITY_MISMATCH')
    old=set(baseline_200)
    if len(old)!=200 or not old.issubset(files):raise ValueError('FROZEN_200_NOT_SUBSET')
    return old


def verify_one_event(event, d, market_lookup, canonical_cache=None):
    """Strict original rule replay for one R118 ledger signal; return actual prices."""
    s=features(d).reset_index(drop=True)
    code=str(event['code']).zfill(6)
    i=int(event['signal_idx']);a=int(event['anchor_idx']);ex=i+5
    if not (0<=a<=i and ex<len(s)):raise ValueError('INVALID_SIGNAL_INDEX')
    if str(s.at[i,'date'])!=str(event['signal_date']) or str(s.at[a,'date'])!=str(event['anchor_date']):
        raise ValueError('SIGNAL_ANCHOR_DATE_MISMATCH')
    if str(s.at[i+1,'date'])!=str(event['entry_date']) or str(s.at[ex,'date'])!=str(event['exit_date']):
        raise ValueError('ENTRY_EXIT_DATE_MISMATCH')
    ent=float(s.at[i+1,'open']);out=float(s.at[ex,'close'])
    expected=100*(out/ent-1)-COST_PCT
    if not math.isclose(expected,float(event['d5_net_pct']),rel_tol=0,abs_tol=1e-6):
        raise ValueError('LEDGER_PNL_VS_OHLCV_MISMATCH')
    k=(str(event['market']),str(event['signal_date']))
    if k not in market_lookup:raise ValueError('MARKET_CONTEXT_MISSING')
    if not math.isclose(float(event['market_ret20_pct']),market_lookup[k],rel_tol=0,abs_tol=1e-6):
        raise ValueError('MARKET_RET20_MISMATCH')
    if str(event['family'])!='LOWER_RECLAIM' or str(event['band'])!='BB20':
        raise ValueError('NON_BB20_RECLAIM')
    if canonical_cache is None:
        ev,_=lower_events(code,s)
        can=dedup_events(ev)
        canonical_cache=set((str(x.family),str(x.band),str(x.signal_date),int(x.signal_idx),int(x.anchor_idx))
                            for x in can.itertuples(index=False))
    key=('LOWER_RECLAIM','BB20',str(event['signal_date']),i,a)
    if key not in canonical_cache:raise ValueError('NOT_CANONICAL_DEDUP_BB20_RECLAIM')
    if not market_lookup[k] < -2.0:raise ValueError('NOT_MARKET_DOWN_2')
    if not float(s.at[a,'low'])<=float(s.at[a,'BB20_LOWER']) or not float(s.at[i,'close'])>=float(s.at[i,'BB20_LOWER']):
        raise ValueError('BAND_TOUCH_RECLAIM_VIOLATION')
    return s,{'entry_open':ent,'exit_close':out,'bb20_lower_signal':float(s.at[i,'BB20_LOWER']),
             'net_pct':expected,'market_ret20_pct':market_lookup[k]}


def corp_risk(s, i, market_dates, *,lookback=40,lookforward=5):
    """Conservative red flags, not DART/KIND corporate action certification."""
    a=max(1,i-lookback);b=min(len(s)-1,i+lookforward)
    w=s.iloc[a-1:b+1]
    gaps=(w.open/w.close.shift(1)-1).abs()
    moves=w.close.pct_change().abs()
    flags=[]
    if (gaps>0.32).any():flags.append('OVERNIGHT_GAP_GT32PCT')
    if (moves>0.35).any():flags.append('CLOSE_MOVE_GT35PCT')
    # Only check entry-to-exit exchange days: trading suspension invalidates a 5-day sell.
    ds=str(s.at[i,'date']);d5=str(s.at[i+5,'date'])
    expected=[date for date in market_dates if ds<=date<=d5]
    observed=s.iloc[i:i+6].date.astype(str).tolist()
    if expected!=observed:flags.append('MARKET_TRADING_DAYS_MISSING_POSSIBLE_SUSPENSION')
    return flags


def summarize(df,old):
    rows=[]
    cohorts={'ALL_500':df,'FROZEN_200':df[df.code.isin(old)],'ADDED_300':df[~df.code.isin(old)]}
    for nm,d in cohorts.items():
        for group,sub in [('MARKET_DOWN_LT_MINUS2',d[d.market_ret20_pct<-2]),
                          ('CONTROL_MARKET_GE_MINUS2',d[d.market_ret20_pct>=-2])]:
            if sub.empty:continue
            v=sub.d5_net_pct.astype(float)
            rows.append(dict(cohort=nm,market_condition=group,events=len(sub),stocks=sub.code.nunique(),
              dates=sub.signal_date.nunique(),winners=int((v>0).sum()),losers_or_flat=int((v<=0).sum()),
              win_rate_pct=100*float((v>0).mean()),mean_d5_net_pct=float(v.mean()),
              median_d5_net_pct=float(v.median()),
              date_equal_weight_mean_pct=float(sub.groupby('signal_date').d5_net_pct.mean().mean()),
              stock_equal_weight_mean_pct=float(sub.groupby('code').d5_net_pct.mean().mean()),
              research_status='EXPLORATORY_NOT_OOS_CORPORATE_ACTION_UNVERIFIED'))
    return pd.DataFrame(rows,columns=REPORT_COLUMNS)


def diverse_indices(sub,k=5):
    """Representative outcome quantiles; avoid pure best-five/worst-five cherry-picking."""
    if len(sub)<k:return []
    sub=sub.sort_values(['d5_net_pct','code','signal_date']).reset_index(drop=True)
    qidx=[round((len(sub)-1)*q) for q in [0.10,0.30,0.50,0.70,0.90]]
    return [sub.iloc[i].to_dict() for i in qidx]


def make_chart(s,e,details,out_path):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    i=int(e['signal_idx']);a=max(0,i-38);b=min(len(s)-1,i+18)
    w=s.iloc[a:b+1].copy()
    fig,(ax,vax)=plt.subplots(2,1,figsize=(12.5,7.3),sharex=True,
                         gridspec_kw={'height_ratios':[3.2,1.0]})
    for j,(_,r) in enumerate(w.iterrows()):
        c='#cf3b50' if r.close>=r.open else '#2c70af'
        ax.vlines(j,r.low,r.high,color=c,linewidth=1.1)
        low=min(r.open,r.close);height=max(abs(r.close-r.open),max(r.close*0.0008,1))
        ax.add_patch(Rectangle((j-0.30,low),0.60,height,facecolor=c,edgecolor=c,linewidth=.5))
        vax.bar(j,r.amount/1e8,width=.66,color=c,alpha=.80)
    ax.plot(np.arange(len(w)),w.BB20_LOWER.to_numpy(),color='#8d5bbe',label='BB20 lower (MA20 - 2sd)')
    ax.plot(np.arange(len(w)),w.MA20.to_numpy(),color='#e5aa4f',label='MA20',alpha=.8)
    ax.plot(np.arange(len(w)),w.BB20_UPPER.to_numpy(),color='#8d5bbe',linestyle='--',alpha=.5,label='BB20 upper')
    positions={int(e['signal_idx']):('Signal','#262626'),int(e['signal_idx'])+1:('Entry','#228355'),
               int(e['signal_idx'])+5:('Exit','#b04d30')}
    for pos,(label,col) in positions.items():
        if not a<=pos<=b:continue
        k=pos-a;ax.axvline(k,color=col,linestyle=':',linewidth=1.4);vax.axvline(k,color=col,linestyle=':',linewidth=1.2)
        ax.annotate(label,(k,float(s.at[pos,'high'])),xytext=(k+1,float(s.at[pos,'high'])*1.025),color=col,fontsize=10)
    dates=w.date.astype(str).tolist();step=max(1,len(dates)//9)
    ticks=list(range(0,len(dates),step));vax.set_xticks(ticks,labels=[dates[t][5:] for t in ticks],rotation=25,ha='right')
    pnl=float(details['net_pct']);outcome='WIN' if pnl>0 else 'LOSS'
    ax.set_title(f"{e['code']}  BB20 LOWER_RECLAIM  {outcome} {pnl:+.2f}%  signal {e['signal_date']}\n"
                 f"market 20D {details['market_ret20_pct']:+.2f}% | D+1 open entry / D+5 close exit | -0.25pp fee")
    ax.set_ylabel('Price (KRW)');ax.grid(True,alpha=.16)
    ax.legend(loc='upper left',fontsize=8,ncol=2)
    vax.set_ylabel('Amount (100M KRW)');vax.grid(True,alpha=.15)
    fig.text(.5,.01,'REAL HISTORICAL OHLCV | POST-EVENT AUDIT ONLY | OFFICIAL CORPORATE ACTIONS NOT CERTIFIED',
             ha='center',fontsize=8,color='#a52a2a')
    fig.tight_layout(rect=(0,.03,1,1));fig.savefig(out_path,dpi=135);plt.close(fig)


def main(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument('--ledger',required=True);p.add_argument('--manifest',required=True)
    p.add_argument('--market',required=True);p.add_argument('--source-audit',required=True)
    p.add_argument('--baseline-200',required=True);p.add_argument('--out',required=True)
    p.add_argument('--per-outcome',type=int,default=5)
    args=p.parse_args(argv)
    if args.per_outcome!=5:raise ValueError('frozen case count is exactly 5 successes and 5 failures')
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    files=load_manifest(args.manifest)
    audit=json.loads(Path(args.source_audit).read_text(encoding='utf8'))
    old=check_500_cohort(files,audit,json.loads(Path(args.baseline_200).read_text(encoding='utf8'))['codes'])
    ledger=pd.read_csv(args.ledger,dtype={'code':str,'signal_date':str,'anchor_date':str,'entry_date':str,'exit_date':str},low_memory=False)
    if ledger.code.nunique()>500 or not set(ledger.code).issubset(files):raise ValueError('EVENT_COHORT_MISMATCH')
    bb=ledger[(ledger.family=='LOWER_RECLAIM')&(ledger.band=='BB20')].copy()
    if bb.empty or not bb.market_ret20_pct.notna().all():raise ValueError('NO_BB20_VALID_MARKET_EVENT')
    summary=summarize(bb,old)
    summary.to_csv(out/'R118_500_BB20_MARKET_STRATA.csv',index=False,float_format='%.8f')
    mm=pd.read_csv(args.market,dtype={'market':str,'date':str})
    mk=market_context(args.market)
    market_lookup={(x.market,x.date):float(x.market_ret20_pct) for x in mk.itertuples(index=False)
                   if pd.notna(x.market_ret20_pct)}
    trading_dates={k:sorted(v.date.astype(str).tolist()) for k,v in mm.groupby('market')}
    data_cache={};canonical_cache={};risk_rows=[];cases=[];used_codes=set();used_dates=set()
    candidates=bb[(bb.market_ret20_pct<-2)&bb.d5_net_pct.notna()].copy()
    wins=candidates[candidates.d5_net_pct>0];losses=candidates[candidates.d5_net_pct<=0]
    def get_data(code):
        if code not in data_cache:
            one=load_data(files[code]);data_cache[code]=one[code]
        return data_cache[code]
    # Deterministic stratification: inspect success/loss quantiles, then fill
    # from nearest outcome quantile across distinct names & dates.
    for outcome,frame in [('WIN',wins),('LOSS',losses)]:
        f=frame.sort_values(['d5_net_pct','code','signal_date']).reset_index(drop=True)
        qs=[.1,.3,.5,.7,.9]
        targets=[float(f.d5_net_pct.quantile(q)) for q in qs] if len(f) else []
        attempts=set();made=0
        for target in targets:
            ranked=f.assign(distance=(f.d5_net_pct-target).abs()).sort_values(['distance','code','signal_date'])
            for e in ranked.to_dict('records'):
                uid=(e['code'],e['signal_date']);code=e['code'];date=e['signal_date']
                if uid in attempts:continue
                attempts.add(uid)
                if code in used_codes or date in used_dates:continue
                try:
                    d=get_data(code)
                    if code not in canonical_cache:
                        fs=features(d);events,_=lower_events(code,fs)
                        z=dedup_events(events)
                        canonical_cache[code]=set((str(x.family),str(x.band),str(x.signal_date),int(x.signal_idx),int(x.anchor_idx))
                                                 for x in z.itertuples(index=False))
                    s,details=verify_one_event(e,d,market_lookup,canonical_cache[code])
                    flags=corp_risk(s,int(e['signal_idx']),trading_dates[str(e['market'])])
                    if flags:
                        risk_rows.append({'code':code,'signal_date':date,'outcome':outcome,'flags':';'.join(flags)})
                        continue
                    if (outcome=='WIN' and details['net_pct']<=0) or (outcome=='LOSS' and details['net_pct']>0):
                        raise ValueError('OUTCOME_SIGN_MISMATCH')
                    png=f'R118_500_{outcome}_{made+1:02d}_{code}_{date}.png'
                    make_chart(s,e,details,out/png)
                    cases.append({'outcome':outcome,'code':code,'signal_date':date,'anchor_date':e['anchor_date'],
                      'entry_date':e['entry_date'],'exit_date':e['exit_date'],
                      'entry_open':details['entry_open'],'exit_close':details['exit_close'],
                      'net_pct':details['net_pct'],'market_ret20_pct':details['market_ret20_pct'],
                      'bb20_lower_signal':details['bb20_lower_signal'],
                      'amount_eok':e['amount_eok'],'amount_ratio20':e['amount_ratio20'],
                      'price_sha256':sha256(files[code]),'chart':png,
                      'official_corporate_action_status':'NOT_VERIFIED',
                      'selection_rule':'OUTCOME_QUANTILE_DISTINCT_STOCK_AND_SIGNAL_DATE'})
                    used_codes.add(code);used_dates.add(date);made+=1
                    break
                except Exception as ex:
                    if str(ex).startswith('OHLCV_SHA256_MISMATCH'):raise
                    risk_rows.append({'code':code,'signal_date':date,'outcome':outcome,
                                      'flags':'CASE_REPLAY_REJECTED_'+type(ex).__name__+':'+str(ex)[:120]})
            if made>=5:break
        if made!=5:raise AssertionError(f'INSUFFICIENT_VERIFIED_{outcome}_CHARTS {made}/5')
    pd.DataFrame(cases).to_csv(out/'R118_500_VERIFIED_WIN_LOSS_10.csv',index=False,encoding='utf-8-sig')
    pd.DataFrame(risk_rows,columns=['code','signal_date','outcome','flags']).to_csv(out/'R118_500_REVIEW_QUEUE.csv',index=False)
    metadata={'status':'GENERATED_PRICE_REPLAY_VERIFIED_CORPORATE_ACTION_OFFICIAL_UNVERIFIED',
       'universe':500,'frozen_200_subgroup':len(old),'new_names':len(files)-len(old),
       'research_years':[2023,2024],'chart_count':len(cases),'source':'Git blob SHA-pinned Marcap derivative',
       'notoos':'Original data/exploratory period, NOT out-of-sample',
       'rules':'BB20 LOWER_RECLAIM after market_ret20_pct < -2%, same dedup & entry/exit/cost as R118',
       'corporate_action_policy':'heuristic red flags only, must check DART/KIND before approval',
       'source_cohort_asof':audit.get('selection_asof'),
       'input_sha256':{'manifest':sha256(args.manifest),'ledger':sha256(args.ledger),'market':sha256(args.market)}}
    (out/'R118_500_CASE_AUDIT_METADATA.json').write_text(json.dumps(metadata,ensure_ascii=False,indent=2),encoding='utf8')
    lines=['# R118 500종목 BB20 하단 회복 검증 — RESEARCH ONLY','',
      '**주의:** 2023~2024년 재사용 기간의 탐색적 확대 연구입니다. OOS가 아닙니다. 공식 기업행위 검증이 되지 않았습니다.','',
      '- 2022-12-29 시총 선정 500종목 / 고정 200종목 / 신규 300종목 구분','- 시장 20일 수익률 -2% 미만 / 대조군 (-2% 이상) 비교',
      '- 동일 종목·패턴 10거래일 재진입 억제, 다음날 시가~D+5 종가, 왕복 비용 0.25%p','',
      '## 가격원장 직접 재검증한 성공 5건·실패 5건','',
      '|판정|종목코드|신호일|매수가(다음날 시가)|청산가(D+5 종가)|순손익|차트|',
      '|---|---|---|---:|---:|---:|---|']
    for e in cases:
        lines.append(f"|{e['outcome']}|{e['code']}|{e['signal_date']}|{e['entry_open']:,.0f}|{e['exit_close']:,.0f}|{e['net_pct']:+.2f}%|[{e['chart']}]({e['chart']})|")
    lines+=['','## 데이터 품질 경고','',
      '- 주가·거래대금은 KRX 유래 제3자 아카이브이며 공식 KRX 원본 인증이 아닙니다.',
      '- 이 감사는 R118 사건 원장의 진입/청산/PnL을 SHA 고정 원본 일봉으로 재산출합니다.',
      '- 주식분할/인적분할/거래정지 신호를 보수적으로 탐지하지만 공식 공시와 1:1 확인하지 않았습니다.',
      '- 경고가 없는 종목도 기업행위가 없었다는 확증은 아닙니다.',
      '- 판정: 500종목 성과가 좋아도 실전 사용 불가. OOS 및 공식 기업행위 검증 필요.']
    (out/'R118_500_CASE_REPORT_KO.md').write_text('\n'.join(lines)+'\n',encoding='utf8')
    print(json.dumps({'status':metadata['status'],'chart_count':len(cases),
                      'summary_rows':len(summary),'event_count':len(bb)},ensure_ascii=False),flush=True)
    return 0

if __name__=='__main__':sys.exit(main())
