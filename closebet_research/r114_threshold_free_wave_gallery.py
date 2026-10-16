"""R1.14, research-only. Ex-ante descriptive diagnostics and blind pre-D0 chart review.

Intentionally NO inferred wave truth, NO fitted trading thresholds, NO future bars.
Sources: pinned R1.13 + R1.12 + R1.7.1 + four hashed original daily KRX shards.
"""
from __future__ import annotations
import argparse
import hashlib
import html
import json
import math
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
from closebet_research.r111_episode_first_strength_audit import (
    R171_RAW_SHA, CHECK_COLS, build_archives, require, sha, unique_raw,
)
from closebet_research.r112_causal_wave_phase_audit import (
    load_historical_krx, causal_daily_states,
)
from closebet_research.r113_nonwave_root_cause import (
    EXPECTED_SOURCE_HASH, EXPECTED_STAGES, load_pinned_source,
)

EXPECTED_R113_RUN = 38826734127
EXPECTED_R112_RUN = 38823870672
EXPECTED_R171_RUN = 38664095610
R113_STATUS = 'PASS_FORENSIC_ONLY_NOT_VALIDATED_TRADING_RULES'
FIELDS = ('open','high','low','close','volume')
DETECTED = {'FIRST_WAVE_ACTIVE','PULLBACK_UNCONFIRMED','PULLBACK_QUALITY','REBOUND_CONFIRMED'}
CAUSES = ['RECENT_DETECTED_WAVE_RESET_OR_EXPIRED','NO_BELOW224_ANCHOR',
          'NO_16PCT_IMPULSE','NO_1_25_PRIOR_VOLUME','NO_FIVE_CLOSE_BREAKOUT']


def pinned(r112, r113, r171):
    a,m112 = load_pinned_source(r112)
    root=Path(r113);mf=root/'r113_integrity_manifest.json'
    require(mf.is_file(),'R113_MANIFEST_MISSING')
    m=json.loads(mf.read_text(encoding='utf-8'))
    require(m['status']==R113_STATUS,'R113_STATUS_MISMATCH')
    require(m['r112_run_id']==EXPECTED_R112_RUN and m['krx_source_run_id']==EXPECTED_R171_RUN,
            'R113_UPSTREAM_RUN_ID')
    require(m['source_stage_sha256']==EXPECTED_SOURCE_HASH,'R113_UPSTREAM_SHA')
    require(m['missing_wave_explained']==659 and m['native_all_ohlcv_bars_verified']==1059 and
            m['krx_all_compressed_daily_sha_checked']==1907,'R113_UPSTREAM_CONTRACT')
    for f,h in m['output_sha256'].items():
        require((root/f).is_file() and sha((root/f).read_bytes())==h,'R113_FILE_SHA:'+f)
    b=pd.read_csv(root/'r113_nonwave_659_root_causes.csv',dtype={'code':str})
    case=pd.read_csv(root/'r113_blind_review_casebook.csv',dtype={'code':str})
    require(len(b)==659 and not b[['signal_date','code']].duplicated().any(),'R113_659_INTEGRITY')
    require(len(case)==40 and case.blind_case_id.nunique()==40,'R113_CASEBOOK_COUNT')
    require(b.primary_explanation.isin(CAUSES).all(),'R113_UNKNOWN_CAUSE')
    expected=set(zip(a.loc[a.stage=='NO_CONFIRMED_WAVE','signal_date'],a.loc[a.stage=='NO_CONFIRMED_WAVE','code']))
    require(expected==set(zip(b.signal_date,b.code)),'R112_R113_SIGNAL_IDENTITY')
    require(set(zip(case.signal_date,case.code)).issubset(expected),'CASEBOOK_NOT_NO_WAVE')
    # Reuse exactly frozen R1.7.1 native selections; do not derive new TOP5 using future universe.
    raw_file=Path(r171)/'r17_krx/native/v49_76_global_selected_top5.csv'
    require(raw_file.is_file() and sha(raw_file.read_bytes())==R171_RAW_SHA,'R171_SOURCE_SHA')
    raw=pd.read_csv(raw_file,dtype={'code':str},low_memory=False)
    require(len(raw)==1133,'R171_SIGNAL_ROWS')
    raw.signal_date=pd.to_datetime(raw.signal_date,format='%Y-%m-%d')
    unique=unique_raw(raw)
    require(len(unique)==1059,'R171_UNIQUE_SIGNAL_COUNT')
    unique['signal_date']=unique.signal_date.dt.strftime('%Y-%m-%d')
    require(set(zip(a.signal_date,a.code))==set(zip(unique.signal_date,unique.code)),
            'R171_R112_SIGNAL_IDENTITY')
    return a,b,case,unique,m


def _ratio(a,b):
    x=float(np.mean(a));y=float(np.mean(b))
    return x/y if x>=0 and y>0 and np.isfinite(x) and np.isfinite(y) else np.nan


def asof_metrics(history, signal_day):
    """Signal-day inclusive history, full causal metrics; no labels or performance read."""
    times=history.index.astype(str).tolist()
    ix={dt:i for i,dt in enumerate(times)}
    require(signal_day in ix,'SIGNAL_HISTORY_ABSENT:'+signal_day)
    i=ix[signal_day]
    if i<270:
        return {'window_224_nontradeable_bar_count':np.nan,'descriptive_window_valid':0,
                **{k:np.nan for k in METRICS}}
    h=history.iloc[:i+1]
    vol=h.volume.to_numpy(dtype=float); close=h.close.to_numpy(dtype=float)
    low=h.low.to_numpy(dtype=float); high=h.high.to_numpy(dtype=float)
    invalid=((h[['open','high','low','volume']]==0).all(axis=1)&h.close.gt(0)).to_numpy()
    ma=h.close.rolling(224,min_periods=224).mean().to_numpy()
    # Strictly exclude altered/absent trading data in measurement windows.
    safe=not bool(np.any(invalid[max(0,i-223):i+1]))
    prefix={'window_224_nontradeable_bar_count':int(np.sum(invalid[max(0,i-223):i+1])),
            'descriptive_window_valid':int(safe)}
    if not safe:
        return {**prefix,**{k:np.nan for k in METRICS}}
    v5_prior=vol[i-5:i];prev20=vol[i-25:i-5]
    v5_inc=vol[i-4:i+1];prev20_inc=vol[i-24:i-4]
    v20=vol[i-19:i+1]; v20_prev=vol[i-39:i-19]
    prev5max=float(np.max(close[i-5:i]))
    def percent(a,b):return 100*(a/b-1) if b>0 else np.nan
    # This log-volume regression is a DESCRIPTION, not a gate or change to LIVE.
    v20_last=np.log1p(vol[i-19:i+1]);x=np.arange(20,dtype=float)
    log_slope_20=float(np.polyfit(x,v20_last,1)[0])
    record={
      'close_vs_ma224_pct':percent(close[i],ma[i]),
      'ma224_slope_20d_pct':percent(ma[i],ma[i-20]),
      'prior5_over_older20':_ratio(v5_prior,prev20),
      'inclusive5_over_older20':_ratio(v5_inc,prev20_inc),
      'recent20_over_previous20':_ratio(v20,v20_prev),
      'today_over_older20':float(vol[i]/np.mean(prev20)) if np.mean(prev20)>0 else np.nan,
      'log_volume_daily_slope20':log_slope_20,
      'close_over_prior5max_pct':percent(close[i],prev5max),
      'close_return_5d_pct':percent(close[i],close[i-5]),
      'close_return_20d_pct':percent(close[i],close[i-20]),
      'close_return_40d_pct':percent(close[i],close[i-40]),
      'close_over_low_10d_pct':percent(close[i],float(np.min(low[i-9:i+1]))),
      'close_over_low_20d_pct':percent(close[i],float(np.min(low[i-19:i+1]))),
      'close_over_low_40d_pct':percent(close[i],float(np.min(low[i-39:i+1]))),
      'close_below_high_20d_pct':percent(close[i],float(np.max(high[i-19:i+1]))),
      'close_below_high_40d_pct':percent(close[i],float(np.max(high[i-39:i+1]))),
      'low_40d_below_ma224_pct':percent(float(np.min(low[i-39:i+1])),ma[i]),
    }
    require(all(np.isfinite(v) for v in record.values()),'METRIC_NAN_UNEXPECTED')
    return {**prefix,**record}


METRICS=['close_vs_ma224_pct','ma224_slope_20d_pct','prior5_over_older20',
'inclusive5_over_older20','recent20_over_previous20','today_over_older20',
'log_volume_daily_slope20','close_over_prior5max_pct','close_return_5d_pct',
'close_return_20d_pct','close_return_40d_pct','close_over_low_10d_pct',
'close_over_low_20d_pct','close_over_low_40d_pct','close_below_high_20d_pct',
'close_below_high_40d_pct','low_40d_below_ma224_pct']


def summarize_metrics(x):
    groups=[]
    for metric in METRICS:
        for label,g in x.groupby('descriptive_group',sort=True):
            v=g[metric].dropna()
            groups.append({'metric':metric,'group':label,'n':len(v),'n_codes':g.code.nunique(),
                'p10':v.quantile(.1),'p25':v.quantile(.25),'median':v.median(),
                'p75':v.quantile(.75),'p90':v.quantile(.9)})
    return pd.DataFrame(groups)


def blind_chart(code,day,h,blind_id,target):
    # Charts stop strictly on D0, and are intentionally unlabeled with post-D0 performance.
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    i=h.index.get_loc(day)
    first=max(0,i-119)
    q=h.iloc[first:i+1]
    require(q.index[-1]==day,'CHART_FUTURE_LEAK')
    ma=h.close.rolling(224,min_periods=224).mean().iloc[first:i+1]
    x=np.arange(len(q))
    fig,(ax,va)=plt.subplots(2,1,figsize=(11.0,6.1),sharex=True,gridspec_kw={'height_ratios':[3,1]})
    ax.plot(x,q.close.values,label='KRX close',linewidth=1.6)
    ax.plot(x,ma.values,label='MA224',linewidth=1.25)
    ax.fill_between(x,q.low.values,q.high.values,alpha=.12,label='daily high-low')
    ax.axvline(len(q)-1,ls='--',linewidth=1,label='Signal D0 (chart ends here)')
    ax.set_title(f'{blind_id} | pre-signal KRX history only | D0 {day}',fontsize=11)
    ax.set_ylabel('KRW (unadjusted)');ax.legend(loc='upper left',fontsize=7,ncol=3)
    va.bar(x,q.volume.values,width=.9)
    va.set_ylabel('Volume')
    keep=np.linspace(0,len(q)-1,min(8,len(q)),dtype=int)
    va.set_xticks(keep);va.set_xticklabels([q.index[z][5:] for z in keep],rotation=25)
    fig.tight_layout();fig.savefig(target,dpi=120);plt.close(fig)


def run(r112,r113,r171,history,out,charts=40):
    require(0<=charts<=40,'MAX_CHARTS_40')
    source,no,case,native,m113=pinned(r112,r113,r171)
    zips,archive,cal=build_archives(history)
    codes=set(source.code)
    by_code,verified_days=load_historical_krx(archive,cal,codes)
    require(verified_days==1907,'KRX_1907_DAILY_HASH_COUNT')
    rawkey=native.set_index(['signal_date','code'])
    keyno=no.set_index(['signal_date','code'])
    rows=[];verified=0;recalculated=0
    for code,g in source.groupby('code',sort=True):
        h=by_code[code]
        stages=causal_daily_states(h).set_index('date')
        for r in g.itertuples(index=False):
            day=r.signal_date
            require(day in stages.index and day in h.index,'SIGNAL_DATE_NOT_PRESENT:'+code+':'+day)
            require(stages.loc[day,'stage']==r.stage,'R112_STATE_MISMATCH:'+code+':'+day)
            recalculated+=1
            raw=rawkey.loc[(day,code)];actual=h.loc[day]
            for rk,k in CHECK_COLS.items():
                require(float(raw[rk])==float(actual[k]),'NATIVE_KRX_PRICE_MISMATCH:'+code+':'+day+':'+k)
            require(float(actual.close)==float(r.entry_close),'R112_CLOSE_MISMATCH')
            verified+=1
            if r.stage in DETECTED:label='RECOGNIZED_WAVE_PROXY'
            elif r.stage=='NO_CONFIRMED_WAVE':label='NO_WAVE_PROXY'
            else:label='SOURCE_QUARANTINE'
            d={'signal_date':day,'code':code,'r112_stage':r.stage,'descriptive_group':label,
               'episode_first_c10':int(r.episode_first_c10)}
            d.update(asof_metrics(h,day))
            if label=='NO_WAVE_PROXY':
                z=keyno.loc[(day,code)]
                d['r113_primary_explanation']=z.primary_explanation
                d['r113_inferred_reset']=z.inferred_reset_event
                d['r113_signal_gain_pct']=float(z.signal_gain_pct)
                d['r113_signal_volume_ratio']=float(z.signal_previous5_to_older20_volume)
                if d['descriptive_window_valid']:
                    require(np.isclose(d['prior5_over_older20'],d['r113_signal_volume_ratio'],rtol=1e-10,atol=1e-10),
                            'R113_VOLUME_RATIO_DISAGREEMENT:'+code+':'+day)
            rows.append(d)
    for z in zips:z.close()
    df=pd.DataFrame(rows).sort_values(['signal_date','code']).reset_index(drop=True)
    require(len(df)==1059 and verified==1059 and recalculated==1059,'RESEARCH_FULL_COVERAGE')
    require(df.descriptive_group.value_counts().to_dict()=={
        'NO_WAVE_PROXY':659,'RECOGNIZED_WAVE_PROXY':357,'SOURCE_QUARANTINE':43},'STUDY_GROUP_COUNTS')
    require((df.loc[df.descriptive_group!='SOURCE_QUARANTINE','descriptive_window_valid']==1).all(),
            'NO_WAVE_HAS_ZERO_OHLCV_WINDOW')
    require(df[df.descriptive_group=='NO_WAVE_PROXY'].r113_primary_explanation.value_counts().to_dict()==m113['cause_distribution'] if 'GATES_ELIGIBLE_BUT_STATE_NONE_INSPECT' not in m113['cause_distribution'] else
            df[df.descriptive_group=='NO_WAVE_PROXY'].r113_primary_explanation.value_counts().to_dict()=={k:v for k,v in m113['cause_distribution'].items() if v},
            'R113_CAUSE_RECOMPUTE')
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    outputs={}
    def csv(name,t):t.to_csv(out/name,index=False,encoding='utf-8-sig');outputs[name]=sha((out/name).read_bytes())
    csv('r114_1059_exante_descriptive_metrics.csv',df)
    csv('r114_metric_quantiles_by_proxy.csv',summarize_metrics(df))
    csv('r114_metric_quantiles_by_rootcause.csv',summarize_metrics(df[df.descriptive_group=='NO_WAVE_PROXY'].assign(
            descriptive_group=lambda x:x.r113_primary_explanation)))
    boolkeys=['signal_origin_below_ma224','signal_gain_ge_16pct','signal_prior5_vol_ge_1_25','signal_close_5bar_high']
    # Multi-label audit: totals overlap; not R1.13's mutually exclusive priority causes.
    overlaps=[]
    for f in boolkeys:
        val=int(no[f].eq(0).sum())
        overlaps.append({'gate':f,'all_659_failing':val,'all_659_passing':int(no[f].eq(1).sum()),
                         'denominator':659,'simultaneous_failure_possible':True})
    csv('r114_all_659_multigate_overlap.csv',pd.DataFrame(overlaps))
    # Mode stratification: distinguish different strategies' purposes, never claim a global threshold.
    modes=pd.read_csv(Path(r113)/'r113_source_mode_vs_wave_proxy.csv')
    csv('r114_original_strategy_mode_mix.csv',modes)
    caseout=case[['blind_case_id','signal_date','code','primary_explanation','source_modes','inferred_reset_event']].copy()
    caseout['image_file']=''
    dest=out/'charts';dest.mkdir(exist_ok=True)
    for idx,r in enumerate(case.itertuples(index=False)):
        if idx>=charts:break
        im=f'{r.blind_case_id}.png';blind_chart(r.code,r.signal_date,by_code[r.code],r.blind_case_id,dest/im)
        caseout.loc[caseout.blind_case_id==r.blind_case_id,'image_file']='charts/'+im
    csv('r114_blind_gallery_crosswalk.csv',caseout)
    labels=pd.DataFrame({'blind_case_id':caseout.blind_case_id,'reviewer_asof_wave_label':'',
                         'reviewer_confidence':'','reviewer_notes_no_future':'',
                         'allowed_labels_hint':'EARLY_VOLUME_CREEP_FIRST_WAVE_PULLBACK | OTHER_WAVE | NO_VISIBLE_FIRST_WAVE | DATA_PROBLEM_SUSPECT | UNCERTAIN'})
    csv('r114_manual_label_blank_template.csv',labels)
    imgs=[]
    for r in caseout.itertuples(index=False):
        if r.image_file:
            imgs.append(f'<article><h2>{html.escape(r.blind_case_id)}</h2><img src="{html.escape(r.image_file)}" alt="Pre-D0 KRX prices and volume" loading="lazy" /></article>')
    gallery='''<!doctype html><html lang="ko"><meta charset="utf-8"><title>R1.14 blind pre-D0 cases</title><style>body{font:16px Arial,sans-serif;max-width:1060px;margin:25px auto;background:#f7f8fa;color:#17212b}article{background:white;margin:20px 0;padding:15px;border:1px solid #dadfe5;border-radius:8px}img{width:100%}small{color:#546}</style><h1>R1.14: 미래성과를 가린 신호일 이전 차트</h1><p><strong>관찰일 종가까지만</strong> 포함. 파동의 정답 라벨도, D+1 이후 가격도 표시하지 않습니다. 각 차트에는 224일선/거래량을 표시했습니다.</p><p><small>번호-종목코드/원인 연결표는 별도 CSV. R1.12 상태와 R1.13 원인은 검증 대상 프록시입니다. 독립적인 수작업 판정 결과가 아닙니다.</small></p>'''+''.join(imgs)+'</html>'
    (out/'R114_BLIND_PRE_D0_GALLERY.html').write_text(gallery,encoding='utf-8');outputs['R114_BLIND_PRE_D0_GALLERY.html']=sha((out/'R114_BLIND_PRE_D0_GALLERY.html').read_bytes())
    status='PASS_DESCRIPTIVE_AND_BLIND_GALLERY_ONLY_NOT_TRADE_APPROVED'
    manifest={'status':status,'pinned_run_ids':{'r113':EXPECTED_R113_RUN,'r112':EXPECTED_R112_RUN,'r171':EXPECTED_R171_RUN},
      'krx_dated_sha_verified':1907,'r112_recomputed_signal_stages':recalculated,
      'native_full_ohlcv_bars_verified':verified,'total_unique_signals':1059,'no_wave_proxy':659,
      'recognized_wave_proxy':357,'quarantine':43,'blind_cases_prechosen_in_r113':len(case),
      'charts_generated':int(charts),'r113_cause_total':m113['cause_distribution'],
      'limitations':{'independent_KRX_vendor_crosscheck':'NOT_DONE', 'true_first_wave_ground_truth':'NOT_LABELED',
       'stage_labels_are_r112_proxy':'YES','past_universe_selection':'FROZEN_907_NOT_PIT',
       'multi_signal_independence':'NOT_ASSUMED','forward_returns_used_for_selection':'NO',
       'pricing_and_lookahead':'KRX UNADJUSTED OBSERVATION DAY ONLY',
       'trade_permission':'NONE'},'output_sha256':outputs}
    (out/'r114_integrity_manifest.json').write_text(json.dumps(manifest,indent=2,ensure_ascii=False),encoding='utf-8')
    print('R114_PASS',json.dumps({'count':1059,'no_wave':659,'detected':357,'quarantine':43,
                              'charts':charts,'krx_sha_days':verified_days},ensure_ascii=False),flush=True)
    return manifest


def main():
    ap=argparse.ArgumentParser()
    for v in ('r112-dir','r113-dir','r171-dir','history-artifacts','out'):
        ap.add_argument('--'+v,required=True)
    ap.add_argument('--charts',type=int,default=40)
    args=ap.parse_args()
    run(args.r112_dir,args.r113_dir,args.r171_dir,args.history_artifacts,args.out,args.charts)
if __name__=='__main__':main()
