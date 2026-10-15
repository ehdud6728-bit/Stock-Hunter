#!/usr/bin/env python3
"""R1.9: fail-closed source audit + historical paired-case chart research, NOT trading.

Sources: pinned R171 (KRX daily snapshot replay) and R18 forensics artifacts.
All pairing/matching uses ex-ante signal features, but contrast-class selection
necessarily uses future outcomes. Therefore results are descriptive, NOT predictive/OOS.
No LIVE scanner modifications, no data network calls, and no trading instructions.
"""
from __future__ import annotations
import argparse, hashlib, io, json, math, zipfile
from pathlib import Path
import numpy as np
import pandas as pd

from closebet_research.r18_wave_entry_forensic import load_history, load_selected, make_prewave

SOURCE_R171 = '38664095610'
SOURCE_R18 = '38693918216'
ASOF = '2026-10-12'
COHORT_COUNTS = {'STRONG_PERSISTENT':145,'OTHER_PROFIT':91,'GIVEBACK':153,'NO_5PCT_RISE':181}
BARS = ['open','high','low','close','volume','amount']
PINNED_CONTENT_SHA256 = {
 'r17_krx/native/v49_76_global_selected_top5.csv':'4c28007a550007ad8a9c64fdaffab74c90faeb786194e291f9f7d9284e104280',
 'r18_all_unique_signal_features.csv':'801f3eff3b51437a0addcd7e270dd6fc5fe6feb3329a5eacce2fec3c949ad179',
 'r18_research_manifest.json':'3824d104fd6dd9ffe70d91a9dfae0aa44e52c5ecdd9702256520a4952cb261f4',
}
FEATURES = ['ma20_signal_dist_pct','ma224_dist_signal_pct','volume_dryup_10_30',
            'prewave_impulse_pct','prewave_pullback_pct','prewave_pullback_vol_ratio',
            'prewave_prevolume_accum_ratio','candle_close_location','wave_setup_stage']


def require(test, message):
    if not bool(test):raise RuntimeError('R19_FAIL_CLOSED:'+message)


def read_one_zip(zpath, file):
    src=Path(zpath)
    if src.is_dir():
        f=src/file
        require(f.is_file(), 'SOURCE_ARTIFACT_FILE_MISSING:'+str(f))
        raw=f.read_bytes()
    else:
        with zipfile.ZipFile(src) as z:
            names=z.namelist()
            require(file in names,'SOURCE_ARTIFACT_FILE_MISSING:'+file)
            raw=z.read(file)
    require(hashlib.sha256(raw).hexdigest()==PINNED_CONTENT_SHA256[file],
            'PINNED_ARTIFACT_SHA256_MISMATCH:'+file)
    return raw


def sha256(f):
    p=Path(f)
    return hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else 'DIR_PINNED_CONTENT_ONLY'


def verify_source(selected_zip, r18_zip, all_unique, r18, history, calendar, history_meta):
    r18manifest=json.loads(read_one_zip(r18_zip,'r18_research_manifest.json'))
    require(r18manifest['status']=='RESEARCH_ONLY_NOT_TRADE_APPROVED','R18_STATUS_CHANGED')
    require(r18manifest['source_run_id']==SOURCE_R171,'R18_UPSTREAM_RUN_ID_MISMATCH')
    require(r18manifest['source_all_unique_signal_date_codes']==1059,'R18_ORIGINAL_KEY_COUNT_CHANGED')
    require(r18manifest['unique_signal_date_codes']==754,'R18_RESEARCH_KEY_COUNT_CHANGED')
    require(r18manifest['D20_mature_unique']==570,'R18_MATURE_N_CHANGED')
    require(r18manifest['D5_recent_unique']==184,'R18_RECENT_D5_N_CHANGED')
    require(r18manifest['outcome_distribution']==COHORT_COUNTS,'R18_OUTCOME_CLASS_CHANGED')
    require(r18manifest['history_provenance']['official_calendar_sha256']==history_meta['official_calendar_sha256'], 'R18_HISTORY_CALENDAR_CHANGED')
    require(r18manifest['risk_flags'].get('KRX_1530_session_exclusive')=='NOT_PROVEN','R18_SEMANTICS_UNEXPECTED')
    require(len(all_unique)==1059 and len(r18)==754,'SOURCE_ROWS_UNEXPECTED')
    require(not all_unique[['signal_date','code']].duplicated().any(),'SOURCE_DUPLICATE_KEYS')
    require(not r18[['signal_date','code']].duplicated().any(),'R18_DUPLICATE_KEYS')
    require(set(map(tuple,r18[['signal_date','code']].to_numpy())) <= set(map(tuple,all_unique[['signal_date','code']].to_numpy())), 'R18_KEY_NOT_IN_NATIVE')
    require(calendar.max()==pd.Timestamp(ASOF),'CALENDAR_END_MISMATCH')
    require(pd.to_datetime(all_unique.signal_date).max()<=pd.Timestamp(ASOF),'SOURCE_FUTURE_SIGNAL')
    require(r18.groupby('outcome_class').size().to_dict().get('CENSORED_D20',0)==184,'R18_CENSORED_COHORT_CHANGED')
    mature=r18[r18.D20_complete.eq(1)]
    require(mature.groupby('outcome_class').size().to_dict()==COHORT_COUNTS,'R18_MATURE_CLASS_BAD')
    require(r18.D20_complete.sum()==570,'R18_D20_MATURE_BAD')
    require(r18[(r18.signal_date>='2026-09-21') & (r18.D5_complete.eq(1))].shape[0]==184,'RECENT_D5_BAD')
    # Re-verify every KRX price bar used by the ORIGINAL selected population, including
    # 305 omitted from R18 summary. Refuse altered/upstream-missing bars.
    price_checked=0
    for _,row in all_unique.iterrows():
        sig=pd.Timestamp(row['signal_date']);code=row['code'];d=history.get(code)
        require(d is not None and sig in d.index,'SIGNAL_HISTORY_MISSING:'+code+':'+str(sig.date()))
        bar=d.loc[sig]
        require(not bar[BARS].isna().any(),'SIGNAL_BAR_NAN:'+code)
        require(bar.high>=max(bar.open,bar.close) and bar.low<=min(bar.open,bar.close) and bar.low>0,'SIGNAL_OHLC_INVALID:'+code)
        require(bar.volume>=0 and bar.amount>=0,'SIGNAL_VOLUME_AMOUNT_INVALID:'+code)
        for c,v in [('open',row['_open']),('high',row['_high']),('low',row['_low']),('close',row['_close']),('volume',row['_vol']),('close',row['entry_price'])]:
            require(float(bar[c])==float(v), f"SIGNAL_PRICE_DISAGREEMENT:{code}:{row['signal_date']}:{c}")
        price_checked+=1
    # Recompute key ex-ante features and D20/D5 outcomes independently from raw KRX snapshots.
    features_checked=0; outcome_checked=0; horizon_checked=0; absolute_tol=1e-8
    for row in r18.itertuples(index=False):
        sig=pd.Timestamp(row.signal_date);d=history[row.code]
        pos=d.index.get_loc(sig)
        require(isinstance(pos,(int,np.integer)),'HISTORY_INDEX_DUPLICATE')
        features=make_prewave(d,pos)
        for c,a in features.items():
            b=getattr(row,c)
            if pd.isna(a) and pd.isna(b):continue
            if isinstance(a,str):ok=a==b
            elif isinstance(a,(bool,np.bool_)):ok=bool(a)==bool(b)
            else:ok=pd.notna(a) and pd.notna(b) and np.isclose(float(a),float(b),rtol=1e-9,atol=absolute_tol)
            require(ok,'FEATURE_RECALC_DISAGREEMENT:'+row.code+':'+c)
        features_checked+=1
        for k in (1,3,5,10,15,20):
            f=d.iloc[pos+1:pos+k+1]
            full=len(f)==k and not f[BARS].isna().any().any()
            is_marked_complete=bool(getattr(row,f'D{k}_complete'))
            require(full==is_marked_complete,f'D{k}_COMPLETENESS_RECALC_BAD:'+row.code)
            if not full:continue
            entry=float(row.source_entry_price)
            close_pct=(float(f.close.iloc[-1])/entry-1)*100
            high_pct=(float(f.high.max())/entry-1)*100
            low_pct=(float(f.low.min())/entry-1)*100
            for typ,x in [('close_pct',close_pct),('max_high_pct',high_pct),('min_low_pct',low_pct)]:
                require(np.isclose(x,getattr(row,f'D{k}_{typ}'),rtol=0,atol=absolute_tol),f'D{k}_{typ}_RECALC_BAD:'+row.code)
            first_plus5=np.flatnonzero(f.high.to_numpy(dtype=float)>=entry*1.05)
            first_val=int(first_plus5[0]+1) if len(first_plus5) else np.nan
            orig_first=getattr(row,f'D{k}_first_plus5')
            require((pd.isna(first_val) and pd.isna(orig_first)) or
                    (pd.notna(first_val) and pd.notna(orig_first) and int(first_val)==int(orig_first)),
                    f'D{k}_FIRST_PLUS5_RECALC_BAD:'+row.code)
            horizon_checked+=1
            if k==20:
                c=('STRONG_PERSISTENT' if high_pct>=10 and close_pct>=5 else 'OTHER_PROFIT' if close_pct>0
                   else 'GIVEBACK' if high_pct>=5 else 'NO_5PCT_RISE')
                require(c==row.outcome_class,'D20_CLASS_RECALC_BAD:'+row.code)
                outcome_checked+=1
    require(outcome_checked==570,'D20_COUNT_RECALC_INVALID')
    return {'status':'PASS_WITH_KNOWN_LIMITATIONS','source_runs':{'r171':SOURCE_R171,'r18':SOURCE_R18},
            'source_zip_sha256':{'r171_merge':sha256(selected_zip),'r18_forensic':sha256(r18_zip)},
            'all_native_unique_signal_price_bars_verified':price_checked,'R18_feature_rows_recalculated':features_checked,
            'D20_outcomes_independently_recalculated':outcome_checked,'horizon_bars_recalculated_D1_D20':horizon_checked,'historical_calendar_sha256':history_meta['official_calendar_sha256'],
            'data_issues_fail_closed':True,
            'NOT_PROVEN':['historical_universe_PIT','index_membership_PIT','regular_session_15_30_only','historical_corporate_actions','actual_fills','OOS_D20']}


def _cost(a,b,calendar_loc):
    # Variables for matching are available by *signal day*. Outcome labels are never features.
    ds=abs(calendar_loc[a.signal_date]-calendar_loc[b.signal_date])
    if ds>12:return None
    modes_a=set(a.all_modes.split('|'));modes_b=set(b.all_modes.split('|'))
    if not (modes_a & modes_b):return None
    if a.code==b.code:return None
    # neutral fixed distance metric with missing-feature penalty, no fitted or tuned coefficients
    score=ds/12.0
    for f,scale in [('ma20_signal_dist_pct',15),('ma224_dist_signal_pct',35),
                    ('prewave_impulse_pct',70),('prewave_pullback_pct',30),
                    ('volume_dryup_10_30',1.0)]:
        x=getattr(a,f);y=getattr(b,f)
        if pd.isna(x) or pd.isna(y):score+=1
        else:score+=min(2.0,abs(float(x)-float(y))/scale)
    score+=abs(int(a.wave_setup_stage)-int(b.wave_setup_stage))*.4
    return float(score)


def build_pairs(df,calendar,comparison=('STRONG_PERSISTENT','GIVEBACK'),max_pairs=16):
    left=df[df.outcome_class.eq(comparison[0])].copy()
    right=df[df.outcome_class.eq(comparison[1])].copy()
    loc={x.strftime('%Y-%m-%d'):i for i,x in enumerate(calendar)}
    right=right.sort_values(['signal_date','code'])
    left=left.sort_values(['signal_date','code'])
    candidate=[]
    for a in left.itertuples(index=False):
        for b in right.itertuples(index=False):
            z=_cost(a,b,loc)
            if z is not None:candidate.append((z,a.signal_date,a.code,b.signal_date,b.code))
    candidate.sort()
    out=[];used_codes=set();used_left=set();used_right=set()
    li={(r.signal_date,r.code):r for r in left.itertuples(index=False)}
    ri={(r.signal_date,r.code):r for r in right.itertuples(index=False)}
    for cost,ad,ac,bd,bc in candidate:
        if (ad,ac) in used_left or (bd,bc) in used_right or ac in used_codes or bc in used_codes:continue
        a=li[(ad,ac)];b=ri[(bd,bc)]
        used_codes.update([ac,bc]);used_left.add((ad,ac));used_right.add((bd,bc))
        d={'pair_id':f'P{len(out)+1:02d}','pair_type':comparison[0]+'__VS__'+comparison[1],
           'pair_cost_fixed':round(cost,4),'signal_session_gap':abs(loc[ad]-loc[bd]),
           'common_modes':'|'.join(sorted(set(a.all_modes.split('|'))&set(b.all_modes.split('|'))))}
        for prefix,obj in [('left',a),('right',b)]:
            d.update({f'{prefix}_code':obj.code,f'{prefix}_signal_date':obj.signal_date,
                      f'{prefix}_source_name_UNVERIFIED':obj.name,f'{prefix}_class':obj.outcome_class,
                      f'{prefix}_modes':obj.all_modes,f'{prefix}_D20_close_pct':obj.D20_close_pct,
                      f'{prefix}_D20_mfe_pct':obj.D20_max_high_pct,f'{prefix}_D20_mae_pct':obj.D20_min_low_pct,
                      f'{prefix}_stage':obj.wave_setup_stage})
            for feature in FEATURES:
                d[f'{prefix}_{feature}']=getattr(obj,feature)
        out.append(d)
        if len(out)>=max_pairs:break
    return pd.DataFrame(out)


def aggregate_outcomes(df):
    # Descriptive only. Outcomes are used to construct each class; do not present
    # class-separating outcome features as predictive factors.
    out=[]
    for lab,g in df.groupby('outcome_class'):
        x={'class':lab,'n':len(g),'distinct_codes':g.code.nunique(),'distinct_signal_days':g.signal_date.nunique(),
           'D20_close_avg_pct':g.D20_close_pct.mean(), 'D20_mfe_median_pct':g.D20_max_high_pct.median(),
           'D20_mae_median_pct':g.D20_min_low_pct.median()}
        for f in FEATURES:
            x[f+'_median']=pd.to_numeric(g[f],errors='coerce').median()
            x[f+'_valid_n']=int(pd.to_numeric(g[f],errors='coerce').notna().sum())
        out.append(x)
    return pd.DataFrame(out)


def chart_signal(g,raw,calendar,outdir,lab):
    # Price/volume are independently plotted with signal cutoff, so nobody can
    # mistake outcome bars for features available when signal was generated.
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    sig=pd.Timestamp(g.signal_date);loc=calendar.get_loc(sig)
    q=raw.iloc[max(0,loc-90):min(len(raw),loc+21)].copy()
    require(not q.loc[:sig,'close'].isna().any(),'CHART_BAD_HISTORICAL_PRICES:'+g.code)
    q['ma20']=raw.close.rolling(20,min_periods=20).mean().reindex(q.index)
    q['ma224']=raw.close.rolling(224,min_periods=224).mean().reindex(q.index)
    # Hide unknown stock name: known mismatches in metadata, use code/date only.
    name=f"{lab}_{g.code}_{g.signal_date}".replace('-','')
    for typ in ('price','volume'):
        fig,ax=plt.subplots(figsize=(10.5,4.3))
        if typ=='price':
            ax.plot(q.index,q['close'],label='KRX close')
            ax.plot(q.index,q['ma20'],label='MA20')
            ax.plot(q.index,q['ma224'],label='MA224')
            ax.axhline(g.source_entry_price,ls=':',lw=1,label='signal close')
            ax.set_ylabel('Raw KRX KRW (unadjusted)')
        else:
            ax.bar(q.index,q.volume,width=1.0,label='KRX daily volume')
            ax.plot(q.index,raw.volume.rolling(20,min_periods=20).mean().reindex(q.index),label='20-session avg volume')
            ax.set_ylabel('KRX volume')
        ax.axvline(sig,ls='--',lw=1.4,label='Signal / information cutoff')
        ax.axvspan(sig, min(q.index[-1],calendar[-1]),alpha=.10,label='After signal: outcomes only')
        ax.set_title(f'{lab}: {g.code} | signal {g.signal_date} | {typ.upper()} (NOT trade signal)')
        ax.legend(fontsize=8,loc='best');ax.grid(alpha=.2)
        fig.autofmt_xdate();fig.tight_layout()
        dest=outdir/(name+'_'+typ+'.png');fig.savefig(dest,dpi=115);plt.close(fig)
    return name


def main(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument('--r171-zip',required=True)
    p.add_argument('--r18-zip',required=True)
    p.add_argument('--history-artifacts',required=True)
    p.add_argument('--out',required=True)
    p.add_argument('--max-chart-pairs',type=int,default=6)
    args=p.parse_args(argv)
    require(1<=args.max_chart_pairs<=12,'BAD_CHART_LIMIT')
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    data=read_one_zip(args.r18_zip,'r18_all_unique_signal_features.csv')
    r18=pd.read_csv(io.BytesIO(data),dtype={'code':str},low_memory=False)
    r18['code']=r18.code.str.zfill(6)
    # Verify original signal source content SHA before parser sees any data.
    _=read_one_zip(args.r171_zip,'r17_krx/native/v49_76_global_selected_top5.csv')
    native_path=Path(args.r171_zip)
    if native_path.is_dir():native_path=native_path/'r17_krx/native/v49_76_global_selected_top5.csv'
    original,unique=load_selected(native_path)
    require(len(original)==1133,'UPSTREAM_STRATEGY_ROWS_CHANGED')
    codes=set(unique.code);hist,calendar,history_meta=load_history(args.history_artifacts,codes,'2025-01-02',ASOF)
    calendar=calendar[(calendar>=pd.Timestamp('2025-01-02'))&(calendar<=pd.Timestamp(ASOF))]
    hist={c:z.reindex(calendar) for c,z in hist.items()}
    manifest=verify_source(args.r171_zip,args.r18_zip,unique,r18,hist,calendar,history_meta)
    mature=r18[r18.D20_complete.eq(1)].copy()
    tab=aggregate_outcomes(mature);tab.to_csv(out/'r19_outcome_class_feature_profile.csv',index=False,encoding='utf-8-sig')
    # A >35% single-day price jump can signal corporate-action discontinuity;
    # without an independent corporate-action event source exclude these from
    # *matched chart interpretation*, while retaining in raw censuses.
    suspect=mature[mature.prior_224_jump_above35pct.eq(True)]
    clean=mature[~mature.prior_224_jump_above35pct.eq(True)]
    contrasts=[('STRONG_PERSISTENT','GIVEBACK','SG'),
               ('STRONG_PERSISTENT','OTHER_PROFIT','SW'),
               ('OTHER_PROFIT','NO_5PCT_RISE','WF')]
    pools=[]
    for leftclass,rightclass,label in contrasts:
        p=build_pairs(clean,calendar,(leftclass,rightclass),max_pairs=12)
        require(len(p)>=4,'INSUFFICIENT_MATCHES_FOR:'+label)
        p['pair_id']=p.pair_id.map(lambda x:label+'_'+x)
        pools.append(p)
    pairing=pd.concat(pools,ignore_index=True)
    require(len(pairing)>=18,'INSUFFICIENT_MATCHED_CONTRAST_PAIRS')
    pairing.to_csv(out/'r19_matched_contrast_pairs.csv',index=False,encoding='utf-8-sig')
    # Same-stock signals on different days are correlated; publish clustering evidence.
    clusters=r18.groupby('code').agg(n_signal_days=('signal_date','size'),
                                     first_signal=('signal_date','min'),last_signal=('signal_date','max'),
                                     n_D20_mature=('D20_complete','sum')).reset_index().sort_values('n_signal_days',ascending=False)
    clusters.to_csv(out/'r19_repeated_code_signal_clustering.csv',index=False,encoding='utf-8-sig')
    # Recent period has only D5 maturity. Never compare recent D5 against old D20.
    recent=r18[(r18.signal_date>='2026-09-21') & r18.D5_complete.eq(1)]
    shift=[]
    for cohort,g in [('OLDER_D20_COMPLETE_EVALUATED_AT_D5',mature),('RECENT_D5_ONLY',recent)]:
        q={'cohort':cohort,'signals':len(g),'unique_codes':g.code.nunique(),
           'signal_days':g.signal_date.nunique(),'D5_close_win_pct':float((g.D5_close_pct>0).mean()*100),
           'D5_mean_close_pct':g.D5_close_pct.mean(),
           'D5_high_plus5_touch_pct':float((g.D5_max_high_pct>=5).mean()*100)}
        for feat in FEATURES:q[feat+'_median']=pd.to_numeric(g[feat],errors='coerce').median()
        shift.append(q)
    pd.DataFrame(shift).to_csv(out/'r19_old_recent_D5_feature_drift.csv',index=False,encoding='utf-8-sig')

    # Paired differences are descriptive not selection instructions.
    pre=[]
    for pairing_type,g in pairing.groupby('pair_type'):
        for f in FEATURES:
            lv=pd.to_numeric(g['left_'+f],errors='coerce');rv=pd.to_numeric(g['right_'+f],errors='coerce')
            dif=lv-rv
            pre.append({'pair_type':pairing_type,'feature':f,'valid_pairs':int(dif.notna().sum()),
                        'left_minus_right_median':dif.median(),
                        'left_median':lv.median(),'right_median':rv.median(),
                        'status':'DESCRIPTIVE_NOT_CAUSAL_OR_OOS'})
    pd.DataFrame(pre).to_csv(out/'r19_matched_presignal_feature_differences.csv',index=False,encoding='utf-8-sig')
    # Independent all-date schema/provenance report. No auto-correction.
    suspect[['signal_date','code','outcome_class','prior_224_jump_above35pct']].to_csv(out/'r19_corporate_action_suspect_exclusions.csv',index=False,encoding='utf-8-sig')
    case_dir=out/'charts';case_dir.mkdir(exist_ok=True)
    for stale in case_dir.glob('*.png'):stale.unlink()
    idx=[]
    source_lookup={(r.signal_date,r.code):r for r in mature.itertuples(index=False)}
    # Fair representation: two or more contrasted kinds, not only strong/giveback.
    selected_chart_pairs=[]
    for _,g in pairing.groupby('pair_type',sort=True):
        selected_chart_pairs.extend(list(g.head(max(1,args.max_chart_pairs//3)).itertuples(index=False)))
    for row in selected_chart_pairs[:args.max_chart_pairs]:
        for side in ('left','right'):
            code=getattr(row,side+'_code');date=getattr(row,side+'_signal_date');v=source_lookup[(date,code)]
            chart_name=chart_signal(v,hist[code],calendar,case_dir,row.pair_id+'_'+side)
            idx.append({'pair_id':row.pair_id,'side':side,'code':code,'signal_date':date,
                        'outcome_class':v.outcome_class,'chart_prefix':chart_name,
                        'verified_price_series':True,'name_metadata_trusted':False})
    pd.DataFrame(idx).to_csv(out/'r19_chart_index.csv',index=False,encoding='utf-8-sig')
    require(len(list(case_dir.glob('*.png')))==len(idx)*2,'CHART_FILE_COUNT_MISMATCH')
    for x in idx:
        for chart_type in ('price','volume'):
            require((case_dir/(x['chart_prefix']+'_'+chart_type+'.png')).stat().st_size>10000,'CHART_EMPTY')
    # Offline gallery: no network requests, no suspect stock-name labels, no hindsight trade advice.
    from html import escape
    blocks=[]
    for x in idx:
        file_price=x['chart_prefix']+'_price.png';file_vol=x['chart_prefix']+'_volume.png'
        blocks.append('<section><h2>'+escape(x['pair_id'])+' · '+escape(x['side'])+' · code '+escape(x['code'])+
                      ' · '+escape(x['outcome_class'])+'</h2><p>Signal '+escape(x['signal_date'])+
                      ' — dashed vertical line is end-of-day signal; shading is future outcomes only.</p>'+ 
                      '<img src="charts/'+escape(file_price)+'" loading="lazy"/>'+ 
                      '<img src="charts/'+escape(file_vol)+'" loading="lazy"/></section>')
    gallery="""<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<style>body{max-width:1100px;margin:2em auto;padding:0 12px;font-family:sans-serif;background:#f8fafc;color:#172033}section{background:white;border:1px solid #e5e8ef;padding:12px 18px;margin:22px 0;border-radius:12px}img{width:100%;height:auto;display:block}p{line-height:1.55}.risk{border-left:4px solid #bf5d00;padding:12px;background:#fff2d9}</style>
</head><body><h1>R1.9 · KRX matched-case chart gallery (DESCRIPTIVE ONLY)</h1><p class="risk">Code + date identify a case. Source company NAME METADATA UNVERIFIED. Entire post-signal area is observed future data and MUST NOT be used to construct an entry rule. KRX 15:30-only session and historical PIT universe are NOT PROVEN. Corporate-action jumps >35% are excluded from paired comparisons, not independently resolved.</p>
"""+'\n'.join(blocks)+"</body></html>"
    (out/'R19_CHART_GALLERY.html').write_text(gallery,encoding='utf-8')
    manifest.update({'r19_status':'DESCRIPTIVE_RESEARCH_NOT_OOS','r19_paired_cases':len(pairing),
       'charted_cases':len(idx),'corp_action_suspect_D20_signals':len(suspect),
       'matched_case_no_reuse':True, 'strict_scope':'R18_SELECTED_KRX_ONLY_570_MATURE_184_RECENT',
       'pairing_rules':'same mode intersection, <=12 exchange days, distinct codes, greedy fixed presignal distance',
       'source_metadata_names':'UNVERIFIED_DO_NOT_USE_AS_IDENTITY',
       'interpretation':'Outcome-defined labels and chart matching are descriptive, not proof of prospective classification.',
       'next_gate':'PIT_UNIVERSE + 15:30 price source + independent forward/OOS tests before policy selection.'})
    # Fixed file checksums help downstream reviewers detect post-export corruption.
    manifest['output_sha256']={str(p.relative_to(out)):sha256(p) for p in sorted(out.rglob('*')) if p.is_file() and p.name!='r19_integrity_manifest.json'}
    (out/'r19_integrity_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    print('R19_INTEGRITY_PASS',json.dumps({k:manifest[k] for k in ('all_native_unique_signal_price_bars_verified','R18_feature_rows_recalculated','D20_outcomes_independently_recalculated','r19_paired_cases','charted_cases','corp_action_suspect_D20_signals')},ensure_ascii=False),flush=True)
    return manifest

if __name__=='__main__':main()
