#!/usr/bin/env python3
"""R1-D71 source-contract audit of original frozen R1D6/R1D19/R1D7 historical cohort.
Never call historical close re-scaling a verified adjustment. All D20 returns are
old source labels, not re-traded, CA-certified or prospective out-of-sample.
"""
from pathlib import Path
from zipfile import ZipFile
from io import BytesIO
import hashlib,json,numpy as np,pandas as pd
D6='R1D6_ASOF_ONLY_FROZEN_FEATURES_NO_OUTCOME.csv.gz'
D19='R1D19_ASOF_ONLY_BB_RECLAIM_NO_OUTCOME.csv.gz'
D7='R1D7_D5_D10_D15_D20_FROZEN_FORWARD_PRICE_PATHS.csv.gz'

def rd(zip_path,part):
    with ZipFile(zip_path) as z:
        matches=[n for n in z.namelist() if part in n and n.endswith('.csv.gz')]
        assert len(matches)==1,(part,matches)
        return pd.read_csv(BytesIO(z.read(matches[0])),compression='gzip',dtype={'code':str})

def near_fraction(close,band,tolerance):
    return pd.Series(np.isfinite(band)&(band>0)&((close/band-1).abs()<=tolerance),index=close.index)

def run(src6,src19,src7,out):
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    a=rd(src6,D6);b=rd(src19,D19);c=rd(src7,D7)
    assert len(a)==len(b)==5247 and len(c)==20988
    key=['date','code']
    assert not a[key].duplicated().any() and not b[key].duplicated().any() and not c[key+['horizon']].duplicated().any()
    x=b.merge(a,on=key,validate='one_to_one',suffixes=('_r1d19','_r1d6'))
    assert len(x)==5247 and x.asof_status.eq('ASOF_OK').all() and x.status_feature.eq('ASOF_OK').all()
    close=x['signal_close'].astype(float)
    checks=[]
    for nm,bn in [('BB20_L','bb20_lower_signal'),('BB40_L','bb40_lower_signal')]:
        d=x[nm+'_signal_close_distance_pct'].astype(float)
        inferred=close/(1+d/100)
        measured=x[bn].astype(float)
        err=(inferred-measured).abs()
        checks.append({'source_contract':'R1D6 '+nm+' distance vs independent R1D19 same-date band',
                       'n_compared':int(err.notna().sum()),'n_equal_tolerance_0p01_krw':int(err.le(.01).sum()),
                       'max_absolute_krw':float(err.max()),'median_absolute_krw':float(err.median())})
        x[nm+'_band_r1d6_inferred']=inferred
        x[nm+'_band_r1d19']=measured
    # Reconstruct the underlying MA from frozen 10% Envelope lower (MA * 0.9).
    for n in (20,40):
        col=f'ENV{n}_L_10_signal_close_distance_pct'
        lower=close/(1+x[col].astype(float)/100)
        ma=lower/.90
        for pct in (2,10,20,40):
            if n==20 and pct==40:continue
            if n==40 and pct==20:continue
            tag=f'ENV{n}_PCT{pct}'
            line=ma*(1-pct/100)
            x[tag+'_lower_raw_inferred']=line
            # reproduce relevant near windows from original source code
            tolerance=.02 if n==20 else .10
            x[tag+'_near']=near_fraction(close,line,tolerance).astype(int)
            x[tag+'_touch_from_close_below']=((close<line)&np.isfinite(line)).astype(int)
        x[f'ENV{n}_INFERRED_MA']=ma
    # Historic source used 40-sample std ddof=1; R1D19 DD0.
    mean40=x['ENV40_INFERRED_MA']
    std40=(mean40-x['bb40_lower_signal'])/2
    bbd1=mean40-2*std40*np.sqrt(40/39)
    x['BB40_DDOF1_LOWER_RECONSTRUCTED']=bbd1
    x['BB40_DDOF1_CLOSE_NEAR2P5']=near_fraction(close,bbd1,.025).astype(int)
    x['BB40_DDOF0_CLOSE_NEAR2P5']=near_fraction(close,x.bb40_lower_signal,.025).astype(int)
    # Enrich old R1D7 labeled outcomes without changing simulation or CA.
    labels=c[c.horizon.isin([5,20])].copy()
    l20=labels[labels.horizon==20].drop(columns=['horizon']).rename(columns={i:'D20_'+i for i in c.columns if i not in key+['horizon']})
    l5=labels[labels.horizon==5][key+['status_out','net_20bps_pct','exit_day','entry_open']].rename(columns={'status_out':'D5_status_out','net_20bps_pct':'D5_net_20bps_pct','exit_day':'D5_exit_day','entry_open':'D5_entry_open'})
    x=x.merge(l20,on=key,validate='one_to_one').merge(l5,on=key,validate='one_to_one')
    assert len(x)==5247
    x['D20_COMPLETE']=x['D20_status_out'].eq('PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED')
    # Big drawdown/gaps from R1D7 flags risk review, but NOT evidence of a specific corporate action.
    x['LARGE_DRAWDOWN_WATCH']=x['D20_mae_intraday_pct'].le(-30).fillna(False)
    x['D20_GAP_CA_QUARANTINE']=x['D20_status_out'].eq('UNADJUSTED_FUTURE_CORPORATE_ACTION_OR_EXTREME_GAP_BLOCKED')
    names=['bb20_reclaim_today','bb30_reclaim_today','bb40_reclaim_today','bb20_reclaim_recent5','bb30_reclaim_recent5','bb40_reclaim_recent5','BB40_DDOF0_CLOSE_NEAR2P5','BB40_DDOF1_CLOSE_NEAR2P5','ENV20_PCT2_near','ENV20_PCT10_near','ENV20_PCT20_near','ENV40_PCT2_near','ENV40_PCT10_near','ENV40_PCT40_near']
    sums=[]
    for n in names:
        mask=x[n].eq(1)
        sub=x.loc[mask];ok=sub[sub.D20_COMPLETE & ~sub.LARGE_DRAWDOWN_WATCH]
        sums.append({'group':n,'population_n':len(x),'n_flagged':len(sub),'d20_original_price_path_complete':int(sub.D20_COMPLETE.sum()),
                     'd20_excluded_mae30_watch':int((sub.D20_COMPLETE & sub.LARGE_DRAWDOWN_WATCH).sum()),
                     'd20_n_after_mae30_watch':len(ok),
                     'original_d20_avg_pct_UNCERTIFIED':float(ok.D20_net_20bps_pct.mean()) if len(ok) else None,
                     'original_d20_win_pct_UNCERTIFIED':float((ok.D20_net_20bps_pct>0).mean()*100) if len(ok) else None,
                     'original_d20_median_pct_UNCERTIFIED':float(ok.D20_net_20bps_pct.median()) if len(ok) else None,
                     'original_d20_p10_pct_UNCERTIFIED':float(ok.D20_net_20bps_pct.quantile(.1)) if len(ok) else None,
                     'original_d20_p90_pct_UNCERTIFIED':float(ok.D20_net_20bps_pct.quantile(.9)) if len(ok) else None,
                     'distinct_tickers':int(sub.code.nunique()),'distinct_signal_dates':int(sub.date.nunique()),
                     'ca_full_ledger_certified':False,'new_live_eligible':0})
    pd.DataFrame(sums).to_csv(out/'02_HISTORICAL_SIGNAL_DEFINITION_DIFFERENCES.csv',index=False)
    pair=[]
    for z,y in [('ENV20_PCT2_near','ENV20_PCT10_near'),('ENV20_PCT10_near','ENV20_PCT20_near'),('ENV40_PCT2_near','ENV40_PCT10_near'),('ENV40_PCT10_near','ENV40_PCT40_near'),('BB40_DDOF0_CLOSE_NEAR2P5','BB40_DDOF1_CLOSE_NEAR2P5')]:
        zz=x[z].eq(1);yy=x[y].eq(1)
        pair.append(dict(mode_a=z,mode_b=y,both=int((zz&yy).sum()),only_a=int((zz&~yy).sum()),only_b=int((~zz&yy).sum()),neither=int((~zz&~yy).sum())))
    pd.DataFrame(pair).to_csv(out/'03_DEFINITION_SAME_DATE_CROSS_TAB.csv',index=False)
    pd.DataFrame(checks).to_csv(out/'01_FROZEN_SAME_DATE_BB_SOURCE_PARITY.csv',index=False)
    # No optimize on outcomes. For each mode pick chronological earliest plus biggest ex post drawdown and rise, explicitly labeled.
    rev=[]
    cols=['date','code','signal_close','bb20_lower_signal','bb40_lower_signal','ENV20_PCT10_lower_raw_inferred','ENV20_PCT20_lower_raw_inferred',
          'ENV40_PCT10_lower_raw_inferred','ENV40_PCT40_lower_raw_inferred','D20_entry_day','D20_entry_open','D20_exit_day',
          'D20_exit_close','D20_net_20bps_pct','D20_mae_intraday_pct','D20_mfe_intraday_pct','D20_status_out','D5_net_20bps_pct',
          'signal_amount_vs_prior5median','amount_3d_vs_prior10d','LARGE_DRAWDOWN_WATCH']
    for label,flag in [('BB20_RECLAIM','bb20_reclaim_today'),('BB40_RECLAIM','bb40_reclaim_today'),
            ('ENV20_10_NEAR','ENV20_PCT10_near'),('ENV20_20_NEAR','ENV20_PCT20_near'),('ENV40_10_NEAR','ENV40_PCT10_near'),('ENV40_40_NEAR','ENV40_PCT40_near')]:
        pool=x[x[flag].eq(1) & x.D20_COMPLETE & ~x.LARGE_DRAWDOWN_WATCH].copy()
        if len(pool)==0:continue
        for sel,row in [('FIRST_CHRONOLOGICAL',pool.sort_values(['date','code']).iloc[0]),
                        ('LARGEST_D20_LOSS',pool.sort_values(['D20_net_20bps_pct','date']).iloc[0]),
                        ('LARGEST_D20_GAIN',pool.sort_values(['D20_net_20bps_pct','date'],ascending=[False,True]).iloc[0])]:
            record={c:row.get(c) for c in cols}
            record.update({'strategy':label,'selection_method':sel,'not_trade_certified':1,'not_new_OOS':1})
            rev.append(record)
    pd.DataFrame(rev).to_csv(out/'04_PER_STOCK_HISTORICAL_PRICE_REVIEW.csv',index=False,encoding='utf-8-sig')
    detailcols=key+['signal_close','bb20_lower_signal','bb40_lower_signal','bb20_reclaim_today','bb40_reclaim_today','ENV20_PCT2_near','ENV20_PCT10_near','ENV20_PCT20_near','ENV40_PCT2_near','ENV40_PCT10_near','ENV40_PCT40_near','BB40_DDOF0_CLOSE_NEAR2P5','BB40_DDOF1_CLOSE_NEAR2P5','D20_status_out','D20_entry_day','D20_entry_open','D20_exit_day','D20_exit_close','D20_net_20bps_pct','D20_mae_intraday_pct','D20_mfe_intraday_pct','LARGE_DRAWDOWN_WATCH']
    x[detailcols].to_csv(out/'05_ALL_5247_REAUDIT_DETAILS.csv.gz',index=False,compression='gzip')
    audit={'frozen_event_count':len(x),'d20_complete':int(x.D20_COMPLETE.sum()),'source_bb_parity':checks,
       'legacy_R1D19_period':'2024-01-02 ~ 2025-09-30','cohort':'R1D6 preselected 224MA wave pullback proxy, not all KRX event dates',
       'source_sha256':{str(y.name):hashlib.sha256(Path(y).read_bytes()).hexdigest() for y in map(Path,[src6,src19,src7])},
       'analysis':'Original R1D19/R1D6/R1D7 frozen outcome replay and formula ablation. Not an independent price correction',
       'cannot certify_corporate_action_adjusted_prices':True,'cannot_certify_causal_strategy_returns':True,
       'no_new_live_changes':True}
    (out/'00_AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2),encoding='utf8')
    print(json.dumps({'n':len(x),'complete':audit['d20_complete'],'parity':checks},ensure_ascii=False,indent=2))
    return x

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--r1d6',required=True);p.add_argument('--r1d19',required=True);p.add_argument('--r1d7',required=True);p.add_argument('--out',required=True);a=p.parse_args()
    run(a.r1d6,a.r1d19,a.r1d7,a.out)
