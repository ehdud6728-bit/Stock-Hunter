"""Prespecified single-hypothesis retrospective replication on R1D27D 1500.

PRIMARY TEST (ONE): D+15 net return for (institution AND foreign recent10
minus previous10 KRW net-flow > 0), controlled for year/BB/event group.
No threshold optimization. No causal, PIT, LIVE, real-fill, or prospective claim.
"""
from __future__ import annotations
import argparse,gzip,hashlib,io,json,pathlib,zipfile
import numpy as np,pandas as pd
from research_formulas.stockhunter_r1d27d_flow60 import RAW,AUDIT,MAN,KEY,FROZEN_SHA,read_event_index,load_old,check,sha
from research_formulas.stockhunter_r1d27d_fullmarket_freeze import A27_ZIP_SHA,OUTCOME_SHA,O

STRATA=['year','bb_period','event_type']
PRIMARY='both_inst_foreign_10d_net_acceleration_pos'

def fixed_features(freeze_dir,flow_root,out):
    f=read_event_index(freeze_dir)
    # load_old verifies both source file SHA, gross/net identities, 60x4 groups
    raw,old=load_old(flow_root,f)
    good={k for k,v in old.items() if int(v['source_complete'])==1}
    check(len(good)>=1200,'PREDECLARED_MIN_1200_SOURCE')
    q=pd.DataFrame(raw)
    rows=[]
    for key,g in q.groupby(KEY,sort=False):
        acc={};days=sorted(g.trade_date.unique())
        check(len(days)==60,'FEATURE_DAYS')
        for actor,n in [('기관합계','inst'),('외국인합계','foreign')]:
            t=g[g.actor.eq(actor)].sort_values('trade_date')
            check(len(t)==60,'FEATURE_ACTOR_DAYS')
            v=pd.to_numeric(t.net_krw).to_numpy(dtype=float)
            acc[n+'_recent10']=float(v[-10:].sum())
            acc[n+'_previous10']=float(v[-20:-10].sum())
            acc[n+'_acceleration']=acc[n+'_recent10']-acc[n+'_previous10']
        rows.append(dict(zip(KEY,key),**acc,**{PRIMARY:int(acc['inst_acceleration']>0 and acc['foreign_acceleration']>0)}))
    result=pd.DataFrame(rows).sort_values(KEY).reset_index(drop=True)
    check(len(result)==len(good) and result[PRIMARY].isin([0,1]).all(),'FEATURE_COMPLETENESS')
    out=pathlib.Path(out);out.mkdir(parents=True,exist_ok=True)
    path=out/'R1D27D_PRE_SIGNAL_FLOW_FEATURES.csv';result.to_csv(path,index=False,lineterminator='\n')
    m=dict(stage='features_only',primary=PRIMARY,lookback_signal_date_excluded=True,
           n_features=len(result),signal_feature_sha256=sha(path.read_bytes()),
           source_frozen_sha256=FROZEN_SHA,prospective_OOS=False,source_and_features_do_not_read_futures=True)
    (out/'R1D27D_FEATURES_MANIFEST.json').write_text(json.dumps(m,ensure_ascii=False,indent=2)+'\n')
    return result,m

def evaluate(freeze_dir,flow_root,market_zip,out,reps=2000,seed=20261028):
    # Freeze the standalone signal-time feature file BEFORE opening future outcomes.
    f,m=fixed_features(freeze_dir,flow_root,out)
    check(1000<=reps<=10000,'PERM_REPS')
    check(sha(pathlib.Path(market_zip).read_bytes())==A27_ZIP_SHA,'MARKET_SOURCE_DRIFT')
    with zipfile.ZipFile(market_zip) as z:
        b=z.read(O.lstrip('/'));check(sha(b)==OUTCOME_SHA,'OUTCOME_SOURCE_SHA_DRIFT')
        y=pd.read_csv(io.BytesIO(gzip.decompress(b)),dtype={'date':str,'code':str,'bb_period':str,'event_type':str})
    # Only primary D+15 is evaluated for hypothesis; no multiple-horizon shopping.
    y=y.loc[y.horizon.eq(15)].copy()
    check(len(y)==133180 and not y[KEY].duplicated().any(),'OUTCOME_KEYS')
    d=f.merge(y[KEY+['status_out','net_20bps_pct','MFE_intraday_pct','MAE_intraday_pct']],on=KEY,how='left',validate='one_to_one')
    check(len(d)==len(f),'OUTCOME_JOIN_COUNT')
    d['price_complete']=d.status_out.eq('PRICE_PATH_COMPLETE_NOT_FILL_CERTIFIED') & d.net_20bps_pct.notna()
    t=d[d.price_complete].copy();t['year']=t.date.str[:4];
    x=t[PRIMARY].to_numpy(dtype=int);returns=t.net_20bps_pct.to_numpy(float)
    check(0<x.sum()<len(x),'PRIMARY_GROUP_COUNTS')
    raw=returns[x==1].mean()-returns[x==0].mean()
    # Fixed-effects within 30 strata; year, BB length, signal pattern.
    g,_=pd.factorize(t[STRATA].astype(str).agg('|'.join,axis=1),sort=True)
    n=np.bincount(g);nx=np.bincount(g,weights=x);ny=np.bincount(g,weights=returns)
    x0=x-nx[g]/n[g];y0=returns-ny[g]/n[g]
    denom=(x0*x0).sum();check(denom>0,'STRATUM_NO_CONTRAST')
    fe=(x0*y0).sum()/denom
    groups=[np.where(g==i)[0] for i in range(len(n))]
    rng=np.random.default_rng(seed);larger=0
    for j in range(reps):
        pp=np.arange(len(t))
        for ids in groups:pp[ids]=rng.permutation(ids)
        slope=(x0[pp]*y0).sum()/denom
        larger+=abs(slope)>=abs(fe)
    pval=(larger+1)/(reps+1)
    stats=[]
    for flag in (1,0):
        q=t[t[PRIMARY].eq(flag)];v=q.net_20bps_pct
        stats.append(dict(group='both_institution_foreign_10d_accel' if flag else 'other',n=len(q),
            mean_d15_pct=float(v.mean()),median_d15_pct=float(v.median()),win_rate=float((v>0).mean()),
            loss_lt_minus10_rate=float((v<-10).mean()),mae_median=float(q.MAE_intraday_pct.median())))
    out=pathlib.Path(out)
    path=out/'R1D27D_PRIMARY_D15_GROUP_SUMMARY.csv';pd.DataFrame(stats).to_csv(path,index=False,lineterminator='\n')
    d[KEY+[PRIMARY,'status_out','price_complete','net_20bps_pct','MAE_intraday_pct','MFE_intraday_pct']].to_csv(out/'R1D27D_ALL_SOURCE_EVENT_JOIN_AUDIT.csv',index=False,lineterminator='\n')
    summary=dict(stage='evaluate',study='R1D27D',full_universe_price_only_events=133180,
        frozen_ticker_disjoint_sample=1500,source_complete=len(f),evaluable=len(t),price_censored=len(d)-len(t),
        primary=PRIMARY,primary_horizon=15,mean_delta_raw_pct_points=float(raw),
        FE_year_BB_event_delta_pct_points=float(fe),predeclared_single_test_permutation_p=float(pval),
        permutations=reps,source_feature_sha256=m['signal_feature_sha256'],original_R1D27A_sha=A27_ZIP_SHA,
        exploratory_retrospective_same_market_years=True,independent_new_tickers_vs_old300=True,
        independent_temporal_OOS=False,causal_claim=False,real_fill_certified=False,
        strategy_ready=False,orders_placed=0,live_changes=False)
    summary['files_sha256']={q.name:sha(q.read_bytes()) for q in out.glob('*.csv')}
    (out/'R1D27D_EVAL_MANIFEST.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n')
    return summary

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--freeze-dir',required=True);p.add_argument('--checkpoint-root',required=True)
    p.add_argument('--source',required=True);p.add_argument('--out',required=True)
    a=p.parse_args();print(json.dumps(evaluate(a.freeze_dir,a.checkpoint_root,a.source,a.out),ensure_ascii=False))
