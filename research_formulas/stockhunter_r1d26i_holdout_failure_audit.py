#!/usr/bin/env python3
"""R1-D26I evidence audit: frozen G discovery vs H ticker-disjoint historical holdout.

RESEARCH ONLY. This does not collect new KRX values, calibrate thresholds, run
trading backtests, certify original KRX exports or certify historical PIT.
"""
from __future__ import annotations
import argparse, hashlib, io, json, re, zipfile
from pathlib import Path
import numpy as np
import pandas as pd

VERSION = 'R1D26I_G_VS_H_HISTORICAL_HOLDOUT_FAILURE_AND_OFFICIAL_SOURCE_PENDING_V1'
OLD_FILE='reports/r1d26g/result/historical/R1D26G_ALL_100_SELECTED_EVENT_FLOW_FEATURES_WITH_99_D30.csv'
NEW_FILE='reports/r1d26h/result/locked_primary/R1D26H_200_TICKER_DISJOINT_FEATURES_AND_D30.csv'
FLOW_FILE='reports/r1d26h/result/source/R1D26H_COMPLETE_300_PRIOR20_SIGNED_KRW_NOT_PIT.csv'
EVT_FILE='reports/r1d26h/result/source/R1D26H_FROZEN_300_EVENTS_NO_OUTCOMES.csv'
OFFICIAL_FILE='reports/r1d26h/result/source/R1D26H_KRX_INDEPENDENT_AMOUNT_CROSSCHECK_10POINT_TEMPLATE.csv'
H_SOURCE_MANIFEST='reports/r1d26h/result/source/R1D26H_COLLECTION_MANIFEST.json'
H_QA_MANIFEST='reports/r1d26h/result/locked_primary/R1D26H_QA_MANIFEST.json'
G_QA_MANIFEST='reports/r1d26g/result/historical/R1D26G_QA_MANIFEST.json'
STRAT_D='D_LEADER_CONSOLIDATION_BREAKOUT'
STRAT_C='C_ABOVE224_RISING_WAVE_PULLBACK'


def require(ok, msg):
    if not bool(ok):raise ValueError(msg)

def sha(b): return hashlib.sha256(b).hexdigest()

def read_zip(path,expected):
    b=Path(path).read_bytes();require(sha(b)==expected,'UPSTREAM_ARCHIVE_SHA_DRIFT')
    return zipfile.ZipFile(io.BytesIO(b))

def csv_from(z,name): return pd.read_csv(io.BytesIO(z.read(name)), dtype={'code':str})

def integer(v):
    s=str(v).strip()
    require(bool(re.fullmatch(r'-?\d+',s)), 'OFFICIAL_NON_INTEGER_KRW')
    return int(s)

def prepare(gzip,hzip,p):
    g=read_zip(gzip,p['g']['zip_sha256']);h=read_zip(hzip,p['h']['zip_sha256'])
    old=csv_from(g,OLD_FILE);new=csv_from(h,NEW_FILE)
    flow=csv_from(h,FLOW_FILE);evt=csv_from(h,EVT_FILE)
    off=csv_from(h,OFFICIAL_FILE)
    gqa=json.loads(g.read(G_QA_MANIFEST)); hsource=json.loads(h.read(H_SOURCE_MANIFEST)); hqa=json.loads(h.read(H_QA_MANIFEST))
    require(sha(g.read(OLD_FILE))==gqa['output_file_sha256'][Path(OLD_FILE).name], 'G_PRICE_SEAL_MISMATCH')
    require(sha(h.read(FLOW_FILE))==hsource['files_sha256'][Path(FLOW_FILE).name], 'FLOW_SEAL_MISMATCH')
    require(sha(h.read(EVT_FILE))==hsource['files_sha256'][Path(EVT_FILE).name], 'EVENT_SEAL_MISMATCH')
    require(sha(h.read(NEW_FILE))==hqa['output_file_sha256'][Path(NEW_FILE).name], 'H_PRICE_SEAL_MISMATCH')
    require(hsource['combined_complete']==300 and hsource['combined_rows']==6000 and hsource['holdout_new_completed']==200,'WRONG_H_SOURCE_SCOPE')
    require(hqa['holdout_events']==200 and hqa['control_training_ticker_overlap']==0,'HOLDOUT_DISJOINT_NOT_VERIFIED')
    require(len(old)==100 and len(new)==200 and len(evt)==300 and len(flow)==6000, 'CARDINALITY_DRIFT')
    require(len(set(old.code.astype(str).str.zfill(6))&set(new.code.astype(str).str.zfill(6)))==0, 'OLD_NEW_TICKER_OVERLAP')
    require(new.code.astype(str).str.zfill(6).nunique()==200, 'NEW_DUPLICATE_TICKER')
    require(set(new.strategy)=={STRAT_C,STRAT_D},'STRATEGY_ID_DRIFT')
    require((flow.unit=='KRW').all() and flow.source.astype(str).str.contains('KRX_VIA_PYKRX').all(),'FLOW_UNIT_OR_PROVIDER_DRIFT')
    require(len(flow[['signal_date','code','trade_date']].drop_duplicates())==6000,'DUPLICATE_FLOW_DAY')
    flow['code']=flow.code.astype(str).str.zfill(6)
    flow['foreign_net_krw']=flow.foreign_net_krw.map(integer)
    flow['institution_net_krw']=flow.institution_net_krw.map(integer)
    flow['trade_date']=pd.to_datetime(flow.trade_date);flow['signal_date']=pd.to_datetime(flow.signal_date)
    require((flow.trade_date<flow.signal_date).all(),'POST_SIGNAL_FLOW_LEAK')
    fcounts=flow.groupby(['signal_date','code']).size()
    require(len(fcounts)==300 and (fcounts==20).all(),'FLOW_20SESSIONS_INCOMPLETE')
    # Recompute the central H indicator from sealed signed investor amounts, not H feature output.
    hnew=new.copy();hnew.code=hnew.code.astype(str).str.zfill(6)
    for row in hnew.itertuples(index=False):
        rows=flow[(flow.signal_date==pd.Timestamp(row.date))&(flow.code==row.code)].sort_values('trade_date')
        require(len(rows)==20,'NEW_EVENT_FLOW_NOT_20')
        v=rows.institution_net_krw.tolist();flag=int(3*sum(v[-5:])>sum(v[:15]))
        require(flag==int(row.institution_recent5_accel_pos),'H_INSTITUTION_FLAG_NOT_REPRODUCIBLE')
    # Verify 10-point official sample exactly comes from the frozen 6000-row flow file.
    for row in off.itertuples(index=False):
        r=flow[(flow.signal_date==pd.Timestamp(row.signal_date))&(flow.code==str(row.code).zfill(6))&(flow.trade_date==pd.Timestamp(row.trade_date))]
        require(len(r)==1,'OFFICIAL_TEMPLATE_ROW_NOT_IN_FROZEN_FLOW')
        require(int(r.iloc[0].foreign_net_krw)==int(row.pykrx_foreign_net_krw),'OFFICIAL_TEMPLATE_FOREIGN_SOURCE_CHANGED')
        require(int(r.iloc[0].institution_net_krw)==int(row.pykrx_institution_net_krw),'OFFICIAL_TEMPLATE_INST_SOURCE_CHANGED')
    require(len(off)==10,'OFFICIAL_10POINT_COUNT_DRIFT')
    require(p['no_strategy_rule_change'] and p['official_verification_is_pending_without_official_export'],'PROTOCOL_UNSAFE')
    return old,new,flow,evt,off,gqa,hsource,hqa


def extract_complete(d,mode):
    d=d.copy();d['code']=d.code.astype(str).str.zfill(6)
    mask=d.D30_completed.astype(str).str.lower().isin(('true','1')) if mode=='G' else d.price_complete.astype(str).str.lower().isin(('true','1'))
    require(mask.sum()==(99 if mode=='G' else 195),'D30_COMPLETE_MISMATCH')
    d=d[mask].copy()
    for k in ('hit5_by_d30','hit10_by_d30','hit15_by_d30'):
        require(d[k].isin((0,1)).all(),'NON_BINARY_HIT_'+k)
    require((d.hit15_by_d30<=d.hit10_by_d30).all() and (d.hit10_by_d30<=d.hit5_by_d30).all(),'NON_MONOTONIC_HITS')
    d['inst_accel']=d.institution_recent5_accel_pos.astype(int)
    d['sample']='G_EXISTING_100_EXPLORATION' if mode=='G' else 'H_NEW_200_TICKER_DISJOINT'
    return d

def stats(s,sample,strategy,year=None):
    rows=[]
    for flag in (0,1):
        d=s[s.inst_accel==flag];n=len(d)
        rows.append(dict(sample=sample,strategy=strategy,year=('ALL' if year is None else year),inst_accel=flag,n=n,
                         hit5=int(d.hit5_by_d30.sum()),hit10=int(d.hit10_by_d30.sum()),hit15=int(d.hit15_by_d30.sum()),
                         rate5_pct=round(100*d.hit5_by_d30.mean(),5) if n else None,
                         rate10_pct=round(100*d.hit10_by_d30.mean(),5) if n else None,
                         rate15_pct=round(100*d.hit15_by_d30.mean(),5) if n else None))
    return rows

def official_reconcile(off,official_path=None):
    template=off.copy();template['code']=template.code.astype(str).str.zfill(6)
    template['official_export_sha256']=''
    template['official_data_attribution']=''
    result=[]
    if official_path is None:
        for r in template.to_dict('records'):
            r.update(verification_state='PENDING_INDEPENDENT_KRX_OFFICIAL_EXPORT',foreign_diff_krw='',institution_diff_krw='')
            result.append(r)
        return pd.DataFrame(result),template,0,'PENDING_INDEPENDENT_EXPORT'
    # This is a user-provided export, NOT an independently retrieved or publication-time certified source.
    src=pd.read_csv(official_path,dtype={'code':str}).fillna('')
    require(len(src)==10 and src.code.astype(str).str.zfill(6).nunique()>=1,'OFFICIAL_INPUT_WRONG_ROWS')
    for col in ('signal_date','trade_date','code','official_KRX_foreign_net_krw','official_KRX_institution_net_krw','source_official_export_name','official_export_sha256'):
        require(col in src.columns,'OFFICIAL_INPUT_SCHEMA_MISSING_'+col)
    idx={(str(r.signal_date),str(r.code).zfill(6),str(r.trade_date)):r for r in src.itertuples(index=False)}
    require(len(idx)==10,'OFFICIAL_INPUT_DUPLICATES')
    matched=0
    for row in template.to_dict('records'):
        key=(str(row['signal_date']),row['code'],str(row['trade_date']))
        require(key in idx,'OFFICIAL_INPUT_KEYS_CHANGED')
        x=idx[key];name=str(x.source_official_export_name).strip();digest=str(x.official_export_sha256).strip()
        require(bool(name) and bool(re.fullmatch('[0-9a-f]{64}',digest)),'OFFICIAL_EXPORT_PROVENANCE_MISSING')
        af=integer(x.official_KRX_foreign_net_krw);ai=integer(x.official_KRX_institution_net_krw)
        deltaf=af-int(row['pykrx_foreign_net_krw']);deltai=ai-int(row['pykrx_institution_net_krw'])
        state='USER_EXPORTED_KRX_VALUE_MATCH_PIT_NOT_VERIFIED' if not deltaf and not deltai else 'MISMATCH_OFFICIAL_VS_PYKRX'
        matched+=int(not deltaf and not deltai)
        row.update(official_KRX_foreign_net_krw=af,official_KRX_institution_net_krw=ai,source_official_export_name=name,official_export_sha256=digest,foreign_diff_krw=deltaf,institution_diff_krw=deltai,verification_state=state)
        result.append(row)
    state='USER_EXPORT_10OF10_AMOUNT_MATCH_PIT_NOT_VERIFIED' if matched==10 else 'OFFICIAL_AMOUNT_MISMATCH_FAIL_CLOSED'
    return pd.DataFrame(result),template,matched,state

def generate(protocol,g_zip,h_zip,out,official_csv=None):
    p=json.loads(Path(protocol).read_text(encoding='utf-8'))
    old,new,flow,evt,off,gqa,hsource,hqa=prepare(g_zip,h_zip,p)
    o=extract_complete(old,'G');n=extract_complete(new,'H')
    require(len(o[o.strategy==STRAT_D])==49 and len(n[n.strategy==STRAT_D])==97,'D_SAMPLE_MISMATCH')
    require(len(n[n.strategy==STRAT_C])==98,'C_SAMPLE_MISMATCH')
    # No parameter optimization, fixed old-vs-new contrasts and year details.
    comparison=[];yearly=[]
    for sample,d in [('G_EXISTING_100_EXPLORATION',o),('H_NEW_200_TICKER_DISJOINT',n)]:
        for strategy in (STRAT_C,STRAT_D):
            q=d[d.strategy==strategy]
            comparison+=stats(q,sample,strategy)
            for year in range(2021,2026):
                yearly+=stats(q[q.year==year],sample,strategy,year)
    comp=pd.DataFrame(comparison);years=pd.DataFrame(yearly)
    dcomp=comp[comp.strategy==STRAT_D].copy()
    detail=[]
    for sample in ('G_EXISTING_100_EXPLORATION','H_NEW_200_TICKER_DISJOINT'):
        a=dcomp[dcomp['sample']==sample].set_index('inst_accel')
        detail.append({'sample':sample,'accel_n':int(a.loc[1,'n']),'non_accel_n':int(a.loc[0,'n']),
                       'accel_hit15':int(a.loc[1,'hit15']),'non_accel_hit15':int(a.loc[0,'hit15']),
                       'hit15_diff_pp':round(float(a.loc[1,'rate15_pct']-a.loc[0,'rate15_pct']),5),
                       'overall_hit15_rate_pct':round(100*(a.hit15.sum()/a.n.sum()),5)})
    detail=pd.DataFrame(detail)
    require(detail.iloc[0].accel_n==24 and detail.iloc[1].accel_n==46,'OLD_NEW_DENOMINATOR_DRIFT')
    require(detail.iloc[0].accel_hit15==12 and detail.iloc[1].accel_hit15==16,'OLD_NEW_HIT15_DRIFT')
    require(abs(float(detail.iloc[1].hit15_diff_pp)-1.44928)<0.001,'H_PRIMARY_DIFFERENCE_MISMATCH')
    year_diff=[]
    for yr in range(2021,2026):
        r={'year':yr}
        for sample,label in [('G_EXISTING_100_EXPLORATION','g'),('H_NEW_200_TICKER_DISJOINT','h')]:
            d=years[(years['year']==yr)&(years['sample']==sample)&(years.strategy==STRAT_D)].set_index('inst_accel')
            r[label+'_n_accel']=int(d.loc[1,'n']);r[label+'_n_other']=int(d.loc[0,'n'])
            r[label+'_hits_accel']=int(d.loc[1,'hit15']);r[label+'_hits_other']=int(d.loc[0,'hit15'])
            r[label+'_diff_pp']=round(float(d.loc[1,'rate15_pct']-d.loc[0,'rate15_pct']),5)
        r['changed_sign']=bool(np.sign(r['g_diff_pp'])!=np.sign(r['h_diff_pp']))
        year_diff.append(r)
    yd=pd.DataFrame(year_diff)
    recon,template,matching,cross_state=official_reconcile(off,official_csv)
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    files={'R1D26I_G_H_ALL_HIT5_10_15_BY_INSTITUTION_ACCEL.csv':comp,
           'R1D26I_G_H_YEAR_STRATEGY_OBSERVATIONAL_DESCRIPTIVE.csv':years,
           'R1D26I_D_G_VS_H_PRIMARY_EFFECT.csv':detail,
           'R1D26I_D_YEARWISE_DISCOVERY_HOLDOUT_DRIFT.csv':yd,
           'R1D26I_KRX_10POINT_USER_EXPORT_TEMPLATE_NOT_OFFICIAL_DATA.csv':template,
           'R1D26I_KRX_10POINT_VALUE_RECONCILIATION_NOT_PIT.csv':recon}
    hashes={}
    for name,d in files.items():
        path=out/name;d.to_csv(path,index=False,encoding='utf-8-sig');hashes[name]=sha(path.read_bytes())
    m={'version':VERSION,'research_only':True,'g_zip_sha256':p['g']['zip_sha256'],'h_zip_sha256':p['h']['zip_sha256'],
       'g_exploration_events':100,'g_D_price_complete':49,'h_new_events':200,'h_D_price_complete':97,
       'h_total_raw_investor_rows':6000,'h_independent_new_tickers':200,'g_vs_h_ticker_overlap':0,
       'G_D_diff_hit15_percentage_points':float(detail.iloc[0].hit15_diff_pp),
       'H_D_diff_hit15_percentage_points':float(detail.iloc[1].hit15_diff_pp),
       'H_locked_holdout_95pct_ci_pp':hqa['primary']['year_stratified_ticker_disjoint_bootstrap_95pct_ci_difference_pp'],
       'H_locked_holdout_permutation_two_sided_p':hqa['primary']['year_stratified_permutation_two_sided_p'],
       'observational_descriptive_not_cause_proven':True,
       'source_independent_crosscheck_status':cross_state,'official_points_provided':10 if official_csv else 0,
       'official_amount_rows_matched_if_supplied':matching,'official_same_date_publication_PIT':'NOT_VERIFIED',
       'historical_ticker_holdout_not_temporal_OOS':True,'strategy_ready':False,'live_changed':False,'orders_placed':0,
       'file_sha256':hashes}
    (out/'R1D26I_QA_MANIFEST.json').write_text(json.dumps(m,ensure_ascii=False,indent=2)+'\n',encoding='utf8')
    print('R1D26I_HOLDOUT_DIAGNOSIS_QA_PASS',json.dumps({k:m[k] for k in ['g_D_price_complete','h_D_price_complete','G_D_diff_hit15_percentage_points','H_D_diff_hit15_percentage_points','source_independent_crosscheck_status','strategy_ready']},ensure_ascii=False))
    return m

if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('--protocol',required=True);ap.add_argument('--g-zip',required=True);ap.add_argument('--h-zip',required=True);ap.add_argument('--out',required=True);ap.add_argument('--official-csv');a=ap.parse_args()
    generate(a.protocol,a.g_zip,a.h_zip,a.out,a.official_csv)
