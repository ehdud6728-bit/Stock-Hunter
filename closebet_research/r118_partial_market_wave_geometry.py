#!/usr/bin/env python3
"""R1.18: independent broad 224-origin wave geometry, coverage gaps and missed opportunities.

RESEARCH_ONLY. No LIVE, rankings, orders, threshold fitting or 15:03 execution claim.
40,305 frozen R1.17 source code-days are an EX-POST 907-name subset, NOT a PIT market.
KRX 1907 session per-day source hashes are verified before computing.
Future D1-open to D5-close returns are OUTCOMES, never candidate features.
"""
from __future__ import annotations
import argparse, gzip, hashlib, io, json
from collections import defaultdict
from pathlib import Path
from zipfile import ZipFile
import numpy as np
import pandas as pd

VERSION='R118_PARTIAL_MARKET_GAP_224_ORIGIN_BROAD_GEOMETRY_V1'
R117_RUN=38856340802
R171_RUN=38664095610
SHA=hashlib.sha256


def require(cond,msg):
    if not cond:raise RuntimeError(msg)

def digest(b):return SHA(b).hexdigest()

def sym(s):
    a=str(s).strip(); return (a[:-2] if a.endswith('.0') else a).zfill(6)

def read_r117(root:Path):
    m=json.loads((root/'r117_integrity_manifest.json').read_text(encoding='utf-8'))
    require(m['version']=='R117_V4976_SELECTED_VS_COVERED_COHORT_AND_SHADOW_FROZEN_V1','R117_VERSION_MISMATCH')
    require(m['status']=='PASS_DESCRIPTIVE_PARTIAL_DENOMINATOR_NO_PROSPECTIVE_EDGE_CLAIM','R117_SCOPE_MISMATCH')
    name='r117_covered_cohort_d0_features_d1open_d5close.csv'
    raw=(root/name).read_bytes()
    require(digest(raw)==m['file_sha256'][name],'R117_COHORT_SHA_MISMATCH')
    a=pd.read_csv(io.BytesIO(raw),dtype={'code':str},low_memory=False)
    a['code']=a.code.map(sym); a['signal_date']=a.signal_date.astype(str)
    require(len(a)==40305 and int(a.selected_any_mode.sum())==1059,'R117_POPULATION_OR_SELECTION_DRIFT')
    require(a[['signal_date','code']].duplicated().sum()==0,'R117_DUPLICATE_CODE_DAY')
    coverage_name='r117_source_coverage_vs_full_market_45_dates.csv'
    c_raw=(root/coverage_name).read_bytes()
    require(digest(c_raw)==m['file_sha256'][coverage_name],'R117_COVERAGE_SHA_MISMATCH')
    c=pd.read_csv(io.BytesIO(c_raw))
    require(len(c)==45 and c.signal_date.nunique()==45,'R117_45_DATE_COVERAGE_MISSING')
    return a,c,m

def read_immutable_krx(history_dir:Path,lowest_date:str='2025-01-01'):
    archives=sorted(history_dir.glob('*.zip'))
    require(len(archives)==4,'FOUR_IMMUTABLE_KRX_SHARDS_REQUIRED')
    seen={};frames=[]; manifests=[];cal=[]
    for path in archives:
        with ZipFile(path) as z:
            mf=[x for x in z.namelist() if x.startswith('krx_history_manifest_') and x.endswith('.json')]
            require(len(mf)==1,'KRX_MANIFEST_ABSENT')
            m=json.loads(z.read(mf[0]));manifests.append(m)
            require(m['status']=='COMPLETE' and m['official_days']==1907,'KRX_MANIFEST_NOT_COMPLETE')
            for x in m['files']:
                name=x['file']; day=x['day'];raw=z.read(name)
                require(digest(raw)==x['sha256'],'KRX_DAY_SHA_MISMATCH '+day)
                require(day not in seen,'KRX_DUPLICATE_DAY '+day)
                seen[day]=(int(x['rows']),int(x['all_market_rows']))
                if day>=lowest_date:
                    q=pd.read_csv(io.BytesIO(gzip.decompress(raw)),dtype={'code':str})
                    require(len(q)==x['rows'],'KRX_DAY_ROW_COUNT_MISMATCH '+day)
                    require({'code','date','open','high','low','close','volume','amount'}.issubset(q.columns),'KRX_SCHEMA_INVALID')
                    q['date']=day;q['code']=q.code.map(sym)
                    require(not q.code.duplicated().any(),'KRX_DAY_CODE_DUPLICATE '+day)
                    frames.append(q)
    require(len(seen)==1907,'KRX_DAY_COUNT_NOT_1907')
    require({m['shard_index'] for m in manifests}=={0,1,2,3},'KRX_SHARDS_INVALID')
    require(len({m['official_calendar_sha256'] for m in manifests})==1,'KRX_CALENDAR_FINGERPRINT_DRIFT')
    official=sorted(seen)
    full=pd.concat(frames,ignore_index=True)
    for f in ['open','high','low','close','volume','amount']:
        full[f]=pd.to_numeric(full[f],errors='coerce')
    full['sess']=full.date.map({d:i for i,d in enumerate(official)})
    require(full.sess.notna().all(),'KRX_UNKNOWN_DAY')
    require(not full.duplicated(['date','code']).any(),'KRX_DUPLICATE_CODE_DAY')
    return full, official, seen

def read_v4976_raw_and_opportunity_shards(shards:Path):
    """Verify all 8 same-run shards, but use their raw membership as ex-ante status only.

    Opportunity census was generated using future outcomes: allowed only for forensic
    annotations AFTER the independent candidate geometry has been frozen.
    """
    paths=sorted(shards.glob('*.zip'))
    require(len(paths)==8,'EIGHT_PINNED_V4976_SHARDS_REQUIRED')
    raw_frames=[]; opp_frames=[]; seen=set();files={}
    for p in paths:
        with ZipFile(p) as z:
            manifests=[x for x in z.namelist() if x.endswith('_manifest.json') and x.startswith('shard_')]
            require(len(manifests)==1,'SHARD_MANIFEST_MISSING '+p.name)
            m=json.loads(z.read(manifests[0])); n=int(m['shard_index'])
            require(n not in seen and 0<=n<=7 and int(m['shard_count'])==8,'SHARD_INDEX_MISMATCH')
            seen.add(n)
            require(m['engine_status']=='VALID' and m['research_lane']=='SKIPPED','SHARD_NOT_VALID_OR_ISOLATED')
            for key, expected, frames, usecols in [
                (f'shard_{n:02d}_raw.csv','raw_sha256',raw_frames,['signal_date','code','mode']),
                (f'shard_{n:02d}_opportunity_census.csv','opportunity_sha256',opp_frames,
                 ['signal_date','code','planned_modes','main_hit_modes','deep_audit_hit_modes','deep_audit_performed'])]:
                require(key in z.namelist(),'SHARD_DATA_MISSING '+key)
                blob=z.read(key)
                require(digest(blob)==m[expected],'SHARD_DATA_SHA_MISMATCH '+key)
                q=pd.read_csv(io.BytesIO(blob),usecols=usecols,dtype={'code':str},low_memory=False)
                require(len(q)==int(m['raw_rows'] if expected=='raw_sha256' else m['opportunity_rows']),
                        'SHARD_ROW_COUNT_BAD '+key)
                q['code']=q.code.map(sym)
                q['signal_date']=q.signal_date.astype(str).str.slice(0,10)
                frames.append(q)
                files[key]=digest(blob)
    require(seen==set(range(8)),'SHARD_SET_NOT_EIGHT')
    raw=pd.concat(raw_frames,ignore_index=True)
    opp=pd.concat(opp_frames,ignore_index=True)
    require(len(raw)==8742,'ORIGINAL_RAW_ROWS_DIFFER_FROM_FROZEN_8742')
    require(len(opp)==15190,'OPPORTUNITY_ROWS_DIFFER_FROM_FROZEN_15190')
    require(not opp[['signal_date','code']].duplicated().any(),'OPPORTUNITY_DUPLICATE_CODE_DAY')
    # Deduplicate modes for one stock/day; selected mode membership is carried by R117.
    r=raw.groupby(['signal_date','code'],as_index=False).agg(
        v4976_raw_hit_modes=('mode',lambda x:','.join(sorted(set(map(str,x))))),
        v4976_raw_hit_mode_rows=('mode','size'))
    r['v4976_raw_predicate_hit']=True
    p=opp[['signal_date','code','planned_modes','main_hit_modes','deep_audit_hit_modes','deep_audit_performed']].copy()
    p['v4976_opportunity_census_present']=True
    require(len(r)==7925,'RAW_UNIQUE_CODE_DAYS_DIFFER_FROM_FROZEN_7925')
    return r,p,{'raw_rows':len(raw),'raw_unique_code_days':len(r),'opportunity_census_rows':len(opp),
             'opportunity_deep_audit_performed_rows':int(pd.to_numeric(opp.deep_audit_performed,errors='coerce').eq(1).sum()),
             'opportunity_deep_audit_hit_modes_rows':int(opp.deep_audit_hit_modes.fillna('').astype(str).str.strip().ne('').sum()),
             'files_sha256':files}

def day_geometry(df:pd.DataFrame, asof_day:str, official_pos:dict)->dict:
    """Closed-form, non-optimized low->peak->D0 pullback geometry; no +16% / 1.25x.

    45 PRIOR OFFICIAL trading sessions required, with contiguous OHLCV records.
    Original low must have its own 224-session moving average with no missing sessions.
    Chart pattern is a broad PROXY, not human-labeled 'true first wave'.
    """
    t=official_pos[asof_day];
    pre=df[df.sess.lt(t)&df.sess.ge(t-45)].sort_values('sess').copy()
    a={'prior45_complete':len(pre)==45 and (pre.sess.nunique()==45),
       'origin_below224_asof':False,'broad_224_pullback_candidate':False,
       'wave_geometry_status':'NOT_EVALUATED','origin_date':'','peak_date':'',
       'origin_below224_pct':np.nan,'wave_rise_low_to_peak_pct':np.nan,
       'pullback_fraction':np.nan,'origin_to_peak_sessions':np.nan,
       'peak_to_d0_sessions':np.nan,'origin_ma224_contiguous':False}
    if not a['prior45_complete']:
        a['wave_geometry_status']='PRIOR45_OFFICIAL_SESSION_GAP'
        return a
    origin=pre.loc[pre.low.idxmin()]
    anchor_sess=int(origin.sess)
    past224=df[(df.sess<=anchor_sess)&(df.sess>=anchor_sess-223)].sort_values('sess')
    if len(past224)!=224 or past224.sess.nunique()!=224 or (past224.close<=0).any():
        a['wave_geometry_status']='ORIGIN_224_MA_INSUFFICIENT_OR_GAP'
        return a
    ma224=float(past224.close.mean());a['origin_ma224_contiguous']=True
    anchor=float(origin.low)
    if not np.isfinite(anchor) or anchor<=0:
        a['wave_geometry_status']='ORIGIN_BAD_LOW';return a
    pct=(anchor/ma224-1)*100
    a.update({'origin_date':str(origin.date),'origin_below224_pct':pct,'origin_below224_asof':anchor<ma224})
    after=pre[pre.sess>anchor_sess]
    if after.empty:
        a['wave_geometry_status']='NO_PRIOR_PEAK_AFTER_ORIGIN';return a
    peak=after.loc[after.high.idxmax()]
    top=float(peak.high)
    a['peak_date']=str(peak.date)
    a['origin_to_peak_sessions']=int(peak.sess)-anchor_sess
    a['peak_to_d0_sessions']=t-int(peak.sess)
    if not np.isfinite(top) or top<=anchor:
        a['wave_geometry_status']='NO_UPWARD_GEOMETRY';return a
    a['wave_rise_low_to_peak_pct']=(top/anchor-1)*100
    today=df[df.sess.eq(t)]
    if len(today)!=1:
        a['wave_geometry_status']='D0_SOURCE_NOT_UNIQUE';return a
    close=float(today.iloc[0].close)
    a['pullback_fraction']=(top-close)/(top-anchor)
    if not a['origin_below224_asof']:
        a['wave_geometry_status']='ORIGIN_NOT_BELOW224';return a
    if not (anchor<close<top):
        a['wave_geometry_status']='D0_NOT_BETWEEN_ORIGIN_AND_PEAK';return a
    a['broad_224_pullback_candidate']=True
    a['wave_geometry_status']='BROAD_LOW_PEAK_PULLBACK_PROXY_ONLY'
    return a

def compute_geometry(cohort:pd.DataFrame, hist:pd.DataFrame, official:list[str])->pd.DataFrame:
    """Return one row per R117 code/date, using strict official-session numpy indexes."""
    day_pos={d:i for i,d in enumerate(official)}
    # 1 pass to index exactly the frozen R117 cohort; never retrieve beyond original scope.
    queries=defaultdict(list)
    for r in cohort[['code','signal_date']].itertuples(index=False):queries[r.code].append(r.signal_date)
    rows=[]
    for code,chunk in hist.groupby('code',sort=False):
        if code not in queries:continue
        chunk=chunk.sort_values('sess')
        sess=chunk.sess.to_numpy(dtype=np.int64)
        dates=chunk.date.to_numpy(dtype=str)
        lows=chunk.low.to_numpy(dtype=float)
        highs=chunk.high.to_numpy(dtype=float)
        closes=chunk.close.to_numpy(dtype=float)
        lookup={int(x):i for i,x in enumerate(sess)}
        for d in queries.pop(code):
            t=day_pos[d]
            a={'prior45_complete':False,'origin_below224_asof':False,'broad_224_pullback_candidate':False,
               'wave_geometry_status':'NOT_EVALUATED','origin_date':'','peak_date':'',
               'origin_below224_pct':np.nan,'wave_rise_low_to_peak_pct':np.nan,
               'pullback_fraction':np.nan,'origin_to_peak_sessions':np.nan,
               'peak_to_d0_sessions':np.nan,'origin_ma224_contiguous':False,'code':code,'signal_date':d}
            i0=np.searchsorted(sess,t-45,side='left');i1=np.searchsorted(sess,t,side='left')
            if (i1-i0)!=45 or sess[i0]!=t-45 or sess[i1-1]!=t-1:
                a['wave_geometry_status']='PRIOR45_OFFICIAL_SESSION_GAP';rows.append(a);continue
            a['prior45_complete']=True
            org=i0+int(np.argmin(lows[i0:i1]));anchor=int(sess[org])
            # 224 trading sessions from origin-223 through origin inclusive, without missing bars
            m0=np.searchsorted(sess,anchor-223,side='left')
            if (org-m0+1)!=224 or sess[m0]!=anchor-223 or not np.isfinite(closes[m0:org+1]).all() or np.any(closes[m0:org+1]<=0):
                a['wave_geometry_status']='ORIGIN_224_MA_INSUFFICIENT_OR_GAP';rows.append(a);continue
            ma=float(closes[m0:org+1].mean());a['origin_ma224_contiguous']=True
            low=float(lows[org]);pct=float((low/ma-1)*100) if low>0 else np.nan
            a.update({'origin_date':str(dates[org]),'origin_below224_pct':pct,'origin_below224_asof':bool(low<ma)})
            if not np.isfinite(low) or low<=0:
                a['wave_geometry_status']='ORIGIN_BAD_LOW';rows.append(a);continue
            if org+1>=i1:
                a['wave_geometry_status']='NO_PRIOR_PEAK_AFTER_ORIGIN';rows.append(a);continue
            pk=org+1+int(np.argmax(highs[org+1:i1])); top=float(highs[pk]);
            a.update({'peak_date':str(dates[pk]),'origin_to_peak_sessions':int(sess[pk])-anchor,
                      'peak_to_d0_sessions':t-int(sess[pk])})
            if not np.isfinite(top) or top<=low:
                a['wave_geometry_status']='NO_UPWARD_GEOMETRY';rows.append(a);continue
            a['wave_rise_low_to_peak_pct']=(top/low-1)*100
            d0i=lookup.get(t)
            if d0i is None:
                a['wave_geometry_status']='D0_SOURCE_NOT_UNIQUE';rows.append(a);continue
            close=float(closes[d0i]);a['pullback_fraction']=(top-close)/(top-low)
            if not a['origin_below224_asof']:
                a['wave_geometry_status']='ORIGIN_NOT_BELOW224';rows.append(a);continue
            if not (low<close<top):
                a['wave_geometry_status']='D0_NOT_BETWEEN_ORIGIN_AND_PEAK';rows.append(a);continue
            a['broad_224_pullback_candidate']=True
            a['wave_geometry_status']='BROAD_LOW_PEAK_PULLBACK_PROXY_ONLY';rows.append(a)
    require(not queries,'GEOMETRY_SUBSET_MISSING_CODES')
    q=pd.DataFrame(rows)
    require(len(q)==len(cohort),'GEOMETRY_MISSING_SUBSET_MEMBER')
    require(not q.duplicated(['code','signal_date']).any(),'GEOMETRY_DUPLICATE')
    return q

def parse_bool(s):
    if pd.isna(s):return False
    return str(s).lower() in ('true','1','yes')

def build_outputs(cohort,scope,geo,raw_agg=None,opportunity_agg=None):
    out=cohort.merge(geo,on=['code','signal_date'],how='left',validate='one_to_one')
    require(out.wave_geometry_status.notna().all(),'WAVE_GEOMETRY_JOIN_MISSING')
    if raw_agg is not None:
        out=out.merge(raw_agg,on=['code','signal_date'],how='left',validate='one_to_one')
        out['v4976_raw_predicate_hit']=out.v4976_raw_predicate_hit.eq(True)
        # Top5 selection MUST be a subset of raw predicate hit.
        require((~out.selected_any_mode.astype(bool)|out.v4976_raw_predicate_hit).all(),
                'ORIGINAL_TOP5_NOT_IN_RAW_PREDICATES')
    else:out['v4976_raw_predicate_hit']=False
    if opportunity_agg is not None:
        out=out.merge(opportunity_agg,on=['code','signal_date'],how='left',validate='one_to_one')
        out['v4976_opportunity_census_present']=out.v4976_opportunity_census_present.eq(True)
    else:out['v4976_opportunity_census_present']=False
    out['selected_any_mode']=out.selected_any_mode.map(parse_bool)
    out['funnel_observed_stage']=np.select([
        out.selected_any_mode,
        out.v4976_raw_predicate_hit,
        out.v4976_opportunity_census_present],
        ['TOP5_SELECTED','RAW_PREDICATE_HIT_NOT_TOP5','OPPORTUNITY_CENSUS_ONLY_NOT_RAW'],
        default='NO_RECORDED_RAW_HIT_WITHIN_907_SOURCE')
    out['outcome_eligible']=out.outcome_eligible.map(parse_bool)
    out['asof_quality_gate']=out.asof_quality_gate.map(parse_bool)
    out['broad_224_pullback_candidate']=out.broad_224_pullback_candidate.map(parse_bool)
    out['broad_research_eligible']=out.broad_224_pullback_candidate & out.asof_quality_gate
    # Repeated daily matches are not independent new opportunities.
    # Count an episode only on the first observed consecutive official session.
    official_date_index={d:i for i,d in enumerate(sorted(out.signal_date.unique()))}
    out['_day_index']=out.signal_date.map(official_date_index)
    out=out.sort_values(['code','_day_index'])
    before=out.groupby('code')['broad_research_eligible'].shift(1).eq(True)
    prev_index=out.groupby('code')['_day_index'].shift(1)
    contiguous=out['_day_index'].sub(prev_index).eq(1)
    out['broad_episode_start_within_window']=out.broad_research_eligible & (~before | ~contiguous)
    out['broad_episode_left_censored']=out.broad_episode_start_within_window & out._day_index.eq(0)
    out=out.drop(columns='_day_index')
    out['group']=np.select([
        out.broad_research_eligible&out.selected_any_mode,
        out.broad_research_eligible&~out.selected_any_mode,
        ~out.broad_research_eligible&out.selected_any_mode,
    ], ['BROAD_AND_V4976_SELECTED','BROAD_NOT_SELECTED','SELECTED_NOT_BROAD'],default='OTHER_COVERED')
    # Future returns are labels only. Never allow into D0 feature eligibility, ranking, or thresholds.
    evaled=out[out.outcome_eligible].copy()
    summaries=[]
    for grp,tab in out.groupby('group',sort=True):
        z=evaled[evaled.group.eq(grp)]
        summaries.append({'group':grp,'unique_code_days':len(tab),'unique_codes':tab.code.nunique(),
           'evaluable_D1open_D5close':len(z),
           'broad_episode_starts_in_observed_window':int(tab.broad_episode_start_within_window.sum()),
           'illustrative_net05_win_pct':float(z.illustrative_net_win.astype(float).mean()*100) if len(z) else np.nan,
           'illustrative_net05_mean_pct':float(z.illustrative_net_05pct.mean()) if len(z) else np.nan,
           'illustrative_net05_median_pct':float(z.illustrative_net_05pct.median()) if len(z) else np.nan,
           'research_warning':'EXPOST_STORED_907_NAMES_NOT_PIT_FULL_MARKET; NO_TRADE_CLAIM'})
    summary=pd.DataFrame(summaries)
    # Explicitly distinguish 'raw hit but not selected' from 'not a recorded raw hit'.
    funnel=out.groupby(['group','funnel_observed_stage'],as_index=False).agg(
        code_days=('code','size'),
        future_outcome_evaluable=('outcome_eligible','sum'),
        unique_codes=('code','nunique'))
    scope=scope.copy()
    scope['missing_unobserved_full_market_count']=scope.full_market_krx_source_rows-scope.stored_krx_source_rows
    require((scope.missing_unobserved_full_market_count>0).all(),'NO_SCOPE_GAP_UNEXPECTED')
    hit=out.groupby('signal_date').agg(
        covered=('code','size'), broad_raw=('broad_224_pullback_candidate','sum'),
        broad_research=('broad_research_eligible','sum'),v4976_selected=('selected_any_mode','sum'),
        broad_not_v4976_selected=('group',lambda s:int((s=='BROAD_NOT_SELECTED').sum())),
        origin_224_unknown=('wave_geometry_status',lambda s:int((s=='ORIGIN_224_MA_INSUFFICIENT_OR_GAP').sum())),
        prior45_gap=('wave_geometry_status',lambda s:int((s=='PRIOR45_OFFICIAL_SESSION_GAP').sum())),
    ).reset_index()
    scope=scope.merge(hit,on='signal_date',how='left',validate='one_to_one')
    # Daily date matching is descriptive. The source 907 names are ex-post; NOT a matched PIT control.
    comp=evaled.groupby(['signal_date','group'],as_index=False).agg(
        n=('code','size'), net05_mean=('illustrative_net_05pct','mean'),
        win05_pct=('illustrative_net_win',lambda s:float(s.astype(float).mean()*100)))
    # Hindsight casebook for human review only, no retroactive gate tuning.
    case=out[out.group.eq('BROAD_NOT_SELECTED')].copy()
    case=case[case.outcome_eligible].copy()
    case['posthoc_outcome_bucket']=np.where(case.illustrative_net_05pct>0,'HINDSIGHT_POSITIVE','HINDSIGHT_NONPOSITIVE')
    order=['signal_date','code','origin_date','peak_date','origin_below224_pct','wave_rise_low_to_peak_pct',
           'pullback_fraction','vol20_to_prev20','vol5_to_prev20','ma224_asof','close_vs_ma224_pct',
           'illustrative_net_05pct','posthoc_outcome_bucket','wave_geometry_status']
    case=case[order].sort_values(['signal_date','code'])
    return out,summary,scope,comp,case,funnel

def save(args, co, summary, scope, matched, case, funnel, src_manifest, official,shards_info):
    args.out.mkdir(parents=True,exist_ok=True)
    files={
      'r118_40305_broad_224_wave_geometry.csv':co,
      'r118_broad_vs_v4976_descriptive.csv':summary,
      'r118_45day_true_market_gap_and_covered_wave.csv':scope,
      'r118_date_stratified_group_descriptive.csv':matched,
      'r118_unselected_broad_geometry_hindsight_casebook.csv':case,
      'r118_broad_x_v4976_raw_funnel.csv':funnel,
    }
    hashes={}
    for name,df in files.items():
        path=args.out/name;df.to_csv(path,index=False,encoding='utf-8-sig');hashes[name]=digest(path.read_bytes())
    statuscounts={str(x):int(y) for x,y in co.wave_geometry_status.value_counts().items()}
    m={
      'version':VERSION,'status':'PASS_FULL_MARKET_GAP_AND_BROAD_WAVE_COVERED_COHORT_ONLY_NOT_FULL_PIT_RECALL',
      'research_only':True,'never_live_or_orders':True,
      'source_r117_run':R117_RUN,'source_r171_run':R171_RUN,
      'r117_source_manifest_version':src_manifest['version'],
      'krx_daily_sha_days_verified':len(official),'stored_code_day_count':len(co),
      'source_date_count':int(co.signal_date.nunique()),
      'source_selected_code_days':int(co.selected_any_mode.sum()),
      'true_market_rows_sum_45days':int(scope.full_market_krx_source_rows.sum()),
      'stored_rows_sum_45days':int(scope.stored_krx_source_rows.sum()),
      'unobserved_rows_sum_45days':int(scope.missing_unobserved_full_market_count.sum()),
      'broad_proxy_raw_count':int(co.broad_224_pullback_candidate.sum()),
      'broad_proxy_research_eligible_count':int(co.broad_research_eligible.sum()),
      'broad_proxy_outside_v4976_selected_count':int((co.group=='BROAD_NOT_SELECTED').sum()),
      'broad_proxy_outside_v4976_d5_evaluable':len(case),
      'broad_episode_starts_in_observed_window':int(co.broad_episode_start_within_window.sum()),
      'broad_episode_left_censored':int(co.broad_episode_left_censored.sum()),
      'wave_geometry_status':statuscounts,
      'v4976_original_shard_verification':shards_info,
      'file_sha256':hashes,
      'limitations':{
       'true_historical_whole_market_eligible_denominator':'UNKNOWN',
       'full_market_code_identifiers_in_this_archive':'NOT_STORED',
       'v49_76_eligibility_for_unobserved_stocks':'UNKNOWN',
       'stored_907_names':'EXPOST_SELECTED_NOT_PIT',
       'v49_76_raw_predicate_hits':'RECOVERED_8_SHA_PINNED_SHARDS_MEMBERSHIP_ONLY',
       'v49_76_gate_reason_per_nonraw':'UNKNOWN_NO_PROOF_FROM_ABSENCE',
       'opportunity_census':'FUTURE_OUTCOME_INFORMED_NON_EXHAUSTIVE_USE_ONLY_FOR_TRACE',
       'technical_false_negative_count':'NO_CONFIRMED_FROM_THESE_EXAMINED_CENSUS_RECORDS',
       'broad_geometry_label':'NOT_HUMAN_VERIFIED_FIRST_WAVE',
       'prior45_origin_low':'STRICT_45_PRIOR_MARKET_SESSIONS',
       'origin_ma224':'STRICT_224_OFFICIAL_SESSIONS_THROUGH_ORIGIN',
       'selected_nonselected':'NO_PROOF_WHETHER_UNSELECTED_FAILED_FILTER_OR_RANK',
       '15_03_inference_from_d0_close':'FORBIDDEN',
       'corporate_actions':'ASOF_35PCT_JUMP_SUSPECT_GATE_ONLY_NOT_INDEPENDENTLY_ADJUSTED',
       'returns':'D1_OPEN_TO_D5_CLOSE_OUTCOME_ONLY_COST_ASSUMPTION_0.5PCT',
       'genuine_trading_signal_or_claim':'NOT_APPROVED',
       'episode_repeated_days':'DEDUPED_WITHIN_45_SIGNAL_DAYS_FIRST_DATE_LEFT_CENSORED'},
      'decision':'NO_PRODUCTION_MODIFICATION; ACQUIRE_VERIFIED_PIT_FULL_MARKET_AND_RUN_INDEPENDENT_SHADOW_LANE'
    }
    (args.out/'r118_integrity_manifest.json').write_text(json.dumps(m,ensure_ascii=False,indent=2),encoding='utf-8')
    return m

def main(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument('--r117-root',required=True,type=Path)
    p.add_argument('--history-dir',required=True,type=Path)
    p.add_argument('--out',required=True,type=Path)
    p.add_argument('--original-shards-dir',required=True,type=Path)
    args=p.parse_args(argv)
    cohort,scope,man=read_r117(args.r117_root)
    hist,official,seen=read_immutable_krx(args.history_dir)
    require(len(official)==1907,'OFFICIAL_MARKET_DAYS_NOT_1907')
    for _,r in scope.iterrows():
        d=r.signal_date
        require(d in seen,'R117_DAY_NOT_IN_IMMUTABLE_KRX')
        require(seen[d]==(int(r.stored_krx_source_rows),int(r.full_market_krx_source_rows)),
                'R117_VS_KRX_DENOM_DRIFT '+d)
    raw,opp,shards_info=read_v4976_raw_and_opportunity_shards(args.original_shards_dir)
    geo=compute_geometry(cohort,hist,official)
    out,summary,scope2,daily,case,funnel=build_outputs(cohort,scope,geo,raw,opp)
    # Original raw rows are only for codes/dates inside the frozen 907 cohort.
    require(int(out.v4976_raw_predicate_hit.sum())==shards_info['raw_unique_code_days'],
            'R118_RAW_POPULATION_MISSING_FROM_COHORT')
    m=save(args,out,summary,scope2,daily,case,funnel,man,official,shards_info)
    print('R118_RESEARCH_PASS',json.dumps({x:m[x] for x in ['status','krx_daily_sha_days_verified','stored_code_day_count','true_market_rows_sum_45days','unobserved_rows_sum_45days','broad_proxy_research_eligible_count','broad_proxy_outside_v4976_selected_count','broad_proxy_outside_v4976_d5_evaluable']},ensure_ascii=False))
    return m

if __name__=='__main__':main()
