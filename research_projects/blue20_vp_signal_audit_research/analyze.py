from pathlib import Path
import argparse,hashlib,json,importlib.util,time
import numpy as np
import pandas as pd
from profile_core import METHODS,snapshot,edges_for,extend_edges,profile,features,query_band,partial_rebase
HERE=Path(__file__).resolve().parent
def module(name,path):
    spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
loader=module('verified_loader',HERE/'vendor/loader.py');structure=module('prior_structure',HERE/'vendor/structure.py');legacy=module('legacy_profile',HERE/'vendor/legacy_profile.py')
CASES={('028300','2023-02-20'):'HLB',('145990','2023-05-08'):'삼양사',('383220','2024-07-17'):'F&F',('180640','2023-09-14'):'한진칼',('006220','2024-01-24'):'제주은행',('222080','2024-03-05'):'씨아이에스',('038500','2024-06-05'):'삼표시멘트',('034120','2023-02-07'):'SBS',('028300','2023-03-07'):'HLB 후속'}

def load_inputs(manifest):
    refs=HERE/'reference'
    for name,digest in json.loads((refs/'SHA256.json').read_text()).items():assert hashlib.sha256((refs/name).read_bytes()).hexdigest()==digest,name
    frames=loader.load_prices(Path(manifest));frozen=json.loads((refs/'FROZEN.json').read_text(encoding='utf-8'))
    assert set(frames)==set(frozen['cohort_codes'])
    for code,digest in frozen['event_stock_prices'].items():assert hashlib.sha256(frames[code].to_csv(index=False,lineterminator='\n').encode('utf-8-sig')).hexdigest()==digest,code
    events=pd.read_csv(refs/'BASELINE.csv.gz',dtype={'code':str})
    assert len(events)==1804 and not events.duplicated(['code','date','band']).any()
    for d in frames.values():
        prev=d.close.shift();d['prior_atr20']=pd.concat([d.high-d.low,(d.high-prev).abs(),(d.low-prev).abs()],axis=1).max(axis=1).rolling(20).mean().shift()
    return frames,events

def bin_records(ident,w,edges,weights,method,bins,kind):
    return [{**ident,'snapshot':kind,'window':len(w),'bins_requested':bins,'method':method,'bin_index':i,'low':edges[i],'high':edges[i+1],
             'estimated_volume':v,'total_volume':w.volume.sum(),'estimated_pct':100*v/w.volume.sum()} for i,v in enumerate(weights)]

def main(args):
    out=HERE/'results';out.mkdir(exist_ok=True);frames,events=load_inputs(args.manifest)
    queries=json.loads((HERE/'reference/HTS_SETTINGS.json').read_text(encoding='utf-8'));actions=json.loads((HERE/'reference/PARTIAL_ACTIONS.json').read_text(encoding='utf-8'))
    rows=[];bins_detail=[];components=[];fixedqueries=[];zone_rows=[];ca_rows=[];legacy_count=0;largest_mass_error=0.;started=time.time()
    unique=events[['code','date']].drop_duplicates().reset_index(drop=True)
    identity=events.groupby(['code','date'],sort=False).band.apply(lambda x:'|'.join(sorted(x))).to_dict()
    for num,e in enumerate(unique.itertuples(index=False)):
        d=frames[e.code];j=int(d.index[d.date.eq(e.date)][0]);bar=d.iloc[j];ident=dict(code=e.code,date=e.date,year=int(e.date[:4]),bands=identity[(e.code,e.date)])
        case=(e.code,e.date) in CASES
        for n in [60,120,240]:
            old=snapshot(d,j,n,'PAST_ONLY');new=snapshot(d,j,n,'SIGNAL_CLOSE')
            for kind,w in [('PAST_ONLY',old),('SIGNAL_CLOSE',new)]:
                last=w.date.iloc[-1];assert last<e.date if kind=='PAST_ONLY' else last==e.date
                for bins in [16,32,64]:
                    edges=edges_for(w,bins)
                    for method in METHODS:
                        weights=profile(w,edges,method);feat=features(weights,edges,bar.close)
                        mass_error=float(weights.sum()-w.volume.sum());largest_mass_error=max(largest_mass_error,abs(mass_error))
                        record={**ident,'snapshot':kind,'window':n,'bins':bins,'method':method,'source_start':w.date.iloc[0],'source_end':last,
                                'known_at':last+' EOD_ASSUMED','publication_time_verified':False,'next_observed_bar_date':d.date.iloc[j+1],'total_volume':float(w.volume.sum()),'mass_error':mass_error,
                                'signal_volume_pct':100*bar.volume/w.volume.sum() if kind=='SIGNAL_CLOSE' else 0.,'signal_close':bar.close,
                                'data_basis':'RAW_ARCHIVE_UNVERIFIED_ACTIONS','actual_tick_volume':'UNVERIFIED',**feat}
                        rows.append(record)
                        if kind=='PAST_ONLY' and method in ['range','typical']:
                            previous=legacy.volume_profile(w,bins,method)
                            assert previous is not None
                            for field in ['poc','vah','val']:assert np.isclose(feat[field],previous[field],rtol=0,atol=1e-7),(ident,field)
                            np.testing.assert_allclose(weights,previous['weights'],rtol=1e-10,atol=1e-6);legacy_count+=1
                        if case:bins_detail+=bin_records(ident,w,edges,weights,method,bins,kind)
            # Frozen-origin, frozen-width decomposition; range/32 is the primary diagnostic.
            base=edges_for(old,32);stages=[('OLD_N',old),('REMOVE_OLDEST',old.iloc[1:]),('ADD_D0',new),('REBIN_NEW_N',new)]
            for stage,w in stages:
                edges=edges_for(w,32) if stage=='REBIN_NEW_N' else extend_edges(base,w)
                weights=profile(w,edges,'range');components.append({**ident,'window':n,'stage':stage,'source_start':w.date.iloc[0],'source_end':w.date.iloc[-1],
                    'observed_bars':len(w),'total_volume':float(w.volume.sum()),'actual_bins':len(weights),'grid_width':float(edges[1]-edges[0]),
                    **features(weights,edges,bar.close)})
            if case:
                zs,_=structure.zones(d.iloc[:j],n)
                for z in zs:
                    pos='ABOVE_CLOSE' if z['low']>bar.close else 'BELOW_CLOSE' if z['high']<bar.close else 'CLOSE_INSIDE'
                    zone_rows.append({**ident,'window':n,'low':z['low'],'high':z['high'],'tests':z['count'],'median_drop_atr':z['median_drop_atr'],
                        'position':pos,'progress_atr':(bar.close-z['high'])/bar.prior_atr20,'known_by':'D-1','test_dates':'|'.join(t['date'] for t in z['tests'])})
                for kind,w in [('PAST_ONLY',old),('SIGNAL_CLOSE',new)]:
                    for q in queries:
                        if q['code']!=e.code or q['signal_date']!=e.date:continue
                        for method in METHODS:
                            mass=query_band(w,q['band_low'],q['band_high'],method)
                            fixedqueries.append({**ident,'window':n,'snapshot':kind,'method':method,'band_low':q['band_low'],'band_high':q['band_high'],
                                'query_basis':q['range_source'],'source_start':w.date.iloc[0],'source_end':w.date.iloc[-1],'estimated_volume':mass,
                                'total_volume':float(w.volume.sum()),'estimated_pct':100*mass/w.volume.sum(),'screen_pct':q['displayed_pct'],
                                'hts_settings_status':q['settings_status']})
                    if e.code=='028300':
                        for qty in ['RAW','INVERSE_PRICE_SYNTHETIC']:
                            adjusted,applied=partial_rebase(w,e.date,actions,qty);ed=edges_for(adjusted,32)
                            ww=profile(adjusted,ed,'range');ca_rows.append({**ident,'window':n,'snapshot':kind,'quantity_scenario':qty,
                                'basis':'PARTIAL_REFERENCE_SCENARIO_NOT_CERTIFIED','applied_actions':'|'.join(applied),
                                'total_volume':float(adjusted.volume.sum()),**features(ww,ed,bar.close)})
        if (num+1)%200==0:print(json.dumps({'unique_completed':num+1,'seconds':round(time.time()-started,1)}),flush=True)
    for name,data in [('SNAPSHOTS',rows),('CASE_BIN_WEIGHTS',bins_detail),('DECOMPOSITION',components),('HTS_BAND_QUERIES',fixedqueries),('ALL_PRICE_RESISTANCE_ZONES',zone_rows),('PARTIAL_ACTION_SCENARIOS',ca_rows)]:
        pd.DataFrame(data).to_csv(out/(name+'.csv.gz'),index=False)
    snapshots=pd.DataFrame(rows)
    # Restore the exact 1,804 opportunity identities separately; 1,803 unique charts avoid double-counting.
    events[['code','date','band','year']].to_csv(out/'EVENT_IDENTITIES.csv',index=False)
    keys=['code','date','window','bins','method'];a=snapshots[snapshots.snapshot.eq('PAST_ONLY')];b=snapshots[snapshots.snapshot.eq('SIGNAL_CLOSE')]
    paired=a.merge(b,on=keys,suffixes=('_past','_signal'),validate='one_to_one')
    paired['poc_delta_pct']=100*(paired.poc_signal/paired.poc_past-1);paired['peak_position_changed']=paired.peak_position_signal!=paired.peak_position_past
    summaries=[]
    for key,g in paired.groupby(['window','bins','method','year_signal']):
        summaries.append(dict(zip(['window','bins','method','year'],key),unique_signals=len(g),median_abs_poc_delta_pct=g.poc_delta_pct.abs().median(),
            pct_poc_move_gt1=100*g.poc_delta_pct.abs().gt(1).mean(),peak_position_change_pct=100*g.peak_position_changed.mean(),
            median_signal_volume_pct=g.signal_volume_pct_signal.median(),mean_signal_volume_pct=g.signal_volume_pct_signal.mean()))
    pd.DataFrame(summaries).to_csv(out/'SNAPSHOT_DIFFERENCES.csv',index=False)
    gaps=[]
    for code,d in frames.items():
        ratio=d.open/d.close.shift()-1
        for i in np.flatnonzero(ratio.abs().gt(.35).to_numpy()):gaps.append(dict(code=code,date=d.date.iloc[i],gap_pct=100*ratio.iloc[i],status='UNVERIFIED_DISCONTINUITY'))
    pd.DataFrame(gaps).to_csv(out/'SOURCE_DISCONTINUITIES.csv',index=False)
    status=dict(events=len(events),unique_signals=len(unique),parameter_rows=len(rows),legacy_profiles_reproduced=legacy_count,manifest_hashes=500,normalized_price_hashes=481,
        max_volume_residual=largest_mass_error,hts_exact_reproduction='MISSING_SETTINGS',price_adjustment='PARTIALLY_EXPLAINED_NOT_CERTIFIED',
        real_price_by_price_volume='UNVERIFIED',future_rows_used=0,performance_optimization='NOT_PERFORMED')
    (out/'STATUS.json').write_text(json.dumps(status,indent=2),encoding='utf-8');print(json.dumps(status),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--manifest',required=True);main(p.parse_args())
