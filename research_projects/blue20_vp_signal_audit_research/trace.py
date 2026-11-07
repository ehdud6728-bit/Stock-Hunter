"""Auditable raw rows and nonzero per-bar allocations, no trading outcome."""
from pathlib import Path
import argparse,json,hashlib
import numpy as np
import pandas as pd
from analyze import HERE,CASES,loader
from profile_core import METHODS,snapshot,edges_for,matrix,query_band,partial_rebase,features,profile

def main(manifest):
    frames=loader.load_prices(Path(manifest));queries=json.loads((HERE/'reference/HTS_SETTINGS.json').read_text(encoding='utf-8'));actions=json.loads((HERE/'reference/PARTIAL_ACTIONS.json').read_text(encoding='utf-8'))
    raw=[];alloc=[];qrows=[];rebase=[];hashes=[]
    for (code,date),name in CASES.items():
        d=frames[code];j=int(d.index[d.date.eq(date)][0])
        for n in [60,120,240]:
            for kind in ['PAST_ONLY','SIGNAL_CLOSE']:
                w=snapshot(d,j,n,kind);ident=dict(code=code,signal_date=date,name=name,window=n,snapshot=kind,profile_cutoff=w.date.iloc[-1])
                normalized=w[['code','date','open','high','low','close','volume','amount']].to_csv(index=False,lineterminator='\n').encode('utf-8-sig')
                hashes.append({**ident,'input_sha256':hashlib.sha256(normalized).hexdigest(),'rows':len(w),'total_volume':w.volume.sum()})
                for b in w.itertuples(index=False):raw.append({**ident,'bar_date':b.date,'open':b.open,'high':b.high,'low':b.low,'close':b.close,'volume':b.volume,'amount':b.amount})
                if n!=120 or not any(q['code']==code and q['signal_date']==date for q in queries):continue
                edges=edges_for(w,32)
                for method in METHODS:
                    a=matrix(w,edges,method)
                    for i,k in np.argwhere(a>0):
                        b=w.iloc[i];estimated=a[i,k]*b.volume
                        alloc.append({**ident,'method':method,'bar_date':b.date,'bar_volume':b.volume,'bin_index':int(k),'bin_low':edges[k],'bin_high':edges[k+1],'allocated_fraction':a[i,k],'estimated_bin_volume':estimated,'window_volume':w.volume.sum()})
                    for q in queries:
                        if q['code']!=code or q['signal_date']!=date:continue
                        for _,b in w.iterrows():
                            mass=query_band(pd.DataFrame([b]),q['band_low'],q['band_high'],method)
                            qrows.append({**ident,'method':method,'bar_date':b.date,'bar_volume':b.volume,'band_low':q['band_low'],'band_high':q['band_high'],'estimated_band_volume':mass,'window_volume':w.volume.sum(),'window_pct_contribution':100*mass/w.volume.sum()})
                if code=='028300':
                    for quantity in ['RAW','INVERSE_PRICE_SYNTHETIC']:
                        adjusted,used=partial_rebase(w,date,actions,quantity)
                        for q in queries:
                            if q['code']!=code or q['signal_date']!=date:continue
                            for method in METHODS:
                                mass=query_band(adjusted,q['band_low'],q['band_high'],method)
                                rebase.append({**ident,'method':method,'quantity_scenario':quantity,'band_low':q['band_low'],'band_high':q['band_high'],'estimated_pct':100*mass/adjusted.volume.sum(),'basis_status':'PARTIAL_SCENARIO_NOT_CERTIFIED','actions':'|'.join(used)})
    out=HERE/'results'
    for name,data in [('RAW_CASE_SNAPSHOTS',raw),('CASE_BAR_BIN_ALLOCATIONS',alloc),('HTS_BAND_BAR_CONTRIBUTIONS',qrows),('PARTIAL_REBASE_BAND_QUERIES',rebase),('CASE_INPUT_HASHES',hashes)]:pd.DataFrame(data).to_csv(out/(name+'.csv.gz'),index=False)
    for _,g in pd.DataFrame(alloc).groupby(['code','signal_date','snapshot','method','bar_date']):assert np.isclose(g.estimated_bin_volume.sum(),g.bar_volume.iloc[0],rtol=1e-10,atol=1e-7)
    print(json.dumps({'raw_case_rows':len(raw),'nonzero_bar_bin_rows':len(alloc),'fixed_band_bar_rows':len(qrows),'all_bar_allocations_conserved':True}))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--manifest',required=True);main(p.parse_args().manifest)
