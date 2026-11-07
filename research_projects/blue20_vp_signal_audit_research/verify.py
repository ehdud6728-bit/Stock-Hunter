from pathlib import Path
import hashlib,json,re
from xml.etree import ElementTree as ET
import numpy as np
import pandas as pd
HERE=Path(__file__).resolve().parent;out=HERE/'results'
def main():
    status=json.loads((out/'STATUS.json').read_text());assert status['events']==1804 and status['unique_signals']==1803
    assert status['parameter_rows']==129816 and status['legacy_profiles_reproduced']==32454
    s=pd.read_csv(out/'SNAPSHOTS.csv.gz',dtype={'code':str});assert not s.duplicated(['code','date','snapshot','window','bins','method']).any()
    a=s[s.snapshot.eq('PAST_ONLY')];b=s[s.snapshot.eq('SIGNAL_CLOSE')]
    assert a.source_end.lt(a.date).all() and b.source_end.eq(b.date).all()
    assert s.next_observed_bar_date.gt(s.date).all()
    bins=pd.read_csv(out/'CASE_BIN_WEIGHTS.csv.gz',dtype={'code':str})
    for _,g in bins.groupby(['code','date','snapshot','window','bins_requested','method']):
        assert np.isclose(g.estimated_volume.sum(),g.total_volume.iloc[0],rtol=1e-10,atol=1e-6)
        assert np.isclose(g.estimated_pct.sum(),100,rtol=1e-10,atol=1e-8)
    de=pd.read_csv(out/'DECOMPOSITION.csv.gz',dtype={'code':str});last=de[de.stage.eq('REBIN_NEW_N')].merge(b[b.bins.eq(32)&b.method.eq('range')],on=['code','date','window'],validate='one_to_one',suffixes=('_stage','_snapshot'))
    for f in ['poc','vah','val','peak_pct','total_volume']:np.testing.assert_allclose(last[f+'_stage'],last[f+'_snapshot'],rtol=1e-10,atol=1e-7)
    raw=pd.read_csv(out/'RAW_CASE_SNAPSHOTS.csv.gz',dtype={'code':str})
    assert raw.bar_date.le(raw.profile_cutoff).all() and raw.profile_cutoff.le(raw.signal_date).all()
    contributions=pd.read_csv(out/'HTS_BAND_BAR_CONTRIBUTIONS.csv.gz',dtype={'code':str})
    keys=['code','signal_date','snapshot','method']
    totals=contributions.groupby(keys).estimated_band_volume.sum().reset_index().rename(columns={'signal_date':'date'})
    queries=pd.read_csv(out/'HTS_BAND_QUERIES.csv.gz',dtype={'code':str});queries=queries[queries.window.eq(120)]
    compare=totals.merge(queries,on=['code','date','snapshot','method'],validate='one_to_one')
    np.testing.assert_allclose(compare.estimated_band_volume,compare.estimated_volume,rtol=1e-10,atol=1e-7)
    html=(out/'GALLERY.html').read_text(encoding='utf-8');svgs=re.findall(r'<svg\b.*?</svg>',html,re.S);assert len(svgs)==18
    for svg in svgs:
        ET.fromstring(svg);assert not re.search(r'(?:x|y|height|width|cx|cy)="(?:nan|inf)',svg)
    check=dict(status='PASS',snapshots=len(s),legacy_profiles=status['legacy_profiles_reproduced'],case_volume_groups=int(bins.groupby(['code','date','snapshot','window','bins_requested','method']).ngroups),
        charts_structurally_checked=18,snapshot_cutoffs_verified=True,decomposition_final_identity_verified=True,raw_case_cutoffs_verified=True,bar_contributions_match_queries=True,
        hts_exact_match='MISSING_SETTINGS',real_tick_volume='UNVERIFIED',fully_adjusted_prices='UNVERIFIED')
    (out/'VERIFICATION.json').write_text(json.dumps(check,indent=2),encoding='utf-8')
    hashes={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in out.iterdir() if p.is_file() and p.name not in ['SHA256.json','VERIFY.log']}
    (out/'SHA256.json').write_text(json.dumps(hashes,indent=2),encoding='utf-8');print(json.dumps(check))
if __name__=='__main__':main()
