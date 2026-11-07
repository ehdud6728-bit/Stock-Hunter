from pathlib import Path
import argparse,json,hashlib,subprocess,re
from xml.etree import ElementTree as ET
import pandas as pd
import numpy as np
HERE=Path(__file__).resolve().parent
def main():
    out=HERE/'results';s=pd.read_csv(out/'SIGNALS.csv.gz',dtype={'code':str});e=pd.read_csv(out/'EPISODES.csv.gz',dtype={'code':str});t=pd.read_csv(out/'TRADES.csv.gz',dtype={'code':str});d=pd.read_csv(out/'ALL_STOCK_DAYS.csv.gz',dtype={'code':str})
    assert not s.signal_id.duplicated().any();assert set(s.stage)=={'P0','P1','P2','P3'}
    assert len(d)==d.drop_duplicates(['code','date']).shape[0] and d.code.nunique()==500
    p0=s[s.stage.eq('P0')].merge(d[['code','date','p0_condition','p1_raw']],on=['code','date'],validate='one_to_one');assert p0.p0_condition.all() and not p0.p1_raw.any()
    assert d.date.between('2023-01-01','2024-12-31').all()
    grid=pd.read_csv(out/'FULL_STOCK_DAY_COVERAGE.csv.gz',dtype={'code':str});assert len(grid)==500*d.date.nunique()
    assert grid.loc[~grid.ohlcv_observed,'P1_signal'].isna().all() and grid.loc[~grid.p0_classifiable,'P0_signal'].isna().all()
    assert not t[t.entered].entry_date.le(t[t.entered].date).any()
    assert t.closed.eq(t.trade_status.eq('CLOSED')).all()
    for r in e[e.origin_stage.eq('P1')].itertuples():
        if pd.notna(r.p2_date):assert r.p2_date>r.p1_date
        if pd.notna(r.p3_date):assert r.p3_date>r.p2_date
    original=pd.read_csv(HERE/'reference/BASELINE.csv.gz',dtype={'code':str});replay=pd.read_csv(out/'BASELINE_REPLAY.csv.gz',dtype={'code':str})
    comp=original.merge(replay,on=['code','date','band'],validate='one_to_one');assert len(comp)==1804
    np.testing.assert_allclose(comp.baseline_net_pct,comp.net_pct,rtol=0,atol=1e-8);assert comp.baseline_exit_date.eq(comp.exit_date).all()
    op=pd.read_csv(out/'P1_SAME_OPPORTUNITIES.csv.gz',dtype={'code':str});assert op.groupby('opportunity_id').policy.nunique().eq(3).all();assert len(op)==3*len(e[e.origin_stage.eq('P1')])
    bp=pd.read_csv(out/'BASELINE_IDENTICAL_OPPORTUNITIES.csv.gz',dtype={'code':str});assert len(bp)==1804*3
    cash=op[op.trade_status.isin(['NO_STAGE_SIGNAL','ENTRY_AFTER_DEADLINE'])];assert cash.opportunity_net_cash.eq(0).all()
    unknown=op[op.trade_status.isin(['RIGHT_CENSORED','MISSING_ENTRY_BAR','MISSING_HOLDING_BAR'])];assert unknown.opportunity_net_cash.isna().all()
    for r in bp[bp.closed].itertuples():assert r.exit_date<=original[(original.code==r.code)&original.date.eq(r.date)&original.band.eq(r.band)].d20_date.iloc[0]
    evidence=pd.read_csv(out/'VOLUME_RESISTANCE_EVIDENCE.csv.gz',dtype={'code':str});assert len(evidence)==len(s) and evidence.actual_sell_inventory.eq('UNVERIFIED').all()
    svg=re.findall(r'<svg\b.*?</svg>',(out/'GALLERY.html').read_text(encoding='utf-8'),re.S)
    for v in svg:ET.fromstring(v);assert not re.search(r'(?:x|y|height|width|cx|cy)="(?:nan|inf)',v)
    protected=json.loads((HERE/'reference/LOCAL_PROTECTED_BEFORE.json').read_text())
    root=HERE.parent
    local_check={}
    for path,digest in protected.items():
        p=root/path
        if p.exists():assert hashlib.sha256(p.read_bytes()).hexdigest()==digest;local_check[path]='UNCHANGED'
        else:local_check[path]='NOT_PRESENT_IN_GITHUB_RUN_LOCAL_WORKSPACE_CHECK_NOT_APPLICABLE'
    result=dict(status='PASS',baseline_replayed=1804,all_stock_days=len(d),signals=len(s),p0_independent_of_baseline=True,next_open_timing_verified=True,
        common_opportunity_denominators_verified=True,unknown_returns_not_zero_filled=True,charts_structurally_checked=len(svg),local_protected=local_check,
        real_volume_profile='UNVERIFIED',price_adjustment='UNVERIFIED',video_formula='UNVERIFIED',github_execution='CHECK_ENVIRONMENT')
    (out/'VERIFICATION.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(result,ensure_ascii=False))
    (out/'SHA256.json').write_text(json.dumps({p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in out.iterdir() if p.is_file() and p.name not in ['SHA256.json','VERIFY.log']},indent=2),encoding='utf-8')

if __name__=='__main__':main()
