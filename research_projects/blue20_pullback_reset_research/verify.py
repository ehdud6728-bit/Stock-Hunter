from pathlib import Path
import hashlib,json,re
from xml.etree import ElementTree as ET
import numpy as np
import pandas as pd
HERE=Path(__file__).resolve().parent
def main():
    out=HERE/'results';o=pd.read_csv(out/'OPPORTUNITIES.csv.gz',dtype={'code':str});t=pd.read_csv(out/'TRADES.csv.gz',dtype={'code':str});c=pd.read_csv(out/'CONFIRMATIONS.csv.gz',dtype={'code':str});z=pd.read_csv(out/'FROZEN_ZONES.csv.gz',dtype={'code':str})
    assert o.universe.eq('BASELINE_1804').sum()==1804 and o.universe.eq('ALL_DAYS_P1').sum()==2178
    assert not o.opportunity_id.duplicated().any() and not t.duplicated(['opportunity_id','policy','stop_mode','horizon']).any()
    assert z.known_at.lt(z.date).all();assert c.low_date.le(c.confirm_date).all() and c.confirm_date.ge(c.date).all() and c.confirmation_age.le(40).all()
    assert t.loc[t.entered,'entry_date'].gt(t.loc[t.entered,'confirm_date']).all()
    assert t.closed.eq(t.status.eq('CLOSED')).all()
    for mode in ['PULLBACK_LOW','LOW_MINUS_025ATR']:
        a=t[t.stop_mode.eq(mode)&t.entered];expected=a.stop_low-(.25*a.atr if mode=='LOW_MINUS_025ATR' else 0)
        np.testing.assert_allclose(a.stop_price,expected,rtol=0,atol=1e-7)
    known_no_entry=t.status.isin(['NO_ENTRY_SIGNAL','INVALID_STOP_AT_ENTRY','ENTRY_AFTER_DEADLINE']);assert t.loc[known_no_entry,'opportunity_net_pct'].eq(0).all()
    unknown=t.status.isin(['RIGHT_CENSORED','MISSING_ENTRY_BAR','MISSING_HOLDING_BAR','MISSING_BAR']);assert t.loc[unknown,'opportunity_net_pct'].isna().all()
    base=pd.read_csv(HERE/'reference/BASELINE.csv.gz',dtype={'code':str});r=pd.read_csv(out/'BASELINE_REPLAY.csv.gz',dtype={'code':str});a=r.merge(base,on=['code','date','band'],validate='one_to_one',suffixes=('','_reference'));assert len(a)==1804
    np.testing.assert_allclose(a.net_pct,a.baseline_net_pct,rtol=0,atol=1e-8);assert a.exit_date.eq(a.baseline_exit_date).all()
    old=pd.read_csv(HERE/'reference/EARLY_TRADES.csv.gz',dtype={'code':str});old=old[old.stage.eq('P1')]
    first=t[t.universe.eq('ALL_DAYS_P1')&t.policy.eq('A_FIRST')&t.horizon.eq('FRESH20')]
    a=first.merge(old,on=['code','date'],validate='one_to_one',suffixes=('','_old'));assert len(a)==2178 and a.status.eq(a.trade_status).all()
    closed=a[a.closed];np.testing.assert_allclose(closed.net_pct,closed.net_pct_old,rtol=0,atol=1e-8);assert closed.exit_date.eq(closed.exit_date_old).all()
    retry=pd.read_csv(out/'RETRY_MAX2.csv.gz',dtype={'code':str});assert retry.attempts.le(2).all();np.testing.assert_allclose(retry.total_cost_pct_points,.25*retry.roundtrips)
    used=retry[retry.retry_confirm_date.notna()];assert used.retry_low_date.le(used.retry_confirm_date).all()
    trace=pd.read_csv(out/'WAIT_TRACE.csv.gz',dtype={'code':str});assert trace.observed_date.gt(trace.date).all()
    cond=pd.read_csv(out/'CONDITIONAL_POLICIES.csv.gz',dtype={'code':str});assert not cond.duplicated(['opportunity_id','switch_rule','followup_policy','followup_stop']).any()
    for _,g in cond.groupby(['switch_rule','followup_policy','followup_stop']):assert len(g)==len(o)
    # Every no-wait conditional branch must equal the initial A price outcome.
    a=cond[~cond.wait_for_resistance&~cond.status.eq('UNKNOWN_INITIAL_CONTEXT')].merge(t[t.policy.eq('A_FIRST')&t.horizon.eq('FRESH20')][['opportunity_id','opportunity_net_pct']],on='opportunity_id',suffixes=('','_first'),validate='many_to_one')
    np.testing.assert_allclose(a.opportunity_net_pct,a.opportunity_net_pct_first,equal_nan=True,rtol=0,atol=1e-8)
    protected=json.loads((HERE/'reference/LOCAL_PROTECTED.json').read_text());checks={}
    for name,digest in protected.items():
        p=HERE.parent/name
        if p.exists():assert hashlib.sha256(p.read_bytes()).hexdigest()==digest;checks[name]='UNCHANGED'
        else:checks[name]='LOCAL_PATH_NOT_IN_CI_CHECKOUT_SEPARATE_REPOSITORY_GUARD_REQUIRED'
    html=(out/'GALLERY.html').read_text(encoding='utf-8');svgs=re.findall(r'<svg\b.*?</svg>',html,re.S)
    for v in svgs:ET.fromstring(v);assert not re.search(r'(?:x|y|height|width|cx|cy)="(?:nan|inf)',v)
    result=dict(status='PASS',baseline_replayed=1804,independent_P1_fixed5_replayed=2178,opportunities=len(o),trade_policy_rows=len(t),frozen_zone_cutoffs_verified=True,
        low_known_before_entry_verified=True,immutable_stop_formula_verified=True,missing_returns_not_zero=True,conditional_branch_and_denominators_verified=True,
        max_two_attempts_and_costs_verified=True,chart_structures_checked=len(svgs),protected_local_files=checks,real_volume_profile='UNVERIFIED',corporate_action_basis='UNVERIFIED')
    (out/'VERIFICATION.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(result,ensure_ascii=False))
    (out/'SHA256.json').write_text(json.dumps({p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in out.iterdir() if p.is_file() and p.name not in ['SHA256.json','VERIFY.log']},indent=2),encoding='utf-8')
if __name__=='__main__':main()
