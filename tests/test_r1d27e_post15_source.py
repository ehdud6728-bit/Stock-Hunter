"""Offline smoke test. Does not call provider or place orders."""
from __future__ import annotations
import importlib.util, pathlib,sys,tempfile
import pandas as pd
from research_formulas import stockhunter_r1d27e_post15_flow as q

def fake_provider(side,start,end,code):
    # All business days expected for the test event, constant balanced gross/net.
    dates=fake_provider.days
    gross_b={'기관합계':1200,'외국인합계':1100,'개인':900,'기타법인':1000}
    gross_s={'기관합계':1000,'외국인합계':1000,'개인':1200,'기타법인':1000}
    vals={'매수':gross_b,'매도':gross_s,'순매수':{k:gross_b[k]-gross_s[k] for k in gross_b}}[side]
    return pd.DataFrame([{**vals,'전체':4200 if side!='순매수' else 0} for d in dates],index=pd.to_datetime(dates))

def test_post15_parse_and_audit():
    d=['2024-01-%02d'%i for i in range(2,17)]
    fake_provider.days=d
    got=q.fetch_three(d,'005930',fetch=fake_provider,cooldown=0)
    assert len(got)==60
    assert sum(x['net_krw'] for x in got if x['actor'] in ('기관합계','외국인합계'))==4500
    def missing(side,start,end,code):return pd.DataFrame()
    try:q.fetch_three(d,'005930',fetch=missing,cooldown=0)
    except Exception:pass
    else:raise AssertionError('empty provider must fail closed')

def test_frozen_input(base_dir,freeze_file):
    events,prices,m=q.read_base(base_dir,freeze_file)
    assert len(events)==1500 and len(prices)==22500
    assert all(prices.groupby(q.KEY).size()==15)

if __name__=='__main__':
    test_post15_parse_and_audit()
    test_frozen_input('research_inputs','research_inputs/R1D27D_FROZEN_1500_TICKER_DISJOINT_OUTCOME_BLIND.csv')
    print('ALL_SMOKE_TESTS_PASS')
