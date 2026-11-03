import importlib,sys,tempfile,unittest
from pathlib import Path
import numpy as np
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT),str(ROOT.parent/'r1d114')]
from atr_cluster_baseline import (raw_check,calc_atr,pivots,cluster_levels_original,
  cluster_levels_with_anchors,original_like_scan,candidate_snapshot,safe_scan_at,group_repeated_events)
from research_r1d115 import prior_r112_context, strict_r114_context,run,FILES


def mock_frame(n=145):
    t=np.arange(n,dtype=float)
    c=110+.35*np.sin(t/5)
    o=c-.35;h=c+2.0;l=c-2.0
    for ix in (107,121):
        if ix<n:h[ix]=120.0
    c[-1]=122;o[-1]=110;h[-1]=124;l[-1]=109
    return pd.DataFrame({'date':pd.bdate_range('2024-01-01',periods=n),
        'open':o,'high':h,'low':l,'close':c,'volume':np.full(n,100_000.),
        'amount':np.full(n,2e8)})


def fake_sources(raw,r112,r114):
    dates=raw.date.dt.strftime('%Y-%m-%d')
    rows=[]
    for t,date in enumerate(dates):
        for alg in ['PIVOT_ATR_PROXY','TS_BODY_WICK_PROXY','AUTOCHARTIST_PROXY','CONTACT_PENETRATION_PROXY']:
            for scale in ('SHORT','MIDDLE','LONG'):
                for kind in ('UPPER','LOWER'):
                    rows.append(dict(code='039840',name='디오',date=date,idx=t,algorithm=alg,
                      scale=scale,kind=kind,state='ACTIVE',active_level=120 if kind=='UPPER' else 100,
                      active_slope_pct_per_bar=-.05 if kind=='UPPER' else .05,active_line_id='X',
                      active_known_date=str(dates.iloc[max(0,t-2)])))
    Path(r112).mkdir(parents=True,exist_ok=True);Path(r114).mkdir(parents=True,exist_ok=True)
    pd.DataFrame(rows).to_csv(Path(r112)/FILES['r112'][1],index=False)
    pd.DataFrame([dict(code='039840',date=dates.iloc[-1],idx=len(raw)-1,
             event='BREAKOUT_UP',strict_eligible=True,strict_pattern='HORIZONTAL_BOX')]).to_csv(
             Path(r114)/FILES['r114'][1],index=False)


class BaseTests(unittest.TestCase):
    def setUp(self): self.d=mock_frame()
    def test_atr_positive(self):self.assertGreater(calc_atr(self.d),0)
    def test_atr_warmup(self):self.assertTrue(np.isnan(calc_atr(self.d.iloc[:10])))
    def test_bad_ohlc_rejected(self):
        d=self.d.copy();d.loc[10,'high']=d.loc[10,'close']-2
        with self.assertRaises(ValueError):raw_check(d)
    def test_unsorted_dates_rejected(self):
        d=self.d.iloc[::-1]
        with self.assertRaises(ValueError):raw_check(d)
    def test_findpeaks_independent(self):
        h,l=pivots(self.d.iloc[:-1],calc_atr(self.d.iloc[:-1]));
        self.assertTrue(107 in h and 121 in h)
    def test_baseline_cluster(self):
        a=cluster_levels_original(np.array([100,100.4,120,200]))
        self.assertEqual(len(a),1)
    def test_anchor_gap_filter(self):
        a=cluster_levels_with_anchors([100,100.1,100.2],[10,11,20])
        self.assertEqual(a[0]['touches'],2)
    def test_past_only_candidate(self):
        c,_=candidate_snapshot(self.d.iloc[:-1])
        self.assertTrue(any(v['kind']=='RESISTANCE' for v in c))
        self.assertTrue(all('2024-' in v['anchor_dates'] for v in c))
    def test_causal_breakout(self):
        e,_=safe_scan_at(self.d,len(self.d)-1)
        self.assertTrue(any(v['event']=='RESISTANCE_CLUSTER_BREAKOUT' for v in e))
        self.assertTrue(all(v['known_before_event'] for v in e))
    def test_original_reference_detects(self):
        sig=original_like_scan(self.d)
        self.assertTrue(any(n.startswith('box_breakout@') for n,_ in sig))
    def test_no_future_leak(self):
        t=len(self.d)-1;first,_=safe_scan_at(self.d,t)
        d=pd.concat([self.d, mock_frame(30)],ignore_index=True)
        d.loc[t+1:,'close']=1e5;d.loc[t+1:,'high']=1e5+10;d.loc[t+1:,'low']=1e5-10
        second,_=safe_scan_at(d,t)
        self.assertEqual(first,second)
    def test_no_same_day_reference(self):
        e,_=safe_scan_at(self.d,len(self.d)-1)
        if e:
            prior=str(self.d.date.iloc[-2].date())
            self.assertTrue(all(z['formed_asof']==prior for z in e))
    def test_declining_breakout_not_buy_score(self):
        e,_=safe_scan_at(self.d,len(self.d)-1)
        self.assertTrue(all('score' not in z for z in e))
    def test_volume_previous_20_only(self):
        d=self.d.copy();d.loc[len(d)-1,'volume']=9e5
        e,_=safe_scan_at(d,len(d)-1)
        self.assertTrue(all(z['volume_surged'] for z in e))
    def test_dedup(self):
        df=pd.DataFrame([dict(code='039840',direction='UP',candidate_type='ATR_PIVOT_CLUSTER',
            idx=x,date=str(x),event='RESISTANCE_CLUSTER_BREAKOUT',level_event=100,
            volume_vs_prior20=2) for x in (101,104,130)])
        x,g=group_repeated_events(df)
        self.assertEqual(len(g),2)
        self.assertEqual(g.observations.tolist(),[2,1])
    def test_dedup_not_combine_directions(self):
        df=pd.DataFrame([dict(code='039840',direction=dir,candidate_type='A',
             idx=101,date='d',event='X',level_event=100,volume_vs_prior20=1)
             for dir in ('UP','DOWN')]);_,g=group_repeated_events(df);self.assertEqual(len(g),2)
    def test_prior_r112_projection(self):
        d=pd.DataFrame([dict(code='039840',date='2024-01-01',kind='UPPER',state='ACTIVE',
            active_level=100,active_slope_pct_per_bar=1,active_line_id='a',scale='SHORT')])
        a=prior_r112_context('039840','2024-01-01',102,d)
        self.assertAlmostEqual(a['r112_nearest_upper_level_projected'],101)
    def test_R114_filter(self):
        d=pd.DataFrame([dict(code='039840',date='d',event='BREAKOUT_UP',strict_eligible=True,strict_pattern='FALLING_WEDGE')])
        x=strict_r114_context('039840','d','UP',d);self.assertEqual(x['r114_same_direction_strict_breaks'],1)
    def test_scipy_is_available(self):self.assertIsNotNone(importlib.import_module('scipy.signal').find_peaks)
    def test_full_replay_smoke(self):
        d=self.d.copy();d.loc[:,'date']=pd.bdate_range('2024-01-01',periods=len(d))
        with tempfile.TemporaryDirectory() as t:
            w=Path(t);fake_sources(d,w/'r112',w/'r114')
            a=run(root112=w/'r112',root114=w/'r114',output=w/'out',codes=['039840'],
                  raw_provider=lambda code:d,draw=False,cases=[('039840','디오',str(d.date.iloc[-1].date()))])
            self.assertTrue(a['upstream_unchanged'])
            self.assertGreaterEqual(a['causal_signal_records'],1)
            self.assertEqual(a['manual_case_audit_rows'],1)
            f=pd.read_csv(w/'out'/'R1D115_CAUSAL_ATR_PIVOT_CLUSTER_EVENTS_PIT.csv')
            self.assertTrue(f.known_before_event.all())
            self.assertTrue((f.r112_prior_active_lanes==24).all())
            self.assertIn('ma20_gap_pct',f.columns)
    def test_full_replay_chart_smoke(self):
        d=self.d.copy()
        with tempfile.TemporaryDirectory() as t:
            w=Path(t);fake_sources(d,w/'r112',w/'r114')
            a=run(root112=w/'r112',root114=w/'r114',output=w/'out',codes=['039840'],
                  raw_provider=lambda code:d,draw=True,cases=[('039840','디오',str(d.date.iloc[-1].date()))])
            self.assertTrue(a['charts'])
            self.assertTrue(all((w/'out'/'charts'/p).is_file() for p in a['charts']))

if __name__=='__main__':unittest.main()
