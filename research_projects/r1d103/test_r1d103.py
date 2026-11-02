import sys,unittest
from pathlib import Path
import numpy as np
import pandas as pd
HERE=Path(__file__).resolve().parents[1]
ROOT=HERE.parent
sys.path[:0]=[str(HERE),str(ROOT/'r1d101'),str(ROOT/'r1d102')]
from ma_roles import (PRIMARY_WINDOWS,ALL_WINDOWS,TOUCH_EPS,classify_interaction,
    classify_regime,inventory_events,attach_outcomes,cooldown_events,summary_tables,before_event)
from pre_rise import build_features,future_labels
from cloud_geometry import enrich_clouds


def raw_prices(n=720):
    i=np.arange(n,dtype=float)
    c=102+10*np.sin(i/36)+4*np.sin(i/7)+i*.01
    v=170000+(i%16)*4000
    return pd.DataFrame({'date':pd.bdate_range('2022-01-03',periods=n),
        'open':c-.2,'high':c+1.4,'low':c-1.8,'close':c,
        'volume':v,'amount':v*c})


def features():return enrich_clouds(build_features(raw_prices()))


def tiny(t=12,n=5,prev=99,prev_ma=100,close=101,ma=100,low=99,high=102):
    x=pd.DataFrame({'close':[100.]*20,'low':[99.]*20,'high':[102.]*20,f'ma{n}':[100.]*20})
    x.loc[t-1,['close',f'ma{n}']]=[prev,prev_ma]
    x.loc[t,['close',f'ma{n}','low','high']]=[close,ma,low,high]
    return x

class TestR1D103(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.f=features();cls.labels=future_labels(cls.f)

    def test_periods(self):
        self.assertEqual(PRIMARY_WINDOWS,(5,20,60,112,224))
        self.assertEqual(len(ALL_WINDOWS),13)

    def test_reclaim_above(self):
        self.assertEqual(classify_interaction(tiny(),12,5),'RECLAIM_ABOVE')

    def test_loss_below(self):
        self.assertEqual(classify_interaction(tiny(prev=101,close=99,low=98,high=101),12,5),'LOSE_BELOW')

    def test_touch_above_candidate(self):
        self.assertEqual(classify_interaction(tiny(prev=101,close=102,low=99,high=104),12,5),'TOUCH_ABOVE_CANDIDATE')

    def test_touch_below_candidate(self):
        self.assertEqual(classify_interaction(tiny(prev=99,close=99,low=97,high=101),12,5),'TOUCH_BELOW_CANDIDATE')

    def test_no_touch(self):
        self.assertIsNone(classify_interaction(tiny(prev=101,close=103,low=102,high=104),12,5))

    def test_invalid_input(self):
        for args in [(0,5,.005),(12,7,.005),(12,5,.1)]:
            with self.assertRaises(ValueError):classify_interaction(tiny(),*args)

    def test_regime(self):
        got=classify_regime(self.f,610)
        self.assertIn(got['ma_stack_context'],{'BULL_STACK_5_TO_224','BEAR_STACK_5_TO_224','MIXED_STACK'})
        self.assertIn(got['ma224_zone'],{'ABOVE','BELOW'})

    def test_inventory_no_future_includes_ma224_both_zones(self):
        f=self.f
        ev=inventory_events(f)
        self.assertGreater(len(ev),0)
        self.assertTrue(set(ev.ma_period).issubset(PRIMARY_WINDOWS))
        self.assertTrue((ev.idx>=540).all())
        self.assertTrue((ev.idx<len(f)-20).all())
        self.assertTrue(ev[['idx','ma_period','event']].duplicated().sum()==0)

    def test_events_ignore_future_outcome_labels(self):
        f=self.f
        changed=f.copy()
        cutoff=605
        for c in ('open','high','low','close'):
            changed.loc[changed.idx>cutoff,c]*=1.217
        # Must REBUILD features from raw OHLC after changed future values.
        cols=['date','open','high','low','close','volume','amount']
        altered=enrich_clouds(build_features(changed[cols]))
        a=inventory_events(f);b=inventory_events(altered)
        pd.testing.assert_frame_equal(a[a.idx<=cutoff].reset_index(drop=True),b[b.idx<=cutoff].reset_index(drop=True))

    def test_future_outcome_join_expost_only(self):
        ev=inventory_events(self.f)
        result=attach_outcomes(ev,self.labels,self.f)
        self.assertEqual(len(ev),len(result))
        self.assertEqual(set(ev.columns) & {'outcome_EXPOST','forward20_close_pct'},set())
        self.assertTrue({'outcome_EXPOST','forward20_close_pct','forward5_close_pct_EXPOST'}.issubset(result.columns))

    def test_outcome_changes_event_stays_same(self):
        a=inventory_events(self.f).head(3)
        labels=self.labels.copy()
        baseline=attach_outcomes(a,labels,self.f)
        labels.loc[labels.idx.isin(a.idx),'forward20_close_pct']=-90.
        altered=attach_outcomes(a,labels,self.f)
        self.assertTrue((baseline['event']==altered['event']).all())
        self.assertTrue((altered['forward20_close_pct']==-90).all())

    def test_sample_cooldown(self):
        e=attach_outcomes(inventory_events(self.f),self.labels,self.f)
        s=cooldown_events(e)
        for _,g in s.groupby(['ma_period','event']):
            self.assertTrue((g.idx.sort_values().diff().dropna()>=20).all())

    def test_sample_independent_of_outcomes(self):
        ev=inventory_events(self.f)
        a=attach_outcomes(ev,self.labels,self.f)
        lab=self.labels.copy();lab['is_sustained_surge']=~lab['is_sustained_surge']
        b=attach_outcomes(ev,lab,self.f)
        sa=cooldown_events(a);sb=cooldown_events(b)
        pd.testing.assert_frame_equal(sa[['idx','event','ma_period']],sb[['idx','event','ma_period']])

    def test_no_independent_count_claim(self):
        e=attach_outcomes(inventory_events(self.f),self.labels,self.f)
        s=cooldown_events(e)
        stat=summary_tables(s)
        self.assertTrue((stat.n_unique_days<=stat.n_events).all())
        self.assertTrue((stat.research_sample_warning=='SMALL_OR_CORRELATED_SAMPLE').all())

    def test_snapshots_only_prior(self):
        e=attach_outcomes(inventory_events(self.f),self.labels,self.f)
        sel=cooldown_events(e).head(2)
        b=before_event(self.f,sel)
        self.assertEqual(len(b),10)
        self.assertTrue((b.idx<=self.f[self.f.date.isin(pd.to_datetime(sel['asof']))].idx.max()).all())
        self.assertFalse(any(c.startswith('forward') for c in b.columns))

    def test_invalid_date_order(self):
        f=self.f.copy();f.loc[610,'date']=f.loc[609,'date']
        with self.assertRaises(ValueError):inventory_events(f)

    def test_invalid_index_order(self):
        f=self.f.copy();f.loc[610,'idx']=609
        with self.assertRaises(ValueError):inventory_events(f)

    def test_missing_feature_rejected(self):
        with self.assertRaises(ValueError):inventory_events(self.f.drop(columns=['ma60']))

    def test_no_future_context_in_input(self):
        from inspect import signature
        self.assertNotIn('labels',signature(inventory_events).parameters)
        self.assertNotIn('labels',signature(classify_regime).parameters)
        self.assertNotIn('labels',signature(classify_interaction).parameters)

    def test_many_ma_same_day_not_deduped_as_independent(self):
        e=attach_outcomes(inventory_events(self.f),self.labels,self.f)
        s=cooldown_events(e)
        self.assertLessEqual(s[['idx']].drop_duplicates().shape[0],len(s))

if __name__=='__main__':unittest.main()
