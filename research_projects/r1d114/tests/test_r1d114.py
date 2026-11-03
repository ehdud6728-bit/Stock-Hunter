import sys,unittest,tempfile
from pathlib import Path
import numpy as np
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from structure_audit import classify_strict,classify_all,replay_lifecycle,event_strict_gate
from full_context import features,join_event_context,MA
from research_r1d114 import combine_r112_prior


def geom(up=0.,dn=0.,first_high=120.,first_low=100.,span=20):
    st=10;t=st+span
    return {'code':'000001','asof_idx':t,'start_idx':st,'asof':'2024-02-20',
        'upper_m':up,'lower_m':dn,'upper_b':first_high-up*st,'lower_b':first_low-dn*st,
        'upper_price':first_high+up*span,'lower_price':first_low+dn*span,
        'upper_touch_count':3,'lower_touch_count':3,'body_inside_fraction':.88,
        'geometry_score':55,'window':span+1,'basis':'WICK','pattern':'HORIZONTAL_BOX'}


def fake(n=85):
    x=np.arange(n,dtype=float)
    c=100+x*.20+2*np.sin(x/5)
    o=c-.4;h=c+1.2;l=o-1.0
    return pd.DataFrame({'date':pd.date_range('2024-01-02',periods=n,freq='B'),
         'open':o,'high':h,'low':l,'close':c,'volume':1e5+x*100,
         'amount':1e9+x*1e6})


class TestStrictGeometry(unittest.TestCase):
    def test_horizontal(self):self.assertEqual(classify_strict(geom())['strict_pattern'],'HORIZONTAL_BOX')
    def test_reject_tilted_box(self):
        r=geom(up=-.35,dn=-.25)
        self.assertNotEqual(classify_strict(r)['strict_pattern'],'HORIZONTAL_BOX')
    def test_descending_channel(self):
        r=geom(up=-.8,dn=-.75)
        self.assertEqual(classify_strict(r)['strict_pattern'],'DESCENDING_CHANNEL')
    def test_ascending_channel(self):
        r=geom(up=.8,dn=.75)
        self.assertEqual(classify_strict(r)['strict_pattern'],'ASCENDING_CHANNEL')
    def test_triangle(self):
        r=geom(up=-.4,dn=.4)
        self.assertEqual(classify_strict(r)['strict_pattern'],'SYMMETRICAL_TRIANGLE')
    def test_falling_wedge(self):
        r=geom(up=-.8,dn=-.4)
        self.assertEqual(classify_strict(r)['strict_pattern'],'FALLING_WEDGE')
    def test_rising_wedge(self):
        r=geom(up=.4,dn=.8)
        self.assertEqual(classify_strict(r)['strict_pattern'],'RISING_WEDGE')
    def test_no_crossed_geometry(self):
        r=geom(up=-1.,dn=1.)
        self.assertFalse(classify_strict(r)['strict_eligible'])
    def test_reject_short_span(self):
        r=geom(span=3)
        self.assertFalse(classify_strict(r)['strict_eligible'])
    def test_reject_insufficient_touches(self):
        r=geom();r['upper_touch_count']=1
        self.assertFalse(classify_strict(r)['strict_eligible'])
    def test_reject_containment(self):
        r=geom();r['body_inside_fraction']=.35
        self.assertFalse(classify_strict(r)['strict_eligible'])
    def test_reclassify_wrong_name(self):
        r=geom(up=-.8,dn=-.75)
        r['pattern']='HORIZONTAL_BOX'
        x=classify_all(pd.DataFrame([r]));self.assertEqual(x.iloc[0].strict_pattern,'DESCENDING_CHANNEL')
        self.assertEqual(x.iloc[0]['pattern'],'HORIZONTAL_BOX')
    def test_geometry_does_not_use_future_returns(self):
        r=geom();a=classify_strict(r)
        r['DPLUS20_EXPOST']=-99;r['future_profit']=999
        self.assertEqual(a,classify_strict(r))


class TestLifecycle(unittest.TestCase):
    def make(self,t,up=0.,dn=0.):
        r=geom(up,dn);r['asof_idx']=t;r['asof']=f'2024-02-{t:02d}'
        r['start_idx']=t-20;r['upper_b']=120.-up*(t-20);r['lower_b']=100.-dn*(t-20)
        r['upper_price']=120.+up*20;r['lower_price']=100.+dn*20
        return r
    def test_new_then_update(self):
        g=classify_all(pd.DataFrame([self.make(20),self.make(21)]))
        q=replay_lifecycle(g)
        self.assertEqual(q.iloc[0].lifecycle_event,'NEW_CONFIRMED_STRUCTURE')
        self.assertEqual(q.iloc[1].lifecycle_event,'UPDATED_SAME_STRUCTURE')
        self.assertEqual(q.iloc[0].regime_id,q.iloc[1].regime_id)
    def test_gap_archive(self):
        g=classify_all(pd.DataFrame([self.make(20),self.make(24)]))
        q=replay_lifecycle(g,hold_bars=2)
        self.assertIn('ENDED_STALE_GEOMETRY',q.lifecycle_event.tolist())
    def test_big_reanchor(self):
        a=self.make(20);b=self.make(21);b['upper_b']+=12;b['upper_price']+=12
        g=classify_all(pd.DataFrame([a,b]));q=replay_lifecycle(g)
        self.assertTrue(any(q.lifecycle_event.eq('ENDED_MAJOR_REANCHOR')))
    def test_event_requires_previous_formation(self):
        g=classify_all(pd.DataFrame([self.make(20)]))
        r=pd.DataFrame([{'code':'000001','idx':21,'date':'2024-02-21',
                         'pattern':'HORIZONTAL_BOX','basis':'WICK',
                         'formed_idx':20,'event':'BREAKOUT_UP'}]);q=event_strict_gate(r,g)
        self.assertTrue(q.iloc[0].strict_eligible)
    def test_future_snapshot_does_not_change_prior_gate(self):
        a=self.make(20);b=self.make(25,up=-.2,dn=-.1)
        e=pd.DataFrame([{'code':'000001','idx':21,'date':'2024-02-21',
          'pattern':'HORIZONTAL_BOX','basis':'WICK','formed_idx':20,'event':'BREAKOUT_UP'}])
        q1=event_strict_gate(e,classify_all(pd.DataFrame([a])))
        q2=event_strict_gate(e,classify_all(pd.DataFrame([a,b])))
        self.assertEqual(q1.iloc[0].strict_signal_status,q2.iloc[0].strict_signal_status)
    def test_no_same_day_creation_break(self):
        g=classify_all(pd.DataFrame([self.make(20)]))
        e=pd.DataFrame([{'code':'000001','idx':20,'pattern':'HORIZONTAL_BOX','basis':'WICK',
          'formed_idx':20,'event':'BREAKOUT_UP'}])
        with self.assertRaises(AssertionError):event_strict_gate(e,g)


class TestFullContext(unittest.TestCase):
    def test_13_ma_present(self):
        d=features(fake(520))
        for n in MA:self.assertIn(f'ma{n}_gap_pct',d)
    def test_tech_is_prefix_invariant(self):
        d=fake(530);a=features(d.iloc[:510].copy());b=features(d.copy())
        fields=['ma224_slope20_pct','bb20_width_delta5_pp','env40_10_position',
          'ichimoku_visible_thickness_pct','amount_vs_prev20','candle_body_pct']
        for k in fields:
            x=a.iloc[509][k];y=b.iloc[509][k]
            if pd.isna(x):self.assertTrue(pd.isna(y))
            elif isinstance(x,str):self.assertEqual(x,y)
            else:self.assertAlmostEqual(x,y,places=8)
    def test_ichimoku_visible_uses_shift(self):
        d=features(fake(105))
        self.assertTrue(pd.isna(d.iloc[76].ichimoku_visible_upper))
        self.assertTrue(pd.notna(d.iloc[77].ichimoku_visible_upper))
    def test_warmup_explicit(self):
        d=features(fake(240))
        self.assertTrue(pd.isna(d.iloc[222]['ma224']))
        self.assertTrue(pd.notna(d.iloc[223]['ma224']))
        self.assertTrue(pd.isna(d.iloc[239]['ma480']))
    def test_candle_ratios(self):
        d=fake(80);d.loc[30,['open','high','low','close']]=[100,110,95,108]
        x=features(d)
        self.assertAlmostEqual(x.iloc[30].candle_body_pct,100*8/15,places=4)
    def test_every_event_has_raw_join(self):
        e=pd.DataFrame([{'code':'000001','idx':20,'event':'BREAKOUT_UP',
            'pattern':'HORIZONTAL_BOX','basis':'WICK'}])
        x=join_event_context(e,{'000001':fake(520)})
        self.assertEqual(len(x),1)
        self.assertTrue(x.iloc[0].all_event_ohlcv_joined)
    def test_out_of_bounds_fails_loudly(self):
        e=pd.DataFrame([{'code':'000001','idx':900,'event':'BREAKOUT_UP',
            'pattern':'HORIZONTAL_BOX','basis':'WICK'}])
        with self.assertRaises(ValueError):join_event_context(e,{'000001':fake(520)})
    def test_r112_prior_is_t_minus_one(self):
        s=[]
        for t in [0,1,2]:
            for a in range(4):
                for scale in ('SHORT','MEDIUM','LONG'):
                    for kind in ('UPPER','LOWER'):
                        s.append({'code':'000001','idx':t,'date':f'2024-01-{t+2:02d}',
                            'algorithm':str(a),'scale':scale,'kind':kind,
                            'state':'ACTIVE' if t==1 else 'WAIT',
                            'event':'NONE'})
        e=pd.DataFrame([{'code':'000001','idx':2,'event':'BREAKOUT_UP'}])
        x=combine_r112_prior(e,pd.DataFrame(s))
        self.assertEqual(int(x.iloc[0].prior_close_r112_active_lanes),24)

if __name__=='__main__':unittest.main()
