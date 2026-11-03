"""Unit/PIT regression cases for the independent R113 structural lane."""
import pathlib,sys,tempfile,unittest
import numpy as np,pandas as pd
HERE=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(HERE))
from structures import classify,candidate_at,scan,_pivots,_raw,_atr
from research_r1d113 import _expost,_confluence


def sample(n=95,typ='down'):
    r=np.arange(n,dtype=float)
    slope=-.65 if typ=='down' else .03
    mid=100+slope*r
    # deterministic oscillating price, contained in 11-KRW-wide channel
    price=mid+4*np.sin(r*np.pi/5)
    o=price-.6;close=price+.6
    d=pd.DataFrame({'date':pd.date_range('2024-01-01',periods=n,freq='B'),
      'open':o,'high':np.maximum(o,close)+.9,'low':np.minimum(o,close)-.9,
      'close':close,'volume':np.full(n,1000.),'amount':np.full(n,100000.)})
    return d

class StructuralResearchTests(unittest.TestCase):
    def test_classify_flat_box(self):
        self.assertEqual(classify(0,.01,10,10,1),'HORIZONTAL_BOX')
    def test_classify_down_channel(self):
        self.assertEqual(classify(-.3,-.31,11,10,1),'DESCENDING_CHANNEL')
    def test_classify_up_channel(self):
        self.assertEqual(classify(.3,.31,11,10,1),'ASCENDING_CHANNEL')
    def test_classify_triangle(self):
        self.assertEqual(classify(-.3,.3,20,9,1),'SYMMETRICAL_TRIANGLE')
    def test_classify_falling_wedge(self):
        self.assertEqual(classify(-.5,-.2,20,10,1),'FALLING_WEDGE')
    def test_classify_rising_wedge(self):
        self.assertEqual(classify(.2,.5,20,10,1),'RISING_WEDGE')
    def test_invalid_crossover(self):
        self.assertIsNone(classify(-.4,.4,2,-1,1))
    def test_pivot_right_2(self):
        a=np.array([1.,2.,5.,3.,2.,1.,2.,3.])
        self.assertNotIn(2,_pivots(a,0,3,upper=True))
        self.assertIn(2,_pivots(a,0,4,upper=True))
    def test_no_candidate_without_past(self):
        self.assertIsNone(candidate_at(sample(),10,22,'WICK'))
    def test_future_mutation_does_not_change_historical_candidate(self):
        d=sample();a=candidate_at(d,70,50,'WICK')
        other=d.copy();other.loc[other.index>70,'high']=other.high.iloc[-1]*2
        other.loc[other.index>70,'low']=other.low.iloc[-1]*.8
        other.loc[other.index>70,'open']=other.low.iloc[-1]
        other.loc[other.index>70,'close']=other.low.iloc[-1]
        b=candidate_at(other,70,50,'WICK')
        self.assertEqual(a,b)
    def test_scan_future_does_not_repaint(self):
        d=sample(96);a,_,_=scan(d)
        e=d.copy();e.loc[e.index>=85,'high']+=20;e.loc[e.index>=85,'close']+=10
        aa,_,_=scan(e)
        cols=['asof','pattern','basis','upper_price','lower_price','geometry_score']
        p=a[a.asof_idx.lt(85)][cols].reset_index(drop=True)
        q=aa[aa.asof_idx.lt(85)][cols].reset_index(drop=True)
        pd.testing.assert_frame_equal(p,q)
    def test_required_raw_fields(self):
        with self.assertRaises(ValueError):_raw(pd.DataFrame({'date':['2024-01-01']}))
    def test_valid_ohlc(self):
        q=sample();q.loc[0,'high']=0
        with self.assertRaises(ValueError):_raw(q)
    def test_atr_left_rolling(self):
        q=sample();a=_atr(q)[35];q.loc[40,'high']+=50;self.assertEqual(a,_atr(q)[35])
    def test_future_outputs_are_separate(self):
        d=sample()
        f=pd.DataFrame([{'idx':80,'date':str(d.date.iloc[80].date()),
           'pattern':'HORIZONTAL_BOX','basis':'WICK','event':'BREAKOUT_UP',
           'formed_asof':str(d.date.iloc[79].date()),'formed_idx':79,
           'level':100.,'close':100.,'atr14':2.}])
        p=_expost(d,f)
        self.assertIn('Dp10_return_EXPOST',p.columns)
        self.assertNotIn('Dp10_return_EXPOST',f.columns)
    def test_no_future_bar_for_incomplete_expost(self):
        d=sample();f=pd.DataFrame([{'idx':len(d)-2,'date':str(d.date.iloc[-2].date()),'pattern':'HORIZONTAL_BOX',
          'basis':'WICK','event':'BREAKOUT_UP','formed_asof':'2024-01-01','formed_idx':70,
          'level':100.,'close':100.,'atr14':2.}])
        p=_expost(d,f);self.assertFalse(bool(p.Dp5_available.iloc[0]))
    def test_nearest_trendline_not_rewritten(self):
        d=pd.DataFrame([{'date':'2024-01-01','algorithm':'A','scale':'SHORT','kind':'UPPER','active_level':110,
           'event':'NONE','state':'ACTIVE','active_line_id':'x'},
          {'date':'2024-01-02','algorithm':'A','scale':'SHORT','kind':'UPPER','active_level':120,
           'event':'NONE','state':'ACTIVE','active_line_id':'x'}])
        before=d.copy(deep=True)
        ev=pd.DataFrame([{'date':'2024-01-02','idx':1,'event':'BREAKOUT_UP','close':100.,'pattern':'HORIZONTAL_BOX','basis':'WICK'}])
        q=_confluence(d,ev,pd.DataFrame())
        self.assertEqual(q.previous_close_active_upper_lanes.iloc[0],1)
        pd.testing.assert_frame_equal(d,before)
    def test_case_manual_refs_not_in_structures(self):
        import inspect,structures
        self.assertNotIn('2024-09-03',inspect.getsource(structures))
        self.assertNotIn('2024-02-20',inspect.getsource(structures))
    def test_shape_candidates_not_necessarily_breakout(self):
        a,s,e=scan(sample())
        self.assertTrue(len(a)>0)
        self.assertTrue(a.when_known.eq('AT_ASOF_CLOSE_ONLY').all())
        self.assertFalse('return_EXPOST' in a)

if __name__=='__main__':unittest.main()
