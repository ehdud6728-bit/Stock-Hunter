import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'closebet_research'))
import r18_wave_entry_forensic as r
import numpy as np,pandas as pd

class R18CausalTests(unittest.TestCase):
    def setUp(self):
        n=400;days=pd.bdate_range('2025-01-02',periods=n)
        x=pd.DataFrame({'open':100.,'high':102.,'low':98.,'close':100.,'volume':1000.,'amount':100000.},index=days)
        x.loc[days[310:324],'high']=112
        x.loc[days[340:349],'low']=97
        x.loc[days[340:349],'volume']=450.
        self.d=x;self.cal=days
    def test_prewave_future_mutation_no_effect(self):
        a=r.make_prewave(self.d,355)
        d2=self.d.copy();d2.iloc[356:]=[1000,1100,500,900,800000,500000]
        b=r.make_prewave(d2,355)
        for k in a:
            if isinstance(a[k],float) and np.isnan(a[k]):self.assertTrue(np.isnan(b[k]),k)
            else:self.assertEqual(a[k],b[k],k)
    def test_signal_day_is_not_future_outcome(self):
        a=r.get_market_forward(self.d,self.cal,self.cal[350],5)
        self.assertEqual(len(a),5)
        self.assertEqual(a.index[0],self.cal[351])
    def test_confirmation_not_same_day_open(self):
        d=self.d.copy();d.iloc[351,d.columns.get_loc('close')]=110.
        d.iloc[351,d.columns.get_loc('high')]=112.
        d.iloc[351,d.columns.get_loc('open')]=100.
        i,label,status=r.entry_trigger(d,self.cal,self.cal[350],'BREAKOUT_CONFIRMED')
        self.assertEqual(status,'TRIGGER')
        self.assertEqual(i,352)
        self.assertEqual(label,'D2_OPEN')
    def test_two_touch_stop_first(self):
        d=self.d.copy()
        d.iloc[351,d.columns.get_loc('high')]=110.
        d.iloc[351,d.columns.get_loc('low')]=90.
        v=r.simulate_five_sessions(d,self.cal,self.cal[350],'NEXT_OPEN')
        self.assertEqual(v['reason'],'STOP_FIRST_OR_ONLY')
        self.assertAlmostEqual(v['net_pct'],-5.5)
    def test_next_open_prevents_signal_close_fill(self):
        v=r.simulate_five_sessions(self.d,self.cal,self.cal[350],'NEXT_OPEN')
        self.assertEqual(v['entry_date'],str(self.cal[351].date()))
        self.assertEqual(v['filled'],1)
    def test_no_lookahead_on_trigger(self):
        d=self.d.copy();s=self.cal[350]
        first=r.entry_trigger(d,self.cal,s,'BREAKOUT_CONFIRMED')
        d.loc[self.cal[355]:,'close']=200
        self.assertEqual(first,r.entry_trigger(d,self.cal,s,'BREAKOUT_CONFIRMED'))

if __name__=='__main__':unittest.main()
