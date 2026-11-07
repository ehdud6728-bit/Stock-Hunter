import unittest
import pandas as pd
import numpy as np
from unittest.mock import patch
from analyze import lows,simulate,credibility,structure
from reliability import vp

class TimingTests(unittest.TestCase):
    def frame(self):
        return pd.DataFrame(dict(date=[f'D{i}' for i in range(30)],open=[100.]*30,high=[102.]*30,low=[99.]*30,close=[101.]*30))
    def test_pivot_needs_two_completed_bars(self):
        d=self.frame();d.loc[8,'low']=90
        self.assertEqual(lows(d,10)['chain'],0)
        self.assertEqual(lows(d,11)['pullback'],90)
        d.loc[11:,'low']=1
        self.assertEqual(lows(d,11)['pullback'],90)
    def test_gap_real_open(self):
        d=self.frame();d.loc[2,'open']=90;d.loc[2,'low']=88
        self.assertEqual(simulate(d,0,1,95)['exit_price'],90)
    def test_same_bar_stop_first(self):
        d=self.frame();d.loc[1,['low','high']]=[94,112]
        self.assertEqual(simulate(d,0,1,95)['reason'],'SL_BOTH_HIT')
    def test_close_break_exits_next_open(self):
        d=self.frame();d.loc[1,['low','close']]=[90,94];d.loc[2,'open']=92
        r=simulate(d,0,1,95,True)
        self.assertEqual((r['exit_date'],r['exit_price']),('D2',92))
    def test_expiry_close_break_no_future_open(self):
        d=self.frame();d.loc[20,'close']=94;d.loc[21,'open']=1
        self.assertEqual(simulate(d,0,1,95,True)['exit_price'],94)
    def test_rising_chain(self):
        d=self.frame();d.loc[3,'low']=80;d.loc[7,'low']=90
        self.assertEqual(lows(d,11)['origin'],80)
        self.assertEqual(lows(d,11)['chain'],2)
    def test_hand_calculated_volume_proxy(self):
        d=pd.DataFrame(dict(low=[0.],high=[2.],close=[2.],volume=[20.]))
        np.testing.assert_allclose(vp.volume_profile(d,2,'range')['weights'],[10,10])
        np.testing.assert_allclose(vp.volume_profile(d,2,'typical')['weights'],[0,20])
    def test_discovery_does_not_receive_validation_or_d0(self):
        d=pd.concat([self.frame()]*5,ignore_index=True);d['date']=[str(i) for i in range(len(d))];d['prior_atr20']=2
        seen=[]
        def capture(h,n):seen.append(h.copy());return [],h
        with patch.object(structure,'zones',capture):
            credibility(d,120);d.loc[80:,'high']=9999;credibility(d,120)
        pd.testing.assert_frame_equal(seen[0],seen[1]);self.assertEqual(len(seen[0]),80)

if __name__=='__main__':unittest.main()
