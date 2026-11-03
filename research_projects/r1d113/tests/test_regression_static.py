"""Additional R113 event timing assertions."""
import sys,pathlib,unittest
import numpy as np,pandas as pd
HERE=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(HERE))
from structures import scan


def dframe(n=105):
    x=np.arange(n,dtype=float)
    base=110-.35*x+4*np.sin(x/4)
    o=base-.5;c=base+.5
    return pd.DataFrame({'date':pd.date_range('2024-01-01',periods=n,freq='B'),
        'open':o,'close':c,'high':np.maximum(o,c)+1,'low':np.minimum(o,c)-1,
        'volume':np.full(n,2000),'amount':np.full(n,1e7)})

class TimingTests(unittest.TestCase):
    def test_all_recorded_breaks_have_preexisting_formation(self):
        _,_,ev=scan(dframe())
        if ev.empty:return
        b=ev[ev.event.isin(['BREAKOUT_UP','BREAKDOWN_DOWN'])]
        if len(b):self.assertTrue((b.formed_idx<b.idx).all())
    def test_outcome_future_cannot_change_prior_events(self):
        a=dframe(); _,_,x=scan(a)
        b=a.copy();i=90
        b.loc[i:,'high']+=20;b.loc[i:,'close']+=10
        _,_,y=scan(b)
        if not x.empty and not y.empty:
            cols=['date','pattern','basis','event','formed_asof','level']
            px=x[x.idx.lt(i)][cols].reset_index(drop=True)
            py=y[y.idx.lt(i)][cols].reset_index(drop=True)
            pd.testing.assert_frame_equal(px,py)
    def test_candidate_was_not_named_as_trade_signal(self):
        can,_,_=scan(dframe())
        if len(can):self.assertTrue(can.warning.str.contains('NOT_TRADE_SIGNAL').all())

if __name__=='__main__':unittest.main()
