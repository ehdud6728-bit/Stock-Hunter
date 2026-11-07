import unittest
from types import SimpleNamespace

import pandas as pd
from structure import pivots,zones,choose


def frame():
    h=[100,101,110,102,100,99,100,101,110,102,101,100,100]
    return pd.DataFrame(dict(date=[f"d{i:02}" for i in range(len(h))],high=h,
                             close=[v-1 for v in h],low=[v-2 for v in h],prior_atr20=[4.]*len(h),
                             volume=[1]*len(h)))


class StructureTests(unittest.TestCase):
    def test_unconfirmed_last_pivot_is_excluded(self):
        d=frame()
        self.assertEqual(pivots(d.iloc[:10]),[2])
        self.assertEqual(pivots(d.iloc[:12]),[2,8])

    def test_zone_uses_prices_independently_of_volume(self):
        d=frame()
        a,w=zones(d,len(d))
        self.assertEqual(len(a),1)
        self.assertEqual(a[0]["count"],2)
        self.assertEqual([v["known_date"] for v in a[0]["tests"]],["d05","d11"])
        d.volume=[i*100000 for i in range(len(d))]
        b,_=zones(d,len(d))
        self.assertEqual(a,b)
        self.assertEqual((a[0]["low"],a[0]["high"]),(108.,112.))

    def test_states_include_prior_clearance_and_weak_strong_boundary(self):
        z=zones(frame(),len(frame()))[0]
        for close,previous,expected in [(107,107,"OVERHEAD"),(112,107,"CLOSE_INSIDE"),
                                        (113,107,"BREAK_WEAK"),(114,107,"BREAK_STRONG"),
                                        (114,113,"ALREADY_CLEARED"),(110,113,"REENTRY_BELOW_OLD_RESISTANCE")]:
            b=SimpleNamespace(open=100.,close=close,prior_atr20=4.)
            self.assertEqual(choose(z,b,previous)["state"],expected)

    def test_confirmed_history_is_prefix_invariant(self):
        d=frame()
        a=zones(d,len(d))[0]
        extended=pd.concat([d,pd.DataFrame(dict(date=["d13"],high=[1000],close=[999],low=[998],
                                               prior_atr20=[4.],volume=[100000]))],ignore_index=True)
        # A future bar is never passed to the D0 feature computation.
        b=zones(extended.iloc[:len(d)],len(d))[0]
        self.assertEqual(a,b)
        self.assertEqual(a[0]["tests"][0]["close_to_min_pct"],
                         100*(98/109-1))


if __name__=="__main__":
    unittest.main()
