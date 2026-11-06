import unittest
from types import SimpleNamespace

import numpy as np
import pandas as pd

from research import confirmations, mark_cohorts, rejection_tests, resistance, trade


def bars(closes, lows=None):
    return pd.DataFrame(dict(
        date=pd.date_range("2024-01-01", periods=len(closes)).strftime("%Y-%m-%d"),
        open=np.array(closes)-.2, close=closes,
        low=lows if lows is not None else np.array(closes)-.4,
        high=np.array(closes)+.4, prior_atr20=1., amount=10.,
        amount_ratio20=2., bb20_upper_shift26=80.))


class ResistanceTests(unittest.TestCase):
    def test_excludes_pending_rejection_and_clusters(self):
        h = bars([8., 10., 8., 8., 10., 8., 8., 8., 10., 8., 8.])
        self.assertEqual([i for i, _, _ in rejection_tests(h, 9.5, 10.5)], [1])
        extended = bars([*h.close, 8.])
        self.assertEqual([i for i, _, _ in rejection_tests(extended, 9.5, 10.5)], [1, 8])

    def test_block_requires_close_near_zone_and_large_overlap(self):
        h = bars([8., 10., 8., 8., 8., 8., 8., 10., 8., 8., 8., 8.])
        profile = lambda *_args, **_kwargs: dict(weights=np.array([4., 1., 1.]),
                                                centers=np.array([10., 12., 14.]), width=1.)
        within, _ = resistance(h, SimpleNamespace(open=8., close=10.2, prior_atr20=1.), 12, "range", profile)
        cleared, _ = resistance(h, SimpleNamespace(open=8., close=11.1, prior_atr20=1.), 12, "range", profile)
        self.assertTrue(within["block"])
        self.assertTrue(cleared["strong"])
        self.assertFalse(cleared["block"])
        missing, _ = resistance(h, SimpleNamespace(open=8., close=10.2, prior_atr20=1.), 13, "range", profile)
        self.assertFalse(missing["coverage"])

    def test_future_confirmation_is_effective_at_next_open(self):
        d = bars([10.6, 10.7, 10.6] + [11.]*18, [10.5, 10.5, 10.1]+[10.8]*18)
        d.loc[2,"amount"] = 5.
        facts = dict(zone_low=9.8, zone_high=10.3)
        c, p, route, evidence = confirmations(d, 0, facts, "UPPER")
        self.assertEqual((c, p, route), (1, 2, "PULLBACK"))
        self.assertEqual(confirmations(d.iloc[:2], 0, facts, "UPPER")[0], c)
        self.assertIsNone(confirmations(d.iloc[:1], 0, facts, "UPPER")[0])
        pd.testing.assert_frame_equal(pd.DataFrame(evidence[:2]),
                                      pd.DataFrame(confirmations(d.iloc[:2], 0, facts, "UPPER")[3]))
        def simulate(frame, d0, entry, px, structural):
            self.assertEqual((d0, entry, structural), (0, 2, False))
            return dict(exit_date=frame.date.iloc[entry], exit_price=px, net_pct=-.25)
        result = trade(d, 0, c, "SETTLE", simulate)
        self.assertEqual(result["entry_date"], d.date.iloc[2])

    def test_nonoverlap_uses_both_bands_and_keeps_boundary_closed(self):
        ledger = pd.DataFrame(dict(code=["1"]*4, date=["01","01","03","04"],
                                   band=["UPPER","LOWER","UPPER","LOWER"],
                                   d20_date=["03","03","04","06"]))
        got = mark_cohorts(ledger)
        self.assertEqual(got.unique_day.tolist(), [False, True, True, True])
        self.assertEqual(got.nonoverlap.tolist(), [False, True, False, True])


if __name__ == "__main__":
    unittest.main()
