"""Synthetic guardrails.  These tests do not attest source independence."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from frozen_300_forward_path import classify, forward_path, ma20, valid_price, aggregate


def synthetic_panel(*, d0_low=89.0, d1_open=100.0, d5_close=110.0):
    records = []
    for d in range(-20, 21):
        o, h, lo, c = 100.0, 101.0, 99.0, 100.0
        if d == 0:
            lo = d0_low
        if d == 1:
            o, h, lo, c = d1_open, max(d1_open, 102), min(d1_open, 98), 100.0
        if d == 5:
            o, h, lo, c = 100.0, max(111.0, d5_close), 99.0, d5_close
        records.append({'code':'000001','date':'2026-01-01','trade_date':f'2026-02-{d+21:02d}',
                        'D':d,'valid_price':1,'open':o,'high':h,'low':lo,'close':c})
    # Synthetic date labels need only be strictly increasing and later than D0.
    # A numeric-day range crosses February, so use an anchored 60-day timeline.
    anchor = pd.Timestamp('2026-02-01')
    for row in records:
        row['trade_date'] = (anchor + pd.Timedelta(days=row['D'])).strftime('%Y-%m-%d')
    for row in records:
        row['date'] = anchor.strftime('%Y-%m-%d')
    return pd.DataFrame(records)


class FrozenForwardPathTests(unittest.TestCase):
    def event(self, panel):
        return {'code':'000001','date':str(panel.iloc[0]['date']),
                'year':2026,'liquidity_tier':'MID'}

    def test_env10_touch_does_not_imply_env20(self):
        panel = synthetic_panel(d0_low=89)
        s = classify(self.event(panel), panel)
        self.assertEqual((s['env10_touch'],s['env20_touch']), (True, False))
        self.assertTrue(s['env10_first_touch'])

    def test_env20_always_implies_env10(self):
        panel = synthetic_panel(d0_low=79)
        s = classify(self.event(panel), panel)
        self.assertEqual(s['signal_group'], 'ENV20_BOTH')
        self.assertTrue(s['env20_first_touch'])

    def test_no_touch_is_kept_as_control_context(self):
        panel = synthetic_panel(d0_low=95)
        self.assertEqual(classify(self.event(panel),panel)['signal_group'], 'NO_TOUCH')

    def test_dplus_change_does_not_change_signal(self):
        panel = synthetic_panel(d0_low=89)
        changed = panel.copy()
        changed.loc[changed.D.eq(5), ['high','close']] = [1000,1000]
        a,b = classify(self.event(panel),panel),classify(self.event(changed),changed)
        self.assertEqual((a['env10_touch'],a['env20_touch'],a['env10_first_touch']),
                         (b['env10_touch'],b['env20_touch'],b['env10_first_touch']))
        self.assertNotEqual(a['forward_paths']['5']['gross_return_pct'],
                            b['forward_paths']['5']['gross_return_pct'])

    def test_five_day_open_to_close_net_20bps(self):
        panel = synthetic_panel(d0_low=89,d1_open=100,d5_close=110)
        p = forward_path(panel, self.event(panel)['date'], 5)
        self.assertAlmostEqual(p['gross_return_pct'],10.0)
        self.assertAlmostEqual(p['net_return_pct_by_total_cost_bps']['20'],9.8)

    def test_missing_exit_day_not_zero(self):
        panel = synthetic_panel()
        panel = panel.loc[panel.D.ne(5)]
        self.assertEqual(forward_path(panel,self.event(panel)['date'],5)['status'],
                         'UNKNOWN_MISSING_PATH_BARS')

    def test_invalid_intermediate_day_not_filled(self):
        panel = synthetic_panel()
        panel.loc[panel.D.eq(3),'valid_price'] = 0
        self.assertEqual(forward_path(panel,self.event(panel)['date'],5)['status'],
                         'UNKNOWN_INVALID_PATH_BARS')

    def test_no_ma20_if_prelisting_gap(self):
        panel = synthetic_panel()
        panel.loc[panel.D.eq(-8),'valid_price'] = 0
        self.assertIsNone(ma20(panel,0))
        self.assertEqual(classify(self.event(panel), panel)['signal_status'],'UNKNOWN_MA20')

    def test_nonsense_ohlc_rejected(self):
        panel = synthetic_panel()
        panel.loc[panel.D.eq(0),'low'] = 103
        self.assertFalse(valid_price(panel.loc[panel.D.eq(0)].iloc[0]))

    def test_out_of_order_future_date_is_unknown(self):
        panel = synthetic_panel()
        panel.loc[panel.D.eq(2),'trade_date'] = self.event(panel)['date']
        self.assertEqual(forward_path(panel,self.event(panel)['date'],5)['status'],
                         'UNKNOWN_OUT_OF_ORDER_PRICE_DATES')

    def test_unknowns_do_not_count_as_positive_or_zero_returns(self):
        panel = synthetic_panel()
        x = classify(self.event(panel),panel)
        x['forward_paths']['5']={'status':'UNKNOWN_INVALID_PATH_BARS'}
        a = aggregate([x])
        v = a['ENV10_ONLY']['horizons']['5']
        self.assertEqual((v['available'],v['unknown']), (0,1))
        self.assertIsNone(v['provisional_mean_net_20bps_pct'])


if __name__ == '__main__':
    unittest.main()
