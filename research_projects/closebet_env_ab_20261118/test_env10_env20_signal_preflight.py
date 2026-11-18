import json
import math
import unittest

import pandas as pd

from env10_env20_signal_preflight import classify_one, summarize


EVENT = {'year': 2024, 'liquidity_tier': 'MID', 'code': '000123', 'date': '2024-08-22'}


def panel(low=85., invalid=None):
    rows = []
    for i in range(-20, 2):
        v = 100.
        rows.append({'code': EVENT['code'], 'date': EVENT['date'], 'D': i,
                     'valid_price': 0 if i == invalid else 1,
                     'open': v, 'high': 110., 'low': low if i == 0 else 95.,
                     'close': v, 'volume': 100, 'amount': 1000000})
    return pd.DataFrame(rows)


class PairedSignalPreflightTests(unittest.TestCase):
    def test_env10_only_as_of_d0(self):
        r = classify_one(EVENT, panel(low=85))
        self.assertEqual(r['status'], 'D0_POSTCLOSE_OBSERVATION_ONLY')
        self.assertEqual((r['env10_touch'], r['env20_touch']), (True, False))
        self.assertEqual((r['env10_first_touch'], r['env20_first_touch']), (True, False))

    def test_env20_is_nested_in_env10(self):
        r = classify_one(EVENT, panel(low=79))
        self.assertTrue(r['env10_touch'])
        self.assertTrue(r['env20_touch'])

    def test_neither_touch(self):
        r = classify_one(EVENT, panel(low=96))
        self.assertFalse(r['env10_touch'])
        self.assertFalse(r['env20_touch'])

    def test_future_price_mutation_has_no_effect(self):
        d = panel(low=85)
        a = classify_one(EVENT, d)
        d.loc[d.D.eq(1), ['low', 'open', 'close', 'high', 'amount']] = [1, 1000, 2, 5000, 999999999]
        b = classify_one(EVENT, d)
        self.assertEqual(a, b)

    def test_invalid_today_never_becomes_no_touch(self):
        r = classify_one(EVENT, panel(low=70, invalid=0))
        self.assertEqual(r['status'], 'UNKNOWN_INVALID_D0_PRICE')
        self.assertIsNone(r['env10_touch'])
        self.assertIsNone(r['env20_touch'])

    def test_missing_today_is_unknown(self):
        d = panel().loc[lambda x: x.D.ne(0)]
        self.assertEqual(classify_one(EVENT, d)['status'], 'UNKNOWN_MISSING_OR_DUPLICATED_D0')

    def test_invalid_warmup_is_unknown(self):
        d = panel(invalid=-19)
        r = classify_one(EVENT, d)
        self.assertEqual(r['status'], 'UNKNOWN_INSUFFICIENT_20_VALID_DAILY_BARS')
        self.assertIsNone(r['env10_touch'])

    def test_previous_touch_unknown_if_prior_warmup_missing(self):
        r = classify_one(EVENT, panel(invalid=-20))
        self.assertTrue(r['env10_touch'])
        self.assertIsNone(r['env10_first_touch'])

    def test_nan_in_today_is_never_signal(self):
        d = panel()
        d.loc[d.D.eq(0), 'low'] = math.nan
        self.assertIsNone(classify_one(EVENT, d)['env10_touch'])

    def test_json_does_not_contain_nan_and_not_winner(self):
        events = []
        for i in range(77):
            e = {**EVENT, 'code': str(i).zfill(6)}
            d = panel()
            d['code'] = e['code']
            events.append(classify_one(e, d))
        r = summarize(events, 'fixed-hash')
        self.assertEqual(r['env10_low_touch_count'], 77)
        self.assertEqual(r['env20_low_touch_count'], 0)
        self.assertEqual(r['performance_comparison_status'], 'NOT_COMPUTED')
        self.assertFalse(r['can_publish_strategy_performance'])
        self.assertIsNone(r['strategy_winner'])
        self.assertNotIn('NaN', json.dumps(r, allow_nan=False))

    def test_fewer_than_77_cannot_claim_complete(self):
        with self.assertRaisesRegex(ValueError, 'FROZEN_77'):
            summarize([classify_one(EVENT, panel())], 'hash')


if __name__ == '__main__':
    unittest.main()
