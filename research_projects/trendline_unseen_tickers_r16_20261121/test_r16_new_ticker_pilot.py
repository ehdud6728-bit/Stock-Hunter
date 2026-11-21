"""R16 predeclared source, ticker-disjoint, provenance and no-replacement tests."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from r16_new_ticker_pilot import (
    EXCLUDED_FIVE, PREDECLARED_CODES, ROOT, R15_REL, R15_SHA256,
    collect, evaluate, selected_codes, validate_and_normalize,
)


def raw_prices(n=300):
    return pd.DataFrame({
        "시가":[100]*n, "고가":[105]*n, "저가":[95]*n,
        "종가":[101]*n,"거래량":[1000]*n,"거래대금":[100000]*n,
    },index=pd.bdate_range("2021-01-04",periods=n))


class R16TickerHoldoutTests(unittest.TestCase):
    def test_roster_is_hash_pinned_and_tickers_are_disjoint(self):
        self.assertEqual(tuple(selected_codes(ROOT)),PREDECLARED_CODES)
        self.assertEqual(len(PREDECLARED_CODES),30)
        self.assertEqual(len(set(PREDECLARED_CODES)),30)
        self.assertFalse(EXCLUDED_FIVE.intersection(PREDECLARED_CODES))

    def test_r15_parent_is_frozen(self):
        self.assertEqual(hashlib.sha256((ROOT/R15_REL).read_bytes()).hexdigest(),R15_SHA256)

    def test_normalization_preserves_price_and_code(self):
        d=validate_and_normalize(raw_prices(),"093320")
        self.assertEqual(list(d.columns),["date","code","open","high","low","close","volume","amount"])
        self.assertEqual(d.iloc[0].date,"2021-01-04")
        self.assertEqual(d.iloc[-1].code,"093320")
        self.assertEqual(d.iloc[0].high,105)

    def test_halted_zero_price_bar_is_rejected_not_interpolated(self):
        d=raw_prices()
        d.iloc[5,d.columns.get_loc("시가")]=0
        with self.assertRaisesRegex(ValueError,"ZERO_OR_NEGATIVE_TRADE_PRICE"):
            validate_and_normalize(d,"093320")

    def test_suspected_corporate_action_is_rejected_without_guess(self):
        d=raw_prices()
        d.iloc[7,d.columns.get_loc("시가")]=200
        d.iloc[7,d.columns.get_loc("고가")]=205
        d.iloc[7,d.columns.get_loc("저가")]=195
        d.iloc[7,d.columns.get_loc("종가")]=201
        with self.assertRaisesRegex(ValueError,"ACTION_OR_EXTREME_GAP"):
            validate_and_normalize(d,"093320")

    def test_missing_tickers_not_replaced_and_archive_sealed(self):
        good=set(PREDECLARED_CODES[:2])
        def fake_fetch(code):
            if code in good:
                return raw_prices()
            raise OSError("external provider unavailable")
        with tempfile.TemporaryDirectory() as directory:
            out=Path(directory)
            m=collect(ROOT,out,fetcher=fake_fetch)
            self.assertEqual(m["selected_codes"],list(PREDECLARED_CODES))
            self.assertEqual(m["n_selected"],30)
            self.assertEqual(m["n_valid_tickers"],2)
            self.assertEqual(m["n_invalid_or_unknown"],28)
            self.assertEqual(m["valid_codes"],list(PREDECLARED_CODES[:2]))
            self.assertEqual(m["source_row_count"],600)
            self.assertEqual(m["raw_archive_sha256"],hashlib.sha256(
                (out/"R16_FROZEN_HOLDOUT_UNADJUSTED_OHLCV.csv.gz").read_bytes()).hexdigest())
            with self.assertRaisesRegex(ValueError,"INSUFFICIENT_VALID_HOLDOUT"):
                evaluate(ROOT,out)

    def test_access_denied_is_explicit_stop(self):
        def fake_denied(code):
            raise PermissionError("Access denied")
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(PermissionError,"PROVIDER_PERMISSION_DENIED"):
                collect(ROOT,Path(directory),fetcher=fake_denied)


if __name__=="__main__":
    unittest.main()
