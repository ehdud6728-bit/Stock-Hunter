"""Network-free safety tests: synthetic responses are never market evidence."""
import json
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from r9_kis_readonly import (
    FOCAL_DATES, KIS_PERIOD_ENDPOINT, ROOT, SourceUnavailable,
    capture_data, run, safe_transport,
)

import sys
sys.path.insert(0, str(ROOT / "research_projects/trendline_price_lineage_r8_20261119"))
from r8_price_lineage import FROZEN, OHLC, rows


class FakeKIS:
    def __init__(self, *, missing=None, raw_discrepancy=False, bad_token=False, invalid_bar=False):
        self.calls = []
        self.frozen = rows(FROZEN, pinned=True)
        self.missing = missing
        self.raw_discrepancy = raw_discrepancy
        self.bad_token = bad_token
        self.invalid_bar = invalid_bar

    def __call__(self, method, path, headers, body=None):
        self.calls.append((method, path))
        if method == "POST":
            return ({"access_token": "TEST_ONLY_DO_NOT_EXPORT_TOKEN"} if not self.bad_token else {}), "a" * 64
        q = parse_qs(urlparse(path).query)
        flag = q["FID_ORG_ADJ_PRC"][0]
        data = []
        for date in FOCAL_DATES:
            if flag == "0" and date == self.missing:
                continue
            raw = [self.frozen[date][k] for k in OHLC]
            if flag == "0":
                raw = [int(v * 0.979 + 0.5) for v in raw]
            if flag == "1" and self.raw_discrepancy and date == "2023-11-06":
                raw[-1] += 500
            data.append({"stck_bsop_date": date.replace("-", ""),
                         "stck_oprc": str(raw[0]), "stck_hgpr": str(raw[1]),
                         "stck_lwpr": str(raw[2]), "stck_clpr": str(raw[3]),
                         "acml_vol": str(123456)})
        if self.invalid_bar:
            data[0]["stck_clpr"] = "not_a_price"
        return {"rt_cd": "0", "output2": data}, "b" * 64


class R9Tests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.out = Path(self.tempdir.name)

    def test_no_secrets_stays_pending_and_does_not_call_provider(self):
        fake = FakeKIS()
        r = run(self.out, environ={}, transport=fake)
        self.assertEqual(r["status"], "PENDING_DEDICATED_STOCK_HUNTER_R9_SECRETS")
        self.assertFalse(fake.calls)
        self.assertFalse(r["independent_price_lineage_certified"])

    def test_partial_secrets_are_blocked_without_network(self):
        fake = FakeKIS()
        r = run(self.out, environ={"R9_KIS_APP_KEY": "test"}, transport=fake)
        self.assertEqual(r["status"], "BLOCKED_PARTIAL_KIS_SECRETS")
        self.assertFalse(fake.calls)

    def test_two_modes_reuse_one_token_and_compare_offline(self):
        fake = FakeKIS()
        r = run(self.out, environ={"R9_KIS_APP_KEY": "KEY_TEST_ONLY", "R9_KIS_APP_SECRET": "SECRET_TEST_ONLY"}, transport=fake)
        self.assertEqual(r["status"], "FIVE_DATE_COMPARISON_COMPLETE_FULL_REPLAY_PENDING")
        self.assertEqual([m for m, _ in fake.calls], ["POST", "GET", "GET"])
        self.assertIn("FID_ORG_ADJ_PRC=1", fake.calls[1][1])
        self.assertIn("FID_ORG_ADJ_PRC=0", fake.calls[2][1])
        self.assertEqual(r["source_row_counts"], {"raw": 5, "adjusted": 5})
        self.assertFalse(r["independent_price_lineage_certified"])
        output = json.loads((self.out / "R9_R8_INDEPENDENT_COMPARISON.json").read_text(encoding="utf-8"))
        self.assertTrue(output["adjusted_fixed_anchor_line_cross"])
        self.assertFalse(output["r116_adjusted_full_replay_certified"])
        combined = "".join(path.read_text(encoding="utf-8") for path in self.out.iterdir() if path.is_file())
        for secret in ("KEY_TEST_ONLY", "SECRET_TEST_ONLY", "TEST_ONLY_DO_NOT_EXPORT_TOKEN"):
            self.assertNotIn(secret, combined)

    def test_wrong_independent_raw_is_not_approved(self):
        fake = FakeKIS(raw_discrepancy=True)
        r = run(self.out, environ={"R9_KIS_APP_KEY": "x", "R9_KIS_APP_SECRET": "y"}, transport=fake)
        self.assertEqual(r["status"], "INDEPENDENT_SOURCE_DISAGREEMENT_OR_MISSING_DATES")

    def test_missing_focal_bar_is_not_interpolated(self):
        fake = FakeKIS(missing="2023-09-19")
        r = run(self.out, environ={"R9_KIS_APP_KEY": "x", "R9_KIS_APP_SECRET": "y"}, transport=fake)
        self.assertEqual(r["status"], "INDEPENDENT_SOURCE_DISAGREEMENT_OR_MISSING_DATES")

    def test_token_failure_does_not_fabricate_data(self):
        fake = FakeKIS(bad_token=True)
        r = run(self.out, environ={"R9_KIS_APP_KEY": "x", "R9_KIS_APP_SECRET": "y"}, transport=fake)
        self.assertEqual(r["status"], "API_UNAVAILABLE_FAIL_CLOSED")
        self.assertEqual(len(fake.calls), 1)

    def test_bad_price_does_not_get_zero_filled(self):
        fake = FakeKIS(invalid_bar=True)
        r = run(self.out, environ={"R9_KIS_APP_KEY": "x", "R9_KIS_APP_SECRET": "y"}, transport=fake)
        self.assertEqual(r["status"], "API_UNAVAILABLE_FAIL_CLOSED")
        self.assertFalse((self.out / "KIS_006400_202309_202311_raw.csv").exists())

    def test_transport_rejects_trading_and_external_host_without_network(self):
        for path in ("/uapi/domestic-stock/v1/trading/order-cash", "https://evil.example.com/path"):
            with self.assertRaisesRegex(SourceUnavailable, "R9_DISALLOWED_ENDPOINT"):
                safe_transport("GET", path, {})
        with self.assertRaisesRegex(SourceUnavailable, "R9_DISALLOWED_POST_ENDPOINT"):
            safe_transport("POST", KIS_PERIOD_ENDPOINT, {})


if __name__ == "__main__":
    unittest.main()
