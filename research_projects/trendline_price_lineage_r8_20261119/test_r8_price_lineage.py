"""Source and lookahead guards for R8's *fixed-anchor* price audit."""
import csv
import json
import tempfile
import unittest
from pathlib import Path

from r8_price_lineage import (
    FOCAL_DATES, FROZEN, KIS_PERIOD_ENDPOINT, OHLC, ProvenanceError,
    audit, price_line, rows, sha256,
)


class R8AuditTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name)
        self.frozen = rows(FROZEN, pinned=True)

    def fixtures(self, *, tamper_raw=False, absent_adjusted_date=None, wrong_flag=False):
        raw_csv, adj_csv = self.root / "kis_raw.csv", self.root / "kis_adjusted.csv"
        for path, adjusted in ((raw_csv, False), (adj_csv, True)):
            with path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["code", "date", *OHLC])
                for d in FOCAL_DATES:
                    if adjusted and d == absent_adjusted_date:
                        continue
                    values = [self.frozen[d][p] for p in OHLC]
                    if adjusted:
                        values = [int(x * 0.979 + 0.5) for x in values]
                    elif tamper_raw and d == "2023-11-06":
                        values[-1] += 500
                    writer.writerow(["006400", d, *values])
        manifests = []
        for path, flag in ((raw_csv, "0" if wrong_flag else "1"), (adj_csv, "0")):
            manifest = path.with_suffix(".json")
            manifest.write_text(json.dumps({
                "provider": "KIS", "endpoint": KIS_PERIOD_ENDPOINT,
                "symbol": "006400", "period": "D", "fid_org_adj_prc": flag,
                "csv_sha256": sha256(path), "captured_at_utc": "2026-11-19T00:00:00Z"
            }), encoding="utf-8")
            manifests.append(manifest)
        return raw_csv, adj_csv, *manifests

    def test_no_external_prices_must_remain_unknown(self):
        r = audit()
        self.assertEqual(r["independent_status"], "UNKNOWN_MISSING_BOTH_KIS_SERIES")
        self.assertIsNone(r["adjustment_ratio_uniform"])
        self.assertFalse(r["r116_adjusted_full_replay_certified"])

    def test_ex_rights_ratio_is_only_three_decimal_agreement(self):
        r = audit()
        ratio = r["2025_rights_issue_candidate"]["reference_to_preclose_ratio"]
        self.assertEqual(round(ratio, 3), 0.979)
        self.assertNotEqual(ratio, 0.979)
        self.assertFalse(r["2025_rights_issue_candidate"]["causality_proven"])
        self.assertLessEqual(max(abs(v) for v in r["screenshot_raw_times_0_979_residuals"].values()), 0.5)
        self.assertEqual(r["screenshot_minus_exact_ex_rights_ratio_projection"],
                         {"open": -16, "high": -17, "low": -15, "close": -17})

    def test_frozen_raw_anchor_line_independent_of_external_input(self):
        line = price_line(self.frozen, self.frozen)
        self.assertEqual(line["anchor_body_top"], {"2023-09-19": 566000, "2023-10-18": 522000})
        self.assertEqual(line["slope_per_trading_bar"], -2750)
        self.assertTrue(line["nov06_cross_from_nov03_close"])

    def test_matching_fixture_still_does_not_certify_full_replay(self):
        r = audit(*self.fixtures())
        self.assertTrue(r["raw_source_agrees_on_five_dates"])
        self.assertTrue(r["adjustment_ratio_uniform"])
        self.assertTrue(r["adjusted_fixed_anchor_line_cross"])
        self.assertTrue(r["HTS_matches_independent_adjusted_OHLC"])
        self.assertFalse(r["r116_adjusted_full_replay_certified"])
        self.assertIn("FIVE_DATE_COMPARISON_COMPLETE", r["independent_status"])

    def test_mismatched_independent_raw_stops_inference(self):
        r = audit(*self.fixtures(tamper_raw=True))
        self.assertEqual(r["independent_status"], "RAW_SOURCE_DISAGREEMENT")
        self.assertIsNone(r["adjustment_ratio_uniform"])

    def test_missing_date_is_unknown_not_imputed(self):
        r = audit(*self.fixtures(absent_adjusted_date="2023-09-19"))
        self.assertEqual(r["independent_status"], "UNKNOWN_MISSING_REQUIRED_DATES")
        self.assertIn("2023-09-19", r["missing_focal_dates"]["adjusted"])

    def test_wrong_api_flag_fails_closed(self):
        with self.assertRaisesRegex(ProvenanceError, "MANIFEST_MISMATCH fid_org_adj_prc"):
            audit(*self.fixtures(wrong_flag=True))

    def test_missing_manifest_is_error_not_fake_confirmation(self):
        raw, adj, _, m_adj = self.fixtures()
        with self.assertRaisesRegex(ProvenanceError, "BOTH_SERIES_AND_BOTH_MANIFESTS_REQUIRED"):
            audit(raw, adj, None, m_adj)

    def test_changed_csv_after_manifest_is_rejected(self):
        raw, adj, m_raw, m_adj = self.fixtures()
        with raw.open("a", encoding="utf-8") as f:
            f.write("\n")
        with self.assertRaisesRegex(ProvenanceError, "MANIFEST_MISMATCH csv_sha256"):
            audit(raw, adj, m_raw, m_adj)


if __name__ == "__main__":
    unittest.main()
