import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from expanded_historical_case_audit import expanded_sample, run
from historical_case_audit import add_preferred_sentinels, fixed_sample, load_panel


REPO = Path(__file__).resolve().parents[2]


class ExpandedCaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.panel, cls.input_sha = load_panel(REPO)

    def test_exact_75_plus_two_preserves_frozen_17(self):
        sample = expanded_sample(self.panel)
        frozen = add_preferred_sentinels(self.panel, fixed_sample(self.panel))
        self.assertEqual(len(sample), 77)
        self.assertEqual(len(frozen), 17)
        self.assertEqual(len(set(zip(sample.code, sample.date))), 77)
        self.assertTrue(set(zip(frozen.code, frozen.date)) <= set(zip(sample.code, sample.date)))

    def test_each_year_tier_has_predeclared_sorted_ranks(self):
        sample = expanded_sample(self.panel)
        raw = self.panel.loc[self.panel.D.eq(0)]
        for year in range(2021, 2026):
            for tier in ('LOW', 'MID', 'HIGH'):
                ordered = raw.loc[raw.year.eq(year) & raw.liquidity_tier.eq(tier)].sort_values(['code', 'date'])
                expected = set(zip(ordered.iloc[[0, 4, 9, 14, 18]].code,
                                   ordered.iloc[[0, 4, 9, 14, 18]].date))
                actual = set(zip(sample.loc[sample.year.eq(year) & sample.liquidity_tier.eq(tier)].code,
                                 sample.loc[sample.year.eq(year) & sample.liquidity_tier.eq(tier)].date))
                self.assertTrue(expected <= actual)

    def test_does_not_use_outcome_columns(self):
        altered = self.panel.copy()
        altered['future_return'] = 1000000
        altered['later_survived'] = False
        a = expanded_sample(self.panel)
        b = expanded_sample(altered)
        self.assertEqual(list(zip(a.code, a.date)), list(zip(b.code, b.date)))

    def test_frozen_panel_cohort_mutation_blocks(self):
        modified = self.panel.loc[~self.panel.year.eq(2024)]
        with self.assertRaisesRegex(ValueError, 'COHORT_INCOMPLETE'):
            expanded_sample(modified)

    def test_no_provider_is_success_claim_when_fake_quotes_are_incomplete(self):
        class NoReferenceProvider:
            def get_market_ohlcv_by_date(self, start, end, ticker, adjusted):
                self.assert_unadjusted(adjusted)
                raise RuntimeError('NO_PRICE_PROVIDER')

            @staticmethod
            def assert_unadjusted(adjusted):
                if adjusted is not False:
                    raise AssertionError('must request unadjusted source')

        with TemporaryDirectory(prefix='cb_env_77_test_') as temp:
            report = run(REPO, Path(temp), stock=NoReferenceProvider())
            self.assertEqual(report['fixed_event_count'], 77)
            self.assertEqual(report['fetch_or_panel_blocked_count'], 77)
            self.assertEqual(report['status'], 'BLOCKED_INCOMPLETE_OR_MISMATCH')
            self.assertFalse(report['can_publish_strategy_performance'])
            self.assertEqual(report['comparison_performance_status'], 'NOT_COMPUTED')
            self.assertEqual(len(report['strata']), 15)
            self.assertTrue((Path(temp) / 'EXPANDED_CASE_SELECTION.json').is_file())
            self.assertTrue((Path(temp) / 'EXPANDED_HISTORICAL_CASE_AUDIT_RESEARCH_ONLY.json').is_file())


if __name__ == '__main__':
    unittest.main()
