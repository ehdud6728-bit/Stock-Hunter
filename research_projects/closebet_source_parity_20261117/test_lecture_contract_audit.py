import unittest
from pathlib import Path

from lecture_contract_audit import inspect


REPO = Path(__file__).resolve().parents[2]


class LectureContractAuditTests(unittest.TestCase):
    def test_existing_scanner_does_not_mislabel_20pct_lecture_as_10pct(self):
        result = inspect(REPO)
        statuses = {x['id']: x['status'] for x in result['contracts']}
        self.assertEqual(statuses['ENV_BAND_WIDTH'], 'CONFIRMED_DEFINITION_MISMATCH')
        self.assertEqual(statuses['ENV_TOUCH_TIME'], 'CONFIRMED_EVENT_DEFINITION_MISMATCH')
        self.assertEqual(statuses['ENV_ANALYZER_ONLY'], 'BAND_WIDTH_ONLY_MATCH_NO_EXECUTION_EQUIVALENCE')
        self.assertEqual(statuses['CLOSEBET_AMOUNT500'], 'TRUE_AMOUNT_GATE_UNCERTIFIED_PROXY_PATH_DETECTED')
        self.assertFalse(result['can_publish_strategy_performance'])
        self.assertFalse(result['definitions_reconciled_for_real_trading'])

    def test_docs_and_source_provenance_are_recorded(self):
        result = inspect(REPO)
        self.assertEqual(len(result['inputs']), 3)
        self.assertTrue(all(len(x['sha256']) == 64 for x in result['inputs']))
        self.assertEqual(len(result['lecture_documents']), 2)
        self.assertEqual(result['comparison_performance_status'], 'NOT_COMPUTED')


if __name__ == '__main__':
    unittest.main()
