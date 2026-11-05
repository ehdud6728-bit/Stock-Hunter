#!/usr/bin/env python3
import json
import tempfile
import unittest
from pathlib import Path
from guard import audit, normalized_code

class GuardTests(unittest.TestCase):
    def test_known_valid_format(self):
        for raw, expected in [
            ('005930','005930'), ('000660','000660'), ('0126Z0','0126Z0'),
            ('A005930','005930'), ('005930.KS','005930'), (' 005930 ', '005930'),
            (5930, '005930')]:
            self.assertEqual(normalized_code(raw), (expected, ''))

    def test_reject_bad_and_no_digits_strip(self):
        expected = [('000000','PLACEHOLDER_000000'), ('00593-0','MALFORMED_CODE'),
                    ('1234567','MALFORMED_CODE'), ('bad005930','MALFORMED_CODE'),
                    (5930.0,'FLOAT_CODE_LOSES_IDENTITY'), ('','EMPTY_OR_BAD_TYPE')]
        for raw, reason in expected:
            self.assertEqual(normalized_code(raw), (None, reason))

    def test_realistic_snapshot_and_no_source_mutation(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            src = root/'today_candidates.json'
            doc = {'date':'2026-11-04','quick_candidates':[
                {'code':'000000','종목코드':'000000','rank':1},
                {'code':'005930','종목코드':'005930','현재가':60000},
                {'code':'000660','종목코드':'000660','현재가':100000},
            ]}
            src.write_text(json.dumps(doc,ensure_ascii=False),encoding='utf-8')
            original = src.read_bytes()
            result = audit([src],root/'report')
            self.assertEqual(result['checked_candidate_rows'],3)
            self.assertEqual(result['issues_by_reason'],{'PLACEHOLDER_000000':1})
            self.assertEqual(src.read_bytes(),original)
            self.assertEqual(result['membership_unverified'],2)

    def test_alias_conflict_duplicate_and_cross_group_ok(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td)
            src=root/'full.json'
            src.write_text(json.dumps({'date':'2026-11-04','groups':{
                'A':[{'code':'005930','종목코드':'000660'}, {'code':'005930'}, {'code':'005930'}],
                'B':[{'code':'005930'}]
            }},ensure_ascii=False),encoding='utf-8')
            r=audit([src],root/'out')
            self.assertEqual(r['issues_by_reason'],{'CONFLICTING_CODE_ALIASES':1,'DUPLICATE_IN_GROUP':1})

    def test_optional_membership_must_match_date(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td)
            f=root/'c.json'; f.write_text(json.dumps({'date':'2026-11-04','quick_candidates':[{'code':'005930'},{'code':'999999'}]}),encoding='utf-8')
            m=root/'m.csv';m.write_text('date,code,name\n2026-11-04,005930,삼성전자\n',encoding='utf-8')
            r=audit([f],root/'out',membership_path=m)
            self.assertEqual(r['issues_by_reason'],{'NOT_IN_SUPPLIED_KRX_MEMBERSHIP':1})
            self.assertEqual(r['membership']['status'],'USER_SUPPLIED_MEMBERSHIP_UNCERTIFIED')

if __name__=='__main__':unittest.main(verbosity=2)
