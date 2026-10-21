"""Stage7-D no-network KIND full-document canary security and anti-lookahead checks."""
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'research_pit'))
import pit_stage7d_kind_full_document as m

RECEIPT='20231120000599'
VALID='https://kind.krx.co.kr/external/2023/11/20/000599/20231120000999/70799.htm'
NOTICE={'code':'377460','accession':RECEIPT,'title':'주권매매거래정지해제(회생절차개시결정)',
       'submission_date_from_accession':'2023-11-20','listed_cell_time':'2023-11-20 17:31',
       'notice_class':'POSSIBLE_RELEASE_NOTICE_TITLE_ONLY'}


def shell():return b"<html><option value='20231120000999|Y'>1</option></html>"
def resolver():return ('<script>document.location="'+VALID+'";</script>').encode('euc-kr')
def body():
    return ('<html><body><h2>주권매매거래정지해제(회생절차개시결정)</h2><h3>큐에이드</h3>'
            '<table><tr><th>해제일시</th><td>2023년 11월 21일</td></tr>'
            '<tr><th>정지 사유</th><td>주권매매거래정지 해제</td></tr></table></body></html>').encode('utf-8')

class Provider:
    def shell(self,receipt):
        return ("<html><option value='"+receipt+"|Y'>current</option></html>").encode()
    def resolve(self,cid):
        url=f"https://kind.krx.co.kr/external/{cid[:4]}/{cid[4:6]}/{cid[6:8]}/{cid[8:]}/{cid}/70799.htm"
        return ('<script>document.location="'+url+'";</script>').encode()
    def content(self,url):return body()

class Tests(unittest.TestCase):
    def test_fixed_target_list_and_no_live(self):
        self.assertEqual(len(m.REQUIRED),10)
        self.assertIn(RECEIPT,m.REQUIRED)
        src=(ROOT/'research_pit/pit_stage7d_kind_full_document.py').read_text()
        self.assertNotIn('place_order(',src)
        self.assertNotIn('strategy_ready=True',src)
        self.assertNotIn('LIVE_ENABLE=True',src)

    def test_valid_document_url_is_receipt_scoped(self):
        self.assertEqual(m.document_target(VALID,RECEIPT),VALID)
        for bad in (
            VALID.replace('kind.krx.co.kr','example.com'),
            VALID.replace('https','http'),
            VALID.replace('000599','000598'),
            VALID.replace('/external/','/internal/'),
            VALID.replace('/2023/11/20/','/2023/11/19/'),
            VALID.replace('/70799.htm','/70799.php'),
            'https://kind.krx.co.kr/external/2023/11/20/000599/20231120000999/70799.htm/../x.htm',
        ):
            with self.subTest(url=bad),self.assertRaises(RuntimeError):m.document_target(bad,RECEIPT)

    def test_resolver_raw_url(self):
        self.assertEqual(m.resolve_response(resolver(),RECEIPT),VALID)
        with self.assertRaisesRegex(RuntimeError,'NO_VALID_FULL_DOCUMENT_URL'):
            m.resolve_response(b'blank.html',RECEIPT)

    def test_shell_current_version_first(self):
        raw=("<option value='20231120000998|N'>x</option>"
             "<option value='20231120000999|Y'>x</option>").encode()
        self.assertEqual(m.resolve_shell(raw,RECEIPT),['20231120000999','20231120000998'])
        with self.assertRaisesRegex(RuntimeError,'NO_CONTENT_IDS'):
            m.resolve_shell(b'<html>no document</html>',RECEIPT)

    def test_date_label_candidate_is_not_approved_resume(self):
        e=m.interpret_document(body(),NOTICE)
        self.assertTrue(e['body_corroborates_official_company_or_code'])
        self.assertTrue(e['document_mentions_market_action'])
        self.assertEqual(e['parse_state'],'DOCUMENT_CAPTURED_CANDIDATES_ONLY')
        self.assertFalse(e['official_resume_effective_date_verified'])
        self.assertFalse(e['daily_trade_eligibility_verified'])
        self.assertIn('2023년 11월 21일',json.dumps(e,ensure_ascii=False))

    def test_document_wrong_company_keeps_unknown(self):
        raw=body().replace('큐에이드'.encode(),'삼성전자'.encode())
        e=m.interpret_document(raw,NOTICE)
        self.assertFalse(e['body_corroborates_official_company_or_code'])
        self.assertEqual(e['parse_state'],'DOCUMENT_CAPTURED_IDENTITY_OR_ACTION_UNCORROBORATED')
        self.assertFalse(e['official_resume_effective_date_verified'])

    def test_small_or_blank_body_fails(self):
        for v in (b'',b'<html>blank.html</html>',b'abc'):
            with self.assertRaises(RuntimeError):m.interpret_document(v,NOTICE)

    def test_run_preserves_provider_raw_and_research_gate(self):
        index={r:dict(NOTICE,accession=r,submission_date_from_accession=f'{r[:4]}-{r[4:6]}-{r[6:8]}') for r in m.REQUIRED}
        ref={'mock_staged':True}
        with tempfile.TemporaryDirectory() as tmp,patch.object(m,'stage7c_preflight',return_value=(index,ref)):
            summary=m.run('unused',tmp,provider=Provider(),pause=0)
            self.assertEqual(summary['requested_official_receipts'],10)
            self.assertEqual(summary['full_document_resolved_count'],10)
            self.assertFalse(summary['strategy_ready'])
            self.assertFalse(summary['official_resume_effective_date_verified'])
            self.assertFalse(summary['daily_tradability_verified'])
            mf=m.read_json(pathlib.Path(tmp)/'raw_full_document_sha_manifest.json')
            self.assertEqual(len(mf['files']),30)
            q=mf['files'][0]
            self.assertEqual(m.sha((pathlib.Path(tmp)/q['path']).read_bytes()),q['sha256'])
            self.assertTrue((pathlib.Path(tmp)/'stage7d_labeled_table_candidates_for_manual_review.json').exists())

    def test_unresolved_body_cannot_approve(self):
        class NoBody(Provider):
            def content(self,url):raise RuntimeError('source unavailable')
        index={r:dict(NOTICE,accession=r,submission_date_from_accession=f'{r[:4]}-{r[4:6]}-{r[6:8]}') for r in m.REQUIRED}
        with tempfile.TemporaryDirectory() as tmp,patch.object(m,'stage7c_preflight',return_value=(index,{})):
            with self.assertRaisesRegex(RuntimeError,'ALL_TEN_FULL_DOCUMENT_FETCHES_UNRESOLVED'):
                m.run('unused',tmp,provider=NoBody(),pause=0)
            s=m.read_json(pathlib.Path(tmp)/'stage7d_kind_full_document_summary.json')
            self.assertEqual(s['full_document_resolved_count'],0)
            self.assertFalse(s['strategy_ready'])
            self.assertEqual(len(m.read_json(pathlib.Path(tmp)/'raw_full_document_sha_manifest.json')['files']),20)

    def test_stage7c_source_summary_guard(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=pathlib.Path(tmp)
            m.dump(root/'stage7c_kind_resume_source_summary.json',{'strategy_ready':True})
            with self.assertRaisesRegex(RuntimeError,'PINS_DRIFT'):
                m.stage7c_preflight(root)

    def test_stage7c_manifest_missing_guard(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=pathlib.Path(tmp)
            m.dump(root/'stage7c_kind_resume_source_summary.json',{
                'schema':m.prior.SCHEMA,'status':'KIND_MARKET_DISCLOSURE_SOURCE_CANDIDATES_ONLY',
                'request_count':14,'notice_candidate_count':57,'possible_release_title_count':1,
                'matched_codes':['377460','418250'],'strategy_ready':False,
                'official_resume_effective_date_verified':False,
                'source_stage7b':{'stage7b_source_run':m.STAGE7B_RUN}})
            m.dump(root/'source_manifest.json',{'status':'PARTIAL','files':[]})
            with self.assertRaisesRegex(RuntimeError,'MANIFEST_DRIFT'):
                m.stage7c_preflight(root)

if __name__=='__main__':unittest.main()
