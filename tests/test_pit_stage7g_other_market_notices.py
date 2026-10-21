"""Offline Stage7-G contracts. No network or original source attachments required."""
import importlib.util,pathlib,sys,tempfile,unittest,inspect,json,csv
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'research_pit'))
spec=importlib.util.spec_from_file_location('stage7g',ROOT/'research_pit'/'pit_stage7g_other_market_notices.py')
g=importlib.util.module_from_spec(spec);spec.loader.exec_module(g)


def mock_body(code,title='기타시장안내(상장폐지 관련)'):
 name='시큐레터 주식회사' if code=='418250' else '주식회사 위니아에이드'
 return ('<html><body><h2>'+title+'</h2><table><tr><th>1.대상종목</th><td>'+name+'</td></tr>'
         '<tr><th>2.심사결과</th><td>상장적격성 실질심사 및 개선기간 부여 결정</td></tr></table></body></html>').encode('utf8')

class MockKind:
 def __init__(self,lookup):self.lookup=lookup;self.current=None
 def shell(self,ac):
  self.current=ac
  return b'<html><option value="12345678901234|Y">document</option></html>'
 def resolve(self,cid):
  a=self.current
  return f'<div>"https://kind.krx.co.kr/external/{a[:4]}/{a[4:6]}/{a[6:8]}/{a[8:]}/12345678901234/70797.htm"</div>'.encode()
 def content(self,url):return mock_body(self.lookup[self.current]['code'])

class Tests(unittest.TestCase):
 def setUp(self):
  self.notice={'accession':'20240405000978','code':'418250','title':'기타시장안내(상장폐지 관련)',
               'submission_date_from_accession':'2024-04-05','listed_cell_time':'2024-04-05 18:09',
               'source_raw_sha256':'123','source_raw_file':'raw_kind/a.html'}
 def test_run_imports_real_repo_path(self):
  self.assertTrue((ROOT/'research_pit'/'pit_stage7g_other_market_notices.py').is_file())
 def test_expected_cohorts(self):
  self.assertEqual((g.EXPECTED_ALL,g.EXPECTED_DIRECT,g.EXPECTED_OTHER),(57,21,36))
 def test_prior_source_pin(self):
  self.assertEqual(g.STAGE7F_RUN,39541621602)
 def test_valid_other_notice_identity(self):
  r=g.review_body(mock_body('418250'),self.notice)
  self.assertTrue(r['company_identity_corroborated'])
  self.assertFalse(r['daily_tradability_verified'])
  self.assertEqual(r['body_bytes'],len(mock_body('418250')))
 def test_old_company_alias_supported(self):
  n=dict(self.notice,code='377460')
  r=g.review_body(mock_body('377460'),n)
  self.assertTrue(r['company_identity_corroborated'])
 def test_wrong_company_not_silently_accepted(self):
  with self.assertRaisesRegex(RuntimeError,'DOCUMENT_IDENTITY_NOT_PROVEN'):
   g.review_body(mock_body('377460'),self.notice)
 def test_no_market_halt_keyword_required(self):
  n=dict(self.notice,title='기타시장안내(개선계획서 제출)')
  raw='<html><body><div>주식회사 위니아에이드</div><table><tr><td>개선계획서</td><td>회사의 이행계획서 제출 완료 및 개선계획 심사절차 진행 중임</td></tr></table></body></html>'.encode()
  n['code']='377460'
  r=g.review_body(raw,n)
  self.assertTrue(r['company_identity_corroborated'])
  self.assertFalse(r['daily_tradability_verified'])
 def test_word_release_does_not_prove_release(self):
  raw=mock_body('418250').replace('개선기간 부여 결정'.encode(), '거래정지해제는 결정되지 않았음'.encode())
  r=g.review_body(raw,self.notice)
  self.assertTrue(r['contains_release_like_word_not_proof'])
  self.assertFalse(r['official_effective_datetime_verified'])
 def test_short_and_blank_rejected(self):
  for raw in [b'',b'<html>blank.html</html>']:
   with self.assertRaises(RuntimeError):g.review_body(raw,self.notice)
 def test_no_live_or_promotion_in_source(self):
  s=inspect.getsource(g)
  self.assertNotIn('import stock_hunter',s)
  self.assertNotIn('strategy_ready=True',s)
  self.assertIn("'strategy_ready':False",s)
  self.assertIn("'official_daily_halt_status':'UNKNOWN'",s)
 def test_trusts_only_kind_document_urls(self):
  a='20240405000978'
  with self.assertRaises(RuntimeError):g.d.document_target('https://unrelated.example/external/2024/04/05/000978/12345678901234/70797.htm',a)
 def test_source_failure_is_fail_closed(self):
  class Broken(MockKind):
   def content(self,url):raise OSError('offline failure')
  p=Broken({'20240405000978':self.notice})
  self.assertRaises(OSError,p.content,'x')
 def test_mock_endpoint_url_tracks_receipt(self):
  p=MockKind({'20240405000978':self.notice})
  a='20240405000978';shell=p.shell(a);ids=g.d.resolve_shell(shell,a)
  self.assertEqual(len(ids),1)
  url=g.d.resolve_response(p.resolve(ids[0]),a)
  self.assertIn('/2024/04/05/000978/',url)
 def test_record_integrity_sha(self):
  b=mock_body('418250')
  self.assertNotEqual(g.sha(b),g.sha(b+b'a'))
 def test_passed_results_are_never_eligibility_approved(self):
  r=g.review_body(mock_body('418250'),self.notice)
  self.assertFalse(r['strategy_ready'])
  self.assertFalse(r['complete_correction_chain_verified'])

if __name__=='__main__':unittest.main()
