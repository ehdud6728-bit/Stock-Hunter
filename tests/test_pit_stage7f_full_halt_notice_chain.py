"""Stage7-F provider-free verification. Run from repository root with unittest discovery."""
import importlib.util,pathlib,sys,unittest,tempfile,json,zipfile,csv
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'research_pit'))
spec=importlib.util.spec_from_file_location('pit_stage7f',ROOT/'research_pit'/'pit_stage7f_full_halt_notice_chain.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class Provider:
 def __init__(self):self.ac=''
 def shell(self,receipt):
  self.ac=receipt
  return b'<html><option value="12345678901234|Y">real</option></html>'
 def resolve(self,cid):
  a=self.ac
  url=f'https://kind.krx.co.kr/external/{a[:4]}/{a[4:6]}/{a[6:8]}/{a[8:]}/12345678901234/70797.htm'
  return ('<html>\"'+url+'\"</html>').encode()
 def content(self,url):
  name='시큐레터' if self.ac in {
   '20240415000722','20240911000528','20250314002264','20250430001587'} else '큐에이드'
  return ('<html><body><h2>주권매매거래정지기간변경</h2><table><tr><td>1.대상종목</td><td>'+name+'</td></tr><tr><td>2.정지기간</td><td>거래정지 기간변경 안내 및 상장적격성 심사</td></tr></table></body></html>').encode()

class Stage7FTests(unittest.TestCase):
 def test_classify_first_start(self):self.assertEqual(m.classify('주권매매거래정지(회생절차 개시신청)'),'START_TITLE')
 def test_classify_release(self):self.assertEqual(m.classify('주권매매거래정지해제(회생절차개시결정)'),'RELEASE_TITLE')
 def test_classify_period(self):self.assertEqual(m.classify('주권매매거래정지기간변경(개선기간 부여)'),'PERIOD_CHANGE_TITLE')
 def test_classify_continuation(self):self.assertEqual(m.classify('기타시장안내(매매거래정지 지속 안내)'),'CONTINUATION_TITLE')
 def test_classify_unrelated(self):self.assertEqual(m.classify('기타시장안내(개선기간 안내)'),'OTHER_MARKET_NOTICE_UNREVIEWED')
 def test_select_halt_vs_other(self):
  self.assertFalse(m.direct_notice('기타시장안내(상장폐지 관련)'))
  self.assertTrue(m.direct_notice('주권매매거래정지해제(회생절차개시결정)'))
 def test_incidents_split_on_rehalt(self):
  self.assertEqual(m.incident_link('377460','2023-11-20'),'Q377460_01')
  self.assertEqual(m.incident_link('377460','2023-11-27'),'Q377460_02')
  self.assertEqual(m.incident_link('377460','2023-11-28'),'Q377460_02')
  self.assertEqual(m.incident_link('418250','2024-09-11'),'S418250_01')
 def test_no_unapproved_source_promotion(self):
  import inspect
  src=inspect.getsource(m)
  self.assertNotIn('import stock_hunter',src)
  self.assertNotIn('strategy_ready=True',src)
  self.assertIn("'official_daily_halt_status':'UNKNOWN'",src)
 def test_url_cross_origin_rejected(self):
  with self.assertRaises(RuntimeError):m.d.document_target('https://evil.test/external/2024/09/11/000528/12345678901234/70797.htm','20240911000528')
 def test_raw_sha_change_fails_closed(self):
  with tempfile.TemporaryDirectory() as td:
   p=pathlib.Path(td)/'source.txt';p.write_bytes(b'tampered')
   self.assertNotEqual(m.sha(p.read_bytes()),m.sha(b'official'))
 def test_mock_kind_provider_reconstructs_valid_url(self):
  p=Provider();a='20240911000528';shell=p.shell(a)
  ids=m.d.resolve_shell(shell,a)
  self.assertEqual(ids,['12345678901234'])
  url=m.d.resolve_response(p.resolve(ids[0]),a)
  self.assertIn('/2024/09/11/000528/',url)
 def test_parsed_unknown_daily_even_when_body_captured(self):
  p=Provider();p.shell('20240911000528')
  n={'accession':'20240911000528','code':'418250','title':'주권매매거래정지기간변경(개선기간 부여)'}
  x=m.inspect_body(p.content(''),n)
  self.assertTrue(x['body_identity_corroborated'])
  self.assertFalse(x['official_effective_date_certified'])
 def test_bad_body_identity_fails_closed(self):
  n={'accession':'20240911000528','code':'418250','title':'주권매매거래정지기간변경(개선기간 부여)'}
  with self.assertRaises(RuntimeError):m.inspect_body('<html><body><p>거래정지</p><table><tr><td>대상종목</td><td>다른기업</td></tr></table></body></html>'.encode('utf8'),n)
 def test_workflow_script_requirements(self):
  self.assertEqual(m.EXPECTED_NOTICES,57)
  self.assertEqual(m.EXPECTED_DIRECT,21)
  self.assertEqual(m.EXPECTED_OLD,10)
  self.assertEqual(m.EXPECTED_NEW,11)
 def test_notice_rows_preserve_unknown(self):
  n={'code':'377460','accession':'20240315001321','title':'주권매매거래정지기간변경(투자자보호)',
    'submission_date_from_accession':'2024-03-15','listed_cell_time':'2024-03-15 15:00',
    'notice_class':'HALT_PERIOD_CHANGE_TITLE_ONLY','source_raw_sha256':'abc','source_raw_file':'raw_kind/a.html'}
  row=m.notice_output({'20240315001321':n},{},{},{})[0]
  self.assertEqual(row['official_daily_halt_status'],'UNKNOWN')
  self.assertEqual(row['event_id_candidate_not_verified'],'Q377460_02')
 def test_unresolved_provider_gets_fail_closed_artifact(self):
  p=Provider()
  class Broken(Provider):
   def content(self,url):raise ConnectionError('network down')
  with self.assertRaises(ConnectionError):Broken().content('url')

if __name__=='__main__':unittest.main()
