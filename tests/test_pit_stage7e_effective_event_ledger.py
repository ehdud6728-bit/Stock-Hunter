"""Provider-free Stage7-E source parsing and fail-closed regression tests."""
import importlib.util,json,pathlib,sys,tempfile,unittest
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parents[1]/'research_pit'
spec=importlib.util.spec_from_file_location('stage7e',ROOT/'pit_stage7e_effective_event_ledger.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

DUMMY={'notice_date':'2023-11-20','notice_time_display':'2023-11-20 17:31','raw_document_body_sha256':'abcd','resolved_content_url':'https://kind.krx.co.kr/external/2023/11/20/test.htm'}
def body(rows,headline='주권매매거래정지'):
 return ('<html><body><h2>'+headline+'</h2><table>'+''.join('<tr>'+''.join(f'<td>{c}</td>' for c in row)+'</tr>' for row in rows)+'</table></body></html>').encode()
def subject(name):return ['1.대상종목',name,'보통주 (KRX 공식 공시문서 발행종목)']

class Offline(unittest.TestCase):
 def test_first_suspend_effective_from_structured_table(self):
  html=body([subject('주식회사 위니아에이드'),['3.정지기간','가.정지일시','2023-10-27','-']])
  o=m.parse_one('20231026000748',html,DUMMY)
  self.assertEqual(o['document_declared_effective_start_or_repeat_date'],'2023-10-27')
  self.assertEqual(o['action'],'HALT_START')
  self.assertFalse(o['daily_tradability_verified'])
 def test_release_date_is_future_of_publication_date(self):
  html=body([subject('주식회사 위니아에이드'),['3.해제일시','2023-11-21','-']],headline='주권매매거래정지해제')
  o=m.parse_one('20231120000599',html,DUMMY)
  self.assertEqual(o['document_declared_resume_effective_date'],'2023-11-21')
  self.assertEqual(o['notice_display_time'],'2023-11-20 17:31')
  self.assertFalse(o['full_correction_chain_verified'])
 def test_seculeter_intra_day_stop_time(self):
  o=m.parse_one('20240405000391',body([subject('시큐레터 주식회사'),['3.정지기간','가.정지일시','2024-04-05','14:11:00']]),DUMMY)
  self.assertEqual(o['document_declared_effective_start_or_repeat_time'],'14:11:00')
 def test_later_extension_cannot_become_resume(self):
  h=body([subject('시큐레터 주식회사'),['주권매매거래정지 기간변경'],['3.정지기간','나.변경후',"2024년 04월 05일 14:11:00~ 개선기간 종료('27.03.22) 후 상장폐지 여부 결정일까지"]])
  o=m.parse_one('20260722000930',h,DUMMY)
  self.assertEqual(o['document_declared_effective_start_or_repeat_date'],'2024-04-05')
  self.assertEqual(o['document_declared_resume_effective_date'],'')
  self.assertTrue(o['document_repeated_original_start_not_new_resume'])
 def test_wrong_identity_fails_closed(self):
  with self.assertRaisesRegex(RuntimeError,'WRONG_ISSUE_IDENTITY'):
   m.parse_one('20231120000599',body([subject('시큐레터 주식회사'),['3.해제일시','2023-11-21','-']]),DUMMY)
 def test_wrong_original_date_fails_closed(self):
  with self.assertRaisesRegex(RuntimeError,'START_OR_CHANGED_DATE_DRIFT'):
   m.parse_one('20231026000748',body([subject('주식회사 위니아에이드'),['3.정지기간','가.정지일시','2023-10-26','-']]),DUMMY)
 def test_release_requires_unambiguous_single_label(self):
  h=body([subject('주식회사 위니아에이드'),['3.해제일시','2023-11-21','-'],['3.해제일시','2023-11-22','-']])
  with self.assertRaisesRegex(RuntimeError,'ACTION_KEY_CARDINALITY'):
   m.parse_one('20231120000599',h,DUMMY)
 def test_labeled_dates_not_reinterpreted_as_trade_permission(self):
  sample=[{'receipt':'20231026000748','event_id':'Q377460_01','action':'HALT_START','document_declared_effective_start_or_repeat_date':'2023-10-27','document_declared_effective_start_or_repeat_time':''},
          {'receipt':'20231120000599','event_id':'Q377460_01','action':'HALT_RELEASE','document_declared_resume_effective_date':'2023-11-21','document_declared_effective_start_or_repeat_time':''}]
  prices={('377460','2023-10-27'):{'volume':0},('377460','2023-11-21'):{'volume':9889897}}
  # With incomplete event input, the ledger must not falsely approve the entire 3-incident history.
  with self.assertRaises(Exception):m.build_ledger(sample,prices)
 def test_false_positive_price_on_intraday_event_allowed(self):
  self.assertEqual(m.START_DATES['S418250_01'],('2024-04-05','14:11:00'))
  src=(ROOT/'pit_stage7e_effective_event_ledger.py').read_text()
  self.assertIn('anchor[\'volume\']!=51071',src)
  self.assertIn("'daily_tradability':'UNKNOWN'",src)
 def test_denies_missing_raw_sha(self):
  with tempfile.TemporaryDirectory() as t:
   root=pathlib.Path(t)
   m.save(root/'stage7d_kind_full_document_summary.json',{'status':'KIND_FULL_DOCUMENT_CANARY_COLLECTED_CANDIDATE_EVIDENCE_ONLY','requested_official_receipts':10,'full_document_resolved_count':10,'retrieved_raw_provider_files':30,'document_unresolved_count':0,'source_stage7c':{'source_stage7c_run':m.RUN7C,'source_stage7c_commit':m.PINNED7C_SHA,'stage7c_manifest_sha256':m.PINNED7D_SOURCE7C_MANIFEST},'strategy_ready':False,'official_resume_effective_date_verified':False})
   m.save(root/'raw_full_document_sha_manifest.json',{'files':[{'path':'none'}]*30})
   with self.assertRaisesRegex(RuntimeError,'RAW_PATH_OR_IDENTITY_INVALID'):m.source7d(root)
 def test_three_event_ids_with_released_first_incident_but_all_daily_unknown(self):
  actions=[]
  for ac,(code,act,eid) in m.POLICY.items():
   start,tm=m.START_DATES[eid]
   actions.append({'receipt':ac,'event_id':eid,'action':act,
    'document_declared_effective_start_or_repeat_date':'2023-11-21' if act=='HALT_RELEASE' else start,
    'document_declared_resume_effective_date':'2023-11-21' if act=='HALT_RELEASE' else '',
    'document_declared_effective_start_or_repeat_time':tm if act in ('HALT_START','HALT_PERIOD_CHANGE') else ''})
  prices={('377460','2023-10-27'):{'volume':0},('377460','2023-11-21'):{'volume':9889897},
          ('377460','2023-11-28'):{'volume':0},('418250','2024-04-05'):{'volume':51071}}
  ledger=m.build_ledger(actions,prices)
  self.assertEqual(len(ledger),3)
  self.assertEqual(ledger[0]['official_release_effective_date_from_body'],'2023-11-21')
  self.assertEqual(ledger[1]['official_release_effective_date_from_body'],'')
  self.assertEqual(ledger[2]['official_halt_start_time_from_body'],'14:11:00')
  self.assertTrue(all(r['execution_feasibility']=='UNKNOWN' and not r['strategy_ready'] for r in ledger))
 def test_live_production_not_imported(self):
  script=(ROOT/'pit_stage7e_effective_event_ledger.py').read_text()
  self.assertNotIn('place_order(',script)
  self.assertNotIn('from bot',script)
  self.assertNotIn('score_rank(',script)
  self.assertIn("'strategy_ready':False",script)
 def test_policy_has_exact_target_incidents(self):
  self.assertEqual(len(m.POLICY),10)
  self.assertEqual(set(e for _,_,e in m.POLICY.values()),{'Q377460_01','Q377460_02','S418250_01'})
  self.assertEqual(sum(x[1]=='HALT_RELEASE' for x in m.POLICY.values()),1)

if __name__=='__main__':unittest.main()
