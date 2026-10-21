"""Stage7-H source-independent, no-network, 16+ strict fail-closed tests."""
import importlib.util,json,pathlib,sys,tempfile,unittest,hashlib,inspect
ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'research_pit'))
sp=importlib.util.spec_from_file_location('stage7h',ROOT/'research_pit'/'pit_stage7h_57_notice_correction_chain.py')
h=importlib.util.module_from_spec(sp);sp.loader.exec_module(h)

def record(receipt='20240318000555',typ='full_body',data=b'<html><body>'+b'A'*110+b'</body></html>',prefix='raw_kind_other'):
 ext='htm' if typ=='full_body' else 'html'
 return {'receipt':receipt,'type':typ,'path':f'{prefix}/{receipt}_{typ}.{ext}','bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()},data

def fixture(root,receipts=('20240318000555',),prefix='raw_kind_other'):
 entries=[]
 for rec in receipts:
  for typ in ('viewer_shell','searchContents_00','full_body'):
   ent,data=record(rec,typ,prefix=prefix)
   p=root/ent['path'];p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
   entries.append(ent)
 (root/'manifest.json').write_text(json.dumps({'status':'COMPLETE','files':entries}))
 return entries

def example(ac,title='기타시장안내(상장적격성 실질심사 관련 절차 안내)',is_corr=False,versions=('a',)):
 return {'receipt':ac,'code':'377460','notice_date':'2024-03-18','title':title,
  'correction_in_body':is_corr,'viewer_content_ids':list(versions)}

class Tests(unittest.TestCase):
 def test_provider_free(self):
  src=inspect.getsource(h)
  self.assertNotIn('requests.get(',src);self.assertNotIn('requests.post(',src)
 def test_exact_expected_cohort(self):
  self.assertEqual(h.COUNT['c'],57);self.assertEqual(h.COUNT['d']+h.COUNT['f']+h.COUNT['g'],57)
 def test_only_unknown(self):self.assertEqual(h.STATES,{'UNKNOWN'})
 def test_exact_source_runs(self):
  self.assertEqual(h.RUNS['G'],39570031470);self.assertEqual(h.RUNS['E'],39530243566)
 def test_sha_true(self):self.assertEqual(h.sha(b'x'),hashlib.sha256(b'x').hexdigest())
 def test_normalized_title(self):self.assertEqual(h.normalized_title('상장 적격 성'), '상장적격성')
 def test_no_correction_by_ordinary_context(self):
  self.assertFalse(h.is_correction('추후 매매거래정지 해제에 관한 사항을 안내할 예정입니다'))
 def test_detect_explicit_correction(self):
  self.assertTrue(h.is_correction('정정신고(보고) 정정일자 2024-03-18 정정사항 정정전 정정후'))
 def test_no_false_correct_by_notice_title(self):
  self.assertFalse(h.is_correction('기타시장안내(상장폐지 관련)'))
 def test_version_option_evidence_not_approval(self):
  b=b'<option value="20240318000235|N"></option><option value="20240318005964|Y"></option>'
  x=h.version_flags(b);self.assertEqual(len(x),2);self.assertEqual(x[1]['current_viewer_flag'],'Y')
 def test_bad_version_flag_fails(self):
  with self.assertRaisesRegex(RuntimeError,'CONTRADICTORY_VERSION_FLAG'):
   h.version_flags(b'<option value="20240318000235|N"><option value="20240318000235|Y">')
 def test_same_title_candidate_link_not_certified(self):
  edges=h.candidate_links([example('20240318000555',versions=('a','b')),
             example('20240318001339',is_corr=True,versions=('a','b'))])
  self.assertEqual(len(edges),1)
  self.assertEqual(edges[0]['original_receipt_candidate'],'20240318000555')
  self.assertFalse(edges[0]['relationship_verified_complete'])
  self.assertEqual(edges[0]['official_halt_state'],'UNKNOWN')
 def test_cross_date_must_not_link(self):
  a=example('20240318000555');b=example('20240319001339',is_corr=True);b['notice_date']='2024-03-19'
  self.assertEqual(h.candidate_links([a,b]),[])
 def test_different_code_must_not_link(self):
  a=example('20240318000555');b=example('20240318001339',is_corr=True);b['code']='418250'
  self.assertEqual(h.candidate_links([a,b]),[])
 def test_different_title_must_not_link(self):
  a=example('20240318000555');b=example('20240318001339','주권매매거래정지해제',True)
  self.assertEqual(h.candidate_links([a,b]),[])
 def test_release_title_does_not_promote(self):
  a=example('20231120000599','주권매매거래정지해제(회생절차개시결정)')
  a['body_text']='해제일시 2023-11-21';a['release_mention_only']=True
  self.assertEqual(h.label(a),'DIRECT_RELEASE_NOTICE_TITLE_REQUIRES_FULL_CHAIN')
 def test_period_change_cannot_be_release(self):
  a=example('20240315001321','주권매매거래정지기간변경(투자자보호)')
  a['body_text']='개선기간';a['release_mention_only']=True
  self.assertEqual(h.label(a),'DIRECT_PERIOD_CHANGE_NOT_RELEASE')
 def test_correction_more_important_than_release_word(self):
  a=example('20240318001339',is_corr=True);a['body_text']='거래정지 해제 안내 예정';a['release_mention_only']=True
  self.assertEqual(h.label(a),'CONTEXT_EXPLICIT_CORRECTION_NOTICE')
 def test_manifest_provenance_pass(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=pathlib.Path(tmp);fixture(root)
   body,shell,digest=h.check_manifest(root,'manifest.json','raw_kind_other',{'20240318000555'},3)
   self.assertEqual(len(body),1);self.assertEqual(len(shell),1);self.assertEqual(len(digest),64)
 def test_manifest_byte_tamper_rejected(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=pathlib.Path(tmp);entries=fixture(root)
   (root/entries[0]['path']).write_bytes(b'tampered')
   with self.assertRaisesRegex(RuntimeError,'RAW_SHA_OR_BYTES_TAMPER'):
    h.check_manifest(root,'manifest.json','raw_kind_other',{'20240318000555'},3)
 def test_manifest_missing_receipt_rejected(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=pathlib.Path(tmp);fixture(root)
   with self.assertRaisesRegex(RuntimeError,'RAW_COHORT_NOT_EXACT'):
    h.check_manifest(root,'manifest.json','raw_kind_other',{'20240318000555','20240318001339'},3)
 def test_manifest_unexpected_receipt_rejected(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=pathlib.Path(tmp);fixture(root)
   with self.assertRaisesRegex(RuntimeError,'UNKNOWN_RECEIPT'):
    h.check_manifest(root,'manifest.json','raw_kind_other',{'20240318001339'},3)
 def test_manifest_path_traversal_rejected(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=pathlib.Path(tmp);rows=fixture(root)
   data=json.loads((root/'manifest.json').read_text());data['files'][0]['path']='../badfile'
   (root/'manifest.json').write_text(json.dumps(data))
   with self.assertRaisesRegex(RuntimeError,'RAW_PATH_OR_DUPLICATE'):
    h.check_manifest(root,'manifest.json','raw_kind_other',{'20240318000555'},3)
 def test_incomplete_manifest_rejected(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=pathlib.Path(tmp);fixture(root)
   data=json.loads((root/'manifest.json').read_text());data['status']='PARTIAL'
   (root/'manifest.json').write_text(json.dumps(data))
   with self.assertRaisesRegex(RuntimeError,'SOURCE_MANIFEST_NOT_COMPLETE'):
    h.check_manifest(root,'manifest.json','raw_kind_other',{'20240318000555'},3)
 def test_blank_body_not_trade_approval(self):
  self.assertNotIn('strategy_ready=True',inspect.getsource(h))

if __name__=='__main__':unittest.main()
