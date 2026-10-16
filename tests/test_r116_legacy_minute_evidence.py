import unittest
from closebet_research.r116_legacy_minute_evidence import code,validate_minute,is_execution_eligible,BUCKETS
class Tests(unittest.TestCase):
 def setUp(self):
  self.r={'trade_date':'2026-08-05','time':'15:00','code':'3230','open':'12000','high':'12000','low':'12000','close':'12000','volume':'10','value':'120000','source':'YAHOO_CHART:003230.KS','value_quality':'PROXY_CLOSE_X_VOLUME'}
 def test_codes_six_digits(self):self.assertEqual(code('3230'),'003230')
 def test_valid_proxy_minute(self):self.assertEqual(validate_minute(self.r,'2026-08-05.csv.gz')[2],'003230')
 def test_proxy_turnover_never_execution_eligible(self):self.assertFalse(is_execution_eligible(self.r))
 def test_reject_beyond_1500(self):
  self.r['time']='15:30'
  with self.assertRaises(ValueError):validate_minute(self.r,'2026-08-05.csv.gz')
 def test_source_must_match(self):
  self.r['source']='KRX_ACTUAL'
  with self.assertRaises(ValueError):validate_minute(self.r,'2026-08-05.csv.gz')
 def test_value_proxy_arithmetic(self):
  self.r['value']='999'
  with self.assertRaises(ValueError):validate_minute(self.r,'2026-08-05.csv.gz')
 def test_date_mismatch(self):
  self.r['trade_date']='2026-08-06'
  with self.assertRaises(ValueError):validate_minute(self.r,'2026-08-05.csv.gz')
 def test_auction_separate_from_regular(self):
  self.assertNotEqual(next(x for x in BUCKETS if x[0]=='KRX_AUCTION_1530')[1],next(x for x in BUCKETS if x[0]=='PRECLOSE_1500_1520')[1])
 def test_missing_does_not_equal_zero(self):
  self.assertEqual(next(x for x in BUCKETS if x[0]=='KRX_AUCTION_1530')[3],1)
if __name__=='__main__':unittest.main()
