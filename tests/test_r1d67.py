import hashlib,pathlib,sys,unittest,math,zipfile,gzip,io
import pandas as pd
BASE=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(BASE/'research_formulas'))
import r1d67_fullmarket_replay as m

class TestR1D67(unittest.TestCase):
    def test_8_distinct_expected_source_shards(self):
        self.assertEqual(set(m.ARCHIVE_SHA_EXPECTED),set(range(8)))
        self.assertEqual(len(set(m.ARCHIVE_SHA_EXPECTED.values())),8)
        self.assertEqual(len(m.ARCHIVE_SHA_EXPECTED[0]),64)
    def test_frozen_events_fixed(self):
        src=BASE/'research_inputs/R1D67_FROZEN_84922_MINI.csv.gz'
        self.assertEqual(hashlib.sha256(src.read_bytes()).hexdigest(),'0e469362863c9435d64aef10ea991e9bc2618b806b66c326edfcf499d6307972')
        f=m.get_features(src)
        self.assertEqual(len(f),84922)
        self.assertEqual(len(f[(f.code=='094940') & (f.date==20210125)]),1)
    def test_missing_price_fail_closed(self):
        import numpy as np
        e={k:np.array([[100]*225],dtype=float) for k in ('open','high','low','close','amount')}
        e['amount'][:]=1
        fake={'date':20210125,'code':'094940','ma224_dist_pct':0.,'amount_vs_prior20':1.}
        state,x,t,vals=m.parity(fake,['2021-01-25'],{}, {'2021-01-25':0},e)
        self.assertEqual(state,'MISSING_TICKER_DATE')
    def test_price_parity_mismatch_disqualifies(self):
        import numpy as np
        e={k:np.full((1,224),100.,dtype=float) for k in ('open','high','low','close','amount')}
        fake={'date':20210125,'code':'094940','ma224_dist_pct':1.0,'amount_vs_prior20':1.0}
        state,*_=m.parity(fake,[], {'094940':0},{'2021-01-25':223},e)
        self.assertEqual(state,'ASOF_PRICE_BASIS_OR_SOURCE_MISMATCH')
    def test_negative_volume_not_accepted(self):
        import numpy as np
        e={k:np.full((1,224),100.,dtype=float) for k in ('open','high','low','close','amount')}
        e['amount'][0,220]=0
        fake={'date':20210125,'code':'094940','ma224_dist_pct':0.0,'amount_vs_prior20':1.0}
        state,*_=m.parity(fake,[], {'094940':0},{'2021-01-25':223},e)
        self.assertEqual(state,'INVALID_RAW_HISTORY')
    def test_late_CA_is_not_authorized_as_clean_price(self):
        result=BASE/'results/full84922/R1D67_PULOON_20210125.csv'
        if result.exists():
            df=pd.read_csv(result)
            self.assertEqual(int(df.iloc[0]['new_live_eligible']),0)
            self.assertEqual(df.iloc[0]['price_authority_status'],'CA_REPLAY_REQUIRED')

if __name__=='__main__':unittest.main()
