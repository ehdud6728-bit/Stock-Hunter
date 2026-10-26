import os,sys,tempfile,unittest
from pathlib import Path
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from research_formulas.stockhunter_r1d26e_hit_ladder import (
    ST_C,ST_D,HORIZONS,KEY,OK,base_input,build_event_table,summary_one,SourceQAError,produce,sha,verified_zip
)

def tiny():
    rows=[]
    for st,code,hits in [(ST_C,'001111',[4,6,9,11,16]),(ST_D,'002222',[1,2,3,4,4])]:
        for h,mfe in zip(HORIZONS,hits):
            rows.append({'strategy':st,'date':'2024-01-02','code':code,'horizon':h,'status_out':OK,
                         'mfe_intraday_pct':mfe,'net_20bps_pct':-2,'period':'LATE_2024_2025',
                         'entry_day':'2024-01-03','exit_day':'2024-02-12','mae_intraday_pct':-3})
    return pd.DataFrame(rows)

class IntervalTouchTests(unittest.TestCase):
    def test_sorted_source_and_exact_signals(self):
        d=base_input(tiny(),1,1);self.assertEqual(len(d),10)
    def test_exact_hit_bucket_not_exact_day(self):
        x,_=build_event_table(base_input(tiny(),1,1))
        c=x[x.strategy==ST_C].iloc[0]
        self.assertEqual(c.first_hit5_bucket,'D6-D10')
        self.assertEqual(c.first_hit10_bucket,'D16-D20')
        self.assertEqual(c.first_hit15_bucket,'D21-D30')
        self.assertTrue(c.hit15_by_d30)
    def test_non_hit_correct(self):
        x,_=build_event_table(base_input(tiny(),1,1))
        d=x[x.strategy==ST_D].iloc[0]
        self.assertEqual(d.first_hit5_bucket,'NO_HIT_BY_D30')
        self.assertEqual(d.hit5_by_d30,0)
    def test_conditional_conversions_denominator(self):
        x,_=build_event_table(base_input(tiny(),1,1))
        q=summary_one(x)
        self.assertEqual((q['n_hit5'],q['n_hit10'],q['n_hit15']),(1,1,1))
        self.assertEqual(q['pct_hit10_given_hit5'],100)
        self.assertEqual(q['pct_below5'],50)
    def test_missing_horizon_rejected(self):
        d=tiny().drop(index=0)
        with self.assertRaisesRegex(SourceQAError,'MISSING_HORIZON_ROWS'):base_input(d,1,1)
    def test_duplicate_horizon_rejected(self):
        d=pd.concat([tiny(),tiny().head(1)],ignore_index=True)
        with self.assertRaisesRegex(SourceQAError,'DUPLICATE'):base_input(d,1,1)
    def test_nonmonotonic_mfe_rejected(self):
        d=tiny();d.loc[(d.strategy==ST_C)&(d.horizon==30),'mfe_intraday_pct']=5
        with self.assertRaisesRegex(SourceQAError,'MFE_NOT_NONDECREASING'):build_event_table(base_input(d,1,1))
    def test_censored_D30_excluded(self):
        d=tiny();d.loc[(d.strategy==ST_D)&(d.horizon==30),'status_out']='CENSORED_MISSING_OR_ACTION_UNCERTAIN'
        o,ends=build_event_table(base_input(d,1,1));self.assertEqual(len(o),1);self.assertEqual(len(ends),2)
    def test_missing_counts_fail(self):
        with self.assertRaisesRegex(SourceQAError,'SOURCE_C_D_SIGNAL_COUNTS_CHANGED'):base_input(tiny(),7499,2811)
    def test_wrong_sha_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            path=Path(td)/'a.zip';path.write_bytes(b'not-a-zip')
            with self.assertRaisesRegex(SourceQAError,'PINNED_ZIP_SHA256_MISMATCH'):verified_zip(path,'0'*64)
    @unittest.skipUnless(os.environ.get('R1D26E_FULL_R1D25_ZIP') and os.environ.get('R1D26E_FULL_R1D26D_ZIP'),'Full frozen source zips absent')
    def test_original_frozen_files_integration(self):
        with tempfile.TemporaryDirectory() as td:
            w=Path(td)
            src=Path(__file__).resolve().parents[1]
            s,e,m=produce(os.environ['R1D26E_FULL_R1D25_ZIP'],os.environ['R1D26E_FULL_R1D26D_ZIP'],src/'R1D26E_PREDECLARED_PROTOCOL.json',w)
            self.assertEqual(len(e),10198)
            self.assertEqual(m['D30_censored'],112)
            self.assertEqual(len(pd.read_csv(w/'R1D26E_20PILOT_KRX_INVESTOR_FLOW_HIT_LADDER_NOT_OOS.csv')),20)
            self.assertEqual(s[s.cohort=='ALL'].n_hit15.tolist(),[3044,1042])

if __name__=='__main__':unittest.main()
