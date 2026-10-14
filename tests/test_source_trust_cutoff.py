import sys,tempfile,pathlib
import pandas as pd
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]/'closebet_research'))
from native_source_trust_audit import evaluate

def test_cutoff():
    def row(i,d,end):
        return dict(signal_date=d,data_date=d,code=f'{i:06d}',name='TEST',mode='B1',_open=100,_high=110,_low=90,_close=100,_vol=1000,close=100,entry_price=100,eval_end_date=end,eval_available_days=20,eval_is_partial=0,hold_days=20,requested_hold_days=20,ret_close_hd=15)
    with tempfile.TemporaryDirectory() as td:
        root=pathlib.Path(td);p=root/'v49_76_global_selected_top5.csv'
        pd.DataFrame([row(1,'2026-08-03','2026-09-01'),row(2,'2026-09-10','2026-10-14')]).to_csv(p,index=False)
        a=evaluate(p,'2026-10-12',root/'out')
        assert a['native_d20_matured_within_cutoff_internal_ONLY']==1
        assert a['native_d20_complete_but_after_cutoff_EXCLUDED']==1
        assert a['native_eval_after_cutoff_rows']==1

if __name__=='__main__':test_cutoff();print('PASS cutoff')
