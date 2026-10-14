import sys,tempfile,pathlib
import numpy as np,pandas as pd
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]/'closebet_research'))
from native_backtest_report import extract_events,process,metrics,code

def test():
    with tempfile.TemporaryDirectory() as td:
        p=pathlib.Path(td)/'v49_76_global_selected_top5.csv'
        pd.DataFrame([{'signal_date':'2026-08-03','code':'005930','mode':'I','close':100.0,'rule35_pnl':999.0},
                      {'signal_date':'2026-08-03','code':'035720','mode':'C','close':200.0,'rule35_pnl':-99.0}]).to_csv(p,index=False)
        e=extract_events(p)
        ix=pd.bdate_range('2026-08-03',periods=24)
        def prices(start):
            x=pd.DataFrame({'open':start,'high':start+1,'low':start-1,'close':start+np.arange(len(ix)), 'volume':100},index=ix)
            return x
        res=process(e,{'005930':prices(100.0),'035720':prices(200.0)}, {'005930':'SUPPLIED','035720':'SUPPLIED'},pd.Timestamp('2026-09-30'))
        assert len(res)==2
        assert res['D20_complete'].tolist()==[1,1],res['D20_complete']
        assert abs(float(res.loc[res.code=='005930','D20_ret_pct'].iloc[0])-20)<0.0001
        assert not any(np.isclose(res.D20_ret_pct,999.0)), 'must never read rule35 as D20'
        m=metrics(res)
        assert m.loc[m['mode']=='ALL_EVENTS','D20_n'].iloc[0]==2
        short=process(e,{'005930':prices(100.0),'035720':prices(200.0)}, {'005930':'SUPPLIED','035720':'SUPPLIED'},pd.Timestamp('2026-08-13'))
        assert short['D20_complete'].sum()==0
        assert short['D5_complete'].sum()==2
        assert code('005930')=='005930'
    print('PASS fixture: native source separation + D20 maturity + strategy aggregation')

if __name__=='__main__':test()
