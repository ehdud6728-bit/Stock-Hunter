import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'closebet_research'))
import pandas as pd
from prepare_native_krx_prices import standardize_market,fetch_prices,audit_signal_bars
from native_backtest_report import process,metrics

class FakeStock:
    @staticmethod
    def get_market_ohlcv_by_ticker(d,market='ALL'):
        assert market=='ALL'
        return pd.DataFrame({'시가':[100,200], '고가':[110,210],'저가':[90,190], '종가':[100,200], '거래량':[1000,3000]},index=['000001','000002'])

def test_bulk_krx_and_signal_parity():
    dates=pd.DatetimeIndex(['2026-08-03','2026-08-04'])
    prices,audit=fetch_prices(FakeStock,dates,{'000001','000002'},min_universe=2,max_retries=1,pause=0)
    assert len(prices)==4
    assert audit.status.eq('VALID_SESSION').all()
    signal=pd.DataFrame({'signal_date':['2026-08-03'],'code':['1'],'entry_price':[100], '_open':[100], '_high':[110], '_low':[90], '_close':[100], '_vol':[1000]})
    qa,bad=audit_signal_bars(prices,signal)
    assert qa['match_pct']==100.0 and bad.empty
    wrong=signal.copy();wrong['_vol']=5000
    qa2,bad2=audit_signal_bars(prices,wrong)
    assert qa2['match_pct']==0.0 and len(bad2)==1

def test_no_sliding_across_halt_and_mature_denominator():
    dates=pd.bdate_range('2026-08-03',periods=24)
    prices=pd.DataFrame({'open':100.,'high':110.,'low':90.,'close':100.,'volume':1000},index=dates)
    prices.loc[dates[5],'volume']=0   # fifth future KRX day halted
    events=pd.DataFrame({'signal_date':[dates[0]],'code':['000001'],'mode':['A'],'entry_reference':[100.],
                         'source_price_column':['entry_price'],'source_type':['NATIVE']})
    actual=process(events,{'000001':prices},{'000001':'KRX'},dates[-1],dates)
    assert actual.loc[0,'D5_time_eligible']==1
    assert actual.loc[0,'D5_complete']==0
    assert actual.loc[0,'D10_complete']==1
    assert actual.loc[0,'D20_complete']==1
    tab=metrics(actual)
    assert tab.loc[tab['mode']=='ALL_EVENTS','D5_missing_price_after_maturity_n'].iloc[0]==1

if __name__=='__main__':
    test_bulk_krx_and_signal_parity()
    test_no_sliding_across_halt_and_mature_denominator()
    print('R1.5 LOCAL DETERMINISTIC TESTS PASS')
