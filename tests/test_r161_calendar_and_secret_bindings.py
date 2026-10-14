"""Regression tests: research-only KRX credentials presence and no market-calendar compression."""
import json, subprocess, sys, tempfile
from pathlib import Path
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'closebet_research'))
from native_backtest_report import process

def test_readonly_credential_binding():
    workflow=(ROOT/'.github/workflows/closing_bet_v4976_d20_research.yml').read_text(encoding='utf-8')
    assert 'name: Closing Bet v49.76 D20 Research R1.6.1' in workflow
    assert workflow == (ROOT/'closing_bet_v4976_d20_research.yml').read_text(encoding='utf-8')
    start=workflow.index('      - name: Re-fetch KRX dated sessions')
    end=workflow.index('      - name: Evaluate verified D+1 through',start)
    env_step=workflow[start:end]
    for name in ('KRX_DATA_ID:','KRX_DATA_PW:','KRX_ID:','KRX_PW:','KRX_USER_ID:','KRX_PASSWORD:'):
        assert name in env_step
    assert "secrets.KRX_ID" in env_step
    assert "secrets.KRX_PW" in env_step
    assert 'KRX_AUTH_PREFLIGHT' in env_step
    assert '--sessions-csv reports/closebet_d20/native_krx_session_coverage.csv' in workflow

def test_stopped_ticker_does_not_shift_calendar_day():
    days=pd.bdate_range('2026-08-03',periods=23)
    x=pd.DataFrame({'open':100.,'high':110.,'low':90.,'close':100.,'volume':1000.},index=days)
    x=x.drop(days[5])  # no tracked ticker quote on day 5; official market OPEN
    x.loc[days[6],'close']=130.
    e=pd.DataFrame([{'signal_date':days[0],'code':'000001','mode':'A','entry_reference':100.,'source_price_column':'entry_price','source_type':'NATIVE'}])
    r=process(e,{'000001':x},{'000001':'KRX'},days[-1],days)
    assert r['D5_time_eligible'].iloc[0]==1
    assert r['D5_complete'].iloc[0]==0  # not 30% profit at next available quote
    assert r['D10_complete'].iloc[0]==1

def test_gate_uses_official_session_csv_end_to_end():
    from native_backtest_report import main
    import numpy as np
    with tempfile.TemporaryDirectory() as td:
        root=Path(td); n=root/'native'; n.mkdir()
        pd.DataFrame([{'signal_date':'2026-08-03','code':'000001','mode':'A','entry_price':100}]).to_csv(n/'v49_76_global_selected_top5.csv',index=False)
        days=pd.bdate_range('2026-08-03',periods=23)
        pd.DataFrame({'date':days.strftime('%Y-%m-%d'),'status':['VALID_SESSION']*len(days)}).to_csv(root/'native_krx_session_coverage.csv',index=False)
        x=pd.DataFrame({'code':'000001','date':days.strftime('%Y-%m-%d'),'open':100,'high':110,'low':90,'close':100,'volume':1000})
        x=x[x.date != days[5].strftime('%Y-%m-%d')]
        x.to_csv(root/'native_krx_input_prices.csv',index=False)
        (root/'native_krx_price_audit.json').write_text(json.dumps({'status':'VALIDATED','match_pct':100,'asof':str(days[-1].date()),'official_session_count':len(days)}))
        pd.DataFrame(columns=['signal_date','code']).to_csv(root/'native_krx_signal_price_mismatches.csv',index=False)
        old=sys.argv[:]
        try:
            sys.argv=['script','--source-root',str(n),'--asof',str(days[-1].date()),'--out',str(root),
                '--prices-csv',str(root/'native_krx_input_prices.csv'),'--sessions-csv',str(root/'native_krx_session_coverage.csv'),
                '--require-krx-parity-json',str(root/'native_krx_price_audit.json')]
            main()
        finally:
            sys.argv=old
        result=pd.read_csv(root/'native_d20_event_diagnostics.csv')
        quality=json.loads((root/'native_d20_quality.json').read_text())
        assert result['D5_time_eligible'].iloc[0] == 1
        assert result['D5_complete'].iloc[0] == 0
        assert quality['exchange_session_calendar']=='OFFICIAL_KRX_INDEX_DATES_PARITY_GATED'

if __name__=='__main__':
    test_readonly_credential_binding()
    test_stopped_ticker_does_not_shift_calendar_day()
    test_gate_uses_official_session_csv_end_to_end()
    print('R1.6.1 REGRESSIONS PASS')
