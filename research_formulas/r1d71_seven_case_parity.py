#!/usr/bin/env python3
"""Cross-check 7 real raw chart reconstructions vs frozen R1D19/R1D7 lines."""
import pathlib,pandas as pd
p=pathlib.Path(__file__).resolve().parents[1]/'results'
a=pd.read_csv(p/'06_REAL_OHLC_CHART_REVIEW_DATA.csv',dtype={'code':str})
b=pd.read_csv(p/'05_ALL_5247_REAUDIT_DETAILS.csv.gz',dtype={'code':str})
x=a.merge(b,on=['date','code'],how='left',validate='one_to_one')
assert len(x)==7 and x.signal_close.notna().all()
x['close_t_diff']=x.raw_close_t-x.signal_close
x['entry_d1_diff']=x.raw_open_d1-x.D20_entry_open
x['exit_d20_diff']=x.raw_close_d20-x.D20_exit_close
out=x[['date','code','strategy','raw_close_t','signal_close','raw_open_d1','D20_entry_open','raw_close_d20','D20_exit_close','close_t_diff','entry_d1_diff','exit_d20_diff']]
assert (out[['close_t_diff','entry_d1_diff','exit_d20_diff']].abs()<=.01).all().all(),out.to_string()
out.to_csv(p/'07_SEVEN_CASE_OHLC_SOURCE_PARITY.csv',index=False)
print('R1D71_7_SOURCE_PRICE_PARITY_PASS')
