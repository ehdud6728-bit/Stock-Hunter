import numpy as np
def simulate(opens,highs,closes,dates,entry_index,expiry_index,entry_at_close,target_pct,complete=True):
 price=float(closes[entry_index] if entry_at_close else opens[entry_index]);target=price*(1+target_pct/100);n=len(dates);finish=min(expiry_index,n-1);base=dict(entry_date=dates[entry_index],entry_price=price,target_price=target,complete_window=complete,entry_at_close=entry_at_close)
 for k in range(entry_index,finish+1):
  if k==entry_index and entry_at_close:continue
  if k>entry_index and opens[k]>=target:return base|dict(status='TP',exit_date=dates[k],exit_price=float(opens[k]),holding_sessions=k-entry_index+1,net_pct=100*(opens[k]/price-1)-.25)
  if highs[k]>=target:return base|dict(status='TP',exit_date=dates[k],exit_price=target,holding_sessions=k-entry_index+1,net_pct=target_pct-.25)
 if not complete:return base|dict(status='RIGHT_CENSORED',observed_sessions=max(0,n-entry_index))
 return base|dict(status='NO_TP',exit_date=dates[finish],exit_price=float(closes[finish]),holding_sessions=finish-entry_index+1,net_pct=100*(closes[finish]/price-1)-.25)

def intraday_bounds(open_price,line,prev_close,prev_line):
 if not all(np.isfinite(v) for v in [open_price,line,prev_close,prev_line]) or open_price<=0 or not (line<prev_line and prev_close<=prev_line and open_price<line):return None
 lower=max(1.005*open_price,1.25*line-.25*open_price);upper=5*line-4*open_price
 return (lower,upper) if lower<=upper else None

def tick_first_signal(prices,open_price,line,prev_close,prev_line):
 bounds=intraday_bounds(open_price,line,prev_close,prev_line)
 if bounds is None:return None
 # 실시간 현재체결가로 조건확인 후 다음틱가를 모형체결가로 사용한다.
 for k,p in enumerate(prices):
  if bounds[0]<=p<=bounds[1]:return dict(signal_tick=k,entry_tick=k+1,entry_price=float(prices[k+1])) if k+1<len(prices) else dict(signal_tick=k,entry_tick=None,entry_price=None)
 return None
