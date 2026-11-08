import numpy as np

def pre_pullback(d,j,n):
 if j<n or n<2:return dict(status='UNKNOWN_HISTORY')
 w=d.iloc[j-n:j];high=w.high.to_numpy(float);low=w.low.to_numpy(float);prior_peaks=np.maximum.accumulate(high)[:-1];dd=100*(1-low[1:]/prior_peaks);k=int(np.argmax(dd))+1;peak_idx=int(np.argmax(high[:k]));depth=max(0.,float(dd[k-1]));peak=float(high[peak_idx]);trough=float(low[k]);close=float(w.close.iloc[-1]);recovery=(close-trough)/(peak-trough) if peak>trough else np.nan
 return dict(status='KNOWN',depth_pct=depth,peak_index=j-n+peak_idx,trough_index=j-n+k,peak_date=w.date.iloc[peak_idx],trough_date=w.date.iloc[k],peak_price=peak,trough_price=trough,recovery_fraction=recovery,recovered_half=bool(recovery>=.5) if np.isfinite(recovery) else None,depth_group='LT3' if depth<3 else '3_TO_LT7' if depth<7 else '7_TO_LT12' if depth<12 else 'GE12',last_close=close,trough_age=j-(j-n+k),new_lower_low_after_selected=bool((low[k+1:]<trough).any()))
