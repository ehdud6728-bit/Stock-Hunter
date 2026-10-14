#!/usr/bin/env python3
"""Standalone, read-only v49.76 D+20 event/structure/sector research."""
import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd
from wave_research import wave_features

HORIZONS = (1, 3, 5, 10, 15, 20)
MA_WINDOWS = (60, 120, 150, 200, 224, 240)


def require(frame, fields, label):
    missing = sorted(set(fields).difference(frame.columns))
    if missing:
        raise ValueError('%s missing: %s' % (label, ', '.join(missing)))


def previous_completed_pullback(bars):
    """Find most recent *confirmed* prior peak->trough->rebound (two-bar confirmations).

    Prior peak and trough both need two subsequent bars for confirmation. The
    ongoing/current pullback is excluded; no future signal-date bars are used.
    """
    if len(bars) < 12:
        return {}
    x = bars.reset_index(drop=True)
    high, low = x.high.to_numpy(), x.low.to_numpy()
    pivot_high = [i for i in range(2, len(x)-2) if high[i] > max(high[i-1], high[i-2]) and high[i] >= max(high[i+1], high[i+2])]
    pivot_low = [i for i in range(2, len(x)-2) if low[i] < min(low[i-1], low[i-2]) and low[i] <= min(low[i+1], low[i+2])]
    for l in reversed(pivot_low):
        previous = [p for p in pivot_high if p < l]
        if not previous:
            continue
        p = previous[-1]
        # Previous pullback must be completed with a visible bounce, not simply a fresh trough.
        subsequent = x.iloc[l+1:]
        if len(subsequent) < 2 or float(subsequent.high.max()) < float(low[l]) * 1.03:
            continue
        price_range = float(high[p] - low[l])
        if high[p] <= 0 or price_range <= 0:
            continue
        segment = x.iloc[p+1:l+1]
        return {
            'prev_peak_date': str(x.iloc[p].date.date()),
            'prev_low_date': str(x.iloc[l].date.date()),
            'prev_peak_price': float(high[p]),
            'prev_low_price': float(low[l]),
            'prev_pullback_depth_pct': 100 * price_range / float(high[p]),
            'prev_pullback_days': int(l-p),
            'prev_pullback_mean_volume': float(segment.volume.mean()),
            'prev_pullback_mean_amount': float(segment.amount.mean()),
        }
    return {}


def features(hist, sector_bars, universe_today):
    z = hist.reset_index(drop=True)
    last = z.iloc[-1]
    f = {'history_bars': len(z), 'sector': str(last.sector)}
    for w in MA_WINDOWS:
        if len(z) >= w:
            ma = float(z.close.tail(w).mean())
            f['ma%d_gap_pct' % w] = (float(last.close)/ma - 1)*100 if ma else np.nan
            if len(z) >= w+20:
                prev_ma = float(z.close.iloc[-w-20:-20].mean())
                f['ma%d_slope20_pct' % w] = (ma/prev_ma - 1)*100 if prev_ma else np.nan
    f.update(wave_features(z))
    f.update(previous_completed_pullback(z.iloc[:-3]))  # exclude most recent unfinished swing
    lookback = z.tail(20)
    high20 = float(lookback.high.max())
    f['current_drawdown_20d_pct'] = (float(last.close)/high20-1)*100 if high20 else np.nan
    f['current_volume_5d_vs_20d'] = float(z.volume.tail(5).mean()/z.volume.tail(20).mean()) if len(z)>=20 and z.volume.tail(20).mean()>0 else np.nan
    if f.get('prev_low_price'):
        f['distance_prev_low_pct'] = (float(last.close)/f['prev_low_price']-1)*100
        f['current_vs_prev_pb_volume'] = float(z.volume.tail(5).mean()/f['prev_pullback_mean_volume']) if f['prev_pullback_mean_volume']>0 else np.nan
    sec = str(last.sector)
    if sec != 'UNKNOWN' and sector_bars:
        ret5=[]; ret20=[]; amount_ratios=[]
        for bars in sector_bars.get(sec, []):
            h=bars[bars.date<=last.date]
            if len(h)>=21:
                a=h.iloc[-1]
                ret5.append(float(a.close/h.iloc[-6].close-1))
                ret20.append(float(a.close/h.iloc[-21].close-1))
                old=h.iloc[-21:-6].amount.mean(); new=h.iloc[-5:].amount.mean()
                if old>0: amount_ratios.append(float(new/old))
        if ret5:
            f['sector_members_20d_eligible'] = len(ret5)
            f['sector_ret5_equal_weight_pct'] = np.mean(ret5)*100
            f['sector_ret20_equal_weight_pct'] = np.mean(ret20)*100
            f['sector_breadth5_pct'] = np.mean(np.array(ret5)>0)*100
            f['sector_amount_recent_ratio_median'] = float(np.median(amount_ratios)) if amount_ratios else np.nan
            if len(z)>=6:
                f['stock_vs_sector_5d_pctp'] = (float(last.close/z.iloc[-6].close-1)-np.mean(ret5))*100
    return f


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--signals',required=True)
    ap.add_argument('--daily',required=True)
    ap.add_argument('--out',required=True)
    ap.add_argument('--asof',default='2026-10-12')
    ns=ap.parse_args(); out=Path(ns.out);out.mkdir(parents=True,exist_ok=True)
    sig=pd.read_csv(ns.signals,dtype={'code':str});daily=pd.read_csv(ns.daily,dtype={'code':str})
    require(sig,['code','signal_date','mode','entry_price'],'signals')
    require(daily,['code','date','open','high','low','close','volume'],'daily')
    sig['signal_date']=pd.to_datetime(sig.signal_date,errors='raise').dt.normalize()
    daily['date']=pd.to_datetime(daily.date,errors='raise').dt.normalize()
    asof=pd.Timestamp(ns.asof)
    sig=sig[sig.signal_date<=asof].copy()
    daily=daily[daily.date<=asof].copy()
    for x in ['open','high','low','close','volume']:
        daily[x]=pd.to_numeric(daily[x],errors='coerce')
    if 'amount' not in daily: daily['amount']=daily.close*daily.volume
    else: daily['amount']=pd.to_numeric(daily.amount,errors='coerce')
    if 'sector' not in daily: daily['sector']='UNKNOWN'
    daily['sector']=daily.sector.fillna('UNKNOWN')
    if daily.duplicated(['code','date']).any():
        raise ValueError('duplicate daily code/date; repair before research')
    daily=daily.sort_values(['code','date']).reset_index(drop=True)
    by_code={code:group.reset_index(drop=True) for code,group in daily.groupby('code')}
    by_sector={sec:[by_code[c] for c in codes.code.unique() if c in by_code] for sec,codes in daily.groupby('sector') if sec!='UNKNOWN'}
    records=[];bad_prices=0
    for _,signal in sig.iterrows():
        code=str(signal.code).zfill(6)
        b=by_code.get(code)
        row={'code':code,'name':signal.get('name',''),'mode':signal['mode'],'signal_date':str(signal.signal_date.date()),'entry_price':signal.entry_price}
        if b is None:
            row['data_status']='MISSING_CODE';records.append(row);continue
        h=b[b.date<=signal.signal_date]
        if h.empty or h.iloc[-1].date!=signal.signal_date:
            row['data_status']='MISSING_SIGNAL_DATE';records.append(row);continue
        entry=pd.to_numeric(signal.entry_price,errors='coerce')
        if not np.isfinite(entry) or entry<=0:
            row['data_status']='BAD_ENTRY_PRICE';bad_prices+=1;records.append(row);continue
        row['data_status']='OK'
        row.update(features(h,by_sector,daily))
        f=b[b.date>signal.signal_date].head(max(HORIZONS))
        for d in HORIZONS:
            if len(f)<d:
                row[f'd{d}_status']='CENSORED'
                continue
            piece=f.iloc[:d];last=piece.iloc[-1]
            row[f'd{d}_status']='COMPLETE'
            row[f'd{d}_close_ret_pct']=100*(float(last.close)/entry-1)
            row[f'd{d}_mfe_pct']=100*(float(piece.high.max())/entry-1)
            row[f'd{d}_mae_pct']=100*(float(piece.low.min())/entry-1)
            row[f'd{d}_hit_plus3']=int(float(piece.high.max())>=entry*1.03)
            row[f'd{d}_hit_plus5']=int(float(piece.high.max())>=entry*1.05)
            row[f'd{d}_hit_minus3']=int(float(piece.low.min())<=entry*0.97)
        records.append(row)
    res=pd.DataFrame(records)
    res.to_csv(out/'event_diagnostics.csv',index=False,encoding='utf-8-sig')
    metrics=[]
    for mode,g in [('ALL',res)]+list(res.groupby('mode')):
        for d in HORIZONS:
            state=f'd{d}_status';column=f'd{d}_close_ret_pct'
            if state not in g: continue
            complete=g[g[state]=='COMPLETE'];rv=pd.to_numeric(complete[column],errors='coerce').dropna()
            metrics.append({'mode':mode,'horizon':d,'total_events':len(g),'completed':len(rv),'censored':int(g[state].eq('CENSORED').sum()),'win_rate_pct':float((rv>0).mean()*100) if len(rv) else np.nan,'mean_ret_pct':float(rv.mean()) if len(rv) else np.nan,'median_ret_pct':float(rv.median()) if len(rv) else np.nan})
    pd.DataFrame(metrics).to_csv(out/'horizon_metrics.csv',index=False,encoding='utf-8-sig')
    contrast=[]
    if 'd20_close_ret_pct' in res:
        z=res[res.get('d20_status','').eq('COMPLETE')].copy()
        for feature in ['prev_pullback_depth_pct','prev_pullback_days','current_drawdown_20d_pct','distance_prev_low_pct','current_vs_prev_pb_volume','ma224_gap_pct','ma224_slope20_pct','sector_ret5_equal_weight_pct','sector_breadth5_pct','stock_vs_sector_5d_pctp','squeeze_atr5_vs_prior20','squeeze_volume5_vs_prior20','squeeze_amount5_vs_prior20','wave_depth_contraction_ratio','wave_time_symmetry_ratio','wave_amount_contraction_ratio','wave_last_vs_prev_trough_pct']:
            if feature not in z: continue
            wins=pd.to_numeric(z.loc[z.d20_close_ret_pct>0,feature],errors='coerce').dropna()
            losses=pd.to_numeric(z.loc[z.d20_close_ret_pct<=0,feature],errors='coerce').dropna()
            contrast.append({'feature':feature,'winner_n':len(wins),'loser_n':len(losses),'winner_median':wins.median() if len(wins) else np.nan,'loser_median':losses.median() if len(losses) else np.nan,'status':'DESCRIPTIVE_ONLY'})
    pd.DataFrame(contrast,columns=['feature','winner_n','loser_n','winner_median','loser_median','status']).to_csv(out/'feature_contrast.csv',index=False,encoding='utf-8-sig')
    status={'asof':ns.asof,'signals_in_scope':len(sig),'valid_signal_dates':int(res.data_status.eq('OK').sum()) if len(res) else 0,'bad_entry_price':bad_prices,'daily_rows':len(daily),'sector_member_scope':'all_codes_in_daily_input','research_only':True,'production_modified':False,'notes':['No result is evidence of trade fill.','Completed horizons are trading bars, not calendar days.','No stop/target intra-bar ordering assumption.','Daily-sector membership must be as-of historical, not modern retroactive classification.']}
    (out/'quality_checks.json').write_text(json.dumps(status,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(status,ensure_ascii=False))

if __name__=='__main__':main()
