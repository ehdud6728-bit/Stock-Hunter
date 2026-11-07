#!/usr/bin/env python3
"""BLUE20 20,2,26 independent, read-only three-way event audit.

No production scanner or workflow changes. Full-result trading metrics require
point-in-time OHLCV and an explicit market-day calendar supplied by the user.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import numpy as np
import pandas as pd

REQUIRED = ('date', 'code', 'open', 'high', 'low', 'close')
LABELS = ('WICK_ONLY', 'MICRO_BODY', 'CLEAR_BODY', 'GAP_ABOVE')

def sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for b in iter(lambda: f.read(1024 * 1024), b''):
            h.update(b)
    return h.hexdigest()

def read_prices(path):
    path = Path(path)
    if path.suffix.lower() == '.parquet':
        df = pd.read_parquet(path)
    elif path.suffix.lower() in ('.csv', '.gz') or path.name.endswith('.csv.gz'):
        df = pd.read_csv(path, dtype={'code': str})
    else:
        raise ValueError('OHLCV input must be csv, csv.gz or parquet')
    missing = sorted(set(REQUIRED) - set(df.columns))
    if missing:
        raise ValueError('Missing required OHLCV columns: ' + ','.join(missing))
    df = df.copy()
    df['code'] = df['code'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(6)
    df['date'] = pd.to_datetime(df['date'], errors='raise').dt.normalize()
    for c in ('open','high','low','close'):
        df[c] = pd.to_numeric(df[c], errors='raise')
    if df[['date','code']].duplicated().any():
        raise ValueError('Duplicate code/date: refusing to guess authoritative row')
    if (df[list(REQUIRED[2:])] <= 0).any().any() or ((df['low'] > df[['open','close','high']].min(axis=1)) | (df['high'] < df[['open','close','low']].max(axis=1))).any():
        raise ValueError('Invalid OHLC prices: fail-closed')
    return df.sort_values(['code','date']).reset_index(drop=True)

def read_calendar(path):
    data = pd.read_csv(path)
    if 'date' not in data.columns:
        raise ValueError('Calendar CSV must contain date column')
    dates = pd.DatetimeIndex(pd.to_datetime(data['date']).dt.normalize().sort_values().unique())
    if len(dates) != len(data):
        raise ValueError('Duplicate market calendar day')
    return dates

def calculate_bands(df):
    parts = []
    for code, g in df.groupby('code', sort=False):
        g = g.copy().sort_values('date')
        roll = g['close'].rolling(20, min_periods=20)
        mid = roll.mean()
        std = roll.std(ddof=0)
        g['UPPER'] = (mid + 2 * std).shift(26)
        g['LOWER'] = (mid - 2 * std).shift(26)
        g['UPPER_PREV'] = g['UPPER'].shift(1)
        g['LOWER_PREV'] = g['LOWER'].shift(1)
        g['PREV_CLOSE'] = g['close'].shift(1)
        parts.append(g)
    return pd.concat(parts, ignore_index=True).sort_values(['code','date']).reset_index(drop=True)

def classify(open_, high, close, line):
    """Mutually exclusive labels; None for no bullish penetration.

    'GAP_ABOVE' is *not* one of the proposed three bullish penetration
    classes and is always independently accounted for.
    """
    if not (np.isfinite(line) and close > open_ and high > line):
        return None
    if open_ >= line:
        return 'GAP_ABOVE'
    if close <= line:
        return 'WICK_ONLY'
    body = close - open_
    below_fraction = (line - open_) / body
    above_fraction = (close - line) / body
    body_pct = 100.0 * body / open_
    if below_fraction >= 0.20 and above_fraction >= 0.20 and body_pct >= 0.50:
        return 'CLEAR_BODY'
    return 'MICRO_BODY'

def simulate(group, entry_position, trading_days, entry_type='D0_CLOSE', cost_pct=0.25):
    # `group` is one symbol's OHLCV history indexed by market calendar date.
    end = entry_position + 20
    if end >= len(trading_days):
        return None
    entry_date = trading_days[entry_position]
    if entry_type == 'D0_CLOSE':
        entry_px = float(group.loc[entry_date, 'close'])
    else:
        next_date = trading_days[entry_position + 1]
        entry_px = float(group.loc[next_date, 'open'])
    if entry_px <= 0:
        return None
    future_days = trading_days[entry_position+1:end+1]
    if not set(future_days).issubset(group.index):
        return None
    future = group.loc[future_days]
    tp = entry_px * 1.10
    sl = entry_px * 0.95
    hit10_any = bool((future['high'] >= tp).any())
    min_low_net = float((future['low'].min()/entry_px-1)*100-cost_pct)
    max_high_net = float((future['high'].max()/entry_px-1)*100-cost_pct)
    exit_px = float(future.iloc[-1]['close'])
    reason = 'TIME'
    exit_day = 20
    ambiguous = False
    for j, (_, b) in enumerate(future.iterrows(), start=1):
        # gap open is actionable before intraday ranges. For D1_OPEN,
        # entry itself occurs at the D1 opening auction, not before it.
        o, h, l = float(b['open']), float(b['high']), float(b['low'])
        if not (entry_type == 'D1_OPEN' and j == 1):
            if o <= sl:
                exit_px, reason, exit_day = o, 'SL_GAP', j
                break
            if o >= tp:
                exit_px, reason, exit_day = o, 'TP_GAP', j
                break
        if h >= tp and l <= sl:
            ambiguous = True
            exit_px, reason, exit_day = sl, 'SL_AMBIGUOUS', j
            break
        if l <= sl:
            exit_px, reason, exit_day = sl, 'SL', j
            break
        if h >= tp:
            exit_px, reason, exit_day = tp, 'TP', j
            break
    return dict(entry_type=entry_type, entry_price=entry_px, exit_price=exit_px,
                exit_day=exit_day, exit_reason=reason, ambiguous=ambiguous,
                hit10_any=hit10_any, mfe_net_pct=max_high_net,
                mae_net_pct=min_low_net,
                net_pct=(exit_px/entry_px-1)*100-cost_pct)

def audit(df, cal, year_start, year_end, cost_pct=0.25):
    cal_position = {d: i for i, d in enumerate(cal)}
    price_groups = {code: g.set_index('date').sort_index() for code, g in df.groupby('code')}
    events = []
    audit_counts = dict(invalid_calendar_day=0, incomplete_future20=0,
                        precondition_valid=0, gap_above=0,
                        original_body_d20=0, clear_body_d20=0)
    for code, g in price_groups.items():
        for date, r in g.iterrows():
            if not (year_start <= date.year <= year_end):
                continue
            if date not in cal_position:
                audit_counts['invalid_calendar_day'] += 1
                continue
            idx = cal_position[date]
            for band in ('UPPER', 'LOWER'):
                line, prev_line, prev_close = r[band], r[f'{band}_PREV'], r['PREV_CLOSE']
                if not (np.isfinite(line) and np.isfinite(prev_line) and np.isfinite(prev_close)):
                    continue
                # Strict as-of conditions from source report, not future outcomes.
                if not (line < prev_line and prev_close <= prev_line):
                    continue
                label = classify(float(r['open']), float(r['high']), float(r['close']), float(line))
                if label is None:
                    continue
                audit_counts['precondition_valid'] += 1
                if label == 'GAP_ABOVE':
                    audit_counts['gap_above'] += 1
                d0 = simulate(g, idx, cal, 'D0_CLOSE', cost_pct)
                d1 = simulate(g, idx, cal, 'D1_OPEN', cost_pct)
                if d0 is None or d1 is None:
                    audit_counts['incomplete_future20'] += 1
                    continue
                is_body = label in ('MICRO_BODY', 'CLEAR_BODY')
                if is_body: audit_counts['original_body_d20'] += 1
                if label == 'CLEAR_BODY': audit_counts['clear_body_d20'] += 1
                body = float(r['close']-r['open'])
                base = dict(date=date.strftime('%Y-%m-%d'), year=date.year, code=code,
                            band=band, label=label,
                            line_today=float(line), line_previous=float(prev_line),
                            previous_close=float(prev_close),
                            open=float(r['open']), high=float(r['high']), low=float(r['low']), close=float(r['close']),
                            body_pct=100*body/float(r['open']),
                            under_body_fraction=((float(line)-float(r['open']))/body) if body else None,
                            over_body_fraction=((float(r['close'])-float(line))/body) if body else None)
                for trade in (d0,d1):
                    events.append({**base, **trade})
    return pd.DataFrame(events), audit_counts

def summarize(events):
    cols = ['band','label','entry_type','year','n','hit10_pct','net_mean_pct','net_median_pct',
            'win_pct','tp_exit_pct','sl_exit_pct','ambiguous_n','mfe_median_net_pct','mae_median_net_pct']
    if events.empty:
        return pd.DataFrame(columns=cols)
    blocks=[]
    for year in ('ALL', *sorted(events['year'].unique())):
        d=events if year=='ALL' else events[events['year']==year]
        for (band,label,entry_type),sub in d.groupby(['band','label','entry_type']):
            blocks.append(dict(band=band,label=label,entry_type=entry_type,year=year,n=len(sub),
                hit10_pct=100*sub.hit10_any.mean(),net_mean_pct=sub.net_pct.mean(),
                net_median_pct=sub.net_pct.median(),win_pct=100*(sub.net_pct>0).mean(),
                tp_exit_pct=100*sub.exit_reason.str.startswith('TP').mean(),
                sl_exit_pct=100*sub.exit_reason.str.startswith('SL').mean(),
                ambiguous_n=int(sub.ambiguous.sum()),
                mfe_median_net_pct=sub.mfe_net_pct.median(),
                mae_median_net_pct=sub.mae_net_pct.median()))
    return pd.DataFrame(blocks, columns=cols)

def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--ohlcv',required=True,help='KRX-origin point-in-time CSV or Parquet with code,date,open,high,low,close')
    p.add_argument('--calendar',required=True,help='Authoritative KRX market trading dates CSV with a date column')
    p.add_argument('--out',required=True)
    p.add_argument('--start-year',type=int,default=2023)
    p.add_argument('--end-year',type=int,default=2024)
    args=p.parse_args(argv)
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    raw=read_prices(args.ohlcv)
    calendar=read_calendar(args.calendar)
    df=calculate_bands(raw)
    ev, counts=audit(df,calendar,args.start_year,args.end_year)
    summary=summarize(ev)
    ev.to_csv(out/'THREEWAY_EVENTS.csv',index=False,encoding='utf-8-sig')
    summary.to_csv(out/'THREEWAY_SCORECARD.csv',index=False,encoding='utf-8-sig')
    expected={'UPPER':{'original':1992,'clear':1187},'LOWER':{'original':2240,'clear':1302}}
    observed={}
    for band in ('UPPER','LOWER'):
        z=ev[(ev.band==band)&(ev.entry_type=='D0_CLOSE')] if len(ev) else ev
        original=int(z.label.isin(['MICRO_BODY','CLEAR_BODY']).sum()) if len(z) else 0
        clear=int((z.label=='CLEAR_BODY').sum()) if len(z) else 0
        observed[band]={'original':original,'clear':clear,'reference':expected[band],
                        'matches_prior_reference':original==expected[band]['original'] and clear==expected[band]['clear']}
    audit_manifest={'research_only':True,'not_production_ready':True,
        'definition':'SHIFT(BBandsUp/Down(20,2),26), ddof=0',
        'study_years':[args.start_year,args.end_year],
        'entry_types':['D0_CLOSE','D1_OPEN'],
        'future_window_market_days':20,'tp_pct':10,'sl_pct':-5,
        'total_roundtrip_cost_pct_points':0.25,
        'same_bar_ambiguity':'STOP_FIRST',
        'source_sha256':{'ohlcv':sha256(args.ohlcv),'calendar':sha256(args.calendar)},
        'note':'Compare expected reference only for exact same frozen 500-name cohort, price adjustment, and calendar.',
        'counts':counts,'observed_vs_reported_reference':observed,
        'warnings':['Actual app indicator match is unverified',
                    'PIT corporate actions and execution prices are not independently certified',
                    'D0_CLOSE is a research-only close-fill assumption',
                    'WICK_ONLY belongs to a broader universe, not the legacy 4,232 body-cross denominator',
                    'GAP_ABOVE is excluded from the 3 requested classes but always reported']}
    (out/'AUDIT_MANIFEST.json').write_text(json.dumps(audit_manifest,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps({'events':len(ev)//2,'observed_reference':observed,'output':str(out)},ensure_ascii=False,indent=2))

if __name__=='__main__':
    main()
