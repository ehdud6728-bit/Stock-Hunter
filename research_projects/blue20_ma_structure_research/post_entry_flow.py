#!/usr/bin/env python3
"""Research-only daily post-entry candle + OHLCV participation study.

All signals at day t are calculated using data through t close, usable no sooner
than trading day t+1. Turnover/volume are participation proxies, NOT investor net
purchases or bid/ask aggressor flow. The 26-shifted bands are historical delayed
BB(20,2), NOT contemporaneous volatility contraction.
"""
from __future__ import annotations
import json
from pathlib import Path
import numpy as np
import pandas as pd

VERSION = 'BLUE20_POST_ENTRY_FLOW_CANDLE_PIT_R2_20261106'
COST_PCT = 0.25
TP_PCT = 10
SOFT_STOP_PCT = 5
HARD_STOP_PCT = 12


def add_bar_features(frame: pd.DataFrame) -> pd.DataFrame:
    """All features are either same-date EOD facts or prior-only baselines."""
    d = frame.copy()
    req = {'date','open','high','low','close','volume','amount',
           'ma60','ma112','ma224','ma60_slope10','ma112_slope10',
           'bb20_upper_shift26','bb20_lower_shift26'}
    if not req.issubset(d): raise ValueError('BAR_SCHEMA_MISSING '+str(req-set(d)))
    for c in ['open','high','low','close','volume','amount']:
        d[c] = pd.to_numeric(d[c],errors='raise')
    if d[['open','high','low','close']].le(0).any().any() or d[['volume','amount']].lt(0).any().any():
        raise ValueError('BAD_OHLCV_VALUES')
    if not ((d.low<=d[['open','close','high']].min(axis=1)) &
            (d.high>=d[['open','close','low']].max(axis=1))).all():
        raise ValueError('BAD_OHLC_RANGE')
    d['prior20_volume'] = d.volume.shift(1).rolling(20,min_periods=20).mean()
    d['prior20_amount'] = d.amount.shift(1).rolling(20,min_periods=20).mean()
    d['volume_ratio20'] = d.volume / d.prior20_volume.replace(0,np.nan)
    d['amount_ratio20'] = d.amount / d.prior20_amount.replace(0,np.nan)
    d['price_return1_pct'] = 100*(d.close/d.close.shift(1)-1)
    rng = (d.high-d.low)
    denom = rng.replace(0,np.nan)
    body = (d.close-d.open).abs()
    d['body_to_range'] = (body/denom).fillna(0)
    d['upper_wick_to_range'] = ((d.high-d[['open','close']].max(axis=1))/denom).fillna(0)
    d['lower_wick_to_range'] = ((d[['open','close']].min(axis=1)-d.low)/denom).fillna(0)
    d['close_location'] = ((d.close-d.low)/denom).fillna(.5)
    d['bear'] = d.close < d.open
    d['bull'] = d.close > d.open
    d['doji'] = d.body_to_range.le(.12)
    d['long_lower_wick'] = d.lower_wick_to_range.ge(.45) & d.body_to_range.le(.45)
    d['long_upper_wick'] = d.upper_wick_to_range.ge(.45) & d.body_to_range.le(.45)
    d['big_bear'] = d.bear & d.body_to_range.ge(.60) & d.close_location.le(.30)
    d['big_bull'] = d.bull & d.body_to_range.ge(.60) & d.close_location.ge(.70)
    prevopen=d.open.shift(1);prevclose=d.close.shift(1)
    d['bull_engulf'] = (d.bull & prevclose.lt(prevopen) & d.open.le(prevclose) & d.close.ge(prevopen))
    d['bear_engulf'] = (d.bear & prevclose.gt(prevopen) & d.open.ge(prevclose) & d.close.le(prevopen))
    d['inside_bar'] = d.high.le(d.high.shift(1)) & d.low.ge(d.low.shift(1))
    conditions=[d.bull_engulf,d.bear_engulf,d.big_bull,d.big_bear,d.long_lower_wick,
                d.long_upper_wick,d.doji,d.inside_bar]
    choices=['BULL_ENGULF','BEAR_ENGULF','BIG_BULL','BIG_BEAR','LONG_LOWER_WICK',
             'LONG_UPPER_WICK','DOJI','INSIDE_BAR']
    d['candle_shape'] = np.select(conditions,choices,default='OTHER')
    # The following fields are *proxies*, not verified actual buyers/sellers.
    d['up_day_high_participation'] = (d.close.gt(prevclose) & d.amount_ratio20.ge(1.8) & d.close_location.ge(.7))
    d['heavy_down_proxy'] = (d.close.lt(prevclose) & d.amount_ratio20.ge(1.8) & d.close_location.le(.30))
    d['quiet_down_proxy'] = (d.close.lt(prevclose) & d.amount_ratio20.le(.70))
    d['lower_wick_with_turnover'] = d.long_lower_wick & d.close_location.ge(.6) & d.amount_ratio20.ge(1.2)
    upper=d.bb20_upper_shift26;lower=d.bb20_lower_shift26
    d['band_width_pct'] = 100*(upper-lower)/((upper+lower)/2).replace(0,np.nan)
    d.loc[upper.le(lower),'band_width_pct'] = np.nan
    d['band_width_change10_pp'] = d.band_width_pct - d.band_width_pct.shift(10)
    d['upper_slope5_pct'] = 100*(upper/upper.shift(5)-1)
    d['lower_slope5_pct'] = 100*(lower/lower.shift(5)-1)
    d['shifted_band_squeeze'] = (d.band_width_change10_pp.lt(0) &
                                 d.upper_slope5_pct.lt(0) & d.lower_slope5_pct.gt(0))
    d['lower_close_hold'] = d.close.ge(lower) & lower.notna()
    d['lower_intraday_recover'] = d.low.lt(lower) & d.lower_close_hold
    d['lower_close_break'] = d.close.lt(lower) & lower.notna()
    d['upper_body_break'] = (d.open.lt(upper) & d.close.gt(upper) & d.bull & upper.notna())
    d['above_ma60'] = d.close.gt(d.ma60) & d.ma60.notna()
    d['above_ma112'] = d.close.gt(d.ma112) & d.ma112.notna()
    # This is a prospective EOD research hypothesis. DO NOT classify as confirmed pullback.
    # Signal from close(t) may control ONLY the next day's intraday stop action.
    d['pre_next_session_soft_stop_deferral'] = (
        d.shifted_band_squeeze & d.lower_close_hold &
        d.amount_ratio20.le(1.2) & ~d.heavy_down_proxy &
        (d.ma60_slope10.ge(0) | d.long_lower_wick)
    ).fillna(False)
    # Light-volume down days and long lower wicks are separately observable; they are not
    # a categorical statement of institutional accumulation.
    d['postentry_flow_label'] = np.select(
        [d.heavy_down_proxy,d.up_day_high_participation,d.lower_wick_with_turnover,d.quiet_down_proxy],
        ['HEAVY_DOWN_PROXY','STRONG_UP_WITH_TURNOVER','LOWER_WICK_HIGH_TURNOVER','QUIET_DOWN_PROXY'],
        default='OTHER_OR_NOT_ENOUGH_HISTORY')
    return d


def simulate_fixed_or_structural(d:pd.DataFrame,d0_idx:int,entry_idx:int,entry_price:float,
                                 structural:bool=False):
    """A/B share entry, +10% TP, original D0+20 horizon, stop-first and costs.

    A: unconditional -5% stop.
    B: -5% soft stop may be deferred ONLY using EOD of j-1, with -12% hard stop,
       a next-open structural break exit, and intraday stop-first. B is a riskier
       exploratory *policy*, NOT an automatically safer stop rule.
    """
    end = d0_idx+20
    if not (0<entry_idx<end<len(d)):raise ValueError('TRADING_WINDOW_INVALID')
    tp=entry_price*1.10;sl=entry_price*.95;hard=entry_price*.88
    pending=False
    def pack(j,px,reason,ambiguous=False):
        return dict(exit_date=str(d.date.iloc[j]),exit_price=float(px),exit_reason=reason,
                    net_pct=100*(float(px)/entry_price-1)-COST_PCT,
                    ambiguous_same_day=bool(ambiguous))
    for j in range(entry_idx,end+1):
        row=d.iloc[j];o,h,l=(float(row[x]) for x in ('open','high','low'))
        if structural and pending and j>entry_idx:
            return pack(j,o,'STRUCTURE_BREAK_NEXT_OPEN')
        # No hypothetical gap before an opening-price entry on its own day.
        # Evaluate the previous close BEFORE observing today's price path.
        prev=d.iloc[j-1]
        eligible = (structural and pd.notna(prev.pre_next_session_soft_stop_deferral)
                    and bool(prev.pre_next_session_soft_stop_deferral))
        effective_sl=hard if eligible else sl
        if j>entry_idx:
            if structural and o<=hard:return pack(j,o,'HARD_GAP')
            if o<=effective_sl:
                return pack(j,o,'SOFT_GAP' if structural else 'SL_GAP')
            if o>=tp:return pack(j,o,'TP_GAP')
        if structural:
            # Hard stop always active, even when prior EOD suggests soft deferral.
            if l<=hard:return pack(j,hard,'HARD_STOP',h>=tp)
        if l<=effective_sl and h>=tp:
            return pack(j,effective_sl,'SOFT_BOTH_HIT' if structural else 'SL_BOTH_HIT',True)
        if l<=effective_sl:
            return pack(j,effective_sl,'SOFT_STOP' if structural else 'SL')
        if h>=tp:return pack(j,tp,'TP')
        if structural:
            # This EOD classification can trigger exit only at next session open.
            pending=bool(row.lower_close_break and row.heavy_down_proxy)
    return pack(end,float(d.close.iloc[end]),'TIME_D20')


def load_optional_investor_pit(path):
    """Optional archived actual net buying, NEVER inferred from OHLCV.

    'known_at' is the first session date at whose END the source was available.
    Same-bar close inputs can affect ONLY the next session. Future-dated source
    records never populate earlier EOD or intraday decisions.
    """
    if path is None:return None
    p=Path(path)
    if not p.is_file():raise FileNotFoundError('INVESTOR_PIT_SOURCE_NOT_FOUND '+str(p))
    t=pd.read_csv(p,dtype={'code':str,'date':str,'known_at':str,'source_id':str})
    req={'code','date','known_at','foreign_net_krw','institution_net_krw','retail_net_krw','source_id'}
    if not req.issubset(t):raise ValueError('INVESTOR_PIT_SCHEMA_MISSING '+str(req-set(t)))
    if t.empty:raise ValueError('EMPTY_INVESTOR_PIT_NOT_ZERO')
    t.code=t.code.str.zfill(6)
    if t.duplicated(['code','date']).any() or not t.code.str.fullmatch(r'\d{6}').all():
        raise ValueError('INVESTOR_PIT_IDENTITY_INVALID')
    if not (t.date.str.fullmatch(r'\d{4}-\d\d-\d\d').all()
            and t.known_at.str.fullmatch(r'\d{4}-\d\d-\d\d').all()):
        raise ValueError('INVESTOR_PIT_DATE_INVALID')
    if t.source_id.isna().any() or t.source_id.eq('').any():
        raise ValueError('INVESTOR_PIT_MISSING_PROVENANCE')
    for col in ('foreign_net_krw','institution_net_krw','retail_net_krw'):
        t[col]=pd.to_numeric(t[col],errors='raise')
        if not np.isfinite(t[col]).all():raise ValueError('INVESTOR_PIT_BAD_AMOUNT')
    return t.set_index(['code','date'])


def study_post_entry(events:pd.DataFrame,frames:dict,out:Path,investor_pit_path=None):
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    codes=set(events.code)
    enriched={c:add_bar_features(frames[c]) for c in codes}
    pit=load_optional_investor_pit(investor_pit_path)
    pit_used=0
    path=[];comparisons=[];stop_states=[];poststop=[]
    max_window=0
    for e in events.itertuples(index=False):
        d=enriched[e.code]; m=pd.Series(d.index,index=d.date)
        for k in ('date','entry_date','exit_date'):
            if str(getattr(e,k)) not in m:
                raise ValueError('EVENT_DATE_UNRESOLVED '+e.code+' '+str(getattr(e,k)))
        si=int(m[e.date]);ei=int(m[e.entry_date]);xi=int(m[e.exit_date]);end=si+20
        if not(0<ei-si<20 and end<len(d) and ei<=xi<=end):
            raise ValueError('EVENT_CHRONOLOGY_INVALID '+e.code+e.date)
        a=simulate_fixed_or_structural(d,si,ei,float(e.entry_price),False)
        if a['exit_date']!=str(e.exit_date) or abs(a['net_pct']-float(e.net_pct))>1e-5:
            raise ValueError('ORIGINAL_BASELINE_REPLAY_MISMATCH '+e.code+' '+e.date+' '+str(a))
        b=simulate_fixed_or_structural(d,si,ei,float(e.entry_price),True)
        base=dict(code=e.code,date=e.date,year=e.year,band=e.band,gc_date=e.gc_date,
                  entry_date=e.entry_date,entry_price=e.entry_price,reference_exit_date=e.exit_date,
                  reference_exit_reason=e.exit_reason,reference_net_pct=float(e.net_pct))
        comparisons.append(dict(**base,base_net_pct=float(e.net_pct),base_exit_reason=a['exit_reason'],
                                structural_net_pct=b['net_pct'],structural_exit_date=b['exit_date'],
                                structural_exit_reason=b['exit_reason'],
                                structural_minus_fixed_pp=b['net_pct']-float(e.net_pct),
                                exploratory_only=True))
        if e.failed_sl:
            pre=d.iloc[xi-1]
            stoprow=d.iloc[xi]
            stop_states.append(dict(**base,pre_stop_close_date=str(pre.date),
                pre_stop_eod_soft_deferral=bool(pre.pre_next_session_soft_stop_deferral),
                pre_stop_eod_squeeze=bool(pre.shifted_band_squeeze),
                pre_stop_eod_lower_hold=bool(pre.lower_close_hold),
                pre_stop_eod_flow=str(pre.postentry_flow_label),
                pre_stop_eod_candle=str(pre.candle_shape),
                pre_stop_amount_ratio20=pre.amount_ratio20,
                stop_day_candle_EOD_UNAVAILABLE_FOR_INTRADAY_DECISION=str(stoprow.candle_shape),
                stop_day_flow_EOD_UNAVAILABLE_FOR_INTRADAY_DECISION=str(stoprow.postentry_flow_label),
                structural_net_pct=b['net_pct'],structural_minus_fixed_pp=b['net_pct']-float(e.net_pct)))
            # Post-stop upper cross is future-only; never used as an input at stop time.
            future=d.iloc[xi+1:end+1]
            hits=future[future.upper_body_break]
            if len(hits):
                h=hits.iloc[0]
                poststop.append(dict(**base,upper_body_break_after_stop_date=str(h.date),
                                     upper_body_break_after_stop_offset=int(h.name-si),
                                     label='FUTURE_OBSERVATION_ONLY_NOT_PREDICTOR'))
        for j in range(ei,end+1):
            r=d.iloc[j];prev=d.iloc[j-1]
            future_one=d.iloc[j+1] if j+1<len(d) and j<end else None
            # STRICTLY OUTCOMES: never referenced by classification or trading policy.
            next_day_close_return_pct=(100*(float(future_one.close)/float(r.close)-1)
                if future_one is not None else np.nan)
            next_day_low_return_pct=(100*(float(future_one.low)/float(r.close)-1)
                if future_one is not None else np.nan)
            inv={'investor_pit_status':'PIT_MISSING','foreign_net_krw':np.nan,
                 'institution_net_krw':np.nan,'retail_net_krw':np.nan,'investor_source_id':''}
            if pit is not None and (e.code,str(r.date)) in pit.index:
                z=pit.loc[(e.code,str(r.date))]
                if z.known_at<=str(r.date):
                    inv={'investor_pit_status':'SOURCE_AVAILABLE_ASOF_EOD',
                         'foreign_net_krw':z.foreign_net_krw,
                         'institution_net_krw':z.institution_net_krw,
                         'retail_net_krw':z.retail_net_krw,
                         'investor_source_id':z.source_id}
                    pit_used+=1
                else:
                    inv['investor_pit_status']='NOT_YET_KNOWN_AT_EOD'
            path.append(dict(**inv,**base,bar_date=str(r.date),d0_offset=j-si,entry_offset=j-ei,
                 baseline_alive_at_open=j<=xi,
                 baseline_alive_after_close=j<xi,
                 after_baseline_exit_observation_only=j>xi,
                 next_day_close_return_pct_OUTCOME_ONLY=next_day_close_return_pct,
                 next_day_low_return_pct_OUTCOME_ONLY=next_day_low_return_pct,
                 open=r.open,high=r.high,low=r.low,close=r.close,volume=r.volume,amount=r.amount,
                 candle_shape=r.candle_shape,body_to_range=r.body_to_range,
                 lower_wick_to_range=r.lower_wick_to_range,upper_wick_to_range=r.upper_wick_to_range,
                 close_location=r.close_location,volume_ratio20=r.volume_ratio20,
                 amount_ratio20=r.amount_ratio20,price_return1_pct=r.price_return1_pct,
                 flow_label=r.postentry_flow_label,heavy_down_proxy=r.heavy_down_proxy,
                 quiet_down_proxy=r.quiet_down_proxy,lower_wick_with_turnover=r.lower_wick_with_turnover,
                 shifted_upper=r.bb20_upper_shift26,shifted_lower=r.bb20_lower_shift26,
                 shifted_band_width_pct=r.band_width_pct,shifted_band_squeeze=r.shifted_band_squeeze,
                 lower_close_hold=r.lower_close_hold,lower_intraday_recover=r.lower_intraday_recover,
                 upper_body_break=r.upper_body_break,ma60=r.ma60,ma112=r.ma112,ma224=r.ma224,
                 ma60_slope10=r.ma60_slope10,ma112_slope10=r.ma112_slope10,
                 prev_eod_date=str(prev.date),
                 prev_eod_soft_stop_deferral=bool(prev.pre_next_session_soft_stop_deferral),
                 prev_eod_flow_label=prev.postentry_flow_label,
                 prev_eod_candle_shape=prev.candle_shape))
        max_window=max(max_window,end-ei+1)
    diary=pd.DataFrame(path);comp=pd.DataFrame(comparisons);prestop=pd.DataFrame(stop_states)
    future_upper=pd.DataFrame(poststop)
    diary.to_csv(out/'06_POST_ENTRY_DAILY_OHLCV_CANDLES.csv.gz',compression='gzip',index=False)
    comp.to_csv(out/'07_FIXED_STOP_VS_STRUCTURAL_POLICY.csv',index=False)
    prestop.to_csv(out/'08_PRE_STOP_PRIOR_EOD_EVIDENCE.csv',index=False)
    future_upper.to_csv(out/'09_POST_STOP_UPPER_BREAK_FUTURE_ONLY.csv',index=False)
    if len(diary)==0 or len(comp)!=1804:raise ValueError('POST_ENTRY_EVENT_COUNT_ERROR')
    metrics=[]
    for keys in [['band'],['year','band'],['band','base_exit_reason'],['band','structural_exit_reason']]:
        for group,g in comp.groupby(keys,dropna=False):
            if not isinstance(group,tuple):group=(group,)
            metrics.append(dict(group_type=' × '.join(keys),group_value=' × '.join(map(str,group)),
                n=len(g),stocks=g.code.nunique(),avg_fixed_pct=float(g.base_net_pct.mean()),
                avg_structural_pct=float(g.structural_net_pct.mean()),
                avg_policy_difference_pp=float(g.structural_minus_fixed_pp.mean()),
                fraction_structural_better=float((g.structural_net_pct>g.base_net_pct).mean()),
                frac_structural_loss_over10=float((g.structural_net_pct<=-10).mean()),
                exploratory_not_oos=True))
    pd.DataFrame(metrics).to_csv(out/'10_POLICY_OUTCOME_SUMMARY.csv',index=False)
    # This summary is an EOD-t -> t+1 observational association, not a
    # backfilled entry rule or proof of an institution's actual buying.
    active=diary[diary.baseline_alive_after_close & diary.next_day_close_return_pct_OUTCOME_ONLY.notna()].copy()
    active['turnover_bucket']=pd.cut(active.amount_ratio20,
        bins=[-np.inf,0.7,1.2,1.8,np.inf],
        labels=['DRYUP<=0.7','NORMAL0.7-1.2','INCREASE1.2-1.8','SURGE>1.8'])
    active['ma60_slope_sign']=np.select([active.ma60_slope10.gt(.1),active.ma60_slope10.lt(-.1)],
                                        ['MA60_UP','MA60_DOWN'],default='MA60_FLAT_OR_MISSING')
    panels=[['band','candle_shape'],['band','flow_label'],['band','turnover_bucket'],
            ['band','candle_shape','turnover_bucket'],['band','shifted_band_squeeze','lower_close_hold'],
            ['band','flow_label','ma60_slope_sign']]
    frows=[]
    for group_cols in panels:
        for group,g in active.groupby(group_cols,dropna=False,observed=True):
            if not isinstance(group,tuple):group=(group,)
            frows.append(dict(group_type=' × '.join(group_cols),group_value=' × '.join(map(str,group)),
                observed_EOD_bars=len(g),distinct_events=g[['code','date','band']].drop_duplicates().shape[0],
                distinct_stocks=g.code.nunique(),
                year_2023_bars=int((g.year==2023).sum()),year_2024_bars=int((g.year==2024).sum()),
                avg_next_session_close_return_pct=float(g.next_day_close_return_pct_OUTCOME_ONLY.mean()),
                next_session_close_up_rate_pct=float(100*g.next_day_close_return_pct_OUTCOME_ONLY.gt(0).mean()),
                next_session_low_below_minus5_pct=float(100*g.next_day_low_return_pct_OUTCOME_ONLY.le(-5).mean()),
                enough_for_exploratory_read=bool(len(g)>=100 and g.code.nunique()>=20 and
                       (g.year==2023).sum()>=20 and (g.year==2024).sum()>=20),
                repeated_events_and_days_non_independent=True,
                next_day_is_future_outcome_not_feature=True))
    pd.DataFrame(frows).to_csv(out/'12_NEXT_DAY_BY_EOD_CANDLE_TURNOVER.csv',index=False)
    # Representative named event: actual daily bars available only after GitHub run.
    for code,date in [('950130','2024-07-17'),('006220','2024-01-24')]:
        d=diary[(diary.code==code)&(diary.date==date)]
        if not d.empty:d.to_csv(out/f'11_DAILY_CASE_{code}_{date}.csv',index=False)
    audit={'version':VERSION,'primary_events':len(comp),'post_entry_observation_rows':len(diary),
           'first_stop_prior_eod_rows':len(prestop),'future_only_poststop_upper_events':len(future_upper),
           'max_window_bars':max_window,'baseline_replayed_exactly':True,
           'EOD_prediction_next_day_stat_is_future_outcome_only':True,
           'EOD_next_day_summary_has_repeated_nonindependent_days':True,
           'new_policy_evaluated_with_prev_close_only':True,
           'investor_pit_source_provided':bool(investor_pit_path),
           'investor_pit_available_event_bar_rows':pit_used,
           'investor_pit_event_bar_coverage_pct':round(100*pit_used/len(diary),2),
           'investor_rows_supplied_but_not_independently_certified':bool(pit_used),
           'institution_foreign_net_buying_verified':False,
           'aggressor_bid_ask_flow_verified':False,
           'volume_and_turnover_are_proxies_not_investor_flows':True,
           'candle_patterns_require_daily_close_and_not_known_intraday':True,
           'stop_first_for_ambiguous_intraday_bars':True,
           'hard_loss_limit_is_price_level_not_guaranteed_at_gap_open':True,
           'different_stop_risk_profiles_not_directly_equivalent':True,
           'study_not_recommended_for_live_trading':True,'production_ready':False}
    (out/'POST_ENTRY_PIT_AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2),encoding='utf-8')
    return audit,diary
