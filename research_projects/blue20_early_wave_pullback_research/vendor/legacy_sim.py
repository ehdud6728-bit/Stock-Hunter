import pandas as pd
COST_PCT=.25
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
