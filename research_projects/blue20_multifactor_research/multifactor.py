#!/usr/bin/env python3
"""BLUE20 RSI delay × Slow stochastic × market × turnover × first wave research.

External PIT sector/theme mapping optional. An absent or unverifiable mapping is
MISSING, not a neutral/weak sector. No future bars used to create input features.
The outcome fields are outcomes from frozen BLUE20 research, NEVER features.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
VERSION = 'BLUE20_MULTIFACTOR_PIT_R1_20261106'
REQUIRED_PRICE = {'code','date','open','high','low','close','volume','amount'}
REQUIRED_EVENTS = {'code','date','band','label','rsi_mode','gc_status','gc_offset','gc_date',
                   'GC_NEXT_OPEN_entry_price','GC_NEXT_OPEN_net_pct',
                   'GC_NEXT_OPEN_exit_reason','GC_NEXT_OPEN_mfe_pct'}


def sha256(p):
    h=hashlib.sha256()
    with open(p,'rb') as f:
        for b in iter(lambda:f.read(1<<20),b''):h.update(b)
    return h.hexdigest()


def slow_stochastic(d, period=14, k_smooth=3, d_smooth=3):
    lo=d.low.rolling(period,min_periods=period).min()
    hi=d.high.rolling(period,min_periods=period).max()
    span=hi-lo
    fast=(100*(d.close-lo)/span).where(span.ne(0),50.0).clip(0,100)
    fast=fast.where(lo.notna())
    slowk=fast.rolling(k_smooth,min_periods=k_smooth).mean()
    slowd=slowk.rolling(d_smooth,min_periods=d_smooth).mean()
    cross=slowk.shift(1).le(slowd.shift(1)) & slowk.gt(slowd) & slowd.shift(1).notna()
    return slowk,slowd,cross.fillna(False)


def wave_pre_d0(d, idx):
    """All pivot extrema except confirmation close occur before D0.

    Prior peak: maximal High in D-35..D-3, at least two bars before D0.
    Prior trough: minimum Low in 35 bars preceding that peak.
    Pullback trough: minimum Low from peak+1 to D-1.
    Volume contraction: pullback mean Volume vs five days before/at peak.
    This is a heuristic, not claim of user's unique true first wave.
    """
    missing=dict(wave_state='INSUFFICIENT_HISTORY',wave_advance_pct=np.nan,
                 wave_pullback_pct=np.nan,wave_rebound_pct=np.nan,
                 wave_volume_contract_ratio=np.nan,wave_ma224_context='UNKNOWN')
    if idx<75 or idx>=len(d):return missing
    peaks=d.iloc[max(0,idx-35):idx-2]
    if peaks.empty:return missing
    peak_idx=int(np.argmax(peaks.high.to_numpy()))+max(0,idx-35)
    trough_before=d.iloc[max(0,peak_idx-35):peak_idx]
    after=d.iloc[peak_idx+1:idx]
    if trough_before.empty or after.empty:return missing
    peak=float(d.iloc[peak_idx].high)
    trough=float(trough_before.low.min())
    low_after=float(after.low.min())
    if min(peak,trough,low_after)<=0:return missing
    adv=(peak/trough-1)*100;pull=(low_after/peak-1)*100
    rebound=(float(d.iloc[idx].close)/low_after-1)*100
    baseline=d.iloc[max(0,peak_idx-5):peak_idx+1].volume.mean()
    ratio=float(after.volume.mean()/baseline) if baseline and np.isfinite(baseline) else np.nan
    ma224=d.close.iloc[:peak_idx+1].tail(224).mean() if peak_idx>=223 else np.nan
    pre_close=float(d.iloc[peak_idx].close)
    ma_context=('BELOW_MA224' if pre_close<ma224 else 'ABOVE_MA224') if np.isfinite(ma224) else 'UNKNOWN'
    # Multiple independent interpretable factors; strict intersection reported as one label.
    passed=adv>=15 and -30<=pull<=-5 and rebound>=3 and np.isfinite(ratio) and ratio<=0.8
    return dict(wave_state='WAVE_PULLBACK' if passed else 'NOT_WAVE_PULLBACK',
                wave_advance_pct=adv,wave_pullback_pct=pull,wave_rebound_pct=rebound,
                wave_volume_contract_ratio=ratio,wave_ma224_context=ma_context)


def load_verified_manifest(manifest):
    manifest=Path(manifest)
    m=pd.read_csv(manifest,dtype=str).fillna('')
    if not {'code','path','sha256'}.issubset(m) or m.code.duplicated().any():
        raise ValueError('MANIFEST_SCHEMA_OR_DUPLICATE_INVALID')
    if len(m)!=500:raise ValueError('EXACT_500_REQUIRED')
    frames=[];hashes={}
    for row in m.itertuples(index=False):
        rel=Path(row.path)
        if rel.is_absolute() or '..' in rel.parts:raise ValueError('PATH_TRAVERSAL_BLOCKED')
        p=(manifest.parent/rel).resolve()
        if not p.is_relative_to(manifest.parent.resolve()) or not p.is_file():
            raise ValueError('PRICE_FILE_MISSING_OR_OUTSIDE '+row.code)
        got=sha256(p)
        if got.lower()!=row.sha256.lower():raise ValueError('PRICE_SHA_MISMATCH '+row.code)
        t=pd.read_csv(p,dtype={'code':str,'date':str})
        if not REQUIRED_PRICE.issubset(t):raise ValueError('PRICE_FIELDS_MISSING '+row.code)
        if not t.code.eq(row.code).all() or not t.date.is_monotonic_increasing or t.date.duplicated().any():
            raise ValueError('PRICE_IDENTITY_OR_CALENDAR_INVALID '+row.code)
        if t[['open','high','low','close']].le(0).any().any():raise ValueError('NON_POSITIVE_PRICE '+row.code)
        frames.append(t[list(['code','date','open','high','low','close','volume','amount'])]);hashes[row.code]=got
    allprices=pd.concat(frames,ignore_index=True)
    return allprices,hashes


def prepare_panel(prices,market_daily,stoch=(14,3,3)):
    d=prices.copy();d.code=d.code.astype(str).str.zfill(6)
    d.date=d.date.astype(str)
    d=d.sort_values(['code','date']).reset_index(drop=True)
    g=d.groupby('code',sort=False,group_keys=False)
    for n in (5,20,60,112,224):
        d[f'ma{n}']=g.close.transform(lambda s:s.rolling(n,min_periods=n).mean())
    for n in (5,20):
        d[f'ret{n}_pct']=100*(d.close/g.close.shift(n)-1)
    d['amount_prior20']=g.amount.transform(lambda s:s.shift(1).rolling(20,min_periods=20).mean())
    d['vol_prior20']=g.volume.transform(lambda s:s.shift(1).rolling(20,min_periods=20).mean())
    d['amount_ratio']=d.amount/d.amount_prior20.replace(0,np.nan)
    d['volume_ratio']=d.volume/d.vol_prior20.replace(0,np.nan)
    sto=[]
    for code,group in d.groupby('code',sort=False):
        k,dd,cr=slow_stochastic(group,*stoch)
        e=pd.DataFrame({'row':group.index,'stoch_k':k.to_numpy(),'stoch_d':dd.to_numpy(),
                        'stoch_gc':cr.to_numpy(),'stoch_k_slope3':(k-k.shift(3)).to_numpy()})
        sto.append(e)
    extra=pd.concat(sto,ignore_index=True).set_index('row')
    d=d.join(extra)
    # Exact four RSI variants of the original BLUE20 experiment, enabling
    # an unbiased late-Stochastic confirmation check without RSI-state guessing.
    from numpy import nan
    rsi_frames=[]
    for code,group in d.groupby('code',sort=False):
        close=group.close
        delta=close.diff()
        up=delta.clip(lower=0).rolling(14,min_periods=14).mean()
        down=(-delta.clip(upper=0)).rolling(14,min_periods=14).mean()
        rs=up/down.replace(0,np.nan)
        cutler=100-100/(1+rs)
        cutler=cutler.where(down.ne(0),100.0).where(up.ne(0),0.0)
        cutler=cutler.where((up.ne(0))|(down.ne(0)),50.0)
        cutler=cutler.mask(up.isna()|down.isna())
        vals=close.to_numpy(dtype=float)
        out=np.full(len(vals),np.nan)
        if len(vals)>=15:
            gains=np.maximum(np.diff(vals),0.0);losses=np.maximum(-np.diff(vals),0.0)
            ag=float(gains[:14].mean());al=float(losses[:14].mean())
            for i in range(14,len(vals)):
                if i>14:
                    ag=(13*ag+gains[i-1])/14
                    al=(13*al+losses[i-1])/14
                out[i]=50. if ag==0 and al==0 else 100. if al==0 else 100-100/(1+ag/al)
        wilder=pd.Series(out,index=group.index)
        rows={'row':group.index}
        for basis,series in [('CUTLER',cutler),('WILDER',wilder)]:
            for smooth in ('SMA9','EMA9'):
                mode=f'{basis}_{smooth}'
                signal=(series.rolling(9,min_periods=9).mean() if smooth=='SMA9'
                        else series.ewm(span=9,adjust=False,min_periods=9).mean())
                rows['rsi_'+mode]=series.to_numpy()
                rows['rsi_signal_'+mode]=signal.to_numpy()
        rsi_frames.append(pd.DataFrame(rows))
    d=d.join(pd.concat(rsi_frames,ignore_index=True).set_index('row'))
    # Market is available historically only when a vintage market membership file is provided.
    md=market_daily.copy();md.date=md.date.astype(str)
    md=md.sort_values(['market','date']).reset_index(drop=True)
    if set(['market','date','close','amount'])-set(md):raise ValueError('MARKET_INDEX_MISSING_FIELDS')
    if md.duplicated(['market','date']).any():raise ValueError('DUPLICATE_MARKET_INDEX_DATE')
    gm=md.groupby('market',sort=False)
    md['market_ret5_pct']=100*(md.close/gm.close.shift(5)-1)
    md['market_ret20_pct']=100*(md.close/gm.close.shift(20)-1)
    md['market_ma20']=gm.close.transform(lambda s:s.rolling(20,min_periods=20).mean())
    md['market_ma60']=gm.close.transform(lambda s:s.rolling(60,min_periods=60).mean())
    md['market_amount_prior20']=gm.amount.transform(lambda s:s.shift(1).rolling(20,min_periods=20).mean())
    md['market_amount_ratio']=md.amount/md.market_amount_prior20.replace(0,np.nan)
    md['market_regime']=np.select([
        (md.close>md.market_ma20)&(md.market_ma20>md.market_ma60),
        (md.close<md.market_ma20)&(md.market_ma20<md.market_ma60),
    ],['UPTREND','DOWNTREND'],default='MIXED')
    md.loc[md.market_ma60.isna(),'market_regime']='INSUFFICIENT_HISTORY'
    return d,md


def load_pit_membership(path,kind):
    if not path:return None
    p=Path(path)
    if not p.is_file():raise FileNotFoundError(f'{kind} PIT file missing: {p}')
    t=pd.read_csv(p,dtype=str).fillna('')
    required={'code',kind,'known_at','valid_from','valid_to'}
    if not required.issubset(t):raise ValueError(f'{kind.upper()}_PIT_REQUIRED_'+','.join(sorted(required)))
    if t.empty:return None
    t.code=t.code.str.zfill(6)
    if t[['code',kind,'known_at','valid_from']].eq('').any().any():
        raise ValueError('PIT_BLANK_CLASSIFICATION_OR_KNOWN_DATE')
    if ((t.valid_to!='')&(t.valid_to<t.valid_from)).any():raise ValueError('PIT_INVALID_EFFECTIVE_PERIOD')
    if not t[['known_at','valid_from']].stack().str.fullmatch(r'\d{4}-\d\d-\d\d').all():
        raise ValueError('PIT_INVALID_DATE_FORMAT')
    return t


def assign_pit(d, mapping,kind):
    d=d.copy()
    d[kind]='PIT_MISSING'
    if mapping is None:return d
    # Known-at is enforced at each bar; supplied industry mappings never backfill historical future info.
    for r in mapping.itertuples(index=False):
        lo=max(r.known_at,r.valid_from)
        hi=r.valid_to if r.valid_to else '9999-12-31'
        mask=(d.code==r.code)&(d.date>=lo)&(d.date<=hi)
        if not mask.any():continue
        if kind=='sector' and d.loc[mask,kind].ne('PIT_MISSING').any():
            raise ValueError('OVERLAPPING_SECTOR_PIT_MEMBERSHIP '+r.code)
        # Multiple themes per stock supported in separate sector-independent aggregation.
        if kind=='theme':
            prev=d.loc[mask,kind]
            d.loc[mask,kind]=np.where(prev=='PIT_MISSING',getattr(r,kind),prev+';'+getattr(r,kind))
        else:d.loc[mask,kind]=getattr(r,kind)
    return d


def current_day_factors(panel,market,sector_mapping=None,theme_mapping=None):
    d=panel.copy()
    # Use PIT market assignment (code, date) when supplied by source, not current exchange market.
    if 'market' not in d:
        d['market']='PIT_MISSING'
    d=d.merge(market[['market','date','market_ret5_pct','market_ret20_pct','market_ma20','market_ma60','market_regime','market_amount_ratio']],on=['market','date'],how='left',validate='many_to_one')
    d['cohort_amount_rank_pct']=d.groupby(['date','market']).amount.rank(pct=True,ascending=True)
    d['cohort_amount_rank_500']=d.groupby('date').amount.rank(ascending=False,method='min')
    d['cohort_leader_top10']=d.cohort_amount_rank_pct.ge(.90)
    d['cohort_leader_top20']=d.cohort_amount_rank_pct.ge(.80)
    d['breadth_up20']=d.close.gt(d.ma20).groupby([d.date,d.market]).transform('mean')
    d=assign_pit(d,sector_mapping,'sector')
    d=assign_pit(d,theme_mapping,'theme')
    sector_ok=d.sector!='PIT_MISSING'
    if sector_ok.any():
        grp=d[sector_ok].groupby(['date','market','sector'],sort=False)
        x=grp.ret20_pct.agg(['median','count']).reset_index().rename(columns={'median':'sector_ret20_pct','count':'sector_coverage_n'})
        d=d.merge(x,on=['date','market','sector'],how='left')
        d['sector_relative20_pp']=d.sector_ret20_pct-d.market_ret20_pct
        d.loc[d.sector_coverage_n.lt(5),'sector_relative20_pp']=np.nan
    else:
        d['sector_ret20_pct']=np.nan;d['sector_coverage_n']=np.nan;d['sector_relative20_pp']=np.nan
    d['sector_strength']=np.where(d.sector_relative20_pp.notna(),np.where(d.sector_relative20_pp>0,'STRONG','WEAK'), 'PIT_MISSING')
    # Historical sector leadership is strictly scoped to valid members of
    # the 2022-fixed TOP500 cohort, not the all-market industry universe.
    d['sector_amount_rank']=np.nan
    d['sector_amount_rank_pct']=np.nan
    d['sector_leader']='PIT_MISSING'
    if sector_ok.any():
        sub=d[sector_ok].copy()
        rank_groups=sub.groupby(['date','market','sector'],sort=False)
        sub['sector_amount_rank']=rank_groups.amount.rank(ascending=False,method='min')
        sub['sector_amount_rank_pct']=rank_groups.amount.rank(pct=True,ascending=True)
        sub['sector_member_count']=rank_groups.amount.transform('size')
        usable=sub.sector_member_count.ge(5)
        sub['sector_leader']=np.select([
            usable & sub.sector_amount_rank.eq(1),
            usable & sub.sector_amount_rank_pct.ge(.8),
            usable],['COHORT_SECTOR_TURNOVER_1','COHORT_SECTOR_TOP20','COHORT_SECTOR_OTHER'],default='INSUFFICIENT_PEERS')
        for c in ['sector_amount_rank','sector_amount_rank_pct','sector_leader']:
            d.loc[sub.index,c]=sub[c]
    # Per-code theme label presence, not historical theme index unless enough members are PIT verified.
    d['theme_status']=np.where(d.theme=='PIT_MISSING','PIT_MISSING','HISTORICAL_MEMBER_VERIFIED')
    # Theme membership provenance is required. Theme strength calculated
    # only from >=5 concurrently PIT-classified TOP500 members, using
    # cross-sectional median 20-day returns (all known by the event close).
    d['theme_relative20_pp']=np.nan
    d['theme_selected']='PIT_MISSING'
    if theme_mapping is not None and d.theme.ne('PIT_MISSING').any():
        subset=d[d.theme.ne('PIT_MISSING')][['code','date','market','theme','ret20_pct','market_ret20_pct']].copy()
        subset['theme']=subset.theme.str.split(';')
        subset=subset.explode('theme')
        tgr=subset.groupby(['date','market','theme'],sort=False).ret20_pct.agg(['median','count']).reset_index()
        tgr=tgr[tgr['count']>=5].rename(columns={'median':'theme_ret20_pct'})
        subset=subset.merge(tgr[['date','market','theme','theme_ret20_pct']],on=['date','market','theme'],how='left')
        subset['theme_relative20_pp']=subset.theme_ret20_pct-subset.market_ret20_pct
        # When a stock has several themes, mark strongest contemporaneous
        # as a descriptive as-of attribute; never create membership ex-post.
        subset=subset.sort_values('theme_relative20_pp',ascending=False,na_position='last').drop_duplicates(['code','date'])
        a=d.reset_index().merge(subset[['code','date','theme','theme_relative20_pp']],on=['code','date'],how='left')
        d['theme_relative20_pp']=a['theme_relative20_pp_y'].to_numpy()
        d['theme_selected']=a.theme_y.fillna('PIT_MISSING').to_numpy() if 'theme_y' in a else 'PIT_MISSING'
    return d


def simulate_joint_next_open(g, d0_idx, confirm_offset, cost_pct=.25):
    """Evaluate entry at the NEXT open after both indicators confirmed at close.

    The exit horizon remains D0+20. Same-day high/low both trigger: stop first.
    No gap stop may be executed before the entry open.
    """
    entry_idx=d0_idx+int(confirm_offset)+1
    end_idx=d0_idx+20
    if entry_idx>=end_idx or end_idx>=len(g):return None
    entry=float(g.iloc[entry_idx].open)
    if entry<=0:return None
    tp=entry*1.1;sl=entry*.95
    future=g.iloc[entry_idx:end_idx+1]
    high=float(future.high.max())
    low=float(future.low.min())
    exit_px=float(g.iloc[end_idx].close)
    reason='TIME_D20';ambiguous=False
    for i in range(entry_idx,end_idx+1):
        r=g.iloc[i]
        if i>entry_idx and float(r.open)<=sl:
            exit_px=float(r.open);reason='SL_GAP';break
        if i>entry_idx and float(r.open)>=tp:
            exit_px=float(r.open);reason='TP_GAP';break
        if float(r.low)<=sl and float(r.high)>=tp:
            exit_px=sl;reason='SL_BOTH_HIT';ambiguous=True;break
        if float(r.low)<=sl:
            exit_px=sl;reason='SL';break
        if float(r.high)>=tp:
            exit_px=tp;reason='TP';break
    return dict(JOINT_entry_date=str(g.iloc[entry_idx].date),
       JOINT_entry_price=entry,JOINT_exit_reason=reason,JOINT_ambiguous=ambiguous,
       JOINT_net_pct=100*(exit_px/entry-1)-cost_pct,
       JOINT_peak_gross_pct=100*(high/entry-1),JOINT_trough_gross_pct=100*(low/entry-1))


def enrich_stoch_timing(e,per):
    """RSI first GC and first NEW Slow GC in D0..D10; no GC look-ahead in features.

    JOINT uses both confirmed values on the later confirmation day. A subsequent
    Slow cross is a future event relative to RSI-only entry, and is therefore
    kept in JOINT strategy outcomes only, never RSI-entry features.
    """
    rows=[]
    index_cache={c:{v:i for i,v in enumerate(g.date)} for c,g in per.items()}
    for r in e.itertuples(index=False):
        out=dict(slow_first_offset=np.nan,slow_vs_rsi='NO_SLOW_GC_D0_D10',
                 joint_status='NO_TRADE',JOINT_entry_date='',JOINT_entry_price=np.nan,
                 JOINT_exit_reason='',JOINT_ambiguous=False,JOINT_net_pct=np.nan,
                 JOINT_peak_gross_pct=np.nan,JOINT_trough_gross_pct=np.nan)
        g=per.get(r.code)
        i=index_cache.get(r.code,{}).get(r.date)
        if g is None or i is None or i+20>=len(g):
            out['joint_status']='INCOMPLETE_HORIZON';rows.append(out);continue
        crosses=np.flatnonzero(g.iloc[i:i+11].stoch_gc.fillna(False).to_numpy(dtype=bool))
        if len(crosses):
            slow_offset=int(crosses[0]);out['slow_first_offset']=slow_offset
        else:slow_offset=None
        rsi_offset=int(r.gc_offset) if pd.notna(r.gc_offset) and r.gc_status=='GC_OBSERVED' else None
        if rsi_offset is None:
            out['joint_status']='NO_RSI_GC';rows.append(out);continue
        if slow_offset is None:
            out['joint_status']='NO_STOCH_GC';rows.append(out);continue
        if slow_offset<rsi_offset:out['slow_vs_rsi']='SLOW_BEFORE_RSI'
        elif slow_offset==rsi_offset:out['slow_vs_rsi']='SLOW_SAME_DAY'
        else:out['slow_vs_rsi']='SLOW_AFTER_RSI'
        later=max(slow_offset,rsi_offset)
        d=g.iloc[i+later]
        if not (pd.notna(d.stoch_k) and pd.notna(d.stoch_d) and d.stoch_k>d.stoch_d):
            out['joint_status']='SLOW_NOT_BULLISH_AT_JOIN';rows.append(out);continue
        # If Slow crosses AFTER RSI, RSI must still be ABOVE its exact
        # predeclared signal line at the later close. This check is PIT-safe.
        mode=r.rsi_mode
        ri=d.get('rsi_'+mode,np.nan);sg=d.get('rsi_signal_'+mode,np.nan)
        if not (np.isfinite(ri) and np.isfinite(sg) and ri>sg):
            out['joint_status']='RSI_NOT_BULLISH_AT_JOIN';rows.append(out);continue
        out['joint_status']='JOINT_CONFIRMED'
        result=simulate_joint_next_open(g,i,later)
        if result:out.update(result)
        else:out['joint_status']='ENTRY_AFTER_HORIZON'
        rows.append(out)
    return pd.concat([e.reset_index(drop=True),pd.DataFrame(rows)],axis=1)


def compute_event_features(events,panel):
    e=events.copy()
    if not REQUIRED_EVENTS.issubset(e):raise ValueError('ORIGINAL_RSI_EVENTS_SCHEMA_MISMATCH')
    e.code=e.code.astype(str).str.zfill(6)
    e.date=e.date.astype(str)
    e.gc_date=e.gc_date.fillna('').astype(str)
    # Prefix event rows no future values. D0 wave geometry pre-D0, today's close confirmation.
    per={c:g.reset_index(drop=True) for c,g in panel.groupby('code',sort=False)}
    event_keys=e[['code','date']].drop_duplicates()
    waves={}
    for r in event_keys.itertuples(index=False):
        g=per.get(r.code)
        if g is None:continue
        matches=np.flatnonzero(g.date.eq(r.date).to_numpy())
        if not len(matches):continue
        waves[(r.code,r.date)]=wave_pre_d0(g,int(matches[0]))
    wave_rows=[waves.get((r.code,r.date),dict(wave_state='DATA_MISSING')) for r in e[['code','date']].itertuples(index=False)]
    e=pd.concat([e,pd.DataFrame(wave_rows,index=e.index)],axis=1)
    e=enrich_stoch_timing(e,per)
    d0_fields=['market','amount','amount_ratio','volume_ratio','cohort_amount_rank_pct','cohort_amount_rank_500','cohort_leader_top10','cohort_leader_top20','ma224','close']
    has=[c for c in d0_fields if c in panel]
    d0=panel[['code','date']+has].rename(columns={c:'d0_'+c for c in has})
    e=e.merge(d0,on=['code','date'],how='left',validate='many_to_one')
    cols=['market','market_regime','market_ret5_pct','market_ret20_pct','market_amount_ratio',
          'breadth_up20','amount_ratio','volume_ratio','cohort_amount_rank_pct',
          'sector','sector_coverage_n','sector_relative20_pp','sector_strength','sector_leader',
          'theme','theme_status','theme_relative20_pp','theme_selected','stoch_k','stoch_d','stoch_gc','stoch_k_slope3','ret5_pct','ret20_pct']
    cols=[x for x in cols if x in panel]
    gc=panel[['code','date']+cols].rename(columns={'date':'gc_date',**{c:'gc_'+c for c in cols}})
    e=e.merge(gc,on=['code','gc_date'],how='left',validate='many_to_one')
    # RSI GC's future offset exists in outcome ledger, but the features themselves
    # are never exposed at D0 as an ex-ante condition.
    e['stoch_state_gc']=np.select([
        e.gc_stoch_gc.eq(True),e.gc_stoch_k.gt(e.gc_stoch_d)&e.gc_stoch_k_slope3.gt(0),
        e.gc_stoch_k.gt(e.gc_stoch_d),e.gc_stoch_k.notna()],
        ['GC_TODAY','ABOVE_AND_RISING','ABOVE_NOT_RISING','BELOW_OR_FALLING'],default='INSUFFICIENT')
    e['stoch_zone_gc']=pd.cut(e.gc_stoch_k,bins=[-np.inf,20,50,80,np.inf],
                             labels=['OVERSOLD_LT20','LOW_20_50','MID_50_80','OVERBOUGHT_GE80']).astype(str).replace('nan','INSUFFICIENT')
    e['rsi_delay_group']=pd.cut(e.gc_offset,bins=[-1,0,3,10],labels=['D0','D1_D3','D4_D10']).astype(str).replace('nan','NO_GC')
    e['market_regime']=e.gc_market_regime.fillna('PIT_MISSING')
    e['leader_group']=np.select([e.d0_cohort_leader_top10.eq(True),e.d0_cohort_leader_top20.eq(True)],
                                 ['COHORT_TOP10_TURNOVER','COHORT_TOP20_TURNOVER'],default='COHORT_OTHER')
    e['wave_group']=e.wave_state.fillna('INSUFFICIENT_HISTORY')
    e['sector_group']=e.gc_sector_strength.fillna('PIT_MISSING')
    e['theme_group']=e.gc_theme_status.fillna('PIT_MISSING')
    e['sector_leader_group']=e.gc_sector_leader.fillna('PIT_MISSING')
    e['stoch_group']=e.stoch_state_gc
    e['high10_gross']=e.GC_NEXT_OPEN_mfe_pct.add(.25).ge(10)
    e['joint_confirm_group']=e.joint_status
    e['realized_tp']=e.GC_NEXT_OPEN_exit_reason.eq('TP')
    e['realized_sl']=e.GC_NEXT_OPEN_exit_reason.eq('SL')
    e['original_event_key']=e.code+'|'+e.date+'|'+e.band+'|'+e.label
    e['future_outcomes_not_features']='GC_NEXT_OPEN_* are outcomes only; not usable for D0 entry'
    return e


def summarize(df,group):
    out=[]
    for key,x in df.groupby(group,dropna=False,observed=True,sort=True):
        if not isinstance(key,tuple):key=(key,)
        n=len(x)
        if n==0:continue
        out.append(dict(zip(group,key))|{
            'n':n,'stocks':int(x.code.nunique()),'unique_stock_dates':int(x[['code','date']].drop_duplicates().shape[0]),
            'year2023':int(x.date.str[:4].eq('2023').sum()),'year2024':int(x.date.str[:4].eq('2024').sum()),
            'peak_gross_mean_pct':round(float(x.GC_NEXT_OPEN_mfe_pct.mean()+.25),4),
            'peak_gross_ge10_pct':round(float(x.high10_gross.mean()*100),3),
            'tp_pct':round(float(x.realized_tp.mean()*100),3),
            'sl_pct':round(float(x.realized_sl.mean()*100),3),
            'net_mean_pct':round(float(x.GC_NEXT_OPEN_net_pct.mean()),4),
            'net_median_pct':round(float(x.GC_NEXT_OPEN_net_pct.median()),4),
            'screen_n30_stocks15_year10': bool(n>=30 and x.code.nunique()>=15 and
                x.date.str[:4].eq('2023').sum()>=10 and x.date.str[:4].eq('2024').sum()>=10)
            })
    return pd.DataFrame(out)


def audit_and_output(events,panel,market,sector_mapping,theme_mapping,out):
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    e=compute_event_features(events,panel)
    primary=e[(e.rsi_mode=='WILDER_SMA9')&(e.label=='CLEAR_BODY')&
              (e.gc_status=='GC_OBSERVED')&(e.gc_offset.between(0,10))&
              (e.GC_NEXT_OPEN_entry_price.notna())].copy()
    primary['year']=primary.date.str[:4]
    # Keep individual upper and lower as separate reference populations.
    comparisons={
        '01_market_regime':['band','market_regime'],
        '02_market_breadth':['band','market_regime','leader_group'],
        '03_sector_strength':['band','sector_group','sector_leader_group'],
        '04_leader_turnover':['band','leader_group'],
        '05_first_wave_pullback':['band','wave_group','wave_ma224_context'],
        '06_rsi_delay_stochastic':['band','rsi_delay_group','stoch_group'],
        '07_slow_stochastic_zone':['band','stoch_zone_gc','stoch_group'],
        '08_market_wave_leader':['band','market_regime','wave_group','leader_group'],
        '09_six_factor_full':['band','market_regime','sector_group','leader_group','wave_group','rsi_delay_group','stoch_group'],
        '10_year':['band','year'],
        '13_slow_vs_rsi_order':['band','rsi_delay_group','slow_vs_rsi','joint_status'],
        '16_sector_top_leader':['band','sector_group','sector_leader_group'],
        '17_theme_status':['band','theme_group'],
    }
    results={}
    for name,cols in comparisons.items():
        x=summarize(primary,cols)
        x.to_csv(out/(name+'.csv'),index=False,encoding='utf-8-sig');results[name]=len(x)
    all_modes=e[(e.label=='CLEAR_BODY')&(e.gc_status=='GC_OBSERVED')&
               (e.gc_offset.between(0,10))&e.GC_NEXT_OPEN_entry_price.notna()].copy()
    if not all_modes.empty:
        summarize(all_modes,['rsi_mode','band','market_regime','stoch_group']).to_csv(
            out/'15_all_rsi_modes_sensitivity.csv',index=False,encoding='utf-8-sig')
    # Historical events include duplicate band-lanes: retain both and provide unique dedup sensitivity.
    unique=primary.sort_values('band',ascending=False).drop_duplicates(['code','date'])
    summarize(unique,['market_regime','leader_group','wave_group','stoch_group']).to_csv(
        out/'11_unique_stock_date_sensitivity.csv',index=False,encoding='utf-8-sig')
    e.to_csv(out/'12_all_events_with_PIT_features.csv.gz',index=False,compression='gzip',encoding='utf-8')
    # Valid paired RSI vs joint entry (both before RSI confirmation). The
    # delta is an observed conditional outcome, never an ex-ante cohort effect.
    paired=primary[primary.joint_status.eq('JOINT_CONFIRMED') & primary.JOINT_net_pct.notna()].copy()
    if not paired.empty:
        paired['matched_net_change_pp']=paired.JOINT_net_pct-paired.GC_NEXT_OPEN_net_pct
        pair_summary=paired.groupby(['band','slow_vs_rsi'],observed=True).agg(
            n=('code','size'),stocks=('code','nunique'),rsi_only_net=('GC_NEXT_OPEN_net_pct','mean'),
            joint_net=('JOINT_net_pct','mean'),matched_delta_pp=('matched_net_change_pp','mean')).reset_index()
        pair_summary.to_csv(out/'14_joint_confirmation_paired.csv',index=False,encoding='utf-8-sig')
    else:
        pd.DataFrame(columns=['band','slow_vs_rsi','n','stocks','rsi_only_net','joint_net','matched_delta_pp']).to_csv(
            out/'14_joint_confirmation_paired.csv',index=False)
    def rate(field):return round(float(primary[field].ne('PIT_MISSING').mean()*100),2) if len(primary) else 0
    sector_rate=rate('gc_sector');theme_rate=rate('gc_theme')
    status={'version':VERSION,'research_only':True,'production_ready':False,'oos':False,
            'date_note':'2023-2024 historical fixed 2022-asof 500 cohort; no independent OOS',
            'source_events':len(events),'primary_observed_lanes':len(primary),
            'primary_unique_stock_dates':len(unique),
            'formula_lane':'WILDER_SMA9 used as sensitivity lane only; original RSI formula uncertified',
            'sector_pit_coverage_pct':sector_rate,'theme_pit_coverage_pct':theme_rate,
            'sector_source':'USER_SUPPLIED_HISTORICAL_PIT_UNCERTIFIED' if sector_mapping is not None else 'PIT_MISSING',
            'theme_source':'USER_SUPPLIED_HISTORICAL_PIT_UNCERTIFIED' if theme_mapping is not None else 'PIT_MISSING',
            'full_sixway_descriptive_table_available':bool(sector_rate>=80 and theme_rate>=80),
            'full_sixway_conclusion_allowed':False,
            'leader_scope':'2022-asof 500-cohort market and optional PIT-sector turnover ranks, NOT all-market/theme leader',
            'wave_definition':'pre-D0 peak 35 bars and trough, +15%, -5 to -30%, +3% rebound, volume <=0.8 prior 5 days',
            'stochastic':'predeclared Slow preset: SMA(K fast, k_smooth), SMA(slowK, d_smooth); late Slow triggers require still-bullish RSI confirmation',
            'market_definition':'index close > MA20 > MA60=up; inverse down; otherwise mixed',
            'min_screen':'30 events, 15 distinct stocks, at least 10 events each year; exploratory only',
            'source_limitations':['Even supplied PIT sector/theme records require independent historical-source authenticity review',
                                  'Sector strength and theme strength are medians within fixed TOP500 subset, not full official industry/theme indices',
                                  '2022-fixed TOP500, not daily all-market leader','D0 signal measured after D0 close; RSI GC after GC close',
                                  'No independent verification of corporate actions or actual fill',
                                  'Event-lane overlap and repeated stocks; statistical validation required',
                                  'No ex-ante D0 signal can rely on future GC dates or Stochastic confirmations']}
    (out/'AUDIT.json').write_text(json.dumps(status,ensure_ascii=False,indent=2),encoding='utf-8')
    intro=f'''# Stock-Hunter 6요인 연구 (RESEARCH_ONLY)

- 원자료 사건 행 수: {len(events):,} (RSI 계산식 4개가 같은 돌파를 중복 기록)
- 주 연구군: 명확한 몸통 돌파 + WILDER_SMA9 + D0~10 + GC 다음 시가 진입 = {len(primary):,} 사건
- 종목·날짜 중복 제거 민감도: {len(unique):,}건
- 역사적 섹터 소속 확인률: {sector_rate:.2f}% / 테마 소속 확인률: {theme_rate:.2f}%
- 여섯 조건 완전 결론: **실전 결론 금지 — 업종/테마 역사적 출처검증 및 OOS 필요**
- 시황은 과거 시장지수로 재구성. 대장주 표기는 고정 500종목 내 거래대금 상대 순위로만 제한.
- RSI 원본 산식 미인증. Slow Stochastic 프리셋은 AUDIT.json에 기록하며 종가 확정 값 사용. RSI와 Slow가 모두 강세로 확인된 날의 다음 시가를 별도로 평가.
- 사후 D+20 고점은 실제 매도 가능 수익이 아니며, 목표·손절 실현 가정과 구별.
- 6요인 매트릭스는 표본을 너무 세분화하므로 `screen_n30_stocks15_year10=True` 행도 투자 규칙 승인 근거가 아님.

## 파일

1. `01_market_regime.csv` 실제 지수 추세별 비교
2. `03_sector_strength.csv` 역사적 섹터 PIT이 없으면 PIT_MISSING
3. `04_leader_turnover.csv` 500종목 내 거래대금 분위별 비교
4. `05_first_wave_pullback.csv` 사전 고정 1차 파동→눌림 정의
5. `06_rsi_delay_stochastic.csv` RSI 시차와 Slow 동시/추격 여부
6. `08_market_wave_leader.csv` 시황×눌림×거래대금 동시
7. `09_six_factor_full.csv` 전요인 교차, 자료 부족시 완전분석으로 간주 금지
8. `12_all_events_with_PIT_features.csv.gz` 재현 가능한 전 사건 원장
9. `13_slow_vs_rsi_order.csv` RSI 대비 스토캐스틱 선·동시·후행 순서
10. `14_joint_confirmation_paired.csv` RSI 단독 대비 동시 확인 진입의 쌍별 민감도
11. `15_all_rsi_modes_sensitivity.csv` 4종 RSI 계산식에 대한 Slow 결합 민감도
12. `AUDIT.json` 결과 사용 허용 범위

**중요:** 섹터/테마의 입력은 `code,sector/theme,known_at,valid_from,valid_to`를 포함하는 역사적 검증 자료여야 한다. `known_at`이 신호일 이후인 정보는 과거 사건으로 소급하지 않는다. 2026년 현재의 업종·테마 목록을 2023년으로 역적용하지 않는다.
'''
    (out/'REPORT_KO.md').write_text(intro,encoding='utf-8')
    return status


def run(events,prices,markets,out,sector_pit=None,theme_pit=None,stoch=(14,3,3)):
    ev=pd.read_csv(events,dtype={'code':str,'date':str,'gc_date':str},low_memory=False)
    if ev.empty:raise ValueError('EVENT_LEDGER_EMPTY')
    market=pd.read_csv(markets,dtype={'date':str,'market':str})
    sec=load_pit_membership(sector_pit,'sector')
    th=load_pit_membership(theme_pit,'theme')
    panel,md=prepare_panel(prices,market,stoch=stoch)
    # R118 validated per-date market membership is an authoritative available data source;
    # if a date/market for a stock cannot be verified, abort rather than guess.
    if 'market' not in panel.columns:
        if 'market' not in prices:
            raise ValueError('MARKET_MEMBERSHIP_PIT_REQUIRED_FROM_R118_SECTOR_PIT')
    panel=current_day_factors(panel,md,sec,th)
    return audit_and_output(ev,panel,md,sec,th,out)


def main(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument('--events',required=True);p.add_argument('--manifest',required=True)
    p.add_argument('--market-daily',required=True);p.add_argument('--market-membership',required=True)
    p.add_argument('--sector-pit',default='');p.add_argument('--theme-pit',default='')
    p.add_argument('--out',required=True)
    p.add_argument('--stoch',default='14,3,3')
    a=p.parse_args(argv)
    prices,hashes=load_verified_manifest(a.manifest)
    membership=pd.read_csv(a.market_membership,dtype={'code':str,'date':str}).fillna('')
    if not {'code','date','market'}.issubset(membership):raise ValueError('R118_MARKET_MEMBERSHIP_SCHEMA_INVALID')
    if membership.duplicated(['code','date']).any():raise ValueError('DUPLICATE_PIT_MARKET_MEMBERSHIP')
    prices=prices.merge(membership[['code','date','market']],on=['code','date'],how='left',validate='one_to_one')
    if prices.market.isna().any():raise ValueError('MARKET_MEMBERSHIP_PIT_MISSING')
    stoch=tuple(int(x) for x in a.stoch.split(','))
    if len(stoch)!=3 or min(stoch)<1 or max(stoch)>30:raise ValueError('INVALID_STOCH_PRESET')
    status=run(a.events,prices,a.market_daily,a.out,a.sector_pit,a.theme_pit,stoch=stoch)
    status['stoch_preset']=a.stoch
    status['verified_code_count']=len(hashes)
    (Path(a.out)/'AUDIT.json').write_text(json.dumps(status,ensure_ascii=False,indent=2),encoding='utf-8')
    print('BLUE20_MULTIFACTOR_COMPLETE',json.dumps({k:status[k] for k in ['primary_observed_lanes','sector_pit_coverage_pct','theme_pit_coverage_pct','full_sixway_conclusion_allowed']},ensure_ascii=False))
    return status

if __name__=='__main__':main()
