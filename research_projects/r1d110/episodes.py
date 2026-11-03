"""R1-D110 time-causal trend-change episodes; no prediction score.

Input: frozen R1-D109 as-of daily OHLCV and full technical confluence.
A 20-trading-bar episode opens on the first candidate event outside an existing
window. It NEVER joins or moves an anchor retroactively. Later observations
are emitted on their real observation date; close-time summaries are not
available at the episode's opening. Outcomes are NOT referenced by this module.
"""
from __future__ import annotations
import math
import pandas as pd
import numpy as np

EPISODE_BARS=20
GROUPS=('STRUCTURE','LINE','CANDLE','MA','BB','ENV','CLOUD','FLOW','ROLE')
REQUIRED=('code','name','idx','date','open','high','low','close',
    'candidate_trend_change_today','pivot_structure_PIT','pivot_structure_changed_today',
    'has_new_pivot_today','active_support_breaks','active_resistance_breaks',
    'active_slope_sign_flips','reanchors','candle_body_share_pct',
    'candle_upper_wick_share_pct','candle_lower_wick_share_pct',
    'candle_upper_wick_body_ratio','candle_lower_wick_body_ratio',
    'candle_range_vs_prev14_atr','close_vs_prev3_high_max_pct',
    'low_vs_prev3_low_min_pct','ma5_slope5_pct','ma20_slope20_pct',
    'ma20_gap_pct','bb20_width_delta5_pp','bb20_position',
    'env20_10_position','env40_10_position','ma_cloud_fast_position',
    'ichimoku_current_position','amount_vs_prev20','volume_vs_prev20')
# What to read before deciding an event is defined, never selected using D+ returns.
TECH=('pivot_structure_PIT','active_support_breaks','active_resistance_breaks',
 'active_slope_sign_flips','reanchors','candle_direction','candle_body_share_pct',
 'candle_upper_wick_share_pct','candle_lower_wick_share_pct',
 'candle_upper_wick_body_ratio','candle_lower_wick_body_ratio',
 'candle_range_vs_prev14_atr','candle_close_location_pct',
 'close_vs_tminus2_high_pct','close_vs_tminus3_high_pct',
 'close_vs_prev2_high_max_pct','close_vs_prev3_high_max_pct',
 'close_vs_prev2_bodytop_max_pct','close_vs_prev3_bodytop_max_pct',
 'low_vs_prev3_low_min_pct','ma5_slope5_pct','ma20_slope20_pct',
 'ma20_slope_accel5_pp','ma20_gap_pct','ma112_slope20_pct','ma224_slope20_pct',
 'bb20_width_pct','bb20_width_delta5_pp','bb20_upper_slope5_pct','bb20_lower_slope5_pct',
 'bb20_position','env20_10_position','env40_10_position',
 'env20_10_upper_slope5_pct','ma_cloud_fast_position',
 'ma_cloud_fast_thickness_pct','ma_cloud_fast_thickness_change5_pp',
 'ma_cloud_fast_height_vs_price_pct','ma_cloud_fast_height_change5_pp',
 'ma_cloud_fast_upper_slope5_pct','ma_cloud_fast_lower_slope5_pct',
 'ichimoku_current_position','ichimoku_current_thickness_pct',
 'ichimoku_current_thickness_change5_pp',
 'ichimoku_current_height_vs_price_pct','ichimoku_current_height_change5_pp',
 'ichimoku_current_upper_slope5_pct','ichimoku_current_lower_slope5_pct','amount_vs_prev20','volume_vs_prev20',
 'amount_avg5_vs_prev20','volume_avg5_vs_prev20')


def _v(r,k):
    x=r.get(k)
    if x is None or (isinstance(x,float) and not math.isfinite(x)):
        return None
    return x


def _n(r,k):
    x=_v(r,k)
    try:return float(x) if x is not None else None
    except (TypeError,ValueError):return None


def _positive(r,k,cut=0.):
    x=_n(r,k)
    return x is not None and x>cut


def _upturn(cur,prev,key):
    a=_n(cur,key);b=_n(prev,key) if prev else None
    return bool(a is not None and b is not None and a>0>=b)


def _downturn(cur,prev,key):
    a=_n(cur,key);b=_n(prev,key) if prev else None
    return bool(a is not None and b is not None and a<0<=b)


def _entered(cur,prev,k,value):
    return bool(_v(cur,k)==value and (prev is None or _v(prev,k)!=value))


def _threshold_onset(cur,prev,k,level):
    a=_n(cur,k);b=_n(prev,k) if prev else None
    return a is not None and a>=level and (b is None or b<level)


def day_components(cur, prev=None, role_kinds=()):
    """All components as of *this* close. No future bars and no EXPOST columns.

    The individual families are deliberately correlated. Never sum event counts
    into a predictive confidence score, and never imply within-day time order.
    """
    out=[]
    def emit(family,key,flag,description):
        if flag:out.append((family,key,description))
    s=str(_v(cur,'pivot_structure_PIT') or '')
    change=bool(_v(cur,'pivot_structure_changed_today'))
    emit('STRUCTURE','PIVOT_RISING_CONFIRM',change and s=='HIGHER_HIGH_HIGHER_LOW','상승 고점·상승 저점 구조 신규 확인')
    emit('STRUCTURE','PIVOT_FALLING_CONFIRM',change and s=='LOWER_HIGH_LOWER_LOW','하락 고점·하락 저점 구조 신규 확인')
    emit('STRUCTURE','PIVOT_MIXED_CONFIRM',change and s not in ('HIGHER_HIGH_HIGHER_LOW','LOWER_HIGH_LOWER_LOW'),'고점·저점 구조 혼조/수렴 변경')
    emit('STRUCTURE','NEW_CONFIRMED_PIVOT',bool(_v(cur,'has_new_pivot_today')),'2거래일 오른쪽 확인을 마친 피벗')
    emit('LINE','ACTIVE_RESISTANCE_BREAK',_positive(cur,'active_resistance_breaks'),'활성 저항선 종가 상향 돌파')
    emit('LINE','ACTIVE_SUPPORT_BREAK',_positive(cur,'active_support_breaks'),'활성 지지선 종가 하향 이탈')
    emit('LINE','SLOPE_SIGN_REANCHOR',_positive(cur,'active_slope_sign_flips'),'신규 추세선 갱신시 기울기 부호 변경(시장 반전 확정 아님)')
    emit('LINE','REANCHOR',_positive(cur,'reanchors'),'새 기준점으로 활성 추세선 갱신')
    bullish=_n(cur,'close') is not None and _n(cur,'open') is not None and _n(cur,'close')>_n(cur,'open')
    bearish=_n(cur,'close') is not None and _n(cur,'open') is not None and _n(cur,'close')<_n(cur,'open')
    big=_n(cur,'candle_body_share_pct') is not None and _n(cur,'candle_body_share_pct')>=60
    atr=_n(cur,'candle_range_vs_prev14_atr')
    emit('CANDLE','BULL_BODY_60',bullish and big,'몸통비중 60%+ 양봉')
    emit('CANDLE','BEAR_BODY_60',bearish and big,'몸통비중 60%+ 음봉')
    emit('CANDLE','BULL_ATR_EXPANSION',bullish and big and atr is not None and atr>=1.2,'큰 양봉+직전 ATR 대비 긴 캔들')
    emit('CANDLE','UPPER_WICK_DOMINANT',(_n(cur,'candle_upper_wick_share_pct') or 0)>=45 and (_n(cur,'candle_upper_wick_body_ratio') or 0)>=1.2,'몸통보다 긴 윗꼬리(폭의 45%+)')
    emit('CANDLE','LOWER_WICK_DOMINANT',(_n(cur,'candle_lower_wick_share_pct') or 0)>=45 and (_n(cur,'candle_lower_wick_body_ratio') or 0)>=1.2,'몸통보다 긴 아랫꼬리(폭의 45%+)')
    emit('CANDLE','CLOSE_ABOVE_PREV2_HIGH',_positive(cur,'close_vs_prev2_high_max_pct',.3),'종가가 직전2일 최고가 +0.3% 초과')
    emit('CANDLE','CLOSE_ABOVE_PREV3_HIGH',_positive(cur,'close_vs_prev3_high_max_pct',.3),'종가가 직전3일 최고가 +0.3% 초과')
    emit('CANDLE','BODYTOP_ABOVE_PREV3_BODYTOP',_positive(cur,'close_vs_prev3_bodytop_max_pct',.3),'종가가 직전3일 몸통 상단 +0.3% 초과')
    emit('CANDLE','INTRADAY_BELOW_PREV3_LOW',(_n(cur,'low_vs_prev3_low_min_pct') is not None and _n(cur,'low_vs_prev3_low_min_pct')<-.3),'당일 저가가 직전3일 최저가 -0.3% 미만')
    for name,key in (('MA5_SLOPE5','ma5_slope5_pct'),('MA20_SLOPE20','ma20_slope20_pct'),('MA112_SLOPE20','ma112_slope20_pct'),('MA224_SLOPE20','ma224_slope20_pct')):
        emit('MA',name+'_UPTURN',_upturn(cur,prev,key),name+' 기울기 0 상향 전환')
        emit('MA',name+'_DOWNTURN',_downturn(cur,prev,key),name+' 기울기 0 하향 전환')
    emit('MA','PRICE_CROSS_MA20_UP',_upturn(cur,prev,'ma20_gap_pct'),'가격이 MA20 위로 이동')
    emit('MA','PRICE_CROSS_MA20_DOWN',_downturn(cur,prev,'ma20_gap_pct'),'가격이 MA20 아래로 이동')
    emit('BB','BB20_EXPANSION_BEGIN',_upturn(cur,prev,'bb20_width_delta5_pp'),'BB20 폭 변화 5일지표 확장 전환')
    emit('BB','BB20_CONTRACTION_BEGIN',_downturn(cur,prev,'bb20_width_delta5_pp'),'BB20 폭 변화 5일지표 수축 전환')
    emit('BB','BB20_UPPER_SLOPE_UPTURN',_upturn(cur,prev,'bb20_upper_slope5_pct'),'BB20 상단 기울기 상승 전환')
    emit('BB','BB20_LOWER_SLOPE_UPTURN',_upturn(cur,prev,'bb20_lower_slope5_pct'),'BB20 하단 기울기 상승 전환')
    emit('BB','BB20_ABOVE_UPPER',_entered(cur,prev,'bb20_position','ABOVE_UPPER'),'BB20 상단 밖으로 종가 이동')
    emit('BB','BB20_BELOW_LOWER',_entered(cur,prev,'bb20_position','BELOW_LOWER'),'BB20 하단 밖으로 종가 이동')
    for n in (20,40):
        k=f'env{n}_10_position'
        emit('ENV',f'ENV{n}_ABOVE_UPPER',_entered(cur,prev,k,'ABOVE_UPPER'),f'ENV{n} 상단 밖으로 진입')
        emit('ENV',f'ENV{n}_BELOW_LOWER',_entered(cur,prev,k,'BELOW_LOWER'),f'ENV{n} 하단 밖으로 진입')
        emit('ENV',f'ENV{n}_RETURN_INSIDE',_entered(cur,prev,k,'INSIDE'),f'ENV{n} 밴드 내부로 복귀')
    emit('ENV','ENV20_UPPER_SLOPE_UPTURN',_upturn(cur,prev,'env20_10_upper_slope5_pct'),'ENV20 상단 기울기 상승 전환')
    for k,name in (('ma_cloud_fast_position','FAST_MA_CLOUD'),('ichimoku_current_position','ICHIMOKU')):
        for v,sfx in (('ABOVE','ABOVE'),('BELOW','BELOW'),('INSIDE','INSIDE')):
            emit('CLOUD',name+'_'+sfx,_entered(cur,prev,k,v),name+' 가격 위치 '+sfx)
    for k,name in (('ma_cloud_fast_thickness_change5_pp','FAST_MA_CLOUD'),('ichimoku_current_thickness_change5_pp','ICHIMOKU')):
        emit('CLOUD',name+'_THICKEN_BEGIN',_upturn(cur,prev,k),name+' 5일 구름두께 확대 전환')
        emit('CLOUD',name+'_THIN_BEGIN',_downturn(cur,prev,k),name+' 5일 구름두께 축소 전환')
    emit('CLOUD','FAST_CLOUD_UPPER_SLOPE_UPTURN',_upturn(cur,prev,'ma_cloud_fast_upper_slope5_pct'),'빠른 이평구름 상단 기울기 상승 전환')
    emit('CLOUD','ICHIMOKU_UPPER_SLOPE_UPTURN',_upturn(cur,prev,'ichimoku_current_upper_slope5_pct'),'일목 현재구름 상단 기울기 상승 전환')
    emit('FLOW','AMOUNT_ABOVE_1X',_threshold_onset(cur,prev,'amount_vs_prev20',1.),'당일 거래대금 직전20일 평균 상회 시작')
    emit('FLOW','AMOUNT_ABOVE_1P5X',_threshold_onset(cur,prev,'amount_vs_prev20',1.5),'당일 거래대금 직전20일 평균 1.5배 돌파')
    emit('FLOW','VOLUME_ABOVE_1P5X',_threshold_onset(cur,prev,'volume_vs_prev20',1.5),'당일 거래량 직전20일 평균 1.5배 돌파')
    for role in sorted(set(role_kinds)):
        if role in ('POTENTIAL_SUPPORT_RETEST','POTENTIAL_RESISTANCE_RETEST','SUPPORT_RETEST_FAILED','RESISTANCE_RETEST_FAILED','FORMER_RESISTANCE_RELOST','FORMER_SUPPORT_RECLAIMED'):
            emit('ROLE',role,True,'이전선 재접촉/회복 확인일의 관찰; 최초 돌파일의 정보 아님')
    return out


def validate_source(d):
    missing=set(REQUIRED)-set(d.columns)
    if missing:raise ValueError('missing R109 PIT columns: '+str(sorted(missing)))
    if any('EXPOST' in c.upper() for c in d.columns):raise ValueError('future labels in PIT input')
    if d[['code','idx']].duplicated().any():raise ValueError('duplicated stock index')
    if d[['code','date']].duplicated().any():raise ValueError('duplicated stock-date')
    if (d.close.astype(float)<=0).any():raise ValueError('nonpositive close')
    for code,g in d.groupby('code'):
        s=g.sort_values('idx').idx.to_numpy(int)
        if not np.array_equal(s,np.arange(s[0],s[0]+len(s))):raise ValueError('non-contiguous index '+code)


def _simultaneous_groups(first):
    """Calendar-day ties are a SET, never an invented intraday order."""
    bylag={}
    for key,(day,lag,_) in first.items():
        bylag.setdefault((lag,day),[]).append(key)
    return ' / '.join(f'D+{lag}:{date}['+'|'.join(sorted(keys))+']'
                    for (lag,date),keys in sorted(bylag.items()))


def _snap(r):
    return {k:_v(r,k) for k in TECH if k in r}


def compute(d:pd.DataFrame,roles:pd.DataFrame|None=None, *,window:int=EPISODE_BARS):
    """Return (opens, timeline, onsets, closes). Pure causal PIT processing.

    The CLOSE table holds completed history only and must NEVER be interpreted
    as a snapshot at episode_open_idx. A right-censored final episode is
    labelled as such, never magically marked 'success'.
    """
    if not 3<=window<=80:raise ValueError('episode window outside supported scope')
    validate_source(d)
    role_lookup={}
    if roles is not None and len(roles):
        for (c,t),g in roles.groupby(['code','idx']):
            role_lookup[(str(c).zfill(6),int(t))]=tuple(g.role_observation_PIT.dropna().astype(str))
    opens=[];timeline=[];onsets=[];closes=[]
    for code,g in d.groupby('code',sort=True):
        g=g.sort_values('idx').reset_index(drop=True)
        prev=None;ep=None;number=0
        for j,r in enumerate(g.to_dict('records')):
            t=int(r['idx']);today=str(r['date']);start=bool(r['candidate_trend_change_today'])
            if ep is None and start:
                number+=1
                ep={'id':f'{code}|{today}|{number:03d}','code':code,'name':r['name'],
                    'anchor_idx':t,'anchor_date':today,'anchor_close':float(r['close']),
                    'first':{},'events':0,'active_days':0,'family_order':[],
                    'structure_start':str(r['pivot_structure_PIT'])}
                opening={'episode_id':ep['id'],'code':code,'name':r['name'],
                    'anchor_idx':t,'anchor_date':today,'anchor_close':float(r['close']),
                    'start_trigger':'R109_PIT_CANDIDATE_TREND_CHANGE',
                    'pivot_structure_at_open_PIT':str(r['pivot_structure_PIT']),
                    'initial_active_support_breaks':_n(r,'active_support_breaks') or 0,
                    'initial_active_resistance_breaks':_n(r,'active_resistance_breaks') or 0,
                    'episode_max_window_bars':window,'known_at':'ANCHOR_DATE_CLOSE',
                    'no_future_data_used':True,**_snap(r)}
                opens.append(opening)
            if ep is not None:
                lag=t-ep['anchor_idx']
                if not (0<=lag<window):raise AssertionError('overlapping / unexpected episode span')
                comp=day_components(r,prev,role_lookup.get((code,t),()))
                first_today=[]
                for fam,key,description in comp:
                    if key not in ep['first']:
                        ep['first'][key]=(today,lag,fam)
                        first_today.append(key)
                        if fam not in ep['family_order']:ep['family_order'].append(fam)
                    ep['events']+=1
                    onsets.append({'episode_id':ep['id'],'code':code,'name':r['name'],
                     'anchor_date':ep['anchor_date'],'observation_date':today,
                     'observation_idx':t,'lag_bars_from_anchor':lag,'family':fam,
                     'component':key,'description':description,
                     'is_first_this_component_in_episode':key in first_today,
                     'known_at':'OBSERVATION_DATE_CLOSE',
                     'prior_data_only':True})
                ep['active_days']+=1
                state='UP_PIVOT_REGIME' if r['pivot_structure_PIT']=='HIGHER_HIGH_HIGHER_LOW' else (
                    'DOWN_PIVOT_REGIME' if r['pivot_structure_PIT']=='LOWER_HIGH_LOWER_LOW' else 'MIXED_OR_PENDING_PIVOTS')
                tick={'episode_id':ep['id'],'code':code,'name':r['name'],
                    'episode_anchor_idx':ep['anchor_idx'],'episode_anchor_date':ep['anchor_date'],
                    'idx':t,'date':today,'bar_from_anchor':lag,'close':float(r['close']),
                    'pivot_regime_now_PIT':state,
                    'ma20_direction_now_PIT':'UP' if _positive(r,'ma20_slope20_pct') else
                            'DOWN' if _n(r,'ma20_slope20_pct') is not None and _n(r,'ma20_slope20_pct')<0 else 'FLAT_OR_NA',
                    'component_events_today':'|'.join(key for _,key,_ in comp),
                    'new_components_today':'|'.join(first_today),
                    'component_events_seen_so_far':len(ep['first']),
                    'families_seen_so_far_UNORDERED':'|'.join(sorted(ep['family_order'])),
                    'known_at':'THIS_DATE_CLOSE','open_ended_historical_record':True,**_snap(r)}
                timeline.append(tick)
                if lag==window-1 or j==len(g)-1:
                    complete=lag==window-1
                    byfam={fam:[] for fam in GROUPS}
                    for key,(date,offset,fam) in ep['first'].items():byfam[fam].append((offset,date,key))
                    record={'episode_id':ep['id'],'code':code,'name':ep['name'],
                        'anchor_idx':ep['anchor_idx'],'anchor_date':ep['anchor_date'],
                        'end_idx':t,'end_date':today,'bars_observed':ep['active_days'],
                        'complete_window':complete,'right_censored':not complete,
                        'episode_status':'WINDOW_COMPLETE' if complete else 'RIGHT_CENSORED_AT_SAMPLE_END',
                        'observed_component_families_UNORDERED':'|'.join(sorted(ep['family_order'])),
                        'unique_components_observed':len(ep['first']),
                        'total_component_observations':ep['events'],
                        'first_observed_component_sequence':_simultaneous_groups(ep['first']),
                        'known_at':'EPISODE_END_DATE_CLOSE_NOT_AT_ANCHOR',
                        'never_use_as_anchor_day_signal':True}
                    for fam in GROUPS:
                        hits=sorted(byfam[fam]);record[f'first_{fam.lower()}_date']=hits[0][1] if hits else None
                        record[f'first_{fam.lower()}_lag']=hits[0][0] if hits else None
                        record[f'first_{fam.lower()}_component']=hits[0][2] if hits else None
                    closes.append(record);ep=None
            prev=r
    return pd.DataFrame(opens),pd.DataFrame(timeline),pd.DataFrame(onsets),pd.DataFrame(closes)


def expost_outcomes(days,opens,horizons=(1,3,5,10,20,40)):
    """Separate EXPOST output. Never import this result back into PIT scoring."""
    result=[]
    for code,g in days.groupby('code',sort=True):
        g=g.sort_values('idx').reset_index(drop=True)
        lookup={int(x['idx']):i for i,x in enumerate(g.to_dict('records'))}
        for o in opens[opens.code.eq(code)].to_dict('records'):
            pos=lookup[int(o['anchor_idx'])];start=float(g.iloc[pos].close)
            rec={'episode_id':o['episode_id'],'code':code,'anchor_date':o['anchor_date'],
                 'anchor_idx':o['anchor_idx'],'anchor_close':start,
                 'EXPOST_NOT_KNOWN_ON_ANCHOR':True}
            for h in horizons:
                ok=pos+h<len(g)
                rec[f'Dp{h}_full_window_EXPOST']=ok
                rec[f'Dp{h}_close_pct_EXPOST']=round(100*(float(g.iloc[pos+h].close)/start-1),4) if ok else None
                if ok:
                    win=g.iloc[pos+1:pos+h+1]
                    rec[f'Dp{h}_max_high_pct_EXPOST']=round(100*(float(win.high.max())/start-1),4)
                    rec[f'Dp{h}_min_low_pct_EXPOST']=round(100*(float(win.low.min())/start-1),4)
                else:
                    rec[f'Dp{h}_max_high_pct_EXPOST']=None
                    rec[f'Dp{h}_min_low_pct_EXPOST']=None
            result.append(rec)
    return pd.DataFrame(result)


def pre_anchor_context(days:pd.DataFrame,opens:pd.DataFrame,lookback:int=5):
    """Historical six-bar lead-in, all observations already known at anchor close."""
    if lookback<1:raise ValueError('lookback')
    rows=[]
    for code,g in days.groupby('code',sort=True):
        g=g.sort_values('idx').reset_index(drop=True)
        lookup={int(q.idx):i for i,q in enumerate(g.itertuples(index=False))}
        for o in opens[opens.code.eq(code)].to_dict('records'):
            t=int(o['anchor_idx'])
            for age in range(-lookback,1):
                j=lookup.get(t+age)
                if j is None:continue
                r=g.iloc[j].to_dict();prior=g.iloc[j-1].to_dict() if j>0 else None
                components=day_components(r,prior)
                rows.append({'episode_id':o['episode_id'],'code':code,'name':o['name'],
                 'episode_anchor_date':o['anchor_date'],'observation_date':r['date'],
                 'observation_idx':int(r['idx']),'relative_bar_from_anchor':age,
                 'close':float(r['close']),
                 'component_events_on_observation_date':'|'.join(key for _,key,_ in components),
                 'known_at':'ANCHOR_DATE_CLOSE_OR_EARLIER','prior_only':True,**_snap(r)})
    return pd.DataFrame(rows)
