#!/usr/bin/env python3
"""R1D27R locked flow-asof D0 entry/stage research on BB-independent pilot 300.
Past flow D-60..D0 used only at D0, subsequent technical entry conditions asof day d.
D+1 open for D0, day d+1 open for later stage; H=5/10/20 after entry.
No live decisions. Censor missing prices, never impute. Not PIT-certified or live OOS.
"""
import argparse,csv,gzip,json,math,collections,statistics,pathlib,itertools
from research_formulas.stockhunter_r1d27r_flow_collect import load,read_seed,RAW,ACTORS,check,sha

MA=(5,10,20,40,60,112,224,448)
TGT=(1,2,3,5,7,10,15,20);STOPS=(-5,-8,-10);HORIZONS=(5,10,20)
STAGES=('D0_IMMEDIATE','CONTRACTION_SHORT','PRICE_STABILIZATION','MA5_RECOVERY','BB20_LOWER_RECOVERY','BB20_SQUEEZE_RELEASE_UP','CANDLE_VOLUME_RECOVERY')
ACTORS_MAP={'기관합계':'inst','외국인합계':'foreign','개인':'personal','기타법인':'other'}
def num(x):
 try:v=float(x);return v if math.isfinite(v) else None
 except (ValueError,TypeError):return None
def flag(x):return x is not None and x>0
def load_context(base,cohort):
 assert cohort=='PILOT_300'
 path=pathlib.Path(base)/'research_inputs/R1D27R_PILOT_DMINUS60_DPLUS40_PRICE_CONTEXT.csv.gz'
 m=collections.defaultdict(dict)
 with gzip.open(path,'rt',encoding='utf-8',newline='') as f:
  for r in csv.DictReader(f):
   k=(r['date'],r['code']);d=int(r['D']);check(d not in m[k],'CONTEXT_DUP');m[k][d]=r
 check(len(m)==300 and all(len(x)==101 for x in m.values()),'PRICE_CONTEXT_SIZES')
 return m

def stage_hit(name,r,prev,three):
 if not r or r['valid_price']!='1':return False
 if name=='D0_IMMEDIATE':return int(r['D'])==0
 o=num(r['open']);c=num(r['close']);h=num(r['high']);lo=num(r['low']);vr=num(r['volume_ratio_prev5']);ma5=num(r['MA5']);ma20=num(r['MA20'])
 if name=='CONTRACTION_SHORT':return vr is not None and vr<=.8 and o and h and lo is not None and c is not None and abs(c/o-1)<=.015 and (h-lo)/o<=.03
 if name=='PRICE_STABILIZATION':
  if any(q is None or q['valid_price']!='1' for q in three) or len(three)!=6:return False
  return min(float(q['low']) for q in three[3:])>=min(float(q['low']) for q in three[:3]) and c>=float(three[-2]['close'])
 if name=='MA5_RECOVERY':return bool(prev and ma5 and num(prev['MA5']) and c>=ma5 and float(prev['close'])<float(prev['MA5']))
 if name=='BB20_LOWER_RECOVERY':return bool(prev and num(r['BB20_lower']) and num(prev['BB20_lower']) and c>=float(r['BB20_lower']) and float(prev['close'])<float(prev['BB20_lower']))
 if name=='BB20_SQUEEZE_RELEASE_UP':
  prior_width=num(three[0]['BB20_width_pct']) if len(three)==6 and three[0] is not None else None
  return bool(ma20 and c>=ma20 and prior_width is not None and prior_width<=10 and num(r['BB20_width_delta5_pp']) is not None and float(r['BB20_width_delta5_pp'])>=.5)
 if name=='CANDLE_VOLUME_RECOVERY':
  if len(three)<4 or any(q is None or q['valid_price']!='1' for q in three[-4:]):return False
  rank=num(r['close_rank_pct']);return bool(o and c/o>=1.005 and vr and vr>=1.2 and rank is not None and rank>=70 and c>max(float(q['close']) for q in three[-4:-1]))
 return False

def future_path(arr,signal_day,h):
 # Stage confirmed at `signal_day` close; entry next session OPEN then H close/high/low bars.
 cand=[arr.get(d) for d in range(signal_day+1,signal_day+h+1)]
 if len(cand)!=h or any(x is None or x['valid_price']!='1' for x in cand):return None
 vals=[(num(x['open']),num(x['high']),num(x['low']),num(x['close'])) for x in cand]
 if any(any(v is None or v<=0 for v in y) or not(y[1]>=max(y[0],y[3]) and y[2]<=min(y[0],y[3])) for y in vals):return None
 # Corporate action discontinuity is quarantined, not smoothed.
 prior=num(arr.get(signal_day,{}).get('close'))
 for o,hi,lo,c in vals:
  if prior and abs(c/prior-1)>.35:return None
  prior=c
 return vals

def exit_sim(vals,target,stop):
 entry=vals[0][0];tp=entry*(1+target/100);sl=entry*(1+stop/100)
 for j,(o,hi,lo,c) in enumerate(vals,1):
  # Stop gap is realized at worse OPEN; profit gap capped to target (no optimistic slippage).
  if o<=sl:return 'STOP_GAP',100*(o/entry-1)-.2,j
  if lo<=sl:return 'STOP',stop-.2,j
  if hi>=tp:return 'TARGET',target-.2,j
 return 'TIME',100*(vals[-1][3]/entry-1)-.2,len(vals)

def summarize(rows,keys,totals=None):
 g=collections.defaultdict(list)
 for x in rows:g[tuple(x[k] for k in keys)].append(x)
 out=[]
 for key,rs in sorted(g.items(),key=lambda x:str(x[0])):
  ret=[float(x['realized_return_pct']) for x in rs];ex=collections.Counter(x['exit_reason'] for x in rs)
  out.append(dict(zip(keys,key),n=len(rs),n_target=ex['TARGET'],target_first_pct=round(ex['TARGET']/len(rs)*100,3),
    n_stop=ex['STOP']+ex['STOP_GAP'],stop_first_pct=round((ex['STOP']+ex['STOP_GAP'])/len(rs)*100,3),
    win_rate_pct=round(sum(r>0 for r in ret)/len(ret)*100,3),mean_realized_return_pct=round(statistics.mean(ret),4),median_realized_return_pct=round(statistics.median(ret),4)))
 return out

def wr(p,rows,columns=None):
 with p.open('w',encoding='utf-8',newline='') as f:
  w=csv.DictWriter(f,fieldnames=columns or (list(rows[0]) if rows else []),extrasaction='ignore');w.writeheader();w.writerows(rows)

def replay(root,checkpoint,out):
 frozen,calendar,fs=load(root,'PILOT_300');allowed={(v['date'],v['code']) for v in frozen}
 raw,old=read_seed(checkpoint,'PILOT_300',fs,allowed)
 check(len(old)==300 and all(x['status']!='RETRY_PENDING' for x in old.values()),'INCOMPLETE_COHORT_NO_RESULTS')
 completed={k for k,r in old.items() if r['source_complete']=='1'}
 check(len(completed)>=240,'MIN_80PCT_FLOW_COVERAGE')
 context=load_context(root,'PILOT_300')
 flow=collections.defaultdict(lambda:collections.defaultdict(dict))
 for row in raw:flow[(row['date'],row['code'])][row['trade_date']][row['actor']]=row
 stage=[];trades=[];strong15=[];miss_stage=collections.Counter();base=[];price_quarantine=[]
 for fr in frozen:
  key=(fr['date'],fr['code']);price=context[key]
  if key not in completed:continue
  dct=flow[key];check(len(dct)==61,'FLOW_DAY61')
  trd={int(d):r['trade_date'] for d,r in price.items() if -60<=d<=0}
  check(all(trd[d] in dct and set(dct[trd[d]])==set(ACTORS) for d in range(-60,1)),'FLOW_CALENDAR_ACTORS')
  def actor_sum(actor,ds):return sum(int(dct[trd[d]][actor]['net_krw']) for d in ds)
  now={ACTORS_MAP[a]:actor_sum(a,range(-4,1)) for a in ACTORS}
  prev={ACTORS_MAP[a]:actor_sum(a,range(-5,0)) for a in ACTORS}
  total=sum(int(price[d]['amount']) for d in range(-4,1) if price[d]['valid_price']=='1')
  complete_amount=sum(price[d]['valid_price']=='1' for d in range(-4,1))==5
  if not complete_amount or total<=0:
   price_quarantine.append(dict(date=fr['date'],code=fr['code'],reason='D0_PRE5_PRICE_OR_TURNOVER_MISSING_NO_IMPUTATION'))
   continue
  onset_now=now['inst']+now['foreign']>0 and now['personal']<0
  onset_prev=prev['inst']+prev['foreign']>0 and prev['personal']<0
  label=('NEW_ABSORPTION_D0' if onset_now and not onset_prev else 'CONTINUING_ABSORPTION_D0' if onset_now else 'NO_ABSORPTION_D0')
  data=dict(date=fr['date'],code=fr['code'],year=fr['year'],quarter=fr['quarter'],liquidity_tier=fr['liquidity_tier'],
   flow_state_D0=label,flow_net5_inst_krw=now['inst'],flow_net5_foreign_krw=now['foreign'],flow_net5_personal_krw=now['personal'],
   flow_net5_other_krw=now['other'],flow_combined5_turnover_pct=round(100*(now['inst']+now['foreign'])/total,4),
   flow_foreign5_turnover_pct=round(100*now['foreign']/total,4),flow_inst5_turnover_pct=round(100*now['inst']/total,4),
   flow_personal5_turnover_pct=round(100*now['personal']/total,4),
   MA20_MA60_gap_pct='',MA60_MA224_gap_pct='')
  base.append(data)
  for st in STAGES:
   first_day=0 if st=='D0_IMMEDIATE' else None
   if first_day is None:
    for day in range(1,16):
     r=price.get(day);prev_=price.get(day-1)
     hist=[price.get(k) for k in range(day-5,day+1)]
     if stage_hit(st,r,prev_,hist):first_day=day;break
   if first_day is None:miss_stage[(st,label)]+=1;continue
   current=price[first_day]
   stage_data={**data,'stage':st,'stage_confirm_D':first_day,'entry_D':first_day+1,'stage_date':current['trade_date'],
    'stage_body_pct':current['body_pct'],'stage_volume_ratio_prev5':current['volume_ratio_prev5'],
    'stage_BB20_width_pct':current['BB20_width_pct'],'stage_BB20_width_delta5_pp':current['BB20_width_delta5_pp']}
   for x in MA:stage_data[f'MA{x}']=current[f'MA{x}']
   for a,b in itertools.combinations(MA,2):
    m1=num(current[f'MA{a}']);m2=num(current[f'MA{b}'])
    stage_data[f'MA{a}_{b}_gap_pct']=round(100*(m1/m2-1),4) if m1 is not None and m2 else ''
   stage.append(stage_data)
   vals20=future_path(price,first_day,20)
   if vals20:
    entry=vals20[0][0]
    reason15,realized15,day15=exit_sim(vals20,15,-8)
    maxhigh=max(x[1] for x in vals20)
    minlow=min(x[2] for x in vals20)
    lastclose=vals20[-1][3]
    strong15.append({**stage_data,
      'peak20_pct':round(100*(maxhigh/entry-1),4),
      'worst20_low_pct':round(100*(minlow/entry-1),4),
      'close20_net20bps_pct':round(100*(lastclose/entry-1)-.2,4),
      'strong15_touch':int(maxhigh>=entry*1.15),
      'strong15_before_stop8':int(reason15=='TARGET'),
      'strong15_close_durable':int(100*(lastclose/entry-1)-.2>=15),
      'fail_close_below_minus10':int(100*(lastclose/entry-1)-.2<=-10)})
   for h in HORIZONS:
    vals=future_path(price,first_day,h)
    if vals is None:continue
    for t in TGT:
     for stop in STOPS:
      reason,realized,days=exit_sim(vals,t,stop)
      trades.append(dict(date=fr['date'],code=fr['code'],year=fr['year'],liquidity_tier=fr['liquidity_tier'],flow_state_D0=label,stage=st,
       stage_confirm_D=first_day,horizon=h,target_pct=t,stop_pct=stop,exit_reason=reason,realized_return_pct=round(realized,4),days_to_exit=days))
 out=pathlib.Path(out);out.mkdir(parents=True,exist_ok=True)
 wr(out/'01_FLOW_D0_STATE_4ACTORS.csv',base)
 wr(out/'00_PRICE_FLOW_QUALITY_QUARANTINE.csv',price_quarantine,['date','code','reason'])
 wr(out/'02_STAGE_ASOF_ALL28_MA_BB_ACTOR_FEATURES.csv',stage)
 wr(out/'03_TARGET_STOP_HORIZON_TRADES.csv',trades)
 wr(out/'06_STAGE_15PCT_STRONG_DURABLE_FAILURE.csv',strong15)
 summary=summarize(trades,['stage','flow_state_D0','horizon','target_pct','stop_pct'])
 wr(out/'04_FLOW_STAGE_TARGET_STOP_RESULTS.csv',summary)
 # Split chronology must not be treated as pristine OOS given repeated 2021-25 prior investigations.
 temporal=[]
 for x in trades:
  y=int(x['year']);x['split']='TRAIN_2021_2023' if y<=2023 else 'VALIDATE_2024' if y==2024 else 'HOLDOUT_2025'
 temporal=summarize(trades,['split','stage','flow_state_D0','horizon','target_pct','stop_pct'])
 wr(out/'05_TEMPORAL_YEAR_SPLIT_DESCRIPTIVE.csv',temporal)
 stats=dict(research_only=True,cohort='PILOT_300',events_frozen=300,events_complete=len(completed),
  events_quarantined=300-len(completed),events_price_quarantined=len(price_quarantine),n_flow_states=len(base),n_stage_events=len(stage),n_strong15_evaluable=len(strong15),n_horizon_target_stop=len(trades),
  no_BB_event_selection=True,no_future_group_selection=True,actor_flow_at_D0_only=True,
  no_imputation=True,PIT_certified=False,prospective_OOS=False,live_modified=False,
  caveat='Stage-specific groups necessarily condition on an asof later signal, so compare to same-date controls and keep unconditional denominator. Past onset relative to future D0 is not used as a trading entry.',
  source_freeze_sha=fs,checkpoint_sha=sha((pathlib.Path(checkpoint)/RAW).read_bytes()),files_sha256={x.name:sha(x.read_bytes()) for x in out.glob('*.csv')})
 (out/'06_ANALYSIS_AUDIT.json').write_text(json.dumps(stats,indent=2,ensure_ascii=False)+'\n')
 print('R1D27R_FLOW_REPLAY_COMPLETE',json.dumps({k:stats[k] for k in ('events_complete','n_flow_states','n_stage_events','n_horizon_target_stop')},ensure_ascii=False),flush=True)
 return stats

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--root',default='.');p.add_argument('--checkpoint',required=True);p.add_argument('--out',required=True);a=p.parse_args()
 replay(a.root,a.checkpoint,a.out)
