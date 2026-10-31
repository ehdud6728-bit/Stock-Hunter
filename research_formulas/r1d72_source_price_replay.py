#!/usr/bin/env python3
"""R1-D72 research-only source-price independent BB/Envelope entry stage audit.
Reads immutable R1-D71 5,247 selected events, original daily KRX shards; no LIVE mutations.
No corporate-action-adjusted authority and no verified executable fill or true OOS.
"""
from __future__ import annotations
import argparse,collections,csv,gzip,hashlib,io,json,math,pathlib,statistics,zipfile
import numpy as np,pandas as pd

ARCHIVE_EXPECTED={
'00':'84dd5aa62f0c9538cd9c2d0ed0540c8f74063b21be49d9ecfc81e4b1583368b5',
'01':'3d372eebf915c870badf103356c3700d5fa914a32d79ce8057ef1f4c4f89295b',
'02':'5c49df64ad10bafbc09dd7ea78b9377804735fda6ad861018750b396ab2a27cb',
'03':'7a359f5959e2374622c40d068920adf9d695142b4a61b540d0752a8cb9bb4dd8',
'04':'88b35ea4c9a8a6a0ed4b61250a8018d49ee7ba4489c7345f04012915bb60f997',
'05':'eb503f7cabcc40991037092c6463437c8a8b71d43fb26fe646ea4f56885b3814',
'06':'b3cc055d919c7ab92d7f27b1b3ce6d3e56df787b107af961eaa43dedb885d6cd',
'07':'9563747eb8b27928ba0a0d2b7d3cf02e787acd2e9e4a656d410c286139321478'}

BANDS={
'BB20_L':('bb20',.025), 'BB40_L':('bb40',.025),
'ENV20_L_10':('env20_10',.02),'ENV20_L_20':('env20_20',.02),
'ENV40_L_10':('env40_10',.10),'ENV40_L_40':('env40_40',.10),
}
COST_PP=.2

def sha(data):return hashlib.sha256(data).hexdigest()
def check_bar(r):
 try:
  o,h,l,c,v,a=[float(r[k]) for k in ['open','high','low','close','volume','amount']]
  return min(o,h,l,c,v,a)>0 and h>=max(o,c) and l<=min(o,c)
 except (ValueError,TypeError,KeyError):return False

def indicators(df):
 c=df['close']
 for n in [20,40]:
  ma=c.rolling(n,min_periods=n).mean();dev=c.rolling(n,min_periods=n).std(ddof=0)
  df[f'bb{n}']=ma-2*dev
  df[f'env{n}_10']=ma*.9
 df['env20_20']=c.rolling(20,min_periods=20).mean()*.8
 df['env40_40']=c.rolling(40,min_periods=40).mean()*.6
 df['ma5']=c.rolling(5,min_periods=5).mean()
 df['vol20prev']=df['volume'].shift(1).rolling(20,min_periods=20).mean()
 return df

def is_near(close,lower,tol):
 return all(math.isfinite(float(v)) for v in (close,lower)) and lower>0 and abs(close/lower-1)<=tol

def touch(low,high,lower):
 return all(math.isfinite(float(v)) for v in (low,high,lower)) and lower>0 and low<=lower*1.01 and high>=lower*.99

def reclaim(pc,pl,c,l):
 return all(math.isfinite(float(v)) for v in (pc,pl,c,l)) and pl>0 and l>0 and pc<pl and c>=l

def confirm_next(sig,nextday,level):
 # D+1 close confirmed; D+2 open earliest possible entry.
 return bool(nextday is not None and sig is not None and all(math.isfinite(float(v)) for v in [sig.low,sig.close,nextday.low,nextday.close,level])
     and nextday.close>=sig.close and nextday.low>=sig.low and nextday.close>=level)

def replay_exit(df,ix,delay,horizon=20):
 # t day ix (after-close decision). Entry strictly t+delay session OPEN;
 # evaluate H bars inclusive entry, terminal t+delay+H-1 CLOSE.
 start=ix+delay;end=start+horizon
 if start<0 or end>len(df):return {'valid':False,'reason':'INSUFFICIENT_FUTURE'}
 v=df.iloc[start:end];first=v.iloc[0]
 if len(v)!=horizon or not v.valid_bar.all():return {'valid':False,'reason':'INVALID_FUTURE_BAR'}
 p=float(first.open);last=float(v.iloc[-1].close)
 if not p>0:return {'valid':False,'reason':'INVALID_ENTRY'}
 prior=float(df.iloc[start-1].close)
 prevs=np.r_[prior,v.close.to_numpy(float)];gaps=prevs[1:]/prevs[:-1]-1
 intraday=np.r_[v.low.to_numpy(float)/p-1]
 max_abs_jump=float(np.max(np.abs(gaps)))
 return {'valid':True,'reason':'PRICE_PATH_ONLY','entry_day':str(first.date),'entry_open':p,
  'exit_day':str(v.iloc[-1].date),'exit_close':last,
  'ret_net_pct':(last/p-1)*100-COST_PP, 'mae_pct':float(np.min(intraday))*100,
  'max_abs_close_jump_pct':max_abs_jump*100,
  'price_jump_watch':bool(max_abs_jump>.35)}

def load_source(shard_files,events):
 codes=set(events.code)
 bycode=collections.defaultdict(list);all_dates=set();n_gz=0;n_rows=0;digest_map={}
 for p in shard_files:
  p=pathlib.Path(p)
  buf=p.read_bytes()
  with zipfile.ZipFile(io.BytesIO(buf)) as z:
   names=z.namelist();prefix=next((s.split('/')[0] for s in names if '/manifest_shard_' in s),None)
   if not prefix:raise RuntimeError('NO_SHARD_MANIFEST '+str(p))
   num=prefix.rsplit('_',1)[1]
   digest=sha(buf)
   if digest!=ARCHIVE_EXPECTED[num]:raise RuntimeError(f'ARCHIVE_SHA_MISMATCH_{num}: {digest}')
   if num in digest_map:raise RuntimeError('DUPLICATE_SHARD '+num)
   digest_map[num]=digest
   manifest=json.loads(z.read(prefix+f'/manifest_shard_{num}.json'))
   expected={v['file']:v['sha256'] for v in manifest['files']}
   for nm in names:
    if not nm.endswith('.csv.gz'):continue
    dt=nm.rsplit('/',1)[-1][:8]
    if dt<'20231101' or dt>'20251115':continue
    dts=f'{dt[:4]}-{dt[4:6]}-{dt[6:]}'
    raw=z.read(nm)
    if sha(raw)!=expected[nm.rsplit('/',1)[-1]]:raise RuntimeError('DAILY_SHA_MISMATCH '+nm)
    all_dates.add(dts);n_gz+=1
    for r in csv.DictReader(io.StringIO(gzip.decompress(raw).decode('utf-8'))):
     if r['code'] not in codes:continue
     n_rows+=1
     bycode[r['code']].append((dts,float(r['open']),float(r['high']),float(r['low']),float(r['close']),float(r['volume']),float(r['amount'])))
 if len(digest_map)!=8:raise RuntimeError('NOT_ALL_8_SHARDS')
 calendar=sorted(all_dates)
 return bycode,calendar,{'n_shards':8,'shard_sha256':digest_map,'days_used':n_gz,'distinct_days':len(calendar),'selected_price_rows':n_rows,'ticker_count':len(bycode)}

def build_df(rows,calendar):
 q=pd.DataFrame(rows,columns=['date','open','high','low','close','volume','amount'])
 if q.date.duplicated().any():raise RuntimeError('DUPLICATE_SOURCE_DAY')
 q=q.set_index('date').reindex(calendar).rename_axis('date').reset_index()
 q['valid_bar']=(q[['open','high','low','close','volume','amount']].min(axis=1)>0)&(q['high']>=q[['open','close']].max(axis=1))&(q['low']<=q[['open','close']].min(axis=1))
 # Keep missing data as nan; do not fill, even on suspended sessions.
 return indicators(q)

def fnum(x):return float(x) if x is not None and pd.notna(x) else float('nan')

def collect(events,bycode,calendar):
 result=[]; parity=[];raw_case=[]
 g=events.groupby('code',sort=False)
 for code,ev in g:
  if code not in bycode:
   for e in ev.itertuples(): parity.append({'date':e.date,'code':code,'status':'PRICE_MISSING_TICKER'})
   continue
  df=build_df(bycode[code],calendar)
  dmap={date:i for i,date in enumerate(df.date)}
  for e in ev.itertuples():
   t=dmap.get(e.date)
   if t is None:parity.append({'date':e.date,'code':code,'status':'SIGNAL_DAY_MISSING'});continue
   r=df.iloc[t];pr=df.iloc[t-1] if t else None;nr=df.iloc[t+1] if t+1<len(df) else None
   if not r.valid_bar:
    parity.append({'date':e.date,'code':code,'status':'INVALID_SIGNAL_BAR'});continue
   p0=float(r.close)
   eclose=fnum(e.signal_close)
   ok_price=abs(p0/eclose-1)<1e-10 if eclose>0 else False
   bb20=fnum(r.bb20);bb40=fnum(r.bb40)
   ok_bb20=math.isfinite(bb20) and abs(bb20-fnum(e.bb20_lower_signal))<.01
   ok_bb40=math.isfinite(bb40) and abs(bb40-fnum(e.bb40_lower_signal))<.01
   parity.append({'date':e.date,'code':code,'status':'PASS' if ok_price and ok_bb20 and ok_bb40 else 'MISMATCH',
    'krx_close':p0,'frozen_close':eclose,'krx_bb20':bb20,'frozen_bb20':e.bb20_lower_signal,
    'krx_bb40':bb40,'frozen_bb40':e.bb40_lower_signal})
   if not (ok_price and ok_bb20 and ok_bb40):continue
   for band,(field,tol) in BANDS.items():
    level=fnum(r[field]);prevlevel=fnum(pr[field]) if pr is not None else float('nan')
    if not math.isfinite(level) or level<=0:continue
    near=is_near(float(r.close),level,tol)
    hit=touch(float(r.low),float(r.high),level)
    recovery=reclaim(fnum(pr.close) if pr is not None else float('nan'),prevlevel,float(r.close),level)
    close_above=bool(float(r.close)>=level)
    if not(near or hit or recovery):continue
    day1ok=bool(nr is not None and nr.valid_bar and confirm_next(r,nr,fnum(nr[field])))
    imm=replay_exit(df,t,1);delayed=replay_exit(df,t,2) if day1ok else {'valid':False,'reason':'NO_NEXT_DAY_CONFIRMATION'}
    # flag any source price basis risk even if formal R1D7 flagged only a subset
    watch=bool((imm.get('price_jump_watch',False) or delayed.get('price_jump_watch',False))
            or (imm.get('mae_pct',0)<=-30 or delayed.get('mae_pct',0)<=-30))
    row={'date':e.date,'code':code,'band':band,'level':level,'source_close':p0,'distance_pct':100*(p0/level-1),
      'low_touch':int(hit),'close_above':int(close_above),'near':int(near),'fresh_reclaim':int(recovery),
      'signal_amount_ratio_prev20':float(r.volume)/fnum(r.vol20prev) if math.isfinite(fnum(r.vol20prev)) and fnum(r.vol20prev)>0 else float('nan'),
      'signal_ma5_above':int(float(r.close)>=fnum(r.ma5)) if math.isfinite(fnum(r.ma5)) else 0,
      'touch_reject':int(hit and close_above),'next_day_confirm':int(day1ok),
      'source_future_status':e.D20_status_out,'D20_frozen_ret_pct':e.D20_net_20bps_pct,
      'D20_frozen_entry_day':e.D20_entry_day,'D20_frozen_entry_open':e.D20_entry_open,
      'D20_frozen_exit_day':e.D20_exit_day,'D20_frozen_exit_close':e.D20_exit_close,
      'CA_LEDGER_STATUS':'UNCERTIFIED','price_jump_or_mae_watch':int(watch),
      'entry1_valid':int(imm['valid']),'entry1_day':imm.get('entry_day',''),'entry1_open':imm.get('entry_open',float('nan')),
      'exit1_day':imm.get('exit_day',''),'exit1_close':imm.get('exit_close',float('nan')),
      'entry1_net_pct':imm.get('ret_net_pct',float('nan')),
      'entry1_mae_pct':imm.get('mae_pct',float('nan')),
      'entry2_valid':int(delayed['valid']),'entry2_day':delayed.get('entry_day',''),
      'entry2_open':delayed.get('entry_open',float('nan')),
      'exit2_day':delayed.get('exit_day',''),'exit2_close':delayed.get('exit_close',float('nan')),
      'entry2_net_pct':delayed.get('ret_net_pct',float('nan')),
      'entry2_mae_pct':delayed.get('mae_pct',float('nan')),
      'future_reject_reason':imm['reason'],'source_indicators_asof_no_future':True}
    result.append(row)
    if code in {'279060','214420','032980','099220'}:raw_case.append((code,e.date,band,df,t))
 return pd.DataFrame(result),pd.DataFrame(parity),raw_case

def summarise(df):
 out=[]
 for band,b in df.groupby('band'):
  for flag in ['near','low_touch','touch_reject','fresh_reclaim']:
   s=b[b[flag]==1];no=s[(s.entry1_valid==1)&(s.price_jump_or_mae_watch==0)]
   confirm=no[(no.next_day_confirm==1)&(no.entry2_valid==1)]
   out.append({'band':band,'signal':flag,'selected_n':len(s),'after_price_watch_n':len(no),
    'confirm_next_day_n':len(confirm),'T1_mean_pct':no.entry1_net_pct.mean(),
    'T1_median_pct':no.entry1_net_pct.median(),'T1_win_pct':100*(no.entry1_net_pct>0).mean() if len(no) else float('nan'),
    'confirmed_matched_T1_mean_pct':confirm.entry1_net_pct.mean(),
    'confirmed_matched_T2_mean_pct':confirm.entry2_net_pct.mean(),
    'confirmed_T2_minus_T1_pp':(confirm.entry2_net_pct-confirm.entry1_net_pct).mean(),
    'source_status':'CA_LEDGER_UNCERTIFIED_RAW_PRICE_RESEARCH_ONLY'})
 return pd.DataFrame(out)

def make_charts(cases,out):
 import matplotlib;matplotlib.use('Agg')
 import matplotlib.pyplot as plt
 made=[];taken=set()
 for code,date,band,df,t in cases:
  k=(code,date,band)
  if k in taken or len(made)>=12:continue
  taken.add(k)
  a=max(0,t-45);b=min(len(df),t+32);q=df.iloc[a:b].copy().reset_index(drop=True)
  fig,ax=plt.subplots(figsize=(11,5.1),layout='constrained');x=np.arange(len(q))
  ax.plot(x,q.close.to_numpy(float),label='KRX unadjusted close',linewidth=1.65)
  for col in ['bb20','bb40','env20_10','env20_20','env40_10','env40_40']:
   if col==BANDS[band][0]:ax.plot(x,q[col].to_numpy(float),label=col+' lower',linewidth=1.4)
  x0=t-a;ax.axvline(x0,linestyle='--',linewidth=1)
  ax.scatter([x0],[float(df.iloc[t].close)],s=35,zorder=4)
  ax.set_title(f'{code}  {date} | {band} | ORIGINAL KRX (not corporate-action adjusted)')
  labels=[i for i in range(0,len(q),max(1,len(q)//8))]
  ax.set_xticks(labels,[str(q.date[i])[5:] for i in labels],rotation=20)
  ax.legend(fontsize=8);ax.grid(alpha=.25);ax.set_ylabel('KRW (unadjusted)')
  file=out/f'{code}_{date}_{band}.png';fig.savefig(file,dpi=135);plt.close(fig);made.append(file.name)
 return made

def run(args):
 out=pathlib.Path(args.out);out.mkdir(parents=True,exist_ok=True);(out/'charts').mkdir(exist_ok=True)
 with zipfile.ZipFile(args.r1d71) as z:
  details=pd.read_csv(io.BytesIO(gzip.decompress(z.read('05_ALL_5247_REAUDIT_DETAILS.csv.gz'))),dtype={'code':str,'date':str})
 if len(details)!=5247 or details[['date','code']].duplicated().any():raise RuntimeError('FROZEN_COHORT_DRIFT')
 bycode,calendar,src=load_source(args.shards,details)
 frame,parity,cases=collect(details,bycode,calendar)
 summary=summarise(frame)
 frame.to_csv(out/'02_STAGE_EVENT_DETAILS.csv.gz',compression='gzip',index=False,float_format='%.6f')
 parity.to_csv(out/'01_PRICE_SAME_DATE_PARITY.csv.gz',compression='gzip',index=False,float_format='%.7f')
 summary.to_csv(out/'03_PER_BAND_STAGE_MATCHED_OUTCOME.csv',index=False,float_format='%.5f')
 yearly=[]
 for band,g in frame.groupby('band'):
  for flag in ['near','touch_reject','fresh_reclaim']:
   for yr,z in g[(g[flag]==1)&(g.entry1_valid==1)&(g.price_jump_or_mae_watch==0)].groupby(g.date.str[:4]):
    yearly.append({'year':yr,'band':band,'signal':flag,'n':len(z),'distinct_codes':z.code.nunique(),'mean_raw_pct':z.entry1_net_pct.mean(),'median_raw_pct':z.entry1_net_pct.median()})
 pd.DataFrame(yearly).to_csv(out/'06_YEARLY_DESCRIPTIVE.csv',index=False,float_format='%.5f')
 # Cases selected algorithmically across bands and both success/failure; not representative.
 case_pool=frame[(frame.entry1_valid==1)&(frame.price_jump_or_mae_watch==0)].sort_values('entry1_net_pct')
 if len(case_pool):
  keep=[]
  for band in BANDS:
   ss=case_pool[(case_pool.band==band)&((case_pool.fresh_reclaim==1)|(case_pool.touch_reject==1)|(case_pool.near==1))]
   if len(ss):keep.extend([ss.iloc[0].to_dict(),ss.iloc[-1].to_dict()])
  pd.DataFrame(keep).drop_duplicates(['date','code','band']).to_csv(out/'04_GOOD_BAD_EXAMPLES.csv',index=False)
 # chart prioritized named cases then selected representative sample without using outcome labels as future features.
 made=make_charts(cases,out/'charts')
 raw_r1d7=parity.copy()
 valid_same=parity.status.eq('PASS').sum()
 det=frame[(frame.band=='BB20_L') & (frame.date=='2025-02-04') & (frame.code=='214420')]
 matched=frame[(frame.entry1_valid==1)&(frame.source_future_status=='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED')].drop_duplicates(['date','code'])
 parity_trade=matched[(matched.D20_frozen_entry_day==matched.entry1_day)&(matched.D20_frozen_exit_day==matched.exit1_day)&((matched.D20_frozen_entry_open-matched.entry1_open).abs()<1e-8)&((matched.D20_frozen_exit_close-matched.exit1_close).abs()<1e-8)&((matched.D20_frozen_ret_pct-matched.entry1_net_pct).abs()<1e-6)]
 if len(parity_trade)!=len(matched): raise RuntimeError(f'REPLAY_D20_LABEL_MISMATCH {len(parity_trade)}/{len(matched)}')
 audit={'research':'R1D72_BB_ENV_PRICE_CHART_ENTRY_STAGE','research_only':True,
  'source':src,'frozen_events':len(details),'price_parity_pass':int(valid_same),
  'price_parity_fail':int(len(parity)-valid_same),'independent_T1_frozen_trade_parity':int(len(parity_trade)),'events_with_any_stage':int(frame[['date','code']].drop_duplicates().shape[0]),
  'bands':list(BANDS),'stage_signal_count':int(len(frame)),'n_charts':len(made),
  'chart_names':made,'ca_ledger_certified':False,'historical_price_authority':'ORIGINAL_UNADJUSTED_KRX',
  'outcomes':'D+1 OPEN to 20 bars CLOSE, D+2 OPEN after D+1 close confirmation; 0.2 pctpoint cost',
  'price_watch_definition':'abs daily close-to-close >35% or MAE <-30%; FLAG not verified CA',
  'population':'preselected R1-D6 5247 224MA proxy events, NOT market-wide scanning or clean OOS',
  'live_changes':False,'sample_214420_2025_02_04_count':len(det)}
 (out/'00_AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2))
 (out/'05_CASE_CHART_GALLERY.html').write_text('<!doctype html><meta charset="utf-8"><title>R1D72 actual KRX price review</title><style>body{font-family:system-ui;margin:2rem auto;max-width:1100px;background:#f6f8fb;color:#112}.tile{padding:16px;border-radius:14px;background:white;margin:18px auto}img{width:100%;height:auto}</style><h1>R1-D72 · 원시 KRX 일봉 리뷰</h1><p>기업행위 조정 전 가격입니다. 시점별 실제 수정주가/실체결 인증이 아닙니다.</p>'+''.join(f'<div class="tile"><h3>{s}</h3><img src="charts/{s}" loading="lazy"></div>' for s in made))
 print(json.dumps({**audit,'summary':summary[['band','signal','selected_n','after_price_watch_n','confirm_next_day_n','T1_mean_pct','confirmed_T2_minus_T1_pp']].round(3).to_dict('records')},ensure_ascii=False))
 return audit
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--r1d71',required=True);p.add_argument('--shards',nargs='+',required=True);p.add_argument('--out',required=True);run(p.parse_args())
