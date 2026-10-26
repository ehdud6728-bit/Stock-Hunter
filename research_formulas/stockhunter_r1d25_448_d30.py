#!/usr/bin/env python3
"""R1-D25: retrospective 448MA, D+30, pre-breakout money flow, C-vs-D big-winner audit.

Research only. Historical original unadjusted KRX price/amount. No PIT sector labels,
no real institutional order-flow, no native LIVE 66 searches, no orders.
"""
from __future__ import annotations
import argparse, collections, gzip, hashlib, io, json, math, os, pathlib, zipfile
import numpy as np
import pandas as pd

H=(5,10,15,20,30);DONE='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED';CENS='CENSORED_MISSING_OR_ACTION_UNCERTAIN'
FAMILIES=('A_BELOW224_WAVE_PULLBACK','C_ABOVE224_RISING_WAVE_PULLBACK','D_LEADER_CONSOLIDATION_BREAKOUT')
ROOT=pathlib.Path

def require(pred,msg):
    if not pred: raise RuntimeError('R1D25_FAIL_CLOSED_'+str(msg))

def sha(b):return hashlib.sha256(b).hexdigest()
def dump(o,p):
    p=ROOT(p);p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(o,indent=2,ensure_ascii=False,sort_keys=True,default=str)+'\n',encoding='utf-8')
def store(df,p):
    p=ROOT(p);p.parent.mkdir(parents=True,exist_ok=True);df.to_csv(p,index=False,encoding='utf-8-sig',compression='gzip' if p.name.endswith('.gz') else None)
def normcodes(s):return s.astype('string').str.zfill(6)

def load_source(source_zip,protocol):
    b=ROOT(source_zip).read_bytes();require(sha(b)==protocol['r1d23_artifact_zip_sha256'],'R1D23_ARTIFACT_SHA')
    with zipfile.ZipFile(io.BytesIO(b)) as z:
        def get(name):
            m=[x for x in z.namelist() if x.endswith('/'+name) or x==name]
            require(len(m)==1,'SOURCE_MEMBER_'+name)
            return z.read(m[0])
        m=json.loads(get('R1D23_MANIFEST.json'))
        require(m['fullmarket_signal_rows']==23266 and m['price_path_rows']==93064,'SOURCE_MANIFEST')
        f=pd.read_csv(io.BytesIO(gzip.decompress(get('R1D23_FULLMARKET_A_B_C_D_ASOF_ONLY.csv.gz'))),dtype={'code':str,'date':str},low_memory=False)
        p=pd.read_csv(io.BytesIO(gzip.decompress(get('R1D23_FROZEN_FUTURE_PATHS.csv.gz'))),dtype={'code':str,'date':str},low_memory=False)
        k=json.loads(get('R1D23_PREDECLARED_PROTOCOL.json'))
    f['code']=normcodes(f['code']);p['code']=normcodes(p['code'])
    require(len(f)==23266 and len(p)==93064,'SOURCE_ROWS')
    require(not f[['strategy','date','code']].duplicated().any(),'SIGNAL_DUPLICATE')
    require(not p[['strategy','date','code','horizon']].duplicated().any(),'PATH_DUPLICATE')
    require(set(p.horizon)=={5,10,15,20},'SOURCE_HORIZON_CONTRACT')
    require(set(f.strategy)==set(k['strategies'].keys()) if isinstance(k.get('strategies'),dict) else len(set(f.strategy))==4,'SOURCE_STRATEGIES')
    return f,p,m

def prepare_krx(root,expected):
    root=ROOT(root);mp={}; idx_file=root/'shard_00'/'official_indices.csv'
    for shard in range(8):
        for q in root.glob(f'shard_{shard:02d}/manifest_shard_{shard:02d}.json'):
            d=json.loads(q.read_text(encoding='utf-8'))
            for row in d['files']:
                key=row['date'];require(key not in mp,'DUP_DATE_'+key)
                mp[key]=(q.parent/row['file'],row['sha256'])
    days=sorted(mp);require(len(days)==1907 and days[0]=='2019-01-02' and days[-1]=='2026-10-12','KRX_1907_COVERAGE')
    require(idx_file.is_file(),'MISSING_KRX_INDEX')
    x=pd.read_csv(idx_file,dtype={'date':str,'index':str})
    require(not x[['date','index']].duplicated().any(),'INDEX_DUPLICATE')
    y=x.pivot(index='date',columns='index',values='close').sort_index().astype(float)
    require(list(y.index)==days and set(y.columns)=={'KOSPI','KOSDAQ'},'INDEX_DATE_OR_NAME_MISMATCH')
    require(y.notna().all().all(),'INDEX_GAPS')
    return days,mp,y

def price_daily(root,days,mp,codes):
    # SHA check for EVERY day; build only signal-code histories without future code universe membership assumptions.
    rows=collections.defaultdict(list); rawcounts=0
    for t,day in enumerate(days):
        path,digest=mp[day]; require(path.is_file(),'KRX_DAY_MISSING_'+day)
        raw=path.read_bytes(); require(sha(raw)==digest,'KRX_SHA_DRIFT_'+day)
        df=pd.read_csv(io.BytesIO(raw),compression='gzip',dtype={'code':str,'date':str},usecols=['code','date','open','high','low','close','volume','amount'])
        rawcounts+=len(df)
        require(df.date.nunique()==1 and df.date.iat[0]==day and not df.code.duplicated().any(),'DAY_SCHEMA_'+day)
        df=df[df.code.isin(codes)]
        for r in df.itertuples(index=False,name=None):
            code,dt,o,h,l,c,v,a=r
            rows[code].append((t,float(o),float(h),float(l),float(c),float(v),float(a)))
        if t%450==0:print('R1D25_SHA_READING',day,t+1,rawcounts,flush=True)
    return rows,rawcounts

def median_safe(a):
    return float(np.median(a)) if len(a) and np.all(np.isfinite(a)) else np.nan

def get_feature(arry,day_index,ix_index,index_prices):
    t=int(day_index);j=ix_index.get(t)
    if j is None:return None
    # signal row exact at t (no forward information)
    ix=np.asarray(arry[:j+1],dtype=float)
    if len(ix)<66:return None
    if not np.all(np.isfinite(ix[-66:])):return None
    o,h,l,c,v,amt=[ix[:,i] for i in range(1,7)]
    if np.any(c[-66:]<=0) or np.any(o[-66:]<=0) or np.any(amt[-66:]<=0):return None
    # Indicators at signal only, 448 session lookback; MA448 slope uses past 20 sessions.
    ma448=float(c[-448:].mean()) if len(c)>=448 else np.nan
    prevma448=float(c[-468:-20].mean()) if len(c)>=468 else np.nan
    ma224=float(c[-224:].mean()) if len(c)>=224 else np.nan
    sl224base=float(c[-244:-20].mean()) if len(c)>=244 else np.nan
    gap=np.abs(o[1:]/c[:-1]-1)
    valid448=int(len(c)>=468 and all(np.diff(ix[-468:,0])==1) and not np.any(gap[-467:]>0.40))
    if not valid448: ma448=np.nan;prevma448=np.nan
    def ratio(a,b):return a/b if np.isfinite(a) and np.isfinite(b) and b>0 else np.nan
    pre5=median_safe(amt[-6:-1]);pre20=median_safe(amt[-26:-6]);pre60=median_safe(amt[-66:-26])
    pre20wide=median_safe(amt[-21:-1]);old40=median_safe(amt[-61:-21])
    cf=lambda x:float(x)
    pc=np.diff(c[-22:-1]);priorclose=c[-22:-2]
    sr=np.sign(pc)*amt[-21:-1]
    signed_flow20=float(sr.sum()/np.sum(amt[-21:-1])) if np.sum(amt[-21:-1])>0 else np.nan
    ranges=h[-21:-1]-l[-21:-1]
    clv=np.divide((2*c[-21:-1]-h[-21:-1]-l[-21:-1]),ranges,out=np.zeros_like(ranges),where=ranges>0)
    clv_turnover20=float(np.sum(clv*amt[-21:-1])/np.sum(amt[-21:-1])) if np.sum(amt[-21:-1])>0 else np.nan
    # PEAK/large winner labels not accessed in this function.
    k20=100*(c[-1]/c[-21]-1);k60=100*(c[-1]/c[-61]-1)
    i20=100*(index_prices.loc[day_index,'KOSPI']/index_prices.loc[day_index-20,'KOSPI']-1) if day_index>=20 else np.nan
    q20=100*(index_prices.loc[day_index,'KOSDAQ']/index_prices.loc[day_index-20,'KOSDAQ']-1) if day_index>=20 else np.nan
    i60=100*(index_prices.loc[day_index,'KOSPI']/index_prices.loc[day_index-60,'KOSPI']-1) if day_index>=60 else np.nan
    q60=100*(index_prices.loc[day_index,'KOSDAQ']/index_prices.loc[day_index-60,'KOSDAQ']-1) if day_index>=60 else np.nan
    return {
        'ma448':ma448,'ma448_available':valid448,'ma448_slope20_pct':100*(ma448/prevma448-1) if valid448 else np.nan,
        'distance448_pct':100*(c[-1]/ma448-1),
        'ma224_recomputed':ma224,'ma224_slope20_recomputed':100*(ma224/sl224base-1),
        'ma224_above_ma448':int(ma224>ma448) if valid448 else np.nan,
        'above_448':int(c[-1]>ma448) if valid448 else np.nan,'rising_448':int(ma448>prevma448) if valid448 else np.nan,
        'both_above_224_448':int(c[-1]>ma224 and c[-1]>ma448) if valid448 else np.nan,
        'strong_stacked_224_448':int(c[-1]>ma224>ma448 and ma448>prevma448) if valid448 else np.nan,
        'amount_pre5_vs_pre20':ratio(pre5,pre20),
        'amount_pre20_vs_pre40':ratio(pre20wide,old40),
        'amount_signal_vs_prior20':ratio(amt[-1],pre20wide),
        'signed_turnover_pre20':signed_flow20,
        'clv_turnover_pre20':clv_turnover20,
        'ret20_prior_signal_pct':100*(c[-2]/c[-22]-1),
        'ret60_signal_pct':k60,'ret20_signal_pct':k20,
        'rs20_vs_kospi_pctp':k20-i20,'rs20_vs_kosdaq_pctp':k20-q20,
        'rs60_vs_kospi_pctp':k60-i60,'rs60_vs_kosdaq_pctp':k60-q60,
        'rs20_above_both_index':int(k20>i20 and k20>q20),
        'rs60_above_both_index':int(k60>i60 and k60>q60),
        'preflow_rising_and_price_soft':int(ratio(pre5,pre20)>=1.2 and 100*(c[-2]/c[-12]-1)<=0),
        'asof_lookback_last_index':t,
    }

def forward_path30(arry,t,ix_index,days,signalclose):
    j=ix_index.get(t)
    require(j is not None,'SIGNAL_DAY_NOT_IN_TICKER')
    bad=False;last=float(signalclose);res=[];good=[];entry=None
    for h in range(1,31):
        k=t+h
        v=arry[j+h] if j+h<len(arry) and arry[j+h][0]==k else None
        if k>=len(days) or v is None:bad=True
        if not bad:
            _,op,hi,lo,cl,volume,amount=v
            if min(op,hi,lo,cl,volume,amount)<=0 or not all(math.isfinite(a) for a in (op,hi,lo,cl,volume,amount)) or abs(op/last-1)>0.4:
                bad=True
        if not bad:
            if h==1:entry=op
            good.append(v);last=cl
        if h in H:
            row={'horizon':h,'status_out':CENS if bad else DONE,
                 'net_20bps_pct':np.nan,'mfe_intraday_pct':np.nan,'mae_intraday_pct':np.nan,
                 'entry_day':days[t+1] if t+1<len(days) else '',
                 'exit_day':days[k] if k<len(days) else ''}
            if not bad:
                row.update(net_20bps_pct=100*(good[-1][4]/entry-1)-0.20,
                           mfe_intraday_pct=100*(max(x[2] for x in good)/entry-1),
                           mae_intraday_pct=100*(min(x[3] for x in good)/entry-1))
            res.append(row)
    return res

def experiment_groups():
    # Frozen ex-ante descriptive cutoffs; no tuning on label data.
    return {
        'MA448_ABOVE':('above_448',lambda x:x==1),
        'MA448_RISING':('rising_448',lambda x:x==1),
        'MA224_ABOVE_448':('ma224_above_ma448',lambda x:x==1),
        'MA448_STRONG_STACK':('strong_stacked_224_448',lambda x:x==1),
        'PRE5_TURNOVER_UP_1P20':('amount_pre5_vs_pre20',lambda x:x>=1.20),
        'PRE20_TURNOVER_UP_1P20':('amount_pre20_vs_pre40',lambda x:x>=1.20),
        'SIGNED_TURNOVER_POSITIVE':('signed_turnover_pre20',lambda x:x>0),
        'CLV_TURNOVER_POSITIVE':('clv_turnover_pre20',lambda x:x>0),
        'RS20_ABOVE_BOTH_INDICES':('rs20_above_both_index',lambda x:x==1),
        'RS60_ABOVE_BOTH_INDICES':('rs60_above_both_index',lambda x:x==1),
        'PRE_FLOW_UP_PRICE_SOFT':('preflow_rising_and_price_soft',lambda x:x==1),
        'D_BASE10_RANGE_LE10':('base10_range_pct',lambda x:x<=10),
        'D_BREAKOUT_AMOUNT_GE3':('amount_vs_prior20',lambda x:x>=3),
        'D_COMPRESSION_AND_AMOUNT_GE3':('D_compression_amount3',lambda x:x==1),
        'C_A_WAVE_RETRACE_LE50':('pullback_retrace_pct',lambda x:x<=50),
    }

def analyze(feat,paths,basepaths,out,protocol):
    require(feat['asof_lookback_last_index'].eq(feat['signal_ix']).all(),'FUTURE_FEATURE_INDEX')
    require(feat[['strategy','date','code']].notna().all().all(),'MISSING_FEATURE_KEYS')
    k=['strategy','date','code','period'];g=paths.merge(feat,on=k,validate='many_to_one')
    g['win']=g.net_20bps_pct>0
    g['bigwin_close15']=g.net_20bps_pct>=15
    g['bigwin_touch15']=g.mfe_intraday_pct>=15
    g['loss5']=g.net_20bps_pct<=-5
    g['loss10']=g.net_20bps_pct<=-10
    g['giveback_pctp']=g.mfe_intraday_pct-g.net_20bps_pct-0.20
    done=g[g.status_out.eq(DONE)]
    # Full original horizons parity; source and new identical by key, censor status + numeric.
    merged=paths[paths.horizon.ne(30)].merge(basepaths,on=k+['horizon'],suffixes=('_new','_old'),validate='one_to_one')
    require(len(merged)==len(basepaths),'R1D23_ALL_FOUR_HORIZONS_COVERAGE')
    require(merged.status_out_new.eq(merged.status_out_old).all(),'PARENT_CENSOR_PARITY')
    for x in ('net_20bps_pct','mfe_intraday_pct','mae_intraday_pct'):
        a=merged[x+'_new'].to_numpy(dtype=float);b=merged[x+'_old'].to_numpy(dtype=float)
        require(np.allclose(a,b,atol=1e-9,rtol=1e-10,equal_nan=True),'PARENT_PRICE_PARITY_'+x)
    store(pd.DataFrame([{'metric':'all_parent_paths_equal','rows':len(merged),'status':'PASS'},
                        {'metric':'additional_D30_paths','rows':len(paths[paths.horizon.eq(30)]),'status':'PASS'}]),out/'R1D25_PARENT_D5_D20_PRICE_PARITY.csv')
    # Frozen factor scorecards across two disjoint dates (not prospective).
    row=[]; flags=experiment_groups()
    def metrics(s):
        return {'n':len(s),'days':s.date.nunique(), 'win_pct':float(s.win.mean()*100) if len(s) else np.nan,
                'mean_net_pct':float(s.net_20bps_pct.mean()) if len(s) else np.nan,
                'median_net_pct':float(s.net_20bps_pct.median()) if len(s) else np.nan,
                'loss5_pct':float(s.loss5.mean()*100) if len(s) else np.nan,
                'loss10_pct':float(s.loss10.mean()*100) if len(s) else np.nan,
                'bigwin_close15_pct':float(s.bigwin_close15.mean()*100) if len(s) else np.nan,
                'bigwin_touch15_pct':float(s.bigwin_touch15.mean()*100) if len(s) else np.nan,
                'mean_mfe_pct':float(s.mfe_intraday_pct.mean()) if len(s) else np.nan,
                'mean_mae_pct':float(s.mae_intraday_pct.mean()) if len(s) else np.nan,
                'mean_giveback_pctp':float(s.giveback_pctp.mean()) if len(s) else np.nan}
    for (per,strat,h),sub in done.groupby(['period','strategy','horizon']):
        for name,subflag in [('ALL',pd.Series(True,index=sub.index))]+[(label,test(sub[col])) for label,(col,test) in flags.items()]:
            for state,mask in ([(1,subflag)] if name=='ALL' else [(1,subflag),(0,(~subflag) & (sub[flags[name][0]].notna()))]):
                part=sub.loc[mask]
                row.append({'period':per,'strategy':strat,'horizon':int(h),'factor':name,'state':state,**metrics(part)})
    score=pd.DataFrame(row);store(score,out/'R1D25_A_C_D_448_MONEYFLOW_SCORECARDS.csv')
    # C-D (and A) same-date contrast, no mixing matched and unmatched dates.
    contrasts=[]
    for period in sorted(done.period.unique()):
        for horizon in H:
            z=done[(done.period==period)&(done.horizon==horizon)]
            daily=z.groupby(['date','strategy']).agg(mean_net=('net_20bps_pct','mean'),bigwin=('bigwin_touch15','mean'),loss5=('loss5','mean'),n=('code','size')).reset_index()
            for left,right in [(FAMILIES[2],FAMILIES[1]),(FAMILIES[2],FAMILIES[0]),(FAMILIES[1],FAMILIES[0])]:
                a=daily[daily.strategy.eq(left)].set_index('date');b=daily[daily.strategy.eq(right)].set_index('date')
                both=a.join(b,how='inner',lsuffix='_A',rsuffix='_B')
                contrasts.append({'period':period,'horizon':horizon,'left':left,'right':right,'matched_days':len(both),
                    'equal_date_avg_net_diff_pctp':float((both.mean_net_A-both.mean_net_B).mean()) if len(both) else np.nan,
                    'equal_date_bigwin_touch15_diff_pctp':float((both.bigwin_A-both.bigwin_B).mean()*100) if len(both) else np.nan,
                    'equal_date_loss5_diff_pctp':float((both.loss5_A-both.loss5_B).mean()*100) if len(both) else np.nan})
    store(pd.DataFrame(contrasts),out/'R1D25_C_D_A_SAME_DATE_D5_D30.csv')
    # Within D pre-signal comparisons, bigwin vs failure at H30, GROUP labels measured at D30 only.
    d=done[(done.strategy==FAMILIES[2])&(done.horizon==30)].copy()
    d['outcome_group']=np.select([d.bigwin_touch15,d.loss5],['TOUCHED_PLUS15','CLOSE_LE_MINUS5'],default='OTHER')
    profiles=[]
    names=['wave_gain_pct','pullback_retrace_pct','prewave_amount_ratio','pullback_dry_ratio','ma448_slope20_pct','distance448_pct','amount_pre5_vs_pre20','amount_pre20_vs_pre40','signed_turnover_pre20','clv_turnover_pre20','rs20_vs_kospi_pctp','rs20_vs_kosdaq_pctp','rs60_vs_kospi_pctp','rs60_vs_kosdaq_pctp','base10_range_pct','amount_vs_prior20']
    for (per,grp),s in d.groupby(['period','outcome_group']):
        for col in names:
            profiles.append({'period':per,'group':grp,'feature':col,'n':len(s),
                             'median':float(s[col].median()),'q25':float(s[col].quantile(.25)),'q75':float(s[col].quantile(.75))})
    store(pd.DataFrame(profiles),out/'R1D25_D_PLUS15_VS_FAIL_PRE_SIGNAL_PROFILES.csv')
    extended=[]
    for (per,strat,grp),q in done[(done.horizon==30)&done.strategy.isin(FAMILIES)].assign(
        outcome_group=lambda x:np.select([x.bigwin_touch15,x.loss5],['TOUCHED_PLUS15','CLOSE_LE_MINUS5'],default='OTHER')
    ).groupby(['period','strategy','outcome_group']):
        for col in names:
            extended.append({'period':per,'strategy':strat,'group':grp,'feature':col,'n':len(q),
                             'median':float(q[col].median()),'q25':float(q[col].quantile(.25)),'q75':float(q[col].quantile(.75))})
    store(pd.DataFrame(extended),out/'R1D25_A_C_D_PLUS15_VS_FAIL_PRE_SIGNAL_PROFILES.csv')
    # No post-outcome filtering: same-date factor v complement comparisons with min paired days.
    factordiff=[]
    for per in sorted(d.period.unique()):
        part=d[d.period.eq(per)]
        for label,(col,test) in flags.items():
            a=part[part[col].notna()].assign(flg=test(part.loc[part[col].notna(),col])).groupby(['date','flg']).agg(r=('net_20bps_pct','mean'),
                             loss=('loss5','mean'),touch=('bigwin_touch15','mean'),n=('code','size')).reset_index()
            pivot=a.pivot(index='date',columns='flg',values=['r','loss','touch','n']) if len(a) else pd.DataFrame()
            pivot=pivot.dropna() if (len(pivot) and 'r' in pivot.columns.get_level_values(0) and True in pivot['r'].columns and False in pivot['r'].columns) else pivot.iloc[:0]
            factordiff.append({'period':per,'factor':label,'matched_days':len(pivot),
                'mean_net_diff_pctp':float((pivot['r'][True]-pivot['r'][False]).mean()) if len(pivot) else np.nan,
                'touch15_diff_pctp':float(100*(pivot['touch'][True]-pivot['touch'][False]).mean()) if len(pivot) else np.nan,
                'loss5_diff_pctp':float(100*(pivot['loss'][True]-pivot['loss'][False]).mean()) if len(pivot) else np.nan})
    store(pd.DataFrame(factordiff),out/'R1D25_D_SAME_DAY_FLAG_CONTRASTS_D30.csv')
    return score,done

def run(args):
    protocol=json.loads(ROOT(args.protocol).read_text('utf-8'))
    require(protocol['mode']=='FROZEN_RESEARCH_ONLY' and protocol['horizons']==list(H),'LOCKED_PROTO')
    out=ROOT(args.out);out.mkdir(parents=True,exist_ok=True)
    if args.mode=='lock':
        dump({'protocol_sha256':sha(ROOT(args.protocol).read_bytes()),'hypotheses_locked_before_outcomes':True,'source':protocol['r1d23_artifact_zip_sha256']},out/'R1D25_LOCK.json')
        print('R1D25_LOCKED_BEFORE_OUTCOME',flush=True);return
    lock=json.loads((out/'R1D25_LOCK.json').read_text());require(lock['protocol_sha256']==sha(ROOT(args.protocol).read_bytes()),'PROTOCOL_DRIFT')
    f,p,m=load_source(args.source_zip,protocol)
    days,mp,indices=prepare_krx(args.krx_root,protocol)
    codes=set(f.code);raw,rawcount=price_daily(args.krx_root,days,mp,codes)
    dmap={d:i for i,d in enumerate(days)}
    require(len(set(f.date)-set(days))==0,'MISSING_SIGNAL_DATE')
    ixmap={c:{int(r[0]):i for i,r in enumerate(arr)} for c,arr in raw.items()}
    # use trading-calendar integer index for indices, never map future index to historical feature
    ip=indices.reset_index(drop=True)
    feats=[];paths=[];badfeat=collections.Counter()
    for row in f.itertuples(index=False):
        code=row.code;t=dmap[row.date];arr=raw.get(code,[]);im=ixmap.get(code,{})
        require(int(row.signal_ix)==t,'KRX_CALENDAR_DRIFT')
        require(t in im,'MISSING_SIGNAL_PRICE')
        srcbar=arr[im[t]];require(abs(srcbar[4]-float(row.signal_close))<1e-9,'ASOF_CLOSE_PARITY')
        ft=get_feature(arr,t,im,ip)
        if ft is None:
            badfeat['INSUFFICIENT_448_OR_UNADJUSTED_PRICE_GAP']+=1;ft={}
        fields={'strategy':row.strategy,'date':row.date,'code':code,'period':row.period,'signal_ix':t,
                'signal_close':float(row.signal_close),'base10_range_pct':getattr(row,'base10_range_pct'),
                'amount_vs_prior20':getattr(row,'amount_vs_prior20'),
                'wave_gain_pct':getattr(row,'wave_gain_pct'),
                'pullback_retrace_pct':getattr(row,'pullback_retrace_pct'),
                'prewave_amount_ratio':getattr(row,'prewave_amount_ratio'),
                'pullback_dry_ratio':getattr(row,'pullback_dry_ratio'),
                'D_compression_amount3':(int(getattr(row,'base10_range_pct')<=10 and getattr(row,'amount_vs_prior20')>=3) if str(row.strategy).startswith('D_') else np.nan)}
        feats.append({**fields,**ft})
        for price in forward_path30(arr,t,im,days,row.signal_close):paths.append({**{k:fields[k] for k in ['strategy','date','code','period']},**price})
    feat=pd.DataFrame(feats);pp=pd.DataFrame(paths)
    require(len(feat)==23266 and len(pp)==23266*5,'NEW_CARDINALITY')
    require(sum(badfeat.values())<len(feat),'ALL_ASOF_MISSING')
    require(feat.asof_lookback_last_index.eq(feat.signal_ix).all(),'NO_LOOKAHEAD')
    require(np.allclose(feat.ma224_recomputed.to_numpy(),f.ma224.to_numpy(),rtol=1e-10,atol=1e-7,equal_nan=True),'MA224_SOURCE_PARITY')
    require(np.allclose(feat.ma224_slope20_recomputed.to_numpy(),f.ma224_slope20_pct.to_numpy(),rtol=1e-9,atol=1e-7,equal_nan=True),'MA224_SLOPE_SOURCE_PARITY')
    store(feat,out/'R1D25_ALL_STRATEGIES_SIGNALTIME_448_FLOW_RS_NO_OUTCOMES.csv.gz')
    seal=sha(gzip.decompress((out/'R1D25_ALL_STRATEGIES_SIGNALTIME_448_FLOW_RS_NO_OUTCOMES.csv.gz').read_bytes()))
    dump({'features_sha256_plaintext':seal,'features_count':len(feat),'frozen_before_outcomes':True},out/'R1D25_ASOF_SEAL.json')
    store(pp,out/'R1D25_D5_D10_D15_D20_D30_FUTURE_PRICE_PATHS.csv.gz')
    dump({'krx_daily_sha_verified':1907,'krx_price_rows_read':rawcount,'signals_source_price_parity':len(f),'source_price_paths_parity_pending':True},out/'R1D25_RAW_DATA_AUDIT.json')
    score,done=analyze(feat,pp,p,out,protocol)
    raw_audit=json.loads((out/'R1D25_RAW_DATA_AUDIT.json').read_text('utf-8'))
    raw_audit['source_price_paths_parity_pending']=False
    raw_audit['source_price_paths_parity_pass']=True
    dump(raw_audit,out/'R1D25_RAW_DATA_AUDIT.json')
    manifest={'version':'R1D25_448_D30_C_D_PRE_BREAKOUT_MONEYFLOW_V1',
        'source_r1d23_run':40218876941,'source_artifact_id':12609117663,
        'source_zip_verified':True,'krx_daily_sha_verified':1907,'krx_price_rows_read':rawcount,
        'frozen_signal_rows':len(f),'new_horizon_rows':len(pp),'price_parity_old_horizons':len(p),
        'signals_valid_448':int(feat.ma448_available.fillna(0).sum()),'signals_unavailable_448':int(feat.ma448_available.eq(0).sum()),'asof_plaintext_sha256':seal,
        'signal_counts':f.strategy.value_counts().to_dict(),
        'D30_outcomes_completed':int(pp[(pp.horizon==30)&pp.status_out.eq(DONE)].shape[0]),
        'D30_outcomes_censored':int(pp[(pp.horizon==30)&pp.status_out.ne(DONE)].shape[0]),
        'sector_industry_PIT_labels_available':False,
        'sector_leadership_tested':False,'institutional_net_buying_available':False,
        'signed_turnover_is_price_direction_proxy_not_true_net_fund_flows':True,
        'market_relative_strength_proxy_tested':True,
        'multi_testing_risk':True,'historical_only_not_prospective_OOS':True,
        'corporate_action_PIT_certified':False,'actual_fill_certified':False,
        'strategy_ready':False,'live_changed':False,'orders_placed':0}
    dump(manifest,out/'R1D25_MANIFEST.json')
    dump({'industry_source':'KRX_ORIGINAL_DAILY_OHLCV_AMOUNT_CODE_DATE_PLUS_OFFICIAL_INDICES',
          'industry_columns_present':False,'historical_industry_mapping_provided':False,
          'PIT_sector_leadership':'NOT_MEASURABLE_FROM_THIS_SOURCE',
          'substitute_tested':'RS20_RS60_vs_both_indices_as_market_strength_proxy',
          'net_institutional_buyer':'NOT_AVAILABLE; signed turnover is NOT buyer identity'},out/'R1D25_SECTOR_PIT_DATA_AVAILABILITY.json')
    print('R1D25_COMPLETE',json.dumps(manifest,ensure_ascii=False),flush=True)

if __name__=='__main__':
    pa=argparse.ArgumentParser();pa.add_argument('--mode',required=True,choices=['lock','analyze']);pa.add_argument('--protocol',required=True);pa.add_argument('--source-zip',default='');pa.add_argument('--krx-root',default='');pa.add_argument('--out',required=True)
    run(pa.parse_args())
