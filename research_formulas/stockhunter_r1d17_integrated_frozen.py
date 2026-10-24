#!/usr/bin/env python3
"""R1-D17 frozen exact-date-grade overlay x R143 outcome analysis.

DOES NOT change any trading logic, orders, LIVE scoring, ranking, or research definitions.
Matches previous R1-D17 local implementation; supports zip/folder archives.
Only compares archived after-close labels and research outcome, NOT an OOS model.
"""
from __future__ import annotations
from pathlib import Path
from contextlib import contextmanager
import argparse, hashlib, io, json, zipfile
import pandas as pd
import numpy as np

@contextmanager
def archive_reader(path):
    path=Path(path)
    if path.is_dir():
        class Folder:
            def read(self,member):
                p=(path/member).resolve()
                if not p.is_relative_to(path.resolve()) or not p.is_file():
                    raise FileNotFoundError(f'REQUIRED_MEMBER_MISSING:{member}')
                return p.read_bytes()
        yield Folder()
    elif path.is_file() and path.suffix.lower()=='.zip':
        with zipfile.ZipFile(path,'r') as z: yield z
    else: raise FileNotFoundError(f'FROZEN_ARCHIVE_MISSING:{path}')

def read_member(path,member):
    with archive_reader(path) as a: return a.read(member)

def sha_file(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def load_protocol(path):
    p=json.loads(Path(path).read_text(encoding='utf-8'))
    assert p.get('protocol_id')=='STOCKHUNTER_R1D17_FROZEN_INTEGRATED_V1','PROTOCOL_ID_MISMATCH'
    assert p.get('authority')=='RESEARCH_ONLY_NOT_OOS_NO_LIVE_NO_ORDERS','PROTOCOL_AUTHORITY_MISMATCH'
    assert len(p['daily_sources'])==12 and len({x['signal_date'] for x in p['daily_sources']})==12
    assert set(p['pinned_sources'])=={'r143','r1d13'}
    for z in p['daily_sources']+list(p['pinned_sources'].values()):
        assert len(z['head_sha'])==40 and len(z['zip_sha256'])==64
        assert z['artifact_id']>0 and z['run_id']>0 and z['artifact_name']
    return p

def audit_repo_readonly(repo_root,p):
    repo_root=Path(repo_root)
    for rel,sha in p['frozen_code_blobs'].items():
        target=repo_root/rel
        if not target.is_file():raise FileNotFoundError(f'FROZEN_SOURCE_MISSING:{rel}')
        content=target.read_bytes()
        header=f'blob {len(content)}\0'.encode()
        actual=hashlib.sha1(header+content).hexdigest()
        if actual!=sha:raise AssertionError(f'FROZEN_SOURCE_BLOB_DRIFT:{rel}')
    return True

def pick_archive(root,key):
    root=Path(root)
    choices=[root/(f'RealFull_HIST_{key}.zip'),root/key]
    return next((c for c in choices if c.exists()),choices[0])

def source_sha(path,expected):
    if Path(path).is_file() and sha_file(path)!=expected:raise AssertionError(f'ARCHIVE_SHA_DRIFT:{path}')
    return expected

def analysis(archive_root, r143_source, r1d13_source, out, protocol_path, repo_root=None):
    global ROOT,OUT,RUN,FROZEN,PINS,R143_SOURCE,R1D13_SOURCE,P
    P=load_protocol(protocol_path)
    if repo_root is not None:audit_repo_readonly(repo_root,P)
    ROOT=Path(archive_root)
    OUT=Path(out)
    OUT.mkdir(parents=True,exist_ok=True)
    RUN={x['signal_date']:x['run_id'] for x in P['daily_sources']}
    FROZEN=[]; PINS=[]
    R143_SOURCE=Path(r143_source)
    R1D13_SOURCE=Path(r1d13_source)
    source_sha(R143_SOURCE,P['pinned_sources']['r143']['zip_sha256'])
    source_sha(R1D13_SOURCE,P['pinned_sources']['r1d13']['zip_sha256'])
    for datestr,run_id in RUN.items():
     p=pick_archive(ROOT,datestr)
     assert p.exists(),p
     with archive_reader(p) as z:
      x=pd.read_csv(io.BytesIO(z.read('reports/real_full_research_overlay_r1/real_full_research_overlay.csv')),dtype={'code':str})
      y=pd.read_csv(io.BytesIO(z.read('reports/real_full_trust_source.csv')),dtype={'code':str})
      assert len(x)==len(y)==15,(datestr,x.shape,y.shape)
      asof=pd.to_datetime(datestr,format='%Y%m%d').strftime('%Y-%m-%d')
      assert y.signal_date.eq(asof).all(),datestr
      assert set(x.code)==set(y.code),(datestr,'code set mismatch')
      x['signal_date']=asof
      x['origin_run_id']=run_id
      x['origin_archive_sha256']=source_sha(p,next(x['zip_sha256'] for x in P['daily_sources'] if x['signal_date']==datestr))
      x['code']=x.code.astype(str).str.strip().str.upper()
      assert x.code.str.fullmatch('[A-Z0-9]{6}').all()
      FROZEN.append(x)
      PINS.append({'source':f'RealFull_HIST_{datestr}.zip','run_id':run_id,'sha256':source_sha(p,next(x['zip_sha256'] for x in P['daily_sources'] if x['signal_date']==datestr)),'rows':len(x)})
    label=pd.concat(FROZEN,ignore_index=True)
    assert len(label)==180 and not label.duplicated(['signal_date','code']).any()
    assert label.research_level.value_counts().to_dict()=={'🟡 연구관찰':115,'🟢 연구흐름 좋음':52,'🔴 연구상 주의':13}
    label['grade']=label.research_level.map({'🟢 연구흐름 좋음':'GREEN','🟡 연구관찰':'YELLOW','🔴 연구상 주의':'RED'})
    features=['gradual_accumulation','liquidity_retained','ma_compression','strong_wave1','shallow_pullback','supply_drying']
    for s in features:label[s]=label[s].astype(str).str.lower().eq('true')
    label['ma224_below']=label.ma224_phase.eq('BELOW')
    label['wave_plus_shallow']=label.strong_wave1 & label.shallow_pullback
    label['compression_plus_shallow']=label.ma_compression & label.shallow_pullback
    label['original_thesis_full_proxy']=label.ma224_below & label.gradual_accumulation & label.wave_plus_shallow
    for c in ['close_vs_ma224_pct','close_vs_ma448_pct','wave1_pct','pullback_pct']:
     label[c]=pd.to_numeric(label[c],errors='coerce')
    with archive_reader(R143_SOURCE) as z:
     seed=pd.read_csv(io.BytesIO(z.read('real_full_r1c1_r143_seed_ledger.csv')),dtype={'code':str})
     outcome=pd.read_csv(io.BytesIO(z.read('real_full_r1c1_r143_outcome_ledger.csv')),dtype={'code':str})
    assert len(seed)==240 and len(outcome)==705
    assert not seed.duplicated(['signal_date','code']).any()
    assert not outcome.duplicated(['signal_date','code','horizon_td']).any()
    seed_selected=seed[['signal_date','code','event_key','r1c1_shadow_state','pb_volume_vs_wave1','signal_close_vs_ma224_pct','observer_metric_status','ma224_status']]
    exact=label.merge(seed_selected,on=['signal_date','code'],how='left',validate='one_to_one',indicator='r143_seed_join')
    assert exact['event_key'].notna().sum()==150
    path=exact.merge(outcome[['signal_date','code','horizon_td','horizon_date','close_ret_pct','mfe_pct','mae_pct','hit_plus5','wave1_high_retested','pullback_low_breached']],on=['signal_date','code'],how='left',validate='one_to_many')
    for c in ['horizon_td','close_ret_pct','mfe_pct','mae_pct']:
     path[c]=pd.to_numeric(path[c],errors='coerce')
    qz=type('ArchiveAdapter',(),{'read':lambda self,name:read_member(R1D13_SOURCE,name)})()
    qa=pd.read_csv(io.BytesIO(qz.read('reports/r1d13/result/R1D13_EVENT_QUALITY_UNION.csv')),dtype={'code':str})
    qa=qa[['snapshot_date','code','qa_any','quality_status','n_cross_mismatch','qa_identity']].rename(columns={'snapshot_date':'signal_date'})
    path=path.merge(qa,on=['signal_date','code'],how='left',validate='many_to_one')
    path['qa_coverage']=np.where(path.quality_status.notna(),'QA_FLAG_RECORDED','QA_NOT_CONNECTED')
    sub=path[path.horizon_td.notna()].copy()
    assert len(sub)==345 and sub.horizon_td.value_counts().to_dict()=={1.0:135,3.0:105,5.0:75,10.0:30}
    sub['win_gt0']=sub.close_ret_pct>0

    def grp(df,keys):
     g=df.groupby(keys,dropna=False).agg(n=('close_ret_pct','size'),wins=('win_gt0','sum'),median_pct=('close_ret_pct','median'),mean_pct=('close_ret_pct','mean')).reset_index()
     g['win_pct']=(g.wins/g.n*100).round(4)
     return g
    score=grp(sub,['horizon_td','grade'])
    feature_rows=[]
    for h in [5,10]:
     d=sub[sub.horizon_td.eq(h)]
     for f in features+['ma224_below','wave_plus_shallow','compression_plus_shallow','original_thesis_full_proxy']:
      gg=grp(d,[f]);gg.insert(0,'feature',f);gg['feature_value']=gg[f];gg=gg.drop(columns=f);gg.insert(0,'horizon_td',h)
      feature_rows.append(gg)
    features_score=pd.concat(feature_rows,ignore_index=True)
    within=[]
    for h in [5,10]:
     for grade in ['GREEN','YELLOW','RED']:
      d=sub[sub.horizon_td.eq(h)&sub.grade.eq(grade)]
      for f in ['ma_compression','shallow_pullback','gradual_accumulation','liquidity_retained','ma224_below','compression_plus_shallow']:
       gg=grp(d,[f]);gg.insert(0,'feature',f);gg['feature_value']=gg[f];gg=gg.drop(columns=f);gg.insert(0,'grade',grade);gg.insert(0,'horizon_td',h);within.append(gg)
    within=pd.concat(within,ignore_index=True)
    # Avoid false independent-event confidence: matched daily medians count one per date
    matched=[]
    for h in [5,10]:
     d=sub[sub.horizon_td.eq(h)]
     for key in ['ma_compression','shallow_pullback','liquidity_retained','gradual_accumulation','ma224_below','strong_wave1']:
      for day, group in d.groupby('signal_date'):
       yes=group[group[key].eq(True)];no=group[group[key].eq(False)]
       if len(yes)==0 or len(no)==0:continue
       matched.append({'horizon_td':h,'signal_date':day,'feature':key,'n_yes':len(yes),'n_no':len(no),'win_yes_pct':100*yes.win_gt0.mean(),'win_no_pct':100*no.win_gt0.mean(),'median_yes_pct':yes.close_ret_pct.median(),'median_no_pct':no.close_ret_pct.median(),'median_gap_yes_minus_no_pp':yes.close_ret_pct.median()-no.close_ret_pct.median()})
     for day, group in d.groupby('signal_date'):
      yes=group[group.grade.eq('GREEN')];no=group[group.grade.eq('YELLOW')]
      if len(yes)==0 or len(no)==0:continue
      matched.append({'horizon_td':h,'signal_date':day,'feature':'GREEN_VS_YELLOW','n_yes':len(yes),'n_no':len(no),'win_yes_pct':100*yes.win_gt0.mean(),'win_no_pct':100*no.win_gt0.mean(),'median_yes_pct':yes.close_ret_pct.median(),'median_no_pct':no.close_ret_pct.median(),'median_gap_yes_minus_no_pp':yes.close_ret_pct.median()-no.close_ret_pct.median()})
    matched=pd.DataFrame(matched)
    # Pair across horizons: same event only
    pair=sub[sub.horizon_td.isin([5,10])].pivot(index=['signal_date','code'],columns='horizon_td',values='close_ret_pct').dropna().reset_index();pair.columns=['signal_date','code','d5_pct','d10_pct'];pair['change_d10_minus_d5_pp']=pair.d10_pct-pair.d5_pct
    pair=pair.merge(label[['signal_date','code','grade','ma_compression','shallow_pullback','ma224_below','gradual_accumulation']],on=['signal_date','code'],validate='one_to_one')
    # Each output saved as record of prior frozen observations (no new trading rule)
    label.to_csv(OUT/'R1D17_HISTORIC_GRADE_180_SIGNALS.csv',index=False,encoding='utf-8-sig')
    sub.to_csv(OUT/'R1D17_R143_MATCHED_HORIZON_345_ROWS.csv',index=False,encoding='utf-8-sig')
    score.to_csv(OUT/'R1D17_GRADE_WINRATES.csv',index=False,encoding='utf-8-sig')
    features_score.to_csv(OUT/'R1D17_HISTORIC_FEATURE_MARGINAL_EXPLORATORY.csv',index=False,encoding='utf-8-sig')
    within.to_csv(OUT/'R1D17_FEATURE_WITHIN_GRADE.csv',index=False,encoding='utf-8-sig')
    matched.to_csv(OUT/'R1D17_SAME_DATE_MEDIAN_CONTRAST.csv',index=False,encoding='utf-8-sig')
    pair.to_csv(OUT/'R1D17_PAIRED_D5_TO_D10_GIVEBACK.csv',index=False,encoding='utf-8-sig')
    manifest={'authority':'RESEARCH_ONLY','not_new_oos':True,'frozen_archives':PINS,'r143_sha256':P['pinned_sources']['r143']['zip_sha256'],'r1d13_sha256':P['pinned_sources']['r1d13']['zip_sha256'],'historical_graded_signal_rows':len(label),'r143_matched_seed_signals':int(exact.event_key.notna().sum()),'matured_outcome_horizon_rows':len(sub),'n_d5_mature':int(sub.horizon_td.eq(5).sum()),'n_d10_mature':int(sub.horizon_td.eq(10).sum()),'production_unchanged':True,'orders_placed':0,'limitations':['proxy amount Close*Volume, not actual turnover','R143 price paths not certified historic as-used','only 5 dates D5/2 dates D10','multiple examined features nonindependent','same ticker repeated','no historic grade for 9/17–9/28']}
    (OUT/'R1D17_MANIFEST.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')

    return manifest

if __name__=='__main__':
    ap=argparse.ArgumentParser()
    ap.add_argument('--archives',required=True)
    ap.add_argument('--r143',required=True)
    ap.add_argument('--r1d13',required=True)
    ap.add_argument('--out',required=True)
    ap.add_argument('--protocol',required=True)
    ap.add_argument('--repo-root',default='.')
    a=ap.parse_args()
    m=analysis(a.archives,a.r143,a.r1d13,a.out,a.protocol,a.repo_root)
    assert m['orders_placed']==0 and m['historical_graded_signal_rows']==180 and m['matured_outcome_horizon_rows']==345
    print('R1D17_READONLY_FROZEN_REPRODUCTION_PASS',{'grades':m['historical_graded_signal_rows'],'matured_rows':m['matured_outcome_horizon_rows'],'orders_placed':m['orders_placed']})
