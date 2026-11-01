"""R1-D99 preliminary frozen-artifact audit; never claims a full OHLCV rerun.

Usage:
 python research_projects/r1d99/offline_evidence_check.py \\
   --evidence-dir /path/to/R1D98/results --output-dir /path/to/output
"""
from __future__ import annotations
from pathlib import Path
from datetime import date
import argparse,json,sys
import pandas as pd
from recency_dedup import presentation_groups


def normalized(records):
    return [dict(row) for row in records]


def run(evidence_dir,output_dir):
    d=Path(evidence_dir);out=Path(output_dir);out.mkdir(parents=True,exist_ok=True)
    full=pd.read_csv(d/'R1D98_ALL_LINES_CLASSIFIED.csv',dtype={'code':str},keep_default_na=False)
    p=pd.read_csv(d/'R1D98_NEW_PRIMARY_LINES.csv',dtype={'code':str},keep_default_na=False)
    w=pd.read_csv(d/'R1D98_NEW_WATCH_LINES.csv',dtype={'code':str},keep_default_na=False)
    selected=pd.concat([p,w],ignore_index=True)
    if full.shape[0]!=111 or selected.shape[0]!=17 or w.shape[0]!=7:
        # R1D98 fixed artifact expected; reject silently different baselines.
        raise AssertionError('Unexpected R1D98 artifact scope/count; inspect source before comparison')
    if selected.line_id.duplicated().any():
        # As-of stock-specific identification needed in general (line_id reused across codes).
        if selected.duplicated(subset=['code','asof','line_id']).any():raise AssertionError('duplicate selected ID')
    all_ids=set(zip(full.code,full['asof'],full.line_id))
    ids=set(zip(selected.code,selected['asof'],selected.line_id))
    if not ids.issubset(all_ids):raise AssertionError('selected does not match complete candidate data')
    for kind,data in [('PRIMARY',p),('WATCH',w)]:
        if not all(data.bucket==kind):raise AssertionError('R98 bucket changed')
    # Snapshot cannot compute exact contact ages because full raw OHLCV is not embedded.
    rows=[]
    for r in normalized(full.to_dict('records')):
        first=str(r.get('first_event_date') or '')
        last=str(r.get('last_event_date') or '')
        asof=date.fromisoformat(str(r['asof']))
        def age_calendar(x):
            return (asof-date.fromisoformat(x)).days if x else None
        rows.append(dict(r,d99_bucket=r['bucket'],d99_status=r['status'],d99_kept_without_expiry=True,
                         d99_first_event_calendar_days=age_calendar(first),
                         d99_last_cross_calendar_days=age_calendar(last),
                         d99_tradebar_history_requires_raw_ohlcv=True))
    groups=presentation_groups(normalized(selected.to_dict('records')))
    case=[]
    for code,name in [('039840','디오'),('071840','롯데하이마트'),('006400','삼성SDI')]:
        cc=full[full.code==code]; sp=p[p.code==code]; sw=w[w.code==code]
        case.append(dict(code=code,name=name,all_candidates=len(cc),r98_primary=len(sp),r99_primary=len(sp),
                         r98_watch=len(sw),r99_watch=len(sw),
                         all_watch_candidates=sum(cc.bucket=='WATCH'),
                         watch_expired=0,
                         display_groups=sum(v['is_display_representative'] for v in groups if v['code']==code)))
    pd.DataFrame(rows).to_csv(out/'R1D99_FROZEN_111_LINES_UNCHANGED.csv',index=False,encoding='utf-8-sig')
    selected.to_csv(out/'R1D99_FROZEN_SELECTED_17_UNCHANGED.csv',index=False,encoding='utf-8-sig')
    pd.DataFrame(groups).to_csv(out/'R1D99_DISPLAY_GROUPS_NO_REMOVAL.csv',index=False,encoding='utf-8-sig')
    summary=dict(mode='FROZEN_R1D98_ARTIFACT_ONLY',not_raw_ohlcv_rerun=True,
                 status='PRELIMINARY_SNAPSHOT_AUDIT_ONLY',cases=case,
                 original_full_candidates=len(full),preserved_full_candidates=len(rows),
                 original_watch_selected=len(w),preserved_watch_selected=sum(r['r99_watch'] for r in case),
                 expired_watch=0,
                 limitations=['Full OHLCV is required to compute all historical touches and trading-bar event ages',
                              'Similarity grouping changes display only',
                              'No full-market, forward performance, or trading result validated'])
    (out/'R1D99_SNAPSHOT_AUDIT.json').write_text(json.dumps(summary,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps(summary,ensure_ascii=False,indent=2))
    return summary

if __name__=='__main__':
    x=argparse.ArgumentParser();x.add_argument('--evidence-dir',required=True);x.add_argument('--output-dir',required=True)
    args=x.parse_args();run(args.evidence_dir,args.output_dir)
