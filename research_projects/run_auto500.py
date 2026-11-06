#!/usr/bin/env python3
"""Independent frozen 2022-asof 500-name R118 source -> original BLUE20 RSI -> six-factor research."""
from __future__ import annotations
import argparse, importlib.util, json, sys, traceback
from pathlib import Path
HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[1]
OLD=ROOT/'research_projects'/'blue20_rsi_delay_research'
R118=ROOT/'research_projects'/'r1d118'/'auto_data.py'
if str(OLD) not in sys.path:sys.path.insert(0,str(OLD))
if str(HERE) not in sys.path:sys.path.insert(0,str(HERE))

def import_r118():
    if not R118.is_file():raise RuntimeError('R118_PINNED_SOURCE_MODULE_MISSING')
    spec=importlib.util.spec_from_file_location('r118_multifactor_frozen',R118)
    x=importlib.util.module_from_spec(spec);spec.loader.exec_module(x);return x

def run(out,sector_pit='',theme_pit='',stoch='all'):
    # Optional fixed-path historical mappings. Example files are not loaded.
    if not sector_pit and (HERE/'inputs'/'sector_pit.csv').exists():
        sector_pit=str(HERE/'inputs'/'sector_pit.csv')
    if not theme_pit and (HERE/'inputs'/'theme_pit.csv').exists():
        theme_pit=str(HERE/'inputs'/'theme_pit.csv')
    from auto500_research import materialize_verified_500
    import blue20_rsi_delay
    import multifactor
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    audit=out/'RUN_STAGE.json';state={'research_only':True,'success':False,'stage':'INIT'}
    def save(stage,**kwargs):
        state.update(stage=stage,**kwargs);audit.write_text(json.dumps(state,ensure_ascii=False,indent=2),encoding='utf-8')
    save('LOAD_PINNED_2022_2024_DATA')
    try:
        r118=import_r118()
        years,indices,sha_sources=r118.load_pinned_sources(ROOT/'research_projects'/'r1d118'/'.local_source_cache')
        inp=r118.build_inputs(years,indices,HERE/'.auto500_work'/'r118_inputs',count=500,sha_sources=sha_sources)
        del years,indices
        save('CHECK_500_CODE_SHA256')
        merged=materialize_verified_500(inp['manifest'],inp['market'],HERE/'.auto500_work'/'merged')
        save('RUN_ORIGINAL_RSI_DELAY_RESEARCH')
        base=HERE/'.auto500_work'/'original_rsi'
        cli=['--ohlcv',merged['ohlcv'],'--calendar',merged['calendar'],'--out',str(base),
             '--start-year','2023','--end-year','2024']
        blue20_rsi_delay.main(cli)
        sourceaudit=json.loads((base/'RSI_DELAY_AUDIT_MANIFEST.json').read_text(encoding='utf-8'))
        if sourceaudit.get('prior_4232_2489_body_reference',{}).get('UPPER',{}).get('match') is not True or sourceaudit.get('prior_4232_2489_body_reference',{}).get('LOWER',{}).get('match') is not True:
            raise RuntimeError('ORIGINAL_BREAKOUT_REFERENCE_MISMATCH_FAIL_CLOSED')
        save('RUN_SIX_FACTOR_PIT_ANALYSIS')
        presets=['14,3,3','12,5,5','20,12,12'] if stoch=='all' else [stoch]
        outcomes={}
        for preset in presets:
            tag='stoch_'+preset.replace(',','_')
            subout=out/tag
            cli=['--events',str(base/'RSI_DELAY_EVENTS.csv'),
                '--manifest',str(inp['manifest']),'--market-daily',str(inp['market']),
                '--market-membership',str(inp['sector']), '--out',str(subout), '--stoch',preset,
                *(['--sector-pit',sector_pit] if sector_pit else []),
                *(['--theme-pit',theme_pit] if theme_pit else [])]
            outcomes[preset]=multifactor.main(cli)
            save('RUN_SIX_FACTOR_PIT_ANALYSIS',completed_presets=list(outcomes))
        (out/'STOCH_PRESET_AUDIT.json').write_text(json.dumps({
            'research_only':True,'presets':presets,
            'results':{k:{'primary_observed_lanes':v['primary_observed_lanes'],
               'sector_pit_coverage_pct':v['sector_pit_coverage_pct'],
               'theme_pit_coverage_pct':v['theme_pit_coverage_pct']} for k,v in outcomes.items()},
            'rsi_source_reproduced':True,'oos':False,'production_ready':False,
            'multiple_testing_warning':'Do not pick best Slow preset without holdout / OOS'}
            ,ensure_ascii=False,indent=2),encoding='utf-8')
        save('COMPLETE_RESEARCH_ONLY',success=True,
             original_reference_matched=True,
             full_sixway_conclusion_allowed=False,completed_presets=presets)
        return outcomes
    except Exception as exc:
        save('FAILED_CLOSED',error_type=type(exc).__name__,error=str(exc))
        traceback.print_exc();raise

if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--out',default=str(HERE/'results_auto500'))
    p.add_argument('--sector-pit',default='')
    p.add_argument('--theme-pit',default='')
    p.add_argument('--stoch',default='all',choices=['all','14,3,3','12,5,5','20,12,12'])
    a=p.parse_args()
    run(a.out,a.sector_pit,a.theme_pit,a.stoch)
