#!/usr/bin/env python3
"""Read-only source-code contract ledger for two scanned lecture PDFs.

This is a *semantic non-equivalence* audit, not code execution, trade advice,
or a claim that the lectures' discretionary rules have been implemented.
"""
from __future__ import annotations

import argparse
import ast
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

SCANNER = 'Closing_bet_scanner_v2.py'
ANALYZER = 'analyze_single_stock.py'
RESEARCH = 'research_projects/closebet_source_parity_20261117/envelope_lower_parity.py'
REVISION = 'CB_ENV_LECTURE_VS_CODE_DEFINITION_AUDIT_R2'


def _function(tree: ast.AST, name: str) -> ast.FunctionDef:
    funcs = [x for x in getattr(tree, 'body', [])
             if isinstance(x, ast.FunctionDef) and x.name == name]
    if len(funcs) != 1:
        raise ValueError('FUNCTION_MISSING_OR_DUPLICATED:' + name)
    return funcs[0]


def _calls(fn: ast.FunctionDef, name: str) -> set[tuple[int, int]]:
    found = set()
    for node in ast.walk(fn):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == name and len(node.args) >= 3
                and all(isinstance(x, ast.Constant) and isinstance(x.value, (int, float))
                        for x in node.args[1:3])):
            found.add((int(node.args[1].value), int(node.args[2].value)))
    return found


def _const(tree: ast.AST, name: str) -> float | None:
    vals = [x.value.value for x in getattr(tree, 'body', [])
            if isinstance(x, ast.Assign) and isinstance(x.value, ast.Constant)
            and any(isinstance(y, ast.Name) and y.id == name for y in x.targets)
            and isinstance(x.value.value, (int, float))]
    return float(vals[0]) if len(vals) == 1 else None


def inspect(repo: Path) -> dict:
    texts = {f: (repo / f).read_text(encoding='utf-8') for f in (SCANNER, ANALYZER, RESEARCH)}
    trees = {f: ast.parse(t) for f, t in texts.items()}
    scanner_fn = _function(trees[SCANNER], '_check_envelope_bottom')
    scanner_calls = _calls(scanner_fn, '_calc_envelope')
    fn_text = ast.get_source_segment(texts[SCANNER], scanner_fn) or ''
    research_fn = _function(trees[RESEARCH], 'analyze')
    research_text = ast.get_source_segment(texts[RESEARCH], research_fn) or ''
    scanner_definition_recognized = (
        {(20, 10), (40, 10)} <= scanner_calls
        and 'close - lower20' in fn_text
        and "'env20_near'" in fn_text
        and '-2.0 <= env20_pct <= 2.0' in fn_text)
    research_daily_definition_recognized = (
        'lower20 = ma20 * .8' in research_text
        and 'touch20 = (g.low <= lower20)' in research_text
        and "'intraday_first_touch_price': 'UNKNOWN'" in research_text)
    analyzer_pct = _const(trees[ANALYZER], 'ENV20_PCT')
    scanner_amount_proxy_detected = (
        "df['Close'] * df['Volume']" in texts[SCANNER]
        and "df['Amount'] = _amount_raw.fillna" in texts[SCANNER])

    contracts = [
        {
            'id': 'ENV_BAND_WIDTH', 'lecture': '2026-11-02 엔벨하단 p2: MA20 x 0.80',
            'existing_scanner': 'MA20 x 0.90; MA40 x 0.90',
            'status': ('CONFIRMED_DEFINITION_MISMATCH' if scanner_definition_recognized
                       else 'SOURCE_CHANGED_REQUIRES_MANUAL_REAUDIT'),
            'research_daily_status': ('MA20_X_0_80_POSTCLOSE_ONLY' if research_daily_definition_recognized
                                      else 'RESEARCH_SOURCE_CHANGED_REQUIRES_MANUAL_REAUDIT'),
        },
        {
            'id': 'ENV_TOUCH_TIME', 'lecture': '차트 하단 접촉과 1차 매수',
            'existing_scanner': '확정 종가와 10% 밴드 이격 ±2%; 장중 최초 터치 아님',
            'status': ('CONFIRMED_EVENT_DEFINITION_MISMATCH' if scanner_definition_recognized
                       else 'SOURCE_CHANGED_REQUIRES_MANUAL_REAUDIT'),
            'intraday_execution_proven': False,
        },
        {
            'id': 'ENV_ANALYZER_ONLY', 'lecture': 'MA20 x 0.80',
            'existing_analyzer_config_pct': analyzer_pct,
            'status': ('BAND_WIDTH_ONLY_MATCH_NO_EXECUTION_EQUIVALENCE' if analyzer_pct == 20.0
                       else 'ANALYZER_BAND_CHANGED_REQUIRES_MANUAL_REAUDIT'),
        },
        {
            'id': 'ENV_POSITION_MANAGEMENT',
            'lecture': '2026-11-02 p2: 40/30/30, later 7-10% steps, last-stop 5%, half exit at +7%',
            'existing_research': 'daily touch annotations; no order, fill, sizing, or exit engine',
            'status': 'NOT_IMPLEMENTED_NOT_BACKTESTABLE_WITH_DAILY_TOUCH_ALONE',
            'entry_reference_and_exit_basis_ambiguities_remain': True,
        },
        {
            'id': 'ENV_HISTORIC_CONSTITUENTS', 'lecture': '2026-11-02 p2-4: KOSPI200 + quality/news/sector',
            'existing_research': 'as-of members optional; missing membership remains UNKNOWN',
            'status': 'PIT_MEMBERSHIP_AND_QUALITATIVE_CONTEXT_NOT_CERTIFIED',
        },
        {
            'id': 'CLOSEBET_FAMILIES',
            'lecture': '2026-09-28 p2-5: bottom+money, after-high pullback, sector-leader intraday are distinct',
            'existing_scanner': 'many historical strategy branches; no one-to-one full lecture equivalence audited',
            'status': 'SEPARATE_STRATEGY_DEFINITIONS_REQUIRED',
        },
        {
            'id': 'CLOSEBET_AMOUNT500', 'lecture': '2026-09-28 p2: bottom-zone 500억원+ context',
            'existing_scanner': 'some input paths fill Amount with Close*Volume proxy',
            'status': ('TRUE_AMOUNT_GATE_UNCERTIFIED_PROXY_PATH_DETECTED' if scanner_amount_proxy_detected
                       else 'SOURCE_CHANGED_REQUIRES_MANUAL_REAUDIT'),
            'historic_baseline_versus_absolute_threshold_distinct': True,
        },
        {
            'id': 'CLOSEBET_NEXT_OPEN', 'lecture': '2026-09-28 p3,5: sector leader D+0/D+1 fills, open gap, exit',
            'existing_research': 'D0 OHLCVA snapshots, not timestamped 09:05/09:30 fills',
            'status': 'INTRADAY_NOT_VALIDATABLE_WITH_DAILY_BARS',
        },
    ]
    return {
        'revision': REVISION, 'observed_at_utc': datetime.now(timezone.utc).isoformat(),
        'research_only': True, 'no_source_files_modified': True,
        'inputs': [{'path': k, 'sha256': hashlib.sha256(v.encode('utf-8')).hexdigest()}
                   for k, v in texts.items()],
        'lecture_documents': [
            {'path_in_project_sources': '16-260928-.pdf', 'pages_consulted': [2, 3, 4, 5]},
            {'path_in_project_sources': '19-261102-.pdf', 'pages_consulted': [2, 3, 4]},
        ],
        'scanner_envelope_call_period_pct': [list(x) for x in sorted(scanner_calls)],
        'scanner_amount_fallback_detected': scanner_amount_proxy_detected,
        'definitions_reconciled_for_real_trading': False,
        'can_publish_strategy_performance': False,
        'comparison_performance_status': 'NOT_COMPUTED',
        'live_search_watch_order_changed': False,
        'contracts': contracts,
    }


def run(repo: Path, out: Path) -> dict:
    result = inspect(repo)
    out.mkdir(parents=True, exist_ok=True)
    (out / 'LECTURE_CONTRACT_AUDIT_RESEARCH_ONLY.json').write_text(
        json.dumps(result, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps({'revision': result['revision'],
                      'contract_statuses': {v['id']: v['status'] for v in result['contracts']},
                      'can_publish_strategy_performance': False}, ensure_ascii=False, indent=2))
    return result


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repo', default='.')
    p.add_argument('--out', required=True)
    args = p.parse_args()
    run(Path(args.repo), Path(args.out))


if __name__ == '__main__':
    main()
