from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys

import Closing_bet_scanner_v2 as s


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser(description='v49.66 immutable universe + search preflight')
    ap.add_argument('--end-date', required=True)
    ap.add_argument('--output-dir', default='v49_66_prepare_output')
    args = ap.parse_args()
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Build once. Every shard consumes this exact document rather than rebuilding a drifting universe.
    s._refresh_top_mcap_set(s.TOP_N)
    s._refresh_marcap_map()
    s._refresh_index_map()
    codes, meta = s._v4948_build_backtest_universe(args.end_date)
    codes = sorted(set(s._normalize_code(c) for c in codes if s._normalize_code(c)))
    if str((meta or {}).get('status', '')).upper() != 'VALID' or not codes:
        raise RuntimeError(f'invalid universe: {meta}')
    name_map = s._build_name_map_for_codes(codes)
    # v49.66: immutable entry-time classification maps. These are labels only; no future data.
    market_map, sector_map = {}, {}
    try:
        listing = s._get_krx_listing()
        if listing is not None and not listing.empty:
            listing = listing.copy()
            listing['Code'] = listing['Code'].astype(str).str.zfill(6)
            mcol = next((c for c in ['Market','MarketId','시장구분','Exchange'] if c in listing.columns), None)
            scol = next((c for c in ['Sector','Industry','업종','업종명','Dept'] if c in listing.columns), None)
            if mcol:
                market_map = dict(zip(listing.loc[listing.Code.isin(codes),'Code'].astype(str).str.zfill(6), listing.loc[listing.Code.isin(codes),mcol].fillna('').astype(str)))
            if scol:
                sector_map = dict(zip(listing.loc[listing.Code.isin(codes),'Code'].astype(str).str.zfill(6), listing.loc[listing.Code.isin(codes),scol].fillna('').astype(str)))
    except Exception as exc:
        print(f'v49.66 listing classification map fallback: {type(exc).__name__}: {exc}')

    # KRX secret aliases are synchronized by the scanner import. Never print credential values.
    krx_credentials_detected, krx_id_detected, krx_pw_detected = s._v53832_detect_krx_credentials()
    print(
        'KRX CREDENTIAL ENV ' + ('DETECTED' if krx_credentials_detected else 'MISSING') +
        f' · ID {"O" if krx_id_detected else "X"} · PW {"O" if krx_pw_detected else "X"}'
    )

    # Technical preflight is executed once, outside performance shards.
    contract_ok, contract_rows = s._v4958_validate_search_contract(s._V4958_SEARCH_SPEC_DOC)
    explicit_rows = s._v4958_explicit_hist_contract_rows()
    boundary_rows = s._v4958_behavior_boundary_rows()
    runtime = s._v4958_search_thread_determinism_audit()
    explicit_fail = sum(1 for r in explicit_rows if r.get('status') != 'PASS')
    boundary_fail = sum(1 for r in boundary_rows if r.get('status') != 'PASS')
    thread_fail = int(runtime.get('thread_failures', 0) or 0)
    determinism_fail = int(runtime.get('determinism_failures', 0) or 0)
    preflight_status = 'VALID' if contract_ok and explicit_fail == 0 and boundary_fail == 0 and thread_fail == 0 and determinism_fail == 0 else 'INVALID'

    universe_doc = {
        'version': s.CLOSING_BET_SCANNER_VERSION,
        'requested_end_date': args.end_date,
        'codes': codes,
        'names': {c: s._clean_stock_name(c, name_map.get(c, c)) for c in codes},
        'universe_meta': {**dict(meta or {}), 'count': len(codes), 'fingerprint': s._v4948_universe_fingerprint(codes)},
        'index_map': {c: str((s.INDEX_MAP or {}).get(c, '')) for c in codes},
        'marcap_map': {c: float((s.MARCAP_MAP or {}).get(c, 0) or 0) for c in codes},
        'top_mcap_codes': sorted(set(c for c in codes if c in set(s.TOP_MCAP_SET or set()))),
        'market_map': {c: str(market_map.get(c, '')) for c in codes},
        'sector_map': {c: str(sector_map.get(c, '')) for c in codes},
    }
    upath = out / 'v49_66_universe.json'
    upath.write_text(json.dumps(universe_doc, ensure_ascii=False, indent=2, default=str), encoding='utf-8')

    preflight = {
        'version': s.CLOSING_BET_SCANNER_VERSION,
        'status': preflight_status,
        'spec_version': s.CLOSING_BET_V4958_SEARCH_AUDIT_SPEC_VERSION,
        'evaluator_version': s.CLOSING_BET_V4958_COMMON_EVALUATOR_VERSION,
        'contract_valid': bool(contract_ok),
        'contract_rows': contract_rows,
        'explicit_hist_failures': explicit_fail,
        'boundary_failures': boundary_fail,
        'thread_isolation_failures': thread_fail,
        'determinism_failures': determinism_fail,
        'thread_rows': runtime.get('thread_rows', []),
        'determinism_rows': runtime.get('determinism_rows', []),
        'universe_count': len(codes),
        'universe_fingerprint': s._v4948_universe_fingerprint(codes),
        'universe_sha256': sha256_file(upath),
        'krx_credentials_detected': bool(krx_credentials_detected),
        'krx_id_detected': bool(krx_id_detected),
        'krx_password_detected': bool(krx_pw_detected),
        'krx_password_value_logged': False,
        'paper_only': True,
        'real_orders': 0,
    }
    ppath = out / 'v49_66_preflight.json'
    ppath.write_text(json.dumps(preflight, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    print(f"PREPARE UNIVERSE VALID · n {len(codes)} · fp {preflight['universe_fingerprint']}")
    print(f"SEARCH PREFLIGHT {preflight_status} · contract {contract_ok} · explicit {explicit_fail} · boundary {boundary_fail} · thread {thread_fail} · determinism {determinism_fail}")
    return 0 if preflight_status == 'VALID' else 2


if __name__ == '__main__':
    sys.exit(main())
