#!/usr/bin/env python3
"""Independent Samsung SDI price-lineage precheck; RESEARCH_ONLY.

The 2025 ex-rights reference-price ratio is a *candidate explanation* for a
2023 HTS screenshot, not proof of its historical adjustment algorithm.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FROZEN = ROOT / "research_projects/r1d97/data/006400_KRX.csv"
FROZEN_SHA256 = "d0e1ec07da7972728b84865c54312e387db20c3e3dd9dadc813dada394635566"
KIS_PERIOD_ENDPOINT = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
FOCAL_DATES = ("2023-09-19", "2023-10-18", "2023-11-02", "2023-11-03", "2023-11-06")
OHLC = ("open", "high", "low", "close")
HTS_20231106 = {"open": 467962, "high": 511038, "low": 455725, "close": 495374}
EX_RIGHTS = {"date": "2025-04-10", "pre_rights_close": 171700, "reference_price": 168100}
LINE_ID = "TS_BODY_WICK_PROXY|BEAR_BODY_TOP|2023-09-19|2023-10-18|UPPER"


class ProvenanceError(ValueError):
    pass


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rows(path: Path, *, pinned: bool = False) -> dict[str, dict]:
    if pinned and sha256(path) != FROZEN_SHA256:
        raise ProvenanceError("FROZEN_KRX_SOURCE_HASH_CHANGED")
    result = {}
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or not {"date", *OHLC}.issubset(reader.fieldnames):
            raise ProvenanceError("OHLC_COLUMNS_MISSING")
        for rec in reader:
            date = rec["date"].strip()
            if date in result:
                raise ProvenanceError("DUPLICATE_DATE " + date)
            if "code" in rec and rec["code"].strip() != "006400":
                raise ProvenanceError("WRONG_STOCK " + date)
            try:
                datetime.strptime(date, "%Y-%m-%d")
                result[date] = {field: int(rec[field]) for field in OHLC}
            except (ValueError, TypeError) as exc:
                raise ProvenanceError("INVALID_BAR " + date) from exc
            bar = result[date]
            if min(bar.values()) <= 0 or bar["low"] > min(bar["open"], bar["close"]) or bar["high"] < max(bar["open"], bar["close"]):
                raise ProvenanceError("INVALID_OHLC " + date)
    return result


def verify_manifest(csv_path: Path, manifest_path: Path, expected_flag: str) -> dict:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    mandatory = {
        "provider": "KIS",
        "endpoint": KIS_PERIOD_ENDPOINT,
        "symbol": "006400",
        "period": "D",
        "fid_org_adj_prc": expected_flag,
        "csv_sha256": sha256(csv_path),
    }
    for field, expected in mandatory.items():
        if manifest.get(field) != expected:
            raise ProvenanceError("MANIFEST_MISMATCH " + field)
    captured = manifest.get("captured_at_utc", "")
    if not isinstance(captured, str) or not captured.endswith("Z"):
        raise ProvenanceError("CAPTURE_TIME_MISSING_OR_NOT_UTC")
    try:
        datetime.fromisoformat(captured.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ProvenanceError("CAPTURE_TIME_INVALID") from exc
    return {"provider": "KIS", "endpoint": KIS_PERIOD_ENDPOINT,
            "fid_org_adj_prc": expected_flag, "captured_at_utc": captured,
            "csv_sha256": mandatory["csv_sha256"],
            "note": "Manifest is user-supplied provenance; not a cryptographic API signature"}


def price_line(bars: dict, frozen: dict) -> dict:
    """Reconstruct *fixed-anchor* levels, not a fresh R116 active-line ranking."""
    calendar = sorted(frozen)
    indices = {date: i for i, date in enumerate(calendar)}
    first, second = FOCAL_DATES[:2]
    value1 = max(bars[first]["open"], bars[first]["close"])
    value2 = max(bars[second]["open"], bars[second]["close"])
    delta = indices[second] - indices[first]
    if delta <= 0:
        raise ProvenanceError("INVALID_ANCHOR_ORDER")
    slope = (value2 - value1) / delta
    levels = {d: value1 + slope * (indices[d] - indices[first]) for d in FOCAL_DATES[2:]}
    crossed = bars["2023-11-03"]["close"] <= levels["2023-11-03"] and bars["2023-11-06"]["close"] > levels["2023-11-06"]
    return {"anchor_body_top": {first: value1, second: value2},
            "slope_per_trading_bar": round(slope, 8),
            "levels": {d: round(v, 8) for d, v in levels.items()},
            "nov06_cross_from_nov03_close": crossed,
            "scope": "fixed_2023_anchor_dates_only_not_full_R116_active_line_replay"}


def audit(raw_csv: Path | None = None, adjusted_csv: Path | None = None,
          raw_manifest: Path | None = None, adjusted_manifest: Path | None = None) -> dict:
    frozen = rows(FROZEN, pinned=True)
    if any(date not in frozen for date in FOCAL_DATES):
        raise ProvenanceError("FOCAL_DATE_NOT_IN_FROZEN_KRX")
    candidate = EX_RIGHTS["reference_price"] / EX_RIGHTS["pre_rights_close"]
    result = {
        "revision": "R8_SAMSUNG_SDI_INDEPENDENT_PRICE_LINEAGE_RESEARCH_ONLY",
        "research_only": True, "can_modify_live_search_watch": False,
        "source_frozen_sha256": FROZEN_SHA256, "line_id_raw_R5": LINE_ID,
        "focal_dates": list(FOCAL_DATES),
        "2025_rights_issue_candidate": {
            **EX_RIGHTS, "reference_to_preclose_ratio": round(candidate, 12),
            "ratio_rounded_to_three_decimals": round(candidate, 3),
            "source_provenance": [
                "https://invest.kiwoom.com/inv/disclosure_detail?code=01&date=2025.04.09&keyword=%EC%82%BC%EC%84%B1SDI&seq=00036095&stockCode=&tp=&type=02",
                "https://dreago.kr/board/view/?gn=0&no=37347&sgn=3"],
            "causality_proven": False,
            "note": "Observed 0.979 is three-decimal agreement; exact ratio differs."},
        "screenshot_20231106": HTS_20231106,
        "screenshot_raw_times_0_979_residuals": {
            p: round(HTS_20231106[p] - frozen["2023-11-06"][p] * 0.979, 5)
            for p in OHLC},
        "exact_ex_rights_ratio_projected_20231106": {
            p: int(frozen["2023-11-06"][p] * candidate + 0.5) for p in OHLC},
        "screenshot_minus_exact_ex_rights_ratio_projection": {
            p: HTS_20231106[p] - int(frozen["2023-11-06"][p] * candidate + 0.5)
            for p in OHLC},
        "independent_status": "UNKNOWN_MISSING_BOTH_KIS_SERIES",
        "raw_source_agrees_on_five_dates": None,
        "adjustment_ratio_uniform": None,
        "adjusted_fixed_anchor_line_cross": None,
        "HTS_matches_independent_adjusted_OHLC": None,
        "r116_adjusted_full_replay_certified": False,
    }
    supplied = [raw_csv, adjusted_csv, raw_manifest, adjusted_manifest]
    if not any(supplied):
        return result
    if not all(supplied):
        raise ProvenanceError("BOTH_SERIES_AND_BOTH_MANIFESTS_REQUIRED")
    # The *dated* KIS API uses 1=raw, 0=adjusted. The recent-30-day
    # /inquire-daily-price endpoint documents opposite flag semantics.
    provenance = {
        "raw": verify_manifest(raw_csv, raw_manifest, "1"),
        "adjusted": verify_manifest(adjusted_csv, adjusted_manifest, "0"),
    }
    independent_raw, independent_adjusted = rows(raw_csv), rows(adjusted_csv)
    result["provenance"] = provenance
    missing = {series: [date for date in FOCAL_DATES if date not in bars]
               for series, bars in (("raw", independent_raw), ("adjusted", independent_adjusted))}
    result["missing_focal_dates"] = missing
    if any(missing.values()):
        result["independent_status"] = "UNKNOWN_MISSING_REQUIRED_DATES"
        return result
    mismatches = {date: {p: {"frozen": frozen[date][p], "independent": independent_raw[date][p]}
                         for p in OHLC if frozen[date][p] != independent_raw[date][p]}
                  for date in FOCAL_DATES}
    mismatches = {d: fields for d, fields in mismatches.items() if fields}
    result["raw_source_discrepancies"] = mismatches
    result["raw_source_agrees_on_five_dates"] = not bool(mismatches)
    if mismatches:
        result["independent_status"] = "RAW_SOURCE_DISAGREEMENT"
        return result
    ratios = {d: {p: independent_adjusted[d][p] / independent_raw[d][p]
                  for p in OHLC} for d in FOCAL_DATES}
    values = [factor for day in ratios.values() for factor in day.values()]
    spread = max(values) - min(values)
    # Predeclared descriptive tolerance, not adjusted to fit observed data.
    uniform = spread <= 0.0001
    result["adjusted_to_raw_ratio_by_date_ohlc"] = ratios
    result["ratio_spread_max_minus_min"] = spread
    result["ratio_uniform_tolerance"] = 0.0001
    result["adjustment_ratio_uniform"] = uniform
    result["independent_adjusted_line"] = price_line(independent_adjusted, frozen)
    result["adjusted_fixed_anchor_line_cross"] = result["independent_adjusted_line"]["nov06_cross_from_nov03_close"]
    result["HTS_minus_independent_adjusted_20231106"] = {
        p: HTS_20231106[p] - independent_adjusted["2023-11-06"][p] for p in OHLC}
    result["HTS_matches_independent_adjusted_OHLC"] = all(
        abs(v) <= 1 for v in result["HTS_minus_independent_adjusted_20231106"].values())
    result["independent_status"] = "FIVE_DATE_COMPARISON_COMPLETE_FULL_R116_REPLAY_NOT_DONE"
    return result


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", type=Path, required=True)
    for name in ("raw-csv", "adjusted-csv", "raw-manifest", "adjusted-manifest"):
        p.add_argument("--" + name, type=Path)
    args = p.parse_args()
    result = audit(args.raw_csv, args.adjusted_csv, args.raw_manifest, args.adjusted_manifest)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("R8_STATUS=" + result["independent_status"])


if __name__ == "__main__":
    main()
