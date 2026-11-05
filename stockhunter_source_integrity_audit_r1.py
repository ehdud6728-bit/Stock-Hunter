#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Stock-Hunter source integrity audit R1: offline/read-only, no production imports.

Inputs: an existing candidate JSON, optionally a previously captured KRX original-price
CSV.  The audit NEVER collects/repairs/substitutes prices, edits input, chooses stocks,
or calls any trading/Telegram code.  A CSV merely *claimed* to be KRX is not certified
by this script: provenance has to be established by upstream capture evidence.
"""
from __future__ import annotations
import argparse, csv, datetime as dt, hashlib, json, math, re
from collections import Counter
from pathlib import Path

VERSION = "STOCKHUNTER_SOURCE_INTEGRITY_AUDIT_R1_20261105"
CODE_KEYS = ("code", "종목코드", "Code", "ticker", "Ticker", "stock_code")
NAME_KEYS = ("종목명", "name", "Name")
PRICE_KEYS = ("현재가", "snapshot_price", "종가", "Close", "close", "price", "Price")
OHLC = ("open", "high", "low", "close", "volume", "amount")


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def first(row, keys):
    for key in keys:
        if key in row and row[key] is not None and str(row[key]).strip() not in ("", "nan", "None"):
            return row[key]
    return None


def code(raw):
    s = str(raw or "").strip().upper()
    if s.endswith(".0") and s[:-2].isdigit(): s = s[:-2]
    for suffix in (".KS", ".KQ", ".KRX"):
        if s.endswith(suffix): s = s[:-len(suffix)]; break
    if len(s) == 7 and s.startswith("A"): s = s[1:]
    if s.isdigit() and 1 <= len(s) < 6: s = s.zfill(6)
    if not re.fullmatch(r"[0-9][0-9A-Z]{5}", s or "") or s == "000000":
        return None
    return s


def number(raw):
    try:
        val = float(str(raw).replace(",", "").strip())
        return val if math.isfinite(val) else None
    except (TypeError, ValueError):
        return None


def date(raw):
    try:
        return dt.date.fromisoformat(str(raw)[:10]).isoformat()
    except (TypeError, ValueError):
        return None


def extract_candidates(obj):
    if isinstance(obj, list):
        return obj if all(isinstance(x, dict) for x in obj) else None
    if isinstance(obj, dict):
        for key in ("quick_candidates", "candidates", "today_candidates", "ai_candidates", "items", "rows", "data", "results", "stocks", "top15"):
            if key in obj:
                rows = extract_candidates(obj[key])
                if rows is not None: return rows
    return None


def read_krx_csv(path):
    if not path: return {}, {"status": "NOT_PROVIDED", "rows": 0}
    with open(path, encoding="utf-8-sig", newline="") as fh:
        all_rows = list(csv.DictReader(fh))
    need = {"date", "code", "open", "high", "low", "close", "volume", "amount"}
    if not all_rows or not need.issubset(all_rows[0]):
        return {}, {"status": "INVALID_CSV_SCHEMA_OR_EMPTY", "rows": len(all_rows)}
    d = {}; invalid = Counter()
    for row in all_rows:
        k = (date(row.get("date")), code(row.get("code")))
        if None in k:
            invalid["INVALID_CODE_OR_DATE"] += 1; continue
        if k in d:
            invalid["DUPLICATE_DATE_CODE"] += 1
            d[k] = None  # fail closed, including all later duplicates
            continue
        vals = {f: number(row.get(f)) for f in OHLC}
        if any(x is None or x < 0 for x in vals.values()):
            invalid["NONFINITE_OR_NEGATIVE_OHLCVA"] += 1; d[k] = None; continue
        if vals["high"] < vals["low"] or (vals["volume"] > 0 and not (
            vals["low"] <= vals["open"] <= vals["high"] and
            vals["low"] <= vals["close"] <= vals["high"]
        )):
            invalid["BAD_OHLC_RANGE"] += 1; d[k] = None; continue
        d[k] = vals
    return d, {"status": "CLAIMED_KRX_CSV_NOT_AUTHENTICATED", "rows": len(all_rows),
               "valid_keys": sum(x is not None for x in d.values()), "issues": dict(invalid),
               "sha256": digest(path)}


def audit(candidate_path, krx_path=None, snapshot_kind="UNKNOWN", price_basis="UNKNOWN",
          expected_date=None, out_dir="reports/source_integrity_r1"):
    obj = json.loads(Path(candidate_path).read_text(encoding="utf-8-sig"))
    rows = extract_candidates(obj)
    if rows is None: raise ValueError("CANDIDATE_SCHEMA_NOT_RECOGNIZED")
    snap_date = date(expected_date or (obj.get("date") if isinstance(obj, dict) else None))
    if not snap_date: raise ValueError("VALID_SIGNAL_DATE_REQUIRED")
    kmap, meta = read_krx_csv(krx_path)
    details, counter, seen = [], Counter(), set()
    for idx, row in enumerate(rows, 1):
        raw_code = first(row, CODE_KEYS)
        c = code(raw_code)
        name = first(row, NAME_KEYS) or ""
        p = number(first(row, PRICE_KEYS))
        findings = []
        if c is None: findings.append("INVALID_TICKER")
        if p is None or p <= 0: findings.append("MISSING_OR_NONPOSITIVE_PRICE")
        if c in seen and c is not None: findings.append("DUPLICATE_CANDIDATE_CODE")
        if c: seen.add(c)
        sdate = date(first(row, ("signal_date", "날짜", "date", "asof_date")))
        if sdate and sdate != snap_date: findings.append("SIGNAL_DATE_MISMATCH")
        match = "NOT_CHECKED"
        krx_close = None
        if krx_path and c:
            if meta["status"] == "INVALID_CSV_SCHEMA_OR_EMPTY":
                match = "INVALID_REFERENCE"
            elif kmap.get((snap_date, c), "MISSING") == "MISSING":
                match = "KRX_REFERENCE_MISSING"
            elif kmap[(snap_date, c)] is None:
                match = "KRX_REFERENCE_INVALID"
            else:
                krx_close = kmap[(snap_date, c)]["close"]
                if price_basis != "ORIGINAL":
                    match = "PRICE_BASIS_INCOMPARABLE"
                elif snapshot_kind != "EOD":
                    match = "TIME_BASIS_INCOMPARABLE"
                elif p is None or p <= 0:
                    match = "CANDIDATE_PRICE_INVALID"
                else:
                    match = "EXACT_MATCH" if p == krx_close else "CLOSE_MISMATCH"
                    if match != "EXACT_MATCH": findings.append("CANDIDATE_VS_KRX_CLOSE_MISMATCH")
        counter.update(findings)
        details.append({"row": idx, "raw_code": str(raw_code or ""), "code": c or "",
                        "name": str(name), "signal_date": sdate or "", "reference_date": snap_date,
                        "candidate_price": p, "krx_reference_close": krx_close,
                        "krx_comparison": match, "findings": "|".join(findings),
                        "status": "INVALID" if findings else ("COMPARABLE_MATCH" if match == "EXACT_MATCH" else "UNVERIFIED")})
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    columns = ("row", "raw_code", "code", "name", "signal_date", "reference_date",
               "candidate_price", "krx_reference_close", "krx_comparison", "findings", "status")
    with (out / "candidate_integrity.csv").open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns); writer.writeheader(); writer.writerows(details)
    compare_count = Counter(x["krx_comparison"] for x in details)
    summary = {"audit_version": VERSION, "mode": "RESEARCH_ONLY_READ_ONLY", "production_mutations": 0,
               "signal_date": snap_date, "snapshot_kind": snapshot_kind, "price_basis": price_basis,
               "candidate_sha256": digest(candidate_path), "candidate_rows": len(details),
               "invalid_rows": sum(bool(x["findings"]) for x in details), "findings": dict(counter),
               "krx_comparisons": dict(compare_count), "reference": meta,
               "conclusion": "SOURCE_PROVENANCE_NOT_CERTIFIED; EOD and original-price basis required for price parity; do not substitute prices",
               "input_files_unchanged": True}
    (out / "audit_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def self_test():
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        source = root / "candidates.json"
        source.write_text(json.dumps({"date": "2026-11-04", "quick_candidates": [
            {"code": "000000", "현재가": 100},
            {"code": "002900", "현재가": "6,270"},
            {"code": "006400", "현재가": 550000},
            {"code": "0126Z0", "현재가": 1000},
        ]}, ensure_ascii=False), encoding="utf-8")
        ref = root / "krx.csv"
        ref.write_text("date,code,open,high,low,close,volume,amount\n"
                       "2026-11-04,002900,6200,6350,6150,6270,100,625000\n"
                       "2026-11-04,006400,540000,555000,535000,545000,100,54500000\n"
                       "2026-11-04,0126Z0,950,1050,920,1000,100,100000\n", encoding="utf-8")
        a = audit(source, ref, "EOD", "ORIGINAL", out_dir=root / "out")
        assert a["findings"] == {"INVALID_TICKER": 1, "CANDIDATE_VS_KRX_CLOSE_MISMATCH": 1}, a
        assert a["krx_comparisons"]["EXACT_MATCH"] == 2, a
        assert a["krx_comparisons"]["CLOSE_MISMATCH"] == 1, a
        b = audit(source, ref, "INTRADAY", "ORIGINAL", out_dir=root / "out2")
        assert b["findings"] == {"INVALID_TICKER": 1} and "CLOSE_MISMATCH" not in b["krx_comparisons"], b
        assert code("0126Z0") == "0126Z0" and code("000000") is None
    print("SOURCE_INTEGRITY_AUDIT_R1_SELF_TEST_PASS")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--candidate-json", type=Path)
    p.add_argument("--krx-csv", type=Path, help="Optional previously captured original-price KRX CSV")
    p.add_argument("--snapshot-kind", choices=("UNKNOWN", "INTRADAY", "EOD"), default="UNKNOWN")
    p.add_argument("--price-basis", choices=("UNKNOWN", "ORIGINAL", "ADJUSTED"), default="UNKNOWN")
    p.add_argument("--signal-date", help="YYYY-MM-DD; default uses top-level JSON date")
    p.add_argument("--out-dir", default="reports/source_integrity_r1")
    p.add_argument("--self-test", action="store_true")
    args = p.parse_args()
    if args.self_test: return self_test()
    if not args.candidate_json: p.error("--candidate-json is required")
    result = audit(args.candidate_json, args.krx_csv, args.snapshot_kind, args.price_basis,
                   args.signal_date, args.out_dir)
    print(json.dumps(result, ensure_ascii=False, indent=2))

if __name__ == "__main__": main()
