#!/usr/bin/env python3
"""KIS two-mode historical OHLC capture for R8; strictly RESEARCH_ONLY.

No trade endpoints, no BLUE-Signal-Lab credentials, no fallback price data.
Only dedicated Stock-Hunter R9 GitHub Secrets may be used at runtime.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse, parse_qs
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "research_projects/trendline_price_lineage_r8_20261119"))
from r8_price_lineage import (  # noqa: E402
    FOCAL_DATES, KIS_PERIOD_ENDPOINT, OHLC, ProvenanceError, audit, sha256,
)

BASE_URL = "https://openapi.koreainvestment.com:9443"
TOKEN_ENDPOINT = "/oauth2/tokenP"
SYMBOL = "006400"
START_DATE, END_DATE = "20230901", "20231106"
TR_ID = "FHKST03010100"
READONLY_ENDPOINTS = {TOKEN_ENDPOINT, KIS_PERIOD_ENDPOINT}
BAR_FIELDS = {
    "date": "stck_bsop_date", "open": "stck_oprc",
    "high": "stck_hgpr", "low": "stck_lwpr", "close": "stck_clpr",
    "volume": "acml_vol",
}


class SourceUnavailable(RuntimeError):
    pass


def safe_transport(method: str, path: str, headers: dict, body: dict | None = None) -> tuple[dict, str]:
    """Only the two audited read-only quote/auth URLs are allowed."""
    parsed = urlparse(path)
    endpoint = parsed.path
    if parsed.scheme or parsed.netloc or endpoint not in READONLY_ENDPOINTS or method not in {"GET", "POST"}:
        raise SourceUnavailable("R9_DISALLOWED_ENDPOINT")
    if method == "POST" and (endpoint != TOKEN_ENDPOINT or parsed.query):
        raise SourceUnavailable("R9_DISALLOWED_POST_ENDPOINT")
    if method == "GET" and endpoint != KIS_PERIOD_ENDPOINT:
        raise SourceUnavailable("R9_DISALLOWED_GET_ENDPOINT")
    if method == "GET":
        q = parse_qs(parsed.query, strict_parsing=True)
        if set(q) != {"FID_COND_MRKT_DIV_CODE", "FID_INPUT_ISCD", "FID_INPUT_DATE_1",
                      "FID_INPUT_DATE_2", "FID_PERIOD_DIV_CODE", "FID_ORG_ADJ_PRC"} or any(len(v) != 1 for v in q.values()):
            raise SourceUnavailable("R9_UNEXPECTED_QUERY_PARAMETERS")
        if (q["FID_COND_MRKT_DIV_CODE"] != ["J"] or q["FID_INPUT_ISCD"] != [SYMBOL]
                or q["FID_INPUT_DATE_1"] != [START_DATE] or q["FID_INPUT_DATE_2"] != [END_DATE]
                or q["FID_PERIOD_DIV_CODE"] != ["D"] or q["FID_ORG_ADJ_PRC"][0] not in {"0", "1"}):
            raise SourceUnavailable("R9_QUERY_SCOPE_INVALID")
    data = json.dumps(body).encode("utf-8") if body else None
    request = Request(BASE_URL + path, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=25) as response:
            content = response.read(2_000_000 + 1)
            if len(content) > 2_000_000:
                raise SourceUnavailable("R9_OVERSIZE_API_RESPONSE")
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        # Error text/URLs can contain secrets or API response content.
        raise SourceUnavailable("R9_API_NETWORK_OR_AUTH_FAILURE") from None
    try:
        decoded = json.loads(content)
        if not isinstance(decoded, dict):
            raise ValueError("not a dict")
    except (ValueError, UnicodeError):
        raise SourceUnavailable("R9_INVALID_API_JSON") from None
    return decoded, hashlib.sha256(content).hexdigest()


def capture_data(appkey: str, appsecret: str, transport=safe_transport) -> dict:
    token_response, _ = transport(
        "POST", TOKEN_ENDPOINT, {"content-type": "application/json"},
        {"grant_type": "client_credentials", "appkey": appkey, "appsecret": appsecret})
    token = token_response.get("access_token")
    if not isinstance(token, str) or not token:
        raise SourceUnavailable("R9_TOKEN_NOT_ISSUED")
    result = {}
    for flag, label in (("1", "raw"), ("0", "adjusted")):
        params = {
            "FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": SYMBOL,
            "FID_INPUT_DATE_1": START_DATE, "FID_INPUT_DATE_2": END_DATE,
            "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": flag,
        }
        # A dated-period endpoint: 1=original/raw, 0=adjusted.
        # Do NOT reuse the conflicting recent-30-day endpoint's convention.
        url = KIS_PERIOD_ENDPOINT + "?" + urlencode(params)
        response, response_hash = transport(
            "GET", url, {
                "content-type": "application/json", "authorization": "Bearer " + token,
                "appkey": appkey, "appsecret": appsecret,
                "tr_id": TR_ID, "custtype": "P"})
        if str(response.get("rt_cd")) != "0":
            raise SourceUnavailable("R9_KIS_QUOTE_API_ERROR " + str(response.get("msg_cd", ""))[:50])
        output = response.get("output2")
        if not isinstance(output, list) or not output:
            raise SourceUnavailable("R9_NO_HISTORICAL_BARS")
        parsed = []
        seen = set()
        for bar in output:
            if not isinstance(bar, dict):
                raise SourceUnavailable("R9_INVALID_BAR_OBJECT")
            try:
                date = str(bar[BAR_FIELDS["date"]])
                datetime.strptime(date, "%Y%m%d")
                rec = {k: int(bar[field]) for k, field in BAR_FIELDS.items() if k != "date"}
            except (KeyError, TypeError, ValueError) as exc:
                raise SourceUnavailable("R9_INVALID_BAR_FIELDS") from None
            if date in seen or not START_DATE <= date <= END_DATE:
                raise SourceUnavailable("R9_DUPLICATE_OR_OUT_OF_RANGE_DATE")
            seen.add(date)
            if min(rec[k] for k in OHLC) <= 0 or rec["volume"] < 0:
                raise SourceUnavailable("R9_INVALID_PRICE_OR_VOLUME")
            if rec["high"] < max(rec["open"], rec["close"]) or rec["low"] > min(rec["open"], rec["close"]):
                raise SourceUnavailable("R9_INVALID_OHLC_SHAPE")
            parsed.append({"code": SYMBOL, "date": datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d"), **rec})
        parsed.sort(key=lambda x: x["date"])
        result[label] = {"bars": parsed, "response_sha256": response_hash,
                         "request_flag": flag, "rows": len(parsed)}
    return result


def write_csv(path: Path, bars: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["code", "date", *OHLC, "volume"])
        writer.writeheader()
        writer.writerows(bars)


def write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run(out_dir: Path, environ: dict | None = None, transport=safe_transport) -> dict:
    env = os.environ if environ is None else environ
    out_dir.mkdir(parents=True, exist_ok=True)
    key, secret = (env.get("R9_KIS_APP_KEY", "").strip(), env.get("R9_KIS_APP_SECRET", "").strip())
    summary = {
        "revision": "R9_KIS_RAW_ADJUSTED_DATED_SOURCE_RESEARCH_ONLY",
        "research_only": True, "can_modify_live_search_watch": False,
        "symbol": SYMBOL, "date_range": [START_DATE, END_DATE],
        "target_dates": list(FOCAL_DATES), "api_endpoint": KIS_PERIOD_ENDPOINT,
        "original_flag": "1", "adjusted_flag": "0",
        "credentials_exported": False, "token_exported": False,
        "independent_price_lineage_certified": False,
        "full_r116_adjusted_replay_certified": False,
    }
    if not key and not secret:
        summary["status"] = "PENDING_DEDICATED_STOCK_HUNTER_R9_SECRETS"
        write_json(out_dir / "R9_SUMMARY.json", summary)
        return summary
    if not key or not secret:
        summary["status"] = "BLOCKED_PARTIAL_KIS_SECRETS"
        write_json(out_dir / "R9_SUMMARY.json", summary)
        return summary
    try:
        data = capture_data(key, secret, transport)
        captured = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        paths = {}
        for label in ("raw", "adjusted"):
            price_path = out_dir / ("KIS_006400_202309_202311_" + label + ".csv")
            write_csv(price_path, data[label]["bars"])
            manifest_path = out_dir / ("KIS_006400_202309_202311_" + label + "_manifest.json")
            write_json(manifest_path, {
                "provider": "KIS", "endpoint": KIS_PERIOD_ENDPOINT,
                "symbol": SYMBOL, "period": "D",
                "fid_org_adj_prc": data[label]["request_flag"],
                "csv_sha256": sha256(price_path),
                "api_response_sha256": data[label]["response_sha256"],
                "captured_at_utc": captured,
                "request_date_range": [START_DATE, END_DATE],
                "normalization": "source stck_bsop_date/oprc/hgpr/lwpr/clpr/acml_vol to CSV"})
            paths[label] = price_path, manifest_path
        comparison = audit(paths["raw"][0], paths["adjusted"][0], paths["raw"][1], paths["adjusted"][1])
        write_json(out_dir / "R9_R8_INDEPENDENT_COMPARISON.json", comparison)
        summary["r8_independent_status"] = comparison["independent_status"]
        summary["source_row_counts"] = {x: data[x]["rows"] for x in ("raw", "adjusted")}
        summary["status"] = ("FIVE_DATE_COMPARISON_COMPLETE_FULL_REPLAY_PENDING"
                             if comparison["independent_status"] == "FIVE_DATE_COMPARISON_COMPLETE_FULL_R116_REPLAY_NOT_DONE"
                             else "INDEPENDENT_SOURCE_DISAGREEMENT_OR_MISSING_DATES")
    except (SourceUnavailable, ProvenanceError) as exc:
        summary["status"] = "API_UNAVAILABLE_FAIL_CLOSED"
        summary["error_code"] = str(exc)[:120]
    write_json(out_dir / "R9_SUMMARY.json", summary)
    return summary


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out-dir", required=True, type=Path)
    args = p.parse_args()
    summary = run(args.out_dir)
    print("R9_STATUS=" + summary["status"])
    if summary["status"] in {"BLOCKED_PARTIAL_KIS_SECRETS", "API_UNAVAILABLE_FAIL_CLOSED",
                             "INDEPENDENT_SOURCE_DISAGREEMENT_OR_MISSING_DATES"}:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
