from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import time
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

VERSION = "V73.3.6.6.11"
RESEARCH_ONLY = True

UNIFIED_COLUMNS = [
    "source_id", "source_type", "source_name", "source_url", "source_domain",
    "code", "name", "title", "summary", "published_at", "updated_at",
    "first_seen_at", "event_occurred_at", "official_at", "retrieved_at",
    "causal_mode", "official_source", "independent_source", "global_scope",
    "event_type", "materiality", "direct_benefit", "thesis_validity",
    "new_fact", "reference_id", "raw_payload_sha256",
]

HTML_TAG = re.compile(r"<[^>]+>")
SPACE = re.compile(r"\s+")


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _clean(v: Any) -> str:
    s = HTML_TAG.sub(" ", str(v or ""))
    return SPACE.sub(" ", s).strip()


def _norm_code(v: Any) -> str:
    d = re.sub(r"\D", "", str(v or ""))
    return d.zfill(6)[-6:] if d else ""


def _as_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v or "").strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off", ""}:
        return False
    return default


def _domain(url: str) -> str:
    try:
        return urllib.parse.urlparse(str(url or "")).netloc.lower().removeprefix("www.")
    except Exception:
        return ""


def _sha(payload: Any) -> str:
    if isinstance(payload, bytes):
        b = payload
    else:
        b = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def _request_json(url: str, headers: dict[str, str] | None = None, timeout: int = 15) -> dict:
    req = urllib.request.Request(url, headers=headers or {"User-Agent": "StockHunter/73.3.6.6.11"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def _request_bytes(url: str, headers: dict[str, str] | None = None, timeout: int = 20) -> bytes:
    req = urllib.request.Request(url, headers=headers or {"User-Agent": "StockHunter/73.3.6.6.11"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def empty_ledger() -> pd.DataFrame:
    return pd.DataFrame(columns=UNIFIED_COLUMNS)


def normalize_rows(rows: Iterable[dict], causal_mode: str = "FORWARD_CAUSAL") -> pd.DataFrame:
    out: list[dict] = []
    now = _now_iso()
    for raw in rows or []:
        r = dict(raw or {})
        url = str(r.get("source_url") or r.get("url") or "")
        source_name = str(r.get("source_name") or r.get("provider") or r.get("source") or "UNKNOWN")
        title = _clean(r.get("title") or r.get("report_nm") or r.get("공시명") or "")
        summary = _clean(r.get("summary") or r.get("contents") or r.get("content") or "")
        published = r.get("published_at") or r.get("datetime") or r.get("rcept_dt") or ""
        code = _norm_code(r.get("code") or r.get("stock_code") or r.get("종목코드"))
        payload_sha = str(r.get("raw_payload_sha256") or _sha(r))
        source_id = str(r.get("source_id") or r.get("reference_id") or r.get("rcept_no") or payload_sha[:20])
        z = {c: "" for c in UNIFIED_COLUMNS}
        z.update({
            "source_id": source_id,
            "source_type": str(r.get("source_type") or "NEWS"),
            "source_name": source_name,
            "source_url": url,
            "source_domain": str(r.get("source_domain") or _domain(url) or source_name).lower(),
            "code": code,
            "name": _clean(r.get("name") or r.get("corp_name") or r.get("종목명") or ""),
            "title": title,
            "summary": summary,
            "published_at": str(published),
            "updated_at": str(r.get("updated_at") or ""),
            "first_seen_at": str(r.get("first_seen_at") or now),
            "event_occurred_at": str(r.get("event_occurred_at") or ""),
            "official_at": str(r.get("official_at") or (published if _as_bool(r.get("official_source", False)) else "")),
            "retrieved_at": str(r.get("retrieved_at") or now),
            "causal_mode": str(r.get("causal_mode") or causal_mode),
            "official_source": _as_bool(r.get("official_source", False)),
            "independent_source": _as_bool(r.get("independent_source", True), default=True),
            "global_scope": _as_bool(r.get("global_scope", False)),
            "event_type": str(r.get("event_type") or "UNCLASSIFIED"),
            "materiality": str(r.get("materiality") or "UNKNOWN"),
            "direct_benefit": str(r.get("direct_benefit") or "UNKNOWN"),
            "thesis_validity": str(r.get("thesis_validity") or "UNKNOWN"),
            "new_fact": str(r.get("new_fact") or "UNKNOWN"),
            "reference_id": str(r.get("reference_id") or r.get("rcept_no") or ""),
            "raw_payload_sha256": payload_sha,
        })
        out.append(z)
    return pd.DataFrame(out, columns=UNIFIED_COLUMNS) if out else empty_ledger()


def load_csv_ledgers(paths: Iterable[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for p in paths:
        if not p.exists() or p.stat().st_size == 0:
            continue
        try:
            q = pd.read_csv(p, dtype=str, encoding="utf-8-sig")
        except Exception:
            try:
                q = pd.read_csv(p, dtype=str)
            except Exception:
                continue
        frames.append(normalize_rows(q.fillna("").to_dict("records"), causal_mode="FORWARD_CAUSAL"))
    return pd.concat(frames, ignore_index=True) if frames else empty_ledger()


def _dart_corp_map(api_key: str, cache_dir: Path) -> dict[str, dict[str, str]]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache = cache_dir / "dart_corp_code_map.json"
    if cache.exists() and time.time() - cache.stat().st_mtime < 7 * 86400:
        try:
            return json.loads(cache.read_text(encoding="utf-8"))
        except Exception:
            pass
    data = _request_bytes("https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key=" + urllib.parse.quote(api_key))
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        xml = zf.read(zf.namelist()[0]).decode("utf-8", errors="replace")
    items = re.findall(r"<list>(.*?)</list>", xml, flags=re.S)
    out: dict[str, dict[str, str]] = {}
    for item in items:
        def tag(name: str) -> str:
            m = re.search(fr"<{name}>(.*?)</{name}>", item, flags=re.S)
            return _clean(m.group(1)) if m else ""
        stock = _norm_code(tag("stock_code"))
        if stock:
            out[stock] = {"corp_code": tag("corp_code"), "corp_name": tag("corp_name")}
    cache.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def fetch_opendart(codes: Iterable[str], start_date: str, end_date: str, cache_dir: Path) -> pd.DataFrame:
    key = os.environ.get("OPENDART_API_KEY", "").strip()
    if not key:
        return empty_ledger()
    cmap = _dart_corp_map(key, cache_dir)
    rows: list[dict] = []
    for code in sorted({_norm_code(x) for x in codes if _norm_code(x)}):
        info = cmap.get(code)
        if not info:
            continue
        params = {
            "crtfc_key": key, "corp_code": info["corp_code"], "bgn_de": start_date.replace("-", ""),
            "end_de": end_date.replace("-", ""), "page_no": "1", "page_count": "100",
        }
        url = "https://opendart.fss.or.kr/api/list.json?" + urllib.parse.urlencode(params)
        try:
            payload = _request_json(url)
        except Exception:
            continue
        if str(payload.get("status")) not in {"000", "013"}:
            continue
        for it in payload.get("list", []) or []:
            dt = str(it.get("rcept_dt") or "")
            published = f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}T23:59:59+09:00" if len(dt) == 8 else dt
            rcept_no = str(it.get("rcept_no") or "")
            rows.append({
                "source_id": rcept_no,
                "source_type": "OFFICIAL_DISCLOSURE",
                "source_name": "OPENDART",
                "source_url": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}" if rcept_no else "",
                "code": code,
                "name": info.get("corp_name", ""),
                "title": it.get("report_nm", ""),
                "published_at": published,
                "official_at": published,
                "official_source": True,
                "independent_source": True,
                "event_type": "DISCLOSURE",
                "materiality": "UNKNOWN",
                "direct_benefit": "UNKNOWN",
                "thesis_validity": "VALID_UNTIL_CONTRADICTED",
                "new_fact": "UNKNOWN",
                "reference_id": rcept_no,
                "raw_payload_sha256": _sha(it),
            })
    return normalize_rows(rows)


def fetch_google_cse(queries: Iterable[dict]) -> pd.DataFrame:
    key = os.environ.get("GOOGLE_CSE_API_KEY", "").strip()
    cx = os.environ.get("GOOGLE_CSE_ID", "").strip()
    if not key or not cx:
        return empty_ledger()
    rows: list[dict] = []
    for q in queries:
        query = str(q.get("query") or "").strip()
        if not query:
            continue
        params = {"key": key, "cx": cx, "q": query, "num": min(10, int(q.get("num", 10) or 10))}
        try:
            payload = _request_json("https://www.googleapis.com/customsearch/v1?" + urllib.parse.urlencode(params))
        except Exception:
            continue
        for it in payload.get("items", []) or []:
            meta = ((it.get("pagemap") or {}).get("metatags") or [{}])[0]
            published = meta.get("article:published_time") or meta.get("date") or meta.get("og:updated_time") or ""
            rows.append({
                "source_type": "WEB_SEARCH",
                "source_name": "GOOGLE_CSE",
                "source_url": it.get("link", ""),
                "code": q.get("code", ""), "name": q.get("name", ""),
                "title": it.get("title", ""), "summary": it.get("snippet", ""),
                "published_at": published,
                "official_source": False,
                "independent_source": True,
                "global_scope": _as_bool(q.get("global_scope", False)),
                "event_type": q.get("event_type", "UNCLASSIFIED"),
                "causal_mode": q.get("causal_mode", "FORWARD_CAUSAL"),
                "raw_payload_sha256": _sha(it),
            })
    return normalize_rows(rows)


def fetch_kakao_web(queries: Iterable[dict]) -> pd.DataFrame:
    key = os.environ.get("KAKAO_REST_API_KEY", "").strip()
    if not key:
        return empty_ledger()
    rows: list[dict] = []
    headers = {"Authorization": f"KakaoAK {key}", "User-Agent": "StockHunter/73.3.6.6.11"}
    for q in queries:
        query = str(q.get("query") or "").strip()
        if not query:
            continue
        params = {"query": query, "sort": "recency", "size": min(50, int(q.get("size", 20) or 20))}
        try:
            payload = _request_json("https://dapi.kakao.com/v2/search/web?" + urllib.parse.urlencode(params), headers=headers)
        except Exception:
            continue
        for it in payload.get("documents", []) or []:
            rows.append({
                "source_type": "WEB_SEARCH",
                "source_name": "KAKAO_DAUM_WEB",
                "source_url": it.get("url", ""),
                "code": q.get("code", ""), "name": q.get("name", ""),
                "title": it.get("title", ""), "summary": it.get("contents", ""),
                "published_at": it.get("datetime", ""),
                "official_source": False,
                "independent_source": True,
                "global_scope": _as_bool(q.get("global_scope", False)),
                "event_type": q.get("event_type", "UNCLASSIFIED"),
                "causal_mode": q.get("causal_mode", "FORWARD_CAUSAL"),
                "raw_payload_sha256": _sha(it),
            })
    return normalize_rows(rows)


def ensure_templates(output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    files = [
        "v73_catalyst_source_ledger.csv",
        "v73_ai_comment_source_ledger.csv",
        "v73_official_disclosure_ledger.csv",
        "v73_news_source_ledger.csv",
        "v73_global_catalyst_ledger.csv",
    ]
    paths = []
    for name in files:
        p = output_dir / name
        if not p.exists():
            empty_ledger().to_csv(p, index=False, encoding="utf-8-sig")
        paths.append(p)
    return paths


def capture_forward(output_dir: str, queries_path: str = "") -> pd.DataFrame:
    out = Path(output_dir or "reports")
    templates = ensure_templates(out)
    network = os.environ.get("CATALYST_NETWORK_ENABLE", "0").strip().lower() in {"1", "true", "yes", "on"}
    frames = [load_csv_ledgers(templates)]
    queries: list[dict] = []
    qp = Path(queries_path) if queries_path else out / "v73_catalyst_query_universe.csv"
    if qp.exists():
        try:
            queries = pd.read_csv(qp, dtype=str).fillna("").to_dict("records")
        except Exception:
            queries = []
    if network and queries:
        start = os.environ.get("CATALYST_START_DATE", datetime.now().strftime("%Y-%m-%d"))
        end = os.environ.get("CATALYST_END_DATE", start)
        codes = [q.get("code", "") for q in queries]
        frames += [fetch_opendart(codes, start, end, out / ".cache"), fetch_google_cse(queries), fetch_kakao_web(queries)]
    merged = pd.concat([x for x in frames if isinstance(x, pd.DataFrame)], ignore_index=True) if frames else empty_ledger()
    if not merged.empty:
        merged = merged.drop_duplicates(["source_id", "source_url", "code", "title", "published_at"], keep="last")
    merged.to_csv(out / "v73_catalyst_source_ledger.csv", index=False, encoding="utf-8-sig")
    audit = pd.DataFrame([{
        "version": VERSION,
        "network_enabled": network,
        "queries": len(queries),
        "rows": len(merged),
        "opendart_enabled": bool(os.environ.get("OPENDART_API_KEY")),
        "google_enabled": bool(os.environ.get("GOOGLE_CSE_API_KEY") and os.environ.get("GOOGLE_CSE_ID")),
        "kakao_enabled": bool(os.environ.get("KAKAO_REST_API_KEY")),
        "status": "CAPTURED" if len(merged) else "TEMPLATE_READY",
    }])
    audit.to_csv(out / "v73_catalyst_source_capture_audit.csv", index=False, encoding="utf-8-sig")
    return merged


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--capture", action="store_true")
    ap.add_argument("--output-dir", default="reports")
    ap.add_argument("--queries", default="")
    args = ap.parse_args()
    if args.capture:
        q = capture_forward(args.output_dir, args.queries)
        print(f"CATALYST_SOURCE_CAPTURE version={VERSION} rows={len(q)} output={args.output_dir}")
    else:
        ensure_templates(Path(args.output_dir))
        print(f"CATALYST_SOURCE_TEMPLATES version={VERSION} output={args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
