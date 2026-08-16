from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import time
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

VERSION = "V73.3.6.6.12"
RESEARCH_ONLY = True

UNIFIED_COLUMNS = [
    "source_id", "source_key", "source_type", "source_name", "source_url", "source_domain",
    "code", "name", "sector", "theme", "query_text", "title", "summary",
    "published_at", "updated_at", "first_seen_at", "event_occurred_at", "official_at", "retrieved_at",
    "causal_mode", "official_source", "independent_source", "global_scope",
    "event_type", "materiality", "direct_benefit", "thesis_validity", "new_fact",
    "time_precision", "same_day_causal_eligible", "capture_profile", "capture_run_id",
    "reference_id", "raw_payload_sha256",
]

HTML_TAG = re.compile(r"<[^>]+>")
SPACE = re.compile(r"\s+")
TRACKING_KEYS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "fbclid", "gclid",
    "igshid", "ref", "referrer", "source", "from", "campaign", "ncid",
}

COMPONENT_LEDGER_FILES = {
    "master": "v73_catalyst_source_ledger.csv",
    "ai": "v73_ai_comment_source_ledger.csv",
    "official": "v73_official_disclosure_ledger.csv",
    "news": "v73_news_source_ledger.csv",
    "global": "v73_global_catalyst_ledger.csv",
}

QUERY_FILE = "v73_catalyst_query_universe.csv"
GLOBAL_QUERY_FILE = "v73_global_catalyst_query_ledger.csv"
CAPTURE_AUDIT_FILE = "v73_catalyst_source_capture_audit.csv"
FIRST_SEEN_AUDIT_FILE = "v73_catalyst_first_seen_integrity_audit.csv"
MARKET_LEDGER_FILE = "v73_market_sector_context_ledger.csv"
MARKET_CAPTURE_AUDIT_FILE = "v73_market_sector_forward_capture_audit.csv"


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _clean(v: Any) -> str:
    s = HTML_TAG.sub(" ", str(v or ""))
    return SPACE.sub(" ", s).strip()


def _norm_code(v: Any) -> str:
    """Canonical KRX ticker identity; preserves 6-char alphanumeric tickers (e.g. 0126Z0)."""
    raw = str(v or "").strip().upper()
    if raw.endswith(".0") and raw[:-2].isdigit():
        raw = raw[:-2]
    for suffix in (".KS", ".KQ", ".KRX"):
        if raw.endswith(suffix):
            raw = raw[:-len(suffix)]
            break
    s = "".join(ch for ch in raw if ch in "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    if len(s) == 7 and s.startswith("A"):
        s = s[1:]
    if s.isdigit() and len(s) <= 6:
        return s.zfill(6)
    if len(s) >= 6:
        return s[-6:]
    return s

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


def _canonical_url(url: Any) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        p = urllib.parse.urlsplit(raw)
        pairs = urllib.parse.parse_qsl(p.query, keep_blank_values=True)
        query = urllib.parse.urlencode([(k, v) for k, v in pairs if k.lower() not in TRACKING_KEYS])
        path = re.sub(r"/{2,}", "/", p.path or "/")
        return urllib.parse.urlunsplit((p.scheme.lower(), p.netloc.lower().removeprefix("www."), path, query, ""))
    except Exception:
        return raw


def _sha(payload: Any) -> str:
    if isinstance(payload, bytes):
        b = payload
    else:
        b = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def _source_key(row: dict[str, Any]) -> str:
    source_name = str(row.get("source_name") or row.get("provider") or row.get("source") or "UNKNOWN").upper()
    source_id = str(row.get("source_id") or row.get("reference_id") or row.get("rcept_no") or "").strip()
    code = _norm_code(row.get("code") or row.get("stock_code") or row.get("종목코드"))
    url = _canonical_url(row.get("source_url") or row.get("url") or "")
    title = _clean(row.get("title") or row.get("report_nm") or row.get("공시명") or "").lower()
    published = str(row.get("published_at") or row.get("datetime") or row.get("rcept_dt") or "")
    official_id = source_name in {"OPENDART", "DART", "KIND", "KRX", "GOV", "GOVERNMENT", "COMPANY_IR", "COMPANY_OFFICIAL"} or str(row.get("source_type", "")).upper().startswith("OFFICIAL")
    if official_id and source_id:
        seed = f"{source_name}|ID|{source_id}"
    elif url:
        seed = f"{source_name}|URL|{url}"
    elif source_id:
        seed = f"{source_name}|ID|{source_id}"
    else:
        seed = f"{source_name}|TEXT|{code}|{title}|{published}"
    return "SRC-" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:24]


def _request_json(url: str, headers: dict[str, str] | None = None, timeout: int = 15) -> dict:
    req = urllib.request.Request(url, headers=headers or {"User-Agent": "StockHunter/73.3.6.6.12"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def _request_bytes(url: str, headers: dict[str, str] | None = None, timeout: int = 20) -> bytes:
    req = urllib.request.Request(url, headers=headers or {"User-Agent": "StockHunter/73.3.6.6.12"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def empty_ledger() -> pd.DataFrame:
    return pd.DataFrame(columns=UNIFIED_COLUMNS)


def normalize_rows(rows: Iterable[dict], causal_mode: str = "FORWARD_CAUSAL") -> pd.DataFrame:
    out: list[dict] = []
    now = _now_iso()
    for raw in rows or []:
        r = dict(raw or {})
        url = _canonical_url(r.get("source_url") or r.get("url") or "")
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
            "source_key": str(r.get("source_key") or _source_key({**r, "source_id": source_id, "source_url": url, "title": title, "published_at": published})),
            "source_type": str(r.get("source_type") or "NEWS"),
            "source_name": source_name,
            "source_url": url,
            "source_domain": str(r.get("source_domain") or _domain(url) or source_name).lower(),
            "code": code,
            "name": _clean(r.get("name") or r.get("corp_name") or r.get("종목명") or ""),
            "sector": _clean(r.get("sector") or r.get("업종") or r.get("섹터") or ""),
            "theme": _clean(r.get("theme") or r.get("테마") or ""),
            "query_text": _clean(r.get("query_text") or r.get("query") or ""),
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
            "time_precision": str(r.get("time_precision") or ("DATETIME" if "T" in str(published) else ("DATE_ONLY" if published else "UNKNOWN"))),
            "same_day_causal_eligible": _as_bool(r.get("same_day_causal_eligible", False)),
            "capture_profile": str(r.get("capture_profile") or os.environ.get("TEST_PROFILE") or "UNKNOWN"),
            "capture_run_id": str(r.get("capture_run_id") or os.environ.get("GITHUB_RUN_ID") or "LOCAL"),
            "reference_id": str(r.get("reference_id") or r.get("rcept_no") or ""),
            "raw_payload_sha256": payload_sha,
        })
        out.append(z)
    return pd.DataFrame(out, columns=UNIFIED_COLUMNS) if out else empty_ledger()


def _parse_ts(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def merge_append_only(existing: pd.DataFrame, incoming: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    e = normalize_rows(existing.fillna("").to_dict("records")) if isinstance(existing, pd.DataFrame) and not existing.empty else empty_ledger()
    n = normalize_rows(incoming.fillna("").to_dict("records")) if isinstance(incoming, pd.DataFrame) and not incoming.empty else empty_ledger()
    before_first = dict(zip(e.get("source_key", pd.Series(dtype=str)), e.get("first_seen_at", pd.Series(dtype=str))))
    q = pd.concat([e, n], ignore_index=True) if len(e) or len(n) else empty_ledger()
    if q.empty:
        return q, {"existing_rows": 0, "incoming_rows": 0, "merged_rows": 0, "new_unique_rows": 0, "first_seen_regressions": 0}
    q["source_key"] = q.apply(lambda r: str(r.get("source_key") or _source_key(r.to_dict())), axis=1)
    rows: list[dict] = []
    for _, g in q.groupby("source_key", sort=False, dropna=False):
        g = g.copy()
        retrieved_ts = _parse_ts(g["retrieved_at"])
        base = g.iloc[int(retrieved_ts.fillna(pd.Timestamp.min.tz_localize("UTC")).argmax())].to_dict()
        for col, mode in (("first_seen_at", "min"), ("published_at", "min"), ("official_at", "min"), ("event_occurred_at", "min"), ("retrieved_at", "max"), ("updated_at", "max")):
            ts = _parse_ts(g[col])
            if ts.notna().any():
                value = ts.min() if mode == "min" else ts.max()
                base[col] = value.isoformat()
        base["official_source"] = bool(g["official_source"].map(_as_bool).any())
        base["independent_source"] = bool(g["independent_source"].map(_as_bool).any())
        base["global_scope"] = bool(g["global_scope"].map(_as_bool).any())
        modes = set(g["causal_mode"].astype(str))
        if "FORWARD_CAUSAL" in modes:
            base["causal_mode"] = "FORWARD_CAUSAL"
        elif "OFFICIAL_ARCHIVE_CAUSAL" in modes:
            base["causal_mode"] = "OFFICIAL_ARCHIVE_CAUSAL"
        else:
            base["causal_mode"] = "RETROSPECTIVE_RESEARCH"
        rows.append(base)
    merged = normalize_rows(rows).sort_values(["first_seen_at", "source_key"], kind="stable").reset_index(drop=True)
    regressions = 0
    for _, r in merged.iterrows():
        old = before_first.get(str(r["source_key"]))
        if old:
            old_ts = pd.to_datetime(old, errors="coerce", utc=True)
            new_ts = pd.to_datetime(r["first_seen_at"], errors="coerce", utc=True)
            if pd.notna(old_ts) and pd.notna(new_ts) and new_ts > old_ts:
                regressions += 1
    audit = {
        "existing_rows": len(e), "incoming_rows": len(n), "merged_rows": len(merged),
        "new_unique_rows": max(0, len(merged) - len(e)), "first_seen_regressions": regressions,
    }
    return merged, audit


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
    if not frames:
        return empty_ledger()
    merged, _ = merge_append_only(empty_ledger(), pd.concat(frames, ignore_index=True))
    return merged


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
    stats = {"attempted": 0, "errors": 0, "returned": 0, "enabled": bool(key)}
    if not key:
        q = empty_ledger(); q.attrs["capture_stats"] = stats; return q
    try:
        cmap = _dart_corp_map(key, cache_dir)
    except Exception as exc:
        stats["errors"] += 1
        stats["error"] = f"{type(exc).__name__}:{exc}"
        q = empty_ledger(); q.attrs["capture_stats"] = stats; return q
    rows: list[dict] = []
    for code in sorted({_norm_code(x) for x in codes if _norm_code(x)}):
        info = cmap.get(code)
        if not info:
            continue
        stats["attempted"] += 1
        params = {
            "crtfc_key": key, "corp_code": info["corp_code"], "bgn_de": start_date.replace("-", ""),
            "end_de": end_date.replace("-", ""), "page_no": "1", "page_count": "100",
        }
        url = "https://opendart.fss.or.kr/api/list.json?" + urllib.parse.urlencode(params)
        try:
            payload = _request_json(url)
        except Exception:
            stats["errors"] += 1
            continue
        if str(payload.get("status")) not in {"000", "013"}:
            stats["errors"] += 1
            continue
        for it in payload.get("list", []) or []:
            dt = str(it.get("rcept_dt") or "")
            published = f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}T23:59:59+09:00" if len(dt) == 8 else dt
            rcept_no = str(it.get("rcept_no") or "")
            rows.append({
                "source_id": rcept_no, "source_type": "OFFICIAL_DISCLOSURE", "source_name": "OPENDART",
                "source_url": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}" if rcept_no else "",
                "code": code, "name": info.get("corp_name", ""), "title": it.get("report_nm", ""),
                "published_at": published, "official_at": published, "official_source": True,
                "independent_source": True, "event_type": "DISCLOSURE", "materiality": "UNKNOWN",
                "direct_benefit": "UNKNOWN", "thesis_validity": "VALID_UNTIL_CONTRADICTED",
                "new_fact": "UNKNOWN", "reference_id": rcept_no, "time_precision": "DATE_ONLY",
                "same_day_causal_eligible": False, "causal_mode": "FORWARD_CAUSAL",
                "raw_payload_sha256": _sha(it),
            })
    stats["returned"] = len(rows)
    q = normalize_rows(rows)
    q.attrs["capture_stats"] = stats
    return q


def fetch_google_cse(queries: Iterable[dict]) -> pd.DataFrame:
    key = os.environ.get("GOOGLE_CSE_API_KEY", "").strip()
    cx = os.environ.get("GOOGLE_CSE_ID", "").strip()
    stats = {"attempted": 0, "errors": 0, "returned": 0, "enabled": bool(key and cx)}
    if not key or not cx:
        q = empty_ledger(); q.attrs["capture_stats"] = stats; return q
    rows: list[dict] = []
    for qy in queries:
        query = str(qy.get("query") or "").strip()
        if not query:
            continue
        stats["attempted"] += 1
        params = {"key": key, "cx": cx, "q": query, "num": min(10, int(qy.get("num", 10) or 10))}
        try:
            payload = _request_json("https://www.googleapis.com/customsearch/v1?" + urllib.parse.urlencode(params))
        except Exception:
            stats["errors"] += 1
            continue
        for it in payload.get("items", []) or []:
            meta = ((it.get("pagemap") or {}).get("metatags") or [{}])[0]
            published = meta.get("article:published_time") or meta.get("date") or meta.get("og:updated_time") or ""
            rows.append({
                "source_type": "WEB_SEARCH", "source_name": "GOOGLE_CSE", "source_url": it.get("link", ""),
                "code": qy.get("code", ""), "name": qy.get("name", ""), "sector": qy.get("sector", ""),
                "theme": qy.get("theme", ""), "query_text": query, "title": it.get("title", ""),
                "summary": it.get("snippet", ""), "published_at": published,
                "official_source": False, "independent_source": True,
                "global_scope": _as_bool(qy.get("global_scope", False)),
                "event_type": qy.get("event_type", "UNCLASSIFIED"), "causal_mode": "FORWARD_CAUSAL",
                "raw_payload_sha256": _sha(it),
            })
    stats["returned"] = len(rows)
    q = normalize_rows(rows)
    q.attrs["capture_stats"] = stats
    return q


def fetch_kakao_web(queries: Iterable[dict]) -> pd.DataFrame:
    key = os.environ.get("KAKAO_REST_API_KEY", "").strip()
    stats = {"attempted": 0, "errors": 0, "returned": 0, "enabled": bool(key)}
    if not key:
        q = empty_ledger(); q.attrs["capture_stats"] = stats; return q
    rows: list[dict] = []
    headers = {"Authorization": f"KakaoAK {key}", "User-Agent": "StockHunter/73.3.6.6.12"}
    for qy in queries:
        query = str(qy.get("query") or "").strip()
        if not query:
            continue
        stats["attempted"] += 1
        params = {"query": query, "sort": "recency", "size": min(50, int(qy.get("size", 20) or 20))}
        try:
            payload = _request_json("https://dapi.kakao.com/v2/search/web?" + urllib.parse.urlencode(params), headers=headers)
        except Exception:
            stats["errors"] += 1
            continue
        for it in payload.get("documents", []) or []:
            rows.append({
                "source_type": "WEB_SEARCH", "source_name": "KAKAO_DAUM_WEB", "source_url": it.get("url", ""),
                "code": qy.get("code", ""), "name": qy.get("name", ""), "sector": qy.get("sector", ""),
                "theme": qy.get("theme", ""), "query_text": query, "title": it.get("title", ""),
                "summary": it.get("contents", ""), "published_at": it.get("datetime", ""),
                "official_source": False, "independent_source": True,
                "global_scope": _as_bool(qy.get("global_scope", False)),
                "event_type": qy.get("event_type", "UNCLASSIFIED"), "causal_mode": "FORWARD_CAUSAL",
                "raw_payload_sha256": _sha(it),
            })
    stats["returned"] = len(rows)
    q = normalize_rows(rows)
    q.attrs["capture_stats"] = stats
    return q


def ensure_templates(output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for name in COMPONENT_LEDGER_FILES.values():
        p = output_dir / name
        if not p.exists():
            empty_ledger().to_csv(p, index=False, encoding="utf-8-sig")
        paths.append(p)
    gp = output_dir / GLOBAL_QUERY_FILE
    if not gp.exists():
        pd.DataFrame(columns=["query", "sector", "theme", "global_scope", "enabled", "priority", "event_type"]).to_csv(gp, index=False, encoding="utf-8-sig")
    return paths


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    for enc in ("utf-8-sig", "utf-8"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc).fillna("")
        except Exception:
            pass
    return pd.DataFrame()


def build_query_universe(output_dir: str | Path, max_codes: int | None = None, max_global: int | None = None) -> pd.DataFrame:
    out = Path(output_dir or "reports")
    ensure_templates(out)
    max_codes = max_codes if max_codes is not None else int(os.environ.get("CATALYST_CAPTURE_MAX_CODES", "30") or 30)
    max_global = max_global if max_global is not None else int(os.environ.get("CATALYST_CAPTURE_MAX_GLOBAL_QUERIES", "8") or 8)
    sources = [
        ("v1080_stockhunter_signals.csv", 100),
        ("v73_latest_discovery_snapshot.csv", 95),
        ("v73_active_cycle_state.csv", 90),
        ("v73_signal_lifecycle_ledger.csv", 85),
        ("v72_11_prc_candidate_audit.csv", 80),
        ("v73_sequence_state_machine_audit.csv", 75),
    ]
    rows: list[dict] = []
    for filename, priority in sources:
        q = _read_csv(out / filename)
        if q.empty:
            continue
        code_col = next((c for c in ("code", "Code", "종목코드", "stock_code") if c in q.columns), None)
        name_col = next((c for c in ("name", "Name", "종목명", "corp_name") if c in q.columns), None)
        sector_col = next((c for c in ("sector", "Sector", "업종", "섹터", "theme", "테마") if c in q.columns), None)
        score_col = next((c for c in ("score", "raw_score", "N점수", "safe_score", "sequence_quality_score") if c in q.columns), None)
        if not code_col:
            continue
        for _, r in q.iterrows():
            code = _norm_code(r.get(code_col, ""))
            if not code:
                continue
            name = _clean(r.get(name_col, "")) if name_col else ""
            sector = _clean(r.get(sector_col, "")) if sector_col else ""
            score = pd.to_numeric(pd.Series([r.get(score_col, "")]), errors="coerce").iloc[0] if score_col else float("nan")
            rows.append({
                "code": code, "name": name, "sector": sector, "theme": "",
                "query": f"{name or code} {code} 수주 실적 정책 공급계약 증설 인증".strip(),
                "global_scope": False, "event_type": "UNCLASSIFIED", "causal_mode": "FORWARD_CAUSAL",
                "priority": priority + (float(score) / 1000 if pd.notna(score) else 0), "query_source": filename,
            })
    company = pd.DataFrame(rows)
    if not company.empty:
        company = company.sort_values(["priority", "code"], ascending=[False, True], kind="stable").drop_duplicates("code").head(max_codes)

    global_rows: list[dict] = []
    manual = _read_csv(out / GLOBAL_QUERY_FILE)
    if not manual.empty:
        for _, r in manual.iterrows():
            if not _as_bool(r.get("enabled", True), default=True):
                continue
            query = _clean(r.get("query", ""))
            if not query:
                continue
            global_rows.append({
                "code": "", "name": "", "sector": _clean(r.get("sector", "")), "theme": _clean(r.get("theme", "")),
                "query": query, "global_scope": True, "event_type": str(r.get("event_type") or "POLICY_GLOBAL"),
                "causal_mode": "FORWARD_CAUSAL", "priority": float(pd.to_numeric(pd.Series([r.get("priority", 50)]), errors="coerce").fillna(50).iloc[0]),
                "query_source": GLOBAL_QUERY_FILE,
            })
    for filename in ("v73_daily_sector_regime.csv", "v73_sector_tape_snapshot.csv"):
        q = _read_csv(out / filename)
        if q.empty:
            continue
        sec_col = next((c for c in ("sector", "Sector", "업종", "섹터", "theme", "테마") if c in q.columns), None)
        if not sec_col:
            continue
        for sec in q[sec_col].astype(str).map(_clean).drop_duplicates().head(max_global):
            if sec:
                global_rows.append({
                    "code": "", "name": "", "sector": sec, "theme": sec,
                    "query": f"{sec} 글로벌 정책 투자 공급망 수요 가격 동향",
                    "global_scope": True, "event_type": "POLICY_GLOBAL", "causal_mode": "FORWARD_CAUSAL",
                    "priority": 40, "query_source": filename,
                })
    global_df = pd.DataFrame(global_rows)
    if not global_df.empty:
        global_df = global_df.sort_values("priority", ascending=False, kind="stable").drop_duplicates("query").head(max_global)
    result = pd.concat([x for x in (company, global_df) if isinstance(x, pd.DataFrame) and not x.empty], ignore_index=True) if (not company.empty or not global_df.empty) else pd.DataFrame(columns=["code", "name", "sector", "theme", "query", "global_scope", "event_type", "causal_mode", "priority", "query_source"])
    result.to_csv(out / QUERY_FILE, index=False, encoding="utf-8-sig")
    return result


def discover_ai_comment_rows(output_dir: str | Path) -> pd.DataFrame:
    out = Path(output_dir or "reports")
    content_fields = ["AI재료분류", "AI강한근거", "AI심판요약", "AI코멘트", "AI요약", "news_sentiment", "최근뉴스", "뉴스", "공시내용", "공시태그"]
    time_fields = ["source_captured_at", "captured_at", "AI코멘트생성시각", "created_at", "generated_at", "retrieved_at"]
    url_fields = ["source_url", "news_url", "article_url", "url", "뉴스URL", "출처URL"]
    rows: list[dict] = []
    now = _now_iso()
    for p in sorted(out.glob("*.csv")):
        if p.name.startswith("v73_catalyst_") or p.name in COMPONENT_LEDGER_FILES.values() or p.stat().st_size > 80_000_000:
            continue
        q = _read_csv(p)
        if q.empty:
            continue
        content_cols = [c for c in content_fields if c in q.columns]
        if not content_cols:
            continue
        code_col = next((c for c in ("code", "Code", "종목코드") if c in q.columns), None)
        name_col = next((c for c in ("name", "Name", "종목명") if c in q.columns), None)
        time_col = next((c for c in time_fields if c in q.columns), None)
        url_col = next((c for c in url_fields if c in q.columns), None)
        for _, r in q.iterrows():
            text = " | ".join(_clean(r.get(c)) for c in content_cols if _clean(r.get(c)))
            if not text:
                continue
            captured = str(r.get(time_col, "")) if time_col else now
            url = str(r.get(url_col, "")) if url_col else ""
            stable_ai_id = "AI-" + _sha({"file": p.name, "code": _norm_code(r.get(code_col, "")) if code_col else "", "text": text})[:24]
            rows.append({
                "source_id": stable_ai_id, "source_type": "AI_COMMENT", "source_name": f"AI_COMMENT:{p.name}", "source_url": url,
                "code": _norm_code(r.get(code_col, "")) if code_col else "", "name": str(r.get(name_col, "")) if name_col else "",
                "title": text[:240], "summary": text, "published_at": captured if time_col else "", "first_seen_at": now,
                "causal_mode": "FORWARD_CAUSAL", "official_source": False, "independent_source": False,
                "event_type": "AI_COMMENT_HINT", "materiality": "UNKNOWN", "direct_benefit": "UNKNOWN",
                "thesis_validity": "UNVERIFIED", "new_fact": "UNKNOWN", "query_text": "AI_COMMENT_CAPTURE",
            })
    return normalize_rows(rows)


def _append_audit(path: Path, rows: list[dict]) -> None:
    new = pd.DataFrame(rows)
    old = _read_csv(path)
    pd.concat([old, new], ignore_index=True).to_csv(path, index=False, encoding="utf-8-sig")


def _write_raw_snapshot(output_dir: Path, source_name: str, frame: pd.DataFrame) -> str:
    if frame.empty:
        return ""
    now = datetime.now().astimezone()
    folder = output_dir / "catalyst_raw" / now.strftime("%Y%m%d")
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{now.strftime('%H%M%S')}_{source_name.lower()}.jsonl"
    with path.open("w", encoding="utf-8") as f:
        for row in frame.fillna("").to_dict("records"):
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
    return str(path)


def capture_market_sector_context(output_dir: str | Path) -> pd.DataFrame:
    out = Path(output_dir or "reports")
    out.mkdir(parents=True, exist_ok=True)
    now = _now_iso()
    existing = _read_csv(out / MARKET_LEDGER_FILE)
    rows: list[dict] = []
    sources = ["v73_daily_sector_regime.csv", "v73_sector_tape_snapshot.csv", "v72_market_excess_benchmark_daily.csv"]
    for filename in sources:
        q = _read_csv(out / filename)
        if q.empty:
            continue
        date_col = next((c for c in ("signal_date", "date", "Date", "기준일", "날짜") if c in q.columns), None)
        code_col = next((c for c in ("code", "Code", "종목코드") if c in q.columns), None)
        name_col = next((c for c in ("name", "Name", "종목명") if c in q.columns), None)
        sec_col = next((c for c in ("sector", "Sector", "업종", "섹터", "theme", "테마") if c in q.columns), None)
        regime_col = next((c for c in ("market_regime", "regime", "시장국면") if c in q.columns), None)
        sec_ret_col = next((c for c in ("sector_return_5d", "sector_5d_median_pct", "sector_ret_5d", "업종5일수익률") if c in q.columns), None)
        sec_turn_col = next((c for c in ("sector_turnover_ratio", "sector_volume_ratio", "업종거래대금비율") if c in q.columns), None)
        sec_up_col = next((c for c in ("sector_up_ratio_pct", "breadth_pct", "상승종목비율") if c in q.columns), None)
        true_col = next((c for c in ("true_sector_index", "is_official_sector_index") if c in q.columns), None)
        for _, r in q.tail(5000).iterrows():
            sector = _clean(r.get(sec_col, "")) if sec_col else ""
            code = _norm_code(r.get(code_col, "")) if code_col else ""
            if not sector and not code and not regime_col:
                continue
            signal_date = str(r.get(date_col, ""))[:10] if date_col else now[:10]
            sec_ret = pd.to_numeric(pd.Series([r.get(sec_ret_col, "")]), errors="coerce").iloc[0] if sec_ret_col else float("nan")
            sec_turn = pd.to_numeric(pd.Series([r.get(sec_turn_col, "")]), errors="coerce").iloc[0] if sec_turn_col else float("nan")
            sec_up = pd.to_numeric(pd.Series([r.get(sec_up_col, "")]), errors="coerce").iloc[0] if sec_up_col else float("nan")
            positive = bool((pd.notna(sec_ret) and sec_ret > 0) or (pd.notna(sec_up) and sec_up >= 55))
            rows.append({
                "signal_date": signal_date, "signal_cutoff_at": now, "code": code,
                "name": _clean(r.get(name_col, "")) if name_col else "", "sector": sector,
                "market_regime": str(r.get(regime_col, "UNKNOWN")) if regime_col else "UNKNOWN",
                "market_return_5d": r.get("market_return_5d", ""), "market_turnover_ratio": r.get("market_turnover_ratio", ""),
                "market_investor_flow": r.get("market_investor_flow", ""), "sector_return_5d": sec_ret,
                "sector_turnover_ratio": sec_turn, "sector_up_ratio_pct": sec_up, "sector_positive": positive,
                "source_name": f"FORWARD:{filename}", "true_sector_index": _as_bool(r.get(true_col, False)) if true_col else False,
                "captured_at": now, "causal_mode": "FORWARD_CAUSAL",
            })
    incoming = pd.DataFrame(rows)
    combined = pd.concat([existing, incoming], ignore_index=True) if not existing.empty or not incoming.empty else pd.DataFrame(columns=[
        "signal_date", "signal_cutoff_at", "code", "name", "sector", "market_regime", "market_return_5d",
        "market_turnover_ratio", "market_investor_flow", "sector_return_5d", "sector_turnover_ratio",
        "sector_up_ratio_pct", "sector_positive", "source_name", "true_sector_index", "captured_at", "causal_mode",
    ])
    if not combined.empty:
        key_cols = [c for c in ("signal_date", "code", "sector", "source_name", "captured_at") if c in combined.columns]
        combined = combined.drop_duplicates(key_cols, keep="last") if key_cols else combined
    combined.to_csv(out / MARKET_LEDGER_FILE, index=False, encoding="utf-8-sig")
    _append_audit(out / MARKET_CAPTURE_AUDIT_FILE, [{
        "version": VERSION, "captured_at": now, "input_sources": "|".join(sources), "new_rows": len(incoming),
        "ledger_rows": len(combined), "true_sector_index_rows": int(combined.get("true_sector_index", pd.Series(dtype=str)).map(_as_bool).sum()) if not combined.empty else 0,
        "status": "CAPTURED" if len(incoming) else "NO_SOURCE_ROWS", "live_logic_changed": False,
    }])
    return combined


def capture_forward(output_dir: str, queries_path: str = "") -> pd.DataFrame:
    out = Path(output_dir or "reports")
    templates = ensure_templates(out)
    now = _now_iso()
    network = _as_bool(os.environ.get("CATALYST_NETWORK_ENABLE", "0"))
    queries = build_query_universe(out)
    if queries_path:
        external = _read_csv(Path(queries_path))
        if not external.empty:
            queries = external
            queries.to_csv(out / QUERY_FILE, index=False, encoding="utf-8-sig")
    query_rows = queries.fillna("").to_dict("records") if not queries.empty else []
    existing_master = _read_csv(out / COMPONENT_LEDGER_FILES["master"])
    component_existing = load_csv_ledgers([p for p in templates if p.name != COMPONENT_LEDGER_FILES["master"]])
    base_existing, _ = merge_append_only(existing_master, component_existing)

    frames: list[tuple[str, pd.DataFrame, str]] = []
    ai = discover_ai_comment_rows(out)
    frames.append(("AI_COMMENT", ai, "ai"))

    if network and query_rows:
        lookback_days = max(0, int(os.environ.get("CATALYST_LOOKBACK_DAYS", "3") or 3))
        today = datetime.now().astimezone().date()
        start = os.environ.get("CATALYST_START_DATE", (today - timedelta(days=lookback_days)).isoformat())
        end = os.environ.get("CATALYST_END_DATE", today.isoformat())
        dart_limit = int(os.environ.get("CATALYST_DART_MAX_CODES", "30") or 30)
        google_limit = int(os.environ.get("CATALYST_GOOGLE_MAX_QUERIES", "10") or 10)
        kakao_limit = int(os.environ.get("CATALYST_KAKAO_MAX_QUERIES", "30") or 30)
        company_codes = [r.get("code", "") for r in query_rows if r.get("code")][:dart_limit]
        frames.append(("OPENDART", fetch_opendart(company_codes, start, end, out / ".cache"), "official"))
        frames.append(("GOOGLE_CSE", fetch_google_cse(query_rows[:google_limit]), "news"))
        frames.append(("KAKAO_DAUM_WEB", fetch_kakao_web(query_rows[:kakao_limit]), "news"))

    incoming_frames = [f for _, f, _ in frames if isinstance(f, pd.DataFrame) and not f.empty]
    incoming = pd.concat(incoming_frames, ignore_index=True) if incoming_frames else empty_ledger()
    merged, merge_audit = merge_append_only(base_existing, incoming)
    merged.to_csv(out / COMPONENT_LEDGER_FILES["master"], index=False, encoding="utf-8-sig")

    source_audit_rows: list[dict] = []
    any_error = False
    any_new = False
    for source_name, frame, component in frames:
        stats = dict(getattr(frame, "attrs", {}).get("capture_stats", {}))
        comp_path = out / COMPONENT_LEDGER_FILES[component]
        comp_old = _read_csv(comp_path)
        comp_new, comp_audit = merge_append_only(comp_old, frame)
        comp_new.to_csv(comp_path, index=False, encoding="utf-8-sig")
        raw_path = _write_raw_snapshot(out, source_name, frame)
        errors = int(stats.get("errors", 0) or 0)
        any_error = any_error or errors > 0
        any_new = any_new or comp_audit["new_unique_rows"] > 0
        source_audit_rows.append({
            "version": VERSION, "captured_at": now, "source_name": source_name,
            "network_enabled": network, "enabled": stats.get("enabled", source_name == "AI_COMMENT"),
            "queries_attempted": stats.get("attempted", 0), "errors": errors,
            "returned_rows": len(frame), "new_unique_rows": comp_audit["new_unique_rows"],
            "component_ledger_rows": len(comp_new), "raw_snapshot_path": raw_path,
            "status": "ERROR" if errors and len(frame) == 0 else ("PARTIAL" if errors else ("CAPTURED" if len(frame) else "NO_ROWS")),
            "capture_profile": os.environ.get("TEST_PROFILE", "UNKNOWN"), "capture_run_id": os.environ.get("GITHUB_RUN_ID", "LOCAL"),
        })
    if not frames:
        source_audit_rows.append({"version": VERSION, "captured_at": now, "source_name": "NONE", "network_enabled": network, "status": "NO_SOURCES"})
    _append_audit(out / CAPTURE_AUDIT_FILE, source_audit_rows)
    _append_audit(out / FIRST_SEEN_AUDIT_FILE, [{
        "version": VERSION, "captured_at": now, **merge_audit,
        "status": "PASS" if merge_audit["first_seen_regressions"] == 0 else "FAIL",
        "live_logic_changed": False,
    }])
    capture_market_sector_context(out)

    overall = "PARTIAL" if any_error and any_new else ("CAPTURED" if any_new else ("PARTIAL" if any_error else ("LEDGER_READY" if len(merged) else "TEMPLATE_READY")))
    _append_audit(out / CAPTURE_AUDIT_FILE, [{
        "version": VERSION, "captured_at": now, "source_name": "OVERALL", "network_enabled": network,
        "queries": len(query_rows), "rows": len(merged), "new_unique_rows": merge_audit["new_unique_rows"],
        "opendart_enabled": bool(os.environ.get("OPENDART_API_KEY")),
        "google_enabled": bool(os.environ.get("GOOGLE_CSE_API_KEY") and os.environ.get("GOOGLE_CSE_ID")),
        "kakao_enabled": bool(os.environ.get("KAKAO_REST_API_KEY")),
        "status": overall, "first_seen_regressions": merge_audit["first_seen_regressions"],
        "live_logic_changed": False, "real_order_changed": False,
    }])
    return merged


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--capture", action="store_true")
    ap.add_argument("--build-queries", action="store_true")
    ap.add_argument("--capture-market", action="store_true")
    ap.add_argument("--output-dir", default="reports")
    ap.add_argument("--queries", default="")
    args = ap.parse_args()
    if args.build_queries:
        q = build_query_universe(args.output_dir)
        print(f"CATALYST_QUERY_UNIVERSE version={VERSION} rows={len(q)} output={args.output_dir}")
    if args.capture_market:
        m = capture_market_sector_context(args.output_dir)
        print(f"MARKET_SECTOR_FORWARD_CAPTURE version={VERSION} rows={len(m)} output={args.output_dir}")
    if args.capture:
        q = capture_forward(args.output_dir, args.queries)
        print(f"CATALYST_SOURCE_CAPTURE version={VERSION} rows={len(q)} output={args.output_dir}")
    if not (args.capture or args.build_queries or args.capture_market):
        ensure_templates(Path(args.output_dir))
        print(f"CATALYST_SOURCE_TEMPLATES version={VERSION} output={args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
