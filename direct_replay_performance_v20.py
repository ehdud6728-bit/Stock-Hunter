from __future__ import annotations

import gzip
import hashlib
import json
import os
import pickle
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import pandas as pd

VERSION = "V73.3.6.6.20"
RESEARCH_ONLY = True
LIVE_LOGIC_CHANGED = False
REAL_ORDER_CHANGED = False
HEADER = "⚡ [TOP500 Direct Replay 캐시·재개·진행률 진단 · RESEARCH_ONLY]"
AUDIT_FILE = "v73_direct_replay_performance_audit.csv"
REPORT_FILE = "v73_direct_replay_performance_report.txt"
SCHEMA = "V20_PERF_SCHEMA_2"

_LOCK = threading.RLock()
_ORIGINAL_PROVIDER: Callable[..., pd.DataFrame] | None = None
_CACHE_ROOT = Path("reports/.cache/v20_price_history")
_CHECKPOINT_ROOT = Path("reports/.cache/v20_replay_checkpoint")
_OUTPUT_DIR = Path("reports")
_SOURCE_FINGERPRINT = "UNKNOWN"
_REQUIRED_ASOF: pd.Timestamp | None = None
_MEMORY: dict[tuple[str, int], pd.DataFrame] = {}
_RUN_START = time.monotonic()
_DATE_COUNT = 0
_STATS: dict[str, float] = {
    "memory_hit": 0,
    "disk_hit": 0,
    "network_fetch": 0,
    "network_error": 0,
    "disk_invalid": 0,
    "prefetch_codes": 0,
    "prefetch_completed": 0,
    "checkpoint_hit": 0,
    "checkpoint_miss": 0,
    "checkpoint_saved": 0,
    "checkpoint_invalid": 0,
}


def _env_bool(name: str, default: bool = False) -> bool:
    v = str(os.getenv(name, "1" if default else "0")).strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(str(os.getenv(name, default)).strip()))
    except Exception:
        return int(default)


def _norm_code(v: Any) -> str:
    s = re.sub(r"\D", "", str(v or ""))
    return s[-6:].zfill(6) if s else str(v or "").strip()


def _log(msg: str, log_fn: Callable[[str], Any] | None = None) -> None:
    try:
        if callable(log_fn):
            log_fn(msg)
        else:
            print(msg, flush=True)
    except Exception:
        try:
            print(msg, flush=True)
        except Exception:
            pass


def _prune_files(root: Path, pattern: str, max_files: int) -> None:
    try:
        files=sorted(root.glob(pattern), key=lambda x:x.stat().st_mtime, reverse=True)
        for f in files[max(1,max_files):]:
            try: f.unlink()
            except Exception: pass
    except Exception: pass

def configure(
    original_provider: Callable[..., pd.DataFrame],
    output_dir: str | Path = "reports",
    source_fingerprint: str = "UNKNOWN",
) -> Callable[..., pd.DataFrame]:
    """Configure the weekly-backtest-only persistent price provider.

    The caller must decide whether to install it. This module never mutates scanner globals by itself.
    """
    global _ORIGINAL_PROVIDER, _CACHE_ROOT, _CHECKPOINT_ROOT, _OUTPUT_DIR, _SOURCE_FINGERPRINT
    _ORIGINAL_PROVIDER = original_provider
    _OUTPUT_DIR = Path(output_dir or "reports")
    _CACHE_ROOT = Path(os.getenv("V20_PRICE_CACHE_DIR", str(_OUTPUT_DIR / ".cache/v20_price_history")))
    _CHECKPOINT_ROOT = Path(os.getenv("V20_REPLAY_CHECKPOINT_DIR", str(_OUTPUT_DIR / ".cache/v20_replay_checkpoint")))
    _SOURCE_FINGERPRINT = str(source_fingerprint or os.getenv("GITHUB_SHA") or "UNKNOWN")[:64]
    _CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    _CHECKPOINT_ROOT.mkdir(parents=True, exist_ok=True)
    _prune_files(_CACHE_ROOT, "*.pkl.gz", _env_int("V20_PRICE_CACHE_MAX_FILES", 1800))
    _prune_files(_CHECKPOINT_ROOT, "*.pkl.gz", _env_int("V20_CHECKPOINT_MAX_FILES", 120))
    return cached_price_reader


def set_required_asof(asof_date: Any) -> None:
    global _REQUIRED_ASOF
    try:
        _REQUIRED_ASOF = pd.Timestamp(asof_date).normalize()
    except Exception:
        _REQUIRED_ASOF = None


def _cache_file(code: str, days: int) -> Path:
    return _CACHE_ROOT / f"{_norm_code(code)}_{int(days)}.pkl.gz"


def _frame_covers_required(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return False
    if _REQUIRED_ASOF is None:
        return True
    try:
        idx = pd.to_datetime(df.index, errors="coerce")
        idx = idx[idx.notna()]
        return len(idx) > 0 and pd.Timestamp(idx.max()).normalize() >= _REQUIRED_ASOF
    except Exception:
        return False


def _atomic_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    with gzip.open(tmp, "wb", compresslevel=3) as fh:
        pickle.dump(payload, fh, protocol=pickle.HIGHEST_PROTOCOL)
    os.replace(tmp, path)


def _load_dump(path: Path) -> Any:
    with gzip.open(path, "rb") as fh:
        return pickle.load(fh)


def cached_price_reader(ticker: str, days: int = 900) -> pd.DataFrame:
    """Persistent, coverage-aware replacement for the original in-memory fdr_cached provider."""
    code = _norm_code(ticker)
    days_i = max(30, int(days or 900))
    key = (code, days_i)
    with _LOCK:
        mem = _MEMORY.get(key)
    if isinstance(mem, pd.DataFrame) and _frame_covers_required(mem):
        with _LOCK:
            _STATS["memory_hit"] += 1
        return mem

    path = _cache_file(code, days_i)
    if _env_bool("V20_PRICE_DISK_CACHE_ENABLE", True) and path.exists():
        try:
            payload = _load_dump(path)
            df = payload.get("frame") if isinstance(payload, dict) else payload
            if isinstance(df, pd.DataFrame) and _frame_covers_required(df):
                with _LOCK:
                    _MEMORY[key] = df
                    _STATS["disk_hit"] += 1
                return df
            with _LOCK:
                _STATS["disk_invalid"] += 1
        except Exception:
            with _LOCK:
                _STATS["disk_invalid"] += 1

    provider = _ORIGINAL_PROVIDER
    if not callable(provider):
        with _LOCK:
            _STATS["network_error"] += 1
        return pd.DataFrame()
    try:
        df = provider(code, days=days_i)
        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame()
        with _LOCK:
            _STATS["network_fetch"] += 1
            _MEMORY[key] = df
        if not df.empty and _env_bool("V20_PRICE_DISK_CACHE_ENABLE", True):
            try:
                _atomic_dump(path, {
                    "schema": SCHEMA,
                    "code": code,
                    "days": days_i,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    "frame": df,
                })
            except Exception:
                pass
        return df
    except Exception:
        with _LOCK:
            _STATS["network_error"] += 1
        return pd.DataFrame()


def prefetch_codes(codes: Iterable[Any], asof_date: Any = None, days: int = 900, log_fn: Callable[[str], Any] | None = None) -> dict[str, int]:
    if not _env_bool("V20_PRICE_PREFETCH_ENABLE", True):
        return {"requested": 0, "completed": 0, "nonempty": 0}
    if asof_date is not None:
        set_required_asof(asof_date)
    uniq = []
    seen = set()
    for v in codes or []:
        c = _norm_code(v)
        if c and c not in seen:
            seen.add(c); uniq.append(c)
    if not uniq:
        return {"requested": 0, "completed": 0, "nonempty": 0}
    workers = max(1, min(12, _env_int("V20_PRICE_PREFETCH_WORKERS", 6)))
    every = max(10, _env_int("V20_PROGRESS_EVERY", 50))
    with _LOCK:
        _STATS["prefetch_codes"] += len(uniq)
    t0 = time.monotonic()
    done = nonempty = 0
    _log(f"⚡ V20 price prefetch start | asof={pd.Timestamp(asof_date).date() if asof_date is not None else '-'} | codes={len(uniq)} | workers={workers}", log_fn)
    ex = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="v20-price")
    futures = {ex.submit(cached_price_reader, c, days): c for c in uniq}
    try:
        for fut in as_completed(futures):
            done += 1
            try:
                q = fut.result()
                if isinstance(q, pd.DataFrame) and not q.empty:
                    nonempty += 1
            except Exception:
                pass
            if done % every == 0 or done == len(uniq):
                elapsed = max(0.001, time.monotonic() - t0)
                rate = done / elapsed
                eta = (len(uniq) - done) / rate if rate > 0 else 0
                _log(f"⚡ V20 price prefetch {done}/{len(uniq)} | nonempty={nonempty} | elapsed={elapsed/60:.1f}m | ETA={eta/60:.1f}m", log_fn)
    finally:
        ex.shutdown(wait=True, cancel_futures=False)
    with _LOCK:
        _STATS["prefetch_completed"] += done
    return {"requested": len(uniq), "completed": done, "nonempty": nonempty}


def _config_signature(asof_date: Any) -> str:
    keys = [
        "V1081_DIRECT_TOP_N", "V1081_DIRECT_LIMIT_PER_DATE", "V1081_ASOF_LIQUIDITY_DAYS",
        "V1081_ASOF_MIN_PRICE", "V1081_ASOF_MIN_MARCAP", "V1081_EVENT_EXPANSION_MAX",
        "V1081_EVENT_AMOUNT_RATIO", "V1081_EVENT_VOLUME_RATIO", "V1081_EVENT_PREV_RET_PCT",
        "V1081_EVENT_MIN_AMOUNT", "V1081_DIRECT_DATE_MODE",
    ]
    payload = {
        "schema": SCHEMA,
        "version": VERSION,
        "source": _SOURCE_FINGERPRINT,
        "asof": pd.Timestamp(asof_date).normalize().strftime("%Y-%m-%d"),
        "env": {k: str(os.getenv(k, "")) for k in keys},
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:24]


def _checkpoint_file(asof_date: Any) -> Path:
    d = pd.Timestamp(asof_date).normalize().strftime("%Y%m%d")
    return _CHECKPOINT_ROOT / f"{d}_{_config_signature(asof_date)}.pkl.gz"


def load_checkpoint(asof_date: Any) -> dict[str, Any] | None:
    if not _env_bool("V20_REPLAY_RESUME_ENABLE", True):
        return None
    p = _checkpoint_file(asof_date)
    if not p.exists():
        with _LOCK: _STATS["checkpoint_miss"] += 1
        return None
    try:
        z = _load_dump(p)
        if not isinstance(z, dict) or z.get("schema") != SCHEMA or z.get("signature") != _config_signature(asof_date):
            with _LOCK: _STATS["checkpoint_invalid"] += 1
            return None
        rows = z.get("candidate_rows")
        if not isinstance(rows, list):
            with _LOCK: _STATS["checkpoint_invalid"] += 1
            return None
        with _LOCK: _STATS["checkpoint_hit"] += 1
        return z
    except Exception:
        with _LOCK: _STATS["checkpoint_invalid"] += 1
        return None


def save_checkpoint(
    asof_date: Any,
    candidate_rows: list[Any],
    capture_rows: list[dict[str, Any]] | None = None,
    attempt_rows: list[dict[str, Any]] | None = None,
    universe_membership: pd.DataFrame | None = None,
    universe_summary: pd.DataFrame | None = None,
    universe_availability: pd.DataFrame | None = None,
    runtime_sidecars: dict[str, Any] | None = None,
) -> Path | None:
    if not _env_bool("V20_REPLAY_RESUME_ENABLE", True):
        return None
    p = _checkpoint_file(asof_date)
    payload = {
        "schema": SCHEMA,
        "version": VERSION,
        "signature": _config_signature(asof_date),
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "candidate_rows": list(candidate_rows or []),
        "capture_rows": list(capture_rows or []),
        "attempt_rows": list(attempt_rows or []),
        "universe_membership": universe_membership if isinstance(universe_membership, pd.DataFrame) else pd.DataFrame(),
        "universe_summary": universe_summary if isinstance(universe_summary, pd.DataFrame) else pd.DataFrame(),
        "universe_availability": universe_availability if isinstance(universe_availability, pd.DataFrame) else pd.DataFrame(),
        # Small per-date research sidecars whose wrappers must still see the same evidence on resume.
        # Do not place large OHLC history maps here; those are rebuilt from the persistent price cache.
        "runtime_sidecars": dict(runtime_sidecars or {}),
    }
    try:
        _atomic_dump(p, payload)
        with _LOCK: _STATS["checkpoint_saved"] += 1
        return p
    except Exception:
        return None


def read_universe_rows(output_dir: str | Path, asof_date: Any) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    out = Path(output_dir or "reports")
    ds = pd.Timestamp(asof_date).normalize()
    def _read(name: str) -> pd.DataFrame:
        p = out / name
        if not p.exists(): return pd.DataFrame()
        try:
            q = pd.read_csv(p, dtype={"code": str})
            if "signal_date" in q.columns:
                d = pd.to_datetime(q["signal_date"], errors="coerce").dt.normalize()
                q = q[d.eq(ds)].copy()
            return q
        except Exception:
            return pd.DataFrame()
    return (
        _read("v73_universe_asof_membership.csv"),
        _read("v73_universe_asof_summary.csv"),
        _read("v73_universe_data_availability.csv"),
    )


def progress_start(asof_date: Any, total_dates: int, cache_hit: bool, log_fn: Callable[[str], Any] | None = None) -> int:
    global _DATE_COUNT
    with _LOCK:
        _DATE_COUNT += 1
        idx = _DATE_COUNT
    elapsed = max(0.001, time.monotonic() - _RUN_START)
    _log(
        f"⚡ [V20 PROGRESS] date {idx}/{max(idx,total_dates)} | {pd.Timestamp(asof_date).strftime('%Y-%m-%d')} | checkpoint={'HIT' if cache_hit else 'MISS'} | elapsed={elapsed/60:.1f}m",
        log_fn,
    )
    return idx


def progress_done(asof_date: Any, idx: int, total_dates: int, rows: int, cache_hit: bool, started: float, log_fn: Callable[[str], Any] | None = None) -> None:
    elapsed_total = max(0.001, time.monotonic() - _RUN_START)
    elapsed_date = max(0.001, time.monotonic() - started)
    rate = idx / elapsed_total if idx else 0
    eta = (max(total_dates, idx) - idx) / rate if rate > 0 else 0
    _log(
        f"⚡ [V20 PROGRESS] done {idx}/{max(idx,total_dates)} | {pd.Timestamp(asof_date).strftime('%Y-%m-%d')} | candidates={rows} | checkpoint={'HIT' if cache_hit else 'MISS'} | date={elapsed_date/60:.1f}m | total={elapsed_total/60:.1f}m | ETA={max(0,eta)/60:.1f}m",
        log_fn,
    )


def reset_run_stats() -> None:
    global _RUN_START, _DATE_COUNT
    _RUN_START = time.monotonic()
    _DATE_COUNT = 0
    with _LOCK:
        for k in list(_STATS): _STATS[k] = 0


def stats_snapshot() -> dict[str, Any]:
    with _LOCK:
        z = dict(_STATS)
    z["elapsed_sec"] = round(max(0.0, time.monotonic() - _RUN_START), 3)
    z["memory_entries"] = len(_MEMORY)
    z["price_cache_files"] = len(list(_CACHE_ROOT.glob("*.pkl.gz"))) if _CACHE_ROOT.exists() else 0
    z["checkpoint_files"] = len(list(_CHECKPOINT_ROOT.glob("*.pkl.gz"))) if _CHECKPOINT_ROOT.exists() else 0
    return z


def finalize(output_dir: str | Path = "reports", base_report: str = "") -> tuple[str, pd.DataFrame]:
    out = Path(output_dir or "reports"); out.mkdir(parents=True, exist_ok=True)
    st = stats_snapshot()
    row = {
        "version": VERSION,
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "source_fingerprint": _SOURCE_FINGERPRINT,
        "top_n": os.getenv("V1081_DIRECT_TOP_N", "500"),
        "weeks": os.getenv("V1080_BACKTEST_WEEKS", "24"),
        "prefetch_workers": os.getenv("V20_PRICE_PREFETCH_WORKERS", "6"),
        "resume_enabled": _env_bool("V20_REPLAY_RESUME_ENABLE", True),
        **st,
        "research_only": True,
        "live_logic_changed": False,
        "real_order_changed": False,
    }
    df = pd.DataFrame([row])
    p = out / AUDIT_FILE
    try:
        old = pd.read_csv(p) if p.exists() else pd.DataFrame()
    except Exception:
        old = pd.DataFrame()
    pd.concat([old, df], ignore_index=True, sort=False).tail(200).to_csv(p, index=False, encoding="utf-8-sig")
    asof_line = "- As-of 원천캐시: availability 원장 없음"
    try:
        ap = out / "v73_universe_data_availability.csv"
        if ap.exists():
            aq = pd.read_csv(ap)
            def _sum(col):
                return int(pd.to_numeric(aq.get(col, 0), errors="coerce").fillna(0).sum()) if len(aq) else 0
            asof_line = (
                f"- As-of 원천캐시: listing hit {_sum('v20_listing_cache_hit')} | "
                f"market hit/miss {_sum('v20_market_cache_hit')}/{_sum('v20_market_cache_miss')} | "
                f"cap hit/miss {_sum('v20_cap_cache_hit')}/{_sum('v20_cap_cache_miss')}"
            )
    except Exception:
        pass
    block = "\n".join([
        HEADER,
        f"📌 {VERSION} · persistent price cache + as-of snapshot cache + per-date resumable checkpoint + progress/ETA",
        f"- 실행시간: {st['elapsed_sec']/60:.1f}분 | checkpoint hit {int(st['checkpoint_hit'])} / miss {int(st['checkpoint_miss'])} / saved {int(st['checkpoint_saved'])}",
        f"- 가격캐시: memory hit {int(st['memory_hit'])} | disk hit {int(st['disk_hit'])} | network {int(st['network_fetch'])} | error {int(st['network_error'])} | files {int(st['price_cache_files'])}",
        asof_line,
        f"- Prefetch: requested {int(st['prefetch_codes'])} | completed {int(st['prefetch_completed'])} | workers {os.getenv('V20_PRICE_PREFETCH_WORKERS','6')}",
        f"- 재개 체크포인트 파일: {int(st['checkpoint_files'])} | 소스 fingerprint {str(_SOURCE_FINGERPRINT)[:16]}",
        "- 재개 안전계약: 후보행뿐 아니라 AUX 연구 sidecar를 함께 복원해 기존 wrapper-chain의 모집단/후속진단이 사라지지 않게 합니다.",
        "- 안전계약: 캐시는 원천 OHLC/당시 universe 계산을 재사용할 뿐 검색식·점수·후보순위·진입·청산·주문 로직은 변경하지 않습니다.",
        f"- Actions CSV: {AUDIT_FILE}",
    ])
    (out / REPORT_FILE).write_text(block, encoding="utf-8")
    text = str(base_report or "")
    if HEADER in text:
        text = text[:text.find(HEADER)].rstrip()
    return text.rstrip() + "\n\n" + block, df


def force_report(text: str, output_dir: str | Path = "reports") -> str:
    p = Path(output_dir or "reports") / REPORT_FILE
    if not p.exists(): return str(text or "")
    try: block = p.read_text(encoding="utf-8")
    except Exception: return str(text or "")
    raw = str(text or "")
    if HEADER in raw: raw = raw[:raw.find(HEADER)].rstrip()
    return raw.rstrip() + "\n\n" + block.strip()
