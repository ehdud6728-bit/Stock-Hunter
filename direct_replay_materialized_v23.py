from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import pickle
import re
import shutil
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

VERSION = "V73.3.6.6.23"
RESEARCH_ONLY = True
LIVE_LOGIC_CHANGED = False
REAL_ORDER_CHANGED = False
HEADER = "🚄 [TOP500 6-Shard Materialized Result × Merge-Only Parent × Zero-Recompute · RESEARCH_ONLY]"
SCHEMA = "V23_MATERIALIZED_SCHEMA_1"

MATERIALIZED_DIRNAME = "v23_materialized"
SHARD_MANIFEST_JSON = "v73_v23_shard_manifest.json"
SHARD_MANIFEST_CSV = "v73_v23_shard_manifest.csv"
SHARD_MANIFEST_REPORT = "v73_v23_shard_manifest_report.txt"
MERGE_AUDIT_JSON = "v73_v23_handoff_merge_audit.json"
MERGE_AUDIT_CSV = "v73_v23_handoff_merge_audit.csv"
MERGE_AUDIT_REPORT = "v73_v23_handoff_merge_audit_report.txt"
PARENT_PREFLIGHT_JSON = "v73_v23_parent_preflight.json"
PARENT_PREFLIGHT_REPORT = "v73_v23_parent_preflight_report.txt"
REPORT_FILE = "v73_v23_zero_recompute_report.txt"

CACHE_DIRS = (
    ".cache/v20_price_history",
    ".cache/v20_asof_snapshots",
    ".cache/v20_replay_checkpoint",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(str(os.getenv(name, default)).strip()))
    except Exception:
        return int(default)


def _env_bool(name: str, default: bool = False) -> bool:
    v = str(os.getenv(name, "1" if default else "0")).strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


def _handoff_cohort_id() -> str:
    raw = str(os.getenv("V23_HANDOFF_COHORT_ID", os.getenv("V25_COHORT_MODE", "ROLLING"))).strip().upper()
    aliases = {"": "ROLLING", "COHORT_A": "A", "COHORT_B": "B", "COHORT_C": "C", "COHORT_D": "D"}
    return aliases.get(raw, raw)


def _manifest_shard_key(manifest: dict[str, Any]) -> str:
    cohort = str(manifest.get("cohort_id", "ROLLING") or "ROLLING").strip().upper()
    try:
        idx = int(manifest.get("shard_index", -1))
    except Exception:
        idx = -1
    return f"{cohort}:{idx}"


def _expected_handoff_shape(manifests: list[dict[str, Any]]) -> tuple[int, int, list[str], dict[str, int]]:
    """Resolve shard/date denominator without pretending every explicit 6-month cohort is exactly 24 weeks."""
    local_shards = _env_int("V23_SHARD_COUNT", 6)
    mode = str(os.getenv("V25_COHORT_MODE", "ROLLING")).strip().upper()
    cohort_counts: dict[str, int] = {}
    for m in manifests:
        cohort = str(m.get("cohort_id", "ROLLING") or "ROLLING").strip().upper()
        try:
            n = int(m.get("all_date_count", 0) or 0)
        except Exception:
            n = 0
        cohort_counts[cohort] = max(cohort_counts.get(cohort, 0), n)
    if mode == "ALL":
        expected_cohorts = ["A", "B", "C", "D"]
        expected_shards = local_shards * len(expected_cohorts)
        expected_dates = sum(cohort_counts.get(c, 0) for c in expected_cohorts)
        return expected_shards, expected_dates, expected_cohorts, cohort_counts
    explicit = mode in {"A", "B", "C", "D", "CUSTOM"}
    if explicit and cohort_counts:
        expected_dates = max(cohort_counts.values())
    else:
        expected_dates = _env_int("V1080_BACKTEST_WEEKS", 24)
    return local_shards, expected_dates, [mode or "ROLLING"], cohort_counts


def _norm_date(v: Any) -> pd.Timestamp:
    return pd.Timestamp(v).normalize()


def partition_dates(
    dates: Iterable[Any],
    shard_index: int,
    shard_count: int,
    *,
    newest_first: bool = True,
) -> list[pd.Timestamp]:
    """Balanced contiguous date shards; each shard executes newest-first."""
    ds = sorted({_norm_date(x) for x in dates})
    n = max(1, int(shard_count))
    i = max(0, min(int(shard_index), n - 1))
    q, r = divmod(len(ds), n)
    start = i * q + min(i, r)
    size = q + (1 if i < r else 0)
    out = ds[start : start + size]
    return list(reversed(out)) if newest_first else out


def _source_fingerprint() -> str:
    gh = str(os.getenv("GITHUB_SHA", "")).strip()
    if gh:
        return gh[:40]
    h = hashlib.sha256()
    for name in (
        "main7_bugfix_2.py",
        "historical_asof_universe.py",
        "direct_replay_performance_v20.py",
        "search_formula_universe_audit.py",
        "direct_replay_materialized_v23.py",
        "research_readiness_v24.py",
        "original_thesis_reconstruction.py",
    ):
        p = Path(name)
        h.update(name.encode("utf-8")); h.update(b"\0")
        try:
            h.update(p.read_bytes())
        except Exception:
            h.update(b"MISSING")
        h.update(b"\0")
    return h.hexdigest()[:40]


def _config_payload() -> dict[str, str]:
    keys = (
        "V1081_BACKTEST_SOURCE",
        "V1081_DIRECT_TOP_N",
        "V1081_DIRECT_LIMIT_PER_DATE",
        "V1080_BACKTEST_WEEKS",
        "V1080_BACKTEST_HOLD_DAYS",
        "V1080_BACKTEST_STOP_PCT",
        "V1082_BACKTEST_STOP_MODE",
        "V1081_DIRECT_DATE_MODE",
        "V1081_UNIVERSE_MODE",
        "V1081_ASOF_LIQUIDITY_DAYS",
        "V1081_ASOF_MIN_PRICE",
        "V1081_ASOF_MIN_MARCAP",
        "V1081_EVENT_EXPANSION_MAX",
        "V1081_EVENT_AMOUNT_RATIO",
        "V1081_EVENT_VOLUME_RATIO",
        "V1081_EVENT_PREV_RET_PCT",
        "V1081_EVENT_MIN_AMOUNT",
        "V23_SHARD_COUNT",
        "V24_FORMULA_SHADOW_ENABLE",
        "V25_ORIGINAL_THESIS_ENABLE",
        "V25_ORIGINAL_THESIS_REV",
    )
    return {k: str(os.getenv(k, "")) for k in keys}


def current_identity() -> dict[str, Any]:
    cfg = _config_payload()
    cfg_hash = hashlib.sha256(json.dumps(cfg, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:24]
    return {
        "schema": SCHEMA,
        "version": VERSION,
        "source_fingerprint": _source_fingerprint(),
        "config_fingerprint": cfg_hash,
        "config": cfg,
    }


def _materialized_root(output_dir: str | Path) -> Path:
    explicit = str(os.getenv("V23_MATERIALIZED_DIR", "")).strip()
    return Path(explicit) if explicit else Path(output_dir or "reports") / MATERIALIZED_DIRNAME


def materialized_path(output_dir: str | Path, asof_date: Any) -> Path:
    return _materialized_root(output_dir) / f"date_{_norm_date(asof_date).strftime('%Y%m%d')}.pkl.gz"


def _atomic_pickle(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    with gzip.open(tmp, "wb", compresslevel=3) as fh:
        pickle.dump(payload, fh, protocol=pickle.HIGHEST_PROTOCOL)
    os.replace(tmp, path)


def _load_pickle(path: Path) -> Any:
    with gzip.open(path, "rb") as fh:
        return pickle.load(fh)


def file_sha256(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_materialized_date(
    output_dir: str | Path,
    asof_date: Any,
    checkpoint: dict[str, Any],
    *,
    shard_index: int,
    shard_count: int,
) -> dict[str, Any]:
    if not isinstance(checkpoint, dict):
        raise TypeError("checkpoint must be dict")
    rows = checkpoint.get("candidate_rows")
    if not isinstance(rows, list):
        raise ValueError("checkpoint candidate_rows missing")
    ident = current_identity()
    ds = _norm_date(asof_date).strftime("%Y-%m-%d")
    payload = {
        **ident,
        "signal_date": ds,
        "created_at": _utc_now(),
        "shard_index": int(shard_index),
        "shard_count": int(shard_count),
        "candidate_rows": list(rows),
        "capture_rows": list(checkpoint.get("capture_rows") or []),
        "attempt_rows": list(checkpoint.get("attempt_rows") or []),
        "universe_membership": checkpoint.get("universe_membership") if isinstance(checkpoint.get("universe_membership"), pd.DataFrame) else pd.DataFrame(),
        "universe_summary": checkpoint.get("universe_summary") if isinstance(checkpoint.get("universe_summary"), pd.DataFrame) else pd.DataFrame(),
        "universe_availability": checkpoint.get("universe_availability") if isinstance(checkpoint.get("universe_availability"), pd.DataFrame) else pd.DataFrame(),
        "runtime_sidecars": dict(checkpoint.get("runtime_sidecars") or {}),
        "upstream_checkpoint_schema": checkpoint.get("schema", ""),
        "upstream_checkpoint_signature": checkpoint.get("signature", ""),
    }
    p = materialized_path(output_dir, asof_date)
    _atomic_pickle(p, payload)
    sha = file_sha256(p)
    return {
        "signal_date": ds,
        "file": p.name,
        "sha256": sha,
        "candidate_rows": len(payload["candidate_rows"]),
        "capture_rows": len(payload["capture_rows"]),
        "attempt_rows": len(payload["attempt_rows"]),
        "universe_rows": len(payload["universe_membership"]),
        "source_fingerprint": ident["source_fingerprint"],
        "config_fingerprint": ident["config_fingerprint"],
    }


def load_materialized_date(output_dir: str | Path, asof_date: Any, *, require_current_identity: bool = True) -> dict[str, Any] | None:
    p = materialized_path(output_dir, asof_date)
    if not p.exists():
        return None
    try:
        z = _load_pickle(p)
    except Exception:
        return None
    if not isinstance(z, dict) or z.get("schema") != SCHEMA or z.get("version") != VERSION:
        return None
    if str(z.get("signal_date")) != _norm_date(asof_date).strftime("%Y-%m-%d"):
        return None
    if not isinstance(z.get("candidate_rows"), list):
        return None
    if require_current_identity:
        ident = current_identity()
        if z.get("source_fingerprint") != ident["source_fingerprint"] or z.get("config_fingerprint") != ident["config_fingerprint"]:
            return None
    return z


def _date_file_stats(output_dir: str | Path, selected_dates: Iterable[Any]) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    missing: list[str] = []
    for d in selected_dates:
        ds = _norm_date(d).strftime("%Y-%m-%d")
        p = materialized_path(output_dir, d)
        z = load_materialized_date(output_dir, d, require_current_identity=True)
        if z is None:
            missing.append(ds)
            continue
        rows.append({
            "signal_date": ds,
            "file": p.name,
            "sha256": file_sha256(p),
            "candidate_rows": len(z.get("candidate_rows") or []),
            "capture_rows": len(z.get("capture_rows") or []),
            "attempt_rows": len(z.get("attempt_rows") or []),
            "universe_rows": len(z.get("universe_membership")) if isinstance(z.get("universe_membership"), pd.DataFrame) else 0,
        })
    return rows, missing


def write_shard_manifest(
    output_dir: str | Path,
    *,
    shard_index: int,
    shard_count: int,
    all_dates: Iterable[Any],
    selected_dates: Iterable[Any],
    errors: list[str] | None = None,
    elapsed_sec: float = 0.0,
) -> dict[str, Any]:
    out = Path(output_dir or "reports"); out.mkdir(parents=True, exist_ok=True)
    all_ds = [_norm_date(x) for x in all_dates]
    sel = [_norm_date(x) for x in selected_dates]
    date_rows, missing = _date_file_stats(out, sel)
    ident = current_identity()
    errs = list(errors or [])
    status = "COMPLETE" if sel and not missing and not errs else ("EMPTY" if not sel else "PARTIAL")
    manifest = {
        **ident,
        "research_only": True,
        "live_logic_changed": False,
        "real_order_changed": False,
        "created_at": _utc_now(),
        "status": status,
        "cohort_id": _handoff_cohort_id(),
        "shard_index": int(shard_index),
        "shard_count": int(shard_count),
        "all_date_count": len(all_ds),
        "selected_date_count": len(sel),
        "selected_dates": [x.strftime("%Y-%m-%d") for x in sel],
        "materialized_date_count": len(date_rows),
        "materialized_dates": [r["signal_date"] for r in date_rows],
        "missing_dates": missing,
        "date_results": date_rows,
        "candidate_total": int(sum(r["candidate_rows"] for r in date_rows)),
        "capture_total": int(sum(r["capture_rows"] for r in date_rows)),
        "attempt_total": int(sum(r["attempt_rows"] for r in date_rows)),
        "errors": errs,
        "elapsed_sec": float(elapsed_sec or 0.0),
        "newest_first": True,
        "zero_recompute_parent": True,
    }
    (out / SHARD_MANIFEST_JSON).write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    pd.DataFrame([{
        "version": VERSION,
        "status": status,
        "cohort_id": manifest.get("cohort_id", _handoff_cohort_id()),
        "shard_index": int(shard_index),
        "shard_count": int(shard_count),
        "all_date_count": len(all_ds),
        "selected_date_count": len(sel),
        "selected_dates": ",".join(manifest["selected_dates"]),
        "materialized_date_count": len(date_rows),
        "materialized_dates": ",".join(manifest["materialized_dates"]),
        "missing_dates": ",".join(missing),
        "candidate_total": manifest["candidate_total"],
        "capture_total": manifest["capture_total"],
        "attempt_total": manifest["attempt_total"],
        "source_fingerprint": ident["source_fingerprint"],
        "config_fingerprint": ident["config_fingerprint"],
        "errors": " | ".join(errs),
        "elapsed_sec": float(elapsed_sec or 0.0),
    }]).to_csv(out / SHARD_MANIFEST_CSV, index=False, encoding="utf-8-sig")
    lines = [
        f"V23_SHARD status={status} cohort={manifest.get('cohort_id','ROLLING')} shard={int(shard_index)+1}/{int(shard_count)}",
        f"dates={len(sel)} materialized={len(date_rows)} missing={len(missing)} candidates={manifest['candidate_total']}",
        f"source={ident['source_fingerprint'][:12]} config={ident['config_fingerprint']}",
        f"elapsed_min={manifest['elapsed_sec']/60:.1f} newest_first=1 zero_recompute_parent=1",
    ]
    if missing: lines.append("missing=" + ",".join(missing))
    if errs: lines.append("errors=" + " | ".join(errs))
    (out / SHARD_MANIFEST_REPORT).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return manifest


def _safe_extract(tar_path: Path, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    with tarfile.open(tar_path, "r:gz") as tf:
        root = dest.resolve()
        for m in tf.getmembers():
            target = (dest / m.name).resolve()
            if root not in target.parents and target != root:
                raise RuntimeError(f"unsafe tar member: {m.name}")
        tf.extractall(dest)


def create_handoff_archive(output_dir: str | Path, archive_path: str | Path) -> Path:
    """Create a same-run handoff containing selected materialized dates plus reusable V20 caches."""
    out = Path(output_dir or "reports")
    mp = out / SHARD_MANIFEST_JSON
    if not mp.exists():
        raise FileNotFoundError(str(mp))
    manifest = json.loads(mp.read_text(encoding="utf-8"))
    selected = [str(x) for x in manifest.get("selected_dates") or []]
    arc = Path(archive_path); arc.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="v23handoff_") as td:
        stage = Path(td) / "reports"
        stage.mkdir(parents=True, exist_ok=True)
        # Selected materialized results only: prevents stale rolling-window files from crossing shards.
        mroot = stage / MATERIALIZED_DIRNAME; mroot.mkdir(parents=True, exist_ok=True)
        for ds in selected:
            src = materialized_path(out, ds)
            if src.exists():
                shutil.copy2(src, mroot / src.name)
        # V20 caches are reusable by parent diagnostics/evaluation and future resume.
        for rel in CACHE_DIRS:
            src_dir = out / rel
            if src_dir.exists():
                dst_dir = stage / rel
                shutil.copytree(src_dir, dst_dir, dirs_exist_ok=True)
        for name in (SHARD_MANIFEST_JSON, SHARD_MANIFEST_CSV, SHARD_MANIFEST_REPORT):
            src = out / name
            if src.exists(): shutil.copy2(src, stage / name)
        with tarfile.open(arc, "w:gz", compresslevel=3) as tf:
            for p in stage.rglob("*"):
                if p.is_file():
                    tf.add(p, arcname=str(p.relative_to(stage)))
    return arc


def _price_cache_max_date(path: Path) -> pd.Timestamp | None:
    try:
        z = _load_pickle(path)
        df = z.get("frame") if isinstance(z, dict) else z
        if isinstance(df, pd.DataFrame) and not df.empty:
            idx = pd.to_datetime(df.index, errors="coerce")
            idx = idx[idx.notna()]
            if len(idx): return pd.Timestamp(idx.max()).normalize()
    except Exception:
        pass
    return None


def _copy_cache_file(src: Path, dst: Path) -> tuple[str, str]:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not dst.exists():
        shutil.copy2(src, dst); return "COPIED", "new"
    if "v20_price_history" in str(dst):
        smax, dmax = _price_cache_max_date(src), _price_cache_max_date(dst)
        if smax is not None and (dmax is None or smax > dmax):
            shutil.copy2(src, dst); return "REPLACED", f"coverage {dmax}->{smax}"
    return "KEPT", "existing"


def _copy_materialized(src: Path, dst: Path) -> tuple[str, str]:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not dst.exists():
        shutil.copy2(src, dst); return "COPIED", "new"
    ssha, dsha = file_sha256(src), file_sha256(dst)
    if ssha == dsha:
        return "KEPT", "identical"
    # Same date with different bytes is a hard provenance conflict; never choose arbitrarily.
    return "CONFLICT", f"sha {dsha[:12]} != {ssha[:12]}"


def merge_handoff_archives(download_root: str | Path, output_dir: str | Path) -> dict[str, Any]:
    src_root = Path(download_root)
    out = Path(output_dir or "reports"); out.mkdir(parents=True, exist_ok=True)
    archives = sorted(src_root.rglob("*handoff.tar.gz")) if src_root.exists() else []
    manifests: list[dict[str, Any]] = []
    file_rows: list[dict[str, Any]] = []
    errors = conflicts = copied = replaced = kept = 0
    mroot = _materialized_root(out); mroot.mkdir(parents=True, exist_ok=True)
    shard_manifest_root = out / "v23_shard_manifests"; shard_manifest_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="v23merge_") as td:
        troot = Path(td)
        for ai, arc in enumerate(archives):
            ex = troot / f"a{ai}"
            try:
                _safe_extract(arc, ex)
            except Exception as exc:
                errors += 1
                file_rows.append({"archive": arc.name, "kind": "ARCHIVE", "file": "", "status": "ERROR", "detail": f"{type(exc).__name__}:{exc}"})
                continue
            mps = list(ex.rglob(SHARD_MANIFEST_JSON))
            for mp in mps:
                try:
                    man = json.loads(mp.read_text(encoding="utf-8")); manifests.append(man)
                    idx = int(man.get("shard_index", ai))
                    cohort = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(man.get("cohort_id", "ROLLING") or "ROLLING")).strip("-") or "ROLLING"
                    shutil.copy2(mp, shard_manifest_root / f"shard_{cohort}_{idx}.json")
                except Exception as exc:
                    errors += 1
                    file_rows.append({"archive": arc.name, "kind": "MANIFEST", "file": mp.name, "status": "ERROR", "detail": f"{type(exc).__name__}:{exc}"})
            # Materialized selected date files.
            for src in ex.rglob("date_*.pkl.gz"):
                if MATERIALIZED_DIRNAME not in src.parts:
                    continue
                dst = mroot / src.name
                st, det = _copy_materialized(src, dst)
                if st == "COPIED": copied += 1
                elif st == "KEPT": kept += 1
                else: conflicts += 1
                file_rows.append({"archive": arc.name, "kind": MATERIALIZED_DIRNAME, "file": src.name, "status": st, "detail": det})
            # Cache trees.
            for rel in CACHE_DIRS:
                candidates = [p for p in ex.rglob(Path(rel).name) if p.is_dir() and rel.endswith(p.name)]
                if not candidates:
                    continue
                src_dir = candidates[0]
                dst_dir = out / rel
                for src in src_dir.rglob("*"):
                    if not src.is_file(): continue
                    dst = dst_dir / src.relative_to(src_dir)
                    try:
                        st, det = _copy_cache_file(src, dst)
                        if st == "COPIED": copied += 1
                        elif st == "REPLACED": replaced += 1
                        else: kept += 1
                        file_rows.append({"archive": arc.name, "kind": rel, "file": str(src.relative_to(src_dir)), "status": st, "detail": det})
                    except Exception as exc:
                        errors += 1
                        file_rows.append({"archive": arc.name, "kind": rel, "file": str(src), "status": "ERROR", "detail": f"{type(exc).__name__}:{exc}"})

    expected_shards, expected_dates, expected_cohorts, cohort_date_counts = _expected_handoff_shape(manifests)
    unique_shards = sorted({_manifest_shard_key(m) for m in manifests if _manifest_shard_key(m).split(":")[-1] != "-1"})
    complete_shards = sorted({_manifest_shard_key(m) for m in manifests if str(m.get("status")) == "COMPLETE" and _manifest_shard_key(m).split(":")[-1] != "-1"})
    selected_lists = [[str(x) for x in (m.get("selected_dates") or [])] for m in manifests]
    selected_flat = [x for xs in selected_lists for x in xs]
    selected_unique = sorted(set(selected_flat))
    duplicate_selected = len(selected_flat) - len(selected_unique)
    source_set = sorted({str(m.get("source_fingerprint", "")) for m in manifests if str(m.get("source_fingerprint", ""))})
    config_set = sorted({str(m.get("config_fingerprint", "")) for m in manifests if str(m.get("config_fingerprint", ""))})
    ident = current_identity()
    current_identity_match = source_set == [ident["source_fingerprint"]] and config_set == [ident["config_fingerprint"]]
    mode = str(os.getenv("V25_COHORT_MODE", "ROLLING")).strip().upper()
    manifest_cohorts = sorted({str(m.get("cohort_id", "ROLLING") or "ROLLING").strip().upper() for m in manifests})
    cohort_set_ok = (manifest_cohorts == sorted(expected_cohorts)) if mode == "ALL" else True
    materialized_expected = []
    missing_materialized = []
    invalid_materialized = []
    for ds in selected_unique:
        z = load_materialized_date(out, ds, require_current_identity=True)
        if z is None:
            p = materialized_path(out, ds)
            (invalid_materialized if p.exists() else missing_materialized).append(ds)
        else:
            materialized_expected.append(ds)
    complete = (
        len(archives) >= expected_shards
        and len(unique_shards) == expected_shards
        and len(complete_shards) == expected_shards
        and duplicate_selected == 0
        and len(selected_unique) == expected_dates
        and not missing_materialized
        and not invalid_materialized
        and len(source_set) == 1
        and len(config_set) == 1
        and current_identity_match
        and cohort_set_ok
        and errors == 0
        and conflicts == 0
    )
    status = "COMPLETE_HANDOFF" if complete else ("PARTIAL_HANDOFF" if archives else "NO_HANDOFF")
    audit = {
        "version": VERSION,
        "research_only": True,
        "created_at": _utc_now(),
        "status": status,
        "archive_count": len(archives),
        "manifest_count": len(manifests),
        "expected_shards": expected_shards,
        "unique_shards": unique_shards,
        "complete_shards": complete_shards,
        "cohort_mode": mode,
        "expected_cohorts": expected_cohorts,
        "manifest_cohorts": manifest_cohorts,
        "cohort_set_ok": cohort_set_ok,
        "cohort_date_counts": cohort_date_counts,
        "expected_date_count": expected_dates,
        "selected_date_count": len(selected_unique),
        "selected_dates": selected_unique,
        "duplicate_selected_dates": duplicate_selected,
        "materialized_expected_count": len(materialized_expected),
        "missing_materialized_dates": missing_materialized,
        "invalid_materialized_dates": invalid_materialized,
        "source_fingerprints": source_set,
        "config_fingerprints": config_set,
        "current_source_fingerprint": ident["source_fingerprint"],
        "current_config_fingerprint": ident["config_fingerprint"],
        "current_identity_match": current_identity_match,
        "files_copied": copied,
        "files_replaced": replaced,
        "files_kept": kept,
        "errors": errors,
        "conflicts": conflicts,
        "zero_recompute_parent": True,
    }
    (out / MERGE_AUDIT_JSON).write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(file_rows or [{"archive": "", "kind": "", "file": "", "status": status, "detail": ""}]).to_csv(out / MERGE_AUDIT_CSV, index=False, encoding="utf-8-sig")
    lines = [
        f"V23_HANDOFF status={status} archives={len(archives)} shards={len(complete_shards)}/{expected_shards}",
        f"dates={len(selected_unique)}/{expected_dates} materialized={len(materialized_expected)} duplicate_dates={duplicate_selected}",
        f"cohorts mode={mode} expected={','.join(expected_cohorts)} manifest={','.join(manifest_cohorts)} set_ok={cohort_set_ok}",
        f"identity source_consensus={len(source_set)==1} config_consensus={len(config_set)==1} current_match={current_identity_match}",
        f"copy={copied} replace={replaced} keep={kept} errors={errors} conflicts={conflicts}",
    ]
    if missing_materialized: lines.append("missing=" + ",".join(missing_materialized))
    if invalid_materialized: lines.append("invalid=" + ",".join(invalid_materialized))
    (out / MERGE_AUDIT_REPORT).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return audit


def verify_parent_materialized(output_dir: str | Path, expected_dates: Iterable[Any], *, raise_on_error: bool = True) -> dict[str, Any]:
    out = Path(output_dir or "reports"); out.mkdir(parents=True, exist_ok=True)
    expected = sorted({_norm_date(x).strftime("%Y-%m-%d") for x in expected_dates})
    merge = {}
    mp = out / MERGE_AUDIT_JSON
    if mp.exists():
        try: merge = json.loads(mp.read_text(encoding="utf-8"))
        except Exception: merge = {}
    valid, missing, invalid = [], [], []
    candidate_total = capture_total = attempt_total = 0
    for ds in expected:
        p = materialized_path(out, ds)
        z = load_materialized_date(out, ds, require_current_identity=True)
        if z is None:
            (invalid if p.exists() else missing).append(ds)
            continue
        valid.append(ds)
        candidate_total += len(z.get("candidate_rows") or [])
        capture_total += len(z.get("capture_rows") or [])
        attempt_total += len(z.get("attempt_rows") or [])
    merge_selected = sorted(set(str(x) for x in (merge.get("selected_dates") or [])))
    date_set_match = merge_selected == expected
    ok = (
        str(merge.get("status")) == "COMPLETE_HANDOFF"
        and date_set_match
        and len(valid) == len(expected)
        and not missing and not invalid
        and bool(merge.get("current_identity_match"))
    )
    result = {
        "version": VERSION,
        "created_at": _utc_now(),
        "status": "PASS" if ok else "INVALID_INCOMPLETE_SHARD_HANDOFF",
        "merge_status": merge.get("status", "MISSING"),
        "expected_date_count": len(expected),
        "valid_date_count": len(valid),
        "date_set_match": date_set_match,
        "missing_dates": missing,
        "invalid_dates": invalid,
        "candidate_total": candidate_total,
        "capture_total": capture_total,
        "attempt_total": attempt_total,
        "zero_recompute_parent": True,
        "fallback_recompute_allowed": False,
    }
    (out / PARENT_PREFLIGHT_JSON).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        f"V23_PARENT_PREFLIGHT status={result['status']} merge={result['merge_status']}",
        f"dates={len(valid)}/{len(expected)} date_set_match={date_set_match} missing={len(missing)} invalid={len(invalid)}",
        f"candidate_rows={candidate_total} capture_rows={capture_total} attempt_rows={attempt_total}",
        "zero_recompute_parent=1 fallback_recompute_allowed=0",
    ]
    if missing: lines.append("missing=" + ",".join(missing))
    if invalid: lines.append("invalid=" + ",".join(invalid))
    (out / PARENT_PREFLIGHT_REPORT).write_text("\n".join(lines) + "\n", encoding="utf-8")
    if raise_on_error and not ok:
        raise RuntimeError("INVALID_INCOMPLETE_SHARD_HANDOFF: " + json.dumps(result, ensure_ascii=False, sort_keys=True))
    return result


def finalize_parent(output_dir: str | Path, base_report: str = "") -> tuple[str, dict[str, Any]]:
    out = Path(output_dir or "reports")
    merge = {}
    pre = {}
    try: merge = json.loads((out / MERGE_AUDIT_JSON).read_text(encoding="utf-8"))
    except Exception: pass
    try: pre = json.loads((out / PARENT_PREFLIGHT_JSON).read_text(encoding="utf-8"))
    except Exception: pass
    lines = [
        HEADER,
        f"📌 {VERSION} · TOP500 유지 · {merge.get('cohort_mode', str(os.getenv('V25_COHORT_MODE','ROLLING')).upper())} 코호트를 6-shard 단위로 물질화 · parent는 TOP500 원천 재계산 금지",
        f"🧩 handoff={merge.get('status','MISSING')} | shards={len(merge.get('complete_shards') or [])}/{merge.get('expected_shards',_env_int('V23_SHARD_COUNT',6))} | materialized={merge.get('materialized_expected_count',0)}/{merge.get('expected_date_count',_env_int('V1080_BACKTEST_WEEKS',24))}",
        f"🛡️ parent preflight={pre.get('status','MISSING')} | date-set-match={pre.get('date_set_match',False)} | fallback recompute=DISABLED",
        "⚡ [실행구조] 각 코호트는 6개 shard로 독립 계산하며, ALL은 A→B→C→D 배치를 자동 연결합니다. 각 shard는 자신의 기준일을 newest→oldest로 계산해 후보/FULL_UNIVERSE/AUX/Universe provenance를 날짜별 materialized payload로 고정합니다.",
        "🚫 [Zero-Recompute] parent의 _v1081_make_signal_rows_for_asof는 materialized payload만 반환합니다. 날짜 누락·identity 불일치·handoff 불완전 시 전체/누락 TOP500을 대신 계산하지 않고 INVALID_INCOMPLETE_SHARD_HANDOFF로 실패합니다.",
        "🧠 [후속연구] parent는 materialized candidate/full-universe sidecar와 합쳐진 price cache를 사용해 기존 Eval/Formula/Context/Scale-In/Geo/Stability/보고서 체인을 그대로 수행합니다.",
        f"- Actions: {SHARD_MANIFEST_JSON} · {MERGE_AUDIT_JSON} · {PARENT_PREFLIGHT_JSON}",
        "- LIVE 점수·순위·진입·청산·주문 변경 0 · RESEARCH_ONLY=True",
    ]
    block = "\n".join(lines)
    (out / REPORT_FILE).write_text(block + "\n", encoding="utf-8")
    text = str(base_report or "").rstrip()
    if HEADER not in text:
        text = (text + "\n\n" + block).strip()
    return text, {"merge": merge, "preflight": pre}


def force_report(text: str, output_dir: str | Path) -> str:
    raw = str(text or "")
    if HEADER in raw: return raw
    p = Path(output_dir or "reports") / REPORT_FILE
    if p.exists():
        try:
            block = p.read_text(encoding="utf-8").strip()
            if block: return (raw.rstrip() + "\n\n" + block).strip()
        except Exception: pass
    return raw


def _cli() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--merge-handoffs", default="")
    ap.add_argument("--package-handoff", default="")
    ap.add_argument("--output-dir", default="reports")
    ap.add_argument("--require-complete", action="store_true")
    args = ap.parse_args()
    if args.package_handoff:
        p = create_handoff_archive(args.output_dir, args.package_handoff)
        print(f"V23_HANDOFF_ARCHIVE {p} sha256={file_sha256(p)}")
        return 0
    if args.merge_handoffs:
        m = merge_handoff_archives(args.merge_handoffs, args.output_dir)
        print(json.dumps(m, ensure_ascii=False, sort_keys=True))
        if args.require_complete and m.get("status") != "COMPLETE_HANDOFF":
            return 193
        return 0
    ap.print_help(); return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
