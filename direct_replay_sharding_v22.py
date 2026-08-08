from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import os
import pickle
import re
import shutil
import tarfile
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

VERSION = "V73.3.6.6.22"
RESEARCH_ONLY = True
LIVE_LOGIC_CHANGED = False
REAL_ORDER_CHANGED = False
HEADER = "⚡ [TOP500 4-Shard 병렬 × Newest-First Cache Prime × Fast-Gate Audit · RESEARCH_ONLY]"

SHARD_MANIFEST_JSON = "v73_v22_shard_manifest.json"
SHARD_MANIFEST_CSV = "v73_v22_shard_manifest.csv"
SHARD_MANIFEST_REPORT = "v73_v22_shard_manifest_report.txt"
MERGE_AUDIT_JSON = "v73_v22_handoff_merge_audit.json"
MERGE_AUDIT_CSV = "v73_v22_handoff_merge_audit.csv"
MERGE_AUDIT_REPORT = "v73_v22_handoff_merge_audit_report.txt"
FAST_GATE_AUDIT_CSV = "v73_v22_fast_gate_audit.csv"
FAST_GATE_POLICY_CSV = "v73_v22_fast_gate_policy.csv"
REPORT_FILE = "v73_v22_parallel_report.txt"

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


def _norm_code(v: Any) -> str:
    s = re.sub(r"\D", "", str(v or ""))
    return s[-6:].zfill(6) if s else ""


def partition_dates(
    dates: Iterable[Any],
    shard_index: int,
    shard_count: int,
    *,
    contiguous: bool = True,
    newest_first: bool = True,
) -> list[pd.Timestamp]:
    """Deterministically partition replay dates without overlap.

    Contiguous blocks keep each runner's dates clustered. Every block is processed newest-first so
    the first 900-day price fetch covers all older dates inside that shard. The final parent run
    still reports the original chronological date list.
    """
    ds = sorted({pd.Timestamp(x).normalize() for x in dates})
    n = max(1, int(shard_count))
    i = max(0, min(int(shard_index), n - 1))
    if not ds:
        return []
    if contiguous:
        # Balanced contiguous partition, first shards may receive one extra date.
        q, r = divmod(len(ds), n)
        start = i * q + min(i, r)
        size = q + (1 if i < r else 0)
        out = ds[start : start + size]
    else:
        out = ds[i::n]
    if newest_first:
        out = list(reversed(out))
    return out


def expected_checkpoint_date_count(checkpoint_root: str | Path, selected_dates: Iterable[Any]) -> tuple[int, list[str], list[str]]:
    root = Path(checkpoint_root)
    present: list[str] = []
    missing: list[str] = []
    for d in selected_dates:
        ds = pd.Timestamp(d).normalize().strftime("%Y-%m-%d")
        prefix = pd.Timestamp(d).normalize().strftime("%Y%m%d") + "_"
        ok = root.exists() and any(root.glob(prefix + "*.pkl.gz"))
        (present if ok else missing).append(ds)
    return len(present), present, missing


def write_shard_manifest(
    output_dir: str | Path,
    *,
    shard_index: int,
    shard_count: int,
    all_dates: Iterable[Any],
    selected_dates: Iterable[Any],
    candidate_counts: dict[str, int] | None = None,
    errors: list[str] | None = None,
    elapsed_sec: float | None = None,
) -> dict[str, Any]:
    out = Path(output_dir or "reports")
    out.mkdir(parents=True, exist_ok=True)
    cp_root = Path(os.getenv("V20_REPLAY_CHECKPOINT_DIR", str(out / ".cache/v20_replay_checkpoint")))
    selected = [pd.Timestamp(x).normalize() for x in selected_dates]
    present_n, present, missing = expected_checkpoint_date_count(cp_root, selected)
    price_root = Path(os.getenv("V20_PRICE_CACHE_DIR", str(out / ".cache/v20_price_history")))
    asof_root = Path(os.getenv("V20_ASOF_CACHE_DIR", str(out / ".cache/v20_asof_snapshots")))
    status = "COMPLETE" if selected and not missing and not errors else ("EMPTY" if not selected else "PARTIAL")
    manifest = {
        "version": VERSION,
        "research_only": True,
        "live_logic_changed": False,
        "real_order_changed": False,
        "status": status,
        "created_at": _utc_now(),
        "shard_index": int(shard_index),
        "shard_count": int(shard_count),
        "all_date_count": len(list(all_dates)),
        "selected_date_count": len(selected),
        "selected_dates": [x.strftime("%Y-%m-%d") for x in selected],
        "checkpoint_date_count": present_n,
        "checkpoint_dates": present,
        "missing_checkpoint_dates": missing,
        "candidate_counts": dict(candidate_counts or {}),
        "candidate_total": int(sum((candidate_counts or {}).values())),
        "errors": list(errors or []),
        "elapsed_sec": float(elapsed_sec or 0.0),
        "price_cache_files": len(list(price_root.glob("*.pkl.gz"))) if price_root.exists() else 0,
        "asof_cache_files": len([p for p in asof_root.rglob("*") if p.is_file()]) if asof_root.exists() else 0,
        "checkpoint_files": len(list(cp_root.glob("*.pkl.gz"))) if cp_root.exists() else 0,
        "newest_first": True,
        "fast_gate_mode": str(os.getenv("V22_FAST_GATE_MODE", "AUDIT_ONLY")),
    }
    (out / SHARD_MANIFEST_JSON).write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    pd.DataFrame([{
        **{k: v for k, v in manifest.items() if k not in {"selected_dates", "checkpoint_dates", "missing_checkpoint_dates", "candidate_counts", "errors"}},
        "selected_dates": ",".join(manifest["selected_dates"]),
        "checkpoint_dates": ",".join(manifest["checkpoint_dates"]),
        "missing_checkpoint_dates": ",".join(manifest["missing_checkpoint_dates"]),
        "errors": " | ".join(manifest["errors"]),
    }]).to_csv(out / SHARD_MANIFEST_CSV, index=False, encoding="utf-8-sig")
    lines = [
        f"V22_SHARD status={status} shard={int(shard_index)+1}/{int(shard_count)}",
        f"dates={len(selected)} checkpoints={present_n} missing={len(missing)} candidates={manifest['candidate_total']}",
        f"price_cache_files={manifest['price_cache_files']} asof_cache_files={manifest['asof_cache_files']} checkpoint_files={manifest['checkpoint_files']}",
        f"elapsed_min={manifest['elapsed_sec']/60:.1f} newest_first=1 fast_gate=AUDIT_ONLY",
    ]
    if missing:
        lines.append("missing=" + ",".join(missing))
    if errors:
        lines.append("errors=" + " | ".join(errors))
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


def _price_cache_max_date(path: Path) -> pd.Timestamp | None:
    try:
        with gzip.open(path, "rb") as fh:
            z = pickle.load(fh)
        df = z.get("frame") if isinstance(z, dict) else z
        if isinstance(df, pd.DataFrame) and not df.empty:
            idx = pd.to_datetime(df.index, errors="coerce")
            idx = idx[idx.notna()]
            if len(idx):
                return pd.Timestamp(idx.max()).normalize()
    except Exception:
        return None
    return None


def _copy_cache_file(src: Path, dst: Path) -> tuple[str, str]:
    """Merge a cache file. For duplicate price histories, preserve the file with later coverage."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not dst.exists():
        shutil.copy2(src, dst)
        return "COPIED", "new"
    if "v20_price_history" in str(dst):
        smax = _price_cache_max_date(src)
        dmax = _price_cache_max_date(dst)
        if smax is not None and (dmax is None or smax > dmax):
            shutil.copy2(src, dst)
            return "REPLACED", f"coverage {dmax}->{smax}"
    return "KEPT", "existing"


def merge_handoff_archives(download_root: str | Path, output_dir: str | Path) -> dict[str, Any]:
    """Merge same-run matrix shard handoffs into the parent runner's V20 cache trees."""
    src_root = Path(download_root)
    out = Path(output_dir or "reports")
    out.mkdir(parents=True, exist_ok=True)
    archives = sorted(src_root.rglob("*handoff.tar.gz")) if src_root.exists() else []
    manifests: list[dict[str, Any]] = []
    copied = replaced = kept = errors = 0
    file_rows: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="v22merge_") as td:
        troot = Path(td)
        for ai, arc in enumerate(archives):
            ex = troot / f"a{ai}"
            try:
                _safe_extract(arc, ex)
            except Exception as exc:
                errors += 1
                file_rows.append({"archive": str(arc), "kind": "ARCHIVE", "status": "ERROR", "detail": f"{type(exc).__name__}:{exc}"})
                continue
            for mp in ex.rglob(SHARD_MANIFEST_JSON):
                try:
                    manifests.append(json.loads(mp.read_text(encoding="utf-8")))
                except Exception:
                    pass
            # Handoff tar is created from reports/, so .cache is directly under extraction root.
            for rel in CACHE_DIRS:
                src_dir = ex / rel
                if not src_dir.exists():
                    # tolerate one extra reports/ prefix
                    src_dir = ex / "reports" / rel
                if not src_dir.exists():
                    continue
                dst_dir = out / rel
                for src in src_dir.rglob("*"):
                    if not src.is_file():
                        continue
                    relf = src.relative_to(src_dir)
                    dst = dst_dir / relf
                    try:
                        st, det = _copy_cache_file(src, dst)
                        if st == "COPIED": copied += 1
                        elif st == "REPLACED": replaced += 1
                        else: kept += 1
                        file_rows.append({"archive": arc.name, "kind": rel, "file": str(relf), "status": st, "detail": det})
                    except Exception as exc:
                        errors += 1
                        file_rows.append({"archive": arc.name, "kind": rel, "file": str(relf), "status": "ERROR", "detail": f"{type(exc).__name__}:{exc}"})
    cp_root = out / ".cache/v20_replay_checkpoint"
    checkpoint_dates = sorted({p.name[:8] for p in cp_root.glob("????????_*.pkl.gz")}) if cp_root.exists() else []
    shard_complete = sum(1 for m in manifests if str(m.get("status")) == "COMPLETE")
    merged = {
        "version": VERSION,
        "research_only": True,
        "created_at": _utc_now(),
        "archive_count": len(archives),
        "manifest_count": len(manifests),
        "complete_shards": shard_complete,
        "expected_shards": _env_int("V22_SHARD_COUNT", 4),
        "files_copied": copied,
        "files_replaced": replaced,
        "files_kept": kept,
        "errors": errors,
        "checkpoint_date_count": len(checkpoint_dates),
        "checkpoint_dates": checkpoint_dates,
        "status": "COMPLETE_HANDOFF" if shard_complete >= _env_int("V22_SHARD_COUNT", 4) and errors == 0 else ("PARTIAL_HANDOFF" if archives else "NO_HANDOFF"),
    }
    (out / MERGE_AUDIT_JSON).write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(file_rows or [{"archive": "", "kind": "", "file": "", "status": merged["status"], "detail": ""}]).to_csv(out / MERGE_AUDIT_CSV, index=False, encoding="utf-8-sig")
    lines = [
        f"V22_HANDOFF status={merged['status']} archives={len(archives)} complete_shards={shard_complete}/{merged['expected_shards']}",
        f"cache copied={copied} replaced={replaced} kept={kept} errors={errors}",
        f"checkpoint_dates={len(checkpoint_dates)}",
    ]
    (out / MERGE_AUDIT_REPORT).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return merged


def _parse_truth_true(v: Any) -> list[str]:
    if v is None:
        return []
    if isinstance(v, (list, tuple, set)):
        return [str(x) for x in v if str(x)]
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none", "[]", "{}"}:
        return []
    try:
        q = json.loads(s)
        if isinstance(q, list):
            return [str(x) for x in q if str(x)]
        if isinstance(q, dict):
            return [str(k) for k, x in q.items() if bool(x)]
    except Exception:
        pass
    return [x.strip() for x in re.split(r"[|,;]", s) if x.strip()]


def _load_checkpoints(output_dir: str | Path) -> list[dict[str, Any]]:
    out = Path(output_dir or "reports")
    cp_root = Path(os.getenv("V20_REPLAY_CHECKPOINT_DIR", str(out / ".cache/v20_replay_checkpoint")))
    rows: list[dict[str, Any]] = []
    if not cp_root.exists():
        return rows
    for p in sorted(cp_root.glob("*.pkl.gz")):
        try:
            with gzip.open(p, "rb") as fh:
                z = pickle.load(fh)
            if isinstance(z, dict):
                z["_file"] = p.name
                rows.append(z)
        except Exception:
            continue
    return rows


def build_fast_gate_audit(output_dir: str | Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Post-hoc superset-gate audit only; never skips stocks in V22.

    Candidate gates are intentionally evaluated *after* the full scan. A gate may be considered for
    a future locked OOS policy only if it shows zero formula/candidate false negatives over enough
    independent dates. V22 itself leaves FAST_GATE_MODE=AUDIT_ONLY and skips zero stocks.
    """
    cps = _load_checkpoints(output_dir)
    event_rows: list[dict[str, Any]] = []
    for cp in cps:
        um = cp.get("universe_membership")
        if not isinstance(um, pd.DataFrame) or um.empty:
            continue
        um = um.copy()
        ds = ""
        if "signal_date" in um.columns and um["signal_date"].notna().any():
            ds = str(pd.Timestamp(um["signal_date"].dropna().iloc[0]).date())
        if not ds:
            m = re.match(r"(\d{8})_", str(cp.get("_file", "")))
            if m:
                ds = pd.Timestamp(m.group(1)).strftime("%Y-%m-%d")
        formula_codes: set[str] = set()
        for r in cp.get("capture_rows", []) or []:
            if isinstance(r, dict) and _parse_truth_true(r.get("formula_truth_true")):
                formula_codes.add(_norm_code(r.get("code")))
        candidate_codes = {_norm_code(r.get("code")) for r in (cp.get("candidate_rows", []) or []) if isinstance(r, dict)}
        for _, r in um.iterrows():
            code = _norm_code(r.get("code", r.get("Code", "")))
            if not code:
                continue
            rank = pd.to_numeric(pd.Series([r.get("universe_rank")]), errors="coerce").iloc[0]
            ar = pd.to_numeric(pd.Series([r.get("amount_ratio_prev_vs20")]), errors="coerce").iloc[0]
            vr = pd.to_numeric(pd.Series([r.get("volume_ratio_prev_vs20")]), errors="coerce").iloc[0]
            is_event = str(r.get("is_event_expansion", "")).lower() in {"1", "true", "yes"}
            event_rows.append({
                "signal_date": ds,
                "code": code,
                "universe_rank": rank,
                "amount_ratio_prev_vs20": ar,
                "volume_ratio_prev_vs20": vr,
                "is_event_expansion": is_event,
                "actual_formula_hit": code in formula_codes,
                "actual_candidate": code in candidate_codes,
            })
    base = pd.DataFrame(event_rows)
    if base.empty:
        audit = pd.DataFrame(columns=["gate_id","n_rows","n_dates","eligible","would_skip","formula_hit_skipped","candidate_skipped","formula_false_negative_rate","candidate_false_negative_rate","same_sample_zero_false_negative","v22_action"])
        policy = pd.DataFrame([{"version": VERSION, "mode": "AUDIT_ONLY", "auto_skip_enabled": False, "reason": "NO_CHECKPOINT_DATA"}])
        return audit, policy

    gates = {
        "CONTROL_ALL500": lambda q: pd.Series(True, index=q.index),
        "RANK300_OR_EVENT": lambda q: (pd.to_numeric(q["universe_rank"], errors="coerce") <= 300) | q["is_event_expansion"].fillna(False),
        "RANK400_OR_ACTIVITY": lambda q: (pd.to_numeric(q["universe_rank"], errors="coerce") <= 400) | q["is_event_expansion"].fillna(False) | (pd.to_numeric(q["amount_ratio_prev_vs20"], errors="coerce") >= 1.0) | (pd.to_numeric(q["volume_ratio_prev_vs20"], errors="coerce") >= 1.0),
        "RANK450_OR_ACTIVITY_08": lambda q: (pd.to_numeric(q["universe_rank"], errors="coerce") <= 450) | q["is_event_expansion"].fillna(False) | (pd.to_numeric(q["amount_ratio_prev_vs20"], errors="coerce") >= 0.8) | (pd.to_numeric(q["volume_ratio_prev_vs20"], errors="coerce") >= 0.8),
    }
    rows = []
    formula_total = int(base["actual_formula_hit"].sum())
    cand_total = int(base["actual_candidate"].sum())
    n_dates = int(base["signal_date"].nunique())
    for gid, fn in gates.items():
        try:
            elig = fn(base).fillna(False).astype(bool)
        except Exception:
            elig = pd.Series(True, index=base.index)
        fskip = int((base["actual_formula_hit"] & ~elig).sum())
        cskip = int((base["actual_candidate"] & ~elig).sum())
        rows.append({
            "gate_id": gid,
            "n_rows": len(base),
            "n_dates": n_dates,
            "eligible": int(elig.sum()),
            "would_skip": int((~elig).sum()),
            "formula_hit_total": formula_total,
            "formula_hit_skipped": fskip,
            "candidate_total": cand_total,
            "candidate_skipped": cskip,
            "formula_false_negative_rate": (fskip / formula_total) if formula_total else math.nan,
            "candidate_false_negative_rate": (cskip / cand_total) if cand_total else math.nan,
            "same_sample_zero_false_negative": bool(fskip == 0 and cskip == 0),
            "v22_action": "AUDIT_ONLY_NO_SKIP",
        })
    audit = pd.DataFrame(rows)
    # Even if a gate has zero misses on this sample, same-sample discovery is not enough to activate it.
    safe = audit[(audit["gate_id"] != "CONTROL_ALL500") & audit["same_sample_zero_false_negative"] & (audit["n_dates"] >= 10)]
    policy = pd.DataFrame([{
        "version": VERSION,
        "mode": "AUDIT_ONLY",
        "auto_skip_enabled": False,
        "same_sample_zero_miss_gate_count": int(len(safe)),
        "reason": "REQUIRES_FROZEN_GATE_AND_FUTURE_OOS_ZERO_FALSE_NEGATIVE",
        "minimum_oos_dates_before_consideration": 10,
        "live_logic_changed": False,
        "real_order_changed": False,
    }])
    return audit, policy


def finalize_parent(output_dir: str | Path, base_report: str = "") -> tuple[str, dict[str, pd.DataFrame]]:
    out = Path(output_dir or "reports")
    out.mkdir(parents=True, exist_ok=True)
    audit, policy = build_fast_gate_audit(out)
    audit.to_csv(out / FAST_GATE_AUDIT_CSV, index=False, encoding="utf-8-sig")
    policy.to_csv(out / FAST_GATE_POLICY_CSV, index=False, encoding="utf-8-sig")
    merge: dict[str, Any] = {}
    mp = out / MERGE_AUDIT_JSON
    if mp.exists():
        try:
            merge = json.loads(mp.read_text(encoding="utf-8"))
        except Exception:
            merge = {}
    cps = _load_checkpoints(out)
    cp_dates = sorted({re.match(r"(\d{8})_", str(z.get("_file", ""))).group(1) for z in cps if re.match(r"(\d{8})_", str(z.get("_file", "")))})
    expected = _env_int("V1080_BACKTEST_WEEKS", 24)
    noncontrol = audit[audit.get("gate_id", pd.Series(dtype=str)).ne("CONTROL_ALL500")] if not audit.empty else audit
    best = None
    if isinstance(noncontrol, pd.DataFrame) and not noncontrol.empty:
        z = noncontrol.sort_values(["formula_hit_skipped", "candidate_skipped", "would_skip"], ascending=[True, True, False])
        best = z.iloc[0].to_dict()
    lines = [
        HEADER,
        f"📌 {VERSION} · TOP500 범위 유지 · 4개 GitHub matrix shard · 날짜별 newest-first cache prime · 최종 full-report merge",
        f"🧾 checkpoint dates {len(cp_dates)}/{expected} | handoff={merge.get('status','NO_HANDOFF')} | merged shards={merge.get('complete_shards',0)}/{merge.get('expected_shards',_env_int('V22_SHARD_COUNT',4))}",
        "⚡ [속도 구조] 각 shard는 서로 다른 기준일 묶음을 별도 runner에서 계산하고, 완료된 날짜 checkpoint/price/as-of cache를 final job이 병합합니다.",
        "🧠 [캐시 순서] 각 shard는 가장 최신 기준일부터 과거로 진행합니다. 900일 가격 cache가 최신일을 덮으면 같은 shard의 더 과거 날짜에서 재다운로드를 피할 수 있습니다.",
        "🧪 [Fast Superset Gate] V22에서는 AUDIT_ONLY입니다. TOP500 전 종목을 그대로 계산하며 gate로 종목을 제거하지 않습니다.",
    ]
    if best:
        lines.append(
            f"- 가장 보수적인 연구 gate {best.get('gate_id')}: would_skip {int(best.get('would_skip',0))} | formula miss {int(best.get('formula_hit_skipped',0))} | candidate miss {int(best.get('candidate_skipped',0))} | 동일표본 0-miss={bool(best.get('same_sample_zero_false_negative',False))}"
        )
    lines += [
        "- 동일표본에서 0 miss여도 자동 활성화하지 않습니다. 임계를 고정한 뒤 미래 OOS에서 formula/candidate false-negative=0을 확인해야 SAFE_SKIP 검토가 가능합니다.",
        "🛡️ shard 일부 실패/누락 시 final job은 기존 V21 checkpoint-resume 경로로 누락 날짜만 계산합니다. LIVE 점수·순위·진입·청산·주문 변경 0.",
        f"- Actions: {SHARD_MANIFEST_JSON} · {MERGE_AUDIT_CSV} · {FAST_GATE_AUDIT_CSV} · {FAST_GATE_POLICY_CSV}",
    ]
    block = "\n".join(lines)
    (out / REPORT_FILE).write_text(block + "\n", encoding="utf-8")
    report = str(base_report or "").rstrip()
    if HEADER not in report:
        report = (report + "\n\n" + block).strip()
    return report, {"fast_gate_audit": audit, "fast_gate_policy": policy}


def force_report(text: str, output_dir: str | Path) -> str:
    raw = str(text or "")
    if HEADER in raw:
        return raw
    p = Path(output_dir or "reports") / REPORT_FILE
    if p.exists():
        try:
            block = p.read_text(encoding="utf-8").strip()
            if block:
                return (raw.rstrip() + "\n\n" + block).strip()
        except Exception:
            pass
    return raw


def _cli() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--merge-handoffs", default="")
    ap.add_argument("--output-dir", default="reports")
    args = ap.parse_args()
    if args.merge_handoffs:
        m = merge_handoff_archives(args.merge_handoffs, args.output_dir)
        print(json.dumps(m, ensure_ascii=False, sort_keys=True))
        return 0
    ap.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
