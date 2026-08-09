from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

VERSION = "V73.3.6.6.21"
RESEARCH_ONLY = True
LIVE_LOGIC_CHANGED = False
REAL_ORDER_CHANGED = False
HEADER = "🛟 [V21 FAIL-SAFE CHECKPOINT PERSIST / RESUME MANIFEST · RESEARCH_ONLY]"
JSON_FILE = "v73_v21_resume_manifest.json"
CSV_FILE = "v73_v21_resume_manifest.csv"
REPORT_FILE = "v73_v21_resume_manifest_report.txt"


def _walk_stats(path: Path) -> tuple[int, int]:
    if not path.exists():
        return 0, 0
    files = 0
    size = 0
    for p in path.rglob("*"):
        try:
            if p.is_file():
                files += 1
                size += int(p.stat().st_size)
        except OSError:
            pass
    return files, size


def _last_csv_row(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            rows = list(csv.DictReader(fh))
        return dict(rows[-1]) if rows else {}
    except Exception:
        return {}


def _checkpoint_dates(path: Path) -> list[str]:
    dates: list[str] = []
    if not path.exists():
        return dates
    for p in sorted(path.glob("*.pkl.gz")):
        prefix = p.name.split("_", 1)[0]
        if len(prefix) == 8 and prefix.isdigit():
            dates.append(f"{prefix[:4]}-{prefix[4:6]}-{prefix[6:8]}")
    return sorted(set(dates))


def build_manifest(
    output_dir: str | Path = "reports",
    scanner_outcome: str = "UNKNOWN",
    cache_key: str = "",
    restore_hit: str = "",
) -> dict[str, Any]:
    out = Path(output_dir or "reports")
    price_dir = Path(os.getenv("V20_PRICE_CACHE_DIR", str(out / ".cache/v20_price_history")))
    asof_dir = Path(os.getenv("V20_ASOF_CACHE_DIR", str(out / ".cache/v20_asof_snapshots")))
    checkpoint_dir = Path(os.getenv("V20_REPLAY_CHECKPOINT_DIR", str(out / ".cache/v20_replay_checkpoint")))
    for p in (price_dir, asof_dir, checkpoint_dir):
        p.mkdir(parents=True, exist_ok=True)

    price_files, price_bytes = _walk_stats(price_dir)
    asof_files, asof_bytes = _walk_stats(asof_dir)
    checkpoint_files, checkpoint_bytes = _walk_stats(checkpoint_dir)
    checkpoint_dates = _checkpoint_dates(checkpoint_dir)

    perf = _last_csv_row(out / "v73_direct_replay_performance_audit.csv")
    path_audit = _last_csv_row(out / "v72_backtest_data_path_audit.csv")

    if checkpoint_files > 0:
        status = "PARTIAL_CACHE_READY"
    elif price_files > 0 or asof_files > 0:
        status = "CACHE_READY_NO_CHECKPOINT"
    else:
        status = "EMPTY_CACHE"

    total_bytes = price_bytes + asof_bytes + checkpoint_bytes
    manifest: dict[str, Any] = {
        "version": VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "scanner_outcome": str(scanner_outcome or "UNKNOWN"),
        "cache_key": str(cache_key or ""),
        "restore_hit": str(restore_hit or ""),
        "price_cache_dir": str(price_dir),
        "price_cache_files": price_files,
        "price_cache_bytes": price_bytes,
        "asof_cache_dir": str(asof_dir),
        "asof_cache_files": asof_files,
        "asof_cache_bytes": asof_bytes,
        "checkpoint_dir": str(checkpoint_dir),
        "checkpoint_files": checkpoint_files,
        "checkpoint_bytes": checkpoint_bytes,
        "checkpoint_dates": checkpoint_dates,
        "checkpoint_date_count": len(checkpoint_dates),
        "total_cache_bytes": total_bytes,
        "perf_checkpoint_hit": perf.get("checkpoint_hit", ""),
        "perf_checkpoint_miss": perf.get("checkpoint_miss", ""),
        "perf_checkpoint_saved": perf.get("checkpoint_saved", ""),
        "perf_network_fetch": perf.get("network_fetch", ""),
        "perf_disk_hit": perf.get("disk_hit", ""),
        "data_path_status": path_audit.get("status", ""),
        "data_path_reason": path_audit.get("reason", ""),
        "research_only": True,
        "live_logic_changed": False,
        "real_order_changed": False,
    }
    return manifest


def write_manifest(
    output_dir: str | Path = "reports",
    scanner_outcome: str = "UNKNOWN",
    cache_key: str = "",
    restore_hit: str = "",
) -> dict[str, Any]:
    out = Path(output_dir or "reports")
    out.mkdir(parents=True, exist_ok=True)
    m = build_manifest(out, scanner_outcome=scanner_outcome, cache_key=cache_key, restore_hit=restore_hit)
    (out / JSON_FILE).write_text(json.dumps(m, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    row = dict(m)
    row["checkpoint_dates"] = ",".join(m.get("checkpoint_dates", []))
    with (out / CSV_FILE).open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(row.keys()))
        w.writeheader(); w.writerow(row)
    report = "\n".join([
        HEADER,
        f"📌 {VERSION} · status={m['status']} · scanner={m['scanner_outcome']}",
        f"- replay checkpoint: {m['checkpoint_files']} files / {m['checkpoint_date_count']} dates",
        f"- price cache: {m['price_cache_files']} files / {m['price_cache_bytes']/1024/1024:.1f} MiB",
        f"- as-of cache: {m['asof_cache_files']} files / {m['asof_cache_bytes']/1024/1024:.1f} MiB",
        f"- total cache: {m['total_cache_bytes']/1024/1024:.1f} MiB",
        "- 계약: scanner step 실패/timeout 뒤에도 job-level timeout 전에 이 원장을 만들고 dedicated cache/save 단계가 실행되도록 workflow에서 분리합니다.",
        "- 캐시는 원천가격·과거시점 Universe·완료 날짜 checkpoint만 보존하며 LIVE 검색/진입/청산/주문 로직은 변경하지 않습니다.",
    ])
    (out / REPORT_FILE).write_text(report, encoding="utf-8")
    return m


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", default="reports")
    ap.add_argument("--scanner-outcome", default="UNKNOWN")
    ap.add_argument("--cache-key", default="")
    ap.add_argument("--restore-hit", default="")
    args = ap.parse_args(argv)
    m = write_manifest(args.output_dir, args.scanner_outcome, args.cache_key, args.restore_hit)
    print(
        "V21_RESUME_MANIFEST",
        f"status={m['status']}",
        f"scanner={m['scanner_outcome']}",
        f"checkpoint_dates={m['checkpoint_date_count']}",
        f"price_files={m['price_cache_files']}",
        f"asof_files={m['asof_cache_files']}",
        f"total_mib={m['total_cache_bytes']/1024/1024:.1f}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
