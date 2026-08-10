from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import os
import random
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

VERSION = "V73.3.6.6.19"
QUOTA_GUARD_VERSION = "V73.3.6.6.23.3"
RESEARCH_ONLY = True
LIVE_LOGIC_CHANGED = False
REAL_ORDER_CHANGED = False

# User-confirmed Stock-Hunter spreadsheet. This is an identifier, not a secret.
DEFAULT_SPREADSHEET_ID = "16OXJmYNnwWMjEp2UAhNxLPbRiTEYC29-6S7StvJxYt0"
DEFAULT_EXPECTED_SERVICE_ACCOUNT = (
    "stock-call@gen-lang-client-0419376661.iam.gserviceaccount.com"
)

STORAGE_AUDIT_FILE = "v73_google_sheet_storage_audit.csv"
SYNC_AUDIT_FILE = "v73_google_sheet_sync_audit.csv"
STORAGE_REPORT_FILE = "v73_google_sheet_storage_report.txt"

MAX_CELL_CHARS = 45000
DEFAULT_TAB_ROWS = 2000
DEFAULT_TAB_COLS = 40
APPEND_CHUNK_ROWS = 250

# V23.3 Google Sheet quota-resilience defaults.  Reads are consolidated with
# spreadsheet.values_batch_get() so 60+ worksheet tabs no longer perform one
# metadata request plus one values request per tab.  Retries only apply to
# transient quota/server/network failures; credential/schema errors stay
# fail-closed.
DEFAULT_BATCH_READ_SIZE = 8
DEFAULT_RETRY_MAX = 5
DEFAULT_RETRY_BASE_SEC = 5.0
DEFAULT_RETRY_MAX_SEC = 65.0

_GOOGLE_IO_STATS: dict[str, int] = {
    "metadata_reads": 0,
    "batch_read_requests": 0,
    "single_read_fallback_requests": 0,
    "schema_batch_writes": 0,
    "append_write_requests": 0,
    "quota_retries": 0,
}


@dataclass(frozen=True)
class TabSpec:
    title: str
    source_file: str | None
    key_columns: tuple[str, ...]
    merge_mode: str = "EXACT_APPEND"


TAB_SPECS: tuple[TabSpec, ...] = (
    TabSpec("STORAGE_AUDIT", STORAGE_AUDIT_FILE, ("run_id", "phase", "attempted_at")),
    TabSpec("RUN_AUDIT", None, ("run_id", "phase", "attempted_at")),
    TabSpec(
        "SOURCE_CAPTURE_AUDIT",
        "v73_catalyst_source_capture_audit.csv",
        ("version", "captured_at", "source_name", "capture_run_id"),
    ),
    TabSpec(
        "CATALYST_LEDGER",
        "v73_catalyst_source_ledger.csv",
        ("source_key", "source_id", "retrieved_at"),
        merge_mode="CATALYST_MASTER",
    ),
    TabSpec(
        "CATALYST_QUERY_UNIVERSE",
        "v73_catalyst_query_universe.csv",
        ("code", "query_text", "sector", "theme", "priority"),
    ),
    TabSpec(
        "CATALYST_FIRST_SEEN_AUDIT",
        "v73_catalyst_first_seen_integrity_audit.csv",
        ("version", "captured_at"),
    ),
    TabSpec(
        "MARKET_SECTOR_LEDGER",
        "v73_market_sector_context_ledger.csv",
        ("signal_date", "code", "sector", "source_name", "captured_at"),
    ),
    TabSpec(
        "MARKET_SECTOR_CAPTURE_AUDIT",
        "v73_market_sector_forward_capture_audit.csv",
        ("version", "captured_at", "status"),
    ),
    TabSpec("BACKTEST_EVENT_MASTER", "v73_backtest_event_master.csv", ("event_id", "snapshot_id")),
    TabSpec("MARKET_CONTEXT", "v73_market_context_diagnostic.csv", ("dimension", "label", "snapshot_id")),
    TabSpec("SECTOR_CONTEXT", "v73_sector_context_diagnostic.csv", ("dimension", "label", "snapshot_id")),
    TabSpec("RETURN_PATH_CLUSTER", "v73_return_path_cluster.csv", ("dimension", "label", "snapshot_id")),
    TabSpec("WINNER_LOSER_COMMONALITY", "v73_winner_loser_commonality.csv", ("feature", "snapshot_id")),
    TabSpec("FAILURE_REASON", "v73_failure_reason.csv", ("reason", "snapshot_id")),
    TabSpec("CONTEXT_FEATURE_LIFT", "v73_context_feature_lift.csv", ("dimension", "label", "snapshot_id")),
    TabSpec("CONTEXT_ABLATION", "v73_context_ablation.csv", ("dimension", "label", "snapshot_id")),
    TabSpec("REGIME_PERFORMANCE", "v73_context_regime_performance.csv", ("market_state", "sector_state", "context_alignment", "snapshot_id")),
    TabSpec("SEARCH_FORMULA_SCORECARD", "v73_search_formula_scorecard.csv", ("label", "snapshot_id")),
    TabSpec("MISSED_FEATURE_AUDIT", "v73_missed_feature_audit.csv", ("field", "snapshot_id")),
    TabSpec("DIAGNOSTIC_READINESS", "v73_context_diagnostic_readiness.csv", ("snapshot_id", "version")),
    TabSpec("SCALE_IN_EVENT_POLICY", "v73_scale_in_event_policy.csv", ("event_policy_id", "snapshot_id")),
    TabSpec("SCALE_IN_POLICY_SUMMARY", "v73_scale_in_policy_summary.csv", ("dimension", "label", "snapshot_id")),
    TabSpec("SCALE_IN_FORMULA_POLICY", "v73_scale_in_formula_policy_summary.csv", ("formula", "policy", "snapshot_id")),
    TabSpec("SCALE_IN_ADD_TRIGGER", "v73_scale_in_add_trigger_audit.csv", ("formula", "policy", "add_block_reason", "snapshot_id")),
    TabSpec("SCALE_IN_CONFLICT", "v73_scale_in_conflict_audit.csv", ("event_policy_id", "snapshot_id")),
    TabSpec("SCALE_IN_RISK_AUDIT", "v73_scale_in_risk_parity_audit.csv", ("policy", "stop_source", "snapshot_id")),
    TabSpec("SCALE_IN_READINESS", "v73_scale_in_readiness.csv", ("snapshot_id", "version")),
    TabSpec("GEO_EVENT_LEDGER", "v73_geo_event_ledger.csv", ("geo_event_id", "source_key", "snapshot_id")),
    TabSpec("GEO_STAGE_MACHINE", "v73_geo_event_stage_machine.csv", ("event_family", "event_stage", "snapshot_id")),
    TabSpec("GEO_EVENT_SECTOR_MAP", "v73_geo_event_to_sector_map.csv", ("event_family", "sector_keyword", "impact", "snapshot_id")),
    TabSpec("GEO_BENEFICIARY_DIRECTNESS", "v73_geo_beneficiary_directness.csv", ("code", "event_family", "directness", "snapshot_id")),
    TabSpec("BEAR_GEO_WINNER_MASTER", "v73_bear_geo_winner_event_master.csv", ("signal_date", "code", "formula", "snapshot_id")),
    TabSpec("BEAR_GEO_MATCHED_CONTROL", "v73_bear_geo_matched_control.csv", ("match_id", "snapshot_id")),
    TabSpec("BEAR_GEO_COMMONALITY", "v73_bear_geo_commonality.csv", ("feature", "comparison", "snapshot_id")),
    TabSpec("GEO_FORMULA_SCORECARD", "v73_geo_formula_scorecard.csv", ("dimension", "label", "snapshot_id")),
    TabSpec("GEO_SCALE_IN_POLICY", "v73_geo_scale_in_policy.csv", ("bear_geo_bucket", "policy", "snapshot_id")),
    TabSpec("GEO_DEESCALATION_RISK", "v73_geo_deescalation_risk.csv", ("event_family", "snapshot_id")),
    TabSpec("GEO_DATA_AVAILABILITY", "v73_geo_data_availability.csv", ("field", "snapshot_id")),
    TabSpec("GEO_READINESS", "v73_geo_readiness.csv", ("snapshot_id", "version")),
    TabSpec("BEAR_WINNER_UNIQUE", "v73_bear_winner_event_master_unique.csv", ("event_id", "snapshot_id")),
    TabSpec("BEAR_FORMULA_MEMBERSHIP", "v73_bear_winner_formula_membership.csv", ("event_id", "formula", "snapshot_id")),
    TabSpec("BEAR_MATCHED_UNIQUE", "v73_bear_winner_matched_control_unique.csv", ("match_id", "snapshot_id")),
    TabSpec("BEAR_COMMONALITY_UNIQUE", "v73_bear_winner_commonality_unique.csv", ("feature", "comparison", "snapshot_id")),
    TabSpec("ZERO_PATTERN_AUDIT", "v73_zero_pattern_winner_audit.csv", ("dimension", "label", "snapshot_id")),
    TabSpec("ZERO_PATTERN_FEATURE", "v73_zero_pattern_feature_commonality.csv", ("feature", "snapshot_id")),
    TabSpec("FORMULA_STABILITY", "v73_formula_stability_matrix.csv", ("formula", "window_requested", "snapshot_id")),
    TabSpec("FORMULA_STABILITY_POLICY", "v73_formula_stability_policy.csv", ("formula", "snapshot_id")),
    TabSpec("FORMULA_STABILITY_INPUT", "v73_formula_stability_input_audit.csv", ("selected_source_file", "snapshot_id")),
    TabSpec("FORMULA_STABILITY_RECON", "v73_formula_stability_reconciliation.csv", ("formula", "metric", "snapshot_id")),
    TabSpec("REPLAY_DATE_LOCK", "v73_replay_date_lock.csv", ("replay_index", "replay_date", "snapshot_id")),
    TabSpec("DUPLICATE_COMBO_AUDIT", "v73_duplicate_combo_call_audit.csv", ("duplicate_key", "snapshot_id")),
    TabSpec("LOCKED_POLICY_FAILURE", "v73_locked_policy_failure_audit.csv", ("policy_family", "snapshot_id")),
    TabSpec("PATTERN_ONLY_EVENTS", "v73_pattern_only_sequence_event_audit.csv", ("signal_date", "code", "snapshot_id")),
    TabSpec("PATTERN_ONLY_COMMON", "v73_pattern_only_sequence_commonality.csv", ("feature", "snapshot_id")),
    TabSpec("GEO_OFFICIAL_ARCHIVE", "v73_geo_official_archive_ledger.csv", ("source_key", "published_at", "snapshot_id")),
    TabSpec("SECTOR_BREADTH_HISTORY", "v73_sector_breadth_history.csv", ("signal_date", "sector", "source_name", "snapshot_id")),
    TabSpec("SECTOR_BREADTH_JOIN", "v73_sector_breadth_join_audit.csv", ("event_id", "snapshot_id")),
    TabSpec("MINUTE_SCALE_READINESS", "v73_minute_scale_in_readiness.csv", ("source", "snapshot_id")),
    TabSpec("BEAR_STABILITY_READINESS", "v73_bear_winner_stability_readiness.csv", ("snapshot_id", "version")),
    TabSpec("UNIVERSE_ASOF_MEMBERSHIP", "v73_universe_asof_membership.csv", ("signal_date", "code", "snapshot_id")),
    TabSpec("UNIVERSE_ASOF_SUMMARY", "v73_universe_asof_summary.csv", ("signal_date", "snapshot_id")),
    TabSpec("DIRECT_REPLAY_PERF", "v73_direct_replay_performance_audit.csv", ("ts_utc", "source_fingerprint")),
    TabSpec("UNIVERSE_RANK_COVERAGE", "v73_universe_rank_bucket_coverage.csv", ("scope", "snapshot_id")),
    TabSpec("UNIVERSE_DATA_AVAIL", "v73_universe_data_availability.csv", ("signal_date", "snapshot_id")),
)

RUN_AUDIT_COLUMNS = [
    "version",
    "attempted_at",
    "run_id",
    "run_number",
    "workflow",
    "profile",
    "phase",
    "capture_enabled",
    "sheet_sync_enabled",
    "spreadsheet_id",
    "spreadsheet_title",
    "service_account_email",
    "credential_source",
    "tabs_expected",
    "tabs_ready",
    "source_rows",
    "rows_appended",
    "status",
    "error_type",
    "error_message",
    "research_only",
    "live_logic_changed",
    "real_order_changed",
]

STORAGE_AUDIT_COLUMNS = RUN_AUDIT_COLUMNS + [
    "sheet_id_match",
    "service_account_match",
    "hydrated_files",
    "hydrated_rows",
    "sync_detail_file",
    "quota_guard_version",
    "metadata_reads",
    "batch_read_requests",
    "single_read_fallback_requests",
    "schema_batch_writes",
    "append_write_requests",
    "quota_retries",
    "quota_deferred",
    "deferred_reason",
    "deferred_local_safe",
]

SYNC_AUDIT_COLUMNS = [
    "version",
    "attempted_at",
    "run_id",
    "phase",
    "tab",
    "source_file",
    "local_rows_before",
    "sheet_rows_before",
    "merged_local_rows",
    "rows_appended",
    "sheet_rows_after_estimate",
    "headers_added",
    "status",
    "error_type",
    "error_message",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _truthy(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off", ""}:
        return False
    return default


def _clean_cell(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    if isinstance(value, bool):
        text = "TRUE" if value else "FALSE"
    elif isinstance(value, (dict, list, tuple, set)):
        text = json.dumps(value, ensure_ascii=False, default=str, sort_keys=True)
    else:
        text = str(value)
    # Every worksheet write uses value_input_option="RAW", so values are
    # persisted literally and negative numeric strings remain analyzable.
    return text[:MAX_CELL_CHARS]


def _row_hash(headers: Iterable[str], row: dict[str, Any]) -> str:
    payload = "\x1f".join(_clean_cell(row.get(h, "")) for h in headers)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _key_hash(spec: TabSpec, row: dict[str, Any], headers: Iterable[str]) -> str:
    usable = [c for c in spec.key_columns if c in row and _clean_cell(row.get(c, ""))]
    if not usable:
        return _row_hash(headers, row)

    def canonical_key_value(column: str, value: Any) -> str:
        text = _clean_cell(value)
        if column.endswith("_at") or column in {"captured_at", "attempted_at"}:
            parsed = pd.to_datetime(pd.Series([text]), errors="coerce", utc=True).iloc[0]
            if pd.notna(parsed):
                return parsed.isoformat()
        return text.strip()

    payload = "\x1f".join(
        f"{c}={canonical_key_value(c, row.get(c, ''))}" for c in usable
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _read_csv(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame(columns=columns or [])
    for encoding in ("utf-8-sig", "utf-8", "cp949"):
        try:
            frame = pd.read_csv(path, dtype=str, encoding=encoding).fillna("")
            return frame
        except Exception:
            continue
    return pd.DataFrame(columns=columns or [])


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    safe = frame.astype(object).where(pd.notna(frame), "")
    safe.to_csv(path, index=False, encoding="utf-8-sig")


def _append_local_audit(path: Path, row: dict[str, Any], columns: list[str]) -> None:
    old = _read_csv(path, columns)
    current = pd.DataFrame([{c: _clean_cell(row.get(c, "")) for c in columns}], columns=columns)
    merged = pd.concat([old, current], ignore_index=True)
    _write_csv(path, merged)


def _decode_json_candidate(raw: str) -> dict[str, Any]:
    value = str(raw or "").strip()
    if not value:
        raise ValueError("empty credential payload")

    try:
        candidate_path = Path(value).expanduser()
        if len(value) < 240 and candidate_path.exists() and candidate_path.is_file():
            value = candidate_path.read_text(encoding="utf-8")
    except (OSError, ValueError):
        pass

    attempts: list[str] = [value]
    compact = "".join(value.split())
    try:
        decoded = base64.b64decode(compact, validate=True).decode("utf-8")
        attempts.append(decoded)
    except Exception:
        pass

    last_error: Exception | None = None
    for item in attempts:
        try:
            info = json.loads(item)
            if isinstance(info, dict):
                if "private_key" in info and isinstance(info["private_key"], str):
                    info["private_key"] = info["private_key"].replace("\\n", "\n")
                return info
        except Exception as exc:
            last_error = exc
    raise ValueError(f"credential JSON decode failed: {last_error}")


def load_service_account_info() -> tuple[dict[str, Any], str]:
    candidates = (
        "GOOGLE_JSON_KEY",
        "GOOGLE_SERVICE_ACCOUNT_JSON",
        "GOOGLE_SERVICE_ACCOUNT_JSON_B64",
        "GSPREAD_SERVICE_ACCOUNT_JSON",
    )
    errors: list[str] = []
    for name in candidates:
        raw = os.environ.get(name, "")
        if not raw:
            continue
        try:
            info = _decode_json_candidate(raw)
            required = {"client_email", "private_key", "token_uri"}
            missing = sorted(required - set(info))
            if missing:
                raise ValueError("missing credential fields: " + ",".join(missing))
            return info, name
        except Exception as exc:
            errors.append(f"{name}:{type(exc).__name__}:{exc}")
    if errors:
        raise ValueError("; ".join(errors))
    raise KeyError("Google service-account credential env is missing")


def create_google_client(info: dict[str, Any]):
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(credentials)


def _spreadsheet_id() -> str:
    return (
        os.environ.get("STOCKHUNTER_GOOGLE_SHEET_ID")
        or os.environ.get("GOOGLE_SHEET_ID")
        or DEFAULT_SPREADSHEET_ID
    ).strip()


def _expected_service_account() -> str:
    return (
        os.environ.get("GOOGLE_SERVICE_ACCOUNT_EMAIL_EXPECTED")
        or DEFAULT_EXPECTED_SERVICE_ACCOUNT
    ).strip()


def _frame_from_values(values: list[list[str]]) -> pd.DataFrame:
    if not values:
        return pd.DataFrame()
    headers = [str(v).strip() for v in values[0]]
    while headers and not headers[-1]:
        headers.pop()
    if not headers:
        return pd.DataFrame()
    rows = []
    for raw in values[1:]:
        padded = list(raw) + [""] * max(0, len(headers) - len(raw))
        rows.append(dict(zip(headers, padded[: len(headers)])))
    return pd.DataFrame(rows, columns=headers).fillna("")


def _reset_google_io_stats() -> None:
    for key in list(_GOOGLE_IO_STATS):
        _GOOGLE_IO_STATS[key] = 0


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    try:
        return max(minimum, int(str(os.environ.get(name, default)).strip()))
    except Exception:
        return max(minimum, int(default))


def _env_float(name: str, default: float, minimum: float = 0.0) -> float:
    try:
        return max(minimum, float(str(os.environ.get(name, default)).strip()))
    except Exception:
        return max(minimum, float(default))


def _google_error_code(exc: BaseException) -> int | None:
    response = getattr(exc, "response", None)
    for candidate in (
        getattr(response, "status_code", None),
        getattr(response, "status", None),
        getattr(exc, "status_code", None),
        getattr(exc, "code", None),
    ):
        try:
            if candidate is not None:
                return int(candidate)
        except Exception:
            pass
    text = str(exc).lower()
    for code in (429, 500, 502, 503, 504):
        if str(code) in text:
            return code
    return None


def _is_quota_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    return _google_error_code(exc) == 429 or "quota exceeded" in text or "resource_exhausted" in text


def _is_transient_google_error(exc: BaseException) -> bool:
    code = _google_error_code(exc)
    if code in {429, 500, 502, 503, 504}:
        return True
    text = str(exc).lower()
    transient_tokens = (
        "timed out",
        "timeout",
        "temporarily unavailable",
        "connection reset",
        "connection aborted",
        "remote disconnected",
        "rate limit",
        "quota exceeded",
    )
    return any(token in text for token in transient_tokens)


def _google_call(label: str, fn, *, retries: int | None = None):
    """Retry only transient Google/network failures with bounded backoff."""
    max_retry = _env_int("GOOGLE_SHEET_RETRY_MAX", DEFAULT_RETRY_MAX, 0) if retries is None else max(0, int(retries))
    base = _env_float("GOOGLE_SHEET_RETRY_BASE_SEC", DEFAULT_RETRY_BASE_SEC, 0.0)
    cap = _env_float("GOOGLE_SHEET_RETRY_MAX_SEC", DEFAULT_RETRY_MAX_SEC, 0.0)
    attempt = 0
    while True:
        try:
            return fn()
        except Exception as exc:
            if not _is_transient_google_error(exc) or attempt >= max_retry:
                raise
            attempt += 1
            _GOOGLE_IO_STATS["quota_retries"] += 1
            delay = min(cap, base * (2 ** (attempt - 1))) if cap > 0 else 0.0
            if delay > 0:
                delay += random.uniform(0.0, min(1.0, delay * 0.1))
            print(
                f"[GOOGLE_SHEET_RETRY] op={label} attempt={attempt}/{max_retry} "
                f"code={_google_error_code(exc) or '-'} sleep={delay:.1f}s error={str(exc)[:240]}",
                file=sys.stderr,
            )
            if delay > 0:
                time.sleep(delay)


def _google_write_call(label: str, fn, *, retries: int | None = None):
    """Retry writes only for explicit quota/rate-limit rejection.

    Network/5xx write outcomes can be ambiguous (the server may have committed
    before the client lost the response), so those stay fail-closed instead of
    being blindly replayed and possibly duplicating append-only rows.
    """
    max_retry = _env_int("GOOGLE_SHEET_RETRY_MAX", DEFAULT_RETRY_MAX, 0) if retries is None else max(0, int(retries))
    base = _env_float("GOOGLE_SHEET_RETRY_BASE_SEC", DEFAULT_RETRY_BASE_SEC, 0.0)
    cap = _env_float("GOOGLE_SHEET_RETRY_MAX_SEC", DEFAULT_RETRY_MAX_SEC, 0.0)
    attempt = 0
    while True:
        try:
            return fn()
        except Exception as exc:
            if not _is_quota_error(exc) or attempt >= max_retry:
                raise
            attempt += 1
            _GOOGLE_IO_STATS["quota_retries"] += 1
            delay = min(cap, base * (2 ** (attempt - 1))) if cap > 0 else 0.0
            if delay > 0:
                delay += random.uniform(0.0, min(1.0, delay * 0.1))
            print(
                f"[GOOGLE_SHEET_WRITE_RETRY] op={label} attempt={attempt}/{max_retry} "
                f"code={_google_error_code(exc) or '-'} sleep={delay:.1f}s error={str(exc)[:240]}",
                file=sys.stderr,
            )
            if delay > 0:
                time.sleep(delay)


def _quote_sheet_title(title: str) -> str:
    return "'" + str(title).replace("'", "''") + "'"


def _worksheet_headers_and_frame(worksheet) -> tuple[list[str], pd.DataFrame]:
    # Compatibility fallback used by legacy callers. V23.3 run_storage uses
    # batch reads and does not call this once per worksheet.
    values = _google_call(
        f"single_get:{getattr(worksheet, 'title', '?')}",
        lambda: worksheet.get_all_values(),
    )
    _GOOGLE_IO_STATS["single_read_fallback_requests"] += 1
    frame = _frame_from_values(values)
    headers = list(frame.columns) if len(frame.columns) else ([str(v).strip() for v in values[0]] if values else [])
    headers = [h for h in headers if h]
    return headers, frame


def _desired_headers_for_spec(spec: TabSpec, out: Path) -> list[str]:
    if spec.title == "STORAGE_AUDIT":
        return list(STORAGE_AUDIT_COLUMNS)
    if spec.title == "RUN_AUDIT":
        return list(RUN_AUDIT_COLUMNS)
    if spec.source_file:
        desired = list(_read_csv(out / spec.source_file).columns)
        return desired or list(spec.key_columns) or ["created_at"]
    return list(RUN_AUDIT_COLUMNS)


def _worksheet_map(spreadsheet) -> dict[str, Any]:
    # Duck-typed in-memory fakes used by the release regression suite expose a
    # tabs dict rather than the gspread worksheets() method. Production gspread
    # uses worksheets(), which is one metadata read for all tabs.
    if not hasattr(spreadsheet, "worksheets") and isinstance(getattr(spreadsheet, "tabs", None), dict):
        return dict(spreadsheet.tabs)
    _GOOGLE_IO_STATS["metadata_reads"] += 1
    sheets = _google_call("worksheets_metadata", lambda: spreadsheet.worksheets())
    return {str(getattr(ws, "title", "")): ws for ws in sheets}


def _batch_add_missing_worksheets(spreadsheet, worksheet_map: dict[str, Any], desired_map: dict[str, list[str]]) -> dict[str, Any]:
    missing = [title for title in desired_map if title not in worksheet_map]
    if not missing:
        return worksheet_map

    # One raw batchUpdate avoids 60+ write requests on a first-time bootstrap.
    if hasattr(spreadsheet, "batch_update"):
        requests = []
        for title in missing:
            requests.append(
                {
                    "addSheet": {
                        "properties": {
                            "title": title,
                            "gridProperties": {
                                "rowCount": max(DEFAULT_TAB_ROWS, 10),
                                "columnCount": max(DEFAULT_TAB_COLS, len(desired_map[title]), 10),
                            },
                        }
                    }
                }
            )
        _google_write_call("batch_add_worksheets", lambda: spreadsheet.batch_update({"requests": requests}))
        _GOOGLE_IO_STATS["schema_batch_writes"] += 1
        return _worksheet_map(spreadsheet)

    # Conservative compatibility fallback for older gspread.
    for title in missing:
        desired = desired_map[title]
        ws = _google_write_call(
            f"add_worksheet:{title}",
            lambda t=title, d=desired: spreadsheet.add_worksheet(
                title=t,
                rows=max(DEFAULT_TAB_ROWS, 10),
                cols=max(DEFAULT_TAB_COLS, len(d), 10),
            ),
        )
        worksheet_map[title] = ws
    return worksheet_map


def _batch_read_values(spreadsheet, titles: list[str], *, header_only: bool = False) -> dict[str, list[list[str]]]:
    if not titles:
        return {}
    batch_size = _env_int("GOOGLE_SHEET_BATCH_READ_SIZE", DEFAULT_BATCH_READ_SIZE, 1)
    result: dict[str, list[list[str]]] = {title: [] for title in titles}

    if _truthy(os.environ.get("GOOGLE_SHEET_BATCH_READ_ENABLE", "1"), True) and hasattr(spreadsheet, "values_batch_get"):
        for start in range(0, len(titles), batch_size):
            chunk = titles[start : start + batch_size]
            ranges = [
                f"{_quote_sheet_title(title)}!A1:ZZ1" if header_only else f"{_quote_sheet_title(title)}!A:ZZ"
                for title in chunk
            ]
            payload = _google_call(
                f"values_batch_get:{start // batch_size + 1}",
                lambda r=ranges: spreadsheet.values_batch_get(r, params={"majorDimension": "ROWS"}),
            )
            _GOOGLE_IO_STATS["batch_read_requests"] += 1
            value_ranges = list((payload or {}).get("valueRanges", []) or [])
            for idx, title in enumerate(chunk):
                vr = value_ranges[idx] if idx < len(value_ranges) else {}
                result[title] = list((vr or {}).get("values", []) or [])
        return result

    # Compatibility fallback: throttle single-tab reads so old gspread does not
    # immediately consume the per-minute user read quota.
    delay = _env_float("GOOGLE_SHEET_SINGLE_READ_DELAY_SEC", 1.15, 0.0)
    if not str(getattr(spreadsheet.__class__, "__module__", "")).startswith("gspread"):
        delay = 0.0
    worksheet_map = _worksheet_map(spreadsheet)
    for idx, title in enumerate(titles):
        ws = worksheet_map[title]
        values = _google_call(f"single_get:{title}", lambda w=ws: w.get_all_values())
        _GOOGLE_IO_STATS["single_read_fallback_requests"] += 1
        result[title] = values[:1] if header_only else values
        if delay > 0 and idx + 1 < len(titles):
            time.sleep(delay)
    return result


def _headers_frame_from_values(values: list[list[str]]) -> tuple[list[str], pd.DataFrame]:
    frame = _frame_from_values(values)
    headers = list(frame.columns) if len(frame.columns) else ([str(v).strip() for v in values[0]] if values else [])
    return [h for h in headers if h], frame


def _batch_resize_columns(spreadsheet, worksheet_map: dict[str, Any], needed_cols: dict[str, int]) -> None:
    requests = []
    for title, count in needed_cols.items():
        ws = worksheet_map[title]
        current = int(getattr(ws, "col_count", 0) or 0)
        if count <= current:
            continue
        sheet_id = getattr(ws, "id", None)
        if sheet_id is None or not hasattr(spreadsheet, "batch_update"):
            _google_write_call(f"add_cols:{title}", lambda w=ws, n=count-current: w.add_cols(n))
            continue
        requests.append(
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": int(sheet_id), "gridProperties": {"columnCount": int(count)}},
                    "fields": "gridProperties.columnCount",
                }
            }
        )
    if requests:
        _google_write_call("batch_resize_columns", lambda: spreadsheet.batch_update({"requests": requests}))
        _GOOGLE_IO_STATS["schema_batch_writes"] += 1


def _batch_update_headers(spreadsheet, worksheet_map: dict[str, Any], updates: dict[str, list[str]]) -> None:
    if not updates:
        return
    if hasattr(spreadsheet, "values_batch_update"):
        body = {
            "valueInputOption": "RAW",
            "data": [
                {
                    "range": f"{_quote_sheet_title(title)}!A1",
                    "majorDimension": "ROWS",
                    "values": [headers],
                }
                for title, headers in updates.items()
            ],
        }
        _google_write_call("batch_update_headers", lambda: spreadsheet.values_batch_update(body))
        _GOOGLE_IO_STATS["schema_batch_writes"] += 1
        return
    for title, headers in updates.items():
        ws = worksheet_map[title]
        _google_write_call(
            f"update_header:{title}",
            lambda w=ws, h=headers: w.update(range_name="A1", values=[h], value_input_option="RAW"),
        )


def _ensure_worksheet(spreadsheet, title: str, desired_headers: list[str]):
    """Legacy compatibility helper; V23.3 run_storage uses cached worksheet map."""
    import gspread
    try:
        worksheet = _google_call(f"worksheet:{title}", lambda: spreadsheet.worksheet(title))
        created = False
    except gspread.WorksheetNotFound:
        worksheet = _google_write_call(
            f"add_worksheet:{title}",
            lambda: spreadsheet.add_worksheet(
                title=title,
                rows=max(DEFAULT_TAB_ROWS, 10),
                cols=max(DEFAULT_TAB_COLS, len(desired_headers), 10),
            ),
        )
        created = True

    headers, _ = _worksheet_headers_and_frame(worksheet)
    union_headers = list(headers)
    for header in desired_headers:
        if header and header not in union_headers:
            union_headers.append(header)
    if not union_headers:
        union_headers = desired_headers or ["created_at"]
    if created or headers != union_headers:
        if worksheet.col_count < len(union_headers):
            _google_write_call("legacy_add_cols", lambda: worksheet.add_cols(len(union_headers) - worksheet.col_count))
        _google_write_call(
            "legacy_header_update",
            lambda: worksheet.update(range_name="A1", values=[union_headers], value_input_option="RAW"),
        )
    return worksheet, union_headers, created, max(0, len(union_headers) - len(headers))

def _rows_from_frame(frame: pd.DataFrame) -> list[dict[str, str]]:
    if frame is None or frame.empty:
        return []
    return [
        {str(k): _clean_cell(v) for k, v in row.items()}
        for row in frame.fillna("").to_dict("records")
    ]


def _merge_exact(local: pd.DataFrame, remote: pd.DataFrame) -> pd.DataFrame:
    headers = list(local.columns)
    for col in remote.columns:
        if col not in headers:
            headers.append(col)
    if not headers:
        return pd.DataFrame()
    combined = pd.concat(
        [local.reindex(columns=headers, fill_value=""), remote.reindex(columns=headers, fill_value="")],
        ignore_index=True,
    ).fillna("")
    seen: set[str] = set()
    keep: list[int] = []
    for idx, row in combined.iterrows():
        h = _row_hash(headers, row.to_dict())
        if h not in seen:
            seen.add(h)
            keep.append(idx)
    return combined.loc[keep, headers].reset_index(drop=True)


def _merge_catalyst_master(local: pd.DataFrame, remote: pd.DataFrame) -> pd.DataFrame:
    try:
        from catalyst_source_adapters import empty_ledger, merge_append_only

        merged, _ = merge_append_only(
            local if isinstance(local, pd.DataFrame) else empty_ledger(),
            remote if isinstance(remote, pd.DataFrame) else empty_ledger(),
        )
        return merged.fillna("")
    except Exception:
        return _merge_exact(local, remote)


def _merge_for_spec(spec: TabSpec, local: pd.DataFrame, remote: pd.DataFrame) -> pd.DataFrame:
    if spec.merge_mode == "CATALYST_MASTER":
        return _merge_catalyst_master(local, remote)
    return _merge_exact(local, remote)


def _append_rows(worksheet, headers: list[str], rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    matrix = [[_clean_cell(row.get(header, "")) for header in headers] for row in rows]
    appended = 0
    for start in range(0, len(matrix), APPEND_CHUNK_ROWS):
        chunk = matrix[start : start + APPEND_CHUNK_ROWS]
        _google_write_call(
            f"append_rows:{getattr(worksheet, 'title', '?')}:{start}",
            lambda c=chunk: worksheet.append_rows(c, value_input_option="RAW", insert_data_option="INSERT_ROWS"),
        )
        _GOOGLE_IO_STATS["append_write_requests"] += 1
        appended += len(chunk)
    return appended

def _append_only_delta(spec: TabSpec, headers: list[str], remote: pd.DataFrame, local: pd.DataFrame) -> list[dict[str, Any]]:
    remote_rows = _rows_from_frame(remote.reindex(columns=headers, fill_value=""))
    local_rows = _rows_from_frame(local.reindex(columns=headers, fill_value=""))
    # Prefer the declared causal/business key so harmless representation changes
    # (boolean casing, timezone normalization, blank optional columns) do not
    # create duplicate sheet rows. Exact row hashing remains the fail-closed fallback.
    existing_keys = {_key_hash(spec, row, headers) for row in remote_rows}
    delta: list[dict[str, Any]] = []
    for row in local_rows:
        h = _key_hash(spec, row, headers)
        if h not in existing_keys:
            existing_keys.add(h)
            delta.append(row)
    return delta


def _base_run_row(phase: str) -> dict[str, Any]:
    return {
        "version": VERSION,
        "attempted_at": _now_iso(),
        "run_id": os.environ.get("GITHUB_RUN_ID", "LOCAL"),
        "run_number": os.environ.get("GITHUB_RUN_NUMBER", ""),
        "workflow": os.environ.get("GITHUB_WORKFLOW", ""),
        "profile": os.environ.get("TEST_PROFILE", "UNKNOWN"),
        "phase": phase,
        "capture_enabled": os.environ.get("CATALYST_FORWARD_CAPTURE_ENABLE", "0"),
        "sheet_sync_enabled": os.environ.get("CATALYST_GOOGLE_SHEET_SYNC_ENABLE", "0"),
        "spreadsheet_id": _spreadsheet_id(),
        "spreadsheet_title": "",
        "service_account_email": "",
        "credential_source": "",
        "tabs_expected": "|".join(spec.title for spec in TAB_SPECS),
        "tabs_ready": "",
        "source_rows": 0,
        "rows_appended": 0,
        "status": "STARTED",
        "error_type": "",
        "error_message": "",
        "research_only": RESEARCH_ONLY,
        "live_logic_changed": LIVE_LOGIC_CHANGED,
        "real_order_changed": REAL_ORDER_CHANGED,
        "sheet_id_match": True,
        "service_account_match": "",
        "hydrated_files": "",
        "hydrated_rows": 0,
        "sync_detail_file": SYNC_AUDIT_FILE,
        "quota_guard_version": QUOTA_GUARD_VERSION,
        "metadata_reads": 0,
        "batch_read_requests": 0,
        "single_read_fallback_requests": 0,
        "schema_batch_writes": 0,
        "append_write_requests": 0,
        "quota_retries": 0,
        "quota_deferred": False,
        "deferred_reason": "",
        "deferred_local_safe": False,
    }


def _storage_report(row: dict[str, Any]) -> str:
    status = str(row.get("status", ""))
    connected = "DEFERRED" if status == "SHEET_DEFERRED_QUOTA" else ("OK" if status.startswith("SHEET_") and "FAILED" not in status else "FAIL")
    lines = [
        "🗄️ [구글시트 저장 감사]",
        f"- spreadsheet 연결: {connected}",
        f"- spreadsheet title: {row.get('spreadsheet_title', '') or '-'}",
        f"- sheet id 일치: {row.get('sheet_id_match', '')}",
        f"- 서비스 계정: {row.get('service_account_email', '') or '-'}",
        f"- 서비스 계정 일치: {row.get('service_account_match', '')}",
        f"- capture 실효값: {row.get('capture_enabled', '')}",
        f"- sheet sync 실효값: {row.get('sheet_sync_enabled', '')}",
        f"- 생성/확인 탭: {row.get('tabs_ready', '') or '-'}",
        f"- source rows: {row.get('source_rows', 0)}",
        f"- 저장 rows: {row.get('rows_appended', 0)}",
        f"- hydrate rows: {row.get('hydrated_rows', 0)}",
        f"- quota guard: {row.get('quota_guard_version', QUOTA_GUARD_VERSION)}",
        f"- Google read: metadata={row.get('metadata_reads', 0)} batch={row.get('batch_read_requests', 0)} fallback={row.get('single_read_fallback_requests', 0)}",
        f"- Google retry: {row.get('quota_retries', 0)} / deferred={row.get('quota_deferred', False)} / local_safe={row.get('deferred_local_safe', False)}",
        f"- 저장상태: {row.get('status', '')}",
        f"- 오류유형: {row.get('error_type', '') or '-'}",
        f"- 오류메시지: {row.get('error_message', '') or '-'}",
        f"- deferred 사유: {row.get('deferred_reason', '') or '-'}",
        f"- LIVE 변경: {row.get('live_logic_changed', False)} / 실주문 변경: {row.get('real_order_changed', False)}",
    ]
    return "\n".join(lines)

def _write_step_summary(text: str) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY", "")
    if not summary_path:
        return
    try:
        with open(summary_path, "a", encoding="utf-8") as handle:
            handle.write("\n```text\n" + text + "\n```\n")
    except Exception:
        pass


def _sync_storage_audit_tab(
    spreadsheet,
    output_dir: Path,
    *,
    worksheet_map: dict[str, Any] | None = None,
    header_map: dict[str, list[str]] | None = None,
    remote_frame_map: dict[str, pd.DataFrame] | None = None,
) -> int:
    spec = next(s for s in TAB_SPECS if s.title == "STORAGE_AUDIT")
    local = _read_csv(output_dir / STORAGE_AUDIT_FILE, STORAGE_AUDIT_COLUMNS)
    if worksheet_map is not None and spec.title in worksheet_map:
        worksheet = worksheet_map[spec.title]
        headers = list((header_map or {}).get(spec.title, [])) or list(local.columns) or STORAGE_AUDIT_COLUMNS
        remote = (remote_frame_map or {}).get(spec.title, pd.DataFrame())
    else:
        worksheet, headers, _, _ = _ensure_worksheet(spreadsheet, spec.title, list(local.columns) or STORAGE_AUDIT_COLUMNS)
        _, remote = _worksheet_headers_and_frame(worksheet)
    union = list(headers)
    for col in local.columns:
        if col not in union:
            union.append(col)
    if union != headers:
        _google_write_call(
            "storage_audit_header_update",
            lambda: worksheet.update(range_name="A1", values=[union], value_input_option="RAW"),
        )
        headers = union
    delta = _append_only_delta(spec, headers, remote, local)
    return _append_rows(worksheet, headers, delta)


def _local_deferred_quota_safe(out: Path, phase: str) -> tuple[bool, str]:
    """Allow quota deferral only when a durable local continuity path exists."""
    if not _truthy(os.environ.get("GOOGLE_SHEET_QUOTA_DEFER_ENABLE", "0"), False):
        return False, "DEFER_DISABLED"

    phase_u = str(phase).upper()
    source_paths = [out / spec.source_file for spec in TAB_SPECS if spec.source_file and spec.source_file != STORAGE_AUDIT_FILE]
    existing_sources = [p for p in source_paths if p.exists() and p.stat().st_size > 0]

    if "HYDRATE" in phase_u:
        restored = _truthy(os.environ.get("V73_CACHE_RESTORE_MATCHED", "0"), False)
        continuity = [
            out / "v73_catalyst_source_ledger.csv",
            out / "v73_market_sector_context_ledger.csv",
            out / "v73_signal_lifecycle_ledger.csv",
        ]
        present = [p for p in continuity if p.exists() and p.stat().st_size > 0]
        if restored and present:
            return True, "RESTORED_LOCAL_LEDGER:" + "|".join(p.name for p in present)
        return False, "HYDRATE_REQUIRES_RESTORED_LOCAL_LEDGER"

    if "SYNC" in phase_u:
        if existing_sources:
            return True, "LOCAL_SYNC_PENDING:" + "|".join(p.name for p in existing_sources[:8])
        return False, "SYNC_HAS_NO_LOCAL_SOURCE"

    restored = _truthy(os.environ.get("V73_CACHE_RESTORE_MATCHED", "0"), False)
    return (restored, "RESTORED_STATE" if restored else "BOOTSTRAP_NO_RESTORED_STATE")

def run_storage(phase: str, output_dir: str | Path, strict: bool = False) -> dict[str, Any]:
    out = Path(output_dir or "reports")
    out.mkdir(parents=True, exist_ok=True)
    _reset_google_io_stats()
    row = _base_run_row(phase)
    sync_audits: list[dict[str, Any]] = []
    spreadsheet = None
    worksheet_map: dict[str, Any] = {}
    header_map: dict[str, list[str]] = {}
    remote_frame_map: dict[str, pd.DataFrame] = {}

    try:
        info, credential_source = load_service_account_info()
        service_email = str(info.get("client_email", "")).strip()
        expected_email = _expected_service_account()
        account_match = not expected_email or service_email.lower() == expected_email.lower()
        row["service_account_email"] = service_email
        row["credential_source"] = credential_source
        row["service_account_match"] = account_match
        if not account_match:
            raise PermissionError(
                f"service account mismatch expected={expected_email} actual={service_email}"
            )

        client = create_google_client(info)
        spreadsheet_id = _spreadsheet_id()
        spreadsheet = _google_call("open_by_key", lambda: client.open_by_key(spreadsheet_id))
        row["spreadsheet_title"] = str(getattr(spreadsheet, "title", ""))
        row["sheet_id_match"] = str(getattr(spreadsheet, "id", spreadsheet_id)) == spreadsheet_id

        desired_map = {spec.title: _desired_headers_for_spec(spec, out) for spec in TAB_SPECS}
        worksheet_map = _worksheet_map(spreadsheet)
        worksheet_map = _batch_add_missing_worksheets(spreadsheet, worksheet_map, desired_map)
        missing_after = [title for title in desired_map if title not in worksheet_map]
        if missing_after:
            raise RuntimeError("worksheet bootstrap incomplete: " + ",".join(missing_after))

        phase_u = str(phase).upper()
        needs_full_values = "HYDRATE" in phase_u or "SYNC" in phase_u
        read_titles = [spec.title for spec in TAB_SPECS]
        values_map = _batch_read_values(spreadsheet, read_titles, header_only=not needs_full_values)

        header_updates: dict[str, list[str]] = {}
        needed_cols: dict[str, int] = {}
        ready_tabs: list[str] = []
        for spec in TAB_SPECS:
            headers, frame = _headers_frame_from_values(values_map.get(spec.title, []))
            remote_frame_map[spec.title] = frame
            union_headers = list(headers)
            for header in desired_map[spec.title]:
                if header and header not in union_headers:
                    union_headers.append(header)
            if not union_headers:
                union_headers = list(desired_map[spec.title]) or ["created_at"]
            header_map[spec.title] = union_headers
            needed_cols[spec.title] = len(union_headers)
            if headers != union_headers:
                header_updates[spec.title] = union_headers
            ready_tabs.append(spec.title)

        _batch_resize_columns(spreadsheet, worksheet_map, needed_cols)
        _batch_update_headers(spreadsheet, worksheet_map, header_updates)
        row["tabs_ready"] = "|".join(ready_tabs)

        hydrated_files: list[str] = []
        hydrated_rows = 0
        total_source_rows = 0
        total_appended = 0

        if "HYDRATE" in phase_u:
            for spec in TAB_SPECS:
                if not spec.source_file or spec.source_file == STORAGE_AUDIT_FILE:
                    continue
                source_path = out / spec.source_file
                local = _read_csv(source_path)
                remote = remote_frame_map.get(spec.title, pd.DataFrame())
                merged = _merge_for_spec(spec, local, remote)
                if not merged.empty or local.empty:
                    _write_csv(source_path, merged)
                gained = max(0, len(merged) - len(local))
                total_source_rows += len(local)
                hydrated_rows += gained
                if gained:
                    hydrated_files.append(spec.source_file)
                sync_audits.append(
                    {
                        "version": VERSION,
                        "attempted_at": row["attempted_at"],
                        "run_id": row["run_id"],
                        "phase": phase,
                        "tab": spec.title,
                        "source_file": spec.source_file,
                        "local_rows_before": len(local),
                        "sheet_rows_before": len(remote),
                        "merged_local_rows": len(merged),
                        "rows_appended": 0,
                        "sheet_rows_after_estimate": len(remote),
                        "headers_added": max(0, len(header_map.get(spec.title, [])) - len(remote.columns)),
                        "status": "HYDRATED" if gained else "NO_REMOTE_DELTA",
                        "error_type": "",
                        "error_message": "",
                    }
                )

        if "SYNC" in phase_u:
            for spec in TAB_SPECS:
                if not spec.source_file or spec.source_file == STORAGE_AUDIT_FILE:
                    continue
                source_path = out / spec.source_file
                local = _read_csv(source_path)
                total_source_rows += len(local)
                worksheet = worksheet_map[spec.title]
                headers = list(header_map.get(spec.title, [])) or list(local.columns) or list(spec.key_columns) or ["created_at"]
                remote = remote_frame_map.get(spec.title, pd.DataFrame())
                delta = _append_only_delta(spec, headers, remote, local)
                appended = _append_rows(worksheet, headers, delta)
                total_appended += appended
                sync_audits.append(
                    {
                        "version": VERSION,
                        "attempted_at": row["attempted_at"],
                        "run_id": row["run_id"],
                        "phase": phase,
                        "tab": spec.title,
                        "source_file": spec.source_file,
                        "local_rows_before": len(local),
                        "sheet_rows_before": len(remote),
                        "merged_local_rows": len(local),
                        "rows_appended": appended,
                        "sheet_rows_after_estimate": len(remote) + appended,
                        "headers_added": len(header_updates.get(spec.title, [])),
                        "status": "APPENDED" if appended else "NO_LOCAL_DELTA",
                        "error_type": "",
                        "error_message": "",
                    }
                )

        row["source_rows"] = total_source_rows
        row["rows_appended"] = total_appended
        row["hydrated_rows"] = hydrated_rows
        row["hydrated_files"] = "|".join(hydrated_files)
        row["status"] = (
            "SHEET_PRIMARY_OK"
            if "SYNC" in phase_u
            else "SHEET_HYDRATE_OK"
            if "HYDRATE" in phase_u
            else "SHEET_BOOTSTRAP_OK"
        )

        # RUN_AUDIT uses the cached remote frame; no second metadata/value read.
        run_spec = next(s for s in TAB_SPECS if s.title == "RUN_AUDIT")
        run_ws = worksheet_map["RUN_AUDIT"]
        run_headers = header_map["RUN_AUDIT"]
        run_remote = remote_frame_map.get("RUN_AUDIT", pd.DataFrame())
        run_delta = _append_only_delta(
            run_spec,
            run_headers,
            run_remote,
            pd.DataFrame([{c: row.get(c, "") for c in RUN_AUDIT_COLUMNS}]),
        )
        total_appended += _append_rows(run_ws, run_headers, run_delta)
        row["rows_appended"] = total_appended

    except Exception as exc:
        local_safe, local_reason = _local_deferred_quota_safe(out, phase)
        # If the primary hydrate/bootstrap work already completed and only the
        # RUN_AUDIT append hit quota, the freshly hydrated local ledgers are a
        # valid continuity source even when this run started from a cache miss.
        if _is_quota_error(exc) and str(row.get("status", "")) in {"SHEET_HYDRATE_OK", "SHEET_BOOTSTRAP_OK"}:
            local_safe, local_reason = True, "PRIMARY_STAGE_COMPLETE_LOCAL_LEDGER"
        if _is_quota_error(exc) and local_safe:
            row["status"] = "SHEET_DEFERRED_QUOTA"
            row["quota_deferred"] = True
            row["deferred_local_safe"] = True
            row["deferred_reason"] = local_reason
            row["error_type"] = type(exc).__name__
            row["error_message"] = str(exc)[:1000]
            print(
                f"[GOOGLE_SHEET_DEFERRED_QUOTA] phase={phase} reason={local_reason} message={exc}",
                file=sys.stderr,
            )
        else:
            row["status"] = "SHEET_PRIMARY_FAILED"
            row["deferred_local_safe"] = bool(local_safe)
            row["deferred_reason"] = local_reason if _is_quota_error(exc) else ""
            row["error_type"] = type(exc).__name__
            row["error_message"] = str(exc)[:1000]
            print(
                f"[GOOGLE_SHEET_ERROR] type={type(exc).__name__} message={exc}",
                file=sys.stderr,
            )
            traceback.print_exc()

    for key in (
        "metadata_reads",
        "batch_read_requests",
        "single_read_fallback_requests",
        "schema_batch_writes",
        "append_write_requests",
        "quota_retries",
    ):
        row[key] = int(_GOOGLE_IO_STATS.get(key, 0))

    # The local audit is always persisted, including deferred quota events.
    _append_local_audit(out / STORAGE_AUDIT_FILE, row, STORAGE_AUDIT_COLUMNS)
    if sync_audits:
        old_sync = _read_csv(out / SYNC_AUDIT_FILE, SYNC_AUDIT_COLUMNS)
        new_sync = pd.DataFrame(sync_audits, columns=SYNC_AUDIT_COLUMNS).fillna("")
        _write_csv(out / SYNC_AUDIT_FILE, pd.concat([old_sync, new_sync], ignore_index=True))

    # Mirror audit without re-reading the tab. Quota-deferred runs intentionally
    # skip this extra write attempt and leave the local row for the next sync.
    if spreadsheet is not None and row["status"] != "SHEET_DEFERRED_QUOTA":
        try:
            mirrored = _sync_storage_audit_tab(
                spreadsheet,
                out,
                worksheet_map=worksheet_map,
                header_map=header_map,
                remote_frame_map=remote_frame_map,
            )
            row["rows_appended"] = int(row.get("rows_appended", 0) or 0) + mirrored
        except Exception as exc:
            print(
                f"[GOOGLE_SHEET_STORAGE_AUDIT_MIRROR_ERROR] type={type(exc).__name__} message={exc}",
                file=sys.stderr,
            )

    report = _storage_report(row)
    (out / STORAGE_REPORT_FILE).write_text(report + "\n", encoding="utf-8")
    print(report)
    _write_step_summary(report)

    if strict and row["status"] == "SHEET_PRIMARY_FAILED":
        raise RuntimeError(row["error_message"] or row["error_type"])
    return row


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", action="store_true")
    parser.add_argument("--hydrate", action="store_true")
    parser.add_argument("--sync", action="store_true")
    parser.add_argument("--output-dir", default="reports")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    selected = [args.bootstrap, args.hydrate, args.sync]
    if not any(selected):
        args.bootstrap = True

    env_fail_closed = str(os.environ.get("GOOGLE_SHEET_FAIL_CLOSED", "")).strip().lower() in {
        "1", "true", "yes", "on"
    }
    sync_enabled = str(os.environ.get("CATALYST_GOOGLE_SHEET_SYNC_ENABLE", "0")).strip().lower() in {
        "1", "true", "yes", "on"
    }
    effective_strict = bool(args.strict or env_fail_closed or sync_enabled)

    # V23.3: combined flags execute in one connection/session so metadata and
    # batch values are read once. The old implementation ran BOOTSTRAP and then
    # HYDRATE/SYNC as separate full 64-tab passes.
    if args.bootstrap and args.hydrate and args.sync:
        run_storage("BOOTSTRAP_HYDRATE_SYNC", args.output_dir, strict=effective_strict)
    elif args.bootstrap and args.hydrate:
        run_storage("BOOTSTRAP_HYDRATE", args.output_dir, strict=effective_strict)
    elif args.bootstrap and args.sync:
        run_storage("BOOTSTRAP_SYNC", args.output_dir, strict=effective_strict)
    elif args.hydrate and args.sync:
        run_storage("HYDRATE_SYNC", args.output_dir, strict=effective_strict)
    elif args.hydrate:
        run_storage("HYDRATE", args.output_dir, strict=effective_strict)
    elif args.sync:
        run_storage("SYNC", args.output_dir, strict=effective_strict)
    else:
        run_storage("BOOTSTRAP", args.output_dir, strict=effective_strict)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
