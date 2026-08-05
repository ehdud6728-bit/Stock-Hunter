from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import os
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

VERSION = "V73.3.6.6.14"
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


def _worksheet_headers_and_frame(worksheet) -> tuple[list[str], pd.DataFrame]:
    values = worksheet.get_all_values()
    frame = _frame_from_values(values)
    headers = list(frame.columns) if len(frame.columns) else ([str(v).strip() for v in values[0]] if values else [])
    headers = [h for h in headers if h]
    return headers, frame


def _ensure_worksheet(spreadsheet, title: str, desired_headers: list[str]):
    import gspread

    try:
        worksheet = spreadsheet.worksheet(title)
        created = False
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=title,
            rows=max(DEFAULT_TAB_ROWS, 10),
            cols=max(DEFAULT_TAB_COLS, len(desired_headers), 10),
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
            worksheet.add_cols(len(union_headers) - worksheet.col_count)
        worksheet.update("A1", [union_headers], value_input_option="RAW")
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
        worksheet.append_rows(chunk, value_input_option="RAW", insert_data_option="INSERT_ROWS")
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
    }


def _storage_report(row: dict[str, Any]) -> str:
    connected = "OK" if str(row.get("status", "")).startswith("SHEET_") and "FAILED" not in str(row.get("status", "")) else "FAIL"
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
        f"- 저장상태: {row.get('status', '')}",
        f"- 오류유형: {row.get('error_type', '') or '-'}",
        f"- 오류메시지: {row.get('error_message', '') or '-'}",
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


def _sync_storage_audit_tab(spreadsheet, output_dir: Path) -> int:
    spec = next(s for s in TAB_SPECS if s.title == "STORAGE_AUDIT")
    local = _read_csv(output_dir / STORAGE_AUDIT_FILE, STORAGE_AUDIT_COLUMNS)
    worksheet, headers, _, _ = _ensure_worksheet(spreadsheet, spec.title, list(local.columns) or STORAGE_AUDIT_COLUMNS)
    _, remote = _worksheet_headers_and_frame(worksheet)
    union = list(headers)
    for col in local.columns:
        if col not in union:
            union.append(col)
    if union != headers:
        worksheet.update("A1", [union], value_input_option="RAW")
        headers = union
    delta = _append_only_delta(spec, headers, remote, local)
    return _append_rows(worksheet, headers, delta)


def run_storage(phase: str, output_dir: str | Path, strict: bool = False) -> dict[str, Any]:
    out = Path(output_dir or "reports")
    out.mkdir(parents=True, exist_ok=True)
    row = _base_run_row(phase)
    sync_audits: list[dict[str, Any]] = []
    spreadsheet = None

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
        spreadsheet = client.open_by_key(spreadsheet_id)
        row["spreadsheet_title"] = str(getattr(spreadsheet, "title", ""))
        row["sheet_id_match"] = str(getattr(spreadsheet, "id", spreadsheet_id)) == spreadsheet_id

        ready_tabs: list[str] = []
        hydrated_files: list[str] = []
        hydrated_rows = 0
        total_source_rows = 0
        total_appended = 0

        # Bootstrap every tab first. Data-free runs still establish the schema.
        for spec in TAB_SPECS:
            if spec.title == "STORAGE_AUDIT":
                desired = STORAGE_AUDIT_COLUMNS
            elif spec.title == "RUN_AUDIT":
                desired = RUN_AUDIT_COLUMNS
            elif spec.source_file:
                desired = list(_read_csv(out / spec.source_file).columns)
                if not desired:
                    desired = list(spec.key_columns) or ["created_at"]
            else:
                desired = RUN_AUDIT_COLUMNS
            worksheet, _, _, _ = _ensure_worksheet(spreadsheet, spec.title, desired)
            ready_tabs.append(spec.title)

        row["tabs_ready"] = "|".join(ready_tabs)

        if phase in {"HYDRATE", "BOOTSTRAP_HYDRATE"}:
            for spec in TAB_SPECS:
                if not spec.source_file or spec.source_file == STORAGE_AUDIT_FILE:
                    continue
                source_path = out / spec.source_file
                local = _read_csv(source_path)
                worksheet = spreadsheet.worksheet(spec.title)
                _, remote = _worksheet_headers_and_frame(worksheet)
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
                        "headers_added": 0,
                        "status": "HYDRATED" if gained else "NO_REMOTE_DELTA",
                        "error_type": "",
                        "error_message": "",
                    }
                )

        if phase in {"SYNC", "BOOTSTRAP_SYNC"}:
            for spec in TAB_SPECS:
                if not spec.source_file or spec.source_file == STORAGE_AUDIT_FILE:
                    continue
                source_path = out / spec.source_file
                local = _read_csv(source_path)
                total_source_rows += len(local)
                worksheet, headers, _, headers_added = _ensure_worksheet(
                    spreadsheet,
                    spec.title,
                    list(local.columns) or list(spec.key_columns) or ["created_at"],
                )
                _, remote = _worksheet_headers_and_frame(worksheet)
                union_headers = list(headers)
                for col in local.columns:
                    if col not in union_headers:
                        union_headers.append(col)
                if union_headers != headers:
                    if worksheet.col_count < len(union_headers):
                        worksheet.add_cols(len(union_headers) - worksheet.col_count)
                    worksheet.update("A1", [union_headers], value_input_option="RAW")
                    headers_added += len(union_headers) - len(headers)
                    headers = union_headers
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
                        "headers_added": headers_added,
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
            if phase in {"SYNC", "BOOTSTRAP_SYNC"}
            else "SHEET_HYDRATE_OK"
            if phase in {"HYDRATE", "BOOTSTRAP_HYDRATE"}
            else "SHEET_BOOTSTRAP_OK"
        )

        # RUN_AUDIT is generated from the current run, independent of source row count.
        run_ws, run_headers, _, _ = _ensure_worksheet(spreadsheet, "RUN_AUDIT", RUN_AUDIT_COLUMNS)
        run_delta = _append_only_delta(
            next(s for s in TAB_SPECS if s.title == "RUN_AUDIT"),
            run_headers,
            _worksheet_headers_and_frame(run_ws)[1],
            pd.DataFrame([{c: row.get(c, "") for c in RUN_AUDIT_COLUMNS}]),
        )
        total_appended += _append_rows(run_ws, run_headers, run_delta)
        row["rows_appended"] = total_appended

    except Exception as exc:
        row["status"] = "SHEET_PRIMARY_FAILED"
        row["error_type"] = type(exc).__name__
        row["error_message"] = str(exc)[:1000]
        print(
            f"[GOOGLE_SHEET_ERROR] type={type(exc).__name__} message={exc}",
            file=sys.stderr,
        )
        traceback.print_exc()

    # The local audit is always persisted, even when Google is unreachable.
    _append_local_audit(out / STORAGE_AUDIT_FILE, row, STORAGE_AUDIT_COLUMNS)
    if sync_audits:
        old_sync = _read_csv(out / SYNC_AUDIT_FILE, SYNC_AUDIT_COLUMNS)
        new_sync = pd.DataFrame(sync_audits, columns=SYNC_AUDIT_COLUMNS).fillna("")
        _write_csv(out / SYNC_AUDIT_FILE, pd.concat([old_sync, new_sync], ignore_index=True))

    # After the local row exists, mirror STORAGE_AUDIT itself. Failure here must not erase the primary status.
    if spreadsheet is not None:
        try:
            mirrored = _sync_storage_audit_tab(spreadsheet, out)
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

    # GitHub REAL_FULL/forward-capture runs must fail closed.  V73.3.6.6.13
    # created a local failure audit but still returned exit code 0 unless --strict
    # was supplied, allowing the scanner to finish while Sheet persistence failed.
    env_fail_closed = str(os.environ.get("GOOGLE_SHEET_FAIL_CLOSED", "")).strip().lower() in {
        "1", "true", "yes", "on"
    }
    sync_enabled = str(os.environ.get("CATALYST_GOOGLE_SHEET_SYNC_ENABLE", "0")).strip().lower() in {
        "1", "true", "yes", "on"
    }
    effective_strict = bool(args.strict or env_fail_closed or sync_enabled)

    if args.bootstrap:
        run_storage("BOOTSTRAP", args.output_dir, strict=effective_strict)
    if args.hydrate:
        run_storage("HYDRATE", args.output_dir, strict=effective_strict)
    if args.sync:
        run_storage("SYNC", args.output_dir, strict=effective_strict)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
