from __future__ import annotations

import base64
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

VERSION = "V73.3.6.6.13"
RESEARCH_ONLY = True

SHEET_AUDIT_FILE = "v73_catalyst_google_sheet_storage_audit.csv"
DEFAULT_TITLE_CANDIDATES = (
    "Stock-Hunter",
    "StockHunter",
    "Stock Hunter",
    "주식종목분석",
    "종목분석",
)

TAB_FILE_MAP: dict[str, str] = {
    "CATALYST_LEDGER": "v73_catalyst_source_ledger.csv",
    "CATALYST_AI": "v73_ai_comment_source_ledger.csv",
    "CATALYST_OFFICIAL": "v73_official_disclosure_ledger.csv",
    "CATALYST_NEWS": "v73_news_source_ledger.csv",
    "CATALYST_GLOBAL": "v73_global_catalyst_ledger.csv",
    "QUERY_UNIVERSE": "v73_catalyst_query_universe.csv",
    "GLOBAL_QUERY_LEDGER": "v73_global_catalyst_query_ledger.csv",
    "MARKET_SECTOR_LEDGER": "v73_market_sector_context_ledger.csv",
    "SOURCE_CAPTURE_AUDIT": "v73_catalyst_source_capture_audit.csv",
    "FIRST_SEEN_AUDIT": "v73_catalyst_first_seen_integrity_audit.csv",
    "MARKET_SECTOR_AUDIT": "v73_market_sector_forward_capture_audit.csv",
    "RUN_AUDIT": "v73_catalyst_google_sheet_run_audit.csv",
    "STORAGE_AUDIT": "v73_catalyst_google_sheet_storage_audit.csv",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _env_on(name: str, default: str = "0") -> bool:
    return str(os.environ.get(name, default) or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def sheet_primary_requested() -> bool:
    return _env_on("CATALYST_SHEET_PRIMARY_ENABLE", "1")


def _load_json_payload(raw: str) -> dict[str, Any]:
    value = str(raw or "").strip()
    if not value:
        raise ValueError("GOOGLE_JSON_KEY_EMPTY")
    p = Path(value)
    if len(value) < 4096 and p.exists() and p.is_file():
        value = p.read_text(encoding="utf-8")
    candidates = [value, value.replace("\\n", "\n")]
    try:
        candidates.append(base64.b64decode(value).decode("utf-8"))
    except Exception:
        pass
    for candidate in candidates:
        try:
            payload = json.loads(candidate)
            if isinstance(payload, dict) and payload.get("client_email") and payload.get("private_key"):
                return payload
        except Exception:
            continue
    raise ValueError("GOOGLE_JSON_KEY_INVALID")


def _credential_payload() -> dict[str, Any]:
    raw = os.environ.get("GOOGLE_JSON_KEY") or os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or ""
    return _load_json_payload(raw)


def _module_target_hints() -> tuple[str, str, str]:
    """Reuse target hints from the existing google_sheet_manager when that module exposes them."""
    try:
        import google_sheet_manager as gsm  # type: ignore
    except Exception:
        return "", "", ""
    ids = ("CATALYST_GOOGLE_SHEET_ID", "GOOGLE_SHEET_ID", "SPREADSHEET_ID", "SHEET_ID")
    urls = ("CATALYST_GOOGLE_SHEET_URL", "GOOGLE_SHEET_URL", "SPREADSHEET_URL", "SHEET_URL")
    names = ("CATALYST_GOOGLE_SHEET_NAME", "GOOGLE_SHEET_NAME", "SPREADSHEET_NAME", "SHEET_NAME", "SPREADSHEET_TITLE")
    def first(attrs: tuple[str, ...]) -> str:
        for attr in attrs:
            v = getattr(gsm, attr, "")
            if isinstance(v, str) and v.strip():
                return v.strip()
        return ""
    return first(ids), first(urls), first(names)


def _target_hints() -> tuple[str, str, str]:
    mid, murl, mname = _module_target_hints()
    sid = (
        os.environ.get("CATALYST_GOOGLE_SHEET_ID")
        or os.environ.get("STOCKHUNTER_GOOGLE_SHEET_ID")
        or os.environ.get("GOOGLE_SHEET_ID")
        or mid
        or ""
    ).strip()
    surl = (os.environ.get("CATALYST_GOOGLE_SHEET_URL") or os.environ.get("GOOGLE_SHEET_URL") or murl or "").strip()
    sname = (
        os.environ.get("CATALYST_GOOGLE_SHEET_NAME")
        or os.environ.get("STOCKHUNTER_GOOGLE_SHEET_NAME")
        or os.environ.get("GOOGLE_SHEET_NAME")
        or mname
        or ""
    ).strip()
    return sid, surl, sname


@dataclass
class StoreStatus:
    requested: bool = False
    available: bool = False
    spreadsheet_id: str = ""
    spreadsheet_title: str = ""
    target_mode: str = ""
    read_tabs: int = 0
    write_tabs: int = 0
    read_rows: int = 0
    write_rows: int = 0
    status: str = "CSV_ONLY"
    error_type: str = ""
    error_message: str = ""
    details: list[dict[str, Any]] = field(default_factory=list)

    def row(self, phase: str) -> dict[str, Any]:
        return {
            "version": VERSION,
            "captured_at": _now_iso(),
            "phase": phase,
            "requested": self.requested,
            "available": self.available,
            "spreadsheet_id": self.spreadsheet_id,
            "spreadsheet_title": self.spreadsheet_title,
            "target_mode": self.target_mode,
            "read_tabs": self.read_tabs,
            "write_tabs": self.write_tabs,
            "read_rows": self.read_rows,
            "write_rows": self.write_rows,
            "status": self.status,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "capture_profile": os.environ.get("TEST_PROFILE", "UNKNOWN"),
            "capture_run_id": os.environ.get("GITHUB_RUN_ID", "LOCAL"),
            "live_logic_changed": False,
            "real_order_changed": False,
        }


class CatalystGoogleSheetStore:
    def __init__(self) -> None:
        self.status = StoreStatus(requested=sheet_primary_requested())
        self._client: Any = None
        self._book: Any = None

    def connect(self) -> StoreStatus:
        if not self.status.requested:
            self.status.status = "CSV_ONLY_DISABLED"
            return self.status
        try:
            import gspread  # type: ignore
            from google.oauth2.service_account import Credentials  # type: ignore

            info = _credential_payload()
            scopes = (
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.readonly",
            )
            credentials = Credentials.from_service_account_info(info, scopes=list(scopes))
            self._client = gspread.authorize(credentials)
            self._book, mode = self._resolve_book(self._client)
            self.status.available = True
            self.status.spreadsheet_id = str(getattr(self._book, "id", "") or "")
            self.status.spreadsheet_title = str(getattr(self._book, "title", "") or "")
            self.status.target_mode = mode
            self.status.status = "SHEET_PRIMARY_READY"
        except Exception as exc:
            self.status.available = False
            self.status.status = "CSV_FALLBACK_NO_SHEET"
            self.status.error_type = type(exc).__name__
            self.status.error_message = str(exc)[:500]
        return self.status

    def _resolve_book(self, client: Any) -> tuple[Any, str]:
        sid, surl, sname = _target_hints()
        if sid:
            return client.open_by_key(sid), "ID"
        if surl:
            return client.open_by_url(surl), "URL"
        if sname:
            return client.open(sname), "NAME"

        # Reuse an already shared Stock-Hunter spreadsheet without creating an invisible service-account-owned file.
        books = list(client.openall())
        if len(books) == 1:
            return books[0], "SOLE_SHARED"
        by_title = {str(getattr(b, "title", "")).strip().lower(): b for b in books}
        for title in DEFAULT_TITLE_CANDIDATES:
            if title.lower() in by_title:
                return by_title[title.lower()], "KNOWN_TITLE"
        stock_like = [b for b in books if re.search(r"stock\s*[-_ ]?hunter|종목|주식", str(getattr(b, "title", "")), re.I)]
        if len(stock_like) == 1:
            return stock_like[0], "UNIQUE_STOCK_LIKE"
        raise RuntimeError(f"SHEET_TARGET_AMBIGUOUS shared={len(books)}; set CATALYST_GOOGLE_SHEET_ID")

    def _worksheet(self, title: str, create: bool) -> Any:
        if not self._book:
            raise RuntimeError("SHEET_NOT_CONNECTED")
        try:
            return self._book.worksheet(title)
        except Exception:
            if not create:
                return None
            return self._book.add_worksheet(title=title, rows=100, cols=40)

    @staticmethod
    def _values_to_df(values: list[list[Any]]) -> pd.DataFrame:
        if not values:
            return pd.DataFrame()
        headers = [str(x or "").strip() for x in values[0]]
        if not any(headers):
            return pd.DataFrame()
        width = len(headers)
        rows = [(list(r) + [""] * width)[:width] for r in values[1:]]
        q = pd.DataFrame(rows, columns=headers)
        q = q.dropna(how="all")
        if not q.empty:
            q = q.loc[~q.astype(str).apply(lambda r: all(not x.strip() for x in r), axis=1)].reset_index(drop=True)
        return q

    def pull(self, tabs: Mapping[str, str] = TAB_FILE_MAP) -> dict[str, pd.DataFrame]:
        result: dict[str, pd.DataFrame] = {}
        if not self.status.available:
            return result
        try:
            for tab, filename in tabs.items():
                ws = self._worksheet(tab, create=False)
                if ws is None:
                    continue
                values = ws.get_all_values()
                frame = self._values_to_df(values)
                result[filename] = frame
                self.status.read_tabs += 1
                self.status.read_rows += len(frame)
                self.status.details.append({"phase": "READ", "tab": tab, "file": filename, "rows": len(frame), "status": "OK"})
            self.status.status = "SHEET_PRIMARY_READ_OK"
        except Exception as exc:
            self.status.status = "SHEET_READ_FALLBACK"
            self.status.error_type = type(exc).__name__
            self.status.error_message = str(exc)[:500]
        return result

    @staticmethod
    def _safe_cell(value: Any) -> Any:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return ""
        if isinstance(value, (bool, int, float)):
            return value
        text = str(value)
        # Prevent untrusted news titles or summaries from becoming spreadsheet formulas,
        # while preserving numeric strings such as -1.25 and +3.4 as usable values.
        numeric_text = bool(re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", text.strip()))
        if text.startswith(("=", "@")) or (text.startswith(("+", "-")) and not numeric_text):
            return "'" + text
        return text[:50000]

    def _replace_frame(self, tab: str, frame: pd.DataFrame) -> int:
        ws = self._worksheet(tab, create=_env_on("CATALYST_SHEET_CREATE_TABS", "1"))
        if ws is None:
            raise RuntimeError(f"SHEET_TAB_MISSING:{tab}")
        q = frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
        columns = [str(c) for c in q.columns]
        matrix: list[list[Any]] = [columns]
        if not q.empty:
            for row in q.where(pd.notna(q), "").itertuples(index=False, name=None):
                matrix.append([self._safe_cell(v) for v in row])
        needed_rows = max(2, len(matrix) + 5)
        needed_cols = max(2, len(columns) + 2)
        try:
            ws.resize(rows=needed_rows, cols=needed_cols)
        except Exception:
            pass
        ws.clear()
        chunk = max(100, int(os.environ.get("CATALYST_SHEET_BATCH_ROWS", "1000") or 1000))
        for start in range(0, len(matrix), chunk):
            block = matrix[start:start + chunk]
            cell = f"A{start + 1}"
            try:
                ws.update(values=block, range_name=cell, value_input_option="RAW")
            except TypeError:
                ws.update(cell, block, value_input_option="RAW")
        return len(q)

    def push(self, frames_by_file: Mapping[str, pd.DataFrame], tabs: Mapping[str, str] = TAB_FILE_MAP) -> StoreStatus:
        if not self.status.available:
            return self.status
        reverse = {filename: tab for tab, filename in tabs.items()}
        try:
            for filename, frame in frames_by_file.items():
                tab = reverse.get(filename)
                if not tab:
                    continue
                rows = self._replace_frame(tab, frame)
                self.status.write_tabs += 1
                self.status.write_rows += rows
                self.status.details.append({"phase": "WRITE", "tab": tab, "file": filename, "rows": rows, "status": "OK"})
            self.status.status = "SHEET_PRIMARY_OK"
        except Exception as exc:
            self.status.status = "SHEET_WRITE_PENDING"
            self.status.error_type = type(exc).__name__
            self.status.error_message = str(exc)[:500]
        return self.status


def append_storage_audit(output_dir: str | Path, rows: list[dict[str, Any]]) -> Path:
    out = Path(output_dir or "reports")
    out.mkdir(parents=True, exist_ok=True)
    path = out / SHEET_AUDIT_FILE
    incoming = pd.DataFrame(rows)
    if path.exists() and path.stat().st_size:
        try:
            old = pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")
        except Exception:
            old = pd.DataFrame()
        incoming = pd.concat([old, incoming], ignore_index=True)
    incoming.to_csv(path, index=False, encoding="utf-8-sig")
    return path
