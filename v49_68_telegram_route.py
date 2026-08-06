from __future__ import annotations

"""Authoritative Telegram route resolver for both normal run and merge jobs.

The workflow maps all supported GitHub Secret/Variable aliases into the canonical
variables consumed here. This script validates an actual bot-token/chat-id pair
with Telegram getChat, writes the selected canonical pair to GITHUB_ENV, and
never prints credential values.
"""

import argparse
import json
import os
from pathlib import Path
import sys
from typing import Iterable

import requests

TRUTHY = {"1", "true", "yes", "y", "on"}


def _state(name: str) -> str:
    value = str(os.environ.get(name, "") or "")
    return "EMPTY" if not value else f"SET(len={len(value)})"


def _split_ids(value: str) -> list[str]:
    return [x.strip() for x in str(value or "").split(",") if x.strip()]


def _mask_chat(chat_id: str) -> str:
    c = str(chat_id or "").strip()
    if len(c) <= 6:
        return "*" * len(c)
    return c[:3] + "***" + c[-3:]


def _write_env(name: str, value: str) -> None:
    env_file = str(os.environ.get("GITHUB_ENV", "") or "").strip()
    if env_file:
        with open(env_file, "a", encoding="utf-8") as f:
            f.write(f"{name}={value}\n")
    os.environ[name] = value


def _add_pair(
    pairs: list[tuple[str, str, str, str]],
    seen: set[tuple[str, str]],
    token_name: str,
    token: str,
    chat_name: str,
    chats: str,
) -> None:
    token = str(token or "").strip()
    if not token:
        return
    for chat_id in _split_ids(chats):
        key = (token, chat_id)
        if key not in seen:
            seen.add(key)
            pairs.append((token_name, token, chat_name, chat_id))


def resolve(send_requested: bool, timeout: float = 10.0) -> dict:
    print("TELEGRAM ROUTE MATRIX v49.68")
    for name in (
        "CLOSING_BET_TOKEN",
        "TELEGRAM_CLOSEBET_TOKEN",
        "TELEGRAM_TOKEN",
        "CLOSING_BET_CHAT_ID",
        "TELEGRAM_CHAT_ID",
        "TELEGRAM_DYUL_CHAT_ID",
    ):
        print(f"{name:26s}= {_state(name)}")
    print("CLOSING_BET_CHAT_SRC      =", os.environ.get("CLOSING_BET_CHAT_ID_SOURCE", "EMPTY"))

    if not send_requested:
        _write_env("TELEGRAM_ROUTE_VALIDATED", "SKIPPED")
        print("TELEGRAM_ROUTE_CHECK = SKIP(send_telegram=false)")
        return {"status": "SKIPPED", "send_requested": False}

    primary = str(os.environ.get("CLOSING_BET_CHAT_ID", "") or "")
    legacy = str(os.environ.get("TELEGRAM_DYUL_CHAT_ID", "") or "")
    seen: set[tuple[str, str]] = set()
    pairs: list[tuple[str, str, str, str]] = []

    # Dedicated-room candidates only. Generic TELEGRAM_CHAT_ID is diagnostic
    # and is never accepted as an unverified fallback.
    _add_pair(pairs, seen, "CLOSING_BET_TOKEN", os.environ.get("CLOSING_BET_TOKEN", ""), "CLOSING_BET_CHAT_ID", primary)
    _add_pair(pairs, seen, "TELEGRAM_CLOSEBET_TOKEN", os.environ.get("TELEGRAM_CLOSEBET_TOKEN", ""), "CLOSING_BET_CHAT_ID", primary)
    _add_pair(pairs, seen, "TELEGRAM_TOKEN", os.environ.get("TELEGRAM_TOKEN", ""), "CLOSING_BET_CHAT_ID", primary)
    _add_pair(pairs, seen, "TELEGRAM_TOKEN", os.environ.get("TELEGRAM_TOKEN", ""), "TELEGRAM_DYUL_CHAT_ID", legacy)
    _add_pair(pairs, seen, "CLOSING_BET_TOKEN", os.environ.get("CLOSING_BET_TOKEN", ""), "TELEGRAM_DYUL_CHAT_ID", legacy)
    _add_pair(pairs, seen, "TELEGRAM_CLOSEBET_TOKEN", os.environ.get("TELEGRAM_CLOSEBET_TOKEN", ""), "TELEGRAM_DYUL_CHAT_ID", legacy)

    if not pairs:
        raise RuntimeError(
            "TELEGRAM ROUTE INVALID: no dedicated token/chat candidates. "
            "The normal run and merge jobs now share the same alias matrix; check the existing close-bet Secret/Variable aliases."
        )

    failures: list[str] = []
    selected: tuple[str, str, str, str] | None = None
    selected_chat_meta: dict = {}
    for token_name, token, chat_name, chat_id in pairs:
        try:
            response = requests.get(
                f"https://api.telegram.org/bot{token}/getChat",
                params={"chat_id": chat_id},
                timeout=timeout,
            )
            payload = response.json() if response.content else {}
            if response.status_code == 200 and isinstance(payload, dict) and payload.get("ok") is True:
                selected = (token_name, token, chat_name, chat_id)
                result = payload.get("result") or {}
                selected_chat_meta = {
                    "type": str(result.get("type", "")),
                    "title": str(result.get("title", ""))[:80],
                    "username": str(result.get("username", ""))[:80],
                }
                break
            description = str((payload or {}).get("description", response.text[:160]))
            failures.append(f"{token_name}+{chat_name}({_mask_chat(chat_id)}): HTTP {response.status_code} {description[:120]}")
        except Exception as exc:
            failures.append(f"{token_name}+{chat_name}({_mask_chat(chat_id)}): {type(exc).__name__}: {str(exc)[:120]}")

    if selected is None:
        print("TELEGRAM_ROUTE_CANDIDATES_FAILED =")
        for failure in failures:
            print("-", failure)
        raise RuntimeError(
            "TELEGRAM ROUTE INVALID: no valid dedicated TOKEN/CHAT_ID pair. "
            "Check bot membership, -100 group/channel prefix, stale legacy chat id, or a token/chat mismatch."
        )

    token_name, token, chat_name, chat_id = selected
    configured_alias_source = str(os.environ.get('CLOSING_BET_CHAT_ID_SOURCE', '') or '').strip()
    chat_source = configured_alias_source if chat_name == 'CLOSING_BET_CHAT_ID' and configured_alias_source not in ('', 'EMPTY') else chat_name
    pair_source = f"{token_name}+{chat_source}"
    _write_env("TELEGRAM_TOKEN", token)
    _write_env("CLOSING_BET_TOKEN", token)
    _write_env("CLOSING_BET_CHAT_ID", chat_id)
    _write_env("TELEGRAM_CHAT_ID", chat_id)
    _write_env("CLOSING_BET_CHAT_ID_SOURCE", chat_source)
    _write_env("TELEGRAM_ROUTE_ALIAS_SOURCE", chat_source)
    _write_env("TELEGRAM_ROUTE_PAIR_SOURCE", pair_source)
    _write_env("TELEGRAM_ROUTE_VALIDATED", "1")
    _write_env("TELEGRAM_ROUTE_CHAT_MASKED", _mask_chat(chat_id))

    result = {
        "status": "VALID",
        "send_requested": True,
        "pair_source": pair_source,
        "chat_id_masked": _mask_chat(chat_id),
        "chat": selected_chat_meta,
        "failed_candidates": len(failures),
    }
    print(
        "TELEGRAM_ROUTE_CHECK = PASS | "
        f"pair={pair_source} | chat={result['chat_id_masked']} | type={selected_chat_meta.get('type') or '-'}"
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="v49.68 unified Telegram route preflight")
    parser.add_argument("--send", default=os.environ.get("INPUT_SEND_TELEGRAM", "true"))
    parser.add_argument("--json-output", default="")
    args = parser.parse_args()
    send_requested = str(args.send or "true").strip().lower() in TRUTHY
    try:
        result = resolve(send_requested=send_requested)
    except Exception as exc:
        result = {
            "status": "FAILED",
            "send_requested": send_requested,
            "error": f"{type(exc).__name__}: {exc}",
        }
        if args.json_output:
            out = Path(args.json_output)
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        raise
    if args.json_output:
        out = Path(args.json_output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"TELEGRAM_ROUTE_CHECK = FAIL | {type(exc).__name__}: {exc}")
        sys.exit(2)
