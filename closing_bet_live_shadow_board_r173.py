#!/usr/bin/env python3
from __future__ import annotations
import os
import closing_bet_live_shadow_board_r17 as r17

VERSION="CLOSEBET_LIVE_SHADOW_BOARD_R173_SINGLE_ROUTE_LEGACY_LAST_20260921"

TOKEN_KEYS=[
    "CLOSING_BET_TOKEN",
    "TELEGRAM_CLOSEBET_TOKEN",
    "TELEGRAM_CLOSE_BET_TOKEN",
    "CLOSE_BET_TOKEN",
    "CLOSING_BET_TELEGRAM_TOKEN",
    "CLOSE_BET_TELEGRAM_TOKEN",
    "TELEGRAM_TOKEN",
]

DEDICATED_CHAT_KEYS=[
    "CLOSING_BET_CHAT_ID",
    "TELEGRAM_CLOSEBET_CHAT_ID",
    "TELEGRAM_CLOSE_BET_CHAT_ID",
    "CLOSE_BET_CHAT_ID",
    "CLOSING_BET_TELEGRAM_CHAT_ID",
    "CLOSE_BET_TELEGRAM_CHAT_ID",
    "TELEGRAM_CLOSEBET_ROOM_ID",
    "TELEGRAM_CLOSE_BET_ROOM_ID",
    "CLOSING_BET_ROOM_ID",
    "CLOSE_BET_ROOM_ID",
]

LEGACY_LAST_KEYS=[
    "TELEGRAM_DYUL_CHAT_ID",
]

def _first(keys):
    for k in keys:
        v=(os.environ.get(k) or "").strip()
        if v:
            return v,k
    return "","EMPTY"

def _primary_chat(raw):
    vals=[x.strip() for x in (raw or "").split(",") if x.strip()]
    return (vals[0] if vals else ""), len(vals)

def _resolve_chat():
    raw,src=_first(DEDICATED_CHAT_KEYS)
    if raw:
        chat,count=_primary_chat(raw)
        return chat,src,count,False

    raw,src=_first(LEGACY_LAST_KEYS)
    if raw:
        chat,count=_primary_chat(raw)
        return chat,src,count,True

    return "","EMPTY",0,False

def _send_parts(parts):
    import requests

    token,token_source=_first(TOKEN_KEYS)
    chat,chat_source,raw_chat_count,used_legacy=_resolve_chat()

    if not token or not chat:
        return {
            "status":"SKIPPED_NO_ROUTE",
            "parts":len(parts),
            "attempts":0,
            "success":0,
            "errors":[],
            "route_token_source":token_source,
            "route_chat_source":chat_source,
            "route_chat_count":0,
            "raw_route_chat_count":raw_chat_count,
            "single_route_guard":True,
            "legacy_last_fallback_allowed":True,
            "legacy_last_fallback_used":used_legacy,
            "generic_telegram_chat_disabled":True,
        }

    url=f"https://api.telegram.org/bot{token}/sendMessage"
    attempts=ok=0
    errors=[]

    for i,p in enumerate(parts,1):
        attempts+=1
        try:
            rr=requests.post(
                url,
                data={
                    "chat_id":chat,
                    "text":p,
                    "disable_web_page_preview":"true",
                },
                timeout=20,
            )
            if rr.ok:
                ok+=1
            else:
                errors.append({
                    "part":i,
                    "status_code":rr.status_code,
                    "body":rr.text[:160],
                })
        except Exception as e:
            errors.append({"part":i,"error":type(e).__name__})

    return {
        "status":"DELIVERED" if attempts and ok==attempts else "PARTIAL_OR_FAILED",
        "parts":len(parts),
        "chat_count":1,
        "attempts":attempts,
        "success":ok,
        "errors":errors,
        "route_token_source":token_source,
        "route_chat_source":chat_source,
        "route_chat_count":1,
        "raw_route_chat_count":raw_chat_count,
        "single_route_guard":True,
        "legacy_last_fallback_allowed":True,
        "legacy_last_fallback_used":used_legacy,
        "generic_telegram_chat_disabled":True,
    }

def main():
    r17.VERSION=VERSION
    r17._send_parts=_send_parts
    r17.main()

if __name__=="__main__":
    main()
