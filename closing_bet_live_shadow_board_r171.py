#!/usr/bin/env python3
from __future__ import annotations
import os
import closing_bet_live_shadow_board_r17 as r17

VERSION="CLOSEBET_LIVE_SHADOW_BOARD_R171_TELEGRAM_ROUTE_FIX_20260921"

TOKEN_KEYS=[
"CLOSING_BET_TOKEN","TELEGRAM_CLOSEBET_TOKEN","TELEGRAM_CLOSE_BET_TOKEN",
"CLOSE_BET_TOKEN","CLOSING_BET_TELEGRAM_TOKEN","CLOSE_BET_TELEGRAM_TOKEN","TELEGRAM_TOKEN"
]
CHAT_KEYS=[
"CLOSING_BET_CHAT_ID","TELEGRAM_CLOSEBET_CHAT_ID","TELEGRAM_CLOSE_BET_CHAT_ID",
"CLOSE_BET_CHAT_ID","CLOSING_BET_TELEGRAM_CHAT_ID","CLOSE_BET_TELEGRAM_CHAT_ID",
"TELEGRAM_CLOSEBET_ROOM_ID","TELEGRAM_CLOSE_BET_ROOM_ID","CLOSING_BET_ROOM_ID",
"CLOSE_BET_ROOM_ID","TELEGRAM_DYUL_CHAT_ID","TELEGRAM_CHAT_ID"
]

def _first(keys):
    for k in keys:
        v=(os.environ.get(k) or "").strip()
        if v:return v,k
    return "","EMPTY"

def _send_parts(parts):
    import requests
    token,ts=_first(TOKEN_KEYS)
    chat_raw,cs=_first(CHAT_KEYS)
    chats=[x.strip() for x in chat_raw.split(",") if x.strip()]
    if not token or not chats:
        return {"status":"SKIPPED_NO_ROUTE","parts":len(parts),"success":0,"errors":[],
                "route_token_source":ts,"route_chat_source":cs,"route_chat_count":len(chats)}
    attempts=ok=0; errors=[]
    url=f"https://api.telegram.org/bot{token}/sendMessage"
    for chat in chats:
        for i,p in enumerate(parts,1):
            attempts+=1
            try:
                rr=requests.post(url,data={"chat_id":chat,"text":p,"disable_web_page_preview":"true"},timeout=20)
                if rr.ok: ok+=1
                else: errors.append({"part":i,"status_code":rr.status_code,"body":rr.text[:160]})
            except Exception as e:
                errors.append({"part":i,"error":type(e).__name__})
    return {"status":"DELIVERED" if attempts and ok==attempts else "PARTIAL_OR_FAILED",
            "parts":len(parts),"chat_count":len(chats),"attempts":attempts,"success":ok,"errors":errors,
            "route_token_source":ts,"route_chat_source":cs,"route_chat_count":len(chats)}

def main():
    r17.VERSION=VERSION
    r17._send_parts=_send_parts
    r17.main()

if __name__=="__main__":
    main()
