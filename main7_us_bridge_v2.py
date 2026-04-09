# -*- coding: utf-8 -*-
"""US scanner bridge for main7_bugfix_2.py"""

from main7_bugfix_2 import (
    get_indicators,
    classify_style,
    judge_trade_with_sequence,
    build_default_signals,
    inject_tri_result,
    calc_pivot_levels,
    calc_fibonacci_levels,
    calc_atr_targets,
    send_telegram_photo,
    send_telegram_chunks,
    send_tournament_results,
    get_ai_summary_batch,
    run_ai_tournament,
    _fetch_stock_news,
    update_google_sheet,
    TELEGRAM_REAL_CHAT_ID,
    TELEGRAM_TEST_CHAT_ID,
    get_target_chat_ids,
    OPENAI_API_KEY,
    ANTHROPIC_API_KEY,
    GEMINI_API_KEY,
    GROQ_API_KEY,
    TODAY_STR,
    KST,
    prepare_historical_weather,
)

# Backward-compat aliases for older scanner code
TELEGRAM_TOKEN = ''
CHAT_ID_LIST = get_target_chat_ids()
