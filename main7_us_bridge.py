"""Compatibility bridge for usStockScanner -> main7_bugfix_2.

Goal:
- Keep usStockScanner thin and US-specific.
- Reuse shared engine pieces from main7_bugfix_2.
- Provide compatibility names expected by older usStockScanner code.
"""

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
    TELEGRAM_TOKEN,
    OPENAI_API_KEY,
    ANTHROPIC_API_KEY,
    GEMINI_API_KEY,
    GROQ_API_KEY,
    TODAY_STR,
    KST,
    prepare_historical_weather,
    get_target_chat_ids,
)

# Older usStockScanner imported CHAT_ID_LIST, but main7_bugfix_2 uses
# get_target_chat_ids() instead of a flat global list.
try:
    CHAT_ID_LIST, CHAT_MODE = get_target_chat_ids()
except Exception:
    CHAT_ID_LIST, CHAT_MODE = [], 'unknown'
