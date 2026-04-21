from __future__ import annotations

import re
from typing import Callable, List, Tuple


DIVIDER = '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
SECTION_PREFIXES = (
    '🍉 [', '🧭 [', '🚀 [', '🔬 [', '🛡️ [', '📢 [', '👀 [',
    '🤖 [', '🔄 [', '🌍 [', '📰 [', '🇺🇸➡🇰🇷 ['
)


def _normalize_message_layout(message: str) -> str:
    lines = str(message or '').replace('\r\n', '\n').split('\n')
    out = []
    prev_divider = False
    blank_run = 0

    for raw in lines:
        line = raw.rstrip()
        is_divider = line.strip() == DIVIDER
        is_blank = line.strip() == ''

        if is_divider:
            if prev_divider:
                continue
            out.append(DIVIDER)
            prev_divider = True
            blank_run = 0
            continue

        prev_divider = False

        if is_blank:
            blank_run += 1
            if blank_run > 1:
                continue
            out.append('')
            continue

        blank_run = 0
        out.append(line)

    text = '\n'.join(out).strip()
    text = re.sub(r'^(?:' + re.escape(DIVIDER) + r'\n){2,}', DIVIDER + '\n', text)
    text = re.sub(r'(?:\n' + re.escape(DIVIDER) + r'){2,}$', '\n' + DIVIDER, text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text


def _split_sections(message: str) -> List[Tuple[str, str]]:
    sections: List[Tuple[str, str]] = []
    current_header = ''
    current_lines: List[str] = []

    for line in str(message or '').split('\n'):
        s = line.rstrip()
        if s.startswith(SECTION_PREFIXES):
            if current_header or current_lines:
                sections.append((current_header, '\n'.join(current_lines).strip()))
            current_header = s
            current_lines = []
            continue
        current_lines.append(s)

    if current_header or current_lines:
        sections.append((current_header, '\n'.join(current_lines).strip()))

    return sections


def _split_cards(sections: List[Tuple[str, str]]) -> List[str]:
    cards: List[str] = []

    for header, body in sections:
        body = body.strip()
        if not body:
            cards.append(header)
            continue

        parts = [part.strip() for part in body.split(DIVIDER) if part.strip()]
        if not parts:
            cards.append((header + '\n\n' + body).strip())
            continue

        for i, part in enumerate(parts):
            if i == 0:
                cards.append((header + '\n\n' + part).strip())
            else:
                cards.append(part)

    return cards


def build_telegram_chunks(message: str, title: str = '', max_len: int = 3800) -> List[str]:
    message = _normalize_message_layout(str(message or ''))
    if not message:
        return []

    sections = _split_sections(message)
    cards = _split_cards(sections)

    chunks: List[str] = []
    current = (title.strip() + '\n\n') if title else ''

    for card in cards:
        card_text = f"{DIVIDER}\n{card.strip()}\n{DIVIDER}\n\n"
        if len(current) + len(card_text) > max_len and current.strip():
            chunks.append(current.strip())
            current = ((title.strip() + '\n\n') if title else '') + card_text
        else:
            current += card_text

    if current.strip():
        chunks.append(current.strip())

    total = len(chunks)
    if total <= 1:
        return chunks

    return [f"({idx}/{total})\n{chunk}" for idx, chunk in enumerate(chunks, 1)]


def send_telegram_chunks_core(
    message: str,
    send_func: Callable[[str], object],
    title: str = '',
    max_len: int = 3800,
):
    for chunk in build_telegram_chunks(message=message, title=title, max_len=max_len):
        send_func(chunk)
