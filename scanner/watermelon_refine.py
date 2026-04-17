from __future__ import annotations

from .utils import boolish, text_join


def build_refine_validation_text(row) -> str:
    data = dict(row) if isinstance(row, dict) else getattr(row, 'to_dict', lambda: {})()
    passed = []
    lacked = []
    checks = [
        ('수박정제_vol_ok', '거래량'),
        ('수박정제_reclaim_ok', '5일재안착'),
        ('수박정제_candle_ok', '양봉/캔들'),
        ('수박정제_wick_ok', '윗꼬리'),
        ('수박정제_long_ok', '장기이평'),
        ('수박정제_cloud_ok', '저항구름 안착'),
        ('수박정제_obv_ok', 'OBV매집'),
    ]
    for key, label in checks:
        value = data.get(key, None)
        if value is None:
            continue
        if boolish(value):
            passed.append(label)
        else:
            lacked.append(label)

    if passed and not lacked:
        return '통과항목: ' + ' / '.join(passed)
    if lacked and not passed:
        return '의심사유: ' + ' / '.join(lacked)
    if passed or lacked:
        return text_join([
            ('통과항목: ' + ' / '.join(passed)) if passed else '',
            ('보완필요: ' + ' / '.join(lacked)) if lacked else '',
        ], sep=' | ')
    return ''
