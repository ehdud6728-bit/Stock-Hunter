import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
import matplotlib.dates as mdates
import numpy as np
import FinanceDataReader as fdr
from datetime import datetime, timedelta
from indicator_engine import get_indicators

# ================================================================
# ✅ 수박지표 차트 시각화
# 구성:
#   - 상단: 캔들 + MA5/20/60/112/224 + BB40
#   - 중단: 거래량 (초록/빨강 구분)
#   - 하단: 수박지표 (초록=축적, 빨강=신호, 강도 0~3)
# ================================================================

def plot_watermelon_chart(df, ticker, name, save_path=None):
    """
    df: get_indicators() 통과한 DataFrame
    ticker: 종목코드
    name: 종목명
    save_path: 저장 경로 (None이면 화면 출력)
    """
    df = df.copy().tail(120)  # 최근 120일
    df = df.reset_index()

    x = np.arange(len(df))
    dates = df['Date'] if 'Date' in df.columns else df.index

    # ── 레이아웃: 3분할 (캔들 60% / 거래량 20% / 수박 20%)
    fig = plt.figure(figsize=(16, 10), facecolor='#1a1a2e')
    gs  = gridspec.GridSpec(3, 1, height_ratios=[6, 2, 2], hspace=0.04)

    ax_candle = fig.add_subplot(gs[0])
    ax_vol    = fig.add_subplot(gs[1], sharex=ax_candle)
    ax_melon  = fig.add_subplot(gs[2], sharex=ax_candle)

    for ax in [ax_candle, ax_vol, ax_melon]:
        ax.set_facecolor('#1a1a2e')
        ax.tick_params(colors='#aaaaaa', labelsize=8)
        ax.spines['bottom'].set_color('#333355')
        ax.spines['top'].set_color('#333355')
        ax.spines['left'].set_color('#333355')
        ax.spines['right'].set_color('#333355')
        ax.yaxis.label.set_color('#aaaaaa')

    # ────────────────────────────────────────
    # [1] 캔들스틱
    # ────────────────────────────────────────
    for i, row in df.iterrows():
        color = '#ff4444' if row['Close'] >= row['Open'] else '#4488ff'
        # 심지
        ax_candle.plot([i, i], [row['Low'], row['High']], color=color, linewidth=0.8)
        # 몸통
        body_low  = min(row['Open'], row['Close'])
        body_high = max(row['Open'], row['Close'])
        ax_candle.bar(i, body_high - body_low, bottom=body_low,
                      color=color, width=0.6, linewidth=0)

    # 이동평균선
    ma_styles = [
        ('MA5',   '#ffff00', 0.8, '--'),
        ('MA20',  '#ff9900', 1.0, '-'),
        ('MA60',  '#00cc88', 1.0, '-'),
        ('MA112', '#ff44ff', 1.2, '-'),
        ('MA224', '#aaaaff', 1.2, '-'),
    ]
    for col, color, lw, ls in ma_styles:
        if col in df.columns:
            ax_candle.plot(x, df[col], color=color, linewidth=lw,
                           linestyle=ls, label=col, alpha=0.85)

    # BB40 밴드
    if 'BB40_Upper' in df.columns and 'BB40_Lower' in df.columns:
        ax_candle.fill_between(x, df['BB40_Upper'], df['BB40_Lower'],
                               alpha=0.07, color='#8888ff')
        ax_candle.plot(x, df['BB40_Upper'], color='#8888ff', linewidth=0.5, linestyle=':')
        ax_candle.plot(x, df['BB40_Lower'], color='#8888ff', linewidth=0.5, linestyle=':')

    # 수박 신호 마킹 (▲)
    if 'Watermelon_Signal' in df.columns:
        sig_idx = df[df['Watermelon_Signal'] == True].index
        for i in sig_idx:
            ax_candle.annotate('🍉', xy=(i, df.loc[i, 'Low'] * 0.985),
                               fontsize=10, ha='center', color='#ff4444')

    ax_candle.legend(loc='upper left', fontsize=7, facecolor='#1a1a2e',
                     labelcolor='white', framealpha=0.5)
    ax_candle.set_title(f'[{ticker}] {name}  수박지표 차트',
                        color='white', fontsize=13, pad=8)
    ax_candle.set_ylabel('가격', color='#aaaaaa')

    # ────────────────────────────────────────
    # [2] 거래량
    # ────────────────────────────────────────
    for i, row in df.iterrows():
        color = '#ff4444' if row['Close'] >= row['Open'] else '#4488ff'
        ax_vol.bar(i, row['Volume'], color=color, width=0.6,
                   alpha=0.7, linewidth=0)

    if 'VMA20' in df.columns:
        ax_vol.plot(x, df['VMA20'], color='#ffff00', linewidth=0.8,
                    linestyle='--', label='VMA20')

    ax_vol.set_ylabel('거래량', color='#aaaaaa')
    ax_vol.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda val, _: f'{val/1e6:.0f}M' if val >= 1e6 else f'{val/1e3:.0f}K')
    )

    # ────────────────────────────────────────
    # [3] 수박지표 (핵심)
    # ────────────────────────────────────────
    # 색상 규칙:
    #   빨강 (강도 3/3): #ff2222  → 신호 최강
    #   빨강 (강도 2/3): #ff6622  → 신호 보통
    #   빨강 (강도 1/3): #ff9944  → 신호 약
    #   초록 (축적 중):  #22cc44  → 초록 상태
    #   회색 (중립):     #444455  → 기본

    COLOR_MAP = {
        ('red',   3): '#ff2222',
        ('red',   2): '#ff6622',
        ('red',   1): '#ff9944',
        ('green', 3): '#22cc44',
        ('green', 2): '#44aa44',
        ('green', 1): '#336633',
        ('none',  0): '#333344',
    }

    for i, row in df.iterrows():
        wc    = row.get('Watermelon_Color', 'none')
        score = int(row.get('Watermelon_Score', 0))
        key   = (wc, min(score, 3))
        color = COLOR_MAP.get(key, '#333344')
        height = max(score, 1)   # 최소 1칸 높이
        ax_melon.bar(i, height, color=color, width=0.7, linewidth=0)

    # 수박 신호 발생일에 별도 마킹
    if 'Watermelon_Signal' in df.columns:
        sig_idx = df[df['Watermelon_Signal'] == True].index
        for i in sig_idx:
            ax_melon.axvline(x=i, color='#ffffff', linewidth=0.8,
                             linestyle='--', alpha=0.5)

    ax_melon.set_ylabel('수박강도', color='#aaaaaa')
    ax_melon.set_ylim(0, 4)
    ax_melon.set_yticks([1, 2, 3])
    ax_melon.set_yticklabels(['1', '2', '3'], color='#aaaaaa', fontsize=7)

    # 범례
    patches = [
        mpatches.Patch(color='#ff2222', label='빨강 강도3 (신호)'),
        mpatches.Patch(color='#ff6622', label='빨강 강도2'),
        mpatches.Patch(color='#22cc44', label='초록 (축적)'),
        mpatches.Patch(color='#333344', label='중립'),
    ]
    ax_melon.legend(handles=patches, loc='upper left', fontsize=6,
                    facecolor='#1a1a2e', labelcolor='white',
                    framealpha=0.5, ncol=4)

    # ── X축 날짜 표시
    tick_step = max(1, len(df) // 12)
    tick_positions = x[::tick_step]
    tick_labels = []
    for pos in tick_positions:
        try:
            d = df.iloc[pos]['Date'] if 'Date' in df.columns else df.index[pos]
            tick_labels.append(pd.Timestamp(d).strftime('%m/%d'))
        except:
            tick_labels.append('')

    ax_melon.set_xticks(tick_positions)
    ax_melon.set_xticklabels(tick_labels, color='#aaaaaa', fontsize=7, rotation=30)
    plt.setp(ax_candle.get_xticklabels(), visible=False)
    plt.setp(ax_vol.get_xticklabels(), visible=False)

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=130, bbox_inches='tight',
                    facecolor='#1a1a2e')
        plt.close()
        print(f"✅ 차트 저장: {save_path}")
        return save_path
    else:
        plt.show()
        return None


# ================================================================
# ✅ 스캐너 연동 — 포착된 종목 차트 자동 생성 후 텔레그램 전송
# ================================================================

def create_watermelon_charts_for_hits(ai_candidates_df, top_n=5):
    chart_paths = []

    # ✅ 수박 관련 종목만 필터링
    for _col in ['N구분', 'N조합', '구분', '종목명', 'code']:
        if _col not in ai_candidates_df.columns:
            ai_candidates_df[_col] = ''

    melon_df = ai_candidates_df[
        ai_candidates_df['N구분'].astype(str).str.contains('🍉', na=False) |      # 수박 태그
        ai_candidates_df['N조합'].astype(str).str.contains('수박', na=False) |    # 수박 조합
        ai_candidates_df['구분'].astype(str).str.contains('수박', na=False)       # 수박 구분
    ]

    if melon_df.empty:
        print("🍉 수박 신호 종목 없음 → 차트 생성 스킵")
        return []

    print(f"🍉 수박 신호 종목 {len(melon_df)}개 → 상위 {top_n}개 차트 생성")

    for _, item in melon_df.head(top_n).iterrows():
        ticker = item['code']
        name   = item['종목명']
        try:
            df_raw = fdr.DataReader(ticker,
                start=(datetime.now() - timedelta(days=250)).strftime('%Y-%m-%d'))
            if len(df_raw) < 60:
                continue

            df_ind = get_indicators(df_raw)
            if df_ind is None or df_ind.empty:
                continue

            path = f"/tmp/chart_{ticker}.png"
            plot_watermelon_chart(df_ind, ticker, name, save_path=path)
            chart_paths.append(path)
            print(f"📊 차트 생성: {name}({ticker})")

        except Exception as e:
            print(f"🚨 차트 생성 실패 [{name}]: {e}")

    return chart_paths


# ================================================================
# ✅ main 블록 적용 가이드
# ================================================================
"""
[1] 차트 생성 후 텔레그램 전송 (tournament_report 아래에 추가):

    print("📊 수박지표 차트 생성 중...")
    chart_paths = create_watermelon_charts_for_hits(ai_candidates, top_n=5)

    if chart_paths:
        send_telegram_photo("📊 [수박지표 차트 TOP 5]", chart_paths)

[2] 개별 종목 차트만 보고 싶을 때:

    df_raw = fdr.DataReader('017860', start='2025-09-01')
    df_ind = get_indicators(df_raw)
    plot_watermelon_chart(df_ind, '017860', 'DS단석')
"""
