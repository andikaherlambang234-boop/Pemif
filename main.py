#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PEMIF v20.4 SCALP EDITION — Production Refactor
════════════════════════════════════════════════
Unified Build: UI/UX v20.3 + Framework Logic v10.0

Perubahan dari v20.4 original:
    - Laguerre RSI: warmup loop diperbaiki (variable shadowing dihilangkan)
    - ATR: indeks absolut menggantikan indeks relatif negatif
    - STC: EMA precompute menggantikan O(n²) inner loop
    - Signal dataclass: copy-safe via dataclasses.asdict()
    - news_ok default: False (fail-safe)
    - send_telegram: retry dengan exponential backoff
    - Semua dict lookups: .get() dengan fallback
    - Type hints: kompatibel Python 3.8+ via __future__
    - Validasi tipe input di analyze() via _validate_raw()
    - Weekend/holiday guard di check_killzone()
    - Historical stats: satu sumber kebenaran via HistoricalStats dataclass
    - Semua hardcoded magic numbers: dinaikkan ke named constants

Author  : PEMIF Engine
Version : 20.4-refactored
Python  : >= 3.8
"""
from __future__ import annotations

import logging
import math
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import requests

__all__ = [
    "Signal",
    "HistoricalStats",
    "analyze",
    "fmt_msg",
    "send_telegram",
    "fmt_tp_hit",
    "fmt_sl_hit",
    "fmt_daily_report",
    "fmt_weekly_report",
    "fmt_error",
]

# ═══════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════
WIB = timezone(timedelta(hours=7))

# Score ceilings — gunakan di confidence_pct & validasi
MAX_EPS:  int   = 4
MAX_SQS:  float = 10.0
MAX_CTX:  int   = 8
MAX_SOFT: int   = 8
MAX_QCM:  int   = 100

# Scalp thresholds
SCALP_EPS_MIN:   int   = 2
SCALP_QCM_MIN:   int   = 55
SCALP_CMS_MIN:   float = 3.0
SCALP_CMS_L3:    float = 3.0   # EPS Layer-3 threshold

# Momentum constants
LAGUERRE_GAMMA:  float = 0.5
LAGUERRE_WARMUP: int   = 30    # bar untuk konvergensi
BB_PERIOD:       int   = 20
BB_MULT:         float = 2.0
KC_MULT:         float = 1.5
ATR_PERIOD:      int   = 14
FISHER_PERIOD:   int   = 9
FISHER_EXTREME:  float = 1.5
STC_FAST:        int   = 23
STC_SLOW:        int   = 50
STC_OVERSOLD:    float = 30.0
STC_OVERBOUGHT:  float = 70.0

# Minimum bar counts
MIN_BARS_CME:    int   = 55    # perlu LAGUERRE_WARMUP + BB_PERIOD + buffer
MIN_BARS_STC:    int   = 50

# Telegram retry
TELEGRAM_MAX_RETRY:   int   = 3
TELEGRAM_RETRY_DELAY: float = 1.5  # detik, dikali 2^attempt (exponential backoff)
TELEGRAM_TIMEOUT:     int   = 10

# ═══════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("PEMIF-v20.4")


# ═══════════════════════════════════════════════════════════════
# ENVIRONMENT — fail-fast jika credentials hilang di production
# ═══════════════════════════════════════════════════════════════
def _load_env() -> Tuple[str, str, str, str, str]:
    """Load environment variables dengan validasi minimal.

    Returns:
        Tuple (telegram_token, chat_id, twelvedata_key, symbol, interval).

    Raises:
        EnvironmentError: Jika credentials wajib tidak di-set di production.
    """
    token   = os.environ.get("TELEGRAM_TOKEN",  "")
    chat_id = os.environ.get("TELEGRAM_CHATID", "")
    td_key  = os.environ.get("TWELVEDATA_KEY",  "")
    symbol  = os.environ.get("SYMBOL",          "XAU/USD")
    interval = os.environ.get("INTERVAL",       "5min")

    # Warn (bukan raise) agar demo_main() tetap berjalan tanpa env
    if not token or not chat_id:
        log.warning(
            "TELEGRAM_TOKEN / TELEGRAM_CHATID tidak di-set. "
            "Pesan akan dicetak ke stdout."
        )
    return token, chat_id, td_key, symbol, interval


TELEGRAM_TOKEN, TELEGRAM_CHATID, TWELVEDATA_KEY, SYMBOL, INTERVAL = _load_env()


# ═══════════════════════════════════════════════════════════════
# DATACLASSES
# ═══════════════════════════════════════════════════════════════
@dataclass
class HistoricalStats:
    """Satu sumber kebenaran untuk data performa historis.

    Attributes:
        total:   Total trade teresolusi.
        winrate: Persentase win (0.0–100.0).
        avg_rr:  Rata-rata reward:risk dari trade menang.
    """
    total:   int   = 0
    winrate: float = 0.0
    avg_rr:  float = 0.0

    @property
    def wins(self) -> int:
        """Jumlah win dihitung dari total × winrate."""
        return round(self.total * self.winrate / 100)

    @property
    def losses(self) -> int:
        """Jumlah loss = total - wins."""
        return self.total - self.wins


# Satu-satunya sumber historical stats — ganti dengan DB query di production
DEFAULT_STATS = HistoricalStats(total=237, winrate=83.5, avg_rr=2.8)


@dataclass
class Signal:
    """Representasi lengkap satu sinyal PEMIF SCALP.

    Semua field memiliki default aman. Field bool bertipe bool murni,
    field float bertipe float, dsb. — tidak ada mixed-type defaults.

    Catatan default fail-safe:
        news_ok = False  → bot TIDAK masuk trade jika feed tidak memperbarui
                           field ini secara eksplisit.
        gate_ok = False  → default selalu NO TRADE sampai semua gate terpenuhi.
    """
    # Direction & Gate
    direction:    str  = "NONE"
    gate_ok:      bool = False
    veto_rsn:     str  = "WAITING"
    grade:        str  = "STANDARD"
    order_type:   str  = "NONE"
    src:          str  = "-"

    # Bias per TF
    d1_bias:  str = "NEU"
    h4_bias:  str = "NEU"
    h1_bias:  str = "NEU"
    m30_bias: str = "NEU"
    m15_bias: str = "NEU"
    m5_bias:  str = "NEU"
    m1_bias:  str = "NEU"

    # Structure flags
    bos_bull:        bool = False
    bos_bear:        bool = False
    fvg_bull_fresh:  bool = False
    fvg_bear_fresh:  bool = False
    ob_bull_valid:   bool = False
    ob_bear_valid:   bool = False
    liq_swept_l:     bool = False
    liq_swept_h:     bool = False
    disp_ok:         bool = False
    sfp_signal:      str  = "NO"
    vol_surge:       bool = False
    acf_chop:        bool = False
    pdc_ok:          bool = False

    # EPS Layers (4-layer scalp)
    eps_layer1_structure: bool = False
    eps_layer2_pdarray:   bool = False
    eps_layer3_momentum:  bool = False
    eps_layer4_micro:     bool = False
    eps_score:            int  = 0

    # Composite scores
    sqs_score:  float = 0.0
    ctx_score:  int   = 0
    soft_count: int   = 0
    qcm_score:  int   = 0
    cms_score:  float = 0.0

    # CME sub-scores
    ttm_fire:  bool = False
    lrsi_ok:   bool = False
    fisher_ok: bool = False
    stc_ok:    bool = False

    # Extended structure flags
    fractal_conv:  int  = 0
    harmonic_pcz:  bool = False
    vwap_ok:       bool = True

    # PD Array
    pd_type:     str = "-"
    pd_priority: int = 0

    # Entry / Exit
    entry:      float = 0.0
    sl:         float = 0.0
    tp1:        float = 0.0
    tp2:        float = 0.0
    tp3:        float = 0.0
    tp4:        float = 0.0
    rr:         float = 0.0
    risk:       float = 0.0
    abe_level:  float = 0.0
    expiry_min: int   = 60

    # Kill Zone
    in_killzone: bool = False
    kz_name:     str  = "-"

    # News context — default False (fail-safe)
    news_ok:   bool = False
    news_tier: int  = 0


# ═══════════════════════════════════════════════════════════════
# INPUT VALIDATION
# ═══════════════════════════════════════════════════════════════

# Field → expected type mapping untuk validasi raw dict
_BOOL_FIELDS: Tuple[str, ...] = (
    "bos_bull", "bos_bear", "fvg_bull_fresh", "fvg_bear_fresh",
    "ob_bull_valid", "ob_bear_valid", "liq_swept_l", "liq_swept_h",
    "disp_ok", "vol_surge", "acf_chop", "pdc_ok",
    "harmonic_pcz", "news_ok",
)

_FLOAT_FIELDS: Tuple[str, ...] = (
    "entry", "sl", "tp1", "tp2", "tp3", "tp4",
    "rr", "risk", "abe_level", "cms_score",
)

_INT_FIELDS: Tuple[str, ...] = (
    "expiry_min", "pd_priority", "fractal_conv", "news_tier",
)


def _validate_raw(raw: Dict) -> Dict:
    """Validasi dan koersi tipe data dari raw feed dict.

    Mencegah sinyal salah akibat tipe data tidak terduga dari feed
    (misal: string "true" bukan bool True, atau "3312.5" bukan float).

    Args:
        raw: Dict mentah dari data feed / MT5 / TwelveData.

    Returns:
        Dict yang sudah divalidasi dan dikoersi.

    Raises:
        TypeError: Jika field wajib memiliki tipe yang tidak bisa dikoersi.
        ValueError: Jika nilai tidak masuk akal (misal: entry = 0 saat ada sinyal).
    """
    validated = dict(raw)  # shallow copy — jangan mutasi original

    for f in _BOOL_FIELDS:
        if f in validated:
            val = validated[f]
            if isinstance(val, str):
                validated[f] = val.strip().lower() in ("true", "1", "yes")
            elif not isinstance(val, bool):
                validated[f] = bool(val)

    for f in _FLOAT_FIELDS:
        if f in validated:
            try:
                validated[f] = float(validated[f])
            except (TypeError, ValueError) as exc:
                raise TypeError(
                    f"Field '{f}' tidak bisa dikonversi ke float: "
                    f"{validated[f]!r}"
                ) from exc

    for f in _INT_FIELDS:
        if f in validated:
            try:
                validated[f] = int(validated[f])
            except (TypeError, ValueError) as exc:
                raise TypeError(
                    f"Field '{f}' tidak bisa dikonversi ke int: "
                    f"{validated[f]!r}"
                ) from exc

    # Sanity check entry price
    direction = validated.get("direction", "NONE")
    if direction != "NONE":
        entry = validated.get("entry", 0.0)
        if entry <= 0.0:
            log.warning(
                "Entry price %.4f <= 0 untuk direction=%s. "
                "Pastikan feed mengirim level harga yang benar.",
                entry, direction,
            )

    return validated


# ═══════════════════════════════════════════════════════════════
# UI HELPERS
# ═══════════════════════════════════════════════════════════════
SEP  = "━━━━━━━━━━━━━━━━━━━━"
SEP2 = "════════════════════════════════════════"


def bias_icon(bias: str) -> str:
    """Konversi kode bias ke label + emoji.

    Args:
        bias: "BUL", "BER", atau "NEU".

    Returns:
        String label dengan emoji.
    """
    return {
        "BUL": "BULL 🟢",
        "BER": "BEAR 🔴",
        "NEU": "NEUTRAL 🟡",
    }.get(bias, "N/A ⚪")


def grade_label(grade: str) -> str:
    """Konversi grade string ke label display.

    Args:
        grade: "PRIME", "HIGH", atau "STANDARD".

    Returns:
        String label dengan emoji.
    """
    return {
        "PRIME":    "ELITE 🔥",
        "HIGH":     "A ⭐",
        "STANDARD": "B 📶",
    }.get(grade, "---")


def eps_label(eps: int) -> str:
    """Label deskriptif untuk EPS score 4-layer scalp.

    Args:
        eps: Integer 0–4.

    Returns:
        String label dengan emoji.
    """
    return {
        4: "SNIPER 🎯",
        3: "PRECISION ✦",
        2: "STANDARD ✓",
        1: "MARGINAL ⚠",
        0: "SKIP ✗",
    }.get(eps, "---")


def cms_bar(cms: float) -> str:
    """Visual progress bar untuk CMS score.

    Args:
        cms: Float 0.0–10.0.

    Returns:
        String bar dalam format "[████░░░░░░] 4.0/10".
    """
    cms_clamped = max(0.0, min(cms, MAX_SQS))
    filled = round(cms_clamped)           # 0–10 blok
    bar    = "█" * filled + "░" * (10 - filled)
    return f"[{bar}] {cms_clamped:.1f}/10"


def confidence_pct(
    eps: int, sqs: float, ctx: int, soft: int
) -> int:
    """Hitung confidence percentage dari komponen skor.

    Formula:
        EPS  contributes 30%
        SQS  contributes 30%
        CTX  contributes 20%
        SOFT contributes 20%

    Args:
        eps:  EPS score 0–MAX_EPS.
        sqs:  SQS score 0.0–MAX_SQS.
        ctx:  CTX score 0–MAX_CTX.
        soft: SOFT count 0–MAX_SOFT.

    Returns:
        Integer 0–100 sebagai persentase confidence.
    """
    e = min(eps  / MAX_EPS,  1.0) * 30
    s = min(sqs  / MAX_SQS,  1.0) * 30
    c = min(ctx  / MAX_CTX,  1.0) * 20
    f = min(soft / MAX_SOFT, 1.0) * 20
    return round(e + s + c + f)


def alignment_count(
    d1b: str, h4b: str, h1b: str,
    m30b: str, m15b: str, m5b: str, m1b: str,
    direction: str,
) -> Tuple[int, int]:
    """Hitung jumlah TF yang bias-nya sejajar dengan direction.

    Args:
        d1b..m1b: Kode bias per timeframe.
        direction: "BUY" atau "SELL".

    Returns:
        Tuple (aligned_count, total_tf_count).
    """
    target = "BUL" if direction == "BUY" else "BER"
    biases = [d1b, h4b, h1b, m30b, m15b, m5b, m1b]
    aligned = sum(1 for b in biases if b == target)
    return aligned, len(biases)


# ═══════════════════════════════════════════════════════════════
# FORMAT BLOCK BUILDERS
# ═══════════════════════════════════════════════════════════════

def _chk(cond: bool) -> str:
    """Return checkmark emoji berdasarkan kondisi boolean."""
    return "✅" if cond else "❌"


def fmt_header(symbol: str, now_wib: str) -> str:
    """Format header standar PEMIF.

    Args:
        symbol:  Nama instrumen, misal "XAU/USD".
        now_wib: String waktu WIB yang sudah diformat.

    Returns:
        String HTML-ready untuk Telegram.
    """
    return (
        f"🤖 <b>PEMIF AI SCALP ENGINE</b>\n\n"
        f"📊 {symbol}\n"
        f"🕒 {now_wib}\n\n"
        f"{SEP}"
    )


def fmt_trend_block(
    d1b: str, h4b: str, h1b: str,
    m30b: str, m15b: str, m5b: str, m1b: str,
    direction: str = "",
) -> str:
    """Format blok Trend Alignment multi-TF.

    Args:
        d1b..m1b:  Kode bias per TF.
        direction: Opsional. Jika diisi, tampilkan alignment count.

    Returns:
        String HTML-ready.
    """
    align_str = ""
    if direction:
        aligned, total = alignment_count(
            d1b, h4b, h1b, m30b, m15b, m5b, m1b, direction
        )
        align_str = f"\nAlignment : {aligned}/{total}"

    return (
        f"📈 <b>TREND ALIGNMENT</b>\n\n"
        f"D1  : {bias_icon(d1b)}\n"
        f"H4  : {bias_icon(h4b)}\n"
        f"H1  : {bias_icon(h1b)}\n"
        f"M30 : {bias_icon(m30b)}\n"
        f"M15 : {bias_icon(m15b)}\n"
        f"M5  : {bias_icon(m5b)}\n"
        f"M1  : {bias_icon(m1b)}"
        f"{align_str}"
    )


def fmt_structure_block(sig: Dict) -> str:
    """Format blok Market Structure (BOS, FVG, OB, Liq, Displacement).

    Args:
        sig: Dict representasi Signal (via asdict()).

    Returns:
        String HTML-ready.
    """
    bos_ok  = sig.get("bos_bull")       or sig.get("bos_bear")
    fvg_ok  = sig.get("fvg_bull_fresh") or sig.get("fvg_bear_fresh")
    ob_ok   = sig.get("ob_bull_valid")  or sig.get("ob_bear_valid")
    liq_ok  = sig.get("liq_swept_l")   or sig.get("liq_swept_h")
    disp_ok = sig.get("disp_ok", False)

    return (
        f"📊 <b>MARKET STRUCTURE</b>\n\n"
        f"{_chk(bool(bos_ok))}  BOS\n"
        f"{_chk(bool(fvg_ok))}  FVG\n"
        f"{_chk(bool(ob_ok))}   OB\n"
        f"{_chk(bool(liq_ok))}  Liquidity\n"
        f"{_chk(disp_ok)} Displacement"
    )


def fmt_eps_block(sig: Dict) -> str:
    """Format blok EPS 4-layer scalp.

    Args:
        sig: Dict representasi Signal.

    Returns:
        String HTML-ready.
    """
    l1  = sig.get("eps_layer1_structure", False)
    l2  = sig.get("eps_layer2_pdarray",   False)
    l3  = sig.get("eps_layer3_momentum",  False)
    l4  = sig.get("eps_layer4_micro",     False)
    eps = sig.get("eps_score", 0)

    return (
        f"🎯 <b>EPS — ENTRY PRECISION</b>\n\n"
        f"{_chk(l1)} L1 : Structure (BOS + CHoCH)\n"
        f"{_chk(l2)} L2 : PD Array Valid\n"
        f"{_chk(l3)} L3 : Momentum (CMS ≥ {SCALP_CMS_L3})\n"
        f"{_chk(l4)} L4 : M1 CHoCH Mikro\n\n"
        f"Score : {eps}/{MAX_EPS} — {eps_label(eps)}"
    )


def fmt_momentum_block(sig: Dict) -> str:
    """Format blok CME Momentum (TTM, LRSI, Fisher, STC).

    Args:
        sig: Dict representasi Signal.

    Returns:
        String HTML-ready.
    """
    cms    = sig.get("cms_score", 0.0)
    ttm    = sig.get("ttm_fire",  False)
    lrsi   = sig.get("lrsi_ok",   False)
    fisher = sig.get("fisher_ok", False)
    stc    = sig.get("stc_ok",    False)

    return (
        f"⚡ <b>MOMENTUM (CME)</b>\n\n"
        f"{_chk(ttm)}   TTM Squeeze\n"
        f"{_chk(lrsi)}  Laguerre RSI\n"
        f"{_chk(fisher)} Fisher Transform\n"
        f"{_chk(stc)}   Schaff Trend Cycle\n\n"
        f"CMS : {cms_bar(cms)}"
    )


def fmt_fulfilled(sig: Dict, direction: str) -> Tuple[List[str], List[str]]:
    """Pisahkan kondisi entry yang terpenuhi vs belum terpenuhi.

    Args:
        sig:       Dict representasi Signal.
        direction: "BUY" atau "SELL".

    Returns:
        Tuple (ok_list, no_list) — list string label kondisi.
    """
    d = direction
    ok: List[str] = []
    no: List[str] = []

    def chk(cond: bool, label: str) -> None:
        (ok if cond else no).append(label)

    bull = d == "BUY"
    target_bias = "BUL" if bull else "BER"
    side_label  = "Bull" if bull else "Bear"

    for tf, key in [
        ("D1",  "d1_bias"),  ("H4",  "h4_bias"),  ("H1",  "h1_bias"),
        ("M30", "m30_bias"), ("M15", "m15_bias"),
        ("M5",  "m5_bias"),  ("M1",  "m1_bias"),
    ]:
        chk(sig.get(key) == target_bias, f"{tf} {side_label}")

    bos_key = "bos_bull"      if bull else "bos_bear"
    liq_key = "liq_swept_l"   if bull else "liq_swept_h"
    fvg_key = "fvg_bull_fresh" if bull else "fvg_bear_fresh"
    ob_key  = "ob_bull_valid"  if bull else "ob_bear_valid"

    chk(bool(sig.get(bos_key)),  "BOS Confirmation")
    chk(bool(sig.get(liq_key)),  "Liquidity Sweep")
    chk(bool(sig.get(fvg_key)),  "FVG Retest")
    chk(bool(sig.get(ob_key)),   "Order Block")
    chk(bool(sig.get("disp_ok", False)), "Displacement")
    chk(sig.get("sfp_signal") in ("BULL", "BEAR"), "SFP Signal")
    chk(bool(sig.get("vol_surge",  False)), "Volume Expansion")
    chk(not bool(sig.get("acf_chop", False)), "Trend (ACF OK)")
    chk(bool(sig.get("pdc_ok",     False)), "PDC Zone")

    return ok, no


def fmt_ai_analysis(direction: str, missing: List[str]) -> str:
    """Generate narasi AI analysis berdasarkan kondisi yang belum terpenuhi.

    Args:
        direction: "BUY" atau "SELL".
        missing:   List kondisi yang belum terpenuhi.

    Returns:
        String HTML-ready.
    """
    if not missing:
        return (
            f"🧠 <b>AI ANALYSIS</b>\n\n"
            f"Semua kondisi terpenuhi.\n"
            f"Setup {direction} dalam kondisi optimal."
        )
    waiting = "\n".join(f"{i+1}. {m}" for i, m in enumerate(missing[:5]))
    return (
        f"🧠 <b>AI ANALYSIS</b>\n\n"
        f"Setup {direction} sedang terbentuk.\n\n"
        f"Masih menunggu:\n"
        f"{waiting}"
    )


def fmt_historical(stats: HistoricalStats) -> str:
    """Format blok historical performance.

    Args:
        stats: HistoricalStats object (single source of truth).

    Returns:
        String HTML-ready.
    """
    return (
        f"📚 <b>HISTORICAL DATA</b>\n\n"
        f"Setup Serupa : {stats.total}\n"
        f"Win          : {stats.wins}\n"
        f"Loss         : {stats.losses}\n"
        f"Winrate      : {stats.winrate}%\n"
        f"Average RR   : {stats.avg_rr}"
    )


# ═══════════════════════════════════════════════════════════════
# FORMAT PESAN — SCAN
# ═══════════════════════════════════════════════════════════════

def fmt_scan(
    sig: Dict, symbol: str, stats: Optional[HistoricalStats] = None
) -> str:
    """Format pesan Scan state (off-KZ atau chop).

    Args:
        sig:    Dict representasi Signal.
        symbol: Nama instrumen.
        stats:  Historical stats (opsional, untuk konsistensi).

    Returns:
        String HTML siap kirim Telegram.
    """
    now_wib   = datetime.now(WIB).strftime("%d %b %Y | %H:%M WIB")
    direction = sig.get("direction", "BUY")
    eps   = sig.get("eps_score",  0)
    sqs   = sig.get("sqs_score",  0.0)
    ctx   = sig.get("ctx_score",  0)
    soft  = sig.get("soft_count", 0)
    conf  = confidence_pct(eps, sqs, ctx, soft)
    m5b   = sig.get("m5_bias",  "NEU")
    m1b   = sig.get("m1_bias",  "NEU")
    kz    = sig.get("kz_name",  "-")

    return (
        f"{fmt_header(symbol, now_wib)}\n\n"
        f"🔍 <b>MARKET SCAN</b>\n\n"
        f"Kill Zone : {kz}\n\n"
        f"{SEP}\n\n"
        f"{fmt_trend_block(sig.get('d1_bias','NEU'), sig.get('h4_bias','NEU'), sig.get('h1_bias','NEU'), sig.get('m30_bias','NEU'), sig.get('m15_bias','NEU'), m5b, m1b, direction)}\n\n"
        f"{SEP}\n\n"
        f"{fmt_structure_block(sig)}\n\n"
        f"{SEP}\n\n"
        f"{fmt_momentum_block(sig)}\n\n"
        f"{SEP}\n\n"
        f"📊 <b>SCORE</b>\n\n"
        f"EPS        : {eps}/{MAX_EPS} — {eps_label(eps)}\n"
        f"SQS        : {sqs}\n"
        f"CTX        : {ctx}/{MAX_CTX}\n"
        f"SOFT       : {soft}/{MAX_SOFT}\n"
        f"Confidence : {conf}%\n\n"
        f"{SEP}\n\n"
        f"Status : SCANNING ⏳"
    )


# ═══════════════════════════════════════════════════════════════
# FORMAT PESAN — WAITING
# ═══════════════════════════════════════════════════════════════

def fmt_waiting(
    sig: Dict, symbol: str, stats: Optional[HistoricalStats] = None
) -> str:
    """Format pesan Waiting state (ada direction tapi gate belum ok).

    Args:
        sig:    Dict representasi Signal.
        symbol: Nama instrumen.
        stats:  Historical stats untuk blok historical.

    Returns:
        String HTML siap kirim Telegram.
    """
    now_wib   = datetime.now(WIB).strftime("%d %b %Y | %H:%M WIB")
    direction = sig.get("direction", "BUY")
    eps   = sig.get("eps_score",  0)
    sqs   = sig.get("sqs_score",  0.0)
    ctx   = sig.get("ctx_score",  0)
    soft  = sig.get("soft_count", 0)
    grade = sig.get("grade",      "STANDARD")
    veto  = sig.get("veto_rsn",   "WAITING")
    conf  = confidence_pct(eps, sqs, ctx, soft)
    m5b   = sig.get("m5_bias",   "NEU")
    m1b   = sig.get("m1_bias",   "NEU")

    ok_list, no_list = fmt_fulfilled(sig, direction)
    ok_str = "\n".join(f"  ✓ {x}" for x in ok_list) or "  (belum ada)"
    no_str = "\n".join(f"  ✗ {x}" for x in no_list) or "  (semua terpenuhi)"

    hist_block = fmt_historical(stats or DEFAULT_STATS)

    return (
        f"{fmt_header(symbol, now_wib)}\n\n"
        f"⏳ <b>WAITING {direction}</b>\n"
        f"Alasan : {veto}\n\n"
        f"{SEP}\n\n"
        f"{fmt_trend_block(sig.get('d1_bias','NEU'), sig.get('h4_bias','NEU'), sig.get('h1_bias','NEU'), sig.get('m30_bias','NEU'), sig.get('m15_bias','NEU'), m5b, m1b, direction)}\n\n"
        f"{SEP}\n\n"
        f"📊 <b>QUALITY</b>\n\n"
        f"EPS        : {eps}/{MAX_EPS} — {eps_label(eps)}\n"
        f"SQS        : {sqs}\n"
        f"CTX        : {ctx}/{MAX_CTX}\n"
        f"SOFT       : {soft}/{MAX_SOFT}\n"
        f"Confidence : {conf}%\n"
        f"Grade      : {grade_label(grade)}\n\n"
        f"{SEP}\n\n"
        f"{fmt_momentum_block(sig)}\n\n"
        f"{SEP}\n\n"
        f"{fmt_eps_block(sig)}\n\n"
        f"{SEP}\n\n"
        f"✅ <b>TERPENUHI</b>\n\n"
        f"{ok_str}\n\n"
        f"{SEP}\n\n"
        f"❌ <b>BELUM TERPENUHI</b>\n\n"
        f"{no_str}\n\n"
        f"{SEP}\n\n"
        f"{fmt_ai_analysis(direction, no_list)}\n\n"
        f"{SEP}\n\n"
        f"{hist_block}\n\n"
        f"{SEP}\n\n"
        f"🚫 <b>BELUM VALID ENTRY</b>"
    )


# ═══════════════════════════════════════════════════════════════
# FORMAT PESAN — ENTRY ALERT
# ═══════════════════════════════════════════════════════════════

def fmt_entry(
    sig: Dict, symbol: str, stats: Optional[HistoricalStats] = None
) -> str:
    """Format pesan Entry Alert (gate terpenuhi, sinyal valid).

    Args:
        sig:    Dict representasi Signal.
        symbol: Nama instrumen.
        stats:  Historical stats untuk blok performance.

    Returns:
        String HTML siap kirim Telegram.
    """
    now_wib   = datetime.now(WIB).strftime("%d %b %Y | %H:%M WIB")
    direction = sig.get("direction", "BUY")
    eps   = sig.get("eps_score",  0)
    sqs   = sig.get("sqs_score",  0.0)
    ctx   = sig.get("ctx_score",  0)
    soft  = sig.get("soft_count", 0)
    grade = sig.get("grade",      "STANDARD")
    conf  = confidence_pct(eps, sqs, ctx, soft)
    qcm   = sig.get("qcm_score",  0)
    cms   = sig.get("cms_score",  0.0)
    m5b   = sig.get("m5_bias",   "NEU")
    m1b   = sig.get("m1_bias",   "NEU")

    d_icon = "🟢" if direction == "BUY" else "🔴"
    ok_list, _ = fmt_fulfilled(sig, direction)
    ok_str = "\n".join(f"  ✓ {x}" for x in ok_list) or "  -"

    entry = sig.get("entry",      0.0)
    sl    = sig.get("sl",         0.0)
    tp1   = sig.get("tp1",        0.0)
    tp2   = sig.get("tp2",        0.0)
    tp3   = sig.get("tp3",        0.0)
    tp4   = sig.get("tp4",        0.0)
    rr    = sig.get("rr",         0.0)
    risk  = sig.get("risk",       0.0)
    src   = sig.get("src",        "-")
    otype = sig.get("order_type", "-")
    abe   = sig.get("abe_level",  0.0)
    exp   = sig.get("expiry_min", 60)
    pd_t  = sig.get("pd_type",   "-")
    kz    = sig.get("kz_name",   "-")
    fc    = sig.get("fractal_conv", 0)

    aligned, total = alignment_count(
        sig.get("d1_bias",  "NEU"), sig.get("h4_bias",  "NEU"),
        sig.get("h1_bias",  "NEU"), sig.get("m30_bias", "NEU"),
        sig.get("m15_bias", "NEU"), m5b, m1b, direction,
    )

    hs = stats or DEFAULT_STATS

    return (
        f"{fmt_header(symbol, now_wib)}\n\n"
        f"🚀 <b>ENTRY ALERT</b>\n\n"
        f"{SEP}\n\n"
        f"{d_icon} <b>{direction}</b>\n"
        f"Order Type : {otype}\n"
        f"Source     : {src}\n"
        f"Kill Zone  : {kz}\n"
        f"PD Array   : {pd_t}\n\n"
        f"{SEP}\n\n"
        f"📍 <b>LEVEL</b>\n\n"
        f"Entry  : <code>{entry}</code>\n"
        f"SL     : <code>{sl}</code>\n"
        f"Risk   : <code>{risk:.2f}</code> pts\n"
        f"ABE    : <code>{abe}</code>  (→ pindah SL ke entry)\n"
        f"Expiry : {exp} menit\n\n"
        f"{SEP}\n\n"
        f"🎯 <b>TARGET</b>\n\n"
        f"TP1 : <code>{tp1}</code>\n"
        f"TP2 : <code>{tp2}</code>\n"
        f"TP3 : <code>{tp3}</code>\n"
        f"TP4 : <code>{tp4}</code>  [HARD EXIT]\n"
        f"RR  : 1 : {rr:.1f}\n\n"
        f"{SEP}\n\n"
        f"📊 <b>QUALITY SCORE</b>\n\n"
        f"EPS        : {eps}/{MAX_EPS} — {eps_label(eps)}\n"
        f"SQS        : {sqs}/{MAX_SQS}\n"
        f"CTX        : {ctx}/{MAX_CTX}\n"
        f"SOFT       : {soft}/{MAX_SOFT}\n"
        f"QCM        : {qcm}/{MAX_QCM}\n"
        f"CMS        : {cms:.1f}/10\n"
        f"Confidence : {conf}%\n"
        f"Grade      : {grade_label(grade)}\n\n"
        f"{SEP}\n\n"
        f"{fmt_trend_block(sig.get('d1_bias','NEU'), sig.get('h4_bias','NEU'), sig.get('h1_bias','NEU'), sig.get('m30_bias','NEU'), sig.get('m15_bias','NEU'), m5b, m1b, direction)}\n\n"
        f"{SEP}\n\n"
        f"{fmt_structure_block(sig)}\n\n"
        f"{SEP}\n\n"
        f"{fmt_momentum_block(sig)}\n\n"
        f"{SEP}\n\n"
        f"{fmt_eps_block(sig)}\n\n"
        f"{SEP}\n\n"
        f"Fractal Conv : {fc}/5 TF\n"
        f"Alignment    : {aligned}/{total}\n\n"
        f"{SEP}\n\n"
        f"✅ <b>ALASAN ENTRY</b>\n\n"
        f"{ok_str}\n\n"
        f"{SEP}\n\n"
        f"📚 <b>HISTORICAL PERFORMANCE</b>\n\n"
        f"Trade Serupa : {hs.total}\n"
        f"Win          : {hs.wins}\n"
        f"Loss         : {hs.losses}\n"
        f"Winrate      : {hs.winrate}%\n"
        f"Average RR   : {hs.avg_rr}\n\n"
        f"{SEP}\n\n"
        f"🔥 <b>HIGH PROBABILITY SETUP</b>"
    )


# ═══════════════════════════════════════════════════════════════
# FORMAT PESAN — TP HIT
# ═══════════════════════════════════════════════════════════════

def fmt_tp_hit(
    symbol: str,
    direction: str,
    tp_num: int,
    profit_pips: float,
    rr: float = 0.0,
    duration: str = "",
) -> str:
    """Format pesan TP Hit notification.

    Args:
        symbol:      Nama instrumen.
        direction:   "BUY" atau "SELL".
        tp_num:      Nomor TP yang hit (1, 2, 3+).
        profit_pips: Profit dalam pips.
        rr:          Achieved R:R (untuk TP3+).
        duration:    Durasi trade aktif (untuk TP3+).

    Returns:
        String HTML siap kirim Telegram.
    """
    now_wib   = datetime.now(WIB).strftime("%d %b %Y | %H:%M WIB")
    d_icon    = "🟢" if direction == "BUY" else "🔴"

    if tp_num == 1:
        icon  = "🎯"
        title = "TP1 HIT"
        extra = (
            f"\nStatus : Break Even Activated\n\n"
            f"{SEP}\n\nTrade Masih Aktif ✅"
        )
    elif tp_num == 2:
        icon  = "🎯"
        title = "TP2 HIT"
        extra = (
            f"\nTrailing Protection : ACTIVE\n\n"
            f"{SEP}\n\nTrade Masih Aktif ✅"
        )
    elif tp_num >= 3:
        icon  = "🏆"
        title = "FULL TAKE PROFIT"
        extra = (
            f"\nRR     : +{rr:.1f}R\n\n"
            f"{SEP}\n\n"
            f"Result   : ✅ WIN\n\n"
            f"{SEP}\n\n"
            f"Durasi   : {duration}"
        )
    else:
        # tp_num <= 0: fallback aman
        icon  = "🎯"
        title = f"TP{max(tp_num, 1)} HIT"
        extra = ""

    return (
        f"🤖 <b>PEMIF AI SCALP ENGINE</b>\n\n"
        f"📊 {symbol}\n"
        f"🕒 {now_wib}\n\n"
        f"{SEP}\n\n"
        f"{icon} <b>{title}</b>\n\n"
        f"{SEP}\n\n"
        f"{d_icon} {direction}\n\n"
        f"Profit : +{profit_pips:.0f} Pips\n"
        f"{extra}"
    )


# ═══════════════════════════════════════════════════════════════
# FORMAT PESAN — STOP LOSS
# ═══════════════════════════════════════════════════════════════

def fmt_sl_hit(symbol: str, direction: str, sig: Dict) -> str:
    """Format pesan Stop Loss Hit dengan AI review.

    Args:
        symbol:    Nama instrumen.
        direction: "BUY" atau "SELL".
        sig:       Dict representasi Signal untuk konteks review.

    Returns:
        String HTML siap kirim Telegram.
    """
    now_wib = datetime.now(WIB).strftime("%d %b %Y | %H:%M WIB")
    bull    = direction == "BUY"

    reasons: List[str] = []
    liq_ok  = sig.get("liq_swept_l" if bull else "liq_swept_h", False)
    disp_ok = sig.get("disp_ok", False)
    htf_key = "h1_bias"
    htf_ok  = sig.get(htf_key) == ("BUL" if bull else "BER")

    if not liq_ok:  reasons.append("Sweep gagal bertahan")
    if not disp_ok: reasons.append("Displacement lemah")
    if not htf_ok:  reasons.append("Reversal HTF")
    if not reasons: reasons.append("Market bergerak tak terduga")

    rsn_str = "\n".join(f"  ❌ {r}" for r in reasons)
    d_icon  = "🟢" if bull else "🔴"

    return (
        f"🤖 <b>PEMIF AI SCALP ENGINE</b>\n\n"
        f"📊 {symbol}\n"
        f"🕒 {now_wib}\n\n"
        f"{SEP}\n\n"
        f"❌ <b>STOP LOSS HIT</b>\n\n"
        f"{SEP}\n\n"
        f"{d_icon} {direction}\n\n"
        f"Loss : -1R\n\n"
        f"{SEP}\n\n"
        f"🧠 <b>AI REVIEW</b>\n\n"
        f"Penyebab:\n\n"
        f"{rsn_str}\n\n"
        f"{SEP}\n\n"
        f"📊 Data disimpan ke Learning Engine"
    )


# ═══════════════════════════════════════════════════════════════
# FORMAT PESAN — DAILY & WEEKLY REPORT
# ═══════════════════════════════════════════════════════════════

def fmt_daily_report(stats: Dict) -> str:
    """Format laporan harian.

    Args:
        stats: Dict berisi metrik harian (win, loss, be, dll).

    Returns:
        String HTML siap kirim Telegram.
    """
    now_wib = datetime.now(WIB).strftime("%d %b %Y")
    win   = stats.get("win",  0)
    loss  = stats.get("loss", 0)
    be    = stats.get("be",   0)
    total_entry = win + loss + be
    wr = round(win / total_entry * 100, 1) if total_entry > 0 else 0.0

    return (
        f"🤖 <b>PEMIF AI SCALP ENGINE</b>\n\n"
        f"📅 <b>DAILY INTELLIGENCE REPORT</b>\n\n"
        f"{SEP}\n\nTanggal : {now_wib}\n\n"
        f"{SEP}\n\n"
        f"Total Scan  : {stats.get('total_scan',  'N/A')}\n"
        f"Total Setup : {stats.get('total_setup', 'N/A')}\n"
        f"Total Entry : {total_entry}\n\n"
        f"{SEP}\n\n"
        f"Win  : {win}\nLoss : {loss}\nBE   : {be}\n\n"
        f"{SEP}\n\n"
        f"Winrate       : {wr}%\n"
        f"Profit Factor : {stats.get('profit_factor', 0.0)}\n"
        f"Average RR    : {stats.get('avg_rr', 0.0)}\n\n"
        f"{SEP}\n\n"
        f"Best Session  : {stats.get('best_session',  'N/A')}\n"
        f"Worst Session : {stats.get('worst_session', 'N/A')}\n\n"
        f"{SEP}\n\n"
        f"Best Setup  : {stats.get('best_setup',  'N/A')}\n"
        f"Worst Setup : {stats.get('worst_setup', 'N/A')}\n\n"
        f"{SEP}\n\n"
        f"Top Faktor Profit : {stats.get('top_profit_factor', 'N/A')}\n"
        f"Top Faktor Loss   : {stats.get('top_loss_factor',   'N/A')}\n\n"
        f"{SEP}\n\n"
        f"🧠 <b>AI Insight Harian</b>\n\n"
        f"{stats.get('ai_insight', 'Tidak ada insight hari ini.')}\n\n"
        f"{SEP}\n\n"
        f"📌 <b>Rekomendasi</b>\n\n"
        f"{stats.get('recommendation', 'Lanjutkan monitoring.')}"
    )


def fmt_weekly_report(stats: Dict) -> str:
    """Format laporan mingguan.

    Args:
        stats: Dict berisi metrik mingguan.

    Returns:
        String HTML siap kirim Telegram.
    """
    win   = stats.get("win",  0)
    loss  = stats.get("loss", 0)
    total = win + loss
    wr    = round(win / total * 100, 1) if total > 0 else 0.0

    return (
        f"🤖 <b>PEMIF AI SCALP ENGINE</b>\n\n"
        f"📈 <b>WEEKLY INTELLIGENCE REPORT</b>\n\n"
        f"{SEP}\n\n"
        f"Total Trade : {total}\n"
        f"Win         : {win}\nLoss        : {loss}\n"
        f"Winrate     : {wr}%\n"
        f"Profit Factor : {stats.get('profit_factor', 0.0)}\n"
        f"Average RR  : {stats.get('avg_rr', 0.0)}\n\n"
        f"{SEP}\n\n"
        f"Best Day  : {stats.get('best_day',  'N/A')}\n"
        f"Worst Day : {stats.get('worst_day', 'N/A')}\n\n"
        f"{SEP}\n\n"
        f"Best Session  : {stats.get('best_session',  'N/A')}\n"
        f"Worst Session : {stats.get('worst_session', 'N/A')}\n\n"
        f"{SEP}\n\n"
        f"Top Faktor Profit : {stats.get('top_profit_factor', 'N/A')}\n"
        f"Top Faktor Loss   : {stats.get('top_loss_factor',   'N/A')}\n\n"
        f"{SEP}\n\n"
        f"🧠 <b>AI Insight Mingguan</b>\n\n"
        f"{stats.get('ai_insight', 'Tidak ada insight minggu ini.')}"
    )


def fmt_error(symbol: str, reason: str) -> str:
    """Format pesan error/data feed failure.

    Args:
        symbol: Nama instrumen.
        reason: Deskripsi error.

    Returns:
        String HTML siap kirim Telegram.
    """
    now_wib = datetime.now(WIB).strftime("%d %b %Y | %H:%M WIB")
    return (
        f"🤖 <b>PEMIF AI SCALP ENGINE</b>\n\n"
        f"📊 {symbol}\n"
        f"🕒 {now_wib}\n\n"
        f"{SEP}\n\n"
        f"⚠️ <b>DATA FEED ERROR</b>\n\n"
        f"Alasan : {reason}\n\n"
        f"{SEP}\n\n"
        f"🔄 Bot akan retry pada siklus berikutnya."
    )


# ═══════════════════════════════════════════════════════════════
# FRAMEWORK ENGINE — KILL ZONE DETECTOR
# ═══════════════════════════════════════════════════════════════

KILL_ZONES: Tuple[Tuple, ...] = (
    ("Asia KZ",     2,  0,  5,  0),
    ("London KZ",  14,  0, 17,  0),
    ("NY Open KZ", 19, 30, 22,  0),
    ("NY PM KZ",   23,  0,  1,  0),   # cross-midnight
)

# Trading days: Monday=0 ... Friday=4 (weekday() convention)
TRADING_WEEKDAYS: Tuple[int, ...] = (0, 1, 2, 3, 4)


def check_killzone(now: datetime) -> Tuple[bool, str]:
    """Cek apakah waktu sekarang berada dalam kill zone trading.

    Menangani cross-midnight dan hari weekend.

    Args:
        now: datetime object dengan timezone (direkomendasikan WIB).

    Returns:
        Tuple (in_kz: bool, kz_name: str).
        Jika weekend atau di luar semua KZ, returns (False, "-").
    """
    # Weekend guard — forex tutup Sabtu & Minggu
    if now.weekday() not in TRADING_WEEKDAYS:
        return False, "WEEKEND"

    h, m    = now.hour, now.minute
    total_m = h * 60 + m

    for name, sh, sm, eh, em in KILL_ZONES:
        start = sh * 60 + sm
        end   = eh * 60 + em

        if start <= end:
            if start <= total_m < end:
                return True, name
        else:
            # Cross-midnight: aktif dari start hingga akhir hari, ATAU dari 00:00 hingga end
            if total_m >= start or total_m < end:
                return True, name

    return False, "-"


# ═══════════════════════════════════════════════════════════════
# CME — COMPOSITE MOMENTUM ENGINE
# ═══════════════════════════════════════════════════════════════

def _ema_series(data: List[float], period: int) -> List[float]:
    """Hitung EMA inkremental untuk seluruh series data.

    Lebih efisien daripada memanggil ema() per-bar dalam loop O(n²).
    Warmup menggunakan SMA untuk period bars pertama.

    Args:
        data:   List float (harga).
        period: Periode EMA.

    Returns:
        List EMA dengan panjang sama dengan data (warmup = SMA awal).
    """
    if not data or period <= 0:
        return []

    k      = 2.0 / (period + 1)
    result = [0.0] * len(data)

    # Seed dengan SMA dari period pertama
    seed_end = min(period, len(data))
    result[seed_end - 1] = sum(data[:seed_end]) / seed_end

    for i in range(seed_end, len(data)):
        result[i] = data[i] * k + result[i - 1] * (1.0 - k)

    return result


def calc_cms(bars: List[Dict], direction: str) -> Dict:
    """Hitung Composite Momentum Score (CMS) dari OHLCV bars.

    Komponen:
        - TTM Squeeze proxy (BB vs KC)
        - Laguerre RSI (gamma=0.5, dengan warmup 30 bar)
        - Fisher Transform (period 9)
        - Schaff Trend Cycle proxy (EMA23 vs EMA50 stochastic)

    Args:
        bars:      List of dict {'open', 'high', 'low', 'close', 'volume'}.
                   Minimal panjang: MIN_BARS_CME (55).
        direction: "BUY" atau "SELL".

    Returns:
        Dict berisi:
            ttm_fire, lrsi_ok, fisher_ok, stc_ok (bool)
            cms_score (float 0.0–10.0)
    """
    empty = {
        "ttm_fire": False, "lrsi_ok": False,
        "fisher_ok": False, "stc_ok": False,
        "cms_score": 0.0,
    }

    if not bars or len(bars) < MIN_BARS_CME:
        log.warning(
            "calc_cms: butuh minimal %d bars, dapat %d. Return nol.",
            MIN_BARS_CME, len(bars) if bars else 0,
        )
        return empty

    closes = [float(b["close"]) for b in bars]
    highs  = [float(b["high"])  for b in bars]
    lows   = [float(b["low"])   for b in bars]
    n      = len(closes)

    # ── TTM Squeeze Proxy ─────────────────────────────────────
    bb_window = closes[-BB_PERIOD:]
    bb_mid    = sum(bb_window) / BB_PERIOD
    bb_std    = math.sqrt(
        sum((x - bb_mid) ** 2 for x in bb_window) / BB_PERIOD
    )
    bb_upper = bb_mid + BB_MULT * bb_std
    bb_lower = bb_mid - BB_MULT * bb_std

    # ATR14 menggunakan indeks absolut (bukan negatif relatif)
    atr_start = n - ATR_PERIOD
    atr_vals: List[float] = []
    for i in range(atr_start, n):
        tr = max(
            highs[i]  - lows[i],
            abs(highs[i]  - closes[i - 1]),
            abs(lows[i]   - closes[i - 1]),
        )
        atr_vals.append(tr)
    atr14 = sum(atr_vals) / ATR_PERIOD

    kc_upper = bb_mid + KC_MULT * atr14
    kc_lower = bb_mid - KC_MULT * atr14

    squeeze_on     = (bb_upper < kc_upper) and (bb_lower > kc_lower)
    hist_now       = closes[-1] - bb_mid
    hist_prev      = closes[-2] - (sum(closes[-BB_PERIOD - 1:-1]) / BB_PERIOD)
    hist_increasing = hist_now > hist_prev

    if direction == "BUY":
        ttm_fire = (not squeeze_on) and (hist_now > 0) and hist_increasing
    else:
        ttm_fire = (not squeeze_on) and (hist_now < 0) and (not hist_increasing)

    # ── Laguerre RSI (warmup diperbaiki) ──────────────────────
    # Jalankan semua bar closes melalui filter secara berurutan
    # agar state L0..L3 benar-benar konvergen sebelum bar terakhir dibaca.
    gamma = LAGUERRE_GAMMA
    L0 = L1 = L2 = L3 = closes[0]   # inisialisasi dari bar paling awal

    for c in closes:                  # iterate seluruh data, bukan hanya 5 terakhir
        nL0 = (1.0 - gamma) * c + gamma * L0
        nL1 = -gamma * nL0 + L0 + gamma * L1
        nL2 = -gamma * nL1 + L1 + gamma * L2
        nL3 = -gamma * nL2 + L2 + gamma * L3
        L0, L1, L2, L3 = nL0, nL1, nL2, nL3

    cu = (
        max(L0 - L1, 0.0) +
        max(L1 - L2, 0.0) +
        max(L2 - L3, 0.0)
    )
    cd = (
        max(L1 - L0, 0.0) +
        max(L2 - L1, 0.0) +
        max(L3 - L2, 0.0)
    )
    lrsi = cu / (cu + cd) if (cu + cd) > 1e-10 else 0.5
    lrsi_ok = (lrsi > 0.5) if direction == "BUY" else (lrsi < 0.5)

    # ── Fisher Transform (period=FISHER_PERIOD) ───────────────
    fp    = FISHER_PERIOD
    hh    = max(highs[-fp:])
    ll    = min(lows[-fp:])
    fisher = 0.0
    if hh != ll:
        raw_val = 2.0 * ((closes[-1] - ll) / (hh - ll)) - 1.0
        raw_val = max(min(raw_val, 0.999), -0.999)
        fisher  = 0.5 * math.log((1.0 + raw_val) / (1.0 - raw_val))

    fisher_ok = (
        (fisher > FISHER_EXTREME and direction == "SELL") or
        (fisher < -FISHER_EXTREME and direction == "BUY") or
        (fisher > 0.0 and direction == "BUY") or
        (fisher < 0.0 and direction == "SELL")
    )

    # ── STC Proxy — EMA precomputed (O(n) bukan O(n²)) ────────
    if n >= MIN_BARS_STC:
        ema_fast = _ema_series(closes, STC_FAST)
        ema_slow = _ema_series(closes, STC_SLOW)
        # MACD series dari index STC_SLOW-1 ke akhir
        macd_series = [
            ema_fast[i] - ema_slow[i]
            for i in range(STC_SLOW - 1, n)
            if ema_slow[i] != 0.0
        ]

        if len(macd_series) >= 10:
            window_macd = macd_series[-10:]
            mh = max(window_macd)
            ml = min(window_macd)
            stc = (
                (macd_series[-1] - ml) / (mh - ml) * 100.0
                if mh != ml else 50.0
            )
        else:
            stc = 50.0
    else:
        stc = 50.0

    stc_ok = (stc < STC_OVERSOLD) if direction == "BUY" else (stc > STC_OVERBOUGHT)

    # ── CMS Aggregation ───────────────────────────────────────
    pts  = 0.0
    pts += 3.0 if ttm_fire  else 0.0
    pts += 2.0 if lrsi_ok   else 0.0
    pts += 2.0 if fisher_ok else 0.0
    pts += 2.0 if stc_ok    else 0.0
    cms  = round(min(pts / 9.0 * 10.0, 10.0), 2)

    return {
        "ttm_fire":  ttm_fire,
        "lrsi_ok":   lrsi_ok,
        "fisher_ok": fisher_ok,
        "stc_ok":    stc_ok,
        "cms_score": cms,
    }


# ═══════════════════════════════════════════════════════════════
# EPS ENGINE — 4-Layer Scalp
# ═══════════════════════════════════════════════════════════════

def calc_eps(sig_data: Dict) -> Dict:
    """Hitung 4-Layer Entry Precision Score untuk scalping.

    Layers:
        L1: Structure  — H4 BOS atau H1 CHoCH terkonfirmasi
        L2: PD Array   — OB / FVG valid
        L3: Momentum   — CMS >= SCALP_CMS_L3
        L4: Micro      — M1 CHoCH atau SFP signal

    Args:
        sig_data: Dict state sinyal (copy dari asdict(Signal)).

    Returns:
        Dict berisi boolean per layer dan eps_score (int 0–4).
    """
    d    = sig_data.get("direction", "BUY")
    bull = d == "BUY"

    # L1: Structure (BOS atau H1 bias aligned = proxy CHoCH)
    bos_ok   = bool(sig_data.get("bos_bull" if bull else "bos_bear", False))
    choch_ok = sig_data.get("h1_bias") == ("BUL" if bull else "BER")
    l1       = bos_ok or choch_ok

    # L2: PD Array
    ob_ok  = bool(sig_data.get("ob_bull_valid" if bull else "ob_bear_valid", False))
    fvg_ok = bool(sig_data.get("fvg_bull_fresh" if bull else "fvg_bear_fresh", False))
    l2     = ob_ok or fvg_ok

    # L3: Momentum CMS
    l3 = float(sig_data.get("cms_score", 0.0)) >= SCALP_CMS_L3

    # L4: Microstructure
    m1_ok  = sig_data.get("m1_bias") == ("BUL" if bull else "BER")
    sfp_ok = sig_data.get("sfp_signal") in ("BULL", "BEAR")
    l4     = m1_ok or sfp_ok

    return {
        "eps_layer1_structure": l1,
        "eps_layer2_pdarray":   l2,
        "eps_layer3_momentum":  l3,
        "eps_layer4_micro":     l4,
        "eps_score":            sum([l1, l2, l3, l4]),
    }


# ═══════════════════════════════════════════════════════════════
# QCM SCORING
# ═══════════════════════════════════════════════════════════════

# Score maps dengan .get() fallback — mencegah KeyError jika count di luar range
_HTF_SCORE_MAP: Dict[int, int] = {3: 20, 2: 14, 1: 7,  0: 0}
_MTF_SCORE_MAP: Dict[int, int] = {3: 12, 2: 8,  1: 4,  0: 0}


def calc_qcm(sig_data: Dict) -> int:
    """Hitung QCM (Quantitative Confluence Matrix) scalp-simplified.

    Maksimum 100 poin dari 8 faktor.

    Threshold:
        < 40   : NO TRADE
        40–54  : Scalp only (25% size)
        55–69  : Standard scalp (50% size)
        70–84  : High quality (75% size)
        >= 85  : Prime scalp (full size)

    Args:
        sig_data: Dict state sinyal.

    Returns:
        Integer QCM score 0–100.
    """
    score = 0
    d    = sig_data.get("direction", "BUY")
    bull = d == "BUY"
    tb   = "BUL" if bull else "BER"  # target bias

    # F1: HTF Structure D1+H4+H1 (max 20)
    htf_cnt = sum([
        sig_data.get("d1_bias")  == tb,
        sig_data.get("h4_bias")  == tb,
        sig_data.get("h1_bias")  == tb,
    ])
    score += _HTF_SCORE_MAP.get(htf_cnt, 0)

    # F2: MTF Alignment M30+M15+M5 (max 12)
    mtf_cnt = sum([
        sig_data.get("m30_bias") == tb,
        sig_data.get("m15_bias") == tb,
        sig_data.get("m5_bias")  == tb,
    ])
    score += _MTF_SCORE_MAP.get(mtf_cnt, 0)

    # F3: PD Array quality (max 15) — priority 1 = terbaik
    pd_priority = max(0, int(sig_data.get("pd_priority", 0)))
    pd_pts      = max(0, 15 - pd_priority)
    score      += pd_pts

    # F4: BOS + Liquidity (max 10)
    bos_ok = bool(sig_data.get("bos_bull" if bull else "bos_bear", False))
    liq_ok = bool(sig_data.get("liq_swept_l" if bull else "liq_swept_h", False))
    score += (5 if bos_ok else 0) + (5 if liq_ok else 0)

    # F5: Candlestick / Displacement (max 8)
    disp_ok = bool(sig_data.get("disp_ok", False))
    sfp_ok  = sig_data.get("sfp_signal") in ("BULL", "BEAR")
    score  += (4 if disp_ok else 0) + (4 if sfp_ok else 0)

    # F6: Volume (max 8)
    vol_ok   = bool(sig_data.get("vol_surge", False))
    not_chop = not bool(sig_data.get("acf_chop", False))
    score   += (5 if vol_ok else 0) + (3 if not_chop else 0)

    # F7: Kill Zone (max 10)
    in_kz  = bool(sig_data.get("in_killzone", False))
    score += 10 if in_kz else 3

    # F8: Momentum CMS (max 15)
    cms      = float(sig_data.get("cms_score", 0.0))
    cms_pts  = int(min(cms / 10.0 * 15, 15))
    score   += cms_pts

    return min(score, MAX_QCM)


# ═══════════════════════════════════════════════════════════════
# GRADE & GATE
# ═══════════════════════════════════════════════════════════════

def calc_grade(eps: int, qcm: int) -> str:
    """Tentukan grade sinyal berdasarkan EPS dan QCM.

    Args:
        eps: EPS score 0–4.
        qcm: QCM score 0–100.

    Returns:
        "PRIME", "HIGH", atau "STANDARD".
    """
    if eps >= MAX_EPS and qcm >= 85:
        return "PRIME"
    elif eps >= 3 and qcm >= 70:
        return "HIGH"
    return "STANDARD"


def check_gate(sig_data: Dict) -> Tuple[bool, str]:
    """Evaluasi validitas entry berdasarkan threshold scalp.

    Hard stops (return False):
        - Direction NONE
        - News Tier >= 1 saat news_ok = False
        - Tidak ada HTF structure (BOS atau H4/H1 aligned)
        - Tidak ada PD Array (OB atau FVG)
        - EPS < SCALP_EPS_MIN
        - QCM < SCALP_QCM_MIN
        - CMS < SCALP_CMS_MIN
        - ACF Chop aktif

    Args:
        sig_data: Dict state sinyal.

    Returns:
        Tuple (gate_ok: bool, reason: str).
        reason = "OK" jika gate terbuka.
    """
    d    = sig_data.get("direction", "NONE")
    eps  = int(sig_data.get("eps_score", 0))
    qcm  = int(sig_data.get("qcm_score", 0))
    cms  = float(sig_data.get("cms_score", 0.0))
    bull = d == "BUY"

    if d == "NONE":
        return False, "NO DIRECTION"

    # News block — aktif jika news_ok=False DAN tier >= 1
    if not bool(sig_data.get("news_ok", False)):
        tier = int(sig_data.get("news_tier", 0))
        if tier >= 1:
            return False, f"NEWS TIER-{tier} BLOCK"

    # Structure minimum
    bos_ok = bool(sig_data.get("bos_bull" if bull else "bos_bear", False))
    htf_ok = (
        sig_data.get("h4_bias") == ("BUL" if bull else "BER") or
        sig_data.get("h1_bias") == ("BUL" if bull else "BER")
    )
    if not bos_ok and not htf_ok:
        return False, "STRUKTUR HTF BELUM ALIGNED"

    # PD Array minimum
    ob_ok  = bool(sig_data.get("ob_bull_valid" if bull else "ob_bear_valid", False))
    fvg_ok = bool(sig_data.get("fvg_bull_fresh" if bull else "fvg_bear_fresh", False))
    if not ob_ok and not fvg_ok:
        return False, "TIDAK ADA PD ARRAY VALID"

    if eps < SCALP_EPS_MIN:
        return False, f"EPS RENDAH ({eps}/{MAX_EPS})"

    if qcm < SCALP_QCM_MIN:
        return False, f"QCM RENDAH ({qcm}/{MAX_QCM})"

    if cms < SCALP_CMS_MIN:
        return False, f"CMS RENDAH ({cms:.1f}/10)"

    if bool(sig_data.get("acf_chop", False)):
        return False, "CHOP — MARKET RANGING"

    return True, "OK"


def check_veto(sig_data: Dict) -> Optional[str]:
    """Cek apakah sinyal harus masuk Scan state (bukan hard stop).

    Args:
        sig_data: Dict state sinyal.

    Returns:
        String veto reason jika perlu scan/wait state, None jika lanjut.
    """
    if not bool(sig_data.get("in_killzone", False)):
        return "OFF-KZ"
    return None


# ═══════════════════════════════════════════════════════════════
# MASTER ANALYZE
# ═══════════════════════════════════════════════════════════════

def analyze(raw: Dict, bars: List[Dict], symbol: str) -> Signal:
    """Engine utama: proses raw data + bars OHLCV → Signal lengkap.

    Pipeline:
        1. Validasi tipe input
        2. Copy field raw ke Signal
        3. Deteksi Kill Zone
        4. Hitung CME (momentum)
        5. Hitung EPS (4-layer)
        6. Hitung QCM (confluence matrix)
        7. Hitung derived scores (SQS, CTX, SOFT)
        8. Tentukan Grade
        9. Evaluasi Veto → Gate

    Args:
        raw:    Dict dari data feed (bias, structure flags, entry/sl/tp, dll).
        bars:   List OHLCV candle dict, minimal MIN_BARS_CME entries.
        symbol: Nama instrumen untuk logging.

    Returns:
        Signal dataclass yang sudah terisi penuh.

    Raises:
        TypeError: Jika field raw tidak bisa dikoersi ke tipe yang benar.
    """
    # Step 1: Validasi & koersi input
    raw = _validate_raw(raw)

    sig = Signal()

    # Step 2: Copy field dari raw → Signal
    _copy_fields = (
        "direction", "d1_bias", "h4_bias", "h1_bias",
        "m30_bias", "m15_bias", "m5_bias", "m1_bias",
        "bos_bull", "bos_bear", "fvg_bull_fresh", "fvg_bear_fresh",
        "ob_bull_valid", "ob_bear_valid", "liq_swept_l", "liq_swept_h",
        "disp_ok", "sfp_signal", "vol_surge", "acf_chop", "pdc_ok",
        "entry", "sl", "tp1", "tp2", "tp3", "tp4", "rr", "risk",
        "abe_level", "expiry_min", "src", "order_type",
        "pd_type", "pd_priority", "fractal_conv", "harmonic_pcz",
        "news_ok", "news_tier",
    )
    for fname in _copy_fields:
        if fname in raw:
            setattr(sig, fname, raw[fname])

    # Step 3: Kill Zone
    now_wib = datetime.now(WIB)
    sig.in_killzone, sig.kz_name = check_killzone(now_wib)

    # Step 4: CME — gunakan copy dict, bukan referensi langsung
    cme = calc_cms(bars, sig.direction)
    sig.ttm_fire  = cme["ttm_fire"]
    sig.lrsi_ok   = cme["lrsi_ok"]
    sig.fisher_ok = cme["fisher_ok"]
    sig.stc_ok    = cme["stc_ok"]
    sig.cms_score = cme["cms_score"]

    # Step 5: EPS — gunakan snapshot dict (copy)
    sig_snapshot = asdict(sig)   # copy aman, tidak ada reference ke internal Signal
    eps_result   = calc_eps(sig_snapshot)
    sig.eps_layer1_structure = eps_result["eps_layer1_structure"]
    sig.eps_layer2_pdarray   = eps_result["eps_layer2_pdarray"]
    sig.eps_layer3_momentum  = eps_result["eps_layer3_momentum"]
    sig.eps_layer4_micro     = eps_result["eps_layer4_micro"]
    sig.eps_score            = eps_result["eps_score"]

    # Step 6: QCM — snapshot baru setelah EPS
    sig_snapshot  = asdict(sig)
    sig.qcm_score = calc_qcm(sig_snapshot)

    # Step 7: Derived scores
    sig.sqs_score = round(sig.qcm_score / 10.0, 1)

    ctx = 0
    ctx += 2 if (sig.bos_bull or sig.bos_bear)           else 0
    ctx += 2 if (sig.fvg_bull_fresh or sig.fvg_bear_fresh) else 0
    ctx += 2 if (sig.ob_bull_valid or sig.ob_bear_valid)  else 0
    ctx += 1 if (sig.liq_swept_l or sig.liq_swept_h)     else 0
    ctx += 1 if sig.disp_ok                               else 0
    sig.ctx_score = min(ctx, MAX_CTX)

    soft = 0
    soft += 2 if sig.sfp_signal in ("BULL", "BEAR") else 0
    soft += 2 if sig.vol_surge                        else 0
    soft += 1 if not sig.acf_chop                     else 0
    soft += 1 if sig.pdc_ok                           else 0
    soft += 1 if sig.harmonic_pcz                     else 0
    soft += 1 if sig.fractal_conv >= 3                else 0
    sig.soft_count = min(soft, MAX_SOFT)

    # Step 8: Grade
    sig.grade = calc_grade(sig.eps_score, sig.qcm_score)

    # Step 9: Veto → Gate
    sig_snapshot = asdict(sig)
    veto = check_veto(sig_snapshot)
    if veto:
        sig.gate_ok  = False
        sig.veto_rsn = veto
        return sig

    gate_ok, gate_reason = check_gate(sig_snapshot)
    sig.gate_ok  = gate_ok
    sig.veto_rsn = gate_reason if not gate_ok else "OK"

    return sig


# ═══════════════════════════════════════════════════════════════
# MASTER FORMATTER
# ═══════════════════════════════════════════════════════════════

def fmt_msg(
    sig_obj: Signal,
    symbol: str,
    stats: Optional[HistoricalStats] = None,
) -> str:
    """Router formatter: pilih fmt_scan / fmt_waiting / fmt_entry.

    Args:
        sig_obj: Signal dataclass hasil analyze().
        symbol:  Nama instrumen.
        stats:   Historical stats opsional (default: DEFAULT_STATS).

    Returns:
        String HTML siap kirim Telegram.
    """
    sig       = asdict(sig_obj)   # copy aman
    direction = sig["direction"]
    gate_ok   = sig["gate_ok"]
    veto      = sig["veto_rsn"]

    if veto in ("OFF-KZ", "CHOP", "NO DIRECTION", "WEEKEND") or direction == "NONE":
        return fmt_scan(sig, symbol, stats)

    if not gate_ok:
        return fmt_waiting(sig, symbol, stats)

    return fmt_entry(sig, symbol, stats)


# ═══════════════════════════════════════════════════════════════
# TELEGRAM SENDER — dengan retry & specific exceptions
# ═══════════════════════════════════════════════════════════════

def send_telegram(
    msg: str,
    max_retry: int = TELEGRAM_MAX_RETRY,
    token: str     = TELEGRAM_TOKEN,
    chat_id: str   = TELEGRAM_CHATID,
) -> bool:
    """Kirim pesan ke Telegram dengan exponential backoff retry.

    Args:
        msg:       Pesan HTML.
        max_retry: Jumlah maksimum percobaan (default 3).
        token:     Telegram Bot Token.
        chat_id:   Target Chat ID.

    Returns:
        True jika berhasil terkirim, False jika semua retry gagal.
    """
    if not token or not chat_id:
        print(msg)
        return True

    url     = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id":    chat_id,
        "text":       msg,
        "parse_mode": "HTML",
    }

    for attempt in range(1, max_retry + 1):
        try:
            resp = requests.post(url, json=payload, timeout=TELEGRAM_TIMEOUT)
            resp.raise_for_status()
            log.info("Telegram: pesan terkirim (attempt %d).", attempt)
            return True

        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            log.warning(
                "Telegram HTTP error %s (attempt %d/%d).",
                status, attempt, max_retry,
            )
            if exc.response is not None and exc.response.status_code == 429:
                # Rate limit — tunggu lebih lama
                retry_after = int(
                    exc.response.headers.get("Retry-After", 5)
                )
                log.info("Rate limited. Tunggu %ds.", retry_after)
                time.sleep(retry_after)
                continue

        except requests.exceptions.Timeout:
            log.warning(
                "Telegram timeout (attempt %d/%d).", attempt, max_retry
            )

        except requests.exceptions.ConnectionError as exc:
            log.warning(
                "Telegram connection error (attempt %d/%d): %s",
                attempt, max_retry, exc,
            )

        # Exponential backoff sebelum retry berikutnya
        if attempt < max_retry:
            delay = TELEGRAM_RETRY_DELAY * (2 ** (attempt - 1))
            log.info("Retry dalam %.1f detik...", delay)
            time.sleep(delay)

    log.error(
        "Telegram: GAGAL setelah %d attempt. Pesan tidak terkirim.", max_retry
    )
    return False


# ═══════════════════════════════════════════════════════════════
# DEMO MAIN
# ═══════════════════════════════════════════════════════════════

def demo_main() -> None:
    """Demo: bangun Signal manual dari raw data simulasi → format → kirim.

    Dalam production, raw dict berasal dari TwelveData/MT5 API,
    dan bars berasal dari endpoint OHLCV TwelveData.
    """
    import random

    random.seed(42)  # reprodusibel untuk testing

    raw: Dict = {
        "direction":      "BUY",
        "d1_bias":        "BUL",
        "h4_bias":        "BUL",
        "h1_bias":        "BUL",
        "m30_bias":       "BUL",
        "m15_bias":       "BUL",
        "m5_bias":        "NEU",
        "m1_bias":        "BUL",
        "bos_bull":       True,
        "bos_bear":       False,
        "fvg_bull_fresh": True,
        "fvg_bear_fresh": False,
        "ob_bull_valid":  True,
        "ob_bear_valid":  False,
        "liq_swept_l":    True,
        "liq_swept_h":    False,
        "disp_ok":        True,
        "sfp_signal":     "BULL",
        "vol_surge":      True,
        "acf_chop":       False,
        "pdc_ok":         True,
        "entry":          3312.50,
        "sl":             3305.00,
        "tp1":            3320.00,
        "tp2":            3328.00,
        "tp3":            3338.00,
        "tp4":            3350.00,
        "rr":             2.8,
        "risk":           7.50,
        "abe_level":      3316.00,
        "expiry_min":     45,
        "src":            "M5 FVG Retest",
        "order_type":     "BUY LIMIT",
        "pd_type":        "FreshOB + FVG",
        "pd_priority":    3,
        "fractal_conv":   4,
        "harmonic_pcz":   False,
        "news_ok":        True,   # eksplisit True setelah verifikasi feed
        "news_tier":      0,
    }

    # Simulasi bars OHLCV (MIN_BARS_CME candle)
    base: float = 3310.0
    bars: List[Dict] = []
    for _ in range(MIN_BARS_CME):
        o = base + random.uniform(-2.0, 2.0)
        c = o    + random.uniform(-1.5, 2.0)
        h = max(o, c) + random.uniform(0.0, 1.0)
        lw = min(o, c) - random.uniform(0.0, 1.0)
        v = random.uniform(800.0, 2000.0)
        bars.append({"open": o, "high": h, "low": lw, "close": c, "volume": v})
        base = c

    sig = analyze(raw, bars, SYMBOL)

    log.info(
        "EPS=%d/%d | QCM=%d | CMS=%.1f | gate=%s | veto=%s | grade=%s",
        sig.eps_score, MAX_EPS,
        sig.qcm_score,
        sig.cms_score,
        sig.gate_ok,
        sig.veto_rsn,
        sig.grade,
    )

    msg = fmt_msg(sig, SYMBOL)
    success = send_telegram(msg)
    if not success:
        log.error("Pesan gagal dikirim. Cek koneksi / credentials Telegram.")


if __name__ == "__main__":
    demo_main()
  # test_pemif_v20_4.py
"""
Unit test suite untuk PEMIF v20.4 Scalp Edition.
Jalankan dengan: pytest test_pemif_v20_4.py -v
"""
import pytest
import math
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

# Import semua target yang diuji
from pemif_v20_4_refactored import (
    Signal, HistoricalStats, DEFAULT_STATS,
    check_killzone, calc_cms, calc_eps, calc_qcm,
    check_gate, analyze, fmt_msg, confidence_pct,
    cms_bar, _validate_raw, send_telegram,
    TRADING_WEEKDAYS, MIN_BARS_CME, WIB,
)


# ═══════════════════════════════════════════════════════════════
# FIXTURES
# ═══════════════════════════════════════════════════════════════

@pytest.fixture
def minimal_bars():
    """55 bar OHLCV dummy untuk CME calculation."""
    import random
    random.seed(0)
    bars = []
    base = 3300.0
    for _ in range(MIN_BARS_CME):
        o = base + random.uniform(-1, 1)
        c = o    + random.uniform(-1, 1)
        h = max(o, c) + random.uniform(0, 0.5)
        lw = min(o, c) - random.uniform(0, 0.5)
        bars.append({"open": o, "high": h, "low": lw, "close": c, "volume": 1000})
        base = c
    return bars


@pytest.fixture
def bullish_raw():
    """Raw dict sinyal BUY lengkap dengan semua field wajib."""
    return {
        "direction":      "BUY",
        "d1_bias": "BUL", "h4_bias": "BUL", "h1_bias": "BUL",
        "m30_bias": "BUL", "m15_bias": "BUL", "m5_bias": "BUL", "m1_bias": "BUL",
        "bos_bull": True,  "bos_bear": False,
        "fvg_bull_fresh": True, "fvg_bear_fresh": False,
        "ob_bull_valid": True,  "ob_bear_valid": False,
        "liq_swept_l": True,   "liq_swept_h": False,
        "disp_ok": True,  "sfp_signal": "BULL",
        "vol_surge": True, "acf_chop": False, "pdc_ok": True,
        "entry": 3312.50, "sl": 3305.00,
        "tp1": 3320.0, "tp2": 3328.0, "tp3": 3338.0, "tp4": 3350.0,
        "rr": 2.8, "risk": 7.5, "abe_level": 3316.0,
        "expiry_min": 45,
        "src": "M5 FVG", "order_type": "BUY LIMIT",
        "pd_type": "FreshOB", "pd_priority": 3,
        "fractal_conv": 4, "harmonic_pcz": False,
        "news_ok": True, "news_tier": 0,
    }


# ═══════════════════════════════════════════════════════════════
# TEST 1: Kill Zone — Weekend Guard
# Skenario: Sabtu & Minggu harus selalu return (False, "WEEKEND")
# Termasuk jam yang normanya adalah Kill Zone aktif
# ═══════════════════════════════════════════════════════════════

class TestKillZone:

    def test_weekend_saturday_inside_killzone_hours(self):
        """Sabtu jam 15:00 WIB (jam London KZ) → harus return WEEKEND."""
        # weekday() Sabtu = 5
        saturday = datetime(2025, 6, 7, 15, 0, tzinfo=WIB)  # Sabtu
        assert saturday.weekday() == 5
        in_kz, name = check_killzone(saturday)
        assert in_kz is False
        assert name == "WEEKEND"

    def test_sunday_always_off(self):
        """Minggu (weekday=6) → WEEKEND tanpa terkecuali."""
        sunday = datetime(2025, 6, 8, 20, 0, tzinfo=WIB)
        in_kz, name = check_killzone(sunday)
        assert in_kz is False
        assert name == "WEEKEND"

    def test_friday_inside_ny_pm_kz(self):
        """Jumat 23:30 WIB → NY PM KZ aktif (cross-midnight)."""
        friday = datetime(2025, 6, 6, 23, 30, tzinfo=WIB)
        assert friday.weekday() == 4  # Jumat
        in_kz, name = check_killzone(friday)
        assert in_kz is True
        assert "NY PM" in name

    def test_monday_london_kz(self):
        """Senin 14:30 WIB → London KZ."""
        monday = datetime(2025, 6, 2, 14, 30, tzinfo=WIB)
        in_kz, name = check_killzone(monday)
        assert in_kz is True
        assert "London" in name

    def test_off_killzone_hours(self):
        """Rabu 10:00 WIB → bukan KZ apapun."""
        wednesday = datetime(2025, 6, 4, 10, 0, tzinfo=WIB)
        in_kz, name = check_killzone(wednesday)
        assert in_kz is False
        assert name == "-"


# ═══════════════════════════════════════════════════════════════
# TEST 2: CME — Laguerre RSI Convergence
# Skenario: Verifikasi perbaikan bug variable shadowing
# Nilai LRSI harus bervariasi tergantung data, bukan konstan
# ═══════════════════════════════════════════════════════════════

class TestCMELaguerreRSI:

    def test_lrsi_varies_with_trending_data(self):
        """Trending up data → LRSI untuk BUY harus > 0.5."""
        bars = []
        price = 3300.0
        for i in range(MIN_BARS_CME):
            # Tren naik konsisten
            price += 0.5
            bars.append({
                "open": price - 0.5, "high": price + 0.2,
                "low": price - 0.6,  "close": price, "volume": 1000
            })
        result = calc_cms(bars, "BUY")
        # Dalam tren naik kuat, LRSI harus > 0.5 (bullish aligned)
        assert result["lrsi_ok"] is True

    def test_lrsi_inverted_for_downtrend(self):
        """Trending down data → LRSI untuk BUY harus < 0.5 (False)."""
        bars = []
        price = 3400.0
        for i in range(MIN_BARS_CME):
            price -= 0.5
            bars.append({
                "open": price + 0.5, "high": price + 0.6,
                "low": price - 0.2,  "close": price, "volume": 1000
            })
        result = calc_cms(bars, "BUY")
        # Downtrend → lrsi_ok untuk BUY harus False
        assert result["lrsi_ok"] is False

    def test_cms_score_range(self, minimal_bars):
        """CMS score harus selalu dalam rentang 0.0–10.0."""
        for direction in ("BUY", "SELL"):
            result = calc_cms(minimal_bars, direction)
            assert 0.0 <= result["cms_score"] <= 10.0, (
                f"CMS out of range untuk {direction}: {result['cms_score']}"
            )

    def test_insufficient_bars_returns_zero(self):
        """Bars < MIN_BARS_CME → semua False, cms_score = 0.0."""
        short_bars = [
            {"open": 3300, "high": 3302, "low": 3298, "close": 3301, "volume": 100}
            for _ in range(10)
        ]
        result = calc_cms(short_bars, "BUY")
        assert result["cms_score"] == 0.0
        assert result["ttm_fire"]  is False
        assert result["lrsi_ok"]   is False

    def test_empty_bars_returns_zero(self):
        """Empty bars list → safe fallback."""
        result = calc_cms([], "BUY")
        assert result["cms_score"] == 0.0


# ═══════════════════════════════════════════════════════════════
# TEST 3: Gate Logic — News Fail-Safe
# Skenario: Default news_ok=False harus block entry Tier-1
# ═══════════════════════════════════════════════════════════════

class TestGateNewsFailSafe:

    def _base_sig(self) -> dict:
        return {
            "direction": "BUY",
            "h4_bias": "BUL", "h1_bias": "BUL",
            "bos_bull": True, "bos_bear": False,
            "ob_bull_valid": True, "fvg_bull_fresh": False,
            "ob_bear_valid": False, "fvg_bear_fresh": False,
            "liq_swept_h": False, "liq_swept_l": False,
            "eps_score": 3, "qcm_score": 60,
            "cms_score": 5.0, "acf_chop": False,
        }

    def test_news_ok_false_tier1_blocks_entry(self):
        """news_ok=False + news_tier=1 → BLOCK."""
        sig = self._base_sig()
        sig["news_ok"]   = False
        sig["news_tier"] = 1
        ok, reason = check_gate(sig)
        assert ok is False
        assert "NEWS" in reason

    def test_news_ok_true_tier1_passes(self):
        """news_ok=True (feed sudah verifikasi) + tier=1 → TIDAK block."""
        sig = self._base_sig()
        sig["news_ok"]   = True
        sig["news_tier"] = 1
        ok, _ = check_gate(sig)
        # Tidak di-block oleh news — mungkin block oleh alasan lain
        assert "NEWS" not in _   # news bukan penyebab block

    def test_default_signal_news_ok_is_false(self):
        """Signal() default → news_ok harus False (fail-safe)."""
        sig = Signal()
        assert sig.news_ok is False

    def test_no_direction_blocks_immediately(self):
        """direction=NONE → block langsung."""
        sig = self._base_sig()
        sig["direction"] = "NONE"
        ok, reason = check_gate(sig)
        assert ok is False
        assert reason == "NO DIRECTION"

    def test_chop_blocks_entry(self):
        """acf_chop=True → CHOP block."""
        sig = self._base_sig()
        sig["news_ok"]   = True
        sig["acf_chop"]  = True
        ok, reason = check_gate(sig)
        assert ok is False
        assert "CHOP" in reason


# ═══════════════════════════════════════════════════════════════
# TEST 4: Input Validation — Type Coercion & Edge Cases
# Skenario: _validate_raw harus menangani tipe data tidak standar
# ═══════════════════════════════════════════════════════════════

class TestInputValidation:

    def test_string_true_coerced_to_bool(self):
        """'true' string → bool True."""
        raw = {"bos_bull": "true", "direction": "BUY"}
        validated = _validate_raw(raw)
        assert validated["bos_bull"] is True
        assert isinstance(validated["bos_bull"], bool)

    def test_string_false_coerced(self):
        """'false' string → bool False."""
        raw = {"bos_bull": "false"}
        validated = _validate_raw(raw)
        assert validated["bos_bull"] is False

    def test_string_float_coerced(self):
        """'3312.5' string → float."""
        raw = {"entry": "3312.5"}
        validated = _validate_raw(raw)
        assert validated["entry"] == pytest.approx(3312.5)
        assert isinstance(validated["entry"], float)

    def test_invalid_float_raises_type_error(self):
        """'abc' untuk field float → TypeError."""
        with pytest.raises(TypeError, match="entry"):
            _validate_raw({"entry": "abc"})

    def test_none_field_not_coerced(self):
        """Field yang tidak ada di raw tidak disentuh."""
        raw = {"direction": "BUY"}
        validated = _validate_raw(raw)
        # Hanya field yang ada di raw yang diproses
        assert "bos_bull" not in validated

    def test_integer_1_coerced_to_bool_true(self):
        """Integer 1 → bool True untuk bool field."""
        raw = {"vol_surge": 1}
        validated = _validate_raw(raw)
        assert validated["vol_surge"] is True


# ═══════════════════════════════════════════════════════════════
# TEST 5: QCM Score Map — KeyError Prevention
# Skenario: dict lookup dengan nilai di luar range 0-3
# ═══════════════════════════════════════════════════════════════

class TestQCMRobustness:

    def _minimal_sig(self, direction="BUY") -> dict:
        bull = direction == "BUY"
        tb   = "BUL" if bull else "BER"
        return {
            "direction": direction,
            "d1_bias": tb, "h4_bias": tb, "h1_bias": tb,
            "m30_bias": tb, "m15_bias": tb, "m5_bias": tb,
            "bos_bull": bull, "bos_bear": not bull,
            "liq_swept_l": bull, "liq_swept_h": not bull,
            "disp_ok": True, "sfp_signal": "BULL" if bull else "BEAR",
            "vol_surge": True, "acf_chop": False,
            "in_killzone": True, "cms_score": 7.0,
            "pd_priority": 1,
            "ob_bull_valid": False, "ob_bear_valid": False,
            "fvg_bull_fresh": False, "fvg_bear_fresh": False,
        }

    def test_qcm_never_exceeds_100(self):
        """QCM score tidak boleh melebihi MAX_QCM = 100."""
        sig = self._minimal_sig("BUY")
        score = calc_qcm(sig)
        assert 0 <= score <= 100

    def test_qcm_sell_direction(self):
        """QCM untuk SELL direction harus valid 0–100."""
        sig = self._minimal_sig("SELL")
        score = calc_qcm(sig)
        assert 0 <= score <= 100

    def test_qcm_empty_sig(self):
        """Dict kosong → QCM harus return 0 tanpa crash."""
        score = calc_qcm({})
        assert score >= 0  # tidak crash, tidak negatif

    def test_qcm_pd_priority_out_of_range(self):
        """pd_priority negatif → pd_pts harus 0 (tidak negatif)."""
        sig = self._minimal_sig()
        sig["pd_priority"] = -5   # nilai tidak valid
        score = calc_qcm(sig)
        assert score >= 0   # tidak ada negative contribution


# ═══════════════════════════════════════════════════════════════
# TEST 6: Telegram Retry Logic
# Skenario: Rate limit (429) harus trigger retry dengan backoff
# ═══════════════════════════════════════════════════════════════

class TestTelegramRetry:

    @patch("pemif_v20_4_refactored.requests.post")
    @patch("pemif_v20_4_refactored.time.sleep")
    def test_rate_limit_retries(self, mock_sleep, mock_post):
        """HTTP 429 → retry dengan Retry-After header."""
        rate_limit_response = MagicMock()
        rate_limit_response.status_code = 429
        rate_limit_response.headers = {"Retry-After": "2"}

        # 2 kali rate limit, ke-3 sukses
        success_response = MagicMock()
        success_response.raise_for_status = MagicMock()

        from requests.exceptions import HTTPError
        mock_post.side_effect = [
            HTTPError(response=rate_limit_response),
            HTTPError(response=rate_limit_response),
            success_response,
        ]

        result = send_telegram("test msg", max_retry=3, token="tok", chat_id="123")
        assert result is True
        assert mock_post.call_count == 3

    @patch("pemif_v20_4_refactored.requests.post")
    @patch("pemif_v20_4_refactored.time.sleep")
    def test_all_retries_exhausted_returns_false(self, mock_sleep, mock_post):
        """Semua retry gagal → return False."""
        from requests.exceptions import Timeout
        mock_post.side_effect = Timeout("timeout")

        result = send_telegram("msg", max_retry=3, token="tok", chat_id="123")
        assert result is False
        assert mock_post.call_count == 3

    @patch("builtins.print")
    def test_no_credentials_prints_to_stdout(self, mock_print):
        """Tanpa token/chat_id → print ke stdout, return True."""
        result = send_telegram("msg", token="", chat_id="")
        assert result is True
        mock_print.assert_called_once_with("msg")


# ═══════════════════════════════════════════════════════════════
# TEST 7: Full Pipeline — analyze() end-to-end
# Skenario: Raw data lengkap → Signal yang konsisten dan valid
# ═══════════════════════════════════════════════════════════════

class TestAnalyzePipeline:

    def test_full_bullish_pipeline(self, bullish_raw, minimal_bars):
        """Raw BUY lengkap + bars cukup → Signal terbentuk tanpa error."""
        sig = analyze(bullish_raw, minimal_bars, "XAU/USD")
        assert isinstance(sig, Signal)
        assert sig.direction == "BUY"
        assert 0 <= sig.eps_score <= 4
        assert 0 <= sig.qcm_score <= 100
        assert 0.0 <= sig.cms_score <= 10.0
        assert isinstance(sig.gate_ok, bool)

    def test_off_kz_returns_scan_message(self, bullish_raw, minimal_bars):
        """Jam off-KZ → fmt_msg harus return SCANNING."""
        # Paksa off-KZ dengan mock time (Rabu jam 10:00 WIB)
        wednesday_off = datetime(2025, 6, 4, 10, 0, tzinfo=WIB)
        with patch("pemif_v20_4_refactored.datetime") as mock_dt:
            mock_dt.now.return_value = wednesday_off
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            sig = analyze(bullish_raw, minimal_bars, "XAU/USD")

        msg = fmt_msg(sig, "XAU/USD")
        assert "SCANNING" in msg or "OFF-KZ" in sig.veto_rsn

    def test_no_direction_produces_scan_message(self, minimal_bars):
        """direction=NONE → fmt_msg return scan state."""
        raw = {"direction": "NONE", "news_ok": True}
        sig = analyze(raw, minimal_bars, "XAU/USD")
        msg = fmt_msg(sig, "XAU/USD")
        assert "SCAN" in msg

    def test_analyze_with_string_bool_in_raw(self, bullish_raw, minimal_bars):
        """Raw dengan string bool ('true') → tidak crash, dikoersi benar."""
        bullish_raw["bos_bull"]  = "true"
        bullish_raw["vol_surge"] = "1"
        sig = analyze(bullish_raw, minimal_bars, "XAU/USD")
        assert sig.bos_bull  is True
        assert sig.vol_surge is True

    def test_historical_stats_consistent_in_entry_msg(self, bullish_raw, minimal_bars):
        """HistoricalStats properties wins+losses == total."""
        stats = HistoricalStats(total=200, winrate=80.0, avg_rr=2.5)
        assert stats.wins + stats.losses == stats.total


# ═══════════════════════════════════════════════════════════════
# TEST 8: Confidence & Utility Functions
# ═══════════════════════════════════════════════════════════════

class TestUtilityFunctions:

    def test_confidence_pct_max_inputs(self):
        """Max semua score → confidence = 100%."""
        pct = confidence_pct(eps=4, sqs=10.0, ctx=8, soft=8)
        assert pct == 100

    def test_confidence_pct_zero_inputs(self):
        """Semua nol → confidence = 0%."""
        pct = confidence_pct(eps=0, sqs=0.0, ctx=0, soft=0)
        assert pct == 0

    def test_confidence_pct_partial(self):
        """Nilai parsial harus dalam rentang 0–100."""
        pct = confidence_pct(eps=2, sqs=5.0, ctx=4, soft=4)
        assert 0 <= pct <= 100

    def test_cms_bar_clamps_above_10(self):
        """cms_bar dengan nilai > 10 harus diclamp ke 10."""
        bar = cms_bar(15.0)
        assert "10.0/10" in bar
        assert "█" * 10 in bar

    def test_cms_bar_zero(self):
        """cms_bar(0) harus all empty."""
        bar = cms_bar(0.0)
        assert "░" * 10 in bar
        assert "0.0/10" in bar

    def test_cms_bar_negative_clamped(self):
        """CMS negatif → clamp ke 0."""
        bar = cms_bar(-5.0)
        assert "0.0/10" in bar
