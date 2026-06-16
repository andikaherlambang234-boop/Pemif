#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PEMIF v22.3 ADAPTIVE INTELLIGENCE ENGINE — STRUCTURE FIX + AUTO REGIME
+ VISUAL VALIDATOR + FVG ZONE FIX (gabungan satu file)
════════════════════════════════════════════════════════════════
ROOT CAUSE (zero signal / "zonk" 2 hari):
  raw dict di on_bar_close() HANYA mengisi direction="BUY" statis.
  Semua field struktur (bos_bull, ob_bull_valid, fvg_bull_fresh,
  swing_high/low, bias MTF) TIDAK PERNAH dihitung dari bars ->
  selalu default False/0/NEU -> check_gate() selalu gagal di
  STRUKTUR HTF / PD ARRAY check -> PendingOrderEngine selalu
  return valid=False. Hanya arah BUY pernah dicoba, SELL tidak.

FIX v22.2:
  1. StructureEngine baru: hitung swing high/low, BOS/CHoCH,
     Order Block, FVG dari buffer M1 -> resample internal jadi
     M5/M15/M30/H1/H4/D1 (tanpa fetch API tambahan).
  2. Kedua arah BUY & SELL dievaluasi tiap bar close.
  3. RegimeAdaptiveMode: auto switch LOOSE<->STRICT.
  4. Visual Validator (gabungan): jalankan dengan flag --validate
     untuk plot candlestick + marker swing/BOS/OB dari data LIVE
     TwelveData, untuk cross-check manual terhadap TradingView.
     Library plotting (matplotlib/mplfinance/pandas) OPSIONAL —
     bot utama (mode normal) tetap jalan tanpa library ini.

FIX v22.3 (FVG zone coordinates — silent reject pada setup FVG-only):
  ROOT CAUSE: detect_fvg() cuma balikin boolean fresh/tidak, gak
  pernah nyimpen koordinat gap-nya. Signal/PendingOrderEngine sama
  sekali gak punya field fvg_bull_high/low atau fvg_bear_high/low.
  Akibatnya kondisi entry di _bull()/_bear() — yang nulis
  (ob_bull_valid or fvg_bull_fresh) and ob_bull_low > 0 — gagal
  kalau yang valid CUMA FVG (gak ada OB), karena ob_bull_low tetap
  default 0.0. Order jatuh ke fallback BOS+liquidity-sweep yang
  syaratnya lebih ketat (butuh bos_bull True), dan kalau itu juga
  gagal -> order.valid=False -> check_gate() ke-block di check
  "order_valid" dengan reason generic "ORDER LEVEL TIDAK VALID"
  yang gak ngasih clue bahwa akar masalahnya FVG-tanpa-OB. Ini
  sering terjadi karena detect_order_block() WAJIB bos_bull/bos_bear
  True sebagai prasyarat sebelum cari OB candle, sedangkan
  detect_fvg() independen total dari BOS — apalagi di mode LOOSE
  (FVG_MIN_GAP_ATR_MULT_LOOSE=0.05) gap fresh tanpa BOS itu wajar
  sering muncul, terutama di awal tren sebelum structure break.

  FIX:
  1. detect_fvg() sekarang balikin 6 value: fvg_bull, fvg_bear,
     fvg_bull_high, fvg_bull_low, fvg_bear_high, fvg_bear_low.
     Koordinat yang diambil adalah gap PALING FRESH (closest ke bar
     terakhir) untuk masing-masing arah, gak ke-overwrite oleh gap
     yang lebih lama walau loop tetap scan ke belakang.
  2. Signal dataclass nambah 4 field baru: fvg_bull_high/low,
     fvg_bear_high/low — terisi otomatis lewat compute() + setattr
     loop yang sudah ada, gak perlu ubah apa pun di analyze().
  3. PendingOrderEngine._bull()/._bear() sekarang punya jalur
     terpisah: OB valid -> pakai OB (perilaku lama, gak berubah);
     OB gak valid tapi FVG fresh -> pakai koordinat FVG sebagai
     entry zone; baru fallback ke BOS+liquidity-sweep breakout kalau
     dua-duanya gak ada. order_type tetap "BUY/SELL LIMIT" generic
     (gak dipecah jadi sub-tipe) supaya gak merusak bucket
     get_stats_by_order_type() di L1 StatisticalLearner.
  4. Visual Validator: tambah rect FVG Bull (deepskyblue) & FVG Bear
     (magenta) di _build_markers(), plus baris koordinat FVG di info
     box dan di _quick_diagnostic() (--diag), supaya fix ini bisa
     di-cross-check langsung secara visual.
════════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import json
import logging
import math
import os
import sys
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    import websocket
    WS_AVAILABLE = True
except ImportError:
    WS_AVAILABLE = False

try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import StandardScaler
    import numpy as np
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False

try:
    from xgboost import XGBClassifier
    XGB_AVAILABLE = True
except ImportError:
    XGB_AVAILABLE = False

# ── Library untuk mode --validate (OPSIONAL, tidak dibutuhkan bot utama) ──
try:
    import pandas as pd
    import mplfinance as mpf
    import matplotlib.pyplot as plt
    PLOT_AVAILABLE = True
except ImportError:
    PLOT_AVAILABLE = False

__all__ = [
    "Signal", "HistoricalStats", "TradeJournal",
    "PriceStream", "PendingOrderEngine", "StructureEngine",
    "RegimeAdaptiveMode",
    "StatisticalLearner", "PatternRecognizer", "MLEngine",
    "AdaptiveController", "KillZoneScheduler",
    "analyze", "fmt_signal_telegram", "fmt_no_signal_telegram",
    "fmt_scanning_telegram", "send_telegram", "run_engine",
    "run_validator",
]

# ═══════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("PEMIF-v22.3")

# ═══════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════
WIB = timezone(timedelta(hours=7))

MAX_EPS:  int   = 4
MAX_SQS:  float = 10.0
MAX_CTX:  int   = 8
MAX_SOFT: int   = 8
MAX_QCM:  int   = 100

BASE_SCALP_EPS_MIN:  int   = 2
BASE_SCALP_QCM_MIN:  int   = 55
BASE_SCALP_CMS_MIN:  float = 3.5
BASE_SCALP_CMS_L3:   float = 3.5
BASE_WINRATE_MIN:    float = 50.0

LAGUERRE_GAMMA: float = 0.5
BB_PERIOD:      int   = 20
BB_MULT:        float = 2.0
KC_MULT:        float = 1.5
ATR_PERIOD:     int   = 14
FISHER_PERIOD:  int   = 9
FISHER_EXTREME: float = 1.5
STC_FAST:       int   = 23
STC_SLOW:       int   = 50
STC_CYCLE:      int   = 10
STC_OVERSOLD:   float = 25.0
STC_OVERBOUGHT: float = 75.0

OB_ENTRY_ATR_MULT: float = 0.3
BREAKOUT_ATR_MULT: float = 0.1
SL_ATR_MULT:       float = 1.5
SL_SWING_BUFFER:   float = 0.3
FIB_TP1: float = 1.0
FIB_TP2: float = 1.618
FIB_TP3: float = 2.618

MIN_BARS_CME: int = 60
MIN_BARS_STC: int = 55

WS_RECONNECT_DELAY: float = 5.0
REST_POLL_INTERVAL: float = 10.0

TELEGRAM_MAX_RETRY:   int   = 3
TELEGRAM_RETRY_DELAY: float = 1.5
TELEGRAM_TIMEOUT:     int   = 10

JOURNAL_PATH:  Path = Path("pemif_trade_journal.json")
PATTERN_PATH:  Path = Path("pemif_patterns.json")
REGIME_PATH:   Path = Path("pemif_regime_state.json")

ML_MIN_SAMPLES:   int   = 30
ML_RETRAIN_EVERY: int   = 10
ML_WIN_PROB_MIN:  float = 0.60

TRADING_WEEKDAYS: Tuple[int, ...] = (0, 1, 2, 3, 4)

KILL_ZONES_NY: Tuple[Dict, ...] = (
    {"name": "Asian KZ",     "ny_start": (20, 0), "ny_end": (0,  0), "next_day_end": True},
    {"name": "London KZ",    "ny_start": (2,  0), "ny_end": (5,  0), "next_day_end": False},
    {"name": "NY Open KZ",   "ny_start": (7,  0), "ny_end": (10, 0), "next_day_end": False},
    {"name": "London Close", "ny_start": (10, 0), "ny_end": (12, 0), "next_day_end": False},
)

# ─────────────────────────────────────────────────────────────
# STRUCTURE PARAMS (LOOSE vs STRICT)
# ─────────────────────────────────────────────────────────────
SWING_LOOKBACK_STRICT:  int = 5
SWING_LOOKBACK_LOOSE:   int = 2

FVG_MIN_GAP_ATR_MULT_STRICT: float = 0.25
FVG_MIN_GAP_ATR_MULT_LOOSE:  float = 0.05

FVG_MAX_AGE_BARS_STRICT: int = 15
FVG_MAX_AGE_BARS_LOOSE:  int = 40

OB_MAX_AGE_BARS_STRICT: int = 20
OB_MAX_AGE_BARS_LOOSE:  int = 50

BOS_LOOKBACK_BARS: int = 30
LIQ_SWEEP_LOOKBACK_BARS: int = 20

MTF_TIMEFRAMES: Dict[str, int] = {
    "m5":  5, "m15": 15, "m30": 30, "h1": 60, "h4": 240, "d1": 1440,
}
MTF_BIAS_EMA_FAST_STRICT: int = 20
MTF_BIAS_EMA_SLOW_STRICT: int = 50
MTF_BIAS_EMA_FAST_LOOSE:  int = 8
MTF_BIAS_EMA_SLOW_LOOSE:  int = 21
MTF_MIN_CANDLES_FOR_BIAS: int = 5

VOL_SURGE_MULT_STRICT: float = 1.8
VOL_SURGE_MULT_LOOSE:  float = 1.3

# ─────────────────────────────────────────────────────────────
# REGIME ADAPTIVE MODE PARAMS
# ─────────────────────────────────────────────────────────────
REGIME_INITIAL_MODE: str   = "LOOSE"
REGIME_NO_SIGNAL_BARS_TO_LOOSEN: int   = 40
REGIME_WINRATE_MIN_TO_TIGHTEN:   float = 45.0
REGIME_MIN_TRADES_FOR_WINRATE:   int   = 10
REGIME_HYSTERESIS_BARS:          int   = 5


def _resolve_struct_params(mode: str) -> Dict[str, Any]:
    strict = (mode == "STRICT")
    return {
        "swing_lookback":   SWING_LOOKBACK_STRICT if strict else SWING_LOOKBACK_LOOSE,
        "fvg_min_gap_mult": FVG_MIN_GAP_ATR_MULT_STRICT if strict else FVG_MIN_GAP_ATR_MULT_LOOSE,
        "fvg_max_age":      FVG_MAX_AGE_BARS_STRICT if strict else FVG_MAX_AGE_BARS_LOOSE,
        "ob_max_age":       OB_MAX_AGE_BARS_STRICT if strict else OB_MAX_AGE_BARS_LOOSE,
        "ema_fast":         MTF_BIAS_EMA_FAST_STRICT if strict else MTF_BIAS_EMA_FAST_LOOSE,
        "ema_slow":         MTF_BIAS_EMA_SLOW_STRICT if strict else MTF_BIAS_EMA_SLOW_LOOSE,
        "vol_surge_mult":   VOL_SURGE_MULT_STRICT if strict else VOL_SURGE_MULT_LOOSE,
    }


# ═══════════════════════════════════════════════════════════════
# ENVIRONMENT
# ═══════════════════════════════════════════════════════════════
def _load_env() -> Tuple[str, str, str, str, str]:
    token    = os.environ.get("TELEGRAM_TOKEN",  "")
    chat_id  = os.environ.get("TELEGRAM_CHATID", "")
    td_key   = os.environ.get("TWELVEDATA_KEY",  "")
    symbol   = os.environ.get("SYMBOL",          "XAU/USD")
    interval = os.environ.get("INTERVAL",        "1min")
    if not token or not chat_id:
        log.warning("Telegram credentials tidak di-set.")
    if not td_key:
        log.warning("TwelveData API key tidak di-set.")
    return token, chat_id, td_key, symbol, interval


TELEGRAM_TOKEN, TELEGRAM_CHATID, TWELVEDATA_KEY, SYMBOL, INTERVAL = _load_env()


# ═══════════════════════════════════════════════════════════════
# DST HELPER
# ═══════════════════════════════════════════════════════════════
def _us_dst_active(dt: datetime) -> bool:
    y = dt.year
    march1      = date(y, 3, 1)
    days_to_sun = (6 - march1.weekday()) % 7
    dst_start   = date(y, 3, 1 + days_to_sun + 7)
    nov1             = date(y, 11, 1)
    days_to_sun_nov  = (6 - nov1.weekday()) % 7
    dst_end          = date(y, 11, 1 + days_to_sun_nov)
    return dst_start <= dt.date() < dst_end


def _ny_to_wib_offset(dt: datetime) -> int:
    return 11 if _us_dst_active(dt) else 12


# ═══════════════════════════════════════════════════════════════
# KILL ZONE SCHEDULER
# ═══════════════════════════════════════════════════════════════
class KillZoneScheduler:
    def check(self, now_wib: datetime) -> Tuple[bool, str, str, str]:
        if now_wib.weekday() not in TRADING_WEEKDAYS:
            return False, "WEEKEND", "-", "-"
        offset = _ny_to_wib_offset(now_wib)
        for kz in KILL_ZONES_NY:
            sh, sm   = kz["ny_start"]
            eh, em   = kz["ny_end"]
            is_next  = kz["next_day_end"]
            wib_sh   = (sh + offset) % 24
            wib_eh   = (eh + offset) % 24
            cur_min  = now_wib.hour * 60 + now_wib.minute
            start_m  = wib_sh * 60 + sm
            end_m    = wib_eh * 60 + em
            in_kz    = False
            if is_next or start_m > end_m:
                in_kz = (cur_min >= start_m) or (cur_min < end_m)
            else:
                in_kz = (start_m <= cur_min < end_m)
            if in_kz:
                return True, kz["name"], f"{wib_sh:02d}:{sm:02d}", f"{wib_eh:02d}:{em:02d}"
        return False, "-", "-", "-"

    def get_all_windows_wib(self, now_wib: datetime) -> List[Dict]:
        offset = _ny_to_wib_offset(now_wib)
        result = []
        for kz in KILL_ZONES_NY:
            sh, sm = kz["ny_start"]
            eh, em = kz["ny_end"]
            wib_sh = (sh + offset) % 24
            wib_eh = (eh + offset) % 24
            result.append({
                "name":  kz["name"],
                "start": f"{wib_sh:02d}:{sm:02d} WIB",
                "end":   f"{wib_eh:02d}:{em:02d} WIB",
                "dst":   "DST(EDT)" if _us_dst_active(now_wib) else "Non-DST(EST)",
            })
        return result

    def next_killzone(self, now_wib: datetime) -> Optional[Dict]:
        offset    = _ny_to_wib_offset(now_wib)
        cur_min   = now_wib.hour * 60 + now_wib.minute
        best      = None
        best_wait = 99999
        for kz in KILL_ZONES_NY:
            sh, sm  = kz["ny_start"]
            eh, em  = kz["ny_end"]
            wib_sh  = (sh + offset) % 24
            wib_eh  = (eh + offset) % 24
            start_m = wib_sh * 60 + sm
            wait    = (start_m - cur_min) % (24 * 60)
            if 0 < wait < best_wait:
                best_wait = wait
                best = {
                    "name":     kz["name"],
                    "start":    f"{wib_sh:02d}:{sm:02d} WIB",
                    "end":      f"{wib_eh:02d}:{em:02d} WIB",
                    "wait_min": wait,
                }
        return best

    def current_kz_remaining(self, now_wib: datetime) -> int:
        offset  = _ny_to_wib_offset(now_wib)
        cur_min = now_wib.hour * 60 + now_wib.minute
        for kz in KILL_ZONES_NY:
            sh, sm  = kz["ny_start"]
            eh, em  = kz["ny_end"]
            is_next = kz["next_day_end"]
            wib_sh  = (sh + offset) % 24
            wib_eh  = (eh + offset) % 24
            start_m = wib_sh * 60 + sm
            end_m   = wib_eh * 60 + em
            in_kz   = False
            if is_next or start_m > end_m:
                in_kz = (cur_min >= start_m) or (cur_min < end_m)
            else:
                in_kz = (start_m <= cur_min < end_m)
            if in_kz:
                if end_m > cur_min:
                    return end_m - cur_min
                else:
                    return (24 * 60 - cur_min) + end_m
        return 0


kz_scheduler = KillZoneScheduler()


# ═══════════════════════════════════════════════════════════════
# REGIME ADAPTIVE MODE  (auto LOOSE <-> STRICT)
# ═══════════════════════════════════════════════════════════════
class RegimeAdaptiveMode:
    def __init__(self, journal: "TradeJournal", symbol: str, path: Path = REGIME_PATH) -> None:
        self.journal = journal
        self.symbol  = symbol
        self.path    = path
        self._lock   = threading.Lock()
        self._no_signal_streak_in_kz: int = 0
        self._bars_since_switch: int = 0
        self.mode: str = REGIME_INITIAL_MODE
        self._load()

    def _load(self) -> None:
        if self.path.exists():
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    d = json.load(f)
                self.mode = d.get("mode", REGIME_INITIAL_MODE)
                self._no_signal_streak_in_kz = int(d.get("no_signal_streak", 0))
                self._bars_since_switch = int(d.get("bars_since_switch", 0))
                log.info("Regime state dimuat: mode=%s", self.mode)
                return
            except Exception as e:
                log.warning("Regime state rusak, reset: %s", e)
        self.mode = REGIME_INITIAL_MODE

    def _save(self) -> None:
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump({
                    "mode": self.mode,
                    "no_signal_streak": self._no_signal_streak_in_kz,
                    "bars_since_switch": self._bars_since_switch,
                }, f)
        except Exception as e:
            log.warning("Gagal simpan regime state: %s", e)

    def get_mode(self) -> str:
        with self._lock:
            return self.mode

    def get_params(self) -> Dict[str, Any]:
        with self._lock:
            return _resolve_struct_params(self.mode)

    def on_bar(self, in_killzone: bool, gate_ok: bool) -> Optional[str]:
        with self._lock:
            self._bars_since_switch += 1

            if in_killzone:
                if gate_ok:
                    self._no_signal_streak_in_kz = 0
                else:
                    self._no_signal_streak_in_kz += 1

            switched_to: Optional[str] = None
            can_switch = self._bars_since_switch >= REGIME_HYSTERESIS_BARS

            if can_switch and self.mode == "STRICT":
                if self._no_signal_streak_in_kz >= REGIME_NO_SIGNAL_BARS_TO_LOOSEN:
                    self.mode = "LOOSE"
                    self._no_signal_streak_in_kz = 0
                    self._bars_since_switch = 0
                    switched_to = "LOOSE"
                    log.warning(
                        "REGIME SWITCH -> LOOSE (no signal %d bar dalam KZ)",
                        REGIME_NO_SIGNAL_BARS_TO_LOOSEN,
                    )

            elif can_switch and self.mode == "LOOSE":
                stats = self.journal.get_stats(symbol=self.symbol, last_n=REGIME_MIN_TRADES_FOR_WINRATE * 3)
                if stats.total >= REGIME_MIN_TRADES_FOR_WINRATE and stats.winrate < REGIME_WINRATE_MIN_TO_TIGHTEN:
                    self.mode = "STRICT"
                    self._bars_since_switch = 0
                    switched_to = "STRICT"
                    log.warning(
                        "REGIME SWITCH -> STRICT (winrate rolling %.1f%% < %.1f%%, n=%d)",
                        stats.winrate, REGIME_WINRATE_MIN_TO_TIGHTEN, stats.total,
                    )

            self._save()
            return switched_to


# ═══════════════════════════════════════════════════════════════
# DATACLASSES
# ═══════════════════════════════════════════════════════════════
@dataclass
class HistoricalStats:
    total:   int   = 0
    winrate: float = 0.0
    avg_rr:  float = 0.0

    @property
    def wins(self) -> int:
        return round(self.total * self.winrate / 100)

    @property
    def losses(self) -> int:
        return self.total - self.wins


@dataclass
class OrderLevels:
    order_type:  str   = "NONE"
    entry:       float = 0.0
    sl:          float = 0.0
    tp1:         float = 0.0
    tp2:         float = 0.0
    tp3:         float = 0.0
    rr_tp1:      float = 0.0
    rr_tp2:      float = 0.0
    rr_tp3:      float = 0.0
    risk_pips:   float = 0.0
    atr_current: float = 0.0
    reason:      str   = ""
    valid:       bool  = False


@dataclass
class AdaptiveParams:
    eps_min:              int   = BASE_SCALP_EPS_MIN
    qcm_min:              int   = BASE_SCALP_QCM_MIN
    cms_min:              float = BASE_SCALP_CMS_MIN
    winrate_min:          float = BASE_WINRATE_MIN
    ranging_boost_active: bool  = False
    ranging_qcm_add:      int   = 10
    ranging_eps_add:      int   = 1
    ml_active:            bool  = False
    ml_win_prob_min:      float = ML_WIN_PROB_MIN
    skip_kz:              Dict[str, bool]  = field(default_factory=dict)
    skip_order_type:      Dict[str, bool]  = field(default_factory=dict)
    pattern_boosts:       Dict[str, float] = field(default_factory=dict)


@dataclass
class Signal:
    direction:   str   = "NONE"
    gate_ok:     bool  = False
    veto_rsn:    str   = "WAITING"
    grade:       str   = "STANDARD"
    regime_mode: str   = "LOOSE"

    order:    OrderLevels   = field(default_factory=OrderLevels)
    adaptive: AdaptiveParams = field(default_factory=AdaptiveParams)

    d1_bias:  str = "NEU"
    h4_bias:  str = "NEU"
    h1_bias:  str = "NEU"
    m30_bias: str = "NEU"
    m15_bias: str = "NEU"
    m5_bias:  str = "NEU"
    m1_bias:  str = "NEU"

    bos_bull:       bool  = False
    bos_bear:       bool  = False
    fvg_bull_fresh: bool  = False
    fvg_bear_fresh: bool  = False
    fvg_bull_high:  float = 0.0
    fvg_bull_low:   float = 0.0
    fvg_bear_high:  float = 0.0
    fvg_bear_low:   float = 0.0
    ob_bull_valid:  bool  = False
    ob_bear_valid:  bool  = False
    ob_bull_high:   float = 0.0
    ob_bull_low:    float = 0.0
    ob_bear_high:   float = 0.0
    ob_bear_low:    float = 0.0
    swing_high:     float = 0.0
    swing_low:      float = 0.0
    liq_swept_l:    bool  = False
    liq_swept_h:    bool  = False
    disp_ok:        bool  = False
    sfp_signal:     str   = "NO"
    vol_surge:      bool  = False
    acf_chop:       bool  = False
    pdc_ok:         bool  = False

    eps_layer1_structure: bool = False
    eps_layer2_pdarray:   bool = False
    eps_layer3_momentum:  bool = False
    eps_layer4_micro:     bool = False
    eps_score:            int  = 0

    sqs_score:  float = 0.0
    ctx_score:  int   = 0
    soft_count: int   = 0
    qcm_score:  int   = 0
    cms_score:  float = 0.0

    ttm_fire:  bool = False
    lrsi_ok:   bool = False
    fisher_ok: bool = False
    stc_ok:    bool = False

    fractal_conv: int   = 0
    harmonic_pcz: bool  = False
    vwap_ok:      bool  = True
    pd_type:      str   = "-"
    pd_priority:  int   = 0

    in_killzone: bool = False
    kz_name:     str  = "-"
    kz_start:    str  = "-"
    kz_end:      str  = "-"

    news_ok:   bool = False
    news_tier: int  = 0

    current_price: float = 0.0
    atr_current:   float = 0.0

    ml_win_prob:      float = 0.0
    ml_active:        bool  = False
    pattern_bonus:    float = 0.0
    stat_skip_reason: str   = ""
    is_ranging:       bool  = False


# ═══════════════════════════════════════════════════════════════
# TRADE JOURNAL
# ═══════════════════════════════════════════════════════════════
class TradeJournal:
    def __init__(self, path: Path = JOURNAL_PATH) -> None:
        self.path  = path
        self._lock = threading.Lock()
        self._data = self._load()

    def _load(self) -> Dict:
        if self.path.exists():
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                log.warning("Journal rusak, reset: %s", e)
        return {"trades": [], "version": "22.3"}

    def _save(self) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2, ensure_ascii=False)

    def add_trade(
        self,
        symbol: str, direction: str, entry: float, sl: float,
        tp_hit: str, rr_achieved: float,
        kill_zone: str = "-", order_type: str = "-",
        indicator_combo: List[str] = None,
        qcm: int = 0, eps: int = 0, cms: float = 0.0,
        market_regime: str = "UNKNOWN",
        regime_mode: str = "LOOSE",
    ) -> None:
        with self._lock:
            result = "WIN" if tp_hit.startswith("TP") else "LOSS"
            trade  = {
                "id": len(self._data["trades"]) + 1,
                "symbol": symbol, "direction": direction,
                "entry": entry, "sl": sl,
                "tp_hit": tp_hit, "result": result, "rr_achieved": rr_achieved,
                "kill_zone": kill_zone, "order_type": order_type,
                "indicator_combo": sorted(indicator_combo or []),
                "qcm": qcm, "eps": eps, "cms": cms,
                "market_regime": market_regime,
                "regime_mode": regime_mode,
                "timestamp": datetime.now(WIB).isoformat(),
            }
            self._data["trades"].append(trade)
            self._save()
            log.info("Trade #%d: %s %s @ %.2f → %s", trade["id"], result, direction, entry, tp_hit)

    def get_stats(self, symbol: str = "", last_n: int = 100) -> HistoricalStats:
        with self._lock:
            trades = self._data["trades"]
            if symbol:
                trades = [t for t in trades if t.get("symbol") == symbol]
            return self._compute_stats(trades[-last_n:])

    def get_stats_by_kz(self, symbol: str = "", last_n: int = 200) -> Dict[str, HistoricalStats]:
        with self._lock:
            trades = self._data["trades"]
            if symbol:
                trades = [t for t in trades if t.get("symbol") == symbol]
            trades  = trades[-last_n:]
            by_kz: Dict[str, List] = defaultdict(list)
            for t in trades:
                by_kz[t.get("kill_zone", "-")].append(t)
            return {kz: self._compute_stats(lst) for kz, lst in by_kz.items()}

    def get_stats_by_order_type(self, symbol: str = "", last_n: int = 200) -> Dict[str, HistoricalStats]:
        with self._lock:
            trades = self._data["trades"]
            if symbol:
                trades = [t for t in trades if t.get("symbol") == symbol]
            trades  = trades[-last_n:]
            by_ot: Dict[str, List] = defaultdict(list)
            for t in trades:
                by_ot[t.get("order_type", "-")].append(t)
            return {ot: self._compute_stats(lst) for ot, lst in by_ot.items()}

    def get_all_trades_raw(self, symbol: str = "", last_n: int = 500) -> List[Dict]:
        with self._lock:
            trades = self._data["trades"]
            if symbol:
                trades = [t for t in trades if t.get("symbol") == symbol]
            return trades[-last_n:]

    @staticmethod
    def _compute_stats(trades: List[Dict]) -> HistoricalStats:
        total = len(trades)
        if total == 0:
            return HistoricalStats()
        wins    = [t for t in trades if t.get("result") == "WIN"]
        win_rrs = [t.get("rr_achieved", 0.0) for t in wins]
        return HistoricalStats(
            total=total,
            winrate=round(len(wins) / total * 100, 1),
            avg_rr=round(sum(win_rrs) / len(win_rrs), 2) if win_rrs else 0.0,
        )


# ═══════════════════════════════════════════════════════════════
# STRUCTURE ENGINE  (FIX UTAMA — root cause "zonk")
# ═══════════════════════════════════════════════════════════════
class StructureEngine:
    def __init__(self) -> None:
        pass

    @staticmethod
    def resample(bars_m1: List[Dict], bars_per_candle: int) -> List[Dict]:
        if bars_per_candle <= 1 or len(bars_m1) < bars_per_candle:
            return bars_m1
        out: List[Dict] = []
        n = len(bars_m1)
        start = n % bars_per_candle
        chunk_iter = bars_m1[start:] if start else bars_m1
        for i in range(0, len(chunk_iter), bars_per_candle):
            chunk = chunk_iter[i:i + bars_per_candle]
            if len(chunk) < bars_per_candle:
                continue
            out.append({
                "open":  chunk[0]["open"],
                "high":  max(c["high"] for c in chunk),
                "low":   min(c["low"] for c in chunk),
                "close": chunk[-1]["close"],
                "volume": sum(c.get("volume", 0.0) for c in chunk),
                "datetime": chunk[-1].get("datetime", ""),
            })
        return out

    @staticmethod
    def find_swings(bars: List[Dict], lookback: int) -> Tuple[float, float]:
        n = len(bars)
        if n < (2 * lookback + 1):
            return 0.0, 0.0
        highs = [b["high"] for b in bars]
        lows  = [b["low"]  for b in bars]
        swing_high = 0.0
        swing_low  = 0.0
        for i in range(n - lookback - 1, lookback - 1, -1):
            window_h = highs[i - lookback: i + lookback + 1]
            if highs[i] == max(window_h) and swing_high == 0.0:
                swing_high = highs[i]
            window_l = lows[i - lookback: i + lookback + 1]
            if lows[i] == min(window_l) and swing_low == 0.0:
                swing_low = lows[i]
            if swing_high and swing_low:
                break
        return swing_high, swing_low

    @staticmethod
    def detect_bos(bars: List[Dict], swing_high: float, swing_low: float,
                    lookback_bars: int) -> Tuple[bool, bool]:
        if len(bars) < 3 or (swing_high == 0.0 and swing_low == 0.0):
            return False, False
        recent = bars[-lookback_bars:] if len(bars) >= lookback_bars else bars
        closes = [b["close"] for b in recent]
        bos_bull = swing_high > 0 and any(c > swing_high for c in closes)
        bos_bear = swing_low  > 0 and any(c < swing_low  for c in closes)
        return bos_bull, bos_bear

    @staticmethod
    def detect_liq_sweep(bars: List[Dict], swing_high: float, swing_low: float,
                          lookback_bars: int) -> Tuple[bool, bool]:
        if len(bars) < 2:
            return False, False
        recent = bars[-lookback_bars:] if len(bars) >= lookback_bars else bars
        swept_h = False
        swept_l = False
        for b in recent:
            if swing_high > 0 and b["high"] > swing_high and b["close"] < swing_high:
                swept_h = True
            if swing_low > 0 and b["low"] < swing_low and b["close"] > swing_low:
                swept_l = True
        return swept_l, swept_h

    @staticmethod
    def detect_order_block(bars: List[Dict], bos_bull: bool, bos_bear: bool,
                            max_age: int) -> Tuple[bool, bool, float, float, float, float]:
        n = len(bars)
        if n < 5:
            return False, False, 0.0, 0.0, 0.0, 0.0
        window = bars[-max_age:] if n >= max_age else bars

        ob_bull_valid = False
        ob_bull_high = ob_bull_low = 0.0
        if bos_bull:
            for i in range(len(window) - 2, 0, -1):
                c = window[i]
                if c["close"] < c["open"]:
                    ob_bull_high = c["high"]
                    ob_bull_low  = c["low"]
                    ob_bull_valid = True
                    break

        ob_bear_valid = False
        ob_bear_high = ob_bear_low = 0.0
        if bos_bear:
            for i in range(len(window) - 2, 0, -1):
                c = window[i]
                if c["close"] > c["open"]:
                    ob_bear_high = c["high"]
                    ob_bear_low  = c["low"]
                    ob_bear_valid = True
                    break

        return ob_bull_valid, ob_bear_valid, ob_bull_high, ob_bull_low, ob_bear_high, ob_bear_low

    @staticmethod
    def detect_fvg(bars: List[Dict], atr: float, min_gap_mult: float,
                    max_age: int) -> Tuple[bool, bool, float, float, float, float]:
        """
        FIX v22.3: sekarang balikin koordinat zona gap juga, gak cuma boolean.
        Loop scan dari bar paling baru ke lama (i menurun). Begitu gap valid
        pertama kali ketemu untuk satu arah, koordinatnya di-lock (gak
        ke-overwrite oleh gap yang lebih lama) — itu sebabnya guard
        `and not fvg_bull` / `and not fvg_bear` ditambahkan di kondisinya.
        fvg_*_high selalu > fvg_*_low secara konstruksi (gap_up/gap_dn
        dihitung sebagai selisih positif).
        """
        n = len(bars)
        if n < 3 or atr <= 0:
            return False, False, 0.0, 0.0, 0.0, 0.0
        min_gap = atr * min_gap_mult
        window = bars[-max_age:] if n >= max_age else bars
        fvg_bull = False
        fvg_bear = False
        fvg_bull_high = fvg_bull_low = 0.0
        fvg_bear_high = fvg_bear_low = 0.0
        for i in range(len(window) - 1, 1, -1):
            c0, c1, c2 = window[i - 2], window[i - 1], window[i]
            gap_up = c2["low"] - c0["high"]
            if gap_up > min_gap and not fvg_bull:
                fvg_bull = True
                fvg_bull_high = c2["low"]
                fvg_bull_low  = c0["high"]
            gap_dn = c0["low"] - c2["high"]
            if gap_dn > min_gap and not fvg_bear:
                fvg_bear = True
                fvg_bear_high = c0["low"]
                fvg_bear_low  = c2["high"]
            if fvg_bull and fvg_bear:
                break
        return fvg_bull, fvg_bear, fvg_bull_high, fvg_bull_low, fvg_bear_high, fvg_bear_low

    @staticmethod
    def calc_bias(bars_tf: List[Dict], ema_fast: int, ema_slow: int) -> str:
        if len(bars_tf) < max(MTF_MIN_CANDLES_FOR_BIAS, 2):
            return "NEU"
        closes = [b["close"] for b in bars_tf]
        if len(closes) < ema_slow:
            ema_slow = max(2, len(closes) - 1)
            ema_fast = max(1, ema_slow // 2)
        ef = StructureEngine._ema(closes, ema_fast)
        es = StructureEngine._ema(closes, ema_slow)
        if ef[-1] > es[-1]:
            return "BUL"
        if ef[-1] < es[-1]:
            return "BER"
        return "NEU"

    @staticmethod
    def _ema(data: List[float], period: int) -> List[float]:
        if not data:
            return [0.0]
        period = max(1, min(period, len(data)))
        k = 2.0 / (period + 1)
        result = [0.0] * len(data)
        seed_end = min(period, len(data))
        result[seed_end - 1] = sum(data[:seed_end]) / seed_end
        for i in range(seed_end, len(data)):
            result[i] = data[i] * k + result[i - 1] * (1.0 - k)
        for i in range(seed_end - 1):
            result[i] = result[seed_end - 1]
        return result

    @staticmethod
    def detect_vol_surge(bars: List[Dict], mult: float) -> bool:
        if len(bars) < 21:
            return False
        vols = [b.get("volume", 0.0) for b in bars[-21:]]
        avg  = sum(vols[:-1]) / 20 if len(vols) >= 21 else 0.0
        return avg > 0 and vols[-1] > avg * mult

    @staticmethod
    def detect_chop(bars: List[Dict], atr: float) -> bool:
        if len(bars) < 10 or atr <= 0:
            return False
        last10 = bars[-10:]
        rng = max(b["high"] for b in last10) - min(b["low"] for b in last10)
        return rng < atr * 1.2

    @staticmethod
    def detect_displacement(bars: List[Dict], atr: float) -> bool:
        if len(bars) < 2 or atr <= 0:
            return False
        last = bars[-1]
        body = abs(last["close"] - last["open"])
        return body > atr * 0.8

    def compute(self, bars_m1: List[Dict], direction_hint: str,
                atr: float, params: Dict[str, Any]) -> Dict[str, Any]:
        swing_high, swing_low = self.find_swings(bars_m1, params["swing_lookback"])
        bos_bull, bos_bear = self.detect_bos(bars_m1, swing_high, swing_low, BOS_LOOKBACK_BARS)
        liq_swept_l, liq_swept_h = self.detect_liq_sweep(
            bars_m1, swing_high, swing_low, LIQ_SWEEP_LOOKBACK_BARS)
        (ob_bull_valid, ob_bear_valid, ob_bull_high, ob_bull_low,
         ob_bear_high, ob_bear_low) = self.detect_order_block(
            bars_m1, bos_bull, bos_bear, params["ob_max_age"])
        (fvg_bull_fresh, fvg_bear_fresh, fvg_bull_high, fvg_bull_low,
         fvg_bear_high, fvg_bear_low) = self.detect_fvg(
            bars_m1, atr, params["fvg_min_gap_mult"], params["fvg_max_age"])

        biases: Dict[str, str] = {}
        for tf_name, bars_per_candle in MTF_TIMEFRAMES.items():
            tf_bars = self.resample(bars_m1, bars_per_candle)
            biases[tf_name] = self.calc_bias(tf_bars, params["ema_fast"], params["ema_slow"])

        vol_surge = self.detect_vol_surge(bars_m1, params["vol_surge_mult"])
        acf_chop  = self.detect_chop(bars_m1, atr)
        disp_ok   = self.detect_displacement(bars_m1, atr)

        sfp_signal = "NO"
        if liq_swept_l:
            sfp_signal = "BULL"
        elif liq_swept_h:
            sfp_signal = "BEAR"

        pd_priority = 5 if (ob_bull_valid or ob_bear_valid) else 15

        return {
            "swing_high": round(swing_high, 2), "swing_low": round(swing_low, 2),
            "bos_bull": bos_bull, "bos_bear": bos_bear,
            "liq_swept_l": liq_swept_l, "liq_swept_h": liq_swept_h,
            "ob_bull_valid": ob_bull_valid, "ob_bear_valid": ob_bear_valid,
            "ob_bull_high": round(ob_bull_high, 2), "ob_bull_low": round(ob_bull_low, 2),
            "ob_bear_high": round(ob_bear_high, 2), "ob_bear_low": round(ob_bear_low, 2),
            "fvg_bull_fresh": fvg_bull_fresh, "fvg_bear_fresh": fvg_bear_fresh,
            "fvg_bull_high": round(fvg_bull_high, 2), "fvg_bull_low": round(fvg_bull_low, 2),
            "fvg_bear_high": round(fvg_bear_high, 2), "fvg_bear_low": round(fvg_bear_low, 2),
            "m5_bias": biases["m5"], "m15_bias": biases["m15"], "m30_bias": biases["m30"],
            "h1_bias": biases["h1"], "h4_bias": biases["h4"], "d1_bias": biases["d1"],
            "m1_bias": "BUL" if bars_m1[-1]["close"] > bars_m1[-2]["close"] else "BER" if len(bars_m1) >= 2 else "NEU",
            "vol_surge": vol_surge, "acf_chop": acf_chop, "disp_ok": disp_ok,
            "sfp_signal": sfp_signal, "pd_priority": pd_priority,
            "pd_type": "OB" if (ob_bull_valid or ob_bear_valid) else ("FVG" if (fvg_bull_fresh or fvg_bear_fresh) else "-"),
            "fractal_conv": sum([bos_bull or bos_bear, ob_bull_valid or ob_bear_valid,
                                  fvg_bull_fresh or fvg_bear_fresh, liq_swept_l or liq_swept_h]),
            "harmonic_pcz": False,
        }


structure_engine = StructureEngine()


# ═══════════════════════════════════════════════════════════════
# LEVEL 1: STATISTICAL LEARNER
# ═══════════════════════════════════════════════════════════════
class StatisticalLearner:
    MIN_SAMPLE = 10

    def __init__(self, journal: TradeJournal, symbol: str = "") -> None:
        self.journal = journal
        self.symbol  = symbol

    def compute(self, params: AdaptiveParams) -> AdaptiveParams:
        kz_stats = self.journal.get_stats_by_kz(self.symbol)
        ot_stats = self.journal.get_stats_by_order_type(self.symbol)
        params.skip_kz = {}
        for kz, st in kz_stats.items():
            if st.total >= self.MIN_SAMPLE:
                params.skip_kz[kz] = (st.winrate < params.winrate_min)
                if params.skip_kz[kz]:
                    log.info("L1: Auto-skip KZ '%s' (%.1f%%)", kz, st.winrate)
        params.skip_order_type = {}
        for ot, st in ot_stats.items():
            if st.total >= self.MIN_SAMPLE:
                params.skip_order_type[ot] = (st.winrate < params.winrate_min)
        return params

    def get_summary_lines(self) -> List[str]:
        kz_stats = self.journal.get_stats_by_kz(self.symbol)
        ot_stats = self.journal.get_stats_by_order_type(self.symbol)
        lines = []
        for kz in ["Asian KZ", "London KZ", "NY Open KZ", "London Close"]:
            st   = kz_stats.get(kz, HistoricalStats())
            icon = "⛔" if st.total >= self.MIN_SAMPLE and st.winrate < BASE_WINRATE_MIN else "✅"
            lines.append(f"{icon} {kz}: {st.winrate}% ({st.total}T)")
        for ot in ["BUY LIMIT", "SELL LIMIT", "BUY STOP", "SELL STOP"]:
            st   = ot_stats.get(ot, HistoricalStats())
            icon = "⛔" if st.total >= self.MIN_SAMPLE and st.winrate < BASE_WINRATE_MIN else "✅"
            lines.append(f"{icon} {ot}: {st.winrate}% ({st.total}T)")
        return lines


# ═══════════════════════════════════════════════════════════════
# LEVEL 2: PATTERN RECOGNIZER
# ═══════════════════════════════════════════════════════════════
class PatternRecognizer:
    MIN_COMBO_SAMPLE = 8
    BOOST_THRESHOLD  = 0.65

    def __init__(self, journal: TradeJournal, symbol: str = "") -> None:
        self.journal      = journal
        self.symbol       = symbol
        self._pattern_db: Dict[str, Dict] = self._load_patterns()

    def _load_patterns(self) -> Dict:
        if PATTERN_PATH.exists():
            try:
                with open(PATTERN_PATH, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def _save_patterns(self) -> None:
        with open(PATTERN_PATH, "w", encoding="utf-8") as f:
            json.dump(self._pattern_db, f, indent=2, ensure_ascii=False)

    def train(self) -> None:
        trades = self.journal.get_all_trades_raw(self.symbol)
        combo_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: {"win": 0, "total": 0})
        for t in trades:
            combo = t.get("indicator_combo", [])
            if not combo:
                continue
            key = "|".join(sorted(combo))
            combo_counts[key]["total"] += 1
            if t.get("result") == "WIN":
                combo_counts[key]["win"] += 1
        self._pattern_db = {}
        for key, c in combo_counts.items():
            if c["total"] >= self.MIN_COMBO_SAMPLE:
                wr = c["win"] / c["total"] * 100
                self._pattern_db[key] = {
                    "winrate": round(wr, 1),
                    "total": c["total"],
                    "bonus": round(min((wr - 50) / 50 * 3.0, 3.0), 2) if wr > 50 else 0.0,
                }
        self._save_patterns()

    def get_pattern_bonus(self, active_indicators: List[str]) -> float:
        if not active_indicators:
            return 0.0
        key   = "|".join(sorted(active_indicators))
        entry = self._pattern_db.get(key, {})
        return float(entry.get("bonus", 0.0))

    def detect_ranging(self, cms_score: float, acf_chop: bool) -> bool:
        return acf_chop or (cms_score < 5.5)

    def compute(self, params: AdaptiveParams, sig_data: Dict) -> AdaptiveParams:
        cms        = float(sig_data.get("cms_score", 0.0))
        acf        = bool(sig_data.get("acf_chop", False))
        is_ranging = self.detect_ranging(cms, acf)
        params.ranging_boost_active = is_ranging
        if is_ranging:
            params.qcm_min = BASE_SCALP_QCM_MIN + params.ranging_qcm_add
            params.eps_min = BASE_SCALP_EPS_MIN  + params.ranging_eps_add
        else:
            params.qcm_min = BASE_SCALP_QCM_MIN
            params.eps_min = BASE_SCALP_EPS_MIN
        active_inds     = self._build_active_indicators(sig_data)
        bonus           = self.get_pattern_bonus(active_inds)
        params.pattern_boosts = {"|".join(sorted(active_inds)): bonus}
        return params

    @staticmethod
    def _build_active_indicators(sig_data: Dict) -> List[str]:
        d    = sig_data.get("direction", "BUY")
        bull = d == "BUY"
        inds = []
        if sig_data.get("bos_bull" if bull else "bos_bear"):    inds.append("BOS")
        if sig_data.get("fvg_bull_fresh" if bull else "fvg_bear_fresh"): inds.append("FVG")
        if sig_data.get("ob_bull_valid" if bull else "ob_bear_valid"):   inds.append("OB")
        if sig_data.get("liq_swept_l" if bull else "liq_swept_h"):       inds.append("LIQ")
        if sig_data.get("ttm_fire"):    inds.append("TTM")
        if sig_data.get("lrsi_ok"):     inds.append("LRSI")
        if sig_data.get("fisher_ok"):   inds.append("FISHER")
        if sig_data.get("stc_ok"):      inds.append("STC")
        if sig_data.get("disp_ok"):     inds.append("DISP")
        if sig_data.get("sfp_signal") in ("BULL", "BEAR"): inds.append("SFP")
        if sig_data.get("vol_surge"):   inds.append("VOL")
        if sig_data.get("harmonic_pcz"): inds.append("HARMONIC")
        return inds


# ═══════════════════════════════════════════════════════════════
# LEVEL 3: ML ENGINE
# ═══════════════════════════════════════════════════════════════
class MLEngine:
    FEATURE_NAMES = [
        "eps", "qcm", "cms", "ctx", "soft",
        "ttm_fire", "lrsi_ok", "fisher_ok", "stc_ok",
        "bos", "ob", "fvg", "liq", "disp", "sfp", "vol", "harmonic",
        "in_kz", "is_ranging",
        "d1_bull", "h4_bull", "h1_bull", "m30_bull", "m15_bull", "m5_bull", "m1_bull",
    ]

    def __init__(self, journal: TradeJournal, symbol: str = "") -> None:
        self.journal        = journal
        self.symbol         = symbol
        self._rf            = None
        self._xgb           = None
        self._scaler        = None
        self._trained       = False
        self._last_train_at = 0
        self._lock          = threading.Lock()

    def _extract_features(self, t: Dict) -> Optional[List[float]]:
        try:
            d     = t.get("direction", "BUY")
            tb    = "BUL"
            combo = set(t.get("indicator_combo", []))
            biases = t.get("biases", {})
            return [
                float(t.get("eps", 0)), float(t.get("qcm", 0)),
                float(t.get("cms", 0.0)), float(t.get("ctx", 0)),
                float(t.get("soft", 0)),
                1.0 if "TTM"      in combo else 0.0,
                1.0 if "LRSI"     in combo else 0.0,
                1.0 if "FISHER"   in combo else 0.0,
                1.0 if "STC"      in combo else 0.0,
                1.0 if "BOS"      in combo else 0.0,
                1.0 if "OB"       in combo else 0.0,
                1.0 if "FVG"      in combo else 0.0,
                1.0 if "LIQ"      in combo else 0.0,
                1.0 if "DISP"     in combo else 0.0,
                1.0 if "SFP"      in combo else 0.0,
                1.0 if "VOL"      in combo else 0.0,
                1.0 if "HARMONIC" in combo else 0.0,
                1.0 if t.get("kill_zone", "-") != "-" else 0.0,
                1.0 if t.get("market_regime", "") == "RANGING" else 0.0,
                1.0 if biases.get("d1")  == tb else 0.0,
                1.0 if biases.get("h4")  == tb else 0.0,
                1.0 if biases.get("h1")  == tb else 0.0,
                1.0 if biases.get("m30") == tb else 0.0,
                1.0 if biases.get("m15") == tb else 0.0,
                1.0 if biases.get("m5")  == tb else 0.0,
                1.0 if biases.get("m1")  == tb else 0.0,
            ]
        except Exception:
            return None

    def train(self) -> bool:
        if not ML_AVAILABLE:
            return False
        trades = self.journal.get_all_trades_raw(self.symbol)
        if len(trades) < ML_MIN_SAMPLES:
            return False
        X, y = [], []
        for t in trades:
            f = self._extract_features(t)
            if f:
                X.append(f)
                y.append(1 if t.get("result") == "WIN" else 0)
        if len(X) < ML_MIN_SAMPLES:
            return False
        import numpy as np
        Xa = np.array(X, dtype=np.float32)
        ya = np.array(y, dtype=np.int32)
        with self._lock:
            self._scaler = StandardScaler()
            Xs           = self._scaler.fit_transform(Xa)
            self._rf     = RandomForestClassifier(
                n_estimators=100, max_depth=5, min_samples_leaf=3,
                random_state=42, class_weight="balanced")
            self._rf.fit(Xs, ya)
            if XGB_AVAILABLE:
                self._xgb = XGBClassifier(
                    n_estimators=100, max_depth=4, learning_rate=0.05,
                    subsample=0.8, colsample_bytree=0.8,
                    use_label_encoder=False, eval_metric="logloss",
                    verbosity=0, random_state=42)
                self._xgb.fit(Xa, ya)
            self._trained       = True
            self._last_train_at = len(trades)
        log.info("L3: ML trained — %d samples.", len(X))
        return True

    def predict_win_prob(self, sig_data: Dict) -> float:
        if not ML_AVAILABLE or not self._trained:
            return 0.0
        t_like = {
            "direction": sig_data.get("direction", "BUY"),
            "eps": sig_data.get("eps_score", 0), "qcm": sig_data.get("qcm_score", 0),
            "cms": sig_data.get("cms_score", 0.0), "ctx": sig_data.get("ctx_score", 0),
            "soft": sig_data.get("soft_count", 0),
            "kill_zone": sig_data.get("kz_name", "-"),
            "market_regime": "RANGING" if sig_data.get("acf_chop") else "TRENDING",
            "indicator_combo": PatternRecognizer._build_active_indicators(sig_data),
            "biases": {
                "d1": sig_data.get("d1_bias","NEU"), "h4": sig_data.get("h4_bias","NEU"),
                "h1": sig_data.get("h1_bias","NEU"), "m30": sig_data.get("m30_bias","NEU"),
                "m15": sig_data.get("m15_bias","NEU"), "m5": sig_data.get("m5_bias","NEU"),
                "m1": sig_data.get("m1_bias","NEU"),
            },
        }
        feat = self._extract_features(t_like)
        if not feat:
            return 0.0
        import numpy as np
        fa = np.array([feat], dtype=np.float32)
        probs = []
        with self._lock:
            try:
                probs.append(self._rf.predict_proba(self._scaler.transform(fa))[0][1])
            except Exception:
                pass
            if XGB_AVAILABLE and self._xgb:
                try:
                    probs.append(self._xgb.predict_proba(fa)[0][1])
                except Exception:
                    pass
        return round(sum(probs) / len(probs), 3) if probs else 0.0

    def maybe_retrain(self) -> None:
        trades = self.journal.get_all_trades_raw(self.symbol)
        if len(trades) - self._last_train_at >= ML_RETRAIN_EVERY:
            threading.Thread(target=self.train, daemon=True).start()

    def get_feature_importance(self) -> Optional[Dict[str, float]]:
        if not self._trained or self._rf is None:
            return None
        return dict(zip(self.FEATURE_NAMES,
                        [round(float(v), 4) for v in self._rf.feature_importances_]))


# ═══════════════════════════════════════════════════════════════
# ADAPTIVE CONTROLLER
# ═══════════════════════════════════════════════════════════════
class AdaptiveController:
    def __init__(self, journal: TradeJournal, symbol: str = "") -> None:
        self.journal       = journal
        self.symbol        = symbol
        self._lock         = threading.Lock()
        self.stat_learner  = StatisticalLearner(journal, symbol)
        self.pat_recog     = PatternRecognizer(journal, symbol)
        self.ml_engine     = MLEngine(journal, symbol)
        self._params       = AdaptiveParams()
        self._params_lock  = threading.Lock()
        threading.Thread(target=self._bg_init, daemon=True).start()

    def _bg_init(self) -> None:
        time.sleep(2)
        try:
            self.pat_recog.train()
        except Exception as e:
            log.warning("L2 init: %s", e)
        try:
            self.ml_engine.train()
        except Exception as e:
            log.warning("L3 init: %s", e)

    def get_params(self) -> AdaptiveParams:
        with self._params_lock:
            return self._params

    def update(self, sig_data: Dict) -> Tuple[AdaptiveParams, float]:
        with self._lock:
            params  = AdaptiveParams()
            params  = self.stat_learner.compute(params)
            params  = self.pat_recog.compute(params, sig_data)
            self.ml_engine.maybe_retrain()
            ml_prob = self.ml_engine.predict_win_prob(sig_data)
            params.ml_active     = ML_AVAILABLE and self.ml_engine._trained
            params.ml_win_prob_min = ML_WIN_PROB_MIN
            with self._params_lock:
                self._params = params
            return params, ml_prob

    def record_trade(self, sig: Signal, tp_hit: str, rr_achieved: float, symbol: str) -> None:
        active_inds = PatternRecognizer._build_active_indicators(asdict(sig))
        self.journal.add_trade(
            symbol=symbol, direction=sig.direction,
            entry=sig.order.entry, sl=sig.order.sl,
            tp_hit=tp_hit, rr_achieved=rr_achieved,
            kill_zone=sig.kz_name, order_type=sig.order.order_type,
            indicator_combo=active_inds,
            qcm=sig.qcm_score, eps=sig.eps_score, cms=sig.cms_score,
            market_regime="RANGING" if sig.is_ranging else "TRENDING",
            regime_mode=sig.regime_mode,
        )
        self.pat_recog.train()
        self.ml_engine.maybe_retrain()


# ═══════════════════════════════════════════════════════════════
# ATR WILDER'S RMA
# ═══════════════════════════════════════════════════════════════
def calc_atr_rma(bars: List[Dict], period: int = ATR_PERIOD) -> float:
    if len(bars) < period + 1:
        return 0.0
    tr_s: List[float] = []
    for i in range(1, len(bars)):
        h, l, pc = float(bars[i]["high"]), float(bars[i]["low"]), float(bars[i-1]["close"])
        tr_s.append(max(h-l, abs(h-pc), abs(l-pc)))
    if len(tr_s) < period:
        return 0.0
    atr  = sum(tr_s[:period]) / period
    mult = 1.0 / period
    for tr in tr_s[period:]:
        atr = tr * mult + atr * (1.0 - mult)
    return round(atr, 4)


def _ema_series(data: List[float], period: int) -> List[float]:
    if not data or period <= 0:
        return []
    k        = 2.0 / (period + 1)
    result   = [0.0] * len(data)
    seed_end = min(period, len(data))
    result[seed_end - 1] = sum(data[:seed_end]) / seed_end
    for i in range(seed_end, len(data)):
        result[i] = data[i] * k + result[i-1] * (1.0 - k)
    return result


# ═══════════════════════════════════════════════════════════════
# CME
# ═══════════════════════════════════════════════════════════════
def calc_cms(bars: List[Dict], direction: str) -> Dict:
    empty = {"ttm_fire": False, "lrsi_ok": False,
             "fisher_ok": False, "stc_ok": False, "cms_score": 0.0, "atr": 0.0}
    if not bars or len(bars) < MIN_BARS_CME:
        return empty
    closes = [float(b["close"]) for b in bars]
    highs  = [float(b["high"])  for b in bars]
    lows   = [float(b["low"])   for b in bars]
    n      = len(closes)
    atr14  = calc_atr_rma(bars, ATR_PERIOD)

    bb_w       = closes[-BB_PERIOD:]
    bb_wp      = closes[-(BB_PERIOD+1):-1]
    bb_mid     = sum(bb_w)  / BB_PERIOD
    bb_mid_p   = sum(bb_wp) / BB_PERIOD
    bb_std     = math.sqrt(sum((x-bb_mid)**2 for x in bb_w)/BB_PERIOD)
    squeeze_on = (bb_mid+BB_MULT*bb_std < bb_mid+KC_MULT*atr14) and \
                 (bb_mid-BB_MULT*bb_std > bb_mid-KC_MULT*atr14)
    hn, hp     = closes[-1]-bb_mid, closes[-2]-bb_mid_p
    if direction=="BUY":
        ttm_fire = (not squeeze_on) and (hn>0) and (hn>hp)
    else:
        ttm_fire = (not squeeze_on) and (hn<0) and (hn<hp)

    gamma = LAGUERRE_GAMMA
    L0=L1=L2=L3=closes[0]
    for c in closes:
        nL0=(1-gamma)*c+gamma*L0
        nL1=-gamma*nL0+L0+gamma*L1
        nL2=-gamma*nL1+L1+gamma*L2
        nL3=-gamma*nL2+L2+gamma*L3
        L0,L1,L2,L3=nL0,nL1,nL2,nL3
    cu=max(L0-L1,0)+max(L1-L2,0)+max(L2-L3,0)
    cd=max(L1-L0,0)+max(L2-L1,0)+max(L3-L2,0)
    lrsi   = cu/(cu+cd) if (cu+cd)>1e-10 else 0.5
    lrsi_ok = (lrsi>0.55) if direction=="BUY" else (lrsi<0.45)

    hh=max(highs[-FISHER_PERIOD:]); ll=min(lows[-FISHER_PERIOD:])
    fish=0.0
    if hh!=ll:
        rv  = max(min(2*((closes[-1]-ll)/(hh-ll))-1, 0.999), -0.999)
        fish= 0.5*math.log((1+rv)/(1-rv))
    fisher_ok = (fish<-FISHER_EXTREME and direction=="BUY") or \
                (fish> FISHER_EXTREME and direction=="SELL")

    stc=50.0
    if n>=MIN_BARS_STC:
        ef=_ema_series(closes,STC_FAST); es=_ema_series(closes,STC_SLOW)
        ms=[ef[i]-es[i] for i in range(STC_SLOW-1,n) if es[i]!=0]
        if len(ms)>=STC_CYCLE:
            w=ms[-STC_CYCLE:]; mh,ml=max(w),min(w)
            stc=(ms[-1]-ml)/(mh-ml)*100 if mh!=ml else 50.0
    stc_ok=(stc<STC_OVERSOLD) if direction=="BUY" else (stc>STC_OVERBOUGHT)

    pts  = (3.0 if ttm_fire else 0)+(2.5 if lrsi_ok else 0)+ \
           (2.5 if fisher_ok else 0)+(2.0 if stc_ok else 0)
    return {"ttm_fire":ttm_fire,"lrsi_ok":lrsi_ok,"fisher_ok":fisher_ok,
            "stc_ok":stc_ok,"cms_score":round(min(pts,10.0),2),"atr":atr14}


# ═══════════════════════════════════════════════════════════════
# PENDING ORDER ENGINE
# ═══════════════════════════════════════════════════════════════
class PendingOrderEngine:
    def __init__(self, atr: float) -> None:
        self.atr = max(atr, 0.01)

    def calc(self, sig: Signal) -> OrderLevels:
        if sig.direction not in ("BUY","SELL"):
            return OrderLevels(reason="NO DIRECTION")
        return self._bull(sig) if sig.direction=="BUY" else self._bear(sig)

    def _bull(self, sig: Signal) -> OrderLevels:
        # OB tetap prioritas utama — perilaku lama, gak berubah.
        if sig.ob_bull_valid and sig.ob_bull_low > 0:
            return self._limit("BUY", sig.ob_bull_high, sig.ob_bull_low, sig.swing_low,
                f"BUY LIMIT di OB [{sig.ob_bull_low:.2f}–{sig.ob_bull_high:.2f}]")
        # FIX v22.3: FVG-only sekarang punya koordinat zona sendiri.
        if sig.fvg_bull_fresh and sig.fvg_bull_low > 0:
            return self._limit("BUY", sig.fvg_bull_high, sig.fvg_bull_low, sig.swing_low,
                f"BUY LIMIT di FVG [{sig.fvg_bull_low:.2f}–{sig.fvg_bull_high:.2f}]")
        if sig.swing_high>0 and sig.bos_bull and sig.liq_swept_l:
            return self._stop("BUY", sig.swing_high, sig.swing_low,
                f"BUY STOP breakout [{sig.swing_high:.2f}]")
        return OrderLevels(reason="Tidak ada zona BUY valid", valid=False)

    def _bear(self, sig: Signal) -> OrderLevels:
        if sig.ob_bear_valid and sig.ob_bear_high > 0:
            return self._limit("SELL", sig.ob_bear_high, sig.ob_bear_low, sig.swing_high,
                f"SELL LIMIT di OB [{sig.ob_bear_low:.2f}–{sig.ob_bear_high:.2f}]")
        if sig.fvg_bear_fresh and sig.fvg_bear_high > 0:
            return self._limit("SELL", sig.fvg_bear_high, sig.fvg_bear_low, sig.swing_high,
                f"SELL LIMIT di FVG [{sig.fvg_bear_low:.2f}–{sig.fvg_bear_high:.2f}]")
        if sig.swing_low>0 and sig.bos_bear and sig.liq_swept_h:
            return self._stop("SELL", sig.swing_low, sig.swing_high,
                f"SELL STOP breakdown [{sig.swing_low:.2f}]")
        return OrderLevels(reason="Tidak ada zona SELL valid", valid=False)

    def _limit(self, d, zh, zl, sr, reason) -> OrderLevels:
        atr=self.atr; bull=d=="BUY"
        if bull:
            e=max(zh-atr*OB_ENTRY_ATR_MULT, zl)
            sl=min(zl-SL_SWING_BUFFER, e-atr*SL_ATR_MULT)
        else:
            e=min(zl+atr*OB_ENTRY_ATR_MULT, zh)
            sl=max(zh+SL_SWING_BUFFER, e+atr*SL_ATR_MULT)
        return self._fin(f"{d} LIMIT", d, e, sl, reason)

    def _stop(self, d, level, sr, reason) -> OrderLevels:
        atr=self.atr; bull=d=="BUY"
        if bull:
            e=level+atr*BREAKOUT_ATR_MULT
            sl=min(sr-SL_SWING_BUFFER, e-atr*SL_ATR_MULT)
        else:
            e=level-atr*BREAKOUT_ATR_MULT
            sl=max(sr+SL_SWING_BUFFER, e+atr*SL_ATR_MULT)
        return self._fin(f"{d} STOP", d, e, sl, reason)

    def _fin(self, ot, d, e, sl, reason) -> OrderLevels:
        risk=abs(e-sl)
        if risk<0.01:
            return OrderLevels(reason="Risk terlalu kecil", valid=False)
        s=1 if d=="BUY" else -1
        return OrderLevels(
            order_type=ot, entry=round(e,2), sl=round(sl,2),
            tp1=round(e+s*risk*FIB_TP1,2), tp2=round(e+s*risk*FIB_TP2,2),
            tp3=round(e+s*risk*FIB_TP3,2),
            rr_tp1=FIB_TP1, rr_tp2=FIB_TP2, rr_tp3=FIB_TP3,
            risk_pips=round(risk,2), atr_current=round(self.atr,4),
            reason=reason, valid=True,
        )


# ═══════════════════════════════════════════════════════════════
# EPS & QCM
# ═══════════════════════════════════════════════════════════════
def calc_eps(sig_data: Dict) -> Dict:
    d=sig_data.get("direction","BUY"); bull=d=="BUY"
    l1 = bool(sig_data.get("bos_bull" if bull else "bos_bear")) or \
         (sig_data.get("h1_bias")==("BUL" if bull else "BER"))
    l2 = bool(sig_data.get("ob_bull_valid" if bull else "ob_bear_valid")) or \
         bool(sig_data.get("fvg_bull_fresh" if bull else "fvg_bear_fresh"))
    l3 = float(sig_data.get("cms_score",0.0)) >= BASE_SCALP_CMS_L3
    l4 = (sig_data.get("m1_bias")==("BUL" if bull else "BER")) or \
         (sig_data.get("sfp_signal") in ("BULL","BEAR"))
    return {
        "eps_layer1_structure":l1,"eps_layer2_pdarray":l2,
        "eps_layer3_momentum":l3,"eps_layer4_micro":l4,
        "eps_score":sum([l1,l2,l3,l4]),
    }

_HTF_MAP={3:20,2:14,1:7,0:0}; _MTF_MAP={3:12,2:8,1:4,0:0}

def calc_qcm(sig_data: Dict) -> int:
    d=sig_data.get("direction","BUY"); bull=d=="BUY"; tb="BUL" if bull else "BER"
    s  = _HTF_MAP.get(sum([sig_data.get("d1_bias")==tb,
                            sig_data.get("h4_bias")==tb,
                            sig_data.get("h1_bias")==tb]),0)
    s += _MTF_MAP.get(sum([sig_data.get("m30_bias")==tb,
                            sig_data.get("m15_bias")==tb,
                            sig_data.get("m5_bias")==tb]),0)
    s += max(0,15-max(0,int(sig_data.get("pd_priority",0))))
    s += 5 if sig_data.get("bos_bull" if bull else "bos_bear") else 0
    s += 5 if sig_data.get("liq_swept_l" if bull else "liq_swept_h") else 0
    s += 4 if sig_data.get("disp_ok") else 0
    s += 4 if sig_data.get("sfp_signal") in ("BULL","BEAR") else 0
    s += 5 if sig_data.get("vol_surge") else 0
    s += 3 if not sig_data.get("acf_chop") else 0
    s += 10 if sig_data.get("in_killzone") else 3
    s += int(min(float(sig_data.get("cms_score",0))/10*15, 15))
    return min(s, MAX_QCM)

def calc_grade(eps: int, qcm: int) -> str:
    if eps>=MAX_EPS and qcm>=85: return "PRIME"
    if eps>=3      and qcm>=70:  return "HIGH"
    return "STANDARD"

def check_gate(sig_data: Dict, params: AdaptiveParams) -> Tuple[bool, str]:
    d=sig_data.get("direction","NONE"); bull=d=="BUY"
    eps=int(sig_data.get("eps_score",0)); qcm=int(sig_data.get("qcm_score",0))
    cms=float(sig_data.get("cms_score",0.0))
    if d=="NONE": return False,"NO DIRECTION"
    kz_name=sig_data.get("kz_name","-")
    if params.skip_kz.get(kz_name,False):
        return False,f"L1-SKIP: {kz_name} winrate rendah"
    ot=sig_data.get("order_type_auto","-")
    if params.skip_order_type.get(ot,False):
        return False,f"L1-SKIP: {ot} winrate rendah"
    if not bool(sig_data.get("news_ok",False)):
        tier=int(sig_data.get("news_tier",0))
        if tier>=1: return False,f"NEWS TIER-{tier} BLOCK"
    bos_ok=bool(sig_data.get("bos_bull" if bull else "bos_bear",False))
    htf_ok=(sig_data.get("h4_bias")==("BUL" if bull else "BER") or
             sig_data.get("h1_bias")==("BUL" if bull else "BER"))
    if not bos_ok and not htf_ok: return False,"STRUKTUR HTF BELUM ALIGNED"
    ob_ok=bool(sig_data.get("ob_bull_valid" if bull else "ob_bear_valid",False))
    fvg_ok=bool(sig_data.get("fvg_bull_fresh" if bull else "fvg_bear_fresh",False))
    if not ob_ok and not fvg_ok: return False,"TIDAK ADA PD ARRAY VALID"
    if eps<params.eps_min: return False,f"EPS RENDAH ({eps}/{MAX_EPS})"
    if qcm<params.qcm_min: return False,f"QCM RENDAH ({qcm}/{MAX_QCM})"
    if cms<params.cms_min: return False,f"CMS RENDAH ({cms:.1f}/10)"
    if bool(sig_data.get("acf_chop",False)): return False,"CHOP — MARKET RANGING"
    if not sig_data.get("order_valid",False): return False,"ORDER LEVEL TIDAK VALID"
    if params.ml_active:
        ml_p=float(sig_data.get("ml_win_prob",0.0))
        if ml_p>0 and ml_p<params.ml_win_prob_min:
            return False,f"L3-ML: prob={ml_p:.0%} < {params.ml_win_prob_min:.0%}"
    return True,"OK"


# ═══════════════════════════════════════════════════════════════
# INPUT VALIDATION
# ═══════════════════════════════════════════════════════════════
_BOOL_F  = ("bos_bull","bos_bear","fvg_bull_fresh","fvg_bear_fresh",
            "ob_bull_valid","ob_bear_valid","liq_swept_l","liq_swept_h",
            "disp_ok","vol_surge","acf_chop","pdc_ok","harmonic_pcz","news_ok")
_FLOAT_F = ("ob_bull_high","ob_bull_low","ob_bear_high","ob_bear_low",
            "swing_high","swing_low","current_price","cms_score")
_INT_F   = ("pd_priority","fractal_conv","news_tier")

def _validate_raw(raw: Dict) -> Dict:
    v=dict(raw)
    for f in _BOOL_F:
        if f in v:
            val=v[f]
            if isinstance(val,str): v[f]=val.strip().lower() in("true","1","yes")
            elif not isinstance(val,bool): v[f]=bool(val)
    for f in _FLOAT_F:
        if f in v:
            try: v[f]=float(v[f])
            except: raise TypeError(f"'{f}' harus float")
    for f in _INT_F:
        if f in v:
            try: v[f]=int(v[f])
            except: raise TypeError(f"'{f}' harus int")
    return v


# ═══════════════════════════════════════════════════════════════
# MASTER ANALYZE
# ═══════════════════════════════════════════════════════════════
def analyze(
    raw: Dict, bars: List[Dict], symbol: str,
    adaptive: Optional[AdaptiveController] = None,
    regime: Optional[RegimeAdaptiveMode] = None,
) -> Signal:
    raw = _validate_raw(raw)
    sig = Signal()

    direction = raw.get("direction", "BUY")
    sig.direction = direction

    now_wib=datetime.now(WIB)
    sig.in_killzone, sig.kz_name, sig.kz_start, sig.kz_end = kz_scheduler.check(now_wib)

    mode = regime.get_mode() if regime is not None else REGIME_INITIAL_MODE
    sig.regime_mode = mode
    struct_params = _resolve_struct_params(mode)

    atr_now = calc_atr_rma(bars, ATR_PERIOD)

    struct = structure_engine.compute(bars, direction, atr_now, struct_params)
    for k, v in struct.items():
        setattr(sig, k, v)

    for fn in ("news_ok", "news_tier", "current_price"):
        if fn in raw:
            setattr(sig, fn, raw[fn])

    cme=calc_cms(bars, sig.direction)
    sig.ttm_fire=cme["ttm_fire"]; sig.lrsi_ok=cme["lrsi_ok"]
    sig.fisher_ok=cme["fisher_ok"]; sig.stc_ok=cme["stc_ok"]
    sig.cms_score=cme["cms_score"]; sig.atr_current=cme["atr"] or atr_now

    snap=asdict(sig); eps=calc_eps(snap)
    sig.eps_layer1_structure=eps["eps_layer1_structure"]
    sig.eps_layer2_pdarray=eps["eps_layer2_pdarray"]
    sig.eps_layer3_momentum=eps["eps_layer3_momentum"]
    sig.eps_layer4_micro=eps["eps_layer4_micro"]
    sig.eps_score=eps["eps_score"]

    snap=asdict(sig); sig.qcm_score=calc_qcm(snap)
    sig.sqs_score=round(sig.qcm_score/10.0,1)

    ctx=0
    ctx+=2 if (sig.bos_bull or sig.bos_bear) else 0
    ctx+=2 if (sig.fvg_bull_fresh or sig.fvg_bear_fresh) else 0
    ctx+=2 if (sig.ob_bull_valid or sig.ob_bear_valid) else 0
    ctx+=1 if (sig.liq_swept_l or sig.liq_swept_h) else 0
    ctx+=1 if sig.disp_ok else 0
    sig.ctx_score=min(ctx,MAX_CTX)

    soft=0
    soft+=2 if sig.sfp_signal in("BULL","BEAR") else 0
    soft+=2 if sig.vol_surge else 0
    soft+=1 if not sig.acf_chop else 0
    soft+=1 if sig.pdc_ok else 0
    soft+=1 if sig.harmonic_pcz else 0
    soft+=1 if sig.fractal_conv>=3 else 0
    sig.soft_count=min(soft,MAX_SOFT)

    sig.grade=calc_grade(sig.eps_score,sig.qcm_score)
    sig.is_ranging=sig.acf_chop or (sig.cms_score<5.5)

    sig.order=PendingOrderEngine(atr=sig.atr_current).calc(sig)

    params=AdaptiveParams(); ml_prob=0.0; pat_bonus=0.0
    if adaptive is not None:
        snap_a=asdict(sig)
        params, ml_prob=adaptive.update(snap_a)
        if params.pattern_boosts:
            pat_bonus=max(params.pattern_boosts.values())
            sig.cms_score=min(sig.cms_score+pat_bonus, 10.0)

    sig.adaptive=params; sig.ml_win_prob=ml_prob
    sig.ml_active=params.ml_active; sig.pattern_bonus=pat_bonus

    snap=asdict(sig)
    snap["order_valid"]=sig.order.valid
    snap["order_type_auto"]=sig.order.order_type
    snap["ml_win_prob"]=ml_prob

    if not sig.in_killzone:
        sig.gate_ok=False
        nxt=kz_scheduler.next_killzone(now_wib)
        if nxt:
            sig.veto_rsn=f"OFF-KZ | Next: {nxt['name']} {nxt['start']} (~{nxt['wait_min']}m)"
        else:
            sig.veto_rsn="OFF-KZ"
        return sig

    gate_ok, gate_reason=check_gate(snap, params)
    sig.gate_ok=gate_ok
    sig.veto_rsn=gate_reason if not gate_ok else "OK"
    return sig


def analyze_both_directions(
    bars: List[Dict], symbol: str,
    adaptive: Optional[AdaptiveController] = None,
    regime: Optional[RegimeAdaptiveMode] = None,
    news_ok: bool = True, news_tier: int = 0,
) -> Signal:
    price = bars[-1]["close"] if bars else 0.0
    base_raw = {"current_price": price, "news_ok": news_ok, "news_tier": news_tier}

    sig_buy  = analyze({**base_raw, "direction": "BUY"},  bars, symbol, adaptive, regime)
    sig_sell = analyze({**base_raw, "direction": "SELL"}, bars, symbol, adaptive, regime)

    if sig_buy.gate_ok and not sig_sell.gate_ok:
        return sig_buy
    if sig_sell.gate_ok and not sig_buy.gate_ok:
        return sig_sell
    if sig_buy.gate_ok and sig_sell.gate_ok:
        return sig_buy if sig_buy.qcm_score >= sig_sell.qcm_score else sig_sell
    return sig_buy if sig_buy.qcm_score >= sig_sell.qcm_score else sig_sell


# ═══════════════════════════════════════════════════════════════
# VISUAL HELPERS
# ═══════════════════════════════════════════════════════════════
def _bar(value: float, max_val: float, width: int = 10) -> str:
    filled = int(round(value / max_val * width)) if max_val > 0 else 0
    filled = max(0, min(filled, width))
    return "█" * filled + "░" * (width - filled)

def _grade_stars(grade: str) -> str:
    return {"PRIME":"🔥🔥🔥","HIGH":"⭐⭐","STANDARD":"📶"}.get(grade,"")

def _dir_icon(direction: str) -> str:
    return "🟢" if direction=="BUY" else "🔴" if direction=="SELL" else "⚪"

def _conf(eps, sqs, ctx, soft) -> int:
    return round(
        min(eps/MAX_EPS,1)*30 + min(sqs/MAX_SQS,1)*30 +
        min(ctx/MAX_CTX,1)*20 + min(soft/MAX_SOFT,1)*20
    )

def _cme_line(sig: Signal) -> str:
    def chk(ok): return "✅" if ok else "❌"
    return (
        f"{chk(sig.ttm_fire)} TTM Squeeze\n"
        f"{chk(sig.lrsi_ok)} Laguerre RSI\n"
        f"{chk(sig.fisher_ok)} Fisher Transform\n"
        f"{chk(sig.stc_ok)} Schaff Trend Cycle"
    )

def _eps_layers(sig: Signal) -> str:
    def chk(ok): return "✅" if ok else "❌"
    return (
        f"{chk(sig.eps_layer1_structure)} L1 Structure (BOS/CHoCH)\n"
        f"{chk(sig.eps_layer2_pdarray)}   L2 PD Array (OB/FVG)\n"
        f"{chk(sig.eps_layer3_momentum)}  L3 Momentum (CME)\n"
        f"{chk(sig.eps_layer4_micro)}     L4 Micro (M1/SFP)"
    )

def _bias_table(sig: Signal) -> str:
    d  = sig.direction
    tb = "BUL" if d=="BUY" else "BER"
    def fmt(tf, val):
        icon = "✅" if val==tb else ("⚠️" if val=="NEU" else "❌")
        return f"{icon} {tf}: {val}"
    return "\n".join([
        fmt("D1 ", sig.d1_bias),
        fmt("H4 ", sig.h4_bias),
        fmt("H1 ", sig.h1_bias),
        fmt("M30", sig.m30_bias),
        fmt("M15", sig.m15_bias),
        fmt("M5 ", sig.m5_bias),
        fmt("M1 ", sig.m1_bias),
    ])

def _order_block(o: OrderLevels, d: str) -> str:
    if not o.valid:
        return f"⚠️ {o.reason}"
    d_icon = _dir_icon(d)
    return (
        f"{d_icon} <b>Type</b>   : <code>{o.order_type}</code>\n"
        f"📍 <b>Entry</b>  : <code>{o.entry:.2f}</code>\n\n"
        f"🛑 <b>SL</b>     : <code>{o.sl:.2f}</code>\n"
        f"   Risk    : {o.risk_pips:.1f} pts\n"
        f"   ATR     : {o.atr_current:.2f}\n\n"
        f"🎯 <b>TP1</b> : <code>{o.tp1:.2f}</code>  [1:{o.rr_tp1:.2f}R] Konservatif\n"
        f"🎯 <b>TP2</b> : <code>{o.tp2:.2f}</code>  [1:{o.rr_tp2:.2f}R] Moderat\n"
        f"🎯 <b>TP3</b> : <code>{o.tp3:.2f}</code>  [1:{o.rr_tp3:.2f}R] Agresif"
    )


# ═══════════════════════════════════════════════════════════════
# FORMATTER 1 — HIGH PROBABILITY SIGNAL (gate_ok=True)
# ═══════════════════════════════════════════════════════════════
def fmt_signal_telegram(sig: Signal, symbol: str, stats: HistoricalStats) -> str:
    now_wib = datetime.now(WIB).strftime("%d %b %Y | %H:%M WIB")
    o       = sig.order
    d       = sig.direction
    tb      = "BUL" if d=="BUY" else "BER"
    biases  = [sig.d1_bias,sig.h4_bias,sig.h1_bias,
               sig.m30_bias,sig.m15_bias,sig.m5_bias,sig.m1_bias]
    aligned = sum(1 for b in biases if b==tb)
    conf    = _conf(sig.eps_score, sig.sqs_score, sig.ctx_score, sig.soft_count)

    conf_bar = _bar(conf, 100, 10)

    adapt_lines = []
    p = sig.adaptive
    if p.ranging_boost_active:
        adapt_lines.append("⚠️ Ranging → threshold diperketat")
    if sig.ml_active and sig.ml_win_prob > 0:
        ml_bar = _bar(sig.ml_win_prob, 1.0, 10)
        adapt_lines.append(f"🤖 ML Prob : [{ml_bar}] {sig.ml_win_prob:.0%}")
    if sig.pattern_bonus > 0:
        adapt_lines.append(f"🔮 Pattern Bonus: +{sig.pattern_bonus:.2f} CMS")
    adapt_lines.append(f"⚙️ Regime Mode : {sig.regime_mode}")
    adapt_str = "\n".join(adapt_lines) if adapt_lines else "—"

    remaining = kz_scheduler.current_kz_remaining(datetime.now(WIB))

    SEP = "═" * 33
    return (
        f"{SEP}\n"
        f"🚨 {symbol} HIGH PROBABILITY SIGNAL 🚨\n"
        f"{SEP}\n\n"

        f"⏰ Kill Zone : <b>{sig.kz_name}</b>\n"
        f"   Window   : {sig.kz_start}–{sig.kz_end} WIB\n"
        f"   Sisa     : ±{remaining} menit\n\n"

        f"{SEP}\n\n"

        f"{_order_block(o, d)}\n\n"
        f"📋 {o.reason}\n\n"

        f"{SEP}\n\n"

        f"📊 <b>MOMENTUM (CME)</b>\n\n"
        f"{_cme_line(sig)}\n\n"
        f"CMS : [{_bar(sig.cms_score,10)}] {sig.cms_score:.1f}/10\n\n"

        f"{SEP}\n\n"

        f"🎯 <b>EPS LAYERS</b>\n\n"
        f"{_eps_layers(sig)}\n\n"
        f"EPS Score : {sig.eps_score}/{MAX_EPS} — "
        f"{'🎯 SNIPER' if sig.eps_score==4 else '✅ OK' if sig.eps_score>=2 else '❌ LEMAH'}\n\n"

        f"{SEP}\n\n"

        f"📐 <b>MTF ALIGNMENT</b>\n\n"
        f"{_bias_table(sig)}\n\n"
        f"Aligned : {aligned}/7 TF\n\n"

        f"{SEP}\n\n"

        f"📈 <b>SCORE</b>\n\n"
        f"EPS        : {sig.eps_score}/{MAX_EPS}\n"
        f"SQS        : {sig.sqs_score}/{MAX_SQS}\n"
        f"CTX        : {sig.ctx_score}/{MAX_CTX}\n"
        f"SOFT       : {sig.soft_count}/{MAX_SOFT}\n"
        f"QCM        : {sig.qcm_score}/{MAX_QCM}\n"
        f"Grade      : {sig.grade} {_grade_stars(sig.grade)}\n"
        f"Confidence : [{conf_bar}] {conf}%\n\n"

        f"{SEP}\n\n"

        f"🧠 <b>ADAPTIVE INTEL</b>\n\n"
        f"{adapt_str}\n\n"

        f"{SEP}\n\n"

        f"📚 <b>HISTORICAL</b> ({stats.total} trades)\n\n"
        f"Winrate : {stats.winrate}%  |  Avg RR : {stats.avg_rr}\n\n"

        f"{SEP}\n"
        f"🕒 {now_wib}\n"
        f"<i>PEMIF v22.3 Adaptive Intelligence</i>\n"
        f"{SEP}"
    )


# ═══════════════════════════════════════════════════════════════
# FORMATTER 2 — SCANNING / NO SIGNAL (gate_ok=False, in KZ)
# ═══════════════════════════════════════════════════════════════
def fmt_scanning_telegram(sig: Signal, symbol: str) -> str:
    now_wib   = datetime.now(WIB).strftime("%H:%M WIB")
    conf      = _conf(sig.eps_score, sig.sqs_score, sig.ctx_score, sig.soft_count)
    remaining = kz_scheduler.current_kz_remaining(datetime.now(WIB))

    SEP = "─" * 33

    if sig.order.valid:
        order_str = (
            f"\n{SEP}\n\n"
            f"📋 <b>PENDING ORDER (Pre-calculated)</b>\n\n"
            f"{_order_block(sig.order, sig.direction)}\n\n"
            f"⚠️ Belum memenuhi semua kriteria gate.\n"
            f"   Veto: <i>{sig.veto_rsn}</i>\n"
        )
    else:
        order_str = (
            f"\n{SEP}\n\n"
            f"⚠️ Order level belum valid: {sig.order.reason}\n"
        )

    ml_str = ""
    if sig.ml_active and sig.ml_win_prob > 0:
        ml_bar = _bar(sig.ml_win_prob, 1.0, 10)
        ml_str = f"\n🤖 ML Prob   : [{ml_bar}] {sig.ml_win_prob:.0%}"

    return (
        f"⏳ <b>SCANNING</b> | {symbol} | {now_wib} | Arah dicek: {sig.direction}\n\n"

        f"⏰ Kill Zone : <b>{sig.kz_name}</b>\n"
        f"   Window   : {sig.kz_start}–{sig.kz_end} WIB\n"
        f"   Sisa     : ±{remaining} menit\n"
        f"   Regime   : {sig.regime_mode}\n\n"

        f"{SEP}\n\n"

        f"📊 <b>MOMENTUM (CME)</b>\n\n"
        f"{_cme_line(sig)}\n\n"
        f"CMS : [{_bar(sig.cms_score,10)}] {sig.cms_score:.1f}/10\n\n"

        f"{SEP}\n\n"

        f"🎯 <b>EPS LAYERS</b>\n\n"
        f"{_eps_layers(sig)}\n\n"

        f"{SEP}\n\n"

        f"📈 <b>SCORE</b>\n\n"
        f"EPS        : {sig.eps_score}/{MAX_EPS} "
        f"{'🎯 SNIPER' if sig.eps_score==4 else ''}\n"
        f"SQS        : {sig.sqs_score}/{MAX_SQS}\n"
        f"CTX        : {sig.ctx_score}/{MAX_CTX}\n"
        f"SOFT       : {sig.soft_count}/{MAX_SOFT}\n"
        f"QCM        : {sig.qcm_score}/{MAX_QCM}\n"
        f"CMS        : {sig.cms_score:.1f}/10\n"
        f"Grade      : {sig.grade} {_grade_stars(sig.grade)}\n"
        f"Confidence : [{_bar(conf,100)}] {conf}%"
        f"{ml_str}\n\n"

        f"{SEP}\n\n"

        f"📐 <b>MTF ALIGNMENT</b>\n\n"
        f"{_bias_table(sig)}\n\n"

        f"Status : SCANNING ⏳\n"
        f"Veto   : <i>{sig.veto_rsn}</i>"

        f"{order_str}"
    )


# ═══════════════════════════════════════════════════════════════
# FORMATTER 3 — OFF KILL ZONE
# ═══════════════════════════════════════════════════════════════
def fmt_no_signal_telegram(sig: Signal, symbol: str) -> str:
    now_wib = datetime.now(WIB).strftime("%H:%M WIB")
    nxt     = kz_scheduler.next_killzone(datetime.now(WIB))
    conf    = _conf(sig.eps_score, sig.sqs_score, sig.ctx_score, sig.soft_count)

    nxt_str = ""
    if nxt:
        h, m   = divmod(nxt["wait_min"], 60)
        countdown = f"{h}j {m}m" if h > 0 else f"{m} menit"
        nxt_str = (
            f"\n⏰ <b>Next Kill Zone</b>\n"
            f"   {nxt['name']}\n"
            f"   {nxt['start']} – {nxt['end']}\n"
            f"   Dalam : {countdown}\n"
        )

    order_str = ""
    if sig.order.valid:
        order_str = (
            f"\n─────────────────────────────────\n\n"
            f"📋 <b>Pre-calculated Order</b>\n\n"
            f"{_order_block(sig.order, sig.direction)}\n\n"
            f"<i>Akan aktif saat Kill Zone buka.</i>\n"
        )

    now_windows = kz_scheduler.get_all_windows_wib(datetime.now(WIB))
    dst_tag     = now_windows[0]["dst"] if now_windows else "?"

    return (
        f"🤖 <b>PEMIF v22.3</b> | {symbol} | {now_wib}\n"
        f"Status : OFF Kill Zone ({dst_tag}) | Regime: {sig.regime_mode}\n\n"

        f"─────────────────────────────────\n\n"

        f"📊 Score Terakhir ({sig.direction})\n\n"
        f"EPS : {sig.eps_score}/{MAX_EPS}  QCM : {sig.qcm_score}/{MAX_QCM}\n"
        f"CMS : [{_bar(sig.cms_score,10)}] {sig.cms_score:.1f}/10\n"
        f"Confidence : {conf}%\n\n"

        f"─────────────────────────────────"

        f"{nxt_str}"
        f"{order_str}"
    )


# ═══════════════════════════════════════════════════════════════
# FORMATTER 4 — KILL ZONE SCHEDULE
# ═══════════════════════════════════════════════════════════════
def fmt_kz_schedule(now_wib: datetime) -> str:
    windows = kz_scheduler.get_all_windows_wib(now_wib)
    dst_tag = windows[0]["dst"] if windows else "?"
    in_kz, kz_name, _, _ = kz_scheduler.check(now_wib)
    lines   = [f"📅 <b>Kill Zone Schedule — {dst_tag}</b>\n"]
    for w in windows:
        active = " ← AKTIF" if (in_kz and w["name"]==kz_name) else ""
        lines.append(f"  🕐 {w['name']}: {w['start']} – {w['end']}{active}")
    return "\n".join(lines)


def fmt_regime_switch_telegram(new_mode: str, symbol: str) -> str:
    icon = "🔓 LOOSE" if new_mode == "LOOSE" else "🔒 STRICT"
    reason = ("terlalu lama tanpa sinyal valid dalam Kill Zone"
              if new_mode == "LOOSE" else
              "winrate rolling jatuh di bawah ambang saat mode LOOSE")
    return (
        f"⚙️ <b>REGIME AUTO-SWITCH</b> | {symbol}\n\n"
        f"Mode baru : <b>{icon}</b>\n"
        f"Alasan    : {reason}\n"
        f"🕒 {datetime.now(WIB).strftime('%d %b %Y | %H:%M WIB')}"
    )


# ═══════════════════════════════════════════════════════════════
# TELEGRAM SENDER
# ═══════════════════════════════════════════════════════════════
def send_telegram(
    msg: str,
    max_retry: int = TELEGRAM_MAX_RETRY,
    token: str     = TELEGRAM_TOKEN,
    chat_id: str   = TELEGRAM_CHATID,
) -> bool:
    if not token or not chat_id:
        print(msg)
        return True
    url     = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}
    for attempt in range(1, max_retry + 1):
        try:
            resp = requests.post(url, json=payload, timeout=TELEGRAM_TIMEOUT)
            resp.raise_for_status()
            return True
        except requests.exceptions.HTTPError as exc:
            if exc.response and exc.response.status_code == 429:
                time.sleep(int(exc.response.headers.get("Retry-After", 5)))
                continue
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError) as exc:
            log.warning("Telegram attempt %d/%d: %s", attempt, max_retry, exc)
        if attempt < max_retry:
            time.sleep(TELEGRAM_RETRY_DELAY * (2 ** (attempt-1)))
    log.error("Telegram GAGAL setelah %d attempt.", max_retry)
    return False


# ═══════════════════════════════════════════════════════════════
# PRICE STREAM
# ═══════════════════════════════════════════════════════════════
class PriceStream:
    def __init__(self, symbol, api_key, interval="1min", on_bar_close=None):
        self.symbol=symbol; self.api_key=api_key; self.interval=interval
        self.on_bar_close=on_bar_close
        self._bars: List[Dict]=[]
        self._lock=threading.Lock(); self._stop_evt=threading.Event()
        self._ws_app=None; self._threads: List[threading.Thread]=[]
        self._current_bar=None
        self._bar_sec={"1min":60,"5min":300,"15min":900,"30min":1800,
                       "1h":3600,"4h":14400,"1day":86400}.get(interval,60)

    def start(self):
        self._stop_evt.clear(); self._fetch_initial()
        t=threading.Thread(
            target=self._run_ws if (WS_AVAILABLE and self.api_key) else self._run_rest,
            daemon=True)
        self._threads.append(t); t.start()

    def stop(self):
        self._stop_evt.set()
        if self._ws_app:
            try: self._ws_app.close()
            except: pass

    def get_bars(self, n=MIN_BARS_CME) -> List[Dict]:
        with self._lock: return list(self._bars[-n:])

    def get_latest_price(self) -> float:
        with self._lock: return float(self._bars[-1]["close"]) if self._bars else 0.0

    def _run_ws(self):
        url="wss://ws.twelvedata.com/v1/quotes/price"
        while not self._stop_evt.is_set():
            try:
                self._ws_app=websocket.WebSocketApp(
                    url,
                    on_open=lambda ws: ws.send(json.dumps({
                        "action":"subscribe","params":{
                            "symbols":self.symbol.replace("/",""),
                            "apikey":self.api_key}})),
                    on_message=self._ws_msg,
                    on_error=lambda ws,e: log.warning("WS: %s",e),
                    on_close=lambda ws,c,m: None)
                self._ws_app.run_forever(ping_interval=30,ping_timeout=10)
            except Exception as e:
                log.warning("WS exception: %s",e)
            if not self._stop_evt.is_set():
                time.sleep(WS_RECONNECT_DELAY)

    def _ws_msg(self, ws, message):
        try:
            d=json.loads(message)
            p=float(d.get("price",0))
            if p>0: self._tick(p, float(d.get("timestamp",time.time())))
        except: pass

    def _run_rest(self):
        while not self._stop_evt.is_set():
            try:
                r=requests.get("https://api.twelvedata.com/time_series",
                    params={"symbol":self.symbol,"interval":self.interval,
                            "outputsize":1,"apikey":self.api_key},timeout=10)
                r.raise_for_status()
                vals=r.json().get("values",[])
                if vals: self._append(self._pbar(vals[0]))
            except Exception as e: log.warning("REST: %s",e)
            self._stop_evt.wait(REST_POLL_INTERVAL)

    def _fetch_initial(self):
        if not self.api_key: return
        try:
            r=requests.get("https://api.twelvedata.com/time_series",
                params={"symbol":self.symbol,"interval":self.interval,
                        "outputsize":MIN_BARS_CME+10,"apikey":self.api_key,"order":"ASC"},
                timeout=15)
            r.raise_for_status()
            with self._lock:
                self._bars=[self._pbar(v) for v in r.json().get("values",[])]
            log.info("Initial: %d bars.", len(self._bars))
        except Exception as e: log.error("Fetch initial: %s",e)

    @staticmethod
    def _pbar(v) -> Dict:
        return {k:float(v.get(k,0)) for k in("open","high","low","close","volume")} | \
               {"datetime":v.get("datetime","")}

    def _tick(self, price: float, ts: float):
        bts=int(ts//self._bar_sec)*self._bar_sec
        with self._lock:
            if self._current_bar is None:
                self._current_bar={"open":price,"high":price,"low":price,
                    "close":price,"volume":1.0,"bar_ts":bts,
                    "datetime":datetime.fromtimestamp(bts,tz=WIB).isoformat()}
            elif bts>self._current_bar["bar_ts"]:
                self._bars.append(dict(self._current_bar))
                if len(self._bars)>MIN_BARS_CME*3:
                    self._bars=self._bars[-(MIN_BARS_CME*2):]
                self._current_bar={"open":price,"high":price,"low":price,
                    "close":price,"volume":1.0,"bar_ts":bts,
                    "datetime":datetime.fromtimestamp(bts,tz=WIB).isoformat()}
                if self.on_bar_close and len(self._bars)>=MIN_BARS_CME:
                    snap=list(self._bars[-MIN_BARS_CME:])
                    threading.Thread(target=self.on_bar_close,args=(snap,),daemon=True).start()
            else:
                self._current_bar["high"]=max(self._current_bar["high"],price)
                self._current_bar["low"]=min(self._current_bar["low"],price)
                self._current_bar["close"]=price; self._current_bar["volume"]+=1.0

    def _append(self, bar: Dict):
        with self._lock:
            if not self._bars or bar["datetime"]!=self._bars[-1]["datetime"]:
                self._bars.append(bar)
                if len(self._bars)>MIN_BARS_CME*3:
                    self._bars=self._bars[-(MIN_BARS_CME*2):]
                if self.on_bar_close and len(self._bars)>=MIN_BARS_CME:
                    snap=list(self._bars[-MIN_BARS_CME:])
                    threading.Thread(target=self.on_bar_close,args=(snap,),daemon=True).start()


# ═══════════════════════════════════════════════════════════════
# MAIN ENGINE
# ═══════════════════════════════════════════════════════════════
def run_engine(
    symbol:   str = SYMBOL,
    api_key:  str = TWELVEDATA_KEY,
    interval: str = INTERVAL,
    journal:  Optional[TradeJournal]       = None,
    adaptive: Optional[AdaptiveController] = None,
    regime:   Optional[RegimeAdaptiveMode] = None,
) -> None:
    if journal  is None: journal  = TradeJournal()
    if adaptive is None: adaptive = AdaptiveController(journal, symbol)
    if regime   is None: regime   = RegimeAdaptiveMode(journal, symbol)

    log.info("PEMIF v22.3 starting: %s @ %s | Regime awal=%s", symbol, interval, regime.get_mode())

    send_telegram(fmt_kz_schedule(datetime.now(WIB)))
    send_telegram(f"🚀 PEMIF v22.3 aktif. Regime mode awal: <b>{regime.get_mode()}</b>")

    _last_kz_hour  = {"h": -1}
    _scan_counter  = {"n": 0}
    SCAN_MSG_EVERY = 5

    def on_bar_close(bars: List[Dict]) -> None:
        try:
            now    = datetime.now(WIB)
            stats  = journal.get_stats(symbol=symbol, last_n=100)

            sig = analyze_both_directions(
                bars, symbol, adaptive=adaptive, regime=regime,
                news_ok=True, news_tier=0,
            )

            switched = regime.on_bar(in_killzone=sig.in_killzone, gate_ok=sig.gate_ok)
            if switched:
                send_telegram(fmt_regime_switch_telegram(switched, symbol))

            if now.hour != _last_kz_hour["h"]:
                _last_kz_hour["h"] = now.hour
                send_telegram(fmt_kz_schedule(now))

            if sig.gate_ok and sig.order.valid:
                msg = fmt_signal_telegram(sig, symbol, stats)
                log.info("SIGNAL %s %s entry=%.2f ML=%.0f%% mode=%s",
                         sig.direction, sig.order.order_type,
                         sig.order.entry, sig.ml_win_prob*100, sig.regime_mode)
                send_telegram(msg)
                _scan_counter["n"] = 0

            elif sig.in_killzone:
                _scan_counter["n"] += 1
                if _scan_counter["n"] >= SCAN_MSG_EVERY:
                    msg = fmt_scanning_telegram(sig, symbol)
                    log.info("Scanning: %s EPS=%d QCM=%d CMS=%.1f mode=%s",
                             sig.veto_rsn, sig.eps_score,
                             sig.qcm_score, sig.cms_score, sig.regime_mode)
                    send_telegram(msg)
                    _scan_counter["n"] = 0
                else:
                    log.info("KZ scanning [%d/%d]: %s EPS=%d QCM=%d mode=%s",
                             _scan_counter["n"], SCAN_MSG_EVERY,
                             sig.veto_rsn, sig.eps_score, sig.qcm_score, sig.regime_mode)

            else:
                log.info("OFF-KZ: %s mode=%s", sig.veto_rsn, sig.regime_mode)
                if _scan_counter["n"] % 30 == 0:
                    send_telegram(fmt_no_signal_telegram(sig, symbol))
                _scan_counter["n"] += 1

        except Exception as e:
            log.exception("on_bar_close error: %s", e)

    stream = PriceStream(symbol=symbol, api_key=api_key,
                         interval=interval, on_bar_close=on_bar_close)
    try:
        stream.start()
        log.info("Engine aktif. Ctrl+C untuk stop.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutdown.")
    finally:
        stream.stop()
        log.info("PEMIF v22.3 stopped.")


# ═══════════════════════════════════════════════════════════════
# VISUAL VALIDATOR  (mode --validate, butuh matplotlib/mplfinance/pandas)
# ═══════════════════════════════════════════════════════════════
def _fetch_bars_rest(symbol: str, interval: str, api_key: str, outputsize: int) -> List[Dict]:
    """Fetch bar via REST — sama persis dengan PriceStream._fetch_initial(),
    dipakai khusus oleh validator agar tidak perlu start WebSocket/thread."""
    if not api_key:
        print("❌ TWELVEDATA_KEY tidak di-set di environment variable.")
        sys.exit(1)
    r = requests.get(
        "https://api.twelvedata.com/time_series",
        params={
            "symbol": symbol, "interval": interval,
            "outputsize": outputsize, "apikey": api_key, "order": "ASC",
        },
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    if "values" not in data:
        print(f"❌ Response API tidak ada 'values'. Isi response: {data}")
        sys.exit(1)
    bars = []
    for v in data["values"]:
        bars.append({
            "open":  float(v["open"]),
            "high":  float(v["high"]),
            "low":   float(v["low"]),
            "close": float(v["close"]),
            "volume": float(v.get("volume", 0) or 0),
            "datetime": v["datetime"],
        })
    return bars


def _bars_to_df(bars: List[Dict]):
    df = pd.DataFrame(bars)
    df["datetime"] = pd.to_datetime(df["datetime"])
    df.set_index("datetime", inplace=True)
    df = df.rename(columns={
        "open": "Open", "high": "High", "low": "Low",
        "close": "Close", "volume": "Volume",
    })
    return df[["Open", "High", "Low", "Close", "Volume"]]


def _build_markers(struct: Dict) -> Dict:
    markers = {
        "hlines": [], "hline_colors": [], "hline_styles": [], "hline_labels": [],
        "rects": [],
    }
    if struct["swing_high"] > 0:
        markers["hlines"].append(struct["swing_high"])
        markers["hline_colors"].append("orange")
        markers["hline_styles"].append("--")
        markers["hline_labels"].append(f"Swing High {struct['swing_high']:.2f}")

    if struct["swing_low"] > 0:
        markers["hlines"].append(struct["swing_low"])
        markers["hline_colors"].append("blue")
        markers["hline_styles"].append("--")
        markers["hline_labels"].append(f"Swing Low {struct['swing_low']:.2f}")

    if struct["ob_bull_valid"] and struct["ob_bull_low"] > 0:
        markers["rects"].append({
            "y0": struct["ob_bull_low"], "y1": struct["ob_bull_high"],
            "color": "green", "alpha": 0.18,
            "label": f"OB Bull [{struct['ob_bull_low']:.2f}-{struct['ob_bull_high']:.2f}]",
        })

    if struct["ob_bear_valid"] and struct["ob_bear_high"] > 0:
        markers["rects"].append({
            "y0": struct["ob_bear_low"], "y1": struct["ob_bear_high"],
            "color": "red", "alpha": 0.18,
            "label": f"OB Bear [{struct['ob_bear_low']:.2f}-{struct['ob_bear_high']:.2f}]",
        })

    # FIX v22.3: zona FVG digambar juga, warna beda dari OB biar gak rancu.
    if struct["fvg_bull_fresh"] and struct["fvg_bull_high"] > 0:
        markers["rects"].append({
            "y0": struct["fvg_bull_low"], "y1": struct["fvg_bull_high"],
            "color": "deepskyblue", "alpha": 0.15,
            "label": f"FVG Bull [{struct['fvg_bull_low']:.2f}-{struct['fvg_bull_high']:.2f}]",
        })

    if struct["fvg_bear_fresh"] and struct["fvg_bear_high"] > 0:
        markers["rects"].append({
            "y0": struct["fvg_bear_low"], "y1": struct["fvg_bear_high"],
            "color": "magenta", "alpha": 0.15,
            "label": f"FVG Bear [{struct['fvg_bear_low']:.2f}-{struct['fvg_bear_high']:.2f}]",
        })

    return markers


def _plot_validation(bars: List[Dict], struct: Dict, atr: float,
                      symbol: str, interval: str, regime_mode: str,
                      save_path: Path) -> None:
    df = _bars_to_df(bars)

    fig = mpf.figure(figsize=(16, 9), style="charles")
    ax_main = fig.add_subplot(1, 1, 1)

    markers = _build_markers(struct)

    mpf.plot(
        df, type="candle", ax=ax_main,
        style="charles", show_nontrading=False,
        warn_too_much_data=10000,
    )

    for level, color, style, label in zip(
        markers["hlines"], markers["hline_colors"],
        markers["hline_styles"], markers["hline_labels"],
    ):
        ax_main.axhline(y=level, color=color, linestyle=style, linewidth=1.3, alpha=0.85)
        ax_main.text(
            len(df) - 1, level, f"  {label}",
            color=color, fontsize=9, va="center", fontweight="bold",
        )

    for rect in markers["rects"]:
        ax_main.axhspan(rect["y0"], rect["y1"], color=rect["color"], alpha=rect["alpha"])
        mid_y = (rect["y0"] + rect["y1"]) / 2
        ax_main.text(
            2, mid_y, rect["label"],
            color=rect["color"], fontsize=8, va="center", fontweight="bold",
            bbox=dict(facecolor="white", alpha=0.6, edgecolor="none", pad=1),
        )

    last_idx = len(df) - 1
    if struct["bos_bull"]:
        ax_main.annotate(
            "BOS BULL ▲", xy=(last_idx, df["High"].iloc[-1]),
            xytext=(last_idx, df["High"].iloc[-1] * 1.0015),
            color="lime", fontsize=11, fontweight="bold", ha="center",
            arrowprops=dict(arrowstyle="->", color="lime"),
        )
    if struct["bos_bear"]:
        ax_main.annotate(
            "BOS BEAR ▼", xy=(last_idx, df["Low"].iloc[-1]),
            xytext=(last_idx, df["Low"].iloc[-1] * 0.9985),
            color="red", fontsize=11, fontweight="bold", ha="center",
            arrowprops=dict(arrowstyle="->", color="red"),
        )

    fvg_text = []
    if struct["fvg_bull_fresh"]:
        fvg_text.append(f"FVG Bull: FRESH [{struct['fvg_bull_low']:.2f}-{struct['fvg_bull_high']:.2f}] ✅")
    if struct["fvg_bear_fresh"]:
        fvg_text.append(f"FVG Bear: FRESH [{struct['fvg_bear_low']:.2f}-{struct['fvg_bear_high']:.2f}] ✅")
    if not fvg_text:
        fvg_text.append("FVG: tidak ada gap fresh")

    info_lines = [
        f"Symbol: {symbol}  |  Interval: {interval}  |  Regime: {regime_mode}",
        f"ATR({ATR_PERIOD}): {atr:.4f}",
        f"Swing High: {struct['swing_high']:.2f}   Swing Low: {struct['swing_low']:.2f}",
        f"BOS Bull: {struct['bos_bull']}   BOS Bear: {struct['bos_bear']}",
        f"OB Bull Valid: {struct['ob_bull_valid']}   OB Bear Valid: {struct['ob_bear_valid']}",
        f"Liq Swept L: {struct['liq_swept_l']}   Liq Swept H: {struct['liq_swept_h']}",
        " | ".join(fvg_text),
        f"Bias -> D1:{struct['d1_bias']} H4:{struct['h4_bias']} H1:{struct['h1_bias']} "
        f"M30:{struct['m30_bias']} M15:{struct['m15_bias']} M5:{struct['m5_bias']} M1:{struct['m1_bias']}",
        f"Vol Surge: {struct['vol_surge']}   ACF Chop: {struct['acf_chop']}   Disp OK: {struct['disp_ok']}",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
    ]
    ax_main.text(
        0.01, 0.99, "\n".join(info_lines),
        transform=ax_main.transAxes, fontsize=8.5,
        va="top", ha="left", family="monospace",
        bbox=dict(facecolor="white", alpha=0.85, edgecolor="gray", pad=6),
    )

    ax_main.set_title(
        f"PEMIF Structure Validator — {symbol} ({interval}) — Mode: {regime_mode}",
        fontsize=13, fontweight="bold",
    )

    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    print(f"✅ Chart tersimpan: {save_path.resolve()}")

    plt.show()


def run_validator(
    symbol: str = SYMBOL, api_key: str = TWELVEDATA_KEY, interval: str = INTERVAL,
) -> None:
    """
    Mode --validate: ambil bar LIVE terbaru dari TwelveData (sumber data SAMA
    dengan bot live), jalankan StructureEngine, plot candlestick + marker
    swing/BOS/OB/FVG untuk cross-check manual terhadap TradingView.
    Read-only: TIDAK mengirim Telegram, TIDAK menulis journal/regime state.
    """
    if not PLOT_AVAILABLE:
        print("❌ Library plotting belum lengkap. Jalankan dulu:")
        print("   pip install matplotlib mplfinance pandas")
        sys.exit(1)

    print(f"📡 Mengambil {MIN_BARS_CME + 30} bar terbaru dari TwelveData...")
    bars = _fetch_bars_rest(
        symbol=symbol, interval=interval, api_key=api_key,
        outputsize=MIN_BARS_CME + 30,
    )
    print(f"   Diterima: {len(bars)} bar | Range: {bars[0]['datetime']} -> {bars[-1]['datetime']}")

    if len(bars) < MIN_BARS_CME:
        print(f"❌ Bar tidak cukup ({len(bars)} < {MIN_BARS_CME}). Tidak bisa lanjut.")
        sys.exit(1)

    atr = calc_atr_rma(bars, ATR_PERIOD)
    params = _resolve_struct_params(REGIME_INITIAL_MODE)
    struct = structure_engine.compute(bars, "BUY", atr, params)

    print("\n── Hasil StructureEngine ──")
    for k in ("swing_high", "swing_low", "bos_bull", "bos_bear",
              "ob_bull_valid", "ob_bear_valid", "fvg_bull_fresh", "fvg_bear_fresh",
              "fvg_bull_high", "fvg_bull_low", "fvg_bear_high", "fvg_bear_low",
              "liq_swept_l", "liq_swept_h", "d1_bias", "h4_bias", "h1_bias",
              "m30_bias", "m15_bias", "m5_bias", "m1_bias",
              "vol_surge", "acf_chop", "disp_ok"):
        print(f"   {k:18s}: {struct[k]}")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    save_path = Path(f"pemif_validation_{ts}.png")

    _plot_validation(
        bars=bars, struct=struct, atr=atr,
        symbol=symbol, interval=interval, regime_mode=REGIME_INITIAL_MODE,
        save_path=save_path,
    )


def _quick_diagnostic() -> None:
    """Mode --diag: cek apakah pipeline StructureEngine mengisi nilai
    variatif dari bars live (bukan stuck di default False/0/NEU)."""
    log.info("=== QUICK DIAGNOSTIC START ===")
    bars = _fetch_bars_rest(SYMBOL, INTERVAL, TWELVEDATA_KEY, MIN_BARS_CME + 10)
    log.info("Jumlah bar tersedia: %d (minimum butuh: %d)", len(bars), MIN_BARS_CME)
    if len(bars) < MIN_BARS_CME:
        log.error("❌ STOP DI SINI: bar < %d. CME/EPS/QCM semua akan return kosong/0.", MIN_BARS_CME)
        return

    journal  = TradeJournal()
    adaptive = AdaptiveController(journal, SYMBOL)
    regime   = RegimeAdaptiveMode(journal, SYMBOL)
    sig = analyze_both_directions(bars, SYMBOL, adaptive=adaptive, regime=regime)

    log.info("Regime mode      : %s", sig.regime_mode)
    log.info("ATR current       : %.4f", sig.atr_current)
    log.info("swing_high/low    : %.2f / %.2f", sig.swing_high, sig.swing_low)
    log.info("bos_bull/bear     : %s / %s", sig.bos_bull, sig.bos_bear)
    log.info("ob_bull/bear_valid: %s / %s", sig.ob_bull_valid, sig.ob_bear_valid)
    log.info("fvg_bull/bear     : %s / %s", sig.fvg_bull_fresh, sig.fvg_bear_fresh)
    log.info("fvg_bull zone     : %.2f - %.2f", sig.fvg_bull_low, sig.fvg_bull_high)
    log.info("fvg_bear zone     : %.2f - %.2f", sig.fvg_bear_low, sig.fvg_bear_high)
    log.info("Bias MTF  d1=%s h4=%s h1=%s m30=%s m15=%s m5=%s m1=%s",
              sig.d1_bias, sig.h4_bias, sig.h1_bias, sig.m30_bias, sig.m15_bias, sig.m5_bias, sig.m1_bias)
    log.info("eps_score=%d  qcm_score=%d  cms_score=%.1f", sig.eps_score, sig.qcm_score, sig.cms_score)
    log.info("in_killzone=%s kz_name=%s", sig.in_killzone, sig.kz_name)
    log.info("gate_ok=%s  veto_rsn=%s", sig.gate_ok, sig.veto_rsn)
    log.info("=== QUICK DIAGNOSTIC END ===")


# ═══════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    if "--validate" in sys.argv:
        run_validator(symbol=SYMBOL, api_key=TWELVEDATA_KEY, interval=INTERVAL)
    elif "--diag" in sys.argv:
        _quick_diagnostic()
    else:
        run_engine(symbol=SYMBOL, api_key=TWELVEDATA_KEY, interval=INTERVAL)
