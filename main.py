#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PEMIF v22.0 ADAPTIVE INTELLIGENCE ENGINE
════════════════════════════════════════════════════════════════
Upgrade dari v21.0:
  ┌─ Level 1: Statistical Learning ─────────────────────────┐
  │  • Winrate per Kill Zone                                 │
  │  • Winrate per Order Type                                │
  │  • Auto-skip setup winrate < 50%                        │
  └──────────────────────────────────────────────────────────┘
  ┌─ Level 2: Pattern Recognition ──────────────────────────┐
  │  • Deteksi kombinasi indikator paling profit             │
  │  • Auto-boost QCM/EPS threshold saat ranging             │
  └──────────────────────────────────────────────────────────┘
  ┌─ Level 3: ML Engine ────────────────────────────────────┐
  │  • Random Forest / XGBoost                               │
  │  • Prediksi probabilitas win sebelum entry               │
  │  • Auto-adjust semua parameter                           │
  └──────────────────────────────────────────────────────────┘
  ┌─ Kill Zone ICT Akurat (DST-Aware) ──────────────────────┐
  │  • Asian KZ     : 07:00–11:00 WIB (DST) / 08:00–12:00  │
  │  • London KZ    : 13:00–16:00 WIB (DST) / 14:00–17:00  │
  │  • NY Open KZ   : 18:00–21:00 WIB (DST) / 19:00–22:00  │
  │  • London Close : 21:00–23:00 WIB (DST) / 22:00–00:00  │
  └──────────────────────────────────────────────────────────┘

Python  : >= 3.9
Deps    : pip install requests websocket-client scikit-learn xgboost
Author  : PEMIF Engine v22.0
"""
from __future__ import annotations

import json
import logging
import math
import os
import queue
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

# ── Optional deps ─────────────────────────────────────────────
try:
    import websocket
    WS_AVAILABLE = True
except ImportError:
    WS_AVAILABLE = False

try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import cross_val_score
    import numpy as np
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False

try:
    from xgboost import XGBClassifier
    XGB_AVAILABLE = True
except ImportError:
    XGB_AVAILABLE = False

__all__ = [
    "Signal", "HistoricalStats", "TradeJournal",
    "PriceStream", "PendingOrderEngine",
    "StatisticalLearner", "PatternRecognizer", "MLEngine",
    "AdaptiveController", "KillZoneScheduler",
    "analyze", "fmt_signal_telegram",
    "send_telegram", "run_engine",
]

# ═══════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("PEMIF-v22.0")

# ═══════════════════════════════════════════════════════════════
# CONSTANTS — BASE (sebelum adaptive override)
# ═══════════════════════════════════════════════════════════════
WIB = timezone(timedelta(hours=7))

MAX_EPS:  int   = 4
MAX_SQS:  float = 10.0
MAX_CTX:  int   = 8
MAX_SOFT: int   = 8
MAX_QCM:  int   = 100

# Base thresholds (akan di-override AdaptiveController)
BASE_SCALP_EPS_MIN:   int   = 2
BASE_SCALP_QCM_MIN:   int   = 55
BASE_SCALP_CMS_MIN:   float = 3.5
BASE_SCALP_CMS_L3:    float = 3.5
BASE_WINRATE_MIN:     float = 50.0   # auto-skip < 50%

# Momentum
LAGUERRE_GAMMA:  float = 0.5
BB_PERIOD:       int   = 20
BB_MULT:         float = 2.0
KC_MULT:         float = 1.5
ATR_PERIOD:      int   = 14
FISHER_PERIOD:   int   = 9
FISHER_EXTREME:  float = 1.5
STC_FAST:        int   = 23
STC_SLOW:        int   = 50
STC_CYCLE:       int   = 10
STC_OVERSOLD:    float = 25.0
STC_OVERBOUGHT:  float = 75.0

# Entry / SL / TP
OB_ENTRY_ATR_MULT: float = 0.3
BREAKOUT_ATR_MULT: float = 0.1
SL_ATR_MULT:       float = 1.5
SL_SWING_BUFFER:   float = 0.3
FIB_TP1: float = 1.0
FIB_TP2: float = 1.618
FIB_TP3: float = 2.618

# Bar counts
MIN_BARS_CME: int = 60
MIN_BARS_STC: int = 55

# Stream
WS_RECONNECT_DELAY: float = 5.0
REST_POLL_INTERVAL: float = 10.0
TICK_BUFFER_SIZE:   int   = 500

# Telegram
TELEGRAM_MAX_RETRY:   int   = 3
TELEGRAM_RETRY_DELAY: float = 1.5
TELEGRAM_TIMEOUT:     int   = 10

# Paths
JOURNAL_PATH:    Path = Path("pemif_trade_journal.json")
PATTERN_PATH:    Path = Path("pemif_patterns.json")
ML_MODEL_PATH:   Path = Path("pemif_ml_model.json")
ADAPTIVE_PATH:   Path = Path("pemif_adaptive_params.json")

# ML
ML_MIN_SAMPLES:  int = 30    # minimum trade sebelum ML aktif
ML_RETRAIN_EVERY: int = 10   # retrain tiap N trade baru
ML_WIN_PROB_MIN: float = 0.60  # probabilitas minimal dari ML

# Weekdays aktif
TRADING_WEEKDAYS: Tuple[int, ...] = (0, 1, 2, 3, 4)

# ─── DST US Eastern: DST aktif Maret minggu ke-2 s/d November minggu ke-1
# Offset NY ke WIB:
#   DST  (EDT, UTC-4) → WIB = UTC+7 → selisih +11 jam
#   Non-DST (EST, UTC-5) → WIB = UTC+7 → selisih +12 jam
# Kill Zone dalam NY (EST), dikonversi ke WIB otomatis di KillZoneScheduler

# Kill Zone definisi dalam NY hour (24h)
KILL_ZONES_NY: Tuple[Dict, ...] = (
    {"name": "Asian KZ",      "ny_start": (20, 0),  "ny_end": (0,  0),  "next_day_end": True},
    {"name": "London KZ",     "ny_start": (2,  0),  "ny_end": (5,  0),  "next_day_end": False},
    {"name": "NY Open KZ",    "ny_start": (7,  0),  "ny_end": (10, 0),  "next_day_end": False},
    {"name": "London Close",  "ny_start": (10, 0),  "ny_end": (12, 0),  "next_day_end": False},
)


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
    """Deteksi apakah US Eastern DST aktif pada tanggal dt.

    DST aktif: Minggu ke-2 Maret (02:00) → Minggu ke-1 November (02:00).
    Berlaku mulai 2007 (Energy Policy Act 2005).
    """
    y = dt.year
    # Minggu ke-2 Maret
    march1 = date(y, 3, 1)
    days_to_sun = (6 - march1.weekday()) % 7   # hari ke-Minggu pertama
    dst_start = date(y, 3, 1 + days_to_sun + 7)   # +7 = minggu ke-2

    # Minggu ke-1 November
    nov1 = date(y, 11, 1)
    days_to_sun_nov = (6 - nov1.weekday()) % 7
    dst_end = date(y, 11, 1 + days_to_sun_nov)

    d = dt.date()
    return dst_start <= d < dst_end


def _ny_to_wib_offset(dt: datetime) -> int:
    """Return selisih jam NY → WIB (jam yang ditambahkan ke NY untuk dapat WIB)."""
    return 11 if _us_dst_active(dt) else 12


# ═══════════════════════════════════════════════════════════════
# KILL ZONE SCHEDULER (DST-Aware)
# ═══════════════════════════════════════════════════════════════
class KillZoneScheduler:
    """Kill Zone ICT akurat dengan konversi NY ↔ WIB DST-aware.

    Menghitung window aktif dalam WIB secara real-time berdasarkan
    apakah US Eastern sedang DST (EDT) atau non-DST (EST).
    """

    def check(self, now_wib: datetime) -> Tuple[bool, str, str, str]:
        """Periksa apakah now_wib berada dalam Kill Zone.

        Returns:
            (in_kz, kz_name, wib_start_str, wib_end_str)
        """
        if now_wib.weekday() not in TRADING_WEEKDAYS:
            return False, "WEEKEND", "-", "-"

        offset = _ny_to_wib_offset(now_wib)

        for kz in KILL_ZONES_NY:
            sh, sm = kz["ny_start"]
            eh, em = kz["ny_end"]
            is_next = kz["next_day_end"]

            wib_sh = (sh + offset) % 24
            wib_sm = sm
            wib_eh = (eh + offset) % 24
            wib_em = em

            # Total minutes dalam WIB
            cur_min = now_wib.hour * 60 + now_wib.minute
            start_m = wib_sh * 60 + wib_sm
            end_m   = wib_eh * 60 + wib_em

            in_kz = False
            if is_next or start_m > end_m:
                # Melewati tengah malam
                in_kz = (cur_min >= start_m) or (cur_min < end_m)
            else:
                in_kz = (start_m <= cur_min < end_m)

            if in_kz:
                start_str = f"{wib_sh:02d}:{wib_sm:02d}"
                end_str   = f"{wib_eh:02d}:{wib_em:02d}"
                return True, kz["name"], start_str, end_str

        return False, "-", "-", "-"

    def get_all_windows_wib(self, now_wib: datetime) -> List[Dict]:
        """Return semua window Kill Zone hari ini dalam WIB (untuk info/log)."""
        offset = _ny_to_wib_offset(now_wib)
        result = []
        for kz in KILL_ZONES_NY:
            sh, sm = kz["ny_start"]
            eh, em = kz["ny_end"]
            wib_sh = (sh + offset) % 24
            wib_eh = (eh + offset) % 24
            dst_tag = "DST(EDT)" if _us_dst_active(now_wib) else "Non-DST(EST)"
            result.append({
                "name":  kz["name"],
                "start": f"{wib_sh:02d}:{sm:02d} WIB",
                "end":   f"{wib_eh:02d}:{em:02d} WIB",
                "dst":   dst_tag,
            })
        return result

    def next_killzone(self, now_wib: datetime) -> Optional[Dict]:
        """Cari Kill Zone berikutnya yang belum aktif."""
        offset  = _ny_to_wib_offset(now_wib)
        cur_min = now_wib.hour * 60 + now_wib.minute
        best    = None
        best_wait = 99999

        for kz in KILL_ZONES_NY:
            sh, sm = kz["ny_start"]
            wib_sh = (sh + offset) % 24
            start_m = wib_sh * 60 + sm
            wait = (start_m - cur_min) % (24 * 60)
            if 0 < wait < best_wait:
                best_wait = wait
                eh, em   = kz["ny_end"]
                wib_eh   = (eh + offset) % 24
                best = {
                    "name": kz["name"],
                    "start": f"{wib_sh:02d}:{sm:02d} WIB",
                    "end":   f"{wib_eh:02d}:{em:02d} WIB",
                    "wait_min": wait,
                }
        return best


# Singleton
kz_scheduler = KillZoneScheduler()


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
    """Parameter yang di-adjust secara adaptif oleh 3 level learning."""
    # Base thresholds
    eps_min:    int   = BASE_SCALP_EPS_MIN
    qcm_min:    int   = BASE_SCALP_QCM_MIN
    cms_min:    float = BASE_SCALP_CMS_MIN
    winrate_min: float = BASE_WINRATE_MIN

    # Ranging market boost
    ranging_boost_active: bool  = False
    ranging_qcm_add:      int   = 10   # tambah 10 poin ke qcm_min saat ranging
    ranging_eps_add:      int   = 1    # tambah 1 ke eps_min saat ranging

    # ML override
    ml_active:         bool  = False
    ml_win_prob_min:   float = ML_WIN_PROB_MIN

    # Auto-skip flags
    skip_kz:           Dict[str, bool]  = field(default_factory=dict)
    skip_order_type:   Dict[str, bool]  = field(default_factory=dict)

    # Pattern boosts (combo → score_add)
    pattern_boosts: Dict[str, float] = field(default_factory=dict)


@dataclass
class Signal:
    """Representasi sinyal PEMIF v22.0."""
    direction:   str   = "NONE"
    gate_ok:     bool  = False
    veto_rsn:    str   = "WAITING"
    grade:       str   = "STANDARD"

    order:       OrderLevels  = field(default_factory=OrderLevels)
    adaptive:    AdaptiveParams = field(default_factory=AdaptiveParams)

    # Bias per TF
    d1_bias:  str = "NEU"
    h4_bias:  str = "NEU"
    h1_bias:  str = "NEU"
    m30_bias: str = "NEU"
    m15_bias: str = "NEU"
    m5_bias:  str = "NEU"
    m1_bias:  str = "NEU"

    # Structure
    bos_bull:        bool  = False
    bos_bear:        bool  = False
    fvg_bull_fresh:  bool  = False
    fvg_bear_fresh:  bool  = False
    ob_bull_valid:   bool  = False
    ob_bear_valid:   bool  = False
    ob_bull_high:    float = 0.0
    ob_bull_low:     float = 0.0
    ob_bear_high:    float = 0.0
    ob_bear_low:     float = 0.0
    swing_high:      float = 0.0
    swing_low:       float = 0.0
    liq_swept_l:     bool  = False
    liq_swept_h:     bool  = False
    disp_ok:         bool  = False
    sfp_signal:      str   = "NO"
    vol_surge:       bool  = False
    acf_chop:        bool  = False
    pdc_ok:          bool  = False

    # EPS
    eps_layer1_structure: bool = False
    eps_layer2_pdarray:   bool = False
    eps_layer3_momentum:  bool = False
    eps_layer4_micro:     bool = False
    eps_score:            int  = 0

    # Scores
    sqs_score:    float = 0.0
    ctx_score:    int   = 0
    soft_count:   int   = 0
    qcm_score:    int   = 0
    cms_score:    float = 0.0

    # CME
    ttm_fire:   bool = False
    lrsi_ok:    bool = False
    fisher_ok:  bool = False
    stc_ok:     bool = False

    # Extended
    fractal_conv:  int   = 0
    harmonic_pcz:  bool  = False
    vwap_ok:       bool  = True
    pd_type:       str   = "-"
    pd_priority:   int   = 0

    # Kill Zone
    in_killzone:  bool = False
    kz_name:      str  = "-"
    kz_start:     str  = "-"
    kz_end:       str  = "-"

    # News
    news_ok:   bool = False
    news_tier: int  = 0

    # Market data
    current_price: float = 0.0
    atr_current:   float = 0.0

    # Learning outputs
    ml_win_prob:      float = 0.0
    ml_active:        bool  = False
    pattern_bonus:    float = 0.0
    stat_skip_reason: str   = ""
    is_ranging:       bool  = False


# ═══════════════════════════════════════════════════════════════
# TRADE JOURNAL — Persistent JSON (Extended untuk Learning)
# ═══════════════════════════════════════════════════════════════
class TradeJournal:
    """Persistent trade journal — extended dengan metadata untuk 3-level learning.

    Setiap trade entry menyimpan:
        • kill_zone       : nama KZ saat entry
        • order_type      : BUY LIMIT / SELL LIMIT / BUY STOP / SELL STOP
        • indicator_combo : frozen set indikator aktif saat entry
        • qcm / eps / cms : skor saat entry
        • result / rr_achieved
        • market_regime   : "TRENDING" / "RANGING"
    """

    def __init__(self, path: Path = JOURNAL_PATH) -> None:
        self.path  = path
        self._lock = threading.Lock()
        self._data = self._load()

    def _load(self) -> Dict:
        if self.path.exists():
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError) as e:
                log.warning("Journal rusak, reset: %s", e)
        return {"trades": [], "version": "22.0"}

    def _save(self) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2, ensure_ascii=False)

    def add_trade(
        self,
        symbol:      str,
        direction:   str,
        entry:       float,
        sl:          float,
        tp_hit:      str,
        rr_achieved: float,
        kill_zone:   str   = "-",
        order_type:  str   = "-",
        indicator_combo: List[str] = None,
        qcm:         int   = 0,
        eps:         int   = 0,
        cms:         float = 0.0,
        market_regime: str = "UNKNOWN",
    ) -> None:
        with self._lock:
            result = "WIN" if tp_hit.startswith("TP") else "LOSS"
            trade  = {
                "id":              len(self._data["trades"]) + 1,
                "symbol":          symbol,
                "direction":       direction,
                "entry":           entry,
                "sl":              sl,
                "tp_hit":          tp_hit,
                "result":          result,
                "rr_achieved":     rr_achieved,
                "kill_zone":       kill_zone,
                "order_type":      order_type,
                "indicator_combo": sorted(indicator_combo or []),
                "qcm":             qcm,
                "eps":             eps,
                "cms":             cms,
                "market_regime":   market_regime,
                "timestamp":       datetime.now(WIB).isoformat(),
            }
            self._data["trades"].append(trade)
            self._save()
            log.info("Trade #%d: %s %s @ %.2f → %s", trade["id"], result, direction, entry, tp_hit)

    def get_stats(self, symbol: str = "", last_n: int = 100) -> HistoricalStats:
        with self._lock:
            trades = self._data["trades"]
            if symbol:
                trades = [t for t in trades if t.get("symbol") == symbol]
            trades = trades[-last_n:]
            return self._compute_stats(trades)

    def get_stats_by_kz(self, symbol: str = "", last_n: int = 200) -> Dict[str, HistoricalStats]:
        """Winrate per Kill Zone."""
        with self._lock:
            trades = self._data["trades"]
            if symbol:
                trades = [t for t in trades if t.get("symbol") == symbol]
            trades = trades[-last_n:]

            by_kz: Dict[str, List] = defaultdict(list)
            for t in trades:
                by_kz[t.get("kill_zone", "-")].append(t)

            return {kz: self._compute_stats(lst) for kz, lst in by_kz.items()}

    def get_stats_by_order_type(self, symbol: str = "", last_n: int = 200) -> Dict[str, HistoricalStats]:
        """Winrate per order type."""
        with self._lock:
            trades = self._data["trades"]
            if symbol:
                trades = [t for t in trades if t.get("symbol") == symbol]
            trades = trades[-last_n:]

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
# LEVEL 1: STATISTICAL LEARNER
# ═══════════════════════════════════════════════════════════════
class StatisticalLearner:
    """Level 1 — Statistik sederhana dari trade journal nyata.

    Fungsi:
        1. Hitung winrate per Kill Zone
        2. Hitung winrate per order type
        3. Flag skip jika winrate < threshold
        4. Update AdaptiveParams.skip_kz dan skip_order_type
    """

    MIN_SAMPLE = 10   # minimum trade sebelum skip aktif

    def __init__(self, journal: TradeJournal, symbol: str = "") -> None:
        self.journal = journal
        self.symbol  = symbol

    def compute(self, params: AdaptiveParams) -> AdaptiveParams:
        """Update params.skip_kz dan skip_order_type dari statistik journal."""
        kz_stats = self.journal.get_stats_by_kz(self.symbol)
        ot_stats = self.journal.get_stats_by_order_type(self.symbol)

        # Skip Kill Zone
        params.skip_kz = {}
        for kz, st in kz_stats.items():
            if st.total >= self.MIN_SAMPLE:
                params.skip_kz[kz] = (st.winrate < params.winrate_min)
                if params.skip_kz[kz]:
                    log.info("L1: Auto-skip KZ '%s' (winrate=%.1f%% < %.1f%%)",
                             kz, st.winrate, params.winrate_min)

        # Skip order type
        params.skip_order_type = {}
        for ot, st in ot_stats.items():
            if st.total >= self.MIN_SAMPLE:
                params.skip_order_type[ot] = (st.winrate < params.winrate_min)
                if params.skip_order_type[ot]:
                    log.info("L1: Auto-skip order_type '%s' (winrate=%.1f%%)",
                             ot, st.winrate)

        return params

    def get_summary(self) -> str:
        """Return ringkasan statistik untuk Telegram."""
        kz_stats = self.journal.get_stats_by_kz(self.symbol)
        ot_stats = self.journal.get_stats_by_order_type(self.symbol)

        lines = ["📊 <b>L1 STATISTICAL LEARNING</b>\n"]

        lines.append("<b>Winrate per Kill Zone:</b>")
        for kz in ["Asian KZ", "London KZ", "NY Open KZ", "London Close"]:
            st = kz_stats.get(kz, HistoricalStats())
            skip = "⛔" if st.total >= self.MIN_SAMPLE and st.winrate < BASE_WINRATE_MIN else "✅"
            lines.append(f"  {skip} {kz}: {st.winrate}% ({st.total}T)")

        lines.append("\n<b>Winrate per Order Type:</b>")
        for ot in ["BUY LIMIT", "SELL LIMIT", "BUY STOP", "SELL STOP"]:
            st = ot_stats.get(ot, HistoricalStats())
            skip = "⛔" if st.total >= self.MIN_SAMPLE and st.winrate < BASE_WINRATE_MIN else "✅"
            lines.append(f"  {skip} {ot}: {st.winrate}% ({st.total}T)")

        return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# LEVEL 2: PATTERN RECOGNIZER
# ═══════════════════════════════════════════════════════════════
class PatternRecognizer:
    """Level 2 — Pattern recognition dari kombinasi indikator.

    Fungsi:
        1. Deteksi kombinasi indikator yang paling sering profit
        2. Auto-boost QCM/EPS threshold saat market ranging
        3. Assign bonus score ke setup dengan pattern tinggi

    Pattern key: tuple sorted dari indikator aktif, contoh:
        ("BOS", "FVG", "LRSI", "TTM")
    """

    MIN_COMBO_SAMPLE = 8   # minimum kemunculan combo sebelum dianggap valid
    BOOST_THRESHOLD  = 0.65  # winrate combo > 65% → bonus
    RANGE_THRESHOLD  = 0.55  # ADX proxy: cms < ini → possibly ranging

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
        """Bangun pattern database dari trade journal."""
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
        for key, counts in combo_counts.items():
            tot = counts["total"]
            win = counts["win"]
            if tot >= self.MIN_COMBO_SAMPLE:
                wr  = win / tot * 100
                self._pattern_db[key] = {
                    "winrate": round(wr, 1),
                    "total":   tot,
                    "wins":    win,
                    "bonus":   round(min((wr - 50) / 50 * 3.0, 3.0), 2) if wr > 50 else 0.0,
                }

        self._save_patterns()
        log.info("L2: Pattern DB diperbarui — %d combo aktif.", len(self._pattern_db))

    def get_pattern_bonus(self, active_indicators: List[str]) -> float:
        """Return bonus CMS score untuk kombinasi indikator aktif."""
        if not active_indicators:
            return 0.0
        key = "|".join(sorted(active_indicators))
        entry = self._pattern_db.get(key, {})
        return float(entry.get("bonus", 0.0))

    def detect_ranging(self, cms_score: float, acf_chop: bool) -> bool:
        """Proxy deteksi ranging market dari CMS + ACF Chop."""
        return acf_chop or (cms_score < self.RANGE_THRESHOLD * 10)

    def compute(self, params: AdaptiveParams, sig_data: Dict) -> AdaptiveParams:
        """Update params berdasarkan pattern detection."""
        cms        = float(sig_data.get("cms_score", 0.0))
        acf        = bool(sig_data.get("acf_chop", False))
        is_ranging = self.detect_ranging(cms, acf)

        params.ranging_boost_active = is_ranging
        if is_ranging:
            params.qcm_min = BASE_SCALP_QCM_MIN + params.ranging_qcm_add
            params.eps_min = BASE_SCALP_EPS_MIN  + params.ranging_eps_add
            log.info("L2: Ranging terdeteksi → QCM threshold +%d, EPS +%d",
                     params.ranging_qcm_add, params.ranging_eps_add)
        else:
            params.qcm_min = BASE_SCALP_QCM_MIN
            params.eps_min = BASE_SCALP_EPS_MIN

        # Pattern bonus
        active_inds = self._build_active_indicators(sig_data)
        bonus       = self.get_pattern_bonus(active_inds)
        params.pattern_boosts = {"|".join(sorted(active_inds)): bonus}

        return params

    @staticmethod
    def _build_active_indicators(sig_data: Dict) -> List[str]:
        """Buat list nama indikator aktif dari sig_data."""
        inds = []
        d    = sig_data.get("direction", "BUY")
        bull = d == "BUY"
        if sig_data.get("bos_bull" if bull else "bos_bear"): inds.append("BOS")
        if sig_data.get("fvg_bull_fresh" if bull else "fvg_bear_fresh"): inds.append("FVG")
        if sig_data.get("ob_bull_valid" if bull else "ob_bear_valid"): inds.append("OB")
        if sig_data.get("liq_swept_l" if bull else "liq_swept_h"): inds.append("LIQ")
        if sig_data.get("ttm_fire"):  inds.append("TTM")
        if sig_data.get("lrsi_ok"):   inds.append("LRSI")
        if sig_data.get("fisher_ok"): inds.append("FISHER")
        if sig_data.get("stc_ok"):    inds.append("STC")
        if sig_data.get("disp_ok"):   inds.append("DISP")
        if sig_data.get("sfp_signal") in ("BULL", "BEAR"): inds.append("SFP")
        if sig_data.get("vol_surge"): inds.append("VOL")
        if sig_data.get("harmonic_pcz"): inds.append("HARMONIC")
        return inds


# ═══════════════════════════════════════════════════════════════
# LEVEL 3: ML ENGINE
# ═══════════════════════════════════════════════════════════════
class MLEngine:
    """Level 3 — Machine Learning: Random Forest + XGBoost.

    Feature vector per trade:
        eps, qcm, cms, ctx, soft,
        ttm_fire, lrsi_ok, fisher_ok, stc_ok,
        bos, ob, fvg, liq, disp, sfp, vol, harmonic,
        in_killzone, is_ranging,
        d1_bull, h4_bull, h1_bull, m30_bull, m15_bull, m5_bull, m1_bull

    Target: 1 = WIN, 0 = LOSS

    Auto-retrain setiap ML_RETRAIN_EVERY trade baru.
    """

    FEATURE_NAMES = [
        "eps", "qcm", "cms", "ctx", "soft",
        "ttm_fire", "lrsi_ok", "fisher_ok", "stc_ok",
        "bos", "ob", "fvg", "liq", "disp", "sfp", "vol", "harmonic",
        "in_kz", "is_ranging",
        "d1_bull", "h4_bull", "h1_bull",
        "m30_bull", "m15_bull", "m5_bull", "m1_bull",
    ]

    def __init__(self, journal: TradeJournal, symbol: str = "") -> None:
        self.journal        = journal
        self.symbol         = symbol
        self._rf:  Optional[Any] = None
        self._xgb: Optional[Any] = None
        self._scaler: Optional[Any] = None
        self._trained       = False
        self._trade_count   = 0
        self._last_train_at = 0
        self._lock          = threading.Lock()

        if not ML_AVAILABLE:
            log.warning("L3: scikit-learn tidak tersedia. ML Engine nonaktif.")

    def _extract_features(self, t: Dict) -> Optional[List[float]]:
        """Ekstrak feature vector dari satu trade dict."""
        try:
            d    = t.get("direction", "BUY")
            bull = d == "BUY"
            tb   = "BUL"

            combo = set(t.get("indicator_combo", []))
            biases = t.get("biases", {})

            feat = [
                float(t.get("eps",  0)),
                float(t.get("qcm",  0)),
                float(t.get("cms",  0.0)),
                float(t.get("ctx",  0)),
                float(t.get("soft", 0)),
                1.0 if "TTM"     in combo else 0.0,
                1.0 if "LRSI"    in combo else 0.0,
                1.0 if "FISHER"  in combo else 0.0,
                1.0 if "STC"     in combo else 0.0,
                1.0 if "BOS"     in combo else 0.0,
                1.0 if "OB"      in combo else 0.0,
                1.0 if "FVG"     in combo else 0.0,
                1.0 if "LIQ"     in combo else 0.0,
                1.0 if "DISP"    in combo else 0.0,
                1.0 if "SFP"     in combo else 0.0,
                1.0 if "VOL"     in combo else 0.0,
                1.0 if "HARMONIC" in combo else 0.0,
                1.0 if t.get("kill_zone", "-") != "-" else 0.0,
                1.0 if t.get("market_regime", "") == "RANGING" else 0.0,
                1.0 if biases.get("d1") == tb else 0.0,
                1.0 if biases.get("h4") == tb else 0.0,
                1.0 if biases.get("h1") == tb else 0.0,
                1.0 if biases.get("m30") == tb else 0.0,
                1.0 if biases.get("m15") == tb else 0.0,
                1.0 if biases.get("m5") == tb else 0.0,
                1.0 if biases.get("m1") == tb else 0.0,
            ]
            return feat
        except Exception as e:
            log.debug("ML feature extraction error: %s", e)
            return None

    def train(self) -> bool:
        """Train RF + XGB dari journal. Return True jika berhasil."""
        if not ML_AVAILABLE:
            return False

        trades = self.journal.get_all_trades_raw(self.symbol)
        if len(trades) < ML_MIN_SAMPLES:
            log.info("L3: Belum cukup data (%d/%d trades).", len(trades), ML_MIN_SAMPLES)
            return False

        X, y = [], []
        for t in trades:
            feat = self._extract_features(t)
            if feat is None:
                continue
            label = 1 if t.get("result") == "WIN" else 0
            X.append(feat)
            y.append(label)

        if len(X) < ML_MIN_SAMPLES:
            return False

        import numpy as np
        X_arr = np.array(X, dtype=np.float32)
        y_arr = np.array(y, dtype=np.int32)

        with self._lock:
            self._scaler = StandardScaler()
            X_scaled     = self._scaler.fit_transform(X_arr)

            self._rf = RandomForestClassifier(
                n_estimators=100,
                max_depth=5,
                min_samples_leaf=3,
                random_state=42,
                class_weight="balanced",
            )
            self._rf.fit(X_scaled, y_arr)

            if XGB_AVAILABLE:
                self._xgb = XGBClassifier(
                    n_estimators=100,
                    max_depth=4,
                    learning_rate=0.05,
                    subsample=0.8,
                    colsample_bytree=0.8,
                    use_label_encoder=False,
                    eval_metric="logloss",
                    verbosity=0,
                    random_state=42,
                )
                self._xgb.fit(X_arr, y_arr)

            self._trained       = True
            self._last_train_at = len(trades)

        # Cross-val score log
        try:
            cv_scores = cross_val_score(self._rf, X_scaled, y_arr, cv=min(5, len(X)//5), scoring="accuracy")
            log.info("L3: RF trained — CV accuracy: %.2f ± %.2f", cv_scores.mean(), cv_scores.std())
        except Exception:
            log.info("L3: RF trained — %d samples.", len(X))

        return True

    def predict_win_prob(self, sig_data: Dict) -> float:
        """Prediksi probabilitas WIN. Return 0.0 jika model belum trained."""
        if not ML_AVAILABLE or not self._trained:
            return 0.0

        # Build trade-like dict dari sig_data
        t_like = {
            "direction":       sig_data.get("direction", "BUY"),
            "eps":             sig_data.get("eps_score", 0),
            "qcm":             sig_data.get("qcm_score", 0),
            "cms":             sig_data.get("cms_score", 0.0),
            "ctx":             sig_data.get("ctx_score", 0),
            "soft":            sig_data.get("soft_count", 0),
            "kill_zone":       sig_data.get("kz_name", "-"),
            "market_regime":   "RANGING" if sig_data.get("acf_chop") else "TRENDING",
            "indicator_combo": self._active_combo(sig_data),
            "biases": {
                "d1":  sig_data.get("d1_bias",  "NEU"),
                "h4":  sig_data.get("h4_bias",  "NEU"),
                "h1":  sig_data.get("h1_bias",  "NEU"),
                "m30": sig_data.get("m30_bias", "NEU"),
                "m15": sig_data.get("m15_bias", "NEU"),
                "m5":  sig_data.get("m5_bias",  "NEU"),
                "m1":  sig_data.get("m1_bias",  "NEU"),
            },
        }
        feat = self._extract_features(t_like)
        if feat is None:
            return 0.0

        import numpy as np
        feat_arr = np.array([feat], dtype=np.float32)

        probs = []
        with self._lock:
            try:
                feat_scaled = self._scaler.transform(feat_arr)
                rf_prob = self._rf.predict_proba(feat_scaled)[0][1]
                probs.append(rf_prob)
            except Exception:
                pass

            if XGB_AVAILABLE and self._xgb is not None:
                try:
                    xgb_prob = self._xgb.predict_proba(feat_arr)[0][1]
                    probs.append(xgb_prob)
                except Exception:
                    pass

        if not probs:
            return 0.0

        # Ensemble: rata-rata RF + XGB
        return round(sum(probs) / len(probs), 3)

    def maybe_retrain(self) -> None:
        """Retrain jika ada trade baru cukup sejak last train."""
        trades = self.journal.get_all_trades_raw(self.symbol)
        total  = len(trades)
        if total - self._last_train_at >= ML_RETRAIN_EVERY:
            log.info("L3: Auto-retrain dipicu (%d trade baru).", total - self._last_train_at)
            threading.Thread(target=self.train, daemon=True).start()

    @staticmethod
    def _active_combo(sig_data: Dict) -> List[str]:
        return PatternRecognizer._build_active_indicators(sig_data)

    def get_feature_importance(self) -> Optional[Dict[str, float]]:
        """Return feature importance dari RF (untuk diagnostik)."""
        if not self._trained or self._rf is None:
            return None
        fi = self._rf.feature_importances_
        return dict(zip(self.FEATURE_NAMES, [round(float(v), 4) for v in fi]))


# ═══════════════════════════════════════════════════════════════
# ADAPTIVE CONTROLLER — Integrator semua Level
# ═══════════════════════════════════════════════════════════════
class AdaptiveController:
    """Integrasikan output Level 1 + 2 + 3 menjadi AdaptiveParams terkini.

    Dipanggil sekali per bar close, hasilnya digunakan oleh analyze().
    """

    def __init__(
        self,
        journal:  TradeJournal,
        symbol:   str = "",
    ) -> None:
        self.journal  = journal
        self.symbol   = symbol
        self._lock    = threading.Lock()

        self.stat_learner = StatisticalLearner(journal, symbol)
        self.pat_recog    = PatternRecognizer(journal, symbol)
        self.ml_engine    = MLEngine(journal, symbol)

        # Inisialisasi params
        self._params      = AdaptiveParams()
        self._params_lock = threading.Lock()

        # Background train ML kalau data cukup
        threading.Thread(target=self._bg_init, daemon=True).start()

    def _bg_init(self) -> None:
        """Background: train pattern + ML saat startup."""
        time.sleep(2)
        try:
            self.pat_recog.train()
        except Exception as e:
            log.warning("L2 train error: %s", e)
        try:
            self.ml_engine.train()
        except Exception as e:
            log.warning("L3 train error: %s", e)

    def get_params(self) -> AdaptiveParams:
        with self._params_lock:
            return self._params

    def update(self, sig_data: Dict) -> Tuple[AdaptiveParams, float]:
        """Hitung params terbaru dan ML win probability.

        Returns:
            (AdaptiveParams, ml_win_prob)
        """
        with self._lock:
            params = AdaptiveParams()

            # Level 1
            params = self.stat_learner.compute(params)

            # Level 2
            params = self.pat_recog.compute(params, sig_data)

            # Level 3
            self.ml_engine.maybe_retrain()
            ml_prob = self.ml_engine.predict_win_prob(sig_data)
            params.ml_active      = ML_AVAILABLE and self.ml_engine._trained
            params.ml_win_prob_min = ML_WIN_PROB_MIN

            with self._params_lock:
                self._params = params

            return params, ml_prob

    def record_trade(
        self,
        sig: Signal,
        tp_hit: str,
        rr_achieved: float,
        symbol: str,
    ) -> None:
        """Catat hasil trade ke journal + trigger retrain."""
        active_inds = PatternRecognizer._build_active_indicators(asdict(sig))
        self.journal.add_trade(
            symbol=symbol,
            direction=sig.direction,
            entry=sig.order.entry,
            sl=sig.order.sl,
            tp_hit=tp_hit,
            rr_achieved=rr_achieved,
            kill_zone=sig.kz_name,
            order_type=sig.order.order_type,
            indicator_combo=active_inds,
            qcm=sig.qcm_score,
            eps=sig.eps_score,
            cms=sig.cms_score,
            market_regime="RANGING" if sig.is_ranging else "TRENDING",
        )
        self.pat_recog.train()
        self.ml_engine.maybe_retrain()

    def get_learning_report(self) -> str:
        """Report lengkap 3 level untuk Telegram."""
        lines = [
            "🧠 <b>ADAPTIVE INTELLIGENCE REPORT</b>\n",
            self.stat_learner.get_summary(),
            "\n",
        ]

        # L2
        params = self.get_params()
        rang = "✅ RANGING — threshold diperketat" if params.ranging_boost_active else "✅ TRENDING"
        lines.append(f"🔍 <b>L2 Pattern:</b> {rang}")
        if params.pattern_boosts:
            for combo, bonus in params.pattern_boosts.items():
                short = combo.replace("|", "+")[:40]
                lines.append(f"  Pattern Bonus: +{bonus:.2f} CMS → {short}")

        # L3
        lines.append("\n🤖 <b>L3 ML Engine:</b>")
        if ML_AVAILABLE and self.ml_engine._trained:
            fi = self.ml_engine.get_feature_importance()
            if fi:
                top3 = sorted(fi.items(), key=lambda x: -x[1])[:3]
                for fname, imp in top3:
                    lines.append(f"  Top feature: {fname} ({imp:.3f})")
        else:
            lines.append(f"  Butuh ≥{ML_MIN_SAMPLES} trade untuk aktivasi.")

        return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# ATR WILDER'S RMA
# ═══════════════════════════════════════════════════════════════
def calc_atr_rma(bars: List[Dict], period: int = ATR_PERIOD) -> float:
    if len(bars) < period + 1:
        return 0.0
    tr_series: List[float] = []
    for i in range(1, len(bars)):
        h  = float(bars[i]["high"])
        l  = float(bars[i]["low"])
        pc = float(bars[i - 1]["close"])
        tr_series.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(tr_series) < period:
        return 0.0
    atr = sum(tr_series[:period]) / period
    mult = 1.0 / period
    for tr in tr_series[period:]:
        atr = tr * mult + atr * (1.0 - mult)
    return round(atr, 4)


# ═══════════════════════════════════════════════════════════════
# EMA SERIES
# ═══════════════════════════════════════════════════════════════
def _ema_series(data: List[float], period: int) -> List[float]:
    if not data or period <= 0:
        return []
    k      = 2.0 / (period + 1)
    result = [0.0] * len(data)
    seed_end = min(period, len(data))
    result[seed_end - 1] = sum(data[:seed_end]) / seed_end
    for i in range(seed_end, len(data)):
        result[i] = data[i] * k + result[i - 1] * (1.0 - k)
    return result


# ═══════════════════════════════════════════════════════════════
# CME — COMPOSITE MOMENTUM ENGINE
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

    atr14 = calc_atr_rma(bars, ATR_PERIOD)

    # TTM Squeeze
    bb_window   = closes[-BB_PERIOD:]
    bb_mid_now  = sum(bb_window) / BB_PERIOD
    bb_window_p = closes[-(BB_PERIOD + 1):-1]
    bb_mid_prev = sum(bb_window_p) / BB_PERIOD
    bb_std = math.sqrt(sum((x - bb_mid_now) ** 2 for x in bb_window) / BB_PERIOD)
    bb_upper     = bb_mid_now + BB_MULT * bb_std
    bb_lower     = bb_mid_now - BB_MULT * bb_std
    kc_upper     = bb_mid_now + KC_MULT * atr14
    kc_lower     = bb_mid_now - KC_MULT * atr14
    squeeze_on   = (bb_upper < kc_upper) and (bb_lower > kc_lower)
    hist_now     = closes[-1] - bb_mid_now
    hist_prev    = closes[-2] - bb_mid_prev
    hist_incr    = hist_now > hist_prev

    if direction == "BUY":
        ttm_fire = (not squeeze_on) and (hist_now > 0) and hist_incr
    else:
        ttm_fire = (not squeeze_on) and (hist_now < 0) and (not hist_incr)

    # Laguerre RSI
    gamma = LAGUERRE_GAMMA
    L0 = L1 = L2 = L3 = closes[0]
    for c in closes:
        nL0 = (1.0 - gamma) * c + gamma * L0
        nL1 = -gamma * nL0 + L0 + gamma * L1
        nL2 = -gamma * nL1 + L1 + gamma * L2
        nL3 = -gamma * nL2 + L2 + gamma * L3
        L0, L1, L2, L3 = nL0, nL1, nL2, nL3
    cu = max(L0-L1,0)+max(L1-L2,0)+max(L2-L3,0)
    cd = max(L1-L0,0)+max(L2-L1,0)+max(L3-L2,0)
    lrsi    = cu / (cu + cd) if (cu + cd) > 1e-10 else 0.5
    lrsi_ok = (lrsi > 0.55) if direction == "BUY" else (lrsi < 0.45)

    # Fisher Transform
    fp   = FISHER_PERIOD
    hh   = max(highs[-fp:])
    ll   = min(lows[-fp:])
    fish = 0.0
    if hh != ll:
        rv  = max(min(2.0 * ((closes[-1] - ll) / (hh - ll)) - 1.0, 0.999), -0.999)
        fish = 0.5 * math.log((1.0 + rv) / (1.0 - rv))
    fisher_ok = (
        (fish < -FISHER_EXTREME and direction == "BUY") or
        (fish >  FISHER_EXTREME and direction == "SELL")
    )

    # STC
    stc = 50.0
    if n >= MIN_BARS_STC:
        ef = _ema_series(closes, STC_FAST)
        es = _ema_series(closes, STC_SLOW)
        macd_s = [ef[i]-es[i] for i in range(STC_SLOW-1, n) if es[i] != 0.0]
        if len(macd_s) >= STC_CYCLE:
            w  = macd_s[-STC_CYCLE:]
            mh, ml = max(w), min(w)
            stc = (macd_s[-1]-ml)/(mh-ml)*100.0 if mh!=ml else 50.0
    stc_ok = (stc < STC_OVERSOLD) if direction == "BUY" else (stc > STC_OVERBOUGHT)

    pts  = 0.0
    pts += 3.0 if ttm_fire  else 0.0
    pts += 2.5 if lrsi_ok   else 0.0
    pts += 2.5 if fisher_ok else 0.0
    pts += 2.0 if stc_ok    else 0.0
    cms  = round(min(pts / 10.0 * 10.0, 10.0), 2)

    return {"ttm_fire": ttm_fire, "lrsi_ok": lrsi_ok,
            "fisher_ok": fisher_ok, "stc_ok": stc_ok,
            "cms_score": cms, "atr": atr14}


# ═══════════════════════════════════════════════════════════════
# PENDING ORDER ENGINE
# ═══════════════════════════════════════════════════════════════
class PendingOrderEngine:
    def __init__(self, atr: float) -> None:
        self.atr = max(atr, 0.01)

    def calc(self, sig: "Signal") -> OrderLevels:
        if sig.direction not in ("BUY", "SELL"):
            return OrderLevels(reason="NO DIRECTION")
        return self._calc_bull(sig) if sig.direction == "BUY" else self._calc_bear(sig)

    def _calc_bull(self, sig: "Signal") -> OrderLevels:
        if (sig.ob_bull_valid or sig.fvg_bull_fresh) and sig.ob_bull_low > 0:
            return self._build_limit("BUY", sig.ob_bull_high, sig.ob_bull_low,
                sig.swing_low, sig.current_price,
                f"BUY LIMIT di OB/FVG [{sig.ob_bull_low:.2f}–{sig.ob_bull_high:.2f}]")
        if sig.swing_high > 0 and sig.bos_bull and sig.liq_swept_l:
            return self._build_stop("BUY", sig.swing_high, sig.swing_low,
                f"BUY STOP breakout swing high [{sig.swing_high:.2f}]")
        return OrderLevels(reason="Tidak ada zona BUY valid", valid=False)

    def _calc_bear(self, sig: "Signal") -> OrderLevels:
        if (sig.ob_bear_valid or sig.fvg_bear_fresh) and sig.ob_bear_high > 0:
            return self._build_limit("SELL", sig.ob_bear_high, sig.ob_bear_low,
                sig.swing_high, sig.current_price,
                f"SELL LIMIT di OB/FVG [{sig.ob_bear_low:.2f}–{sig.ob_bear_high:.2f}]")
        if sig.swing_low > 0 and sig.bos_bear and sig.liq_swept_h:
            return self._build_stop("SELL", sig.swing_low, sig.swing_high,
                f"SELL STOP breakdown swing low [{sig.swing_low:.2f}]")
        return OrderLevels(reason="Tidak ada zona SELL valid", valid=False)

    def _build_limit(self, direction, zh, zl, swing_ref, cp, reason) -> OrderLevels:
        atr  = self.atr
        bull = direction == "BUY"
        if bull:
            entry = max(zh - atr * OB_ENTRY_ATR_MULT, zl)
            sl    = min(zl - SL_SWING_BUFFER, entry - atr * SL_ATR_MULT)
        else:
            entry = min(zl + atr * OB_ENTRY_ATR_MULT, zh)
            sl    = max(zh + SL_SWING_BUFFER, entry + atr * SL_ATR_MULT)
        return self._finalize(f"{direction} LIMIT", direction, entry, sl, reason)

    def _build_stop(self, direction, level, swing_ref, reason) -> OrderLevels:
        atr  = self.atr
        bull = direction == "BUY"
        if bull:
            entry = level + atr * BREAKOUT_ATR_MULT
            sl    = min(swing_ref - SL_SWING_BUFFER, entry - atr * SL_ATR_MULT)
        else:
            entry = level - atr * BREAKOUT_ATR_MULT
            sl    = max(swing_ref + SL_SWING_BUFFER, entry + atr * SL_ATR_MULT)
        return self._finalize(f"{direction} STOP", direction, entry, sl, reason)

    def _finalize(self, order_type, direction, entry, sl, reason) -> OrderLevels:
        risk = abs(entry - sl)
        if risk < 0.01:
            return OrderLevels(reason="Risk terlalu kecil", valid=False)
        bull = direction == "BUY"
        sign = 1 if bull else -1
        return OrderLevels(
            order_type=order_type,
            entry=round(entry,2), sl=round(sl,2),
            tp1=round(entry+sign*risk*FIB_TP1, 2),
            tp2=round(entry+sign*risk*FIB_TP2, 2),
            tp3=round(entry+sign*risk*FIB_TP3, 2),
            rr_tp1=FIB_TP1, rr_tp2=FIB_TP2, rr_tp3=FIB_TP3,
            risk_pips=round(risk,2),
            atr_current=round(self.atr,4),
            reason=reason, valid=True,
        )


# ═══════════════════════════════════════════════════════════════
# EPS ENGINE
# ═══════════════════════════════════════════════════════════════
def calc_eps(sig_data: Dict) -> Dict:
    d    = sig_data.get("direction", "BUY")
    bull = d == "BUY"
    l1   = bool(sig_data.get("bos_bull" if bull else "bos_bear")) or \
           (sig_data.get("h1_bias") == ("BUL" if bull else "BER"))
    l2   = bool(sig_data.get("ob_bull_valid" if bull else "ob_bear_valid")) or \
           bool(sig_data.get("fvg_bull_fresh" if bull else "fvg_bear_fresh"))
    l3   = float(sig_data.get("cms_score", 0.0)) >= BASE_SCALP_CMS_L3
    l4   = (sig_data.get("m1_bias") == ("BUL" if bull else "BER")) or \
           (sig_data.get("sfp_signal") in ("BULL", "BEAR"))
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
_HTF_MAP = {3: 20, 2: 14, 1: 7, 0: 0}
_MTF_MAP = {3: 12, 2: 8,  1: 4, 0: 0}

def calc_qcm(sig_data: Dict) -> int:
    d    = sig_data.get("direction", "BUY")
    bull = d == "BUY"
    tb   = "BUL" if bull else "BER"
    s    = 0
    s   += _HTF_MAP.get(sum([sig_data.get("d1_bias")==tb,
                              sig_data.get("h4_bias")==tb,
                              sig_data.get("h1_bias")==tb]), 0)
    s   += _MTF_MAP.get(sum([sig_data.get("m30_bias")==tb,
                              sig_data.get("m15_bias")==tb,
                              sig_data.get("m5_bias")==tb]), 0)
    s   += max(0, 15 - max(0, int(sig_data.get("pd_priority", 0))))
    s   += 5 if sig_data.get("bos_bull" if bull else "bos_bear") else 0
    s   += 5 if sig_data.get("liq_swept_l" if bull else "liq_swept_h") else 0
    s   += 4 if sig_data.get("disp_ok") else 0
    s   += 4 if sig_data.get("sfp_signal") in ("BULL","BEAR") else 0
    s   += 5 if sig_data.get("vol_surge") else 0
    s   += 3 if not sig_data.get("acf_chop") else 0
    s   += 10 if sig_data.get("in_killzone") else 3
    s   += int(min(float(sig_data.get("cms_score",0))/10.0*15, 15))
    return min(s, MAX_QCM)


# ═══════════════════════════════════════════════════════════════
# GRADE & GATE (Adaptive)
# ═══════════════════════════════════════════════════════════════
def calc_grade(eps: int, qcm: int) -> str:
    if eps >= MAX_EPS and qcm >= 85: return "PRIME"
    if eps >= 3 and qcm >= 70:       return "HIGH"
    return "STANDARD"


def check_gate(sig_data: Dict, params: AdaptiveParams) -> Tuple[bool, str]:
    """Gate check dengan threshold adaptif dari AdaptiveParams."""
    d    = sig_data.get("direction", "NONE")
    eps  = int(sig_data.get("eps_score",  0))
    qcm  = int(sig_data.get("qcm_score",  0))
    cms  = float(sig_data.get("cms_score", 0.0))
    bull = d == "BUY"

    if d == "NONE":
        return False, "NO DIRECTION"

    # L1: Skip KZ
    kz_name = sig_data.get("kz_name", "-")
    if params.skip_kz.get(kz_name, False):
        return False, f"L1-SKIP: {kz_name} winrate rendah"

    # L1: Skip order type
    ot = sig_data.get("order_type_auto", "-")
    if params.skip_order_type.get(ot, False):
        return False, f"L1-SKIP: {ot} winrate rendah"

    # News
    if not bool(sig_data.get("news_ok", False)):
        tier = int(sig_data.get("news_tier", 0))
        if tier >= 1:
            return False, f"NEWS TIER-{tier} BLOCK"

    # Struktur
    bos_ok = bool(sig_data.get("bos_bull" if bull else "bos_bear", False))
    htf_ok = (sig_data.get("h4_bias") == ("BUL" if bull else "BER") or
               sig_data.get("h1_bias") == ("BUL" if bull else "BER"))
    if not bos_ok and not htf_ok:
        return False, "STRUKTUR HTF BELUM ALIGNED"

    # PD Array
    ob_ok  = bool(sig_data.get("ob_bull_valid" if bull else "ob_bear_valid", False))
    fvg_ok = bool(sig_data.get("fvg_bull_fresh" if bull else "fvg_bear_fresh", False))
    if not ob_ok and not fvg_ok:
        return False, "TIDAK ADA PD ARRAY VALID"

    # EPS (adaptive)
    if eps < params.eps_min:
        return False, f"EPS RENDAH ({eps}/{MAX_EPS}) min={params.eps_min}"

    # QCM (adaptive)
    if qcm < params.qcm_min:
        return False, f"QCM RENDAH ({qcm}/{MAX_QCM}) min={params.qcm_min}"

    # CMS (adaptive)
    if cms < params.cms_min:
        return False, f"CMS RENDAH ({cms:.1f}/10) min={params.cms_min:.1f}"

    if bool(sig_data.get("acf_chop", False)):
        return False, "CHOP — MARKET RANGING"

    # Order validity
    if not sig_data.get("order_valid", False):
        return False, "PENDING ORDER LEVEL TIDAK VALID"

    # L3: ML gate (hanya aktif jika ML trained)
    if params.ml_active:
        ml_prob = float(sig_data.get("ml_win_prob", 0.0))
        if ml_prob > 0 and ml_prob < params.ml_win_prob_min:
            return False, f"L3-ML: prob={ml_prob:.0%} < {params.ml_win_prob_min:.0%}"

    return True, "OK"


# ═══════════════════════════════════════════════════════════════
# INPUT VALIDATION
# ═══════════════════════════════════════════════════════════════
_BOOL_FIELDS = (
    "bos_bull","bos_bear","fvg_bull_fresh","fvg_bear_fresh",
    "ob_bull_valid","ob_bear_valid","liq_swept_l","liq_swept_h",
    "disp_ok","vol_surge","acf_chop","pdc_ok","harmonic_pcz","news_ok",
)
_FLOAT_FIELDS = (
    "ob_bull_high","ob_bull_low","ob_bear_high","ob_bear_low",
    "swing_high","swing_low","current_price","cms_score",
)
_INT_FIELDS = ("pd_priority","fractal_conv","news_tier")

def _validate_raw(raw: Dict) -> Dict:
    v = dict(raw)
    for f in _BOOL_FIELDS:
        if f in v:
            val = v[f]
            if isinstance(val, str): v[f] = val.strip().lower() in ("true","1","yes")
            elif not isinstance(val, bool): v[f] = bool(val)
    for f in _FLOAT_FIELDS:
        if f in v:
            try: v[f] = float(v[f])
            except: raise TypeError(f"'{f}' tidak bisa ke float: {v[f]!r}")
    for f in _INT_FIELDS:
        if f in v:
            try: v[f] = int(v[f])
            except: raise TypeError(f"'{f}' tidak bisa ke int: {v[f]!r}")
    return v


# ═══════════════════════════════════════════════════════════════
# MASTER ANALYZE
# ═══════════════════════════════════════════════════════════════
def analyze(
    raw:        Dict,
    bars:       List[Dict],
    symbol:     str,
    adaptive:   Optional["AdaptiveController"] = None,
) -> Signal:
    """Engine utama v22.0 dengan 3-level adaptive learning."""
    raw = _validate_raw(raw)
    sig = Signal()

    _copy_fields = (
        "direction","d1_bias","h4_bias","h1_bias",
        "m30_bias","m15_bias","m5_bias","m1_bias",
        "bos_bull","bos_bear","fvg_bull_fresh","fvg_bear_fresh",
        "ob_bull_valid","ob_bear_valid",
        "ob_bull_high","ob_bull_low","ob_bear_high","ob_bear_low",
        "swing_high","swing_low",
        "liq_swept_l","liq_swept_h",
        "disp_ok","sfp_signal","vol_surge","acf_chop","pdc_ok",
        "pd_type","pd_priority","fractal_conv","harmonic_pcz",
        "news_ok","news_tier","current_price",
    )
    for fn in _copy_fields:
        if fn in raw: setattr(sig, fn, raw[fn])

    # Kill Zone (DST-aware)
    now_wib = datetime.now(WIB)
    in_kz, kz_name, kz_start, kz_end = kz_scheduler.check(now_wib)
    sig.in_killzone = in_kz
    sig.kz_name     = kz_name
    sig.kz_start    = kz_start
    sig.kz_end      = kz_end

    # CME
    cme = calc_cms(bars, sig.direction)
    sig.ttm_fire    = cme["ttm_fire"]
    sig.lrsi_ok     = cme["lrsi_ok"]
    sig.fisher_ok   = cme["fisher_ok"]
    sig.stc_ok      = cme["stc_ok"]
    sig.cms_score   = cme["cms_score"]
    sig.atr_current = cme["atr"]

    # EPS
    snap         = asdict(sig)
    eps          = calc_eps(snap)
    sig.eps_layer1_structure = eps["eps_layer1_structure"]
    sig.eps_layer2_pdarray   = eps["eps_layer2_pdarray"]
    sig.eps_layer3_momentum  = eps["eps_layer3_momentum"]
    sig.eps_layer4_micro     = eps["eps_layer4_micro"]
    sig.eps_score            = eps["eps_score"]

    # QCM
    snap          = asdict(sig)
    sig.qcm_score = calc_qcm(snap)
    sig.sqs_score = round(sig.qcm_score / 10.0, 1)

    # CTX & SOFT
    ctx = 0
    ctx += 2 if (sig.bos_bull or sig.bos_bear)             else 0
    ctx += 2 if (sig.fvg_bull_fresh or sig.fvg_bear_fresh)  else 0
    ctx += 2 if (sig.ob_bull_valid or sig.ob_bear_valid)    else 0
    ctx += 1 if (sig.liq_swept_l or sig.liq_swept_h)       else 0
    ctx += 1 if sig.disp_ok                                 else 0
    sig.ctx_score  = min(ctx, MAX_CTX)

    soft = 0
    soft += 2 if sig.sfp_signal in ("BULL","BEAR") else 0
    soft += 2 if sig.vol_surge                       else 0
    soft += 1 if not sig.acf_chop                    else 0
    soft += 1 if sig.pdc_ok                          else 0
    soft += 1 if sig.harmonic_pcz                    else 0
    soft += 1 if sig.fractal_conv >= 3               else 0
    sig.soft_count = min(soft, MAX_SOFT)

    sig.grade      = calc_grade(sig.eps_score, sig.qcm_score)
    sig.is_ranging = sig.acf_chop or (sig.cms_score < 5.5)

    # Pending Order
    order_engine = PendingOrderEngine(atr=sig.atr_current)
    sig.order    = order_engine.calc(sig)

    # ── Adaptive Learning Integration ─────────────────────────
    params     = AdaptiveParams()
    ml_prob    = 0.0
    pat_bonus  = 0.0

    if adaptive is not None:
        snap_for_adaptive = asdict(sig)
        params, ml_prob   = adaptive.update(snap_for_adaptive)

        # Pattern bonus → naikkan CMS efektif
        if params.pattern_boosts:
            pat_bonus = max(params.pattern_boosts.values())
            sig.cms_score = min(sig.cms_score + pat_bonus, 10.0)

    sig.adaptive      = params
    sig.ml_win_prob   = ml_prob
    sig.ml_active     = params.ml_active
    sig.pattern_bonus = pat_bonus

    # Gate check (adaptive thresholds)
    snap = asdict(sig)
    snap["order_valid"]     = sig.order.valid
    snap["order_type_auto"] = sig.order.order_type
    snap["ml_win_prob"]     = ml_prob

    if not sig.in_killzone:
        sig.gate_ok  = False
        sig.veto_rsn = "OFF-KZ"

        # Info KZ berikutnya
        nxt = kz_scheduler.next_killzone(now_wib)
        if nxt:
            sig.veto_rsn = (
                f"OFF-KZ | Next: {nxt['name']} jam {nxt['start']} "
                f"(~{nxt['wait_min']} mnt)"
            )
        return sig

    gate_ok, gate_reason = check_gate(snap, params)
    sig.gate_ok  = gate_ok
    sig.veto_rsn = gate_reason if not gate_ok else "OK"

    return sig


# ═══════════════════════════════════════════════════════════════
# TELEGRAM FORMATTER
# ═══════════════════════════════════════════════════════════════
def _grade_stars(grade: str) -> str:
    return {"PRIME": "🔥🔥🔥", "HIGH": "⭐⭐", "STANDARD": "📶"}.get(grade, "")

def _conf(eps, sqs, ctx, soft) -> int:
    return round(
        min(eps/MAX_EPS,1)*30 + min(sqs/MAX_SQS,1)*30 +
        min(ctx/MAX_CTX,1)*20 + min(soft/MAX_SOFT,1)*20
    )


def fmt_signal_telegram(sig: Signal, symbol: str, stats: HistoricalStats) -> str:
    now_wib  = datetime.now(WIB).strftime("%d %b %Y | %H:%M WIB")
    o        = sig.order
    d        = sig.direction
    d_icon   = "🟢" if d == "BUY" else "🔴"
    tb       = "BUL" if d == "BUY" else "BER"
    biases   = [sig.d1_bias,sig.h4_bias,sig.h1_bias,sig.m30_bias,sig.m15_bias,sig.m5_bias,sig.m1_bias]
    aligned  = sum(1 for b in biases if b == tb)
    conf     = _conf(sig.eps_score, sig.sqs_score, sig.ctx_score, sig.soft_count)

    reasons = []
    if sig.bos_bull or sig.bos_bear: reasons.append("BOS ✓")
    if sig.ob_bull_valid or sig.ob_bear_valid: reasons.append("OB ✓")
    if sig.fvg_bull_fresh or sig.fvg_bear_fresh: reasons.append("FVG ✓")
    if sig.liq_swept_l or sig.liq_swept_h: reasons.append("Liq ✓")
    if sig.ttm_fire:    reasons.append("TTM ✓")
    if sig.lrsi_ok:     reasons.append("LRSI ✓")
    if sig.fisher_ok:   reasons.append("Fisher ✓")
    if sig.stc_ok:      reasons.append("STC ✓")
    if sig.in_killzone: reasons.append(f"{sig.kz_name} ✓")
    reason_str = " | ".join(reasons[:6])

    # Adaptive info
    adapt_lines = []
    p = sig.adaptive
    if p.ranging_boost_active:
        adapt_lines.append("⚠️ Ranging → EPS/QCM threshold diperketat")
    if p.skip_kz.get(sig.kz_name):
        adapt_lines.append(f"🚫 L1: {sig.kz_name} di-skip (winrate rendah)")
    if sig.ml_active and sig.ml_win_prob > 0:
        bar_filled = int(sig.ml_win_prob * 10)
        prob_bar   = "█" * bar_filled + "░" * (10 - bar_filled)
        adapt_lines.append(f"🤖 ML Prob: [{prob_bar}] {sig.ml_win_prob:.0%}")
    if sig.pattern_bonus > 0:
        adapt_lines.append(f"🔮 Pattern Bonus: +{sig.pattern_bonus:.2f} CMS")
    adapt_str = "\n".join(adapt_lines) if adapt_lines else "—"

    SEP = "═" * 33
    msg = (
        f"{SEP}\n"
        f"⚠️ {symbol} HIGH PROBABILITY SIGNAL ⚠️\n"
        f"{SEP}\n\n"
        f"{d_icon} <b>Type</b>     : <code>{o.order_type}</code>\n"
        f"📍 <b>Entry</b>    : <code>{o.entry:.2f}</code>\n\n"
        f"🛑 <b>SL</b>       : <code>{o.sl:.2f}</code> "
        f"(Risk: {o.risk_pips:.1f}pts | ATR: {o.atr_current:.2f})\n\n"
        f"🎯 <b>TP1</b>  : <code>{o.tp1:.2f}</code> [1:{o.rr_tp1:.2f}R]\n"
        f"🎯 <b>TP2</b>  : <code>{o.tp2:.2f}</code> [1:{o.rr_tp2:.2f}R]\n"
        f"🎯 <b>TP3</b>  : <code>{o.tp3:.2f}</code> [1:{o.rr_tp3:.2f}R]\n\n"
        f"{SEP}\n\n"
        f"📋 {o.reason}\n"
        f"🔍 {reason_str}\n"
        f"🕐 KZ: {sig.kz_name} ({sig.kz_start}–{sig.kz_end} WIB)\n\n"
        f"{SEP}\n\n"
        f"📊 <b>QUALITY SCORE</b>\n\n"
        f"Grade      : {sig.grade} {_grade_stars(sig.grade)}\n"
        f"Confidence : {conf}%\n"
        f"EPS        : {sig.eps_score}/{MAX_EPS}\n"
        f"QCM        : {sig.qcm_score}/{MAX_QCM}\n"
        f"CMS        : {sig.cms_score:.1f}/10\n"
        f"Alignment  : {aligned}/7 TF\n\n"
        f"{SEP}\n\n"
        f"🧠 <b>ADAPTIVE INTEL</b>\n\n"
        f"{adapt_str}\n\n"
        f"{SEP}\n\n"
        f"📚 <b>HISTORICAL</b> ({stats.total} trades)\n\n"
        f"Winrate    : {stats.winrate}%\n"
        f"Avg RR     : {stats.avg_rr}\n\n"
        f"{SEP}\n"
        f"🕒 {now_wib}\n"
        f"<i>PEMIF v22.0 Adaptive Intelligence</i>\n"
        f"{SEP}"
    )
    return msg


def fmt_no_signal_telegram(sig: Signal, symbol: str) -> str:
    now_wib = datetime.now(WIB).strftime("%H:%M WIB")
    next_kz = kz_scheduler.next_killzone(datetime.now(WIB))
    nxt_str = (f"\n⏰ Next KZ: {next_kz['name']} jam {next_kz['start']} "
               f"(~{next_kz['wait_min']} mnt)") if next_kz else ""
    return (
        f"🤖 <b>PEMIF v22.0</b> | {symbol} | {now_wib}\n\n"
        f"Status  : {sig.veto_rsn}\n"
        f"KZ      : {sig.kz_name}\n"
        f"EPS     : {sig.eps_score}/{MAX_EPS}\n"
        f"QCM     : {sig.qcm_score}/{MAX_QCM}\n"
        f"CMS     : {sig.cms_score:.1f}/10"
        f"{nxt_str}"
    )


def fmt_kz_schedule(now_wib: datetime) -> str:
    """Format jadwal Kill Zone hari ini untuk notifikasi awal sesi."""
    windows = kz_scheduler.get_all_windows_wib(now_wib)
    dst_tag = windows[0]["dst"] if windows else "?"
    lines   = [f"📅 <b>Kill Zone Schedule — {dst_tag}</b>\n"]
    for w in windows:
        lines.append(f"  🕐 {w['name']}: {w['start']} – {w['end']}")
    return "\n".join(lines)


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
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            log.warning("Telegram attempt %d/%d: %s", attempt, max_retry, exc)
        if attempt < max_retry:
            time.sleep(TELEGRAM_RETRY_DELAY * (2 ** (attempt - 1)))
    log.error("Telegram GAGAL setelah %d attempt.", max_retry)
    return False


# ═══════════════════════════════════════════════════════════════
# PRICE STREAM (WebSocket + REST fallback)
# ═══════════════════════════════════════════════════════════════
class PriceStream:
    def __init__(self, symbol, api_key, interval="1min", on_bar_close=None):
        self.symbol       = symbol
        self.api_key      = api_key
        self.interval     = interval
        self.on_bar_close = on_bar_close
        self._tick_q      = queue.Queue(maxsize=TICK_BUFFER_SIZE)
        self._bars: List[Dict] = []
        self._lock        = threading.Lock()
        self._stop_evt    = threading.Event()
        self._ws_app      = None
        self._threads: List[threading.Thread] = []
        self._current_bar = None
        self._bar_sec     = self._parse_interval(interval)

    @staticmethod
    def _parse_interval(iv: str) -> int:
        return {"1min":60,"5min":300,"15min":900,"30min":1800,
                "1h":3600,"4h":14400,"1day":86400}.get(iv, 60)

    def start(self):
        self._stop_evt.clear()
        self._fetch_initial_bars()
        if WS_AVAILABLE and self.api_key:
            t = threading.Thread(target=self._run_ws, daemon=True, name="ws-feed")
        else:
            t = threading.Thread(target=self._run_rest, daemon=True, name="rest-feed")
        self._threads.append(t)
        t.start()

    def stop(self):
        self._stop_evt.set()
        if self._ws_app:
            try: self._ws_app.close()
            except: pass

    def get_bars(self, n=MIN_BARS_CME) -> List[Dict]:
        with self._lock:
            return list(self._bars[-n:])

    def get_latest_price(self) -> float:
        with self._lock:
            return float(self._bars[-1]["close"]) if self._bars else 0.0

    def _run_ws(self):
        url = "wss://ws.twelvedata.com/v1/quotes/price"
        while not self._stop_evt.is_set():
            try:
                self._ws_app = websocket.WebSocketApp(
                    url,
                    on_open=self._ws_on_open,
                    on_message=self._ws_on_msg,
                    on_error=lambda ws,e: log.warning("WS err: %s", e),
                    on_close=lambda ws,c,m: log.info("WS closed."),
                )
                self._ws_app.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                log.warning("WS exception: %s", e)
            if not self._stop_evt.is_set():
                time.sleep(WS_RECONNECT_DELAY)

    def _ws_on_open(self, ws):
        sym = self.symbol.replace("/","")
        ws.send(json.dumps({"action":"subscribe","params":{"symbols":sym,"apikey":self.api_key}}))

    def _ws_on_msg(self, ws, message):
        try:
            data = json.loads(message)
            price = float(data.get("price", 0.0))
            ts    = data.get("timestamp", time.time())
            if price > 0:
                self._process_tick(price, float(ts))
        except Exception: pass

    def _run_rest(self):
        while not self._stop_evt.is_set():
            try: self._fetch_latest_bar()
            except Exception as e: log.warning("REST poll: %s", e)
            self._stop_evt.wait(REST_POLL_INTERVAL)

    def _fetch_latest_bar(self):
        if not self.api_key: return
        r = requests.get("https://api.twelvedata.com/time_series", params={
            "symbol":self.symbol,"interval":self.interval,"outputsize":1,"apikey":self.api_key
        }, timeout=10)
        r.raise_for_status()
        vals = r.json().get("values", [])
        if vals: self._append_bar(self._parse_bar(vals[0]))

    def _fetch_initial_bars(self):
        if not self.api_key: return
        try:
            r = requests.get("https://api.twelvedata.com/time_series", params={
                "symbol":self.symbol,"interval":self.interval,
                "outputsize":MIN_BARS_CME+10,"apikey":self.api_key,"order":"ASC"
            }, timeout=15)
            r.raise_for_status()
            vals = r.json().get("values", [])
            with self._lock:
                self._bars = [self._parse_bar(v) for v in vals]
            log.info("Initial bars: %d candles.", len(self._bars))
        except Exception as e:
            log.error("Fetch initial bars gagal: %s", e)

    @staticmethod
    def _parse_bar(v) -> Dict:
        return {k: float(v.get(k, 0)) for k in ("open","high","low","close","volume")}| \
               {"datetime": v.get("datetime","")}

    def _process_tick(self, price: float, ts: float):
        bar_ts = int(ts // self._bar_sec) * self._bar_sec
        with self._lock:
            if self._current_bar is None:
                self._current_bar = {"open":price,"high":price,"low":price,
                    "close":price,"volume":1.0,"bar_ts":bar_ts,
                    "datetime":datetime.fromtimestamp(bar_ts,tz=WIB).isoformat()}
            elif bar_ts > self._current_bar["bar_ts"]:
                self._bars.append(dict(self._current_bar))
                if len(self._bars) > MIN_BARS_CME*3:
                    self._bars = self._bars[-(MIN_BARS_CME*2):]
                self._current_bar = {"open":price,"high":price,"low":price,
                    "close":price,"volume":1.0,"bar_ts":bar_ts,
                    "datetime":datetime.fromtimestamp(bar_ts,tz=WIB).isoformat()}
                if self.on_bar_close and len(self._bars) >= MIN_BARS_CME:
                    snap = list(self._bars[-MIN_BARS_CME:])
                    threading.Thread(target=self.on_bar_close, args=(snap,), daemon=True).start()
            else:
                self._current_bar["high"]   = max(self._current_bar["high"], price)
                self._current_bar["low"]    = min(self._current_bar["low"],  price)
                self._current_bar["close"]  = price
                self._current_bar["volume"] += 1.0

    def _append_bar(self, bar: Dict):
        with self._lock:
            if not self._bars or bar["datetime"] != self._bars[-1]["datetime"]:
                self._bars.append(bar)
                if len(self._bars) > MIN_BARS_CME*3:
                    self._bars = self._bars[-(MIN_BARS_CME*2):]
                if self.on_bar_close and len(self._bars) >= MIN_BARS_CME:
                    snap = list(self._bars[-MIN_BARS_CME:])
                    threading.Thread(target=self.on_bar_close, args=(snap,), daemon=True).start()


# ═══════════════════════════════════════════════════════════════
# MAIN ENGINE
# ═══════════════════════════════════════════════════════════════
def run_engine(
    symbol:   str = SYMBOL,
    api_key:  str = TWELVEDATA_KEY,
    interval: str = INTERVAL,
    journal:  Optional[TradeJournal]       = None,
    adaptive: Optional[AdaptiveController] = None,
) -> None:
    """Entry point utama PEMIF v22.0.

    Flow per bar close:
        1. Ambil bars dari PriceStream
        2. analyze() dengan AdaptiveController
        3. Jika gate_ok → kirim Telegram HIGH PROBABILITY SIGNAL
        4. Jika OFF-KZ → log info + kirim status quiet (opsional)
        5. Setiap jam baru → kirim jadwal Kill Zone
    """
    if journal  is None: journal  = TradeJournal()
    if adaptive is None: adaptive = AdaptiveController(journal, symbol)

    log.info("PEMIF v22.0 Engine starting: %s @ %s", symbol, interval)

    # Kirim jadwal KZ saat startup
    now_wib   = datetime.now(WIB)
    kz_msg    = fmt_kz_schedule(now_wib)
    send_telegram(kz_msg)

    _last_kz_hour: Dict[str, int] = {"h": -1}

    def on_bar_close(bars: List[Dict]) -> None:
        nonlocal _last_kz_hour
        try:
            now    = datetime.now(WIB)
            stats  = journal.get_stats(symbol=symbol, last_n=100)
            price  = bars[-1]["close"] if bars else 0.0

            raw: Dict = {
                "direction":     "BUY",
                "current_price": price,
                "news_ok":       True,
                "news_tier":     0,
                # ↓ Inject dari external feed HTF/structure di production
            }

            sig = analyze(raw, bars, symbol, adaptive=adaptive)

            # Kirim jadwal KZ sekali per jam
            if now.hour != _last_kz_hour["h"]:
                _last_kz_hour["h"] = now.hour
                send_telegram(fmt_kz_schedule(now))

            if sig.gate_ok and sig.order.valid:
                msg = fmt_signal_telegram(sig, symbol, stats)
                log.info("SIGNAL: %s %s entry=%.2f ML=%.0f%%",
                         sig.direction, sig.order.order_type,
                         sig.order.entry, sig.ml_win_prob*100)
                send_telegram(msg)
            else:
                log.info("No signal: %s | EPS=%d QCM=%d CMS=%.1f ML=%.0f%%",
                         sig.veto_rsn, sig.eps_score,
                         sig.qcm_score, sig.cms_score, sig.ml_win_prob*100)

        except Exception as e:
            log.exception("Error on_bar_close: %s", e)

    stream = PriceStream(
        symbol=symbol,
        api_key=api_key,
        interval=interval,
        on_bar_close=on_bar_close,
    )

    try:
        stream.start()
        log.info("Engine aktif. Ctrl+C untuk stop.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutdown.")
    finally:
        stream.stop()
        log.info("PEMIF v22.0 stopped.")


# ═══════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    run_engine(symbol=SYMBOL, api_key=TWELVEDATA_KEY, interval=INTERVAL)
