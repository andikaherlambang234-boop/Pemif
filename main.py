#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PEMIF v21.0 PRECISION ENGINE
════════════════════════════════════════════════════════════════
Refactor total dari v20.4:
  - WebSocket streaming real-time (TwelveData WSS)
  - Auto-calculation pending order type & entry zone
  - Dynamic SL (ATR Wilder's RMA + swing structure)
  - TP berjenjang via Fibonacci Extension
  - Fisher Transform fix (threshold ketat)
  - TTM Squeeze histogram fix (aligned window)
  - STC full-cycle window
  - ATR: Wilder's RMA bukan simple average
  - Trade journal dengan persistence JSON
  - Telegram template presisi tinggi

Python  : >= 3.9
Author  : PEMIF Engine v21.0
"""
from __future__ import annotations

import json
import logging
import math
import os
import queue
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

# WebSocket — install: pip install websocket-client
try:
    import websocket
    WS_AVAILABLE = True
except ImportError:
    WS_AVAILABLE = False
    logging.warning(
        "websocket-client tidak terinstall. "
        "Fallback ke REST polling. "
        "Install dengan: pip install websocket-client"
    )

__all__ = [
    "Signal", "HistoricalStats", "TradeJournal",
    "PriceStream", "PendingOrderEngine",
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
log = logging.getLogger("PEMIF-v21.0")

# ═══════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════
WIB = timezone(timedelta(hours=7))

# Score ceilings
MAX_EPS:  int   = 4
MAX_SQS:  float = 10.0
MAX_CTX:  int   = 8
MAX_SOFT: int   = 8
MAX_QCM:  int   = 100

# Scalp thresholds
SCALP_EPS_MIN:   int   = 2
SCALP_QCM_MIN:   int   = 55
SCALP_CMS_MIN:   float = 3.5   # dinaikkan dari 3.0
SCALP_CMS_L3:    float = 3.5

# Momentum constants
LAGUERRE_GAMMA:  float = 0.5
BB_PERIOD:       int   = 20
BB_MULT:         float = 2.0
KC_MULT:         float = 1.5
ATR_PERIOD:      int   = 14
FISHER_PERIOD:   int   = 9
FISHER_EXTREME:  float = 1.5   # threshold ketat — hanya extreme reversal
STC_FAST:        int   = 23
STC_SLOW:        int   = 50
STC_CYCLE:       int   = 10    # STC stochastic cycle length
STC_OVERSOLD:    float = 25.0  # diperketat dari 30
STC_OVERBOUGHT:  float = 75.0  # diperketat dari 70

# Entry zone tolerance (% dari ATR)
OB_ENTRY_ATR_MULT:    float = 0.3   # Buy/Sell Limit masuk di 30% ATR dari OB
BREAKOUT_ATR_MULT:    float = 0.1   # Buy/Sell Stop di 10% ATR di atas swing
LIMIT_ATR_BUFFER:     float = 0.2   # Buffer untuk menghindari noise
SL_ATR_MULT:          float = 1.5   # SL = 1.5× ATR dari entry
SL_SWING_BUFFER:      float = 0.3   # Buffer extra di luar swing (poin)

# Fibonacci TP extensions
FIB_TP1: float = 1.0   # 100% dari risk
FIB_TP2: float = 1.618  # Golden ratio
FIB_TP3: float = 2.618  # Full extension

# Minimum bar counts
MIN_BARS_CME: int = 60   # dinaikkan untuk stabilitas ATR RMA
MIN_BARS_STC: int = 55

# WebSocket / REST
WS_RECONNECT_DELAY: float = 5.0
REST_POLL_INTERVAL: float = 10.0   # detik, untuk fallback
TICK_BUFFER_SIZE:   int   = 500    # tick disimpan dalam ring buffer

# Telegram
TELEGRAM_MAX_RETRY:   int   = 3
TELEGRAM_RETRY_DELAY: float = 1.5
TELEGRAM_TIMEOUT:     int   = 10

# Trade Journal
JOURNAL_PATH: Path = Path("pemif_trade_journal.json")

# Kill Zones (WIB)
KILL_ZONES: Tuple[Tuple, ...] = (
    ("Asia KZ",     2,  0,  5,  0),
    ("London KZ",  14,  0, 17,  0),
    ("NY Open KZ", 19, 30, 22,  0),
    ("NY PM KZ",   23,  0,  1,  0),
)
TRADING_WEEKDAYS: Tuple[int, ...] = (0, 1, 2, 3, 4)


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
# DATACLASSES
# ═══════════════════════════════════════════════════════════════
@dataclass
class HistoricalStats:
    """Performa historis dari trade journal nyata."""
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
    """Level-level kalkulasi otomatis untuk satu pending order."""
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
class Signal:
    """Representasi sinyal PEMIF v21.0."""
    # Core
    direction:   str   = "NONE"
    gate_ok:     bool  = False
    veto_rsn:    str   = "WAITING"
    grade:       str   = "STANDARD"

    # Order levels (kalkulasi otomatis)
    order:       OrderLevels = field(default_factory=OrderLevels)

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
    ob_bull_high:    float = 0.0    # batas atas OB bull
    ob_bull_low:     float = 0.0    # batas bawah OB bull
    ob_bear_high:    float = 0.0
    ob_bear_low:     float = 0.0
    swing_high:      float = 0.0    # swing tertinggi recent
    swing_low:       float = 0.0    # swing terendah recent
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

    # CME components
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

    # News — default fail-safe
    news_ok:   bool = False
    news_tier: int  = 0

    # Current market data
    current_price: float = 0.0
    atr_current:   float = 0.0


# ═══════════════════════════════════════════════════════════════
# TRADE JOURNAL — Persistent JSON
# ═══════════════════════════════════════════════════════════════
class TradeJournal:
    """Persistent trade journal untuk kalkulasi HistoricalStats nyata.

    Format JSON:
        {
          "trades": [
            {"id": 1, "symbol": "XAU/USD", "direction": "BUY",
             "entry": 3312.5, "sl": 3305.0, "tp_hit": "TP2",
             "result": "WIN", "rr_achieved": 1.618,
             "timestamp": "2025-06-01T14:30:00+07:00"},
            ...
          ]
        }
    """

    def __init__(self, path: Path = JOURNAL_PATH) -> None:
        self.path   = path
        self._lock  = threading.Lock()
        self._data  = self._load()

    def _load(self) -> Dict:
        if self.path.exists():
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError) as e:
                log.warning("Journal rusak, reset: %s", e)
        return {"trades": []}

    def _save(self) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2, ensure_ascii=False)

    def add_trade(
        self,
        symbol: str,
        direction: str,
        entry: float,
        sl: float,
        tp_hit: str,      # "TP1" / "TP2" / "TP3" / "SL"
        rr_achieved: float,
    ) -> None:
        """Catat hasil trade yang sudah selesai."""
        with self._lock:
            result = "WIN" if tp_hit.startswith("TP") else "LOSS"
            trade  = {
                "id":           len(self._data["trades"]) + 1,
                "symbol":       symbol,
                "direction":    direction,
                "entry":        entry,
                "sl":           sl,
                "tp_hit":       tp_hit,
                "result":       result,
                "rr_achieved":  rr_achieved,
                "timestamp":    datetime.now(WIB).isoformat(),
            }
            self._data["trades"].append(trade)
            self._save()
            log.info("Trade #%d dicatat: %s %s", trade["id"], result, direction)

    def get_stats(self, symbol: str = "", last_n: int = 100) -> HistoricalStats:
        """Hitung HistoricalStats dari trade journal nyata.

        Args:
            symbol: Filter per symbol (kosong = semua).
            last_n: Ambil N trade terakhir saja.

        Returns:
            HistoricalStats dari data aktual.
        """
        with self._lock:
            trades = self._data["trades"]
            if symbol:
                trades = [t for t in trades if t.get("symbol") == symbol]
            trades = trades[-last_n:]

            total  = len(trades)
            if total == 0:
                return HistoricalStats()

            wins     = [t for t in trades if t.get("result") == "WIN"]
            win_rrs  = [t.get("rr_achieved", 0.0) for t in wins]
            winrate  = round(len(wins) / total * 100, 1)
            avg_rr   = round(sum(win_rrs) / len(win_rrs), 2) if win_rrs else 0.0

            return HistoricalStats(
                total=total, winrate=winrate, avg_rr=avg_rr
            )


# ═══════════════════════════════════════════════════════════════
# REAL-TIME PRICE STREAM
# ═══════════════════════════════════════════════════════════════
class PriceStream:
    """Abstraksi real-time price feed.

    Primary  : TwelveData WebSocket (jika WS_AVAILABLE & td_key ada)
    Fallback : TwelveData REST polling setiap REST_POLL_INTERVAL detik

    Penggunaan:
        stream = PriceStream(symbol="XAU/USD", api_key=TWELVEDATA_KEY)
        stream.start()
        bars = stream.get_bars(60)   # ambil 60 bar OHLCV terakhir
        stream.stop()
    """

    def __init__(
        self,
        symbol: str,
        api_key: str,
        interval: str = "1min",
        on_bar_close: Optional[callable] = None,
    ) -> None:
        self.symbol        = symbol
        self.api_key       = api_key
        self.interval      = interval
        self.on_bar_close  = on_bar_close   # callback(bars: List[Dict])

        self._tick_q:  queue.Queue = queue.Queue(maxsize=TICK_BUFFER_SIZE)
        self._bars:    List[Dict]  = []
        self._lock     = threading.Lock()
        self._stop_evt = threading.Event()
        self._ws_app: Optional[object] = None
        self._threads: List[threading.Thread] = []

        # Bar builder state
        self._current_bar: Optional[Dict] = None
        self._bar_interval_sec = self._parse_interval(interval)

    # ── Interval parser ───────────────────────────────────────
    @staticmethod
    def _parse_interval(interval: str) -> int:
        """Konversi string interval ke detik."""
        mapping = {
            "1min": 60, "5min": 300, "15min": 900,
            "30min": 1800, "1h": 3600, "4h": 14400, "1day": 86400,
        }
        return mapping.get(interval, 60)

    # ── Public API ────────────────────────────────────────────
    def start(self) -> None:
        """Mulai streaming. Gunakan WebSocket jika tersedia, else REST."""
        log.info("PriceStream: starting untuk %s (%s)", self.symbol, self.interval)
        self._stop_evt.clear()

        # Ambil historical bars dulu via REST untuk warmup CME
        self._fetch_initial_bars()

        if WS_AVAILABLE and self.api_key:
            t = threading.Thread(
                target=self._run_websocket, daemon=True, name="ws-feed"
            )
        else:
            log.info("PriceStream: mode REST polling (fallback).")
            t = threading.Thread(
                target=self._run_rest_polling, daemon=True, name="rest-feed"
            )

        self._threads.append(t)
        t.start()

    def stop(self) -> None:
        """Stop semua thread stream."""
        self._stop_evt.set()
        if self._ws_app:
            try:
                self._ws_app.close()
            except Exception:
                pass
        log.info("PriceStream: stopped.")

    def get_bars(self, n: int = MIN_BARS_CME) -> List[Dict]:
        """Return N bar OHLCV terakhir (thread-safe)."""
        with self._lock:
            return list(self._bars[-n:])

    def get_latest_price(self) -> float:
        """Return harga close terakhir."""
        with self._lock:
            if self._bars:
                return float(self._bars[-1].get("close", 0.0))
        return 0.0

    # ── WebSocket Implementation ──────────────────────────────
    def _run_websocket(self) -> None:
        """Loop WebSocket dengan auto-reconnect."""
        url = "wss://ws.twelvedata.com/v1/quotes/price"

        while not self._stop_evt.is_set():
            try:
                self._ws_app = websocket.WebSocketApp(
                    url,
                    on_open=self._ws_on_open,
                    on_message=self._ws_on_message,
                    on_error=self._ws_on_error,
                    on_close=self._ws_on_close,
                )
                self._ws_app.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                log.warning("WebSocket error: %s. Reconnect dalam %.1fs.", e, WS_RECONNECT_DELAY)

            if not self._stop_evt.is_set():
                time.sleep(WS_RECONNECT_DELAY)

    def _ws_on_open(self, ws) -> None:
        symbol_td = self.symbol.replace("/", "")
        sub_msg   = json.dumps({
            "action": "subscribe",
            "params": {
                "symbols": symbol_td,
                "apikey":  self.api_key,
            },
        })
        ws.send(sub_msg)
        log.info("WebSocket: subscribed ke %s", self.symbol)

    def _ws_on_message(self, ws, message: str) -> None:
        try:
            data = json.loads(message)
            price = float(data.get("price", 0.0))
            ts    = data.get("timestamp", time.time())
            if price > 0:
                self._process_tick(price, float(ts))
        except (json.JSONDecodeError, ValueError, TypeError) as e:
            log.debug("WS message parse error: %s", e)

    def _ws_on_error(self, ws, error) -> None:
        log.warning("WebSocket error: %s", error)

    def _ws_on_close(self, ws, code, msg) -> None:
        log.info("WebSocket closed: %s %s", code, msg)

    # ── REST Polling Fallback ─────────────────────────────────
    def _run_rest_polling(self) -> None:
        """Polling REST setiap REST_POLL_INTERVAL detik."""
        while not self._stop_evt.is_set():
            try:
                self._fetch_latest_bar()
            except Exception as e:
                log.warning("REST poll error: %s", e)
            self._stop_evt.wait(timeout=REST_POLL_INTERVAL)

    def _fetch_latest_bar(self) -> None:
        """Ambil 1 bar terbaru via REST dan update buffer."""
        if not self.api_key:
            return
        url    = "https://api.twelvedata.com/time_series"
        params = {
            "symbol":     self.symbol,
            "interval":   self.interval,
            "outputsize": 1,
            "apikey":     self.api_key,
        }
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        values = data.get("values", [])
        if not values:
            return

        bar = self._parse_td_bar(values[0])
        self._append_bar(bar)

    def _fetch_initial_bars(self) -> None:
        """Ambil MIN_BARS_CME bar historis untuk warmup."""
        if not self.api_key:
            log.warning("Tidak ada API key — bars kosong, CME tidak akan berjalan.")
            return
        url    = "https://api.twelvedata.com/time_series"
        params = {
            "symbol":     self.symbol,
            "interval":   self.interval,
            "outputsize": MIN_BARS_CME + 10,
            "apikey":     self.api_key,
            "order":      "ASC",
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            values = data.get("values", [])
            with self._lock:
                self._bars = [self._parse_td_bar(v) for v in values]
            log.info("Initial bars loaded: %d candles.", len(self._bars))
        except Exception as e:
            log.error("Gagal fetch initial bars: %s", e)

    @staticmethod
    def _parse_td_bar(v: Dict) -> Dict:
        """Parse TwelveData OHLCV response ke format internal."""
        return {
            "open":      float(v.get("open",   0)),
            "high":      float(v.get("high",   0)),
            "low":       float(v.get("low",    0)),
            "close":     float(v.get("close",  0)),
            "volume":    float(v.get("volume", 0)),
            "datetime":  v.get("datetime", ""),
        }

    # ── Bar Builder dari Ticks ────────────────────────────────
    def _process_tick(self, price: float, ts: float) -> None:
        """Akumulasi tick menjadi OHLCV bar berdasarkan interval."""
        bar_ts = int(ts // self._bar_interval_sec) * self._bar_interval_sec

        with self._lock:
            if self._current_bar is None:
                self._current_bar = {
                    "open": price, "high": price,
                    "low": price, "close": price,
                    "volume": 1.0, "bar_ts": bar_ts,
                    "datetime": datetime.fromtimestamp(bar_ts, tz=WIB).isoformat(),
                }
            elif bar_ts > self._current_bar["bar_ts"]:
                # Bar selesai — simpan dan mulai bar baru
                finished = dict(self._current_bar)
                self._bars.append(finished)
                # Trim buffer agar tidak unbounded
                if len(self._bars) > MIN_BARS_CME * 3:
                    self._bars = self._bars[-(MIN_BARS_CME * 2):]

                self._current_bar = {
                    "open": price, "high": price,
                    "low": price, "close": price,
                    "volume": 1.0, "bar_ts": bar_ts,
                    "datetime": datetime.fromtimestamp(bar_ts, tz=WIB).isoformat(),
                }
                # Trigger callback di thread terpisah agar tidak block WS
                if self.on_bar_close and len(self._bars) >= MIN_BARS_CME:
                    bars_snapshot = list(self._bars[-MIN_BARS_CME:])
                    threading.Thread(
                        target=self.on_bar_close,
                        args=(bars_snapshot,),
                        daemon=True,
                    ).start()
            else:
                # Update bar berjalan
                self._current_bar["high"]   = max(self._current_bar["high"], price)
                self._current_bar["low"]    = min(self._current_bar["low"],  price)
                self._current_bar["close"]  = price
                self._current_bar["volume"] += 1.0

    def _append_bar(self, bar: Dict) -> None:
        """Append satu bar ke buffer (untuk REST polling)."""
        with self._lock:
            if (not self._bars or
                    bar.get("datetime") != self._bars[-1].get("datetime")):
                self._bars.append(bar)
                if len(self._bars) > MIN_BARS_CME * 3:
                    self._bars = self._bars[-(MIN_BARS_CME * 2):]

                if self.on_bar_close and len(self._bars) >= MIN_BARS_CME:
                    bars_snapshot = list(self._bars[-MIN_BARS_CME:])
                    threading.Thread(
                        target=self.on_bar_close,
                        args=(bars_snapshot,),
                        daemon=True,
                    ).start()


# ═══════════════════════════════════════════════════════════════
# ATR WILDER'S RMA (Fix dari v20.4)
# ═══════════════════════════════════════════════════════════════
def calc_atr_rma(bars: List[Dict], period: int = ATR_PERIOD) -> float:
    """Hitung ATR menggunakan Wilder's Smoothed Moving Average (RMA).

    Lebih stabil daripada simple average pada periode volatil.
    Digunakan sebagai dasar kalkulasi SL dan zona entry.

    Args:
        bars:   List OHLCV bars (minimal period + 1).
        period: ATR period (default 14).

    Returns:
        Float ATR terkini, atau 0.0 jika bars tidak cukup.
    """
    if len(bars) < period + 1:
        return 0.0

    # True Range series
    tr_series: List[float] = []
    for i in range(1, len(bars)):
        h  = float(bars[i]["high"])
        l  = float(bars[i]["low"])
        pc = float(bars[i - 1]["close"])
        tr_series.append(max(h - l, abs(h - pc), abs(l - pc)))

    if len(tr_series) < period:
        return 0.0

    # Seed: SMA dari period pertama
    atr = sum(tr_series[:period]) / period

    # Wilder's RMA untuk sisa bars
    multiplier = 1.0 / period
    for tr in tr_series[period:]:
        atr = tr * multiplier + atr * (1.0 - multiplier)

    return round(atr, 4)


# ═══════════════════════════════════════════════════════════════
# EMA SERIES HELPER
# ═══════════════════════════════════════════════════════════════
def _ema_series(data: List[float], period: int) -> List[float]:
    """EMA inkremental O(n) untuk seluruh series."""
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
# CME — COMPOSITE MOMENTUM ENGINE (Fixed)
# ═══════════════════════════════════════════════════════════════
def calc_cms(bars: List[Dict], direction: str) -> Dict:
    """CME dengan 4 fix utama vs v20.4:
        1. Fisher: hanya EXTREME reversal yang dihitung (bukan setiap > 0)
        2. TTM histogram: window sinkron antar bar
        3. STC: full cycle window STC_CYCLE
        4. ATR: Wilder's RMA (sudah via calc_atr_rma)
    """
    empty = {
        "ttm_fire": False, "lrsi_ok": False,
        "fisher_ok": False, "stc_ok": False,
        "cms_score": 0.0, "atr": 0.0,
    }

    if not bars or len(bars) < MIN_BARS_CME:
        log.warning(
            "calc_cms: butuh %d bars, dapat %d.",
            MIN_BARS_CME, len(bars) if bars else 0,
        )
        return empty

    closes = [float(b["close"]) for b in bars]
    highs  = [float(b["high"])  for b in bars]
    lows   = [float(b["low"])   for b in bars]
    n      = len(closes)

    # ── ATR Wilder's RMA ──────────────────────────────────────
    atr14 = calc_atr_rma(bars, ATR_PERIOD)

    # ── TTM Squeeze (window sinkron) ──────────────────────────
    bb_window    = closes[-BB_PERIOD:]
    bb_mid_now   = sum(bb_window) / BB_PERIOD
    bb_window_p  = closes[-(BB_PERIOD + 1):-1]   # window bar sebelumnya
    bb_mid_prev  = sum(bb_window_p) / BB_PERIOD

    bb_std = math.sqrt(
        sum((x - bb_mid_now) ** 2 for x in bb_window) / BB_PERIOD
    )
    bb_upper = bb_mid_now + BB_MULT * bb_std
    bb_lower = bb_mid_now - BB_MULT * bb_std

    kc_upper = bb_mid_now + KC_MULT * atr14
    kc_lower = bb_mid_now - KC_MULT * atr14

    squeeze_on       = (bb_upper < kc_upper) and (bb_lower > kc_lower)
    hist_now         = closes[-1]  - bb_mid_now    # sinkron: close vs mid dari window yg sama
    hist_prev        = closes[-2]  - bb_mid_prev   # FIX: close[-2] vs mid window yg sama
    hist_increasing  = hist_now > hist_prev

    if direction == "BUY":
        ttm_fire = (not squeeze_on) and (hist_now > 0) and hist_increasing
    else:
        ttm_fire = (not squeeze_on) and (hist_now < 0) and (not hist_increasing)

    # ── Laguerre RSI ──────────────────────────────────────────
    gamma = LAGUERRE_GAMMA
    L0 = L1 = L2 = L3 = closes[0]
    for c in closes:
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
    lrsi     = cu / (cu + cd) if (cu + cd) > 1e-10 else 0.5
    lrsi_ok  = (lrsi > 0.55) if direction == "BUY" else (lrsi < 0.45)

    # ── Fisher Transform (FIX: hanya extreme reversal) ────────
    fp     = FISHER_PERIOD
    hh     = max(highs[-fp:])
    ll     = min(lows[-fp:])
    fisher = 0.0
    if hh != ll:
        raw_val = 2.0 * ((closes[-1] - ll) / (hh - ll)) - 1.0
        raw_val = max(min(raw_val, 0.999), -0.999)
        fisher  = 0.5 * math.log((1.0 + raw_val) / (1.0 - raw_val))

    # FIX v21.0: hanya hitung ok jika benar-benar extreme reversal
    # (bukan setiap nilai positif/negatif seperti di v20.4)
    fisher_ok = (
        (fisher < -FISHER_EXTREME and direction == "BUY") or
        (fisher >  FISHER_EXTREME and direction == "SELL")
    )

    # ── STC Proxy (FIX: full cycle window) ───────────────────
    stc = 50.0
    if n >= MIN_BARS_STC:
        ema_fast    = _ema_series(closes, STC_FAST)
        ema_slow    = _ema_series(closes, STC_SLOW)
        macd_series = [
            ema_fast[i] - ema_slow[i]
            for i in range(STC_SLOW - 1, n)
            if ema_slow[i] != 0.0
        ]
        if len(macd_series) >= STC_CYCLE:
            # FIX: gunakan STC_CYCLE (10), bukan hardcoded 10
            # — lebih eksplisit dan mudah di-tune
            window_macd = macd_series[-STC_CYCLE:]
            mh = max(window_macd)
            ml = min(window_macd)
            stc = (
                (macd_series[-1] - ml) / (mh - ml) * 100.0
                if mh != ml else 50.0
            )

    stc_ok = (stc < STC_OVERSOLD) if direction == "BUY" else (stc > STC_OVERBOUGHT)

    # ── CMS Aggregation ───────────────────────────────────────
    pts  = 0.0
    pts += 3.0 if ttm_fire  else 0.0
    pts += 2.5 if lrsi_ok   else 0.0   # dinaikkan sedikit (LRSI lebih reliable)
    pts += 2.5 if fisher_ok else 0.0   # fisher hanya extreme, tapi bernilai lebih
    pts += 2.0 if stc_ok    else 0.0
    cms  = round(min(pts / 10.0 * 10.0, 10.0), 2)

    return {
        "ttm_fire":  ttm_fire,
        "lrsi_ok":   lrsi_ok,
        "fisher_ok": fisher_ok,
        "stc_ok":    stc_ok,
        "cms_score": cms,
        "atr":       atr14,
    }


# ═══════════════════════════════════════════════════════════════
# PENDING ORDER ENGINE — Kalkulasi Otomatis
# ═══════════════════════════════════════════════════════════════
class PendingOrderEngine:
    """Engine kalkulasi otomatis pending order type, entry, SL, dan TP.

    Logika penentuan order type:
        BUY LIMIT  → harga di atas OB/Demand zone (pantulan bullish)
        SELL LIMIT → harga di bawah OB/Supply zone (pantulan bearish)
        BUY STOP   → breakout di atas swing high + buffer
        SELL STOP  → breakout di bawah swing low + buffer

    SL kalkulasi:
        = entry ± max(ATR × SL_ATR_MULT, jarak ke swing structure) + SL_SWING_BUFFER

    TP kalkulasi (Fibonacci Extension dari Risk):
        TP1 = entry ± risk × FIB_TP1   (1.0R — amankan modal)
        TP2 = entry ± risk × FIB_TP2   (1.618R — golden ratio)
        TP3 = entry ± risk × FIB_TP3   (2.618R — full extension)
    """

    def __init__(self, atr: float) -> None:
        """
        Args:
            atr: ATR saat ini dari calc_atr_rma().
        """
        self.atr = max(atr, 0.01)   # safeguard division-by-zero

    def calc(self, sig: "Signal") -> OrderLevels:
        """Hitung OrderLevels otomatis dari state Signal.

        Args:
            sig: Signal dataclass dengan field structure sudah terisi.

        Returns:
            OrderLevels dengan semua level terhitung.
        """
        direction = sig.direction
        if direction not in ("BUY", "SELL"):
            return OrderLevels(reason="NO DIRECTION")

        bull = direction == "BUY"

        # Pilih strategi order type
        if bull:
            return self._calc_bull(sig)
        else:
            return self._calc_bear(sig)

    # ── Bull Strategies ───────────────────────────────────────
    def _calc_bull(self, sig: "Signal") -> OrderLevels:
        """Hitung level untuk BUY LIMIT atau BUY STOP."""
        current = sig.current_price

        # Prioritas 1: BUY LIMIT di OB + FVG zone
        if (sig.ob_bull_valid or sig.fvg_bull_fresh) and sig.ob_bull_low > 0:
            return self._build_limit_order(
                direction="BUY",
                zone_high=sig.ob_bull_high,
                zone_low=sig.ob_bull_low,
                swing_ref=sig.swing_low,
                current_price=current,
                reason=f"BUY LIMIT di OB/FVG zone [{sig.ob_bull_low:.2f}–{sig.ob_bull_high:.2f}]",
            )

        # Prioritas 2: BUY STOP di atas swing high (breakout)
        if sig.swing_high > 0 and sig.bos_bull and sig.liq_swept_l:
            return self._build_stop_order(
                direction="BUY",
                breakout_level=sig.swing_high,
                swing_ref=sig.swing_low,
                reason=f"BUY STOP breakout swing high [{sig.swing_high:.2f}]",
            )

        # Fallback: tidak ada setup valid
        return OrderLevels(
            reason="Tidak ada zona OB/breakout valid untuk BUY",
            valid=False,
        )

    # ── Bear Strategies ───────────────────────────────────────
    def _calc_bear(self, sig: "Signal") -> OrderLevels:
        """Hitung level untuk SELL LIMIT atau SELL STOP."""
        current = sig.current_price

        # Prioritas 1: SELL LIMIT di OB Supply zone
        if (sig.ob_bear_valid or sig.fvg_bear_fresh) and sig.ob_bear_high > 0:
            return self._build_limit_order(
                direction="SELL",
                zone_high=sig.ob_bear_high,
                zone_low=sig.ob_bear_low,
                swing_ref=sig.swing_high,
                current_price=current,
                reason=f"SELL LIMIT di OB/FVG zone [{sig.ob_bear_low:.2f}–{sig.ob_bear_high:.2f}]",
            )

        # Prioritas 2: SELL STOP di bawah swing low (breakdown)
        if sig.swing_low > 0 and sig.bos_bear and sig.liq_swept_h:
            return self._build_stop_order(
                direction="SELL",
                breakout_level=sig.swing_low,
                swing_ref=sig.swing_high,
                reason=f"SELL STOP breakdown swing low [{sig.swing_low:.2f}]",
            )

        return OrderLevels(
            reason="Tidak ada zona OB/breakout valid untuk SELL",
            valid=False,
        )

    # ── Level Builders ────────────────────────────────────────
    def _build_limit_order(
        self,
        direction: str,
        zone_high: float,
        zone_low: float,
        swing_ref: float,
        current_price: float,
        reason: str,
    ) -> OrderLevels:
        """Bangun BUY LIMIT atau SELL LIMIT dengan SL & TP."""
        bull   = direction == "BUY"
        atr    = self.atr

        if bull:
            # Entry: 30% ATR dari atas zone (beri ruang agar terisi)
            entry = zone_high - atr * OB_ENTRY_ATR_MULT
            entry = max(entry, zone_low)   # jangan di bawah zone

            # SL: di bawah swing_low atau zone_low - buffer
            sl_candidate1 = zone_low - SL_SWING_BUFFER
            sl_candidate2 = entry - atr * SL_ATR_MULT
            sl = min(sl_candidate1, sl_candidate2)   # ambil lebih jauh
        else:
            # SELL LIMIT
            entry = zone_low + atr * OB_ENTRY_ATR_MULT
            entry = min(entry, zone_high)

            sl_candidate1 = zone_high + SL_SWING_BUFFER
            sl_candidate2 = entry + atr * SL_ATR_MULT
            sl = max(sl_candidate1, sl_candidate2)

        return self._finalize_levels(
            order_type=f"{direction} LIMIT",
            direction=direction,
            entry=entry,
            sl=sl,
            reason=reason,
        )

    def _build_stop_order(
        self,
        direction: str,
        breakout_level: float,
        swing_ref: float,
        reason: str,
    ) -> OrderLevels:
        """Bangun BUY STOP atau SELL STOP untuk breakout."""
        bull = direction == "BUY"
        atr  = self.atr

        if bull:
            # Entry: 10% ATR di atas swing high (konfirmasi penembusan)
            entry = breakout_level + atr * BREAKOUT_ATR_MULT
            sl    = swing_ref - SL_SWING_BUFFER   # di bawah swing low
            sl    = min(sl, entry - atr * SL_ATR_MULT)
        else:
            entry = breakout_level - atr * BREAKOUT_ATR_MULT
            sl    = swing_ref + SL_SWING_BUFFER
            sl    = max(sl, entry + atr * SL_ATR_MULT)

        return self._finalize_levels(
            order_type=f"{direction} STOP",
            direction=direction,
            entry=entry,
            sl=sl,
            reason=reason,
        )

    def _finalize_levels(
        self,
        order_type: str,
        direction: str,
        entry: float,
        sl: float,
        reason: str,
    ) -> OrderLevels:
        """Hitung TP1/TP2/TP3 via Fibonacci Extension dari risk."""
        bull      = direction == "BUY"
        risk      = abs(entry - sl)

        if risk < 0.01:
            return OrderLevels(reason="Risk terlalu kecil (<0.01)", valid=False)

        if bull:
            tp1 = entry + risk * FIB_TP1
            tp2 = entry + risk * FIB_TP2
            tp3 = entry + risk * FIB_TP3
        else:
            tp1 = entry - risk * FIB_TP1
            tp2 = entry - risk * FIB_TP2
            tp3 = entry - risk * FIB_TP3

        rr_tp1 = round(FIB_TP1,  3)
        rr_tp2 = round(FIB_TP2,  3)
        rr_tp3 = round(FIB_TP3,  3)

        return OrderLevels(
            order_type=order_type,
            entry=round(entry, 2),
            sl=round(sl,    2),
            tp1=round(tp1,  2),
            tp2=round(tp2,  2),
            tp3=round(tp3,  2),
            rr_tp1=rr_tp1,
            rr_tp2=rr_tp2,
            rr_tp3=rr_tp3,
            risk_pips=round(risk, 2),
            atr_current=round(self.atr, 4),
            reason=reason,
            valid=True,
        )


# ═══════════════════════════════════════════════════════════════
# KILL ZONE
# ═══════════════════════════════════════════════════════════════
def check_killzone(now: datetime) -> Tuple[bool, str]:
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
            if total_m >= start or total_m < end:
                return True, name

    return False, "-"


# ═══════════════════════════════════════════════════════════════
# EPS ENGINE
# ═══════════════════════════════════════════════════════════════
def calc_eps(sig_data: Dict) -> Dict:
    d    = sig_data.get("direction", "BUY")
    bull = d == "BUY"

    bos_ok   = bool(sig_data.get("bos_bull" if bull else "bos_bear", False))
    choch_ok = sig_data.get("h1_bias") == ("BUL" if bull else "BER")
    l1       = bos_ok or choch_ok

    ob_ok  = bool(sig_data.get("ob_bull_valid" if bull else "ob_bear_valid", False))
    fvg_ok = bool(sig_data.get("fvg_bull_fresh" if bull else "fvg_bear_fresh", False))
    l2     = ob_ok or fvg_ok

    l3 = float(sig_data.get("cms_score", 0.0)) >= SCALP_CMS_L3

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
_HTF_SCORE_MAP: Dict[int, int] = {3: 20, 2: 14, 1: 7, 0: 0}
_MTF_SCORE_MAP: Dict[int, int] = {3: 12, 2: 8,  1: 4, 0: 0}


def calc_qcm(sig_data: Dict) -> int:
    score = 0
    d    = sig_data.get("direction", "BUY")
    bull = d == "BUY"
    tb   = "BUL" if bull else "BER"

    htf_cnt = sum([
        sig_data.get("d1_bias")  == tb,
        sig_data.get("h4_bias")  == tb,
        sig_data.get("h1_bias")  == tb,
    ])
    score += _HTF_SCORE_MAP.get(htf_cnt, 0)

    mtf_cnt = sum([
        sig_data.get("m30_bias") == tb,
        sig_data.get("m15_bias") == tb,
        sig_data.get("m5_bias")  == tb,
    ])
    score += _MTF_SCORE_MAP.get(mtf_cnt, 0)

    pd_priority = max(0, int(sig_data.get("pd_priority", 0)))
    score      += max(0, 15 - pd_priority)

    bos_ok = bool(sig_data.get("bos_bull" if bull else "bos_bear", False))
    liq_ok = bool(sig_data.get("liq_swept_l" if bull else "liq_swept_h", False))
    score += (5 if bos_ok else 0) + (5 if liq_ok else 0)

    disp_ok = bool(sig_data.get("disp_ok", False))
    sfp_ok  = sig_data.get("sfp_signal") in ("BULL", "BEAR")
    score  += (4 if disp_ok else 0) + (4 if sfp_ok else 0)

    vol_ok   = bool(sig_data.get("vol_surge", False))
    not_chop = not bool(sig_data.get("acf_chop", False))
    score   += (5 if vol_ok else 0) + (3 if not_chop else 0)

    score += 10 if bool(sig_data.get("in_killzone", False)) else 3

    cms     = float(sig_data.get("cms_score", 0.0))
    score  += int(min(cms / 10.0 * 15, 15))

    return min(score, MAX_QCM)


# ═══════════════════════════════════════════════════════════════
# GRADE & GATE
# ═══════════════════════════════════════════════════════════════
def calc_grade(eps: int, qcm: int) -> str:
    if eps >= MAX_EPS and qcm >= 85:
        return "PRIME"
    elif eps >= 3 and qcm >= 70:
        return "HIGH"
    return "STANDARD"


def check_gate(sig_data: Dict) -> Tuple[bool, str]:
    d    = sig_data.get("direction", "NONE")
    eps  = int(sig_data.get("eps_score",  0))
    qcm  = int(sig_data.get("qcm_score",  0))
    cms  = float(sig_data.get("cms_score", 0.0))
    bull = d == "BUY"

    if d == "NONE":
        return False, "NO DIRECTION"

    if not bool(sig_data.get("news_ok", False)):
        tier = int(sig_data.get("news_tier", 0))
        if tier >= 1:
            return False, f"NEWS TIER-{tier} BLOCK"

    bos_ok = bool(sig_data.get("bos_bull" if bull else "bos_bear", False))
    htf_ok = (
        sig_data.get("h4_bias") == ("BUL" if bull else "BER") or
        sig_data.get("h1_bias") == ("BUL" if bull else "BER")
    )
    if not bos_ok and not htf_ok:
        return False, "STRUKTUR HTF BELUM ALIGNED"

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

    # v21.0 tambahan: order levels harus valid
    order_valid = sig_data.get("order_valid", False)
    if not order_valid:
        return False, "PENDING ORDER LEVEL TIDAK VALID"

    return True, "OK"


# ═══════════════════════════════════════════════════════════════
# INPUT VALIDATION
# ═══════════════════════════════════════════════════════════════
_BOOL_FIELDS = (
    "bos_bull", "bos_bear", "fvg_bull_fresh", "fvg_bear_fresh",
    "ob_bull_valid", "ob_bear_valid", "liq_swept_l", "liq_swept_h",
    "disp_ok", "vol_surge", "acf_chop", "pdc_ok",
    "harmonic_pcz", "news_ok",
)
_FLOAT_FIELDS = (
    "entry", "sl", "tp1", "tp2", "tp3",
    "rr", "risk", "abe_level", "cms_score",
    "ob_bull_high", "ob_bull_low", "ob_bear_high", "ob_bear_low",
    "swing_high", "swing_low", "current_price",
)
_INT_FIELDS = (
    "expiry_min", "pd_priority", "fractal_conv", "news_tier",
)


def _validate_raw(raw: Dict) -> Dict:
    validated = dict(raw)
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
                raise TypeError(f"Field '{f}' tidak bisa ke float: {validated[f]!r}") from exc

    for f in _INT_FIELDS:
        if f in validated:
            try:
                validated[f] = int(validated[f])
            except (TypeError, ValueError) as exc:
                raise TypeError(f"Field '{f}' tidak bisa ke int: {validated[f]!r}") from exc

    return validated


# ═══════════════════════════════════════════════════════════════
# MASTER ANALYZE
# ═══════════════════════════════════════════════════════════════
def analyze(raw: Dict, bars: List[Dict], symbol: str) -> Signal:
    """Engine utama v21.0: tambah kalkulasi PendingOrderEngine otomatis."""
    raw = _validate_raw(raw)
    sig = Signal()

    _copy_fields = (
        "direction", "d1_bias", "h4_bias", "h1_bias",
        "m30_bias", "m15_bias", "m5_bias", "m1_bias",
        "bos_bull", "bos_bear", "fvg_bull_fresh", "fvg_bear_fresh",
        "ob_bull_valid", "ob_bear_valid",
        "ob_bull_high", "ob_bull_low", "ob_bear_high", "ob_bear_low",
        "swing_high", "swing_low",
        "liq_swept_l", "liq_swept_h",
        "disp_ok", "sfp_signal", "vol_surge", "acf_chop", "pdc_ok",
        "src", "order_type",
        "pd_type", "pd_priority", "fractal_conv", "harmonic_pcz",
        "news_ok", "news_tier", "current_price",
    )
    for fname in _copy_fields:
        if fname in raw:
            setattr(sig, fname, raw[fname])

    # Kill Zone
    now_wib = datetime.now(WIB)
    sig.in_killzone, sig.kz_name = check_killzone(now_wib)

    # CME
    cme = calc_cms(bars, sig.direction)
    sig.ttm_fire     = cme["ttm_fire"]
    sig.lrsi_ok      = cme["lrsi_ok"]
    sig.fisher_ok    = cme["fisher_ok"]
    sig.stc_ok       = cme["stc_ok"]
    sig.cms_score    = cme["cms_score"]
    sig.atr_current  = cme["atr"]

    # EPS
    snap = asdict(sig)
    eps  = calc_eps(snap)
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
    ctx += 2 if (sig.bos_bull or sig.bos_bear)            else 0
    ctx += 2 if (sig.fvg_bull_fresh or sig.fvg_bear_fresh) else 0
    ctx += 2 if (sig.ob_bull_valid or sig.ob_bear_valid)   else 0
    ctx += 1 if (sig.liq_swept_l or sig.liq_swept_h)      else 0
    ctx += 1 if sig.disp_ok                                else 0
    sig.ctx_score = min(ctx, MAX_CTX)

    soft = 0
    soft += 2 if sig.sfp_signal in ("BULL", "BEAR") else 0
    soft += 2 if sig.vol_surge                        else 0
    soft += 1 if not sig.acf_chop                     else 0
    soft += 1 if sig.pdc_ok                           else 0
    soft += 1 if sig.harmonic_pcz                     else 0
    soft += 1 if sig.fractal_conv >= 3                else 0
    sig.soft_count = min(soft, MAX_SOFT)

    sig.grade = calc_grade(sig.eps_score, sig.qcm_score)

    # ── Pending Order Kalkulasi Otomatis ──────────────────────
    order_engine = PendingOrderEngine(atr=sig.atr_current)
    sig.order    = order_engine.calc(sig)

    # Gate check
    snap = asdict(sig)
    snap["order_valid"] = sig.order.valid

    if not sig.in_killzone:
        sig.gate_ok  = False
        sig.veto_rsn = "OFF-KZ"
        return sig

    gate_ok, gate_reason = check_gate(snap)
    sig.gate_ok  = gate_ok
    sig.veto_rsn = gate_reason if not gate_ok else "OK"

    return sig


# ═══════════════════════════════════════════════════════════════
# TELEGRAM FORMATTER — Template Presisi Tinggi
# ═══════════════════════════════════════════════════════════════
def _grade_stars(grade: str) -> str:
    return {"PRIME": "🔥🔥🔥", "HIGH": "⭐⭐", "STANDARD": "📶"}.get(grade, "")


def fmt_signal_telegram(sig: Signal, symbol: str, stats: HistoricalStats) -> str:
    """Format pesan Telegram untuk HIGH PROBABILITY SIGNAL.

    Hanya dipanggil jika sig.gate_ok = True dan sig.order.valid = True.
    Template mengikuti spesifikasi yang diminta.
    """
    now_wib = datetime.now(WIB).strftime("%d %b %Y | %H:%M WIB")
    o       = sig.order
    d       = sig.direction
    d_icon  = "🟢" if d == "BUY" else "🔴"

    # Alignment count
    tb      = "BUL" if d == "BUY" else "BER"
    biases  = [sig.d1_bias, sig.h4_bias, sig.h1_bias,
               sig.m30_bias, sig.m15_bias, sig.m5_bias, sig.m1_bias]
    aligned = sum(1 for b in biases if b == tb)

    # Confidence
    def _conf(eps, sqs, ctx, soft):
        e = min(eps / MAX_EPS,   1.0) * 30
        s = min(sqs / MAX_SQS,   1.0) * 30
        c = min(ctx / MAX_CTX,   1.0) * 20
        f = min(soft / MAX_SOFT, 1.0) * 20
        return round(e + s + c + f)

    conf = _conf(sig.eps_score, sig.sqs_score, sig.ctx_score, sig.soft_count)

    # Alasan teknikal ringkas
    reasons = []
    if sig.bos_bull or sig.bos_bear: reasons.append("BOS ✓")
    if sig.ob_bull_valid or sig.ob_bear_valid: reasons.append("Order Block ✓")
    if sig.fvg_bull_fresh or sig.fvg_bear_fresh: reasons.append("FVG ✓")
    if sig.liq_swept_l or sig.liq_swept_h: reasons.append("Liq Sweep ✓")
    if sig.ttm_fire:   reasons.append("TTM Squeeze ✓")
    if sig.lrsi_ok:    reasons.append("LRSI ✓")
    if sig.fisher_ok:  reasons.append("Fisher Extreme ✓")
    if sig.stc_ok:     reasons.append("STC ✓")
    if sig.in_killzone: reasons.append(f"{sig.kz_name} ✓")
    reason_str = " | ".join(reasons[:6])  # max 6 agar tidak terlalu panjang

    SEP = "═" * 33

    msg = (
        f"{SEP}\n"
        f"⚠️ {symbol} HIGH PROBABILITY SIGNAL ⚠️\n"
        f"{SEP}\n\n"
        f"{d_icon} <b>Type</b>     : <code>{o.order_type}</code>\n"
        f"📍 <b>Entry</b>    : <code>{o.entry:.2f}</code>\n\n"
        f"🛑 <b>Stop Loss</b>: <code>{o.sl:.2f}</code>  "
        f"(Risk: {o.risk_pips:.1f} pts | ATR: {o.atr_current:.2f})\n\n"
        f"🎯 <b>TP1</b>  : <code>{o.tp1:.2f}</code>  "
        f"[Konservatif | 1:{o.rr_tp1:.2f}R]\n"
        f"🎯 <b>TP2</b>  : <code>{o.tp2:.2f}</code>  "
        f"[Moderat     | 1:{o.rr_tp2:.2f}R]\n"
        f"🎯 <b>TP3</b>  : <code>{o.tp3:.2f}</code>  "
        f"[Agresif     | 1:{o.rr_tp3:.2f}R]\n\n"
        f"{SEP}\n\n"
        f"📋 <b>Notes</b>: {o.reason}\n"
        f"🔍 Konfirmasi: {reason_str}\n\n"
        f"{SEP}\n\n"
        f"📊 <b>QUALITY SCORE</b>\n\n"
        f"Grade       : {sig.grade} {_grade_stars(sig.grade)}\n"
        f"Confidence  : {conf}%\n"
        f"EPS         : {sig.eps_score}/{MAX_EPS}\n"
        f"QCM         : {sig.qcm_score}/{MAX_QCM}\n"
        f"CMS         : {sig.cms_score:.1f}/10\n"
        f"Alignment   : {aligned}/7 TF\n"
        f"Kill Zone   : {sig.kz_name}\n\n"
        f"{SEP}\n\n"
        f"📚 <b>HISTORICAL</b>  ({stats.total} trades)\n\n"
        f"Winrate     : {stats.winrate}%\n"
        f"Avg RR      : {stats.avg_rr}\n\n"
        f"{SEP}\n"
        f"🕒 {now_wib}\n"
        f"{SEP}"
    )
    return msg


def fmt_no_signal_telegram(sig: Signal, symbol: str) -> str:
    """Format pesan status ketika tidak ada sinyal valid."""
    now_wib = datetime.now(WIB).strftime("%H:%M WIB")
    return (
        f"🤖 <b>PEMIF v21.0</b> | {symbol} | {now_wib}\n\n"
        f"Status  : {sig.veto_rsn}\n"
        f"KZ      : {sig.kz_name}\n"
        f"EPS     : {sig.eps_score}/{MAX_EPS}\n"
        f"QCM     : {sig.qcm_score}/{MAX_QCM}\n"
        f"CMS     : {sig.cms_score:.1f}/10"
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
            log.info("Telegram: terkirim (attempt %d).", attempt)
            return True

        except requests.exceptions.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 429:
                retry_after = int(exc.response.headers.get("Retry-After", 5))
                time.sleep(retry_after)
                continue
            log.warning("HTTP error attempt %d/%d.", attempt, max_retry)

        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError) as exc:
            log.warning("Telegram error attempt %d/%d: %s", attempt, max_retry, exc)

        if attempt < max_retry:
            time.sleep(TELEGRAM_RETRY_DELAY * (2 ** (attempt - 1)))

    log.error("Telegram: GAGAL setelah %d attempt.", max_retry)
    return False


# ═══════════════════════════════════════════════════════════════
# MAIN ENGINE — Real-Time Loop
# ═══════════════════════════════════════════════════════════════
def run_engine(
    symbol:  str = SYMBOL,
    api_key: str = TWELVEDATA_KEY,
    interval: str = INTERVAL,
    journal: Optional[TradeJournal] = None,
) -> None:
    """Entry point utama: stream data → analyze per bar close → alert.

    Args:
        symbol:   Instrumen trading, contoh "XAU/USD".
        api_key:  TwelveData API key.
        interval: Candle interval ("1min", "5min", dll).
        journal:  TradeJournal instance (optional, dibuat jika None).
    """
    if journal is None:
        journal = TradeJournal()

    log.info("PEMIF v21.0 Engine starting: %s @ %s", symbol, interval)

    def on_bar_close(bars: List[Dict]) -> None:
        """Callback per bar close — dipanggil dari thread stream."""
        try:
            # Ambil snapshot stats dari journal nyata
            stats = journal.get_stats(symbol=symbol, last_n=100)

            # Raw dict minimal — di production ini datang dari
            # feed HTF (D1/H4/H1 bias, structure flags, dll)
            # Untuk demo, kita simulasi raw sederhana
            current_price = bars[-1]["close"] if bars else 0.0
            raw: Dict = {
                "direction":      "BUY",   # diganti dari HTF bias engine
                "current_price":  current_price,
                "news_ok":        True,
                "news_tier":      0,
                # Field lain diisi dari external bias/structure feed
                # Di production: inject dari MT5 / TwelveData indicator output
            }

            sig = analyze(raw, bars, symbol)

            if sig.gate_ok and sig.order.valid:
                msg = fmt_signal_telegram(sig, symbol, stats)
                log.info(
                    "HIGH PROBABILITY SIGNAL: %s %s entry=%.2f",
                    sig.direction, sig.order.order_type, sig.order.entry,
                )
                send_telegram(msg)
            else:
                log.info(
                    "No signal: %s | EPS=%d QCM=%d CMS=%.1f",
                    sig.veto_rsn, sig.eps_score, sig.qcm_score, sig.cms_score,
                )

        except Exception as e:
            log.exception("Error dalam on_bar_close: %s", e)

    stream = PriceStream(
        symbol=symbol,
        api_key=api_key,
        interval=interval,
        on_bar_close=on_bar_close,
    )

    try:
        stream.start()
        log.info("Engine berjalan. Ctrl+C untuk stop.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutdown diminta.")
    finally:
        stream.stop()
        log.info("PEMIF v21.0 Engine stopped.")


# ═══════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    run_engine(
        symbol=SYMBOL,
        api_key=TWELVEDATA_KEY,
        interval=INTERVAL,
    )
