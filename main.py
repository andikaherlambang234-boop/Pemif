#!/usr/bin/env python3
"""
PEMIF v20 UNIFIED — GitHub Actions Signal Bot
Gabungan v18 SCALP ENGINE + v19 LITE+ dengan Buy Stop / Sell Stop
Timezone: WIB (UTC+7)
"""

import os
import json
import math
import time
import logging
import requests
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
TELEGRAM_TOKEN  = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHATID = os.environ.get("TELEGRAM_CHATID", "")
TWELVEDATA_KEY  = os.environ.get("TWELVEDATA_KEY", "")

SYMBOL    = os.environ.get("SYMBOL", "XAU/USD")
INTERVAL  = os.environ.get("INTERVAL", "5min")
OUTPUTSZ  = 200

WIB = timezone(timedelta(hours=7))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("PEMIF-v20")

# ═══════════════════════════════════════════════════════════════
# PARAMETERS — v18 + v19 MERGED
# ═══════════════════════════════════════════════════════════════
@dataclass
class Params:
    # Aggressiveness (1=Ultra-Cons, 5=Ultra-Aggr)
    aggr_level: int   = 3

    # Order Engine
    sl_min_atr:  float = 0.5
    sl_max_atr:  float = 2.0
    rr_tp1:      float = 1.5
    rr_tp2:      float = 3.0
    rr_tp3:      float = 5.0
    rr_tp4:      float = 8.0
    expiry_bars: int   = 20
    spread:      float = 0.30
    vol_mult:    float = 1.2
    min_eps:     int   = 4
    min_rr:      float = 2.0

    # FVG / OB
    fvg_age_max: int   = 12
    ob_age_max:  int   = 40
    fib_tol:     float = 0.4

    # ACF / Veto
    adx_chop_thr: int   = 18
    adr_pct_max:  float = 80.0

    # Session (WIB)
    sess_lon_hr: int  = 14
    sess_ny_hr:  int  = 20
    kz_asia:     bool = True

    # Entry Filters
    use_rsi_div:   bool  = True
    use_pinbar:    bool  = True
    use_vol_surge: bool  = True
    use_htf_close: bool  = True
    use_fvg_fresh: bool  = True
    use_precision: bool  = True
    min_cf:        int   = 1
    grace_bars:    int   = 5

    # SQS
    sqs_min: float = 5.0

    # ABE
    abe_on:   bool  = True
    abe_prog: float = 0.40
    abe_mom:  float = 0.50

    # Context (v19)
    ctx_min_full: int  = 3
    ctx_min_half: int  = 1
    use_d1_ctx:   bool = True
    use_h1_ctx:   bool = True
    use_m30_ctx:  bool = True
    use_m15_ctx:  bool = True
    use_vol_ctx:  bool = True
    use_sfp_ctx:  bool = True
    use_liq_ctx:  bool = True
    use_pdc_ctx:  bool = True

    # MTF BOS filter
    use_m15_bos: bool = True
    use_m30_bos: bool = True
    m15_bos_len: int  = 5
    m30_bos_len: int  = 5

    # DPE
    dpe_on: bool = True

    # News veto
    news_active: bool = False

P = Params()

# ═══════════════════════════════════════════════════════════════
# AGGRESSIVENESS DERIVED
# ═══════════════════════════════════════════════════════════════
def get_aggr(level: int) -> dict:
    table = {
        1: dict(eps_go=6, kz_req=True,  kzq_min=0.75, cms_thr=5.0, rank_cut=4,  lbl="UC[EPS≥6]"),
        2: dict(eps_go=5, kz_req=True,  kzq_min=0.50, cms_thr=4.5, rank_cut=6,  lbl="CN[EPS≥5]"),
        3: dict(eps_go=4, kz_req=False, kzq_min=0.33, cms_thr=3.5, rank_cut=9,  lbl="BL[EPS≥4]"),
        4: dict(eps_go=3, kz_req=False, kzq_min=0.20, cms_thr=3.0, rank_cut=12, lbl="AG[EPS≥3]"),
        5: dict(eps_go=2, kz_req=False, kzq_min=0.00, cms_thr=2.0, rank_cut=15, lbl="UA[EPS≥2]"),
    }
    return table.get(level, table[3])

AGGR = get_aggr(P.aggr_level)

# ═══════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════
@dataclass
class Bar:
    ts:     datetime
    open:   float
    high:   float
    low:    float
    close:  float
    volume: float

@dataclass
class MTFData:
    bars_m1:  list = field(default_factory=list)
    bars_m5:  list = field(default_factory=list)
    bars_m15: list = field(default_factory=list)
    bars_m30: list = field(default_factory=list)
    bars_h1:  list = field(default_factory=list)
    bars_h4:  list = field(default_factory=list)
    bars_d1:  list = field(default_factory=list)

@dataclass
class Signal:
    direction:  str   = "NONE"   # BUY / SELL / NONE
    order_type: str   = "NONE"   # BUY LIMIT / BUY STOP / SELL LIMIT / SELL STOP / NONE
    entry:      float = 0.0
    sl:         float = 0.0
    tp1:        float = 0.0
    tp2:        float = 0.0
    tp3:        float = 0.0
    tp4:        float = 0.0
    rr:         float = 0.0
    risk:       float = 0.0
    src:        str   = "-"
    eps_score:  int   = 0
    sqs_score:  float = 0.0
    ctx_score:  int   = 0
    ctx_size:   str   = "SKIP"
    h4_bias:    str   = "NEU"
    h1_bias:    str   = "NEU"
    m30_bias:   str   = "NEU"
    m30_struct: str   = "---"
    m15_bias:   str   = "NEU"
    m15_struct: str   = "---"
    d1_bias:    str   = "NEU"
    kz_name:    str   = "---"
    kz_quality: float = 0.0
    adr_pct:    float = 0.0
    pdc_zone:   str   = "---"
    liq_status: str   = "OK"
    sfp_signal: str   = "NO"
    acf_label:  str   = "OK"
    mtam_label: str   = "NEU"
    veto_rsn:   str   = "PASS"
    gate_ok:    bool  = False

# ═══════════════════════════════════════════════════════════════
# UTILS
# ═══════════════════════════════════════════════════════════════
def clamp(v, lo, hi):
    return max(lo, min(hi, v))

def sma(arr, n):
    if len(arr) < n:
        return sum(arr) / len(arr) if arr else 0.0
    return sum(arr[-n:]) / n

def ema(arr, n):
    if not arr:
        return 0.0
    k = 2.0 / (n + 1)
    e = arr[0]
    for x in arr[1:]:
        e = x * k + e * (1 - k)
    return e

def highest(arr, n):
    if not arr:
        return 0.0
    return max(arr[-n:]) if len(arr) >= n else max(arr)

def lowest(arr, n):
    if not arr:
        return 0.0
    return min(arr[-n:]) if len(arr) >= n else min(arr)

def atr(bars, n=14):
    if len(bars) < 2:
        return bars[0].high - bars[0].low if bars else 1.0
    trs = []
    for i in range(1, len(bars)):
        tr = max(
            bars[i].high - bars[i].low,
            abs(bars[i].high - bars[i-1].close),
            abs(bars[i].low  - bars[i-1].close)
        )
        trs.append(tr)
    return sma(trs, n)

def pivot_high(highs, left, right):
    """Return pivot high values list dari kanan."""
    result = []
    n = len(highs)
    for i in range(left, n - right):
        window_l = highs[i-left:i]
        window_r = highs[i+1:i+right+1]
        if all(highs[i] >= h for h in window_l) and all(highs[i] >= h for h in window_r):
            result.append(highs[i])
    return result

def pivot_low(lows, left, right):
    result = []
    n = len(lows)
    for i in range(left, n - right):
        window_l = lows[i-left:i]
        window_r = lows[i+1:i+right+1]
        if all(lows[i] <= l for l in window_l) and all(lows[i] <= l for l in window_r):
            result.append(lows[i])
    return result

def last_pivot_high(bars, left=5, right=5):
    highs = [b.high for b in bars]
    ph = pivot_high(highs, left, right)
    return ph[-1] if ph else None

def last_pivot_low(bars, left=5, right=5):
    lows = [b.low for b in bars]
    pl = pivot_low(lows, left, right)
    return pl[-1] if pl else None

def prev_pivot_high(bars, left=5, right=5):
    highs = [b.high for b in bars]
    ph = pivot_high(highs, left, right)
    return ph[-2] if len(ph) >= 2 else None

def prev_pivot_low(bars, left=5, right=5):
    lows = [b.low for b in bars]
    pl = pivot_low(lows, left, right)
    return pl[-2] if len(pl) >= 2 else None

def dmi(bars, di_len=14, adx_len=14):
    """Simplified DMI/ADX."""
    if len(bars) < di_len + 2:
        return 0.0, 0.0, 25.0
    plus_dm, minus_dm, tr_list = [], [], []
    for i in range(1, len(bars)):
        h_diff = bars[i].high - bars[i-1].high
        l_diff = bars[i-1].low - bars[i].low
        pdm = h_diff if h_diff > l_diff and h_diff > 0 else 0.0
        mdm = l_diff if l_diff > h_diff and l_diff > 0 else 0.0
        tr  = max(bars[i].high - bars[i].low,
                  abs(bars[i].high - bars[i-1].close),
                  abs(bars[i].low  - bars[i-1].close))
        plus_dm.append(pdm)
        minus_dm.append(mdm)
        tr_list.append(tr)
    smooth = di_len
    atr14  = sma(tr_list, smooth) or 1e-9
    diplus  = sma(plus_dm,  smooth) / atr14 * 100
    diminus = sma(minus_dm, smooth) / atr14 * 100
    dx = abs(diplus - diminus) / max(diplus + diminus, 1e-9) * 100
    adx_val = sma([dx], 1)
    return diplus, diminus, adx_val

def rsi(closes, length=14):
    if len(closes) < length + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sma(gains[-length:], length)
    avg_l = sma(losses[-length:], length) or 1e-9
    rs = avg_g / avg_l
    return 100 - (100 / (1 + rs))

# ═══════════════════════════════════════════════════════════════
# DATA FETCHING
# ═══════════════════════════════════════════════════════════════
def fetch_bars(symbol: str, interval: str, outputsize: int = 200) -> list[Bar]:
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol":     symbol,
        "interval":   interval,
        "outputsize": outputsize,
        "apikey":     TWELVEDATA_KEY,
        "format":     "JSON",
        "order":      "ASC"
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        if "values" not in data:
            log.error(f"No values for {symbol} {interval}: {data.get('message','?')}")
            return []
        bars = []
        for v in data["values"]:
            dt = datetime.fromisoformat(v["datetime"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            bars.append(Bar(
                ts=dt,
                open=float(v["open"]),
                high=float(v["high"]),
                low=float(v["low"]),
                close=float(v["close"]),
                volume=float(v.get("volume", 1000))
            ))
        return bars
    except Exception as e:
        log.error(f"fetch_bars {symbol} {interval}: {e}")
        return []

def fetch_all_tf(symbol: str) -> MTFData:
    mtf = MTFData()
    tf_map = {
        "1min":  "m1",
        "5min":  "m5",
        "15min": "m15",
        "30min": "m30",
        "1h":    "h1",
        "4h":    "h4",
        "1day":  "d1",
    }
    for tf, attr in tf_map.items():
        log.info(f"Fetching {symbol} {tf}...")
        bars = fetch_bars(symbol, tf, OUTPUTSZ)
        setattr(mtf, f"bars_{attr}", bars)
        time.sleep(0.5)
    return mtf

# ═══════════════════════════════════════════════════════════════
# VWAP DAILY
# ═══════════════════════════════════════════════════════════════
def calc_vwap_daily(bars: list[Bar]) -> tuple[Optional[float], bool]:
    if not bars:
        return None, False
    today = bars[-1].ts.astimezone(WIB).date()
    today_bars = [b for b in bars if b.ts.astimezone(WIB).date() == today]
    if not today_bars:
        return None, False
    sv  = sum(((b.high + b.low + b.close) / 3) * b.volume for b in today_bars)
    vol = sum(b.volume for b in today_bars)
    vwap = sv / vol if vol > 0 else None
    mature = len(today_bars) >= 15
    return vwap, mature

# ═══════════════════════════════════════════════════════════════
# KILL ZONE
# ═══════════════════════════════════════════════════════════════
def calc_kz(now_wib: datetime) -> tuple[bool, str, float, bool]:
    wib_hr = now_wib.hour
    kz_asia = P.kz_asia and 7 <= wib_hr < 14
    kz_lon  = P.sess_lon_hr <= wib_hr < (P.sess_lon_hr + 8)
    kz_ny   = wib_hr >= P.sess_ny_hr or wib_hr < ((P.sess_ny_hr + 4) % 24)
    kz_ovr  = kz_lon and kz_ny
    kz_on   = kz_asia or kz_lon or kz_ny

    if kz_ovr:
        kz_nm = "OVR"
        elapsed_pct = 0.0
    elif kz_ny:
        kz_nm = "NY"
        elapsed = wib_hr - P.sess_ny_hr if wib_hr >= P.sess_ny_hr else wib_hr + (24 - P.sess_ny_hr)
        elapsed_pct = min(elapsed / 8.0, 1.0)
    elif kz_lon:
        kz_nm = "LON"
        elapsed_pct = min((wib_hr - P.sess_lon_hr) / 8.0, 1.0)
    elif kz_asia:
        kz_nm = "ASIA"
        elapsed_pct = min((wib_hr - 7) / 7.0, 1.0)
    else:
        kz_nm = "---"
        elapsed_pct = 1.0

    if kz_ovr or elapsed_pct <= 0.33:
        kz_quality = 1.0
    elif elapsed_pct <= 0.67:
        kz_quality = 0.75
    elif elapsed_pct <= 0.90:
        kz_quality = 0.5
    else:
        kz_quality = 0.25

    kz_quality_ok = not kz_on or kz_quality >= AGGR["kzq_min"]
    return kz_on, kz_nm, kz_quality, kz_quality_ok

# ═══════════════════════════════════════════════════════════════
# STRUCTURE — H4, H1, M30, M15, D1
# ═══════════════════════════════════════════════════════════════
def calc_structure(bars: list[Bar], pivot_len: int = 5) -> dict:
    if len(bars) < pivot_len * 2 + 2:
        return {"bias": "NEU", "bos_bull": False, "bos_bear": False,
                "choch_bull": False, "choch_bear": False,
                "bos_struct": "---", "lph": None, "lpl": None,
                "pph": None, "ppl": None}

    highs = [b.high for b in bars]
    lows  = [b.low  for b in bars]

    ph_list = pivot_high(highs, pivot_len, pivot_len)
    pl_list = pivot_low(lows,   pivot_len, pivot_len)

    lph = ph_list[-1] if len(ph_list) >= 1 else None
    pph = ph_list[-2] if len(ph_list) >= 2 else None
    lpl = pl_list[-1] if len(pl_list) >= 1 else None
    ppl = pl_list[-2] if len(pl_list) >= 2 else None

    close = bars[-1].close

    bos_bull = (lph is not None and pph is not None and close > lph)
    bos_bear = (lpl is not None and ppl is not None and close < lpl)

    choch_bull = bos_bull and lph is not None and pph is not None and lph < pph
    choch_bear = bos_bear and lpl is not None and ppl is not None and lpl > ppl

    if bos_bull and not bos_bear:
        bias = "BUL"
    elif bos_bear and not bos_bull:
        bias = "BER"
    else:
        bias = "NEU"

    if choch_bull:
        bos_struct = "CHoCH↑"
    elif choch_bear:
        bos_struct = "CHoCH↓"
    elif bos_bull:
        bos_struct = "BOS↑"
    elif bos_bear:
        bos_struct = "BOS↓"
    else:
        bos_struct = "---"

    return {
        "bias": bias, "bos_bull": bos_bull, "bos_bear": bos_bear,
        "choch_bull": choch_bull, "choch_bear": choch_bear,
        "bos_struct": bos_struct,
        "lph": lph, "lpl": lpl, "pph": pph, "ppl": ppl
    }

def simple_bias(bars: list[Bar], ema_len: int = 20) -> str:
    if len(bars) < ema_len:
        return "NEU"
    closes = [b.close for b in bars]
    e = ema(closes, ema_len)
    return "BUL" if closes[-1] > e else "BER"

# ═══════════════════════════════════════════════════════════════
# FVG
# ═══════════════════════════════════════════════════════════════
def calc_fvg(bars: list[Bar], age_max: int = 12) -> dict:
    if len(bars) < 3:
        return {"bull_tap": False, "bear_tap": False,
                "bull_fresh": False, "bear_fresh": False,
                "bull_mid": None, "bear_mid": None}

    bull_zones, bear_zones = [], []
    for i in range(2, len(bars)):
        if bars[i].low > bars[i-2].high:
            mid  = (bars[i].low + bars[i-2].high) / 2
            age  = len(bars) - 1 - i
            bull_zones.append({"top": bars[i].low, "bot": bars[i-2].high, "mid": mid, "age": age})
        if bars[i].high < bars[i-2].low:
            mid  = (bars[i-2].low + bars[i].high) / 2
            age  = len(bars) - 1 - i
            bear_zones.append({"top": bars[i-2].low, "bot": bars[i].high, "mid": mid, "age": age})

    close = bars[-1].close
    result = {"bull_tap": False, "bear_tap": False,
              "bull_fresh": False, "bear_fresh": False,
              "bull_mid": None, "bear_mid": None}

    best_bd = 1e9
    for z in bull_zones[-5:]:
        if z["bot"] <= close <= z["top"]:
            result["bull_tap"] = True
            if z["age"] <= age_max:
                result["bull_fresh"] = True
                d = abs(close - z["mid"])
                if d < best_bd:
                    best_bd = d
                    result["bull_mid"] = z["mid"]

    best_sd = 1e9
    for z in bear_zones[-5:]:
        if z["bot"] <= close <= z["top"]:
            result["bear_tap"] = True
            if z["age"] <= age_max:
                result["bear_fresh"] = True
                d = abs(close - z["mid"])
                if d < best_sd:
                    best_sd = d
                    result["bear_mid"] = z["mid"]

    return result

# ═══════════════════════════════════════════════════════════════
# ORDER BLOCK
# ═══════════════════════════════════════════════════════════════
def calc_ob(bars: list[Bar], atr14: float, age_max: int = 40) -> dict:
    if len(bars) < 6:
        return {"bull_valid": False, "bear_valid": False,
                "boh": None, "bol": None, "soh": None, "sol": None,
                "bull_age": 0, "bear_age": 0}

    thr = atr14 * 1.2
    closes = [b.close for b in bars]

    boh = bol = soh = sol = None
    bull_age = bear_age = age_max + 1

    for i in range(len(bars) - 1, max(0, len(bars) - 50), -1):
        if i < 1:
            break
        if closes[i] > closes[i-1] and (closes[i] - closes[i-1]) >= thr:
            for j in range(i-1, max(0, i-5), -1):
                if bars[j].close < bars[j].open:
                    boh = bars[j].open
                    bol = bars[j].close
                    bull_age = len(bars) - 1 - i
                    break
            if boh is not None:
                break

    for i in range(len(bars) - 1, max(0, len(bars) - 50), -1):
        if i < 1:
            break
        if closes[i] < closes[i-1] and (closes[i-1] - closes[i]) >= thr:
            for j in range(i-1, max(0, i-5), -1):
                if bars[j].close > bars[j].open:
                    soh = bars[j].close
                    sol = bars[j].open
                    bear_age = len(bars) - 1 - i
                    break
            if soh is not None:
                break

    close = bars[-1].close
    bull_valid = (boh is not None and bol is not None and
                  bol <= close <= boh and bull_age <= age_max)
    bear_valid = (soh is not None and sol is not None and
                  sol <= close <= soh and bear_age <= age_max)

    return {
        "bull_valid": bull_valid, "bear_valid": bear_valid,
        "boh": boh, "bol": bol, "soh": soh, "sol": sol,
        "bull_age": bull_age, "bear_age": bear_age
    }

# ═══════════════════════════════════════════════════════════════
# FIBONACCI H4
# ═══════════════════════════════════════════════════════════════
def calc_fib(h4_str: dict, atr14: float) -> dict:
    fh = h4_str["lph"]
    fl = h4_str["lpl"]
    if fh is None or fl is None or fh <= fl:
        return {"valid": False, "fb618": None, "fb786": None,
                "fs618": None, "fs786": None, "buy": False, "sell": False}
    rng   = fh - fl
    tol   = atr14 * P.fib_tol
    fb618 = fh - rng * 0.618
    fb786 = fh - rng * 0.786
    fs618 = fl + rng * 0.618
    fs786 = fl + rng * 0.786
    # Placeholder — actual close compared at runtime
    return {
        "valid": True,
        "fb618": fb618, "fb786": fb786,
        "fs618": fs618, "fs786": fs786,
        "tol": tol, "buy": False, "sell": False
    }

def eval_fib(fib: dict, close: float) -> dict:
    if not fib["valid"]:
        return fib
    tol = fib["tol"]
    fib["buy"]  = (abs(close - fib["fb618"]) <= tol or
                   abs(close - fib["fb786"]) <= tol)
    fib["sell"] = (abs(close - fib["fs618"]) <= tol or
                   abs(close - fib["fs786"]) <= tol)
    return fib

# ═══════════════════════════════════════════════════════════════
# SFP DETECTOR
# ═══════════════════════════════════════════════════════════════
def calc_sfp(bars: list[Bar], atr14: float) -> tuple[bool, bool]:
    if len(bars) < 12:
        return False, False
    highs = [b.high for b in bars]
    lows  = [b.low  for b in bars]
    ph    = pivot_high(highs, 5, 5)
    pl    = pivot_low(lows,   5, 5)
    ref_h = ph[-1] if ph else None
    ref_l = pl[-1] if pl else None
    b = bars[-1]
    sfp_bull = (ref_l is not None and
                b.low < ref_l and b.close > ref_l and
                (ref_l - b.low) >= atr14 * 0.5 and b.close > b.open)
    sfp_bear = (ref_h is not None and
                b.high > ref_h and b.close < ref_h and
                (b.high - ref_h) >= atr14 * 0.5 and b.close < b.open)
    return sfp_bull, sfp_bear

# ═══════════════════════════════════════════════════════════════
# LIQUIDITY SWEEP
# ═══════════════════════════════════════════════════════════════
def calc_liq(bars: list[Bar], atr14: float) -> dict:
    if len(bars) < 12:
        return {"swept_h": False, "swept_l": False,
                "near_h": False, "near_l": False,
                "eq_high": None, "eq_low": None}
    highs = [b.high for b in bars]
    lows  = [b.low  for b in bars]
    tol   = atr14 * 0.15

    ph3 = pivot_high(highs, 3, 3)
    ph5 = pivot_high(highs, 5, 5)
    pl3 = pivot_low(lows,   3, 3)
    pl5 = pivot_low(lows,   5, 5)

    eq_high = None
    eq_low  = None
    if ph3 and ph5 and abs(ph3[-1] - ph5[-1]) <= tol:
        eq_high = (ph3[-1] + ph5[-1]) / 2
    if pl3 and pl5 and abs(pl3[-1] - pl5[-1]) <= tol:
        eq_low = (pl3[-1] + pl5[-1]) / 2

    b = bars[-1]
    swept_h = (eq_high is not None and b.high > eq_high and b.close < eq_high)
    swept_l = (eq_low  is not None and b.low  < eq_low  and b.close > eq_low)
    near_h  = (eq_high is not None and abs(b.close - eq_high) < atr14 * 0.5)
    near_l  = (eq_low  is not None and abs(b.close - eq_low)  < atr14 * 0.5)

    return {
        "swept_h": swept_h, "swept_l": swept_l,
        "near_h": near_h,   "near_l": near_l,
        "eq_high": eq_high, "eq_low": eq_low
    }

# ═══════════════════════════════════════════════════════════════
# CANDLE PATTERNS
# ═══════════════════════════════════════════════════════════════
def calc_candle(bars: list[Bar], atr14: float) -> tuple[bool, bool]:
    if len(bars) < 2:
        return False, False
    b  = bars[-1]
    b1 = bars[-2]
    body   = abs(b.close - b.open)
    doji   = body < atr14 * 0.1
    pb_bull = ((min(b.open,b.close)-b.low) >= body*2.0 and
               (b.high-max(b.open,b.close)) <= body*0.5 and not doji)
    pb_bear = ((b.high-max(b.open,b.close)) >= body*2.0 and
               (min(b.open,b.close)-b.low) <= body*0.5 and not doji)
    eg_bull = (b1.close < b1.open and b.close > b.open and
               b.close > b1.open and b.open < b1.close)
    eg_bear = (b1.close > b1.open and b.close < b.open and
               b.close < b1.open and b.open > b1.close)
    ibar    = b.high < b1.high and b.low > b1.low
    bull = (pb_bull or eg_bull or ibar) and not doji
    bear = (pb_bear or eg_bear or ibar) and not doji
    return bull, bear

# ═══════════════════════════════════════════════════════════════
# VOLUME FLAGS
# ═══════════════════════════════════════════════════════════════
def calc_vol(bars: list[Bar], atr14: float) -> dict:
    vols  = [b.volume for b in bars]
    vsma  = sma(vols, 20)
    b     = bars[-1]
    body  = abs(b.close - b.open)
    surge  = b.volume > vsma * P.vol_mult
    climax = b.volume > vsma * 2.5 and body < atr14 * 0.3
    dry    = b.volume < vsma * 0.5
    return {"surge": surge, "climax": climax, "dry": dry, "sma": vsma}

# ═══════════════════════════════════════════════════════════════
# PDC (PREMIUM / DISCOUNT)
# ═══════════════════════════════════════════════════════════════
def calc_pdc(h4_str: dict, close: float) -> dict:
    fh = h4_str["lph"]
    fl = h4_str["lpl"]
    if fh is None or fl is None or fh <= fl:
        return {"valid": False, "discount": False, "premium": False,
                "mid": None, "zone": "---"}
    mid = (fh + fl) / 2
    discount = close < mid
    premium  = close > mid
    zone = "DISC" if discount else ("PREM" if premium else "MID")
    return {"valid": True, "discount": discount, "premium": premium,
            "mid": mid, "zone": zone}

# ═══════════════════════════════════════════════════════════════
# ADR FILTER
# ═══════════════════════════════════════════════════════════════
def calc_adr(bars_d1: list[Bar], bars_cur: list[Bar]) -> float:
    if len(bars_d1) < 14:
        return 0.0
    ranges  = [(b.high - b.low) for b in bars_d1[-14:]]
    adr_avg = sma(ranges, 14) or 1e-9
    if not bars_cur:
        return 0.0
    today = bars_cur[-1].ts.astimezone(WIB).date()
    today_bars = [b for b in bars_cur if b.ts.astimezone(WIB).date() == today]
    if not today_bars:
        return 0.0
    day_h = max(b.high for b in today_bars)
    day_l = min(b.low  for b in today_bars)
    return (day_h - day_l) / adr_avg * 100.0

# ═══════════════════════════════════════════════════════════════
# STRUCTURAL SL/TP HELPERS
# ═══════════════════════════════════════════════════════════════
def find_sup(ep, atr14, h4_str, h1_str, m30_str, m15_str,
             ob, fib, vwap, pdl, eq_low):
    candidates = []
    for key in ("lpl", "ppl"):
        v = h4_str.get(key)
        if v and v < ep: candidates.append(v)
    for key in ("lpl", "ppl"):
        v = h1_str.get(key)
        if v and v < ep: candidates.append(v)
    v = m30_str.get("lpl")
    if v and v < ep: candidates.append(v)
    v = m15_str.get("lpl")
    if v and v < ep: candidates.append(v)
    if ob["bull_valid"] and ob["bol"] and ob["bol"] < ep:
        candidates.append(ob["bol"] - atr14 * 0.05)
    if fib["valid"]:
        for k in ("fb786", "fb618"):
            v = fib.get(k)
            if v and v < ep: candidates.append(v)
    if vwap and vwap < ep: candidates.append(vwap)
    if pdl and pdl < ep:   candidates.append(pdl)
    if eq_low and eq_low < ep: candidates.append(eq_low)

    if not candidates:
        return ep - atr14 * 1.0
    best = min(candidates, key=lambda v: ep - v)
    return best

def find_res(ep, atr14, h4_str, h1_str, m30_str, m15_str,
             ob, fib, vwap, pdh, eq_high):
    candidates = []
    for key in ("lph", "pph"):
        v = h4_str.get(key)
        if v and v > ep: candidates.append(v)
    for key in ("lph", "pph"):
        v = h1_str.get(key)
        if v and v > ep: candidates.append(v)
    v = m30_str.get("lph")
    if v and v > ep: candidates.append(v)
    v = m15_str.get("lph")
    if v and v > ep: candidates.append(v)
    if ob["bear_valid"] and ob["soh"] and ob["soh"] > ep:
        candidates.append(ob["soh"] + atr14 * 0.05)
    if fib["valid"]:
        for k in ("fs786", "fs618"):
            v = fib.get(k)
            if v and v > ep: candidates.append(v)
    if vwap and vwap > ep: candidates.append(vwap)
    if pdh and pdh > ep:   candidates.append(pdh)
    if eq_high and eq_high > ep: candidates.append(eq_high)

    if not candidates:
        return ep + atr14 * 1.0
    best = min(candidates, key=lambda v: v - ep)
    return best

# ═══════════════════════════════════════════════════════════════
# BEST ENTRY SELECTION + ORDER TYPE (LIMIT vs STOP)
# ═══════════════════════════════════════════════════════════════
def best_entry_buy(close, atr14, fvg, ob, fib, vwap, sfp_bull,
                   liq, h4_str, h1_str, m30_str, m15_str, pdh, pdl, eq_high, eq_low):
    """
    Kembalikan (entry, name, rr, order_type)
    - Jika entry < close  → BUY LIMIT  (pullback ke zone)
    - Jika entry > close  → BUY STOP   (breakout konfirmasi)
    - Jika entry ≈ close  → BUY LIMIT  (at zone)
    """
    candidates = []

    def _eval(ep, nm):
        sl_r = find_sup(ep, atr14, h4_str, h1_str, m30_str, m15_str,
                        ob, fib, vwap, pdl, eq_low) - atr14 * 0.05
        risk = clamp(ep - sl_r, atr14 * P.sl_min_atr, atr14 * P.sl_max_atr)
        tp   = find_res(ep, atr14, h4_str, h1_str, m30_str, m15_str,
                        ob, fib, vwap, pdh, eq_high)
        rr   = (tp - ep) / risk if risk > 0 else 0.0
        otype = "BUY STOP" if ep > close * 1.0002 else "BUY LIMIT"
        candidates.append((ep, nm, rr, otype))

    if fvg["bull_fresh"] and fvg["bull_mid"] is not None:
        _eval(fvg["bull_mid"], "FVG")
    if ob["bull_valid"] and ob["boh"] and ob["bol"]:
        _eval((ob["boh"] + ob["bol"]) / 2, "OB")
    if fib["valid"] and fib["fb618"] is not None:
        _eval(fib["fb618"], "F618")
    if fib["valid"] and fib["fb786"] is not None:
        _eval(fib["fb786"], "F786")
    if vwap:
        _eval(vwap, "VWAP")
    if sfp_bull:
        ref_l = h4_str.get("lpl") or (close - atr14)
        _eval(ref_l + atr14 * 0.1, "SFP")
    if liq["swept_l"] and liq["eq_low"] is not None:
        _eval(liq["eq_low"] + atr14 * 0.1, "LIQ")

    # BUY STOP: level tepat di atas lph4 (breakout)
    lph = h4_str.get("lph")
    if lph and lph > close:
        _eval(lph + atr14 * 0.05, "BOS-H4")

    if not candidates:
        ep = close - atr14 * 0.7
        return ep, "ATR", 0.0, "BUY LIMIT"

    best = max(candidates, key=lambda x: x[2])
    return best

def best_entry_sell(close, atr14, fvg, ob, fib, vwap, sfp_bear,
                    liq, h4_str, h1_str, m30_str, m15_str, pdh, pdl, eq_high, eq_low):
    """
    - Jika entry > close  → SELL LIMIT
    - Jika entry < close  → SELL STOP  (breakdown konfirmasi)
    """
    candidates = []

    def _eval(ep, nm):
        rs_r = find_res(ep, atr14, h4_str, h1_str, m30_str, m15_str,
                        ob, fib, vwap, pdh, eq_high) + atr14 * 0.05
        risk = clamp(rs_r - ep, atr14 * P.sl_min_atr, atr14 * P.sl_max_atr)
        sp   = find_sup(ep, atr14, h4_str, h1_str, m30_str, m15_str,
                        ob, fib, vwap, pdl, eq_low)
        rr   = (ep - sp) / risk if risk > 0 else 0.0
        otype = "SELL STOP" if ep < close * 0.9998 else "SELL LIMIT"
        candidates.append((ep, nm, rr, otype))

    if fvg["bear_fresh"] and fvg["bear_mid"] is not None:
        _eval(fvg["bear_mid"], "FVG")
    if ob["bear_valid"] and ob["soh"] and ob["sol"]:
        _eval((ob["soh"] + ob["sol"]) / 2, "OB")
    if fib["valid"] and fib["fs618"] is not None:
        _eval(fib["fs618"], "F618")
    if fib["valid"] and fib["fs786"] is not None:
        _eval(fib["fs786"], "F786")
    if vwap:
        _eval(vwap, "VWAP")
    if sfp_bear:
        ref_h = h4_str.get("lph") or (close + atr14)
        _eval(ref_h - atr14 * 0.1, "SFP")
    if liq["swept_h"] and liq["eq_high"] is not None:
        _eval(liq["eq_high"] - atr14 * 0.1, "LIQ")

    # SELL STOP: level tepat di bawah lpl4 (breakdown)
    lpl = h4_str.get("lpl")
    if lpl and lpl < close:
        _eval(lpl - atr14 * 0.05, "BRK-L4")

    if not candidates:
        ep = close + atr14 * 0.7
        return ep, "ATR", 0.0, "SELL LIMIT"

    best = max(candidates, key=lambda x: x[2])
    return best

# ═══════════════════════════════════════════════════════════════
# DPE — DYNAMIC PARTIAL EXIT
# ═══════════════════════════════════════════════════════════════
def dpe_buy(entry, risk, atr14, h4_str, h1_str, m30_str, m15_str,
            ob, fib, vwap, pdh, eq_high):
    r1 = find_res(entry, atr14, h4_str, h1_str, m30_str, m15_str,
                  ob, fib, vwap, pdh, eq_high)
    r2 = find_res(r1 + atr14*0.1, atr14, h4_str, h1_str, m30_str, m15_str,
                  ob, fib, vwap, pdh, eq_high)
    r3 = find_res(r2 + atr14*0.1, atr14, h4_str, h1_str, m30_str, m15_str,
                  ob, fib, vwap, pdh, eq_high)
    if P.dpe_on:
        t1 = max(r1, entry + risk * P.rr_tp1)
        t2 = max(r2, entry + risk * P.rr_tp2)
        t3 = max(r3, entry + risk * P.rr_tp3)
    else:
        t1 = entry + risk * P.rr_tp1
        t2 = entry + risk * P.rr_tp2
        t3 = entry + risk * P.rr_tp3
    t4 = entry + risk * P.rr_tp4
    return [t - P.spread for t in (t1, t2, t3, t4)]

def dpe_sell(entry, risk, atr14, h4_str, h1_str, m30_str, m15_str,
             ob, fib, vwap, pdl, eq_low):
    s1 = find_sup(entry, atr14, h4_str, h1_str, m30_str, m15_str,
                  ob, fib, vwap, pdl, eq_low)
    s2 = find_sup(s1 - atr14*0.1, atr14, h4_str, h1_str, m30_str, m15_str,
                  ob, fib, vwap, pdl, eq_low)
    s3 = find_sup(s2 - atr14*0.1, atr14, h4_str, h1_str, m30_str, m15_str,
                  ob, fib, vwap, pdl, eq_low)
    if P.dpe_on:
        t1 = min(s1, entry - risk * P.rr_tp1)
        t2 = min(s2, entry - risk * P.rr_tp2)
        t3 = min(s3, entry - risk * P.rr_tp3)
    else:
        t1 = entry - risk * P.rr_tp1
        t2 = entry - risk * P.rr_tp2
        t3 = entry - risk * P.rr_tp3
    t4 = entry - risk * P.rr_tp4
    return [t + P.spread for t in (t1, t2, t3, t4)]

# ═══════════════════════════════════════════════════════════════
# EPS GATE — 7 LAYER (v18)
# ═══════════════════════════════════════════════════════════════
def calc_eps(h4_bias, h1_bias, ob, fvg, fib, vwap_mature, vwap,
             pdc, m1_bu, m1_be, arne_wait, kz_on, vol_surge,
             direction) -> tuple[int, str]:
    bias_ok = (h4_bias == direction == "BUY" and h4_bias != "NEU") or \
              (h4_bias == "BER" and direction == "SELL" and h4_bias != "NEU")

    l1 = bias_ok
    l2 = (ob["bull_valid"] or ob["bear_valid"] or
          (fvg["bull_fresh"] and direction == "BUY") or
          (fvg["bear_fresh"] and direction == "SELL") or
          fib.get("buy") or fib.get("sell"))
    l3 = True  # Simplified pivot confluence
    l4 = (vwap_mature and vwap is not None and
          ((h4_bias == "BUL" and vwap is not None) or
           (h4_bias == "BER" and vwap is not None)))
    l5 = (pdc["valid"] and
          ((direction == "BUY" and pdc["discount"]) or
           (direction == "SELL" and pdc["premium"])))
    l6 = (m1_bu if direction == "BUY" else m1_be)
    l7 = not arne_wait and (kz_on or vol_surge)

    score = sum([l1, l2, l3, l4, l5, l6, l7])
    labels = {6: "APEX", 5: "SNIPER", 4: "PRECIS", 3: "STAND", 2: "MARGN"}
    label  = labels.get(score, "SKIP")
    return score, label

# ═══════════════════════════════════════════════════════════════
# MTAM (Multi-Timeframe Alignment)
# ═══════════════════════════════════════════════════════════════
def calc_mtam(bars_m5, bars_m15, bars_h1, bars_h4, bars_d1) -> tuple[str, float]:
    def score(bars):
        if len(bars) < 21:
            return 0
        closes = [b.close for b in bars]
        e = ema(closes, 20)
        return 1 if closes[-1] > e else -1

    s = score(bars_m5) + score(bars_m15) + score(bars_h1) + score(bars_h4) + score(bars_d1)
    sc = s / 5.0
    if   sc >= 0.8:  lbl = "STR-B"
    elif sc >= 0.4:  lbl = "MOD-B"
    elif sc <= -0.8: lbl = "STR-S"
    elif sc <= -0.4: lbl = "MOD-S"
    else:            lbl = "NEU"
    return lbl, sc

# ═══════════════════════════════════════════════════════════════
# SQS — SCALP QUALITY SCORE
# ═══════════════════════════════════════════════════════════════
def calc_sqs(kz_name, kz_quality, vol, sfp_bull, sfp_bear,
             liq, pdc, m1_bu, m1_be, acf_chop, arne_trend,
             arne_wait, direction, live_cf, has_conflict) -> float:
    sqs_kz   = 2.0 if kz_name == "OVR" else (1.5 if kz_quality >= 0.75 else
                1.0 if kz_quality > 0 else 0.0)
    sqs_vol  = (2.0 if vol["surge"] and not vol["climax"] else
                1.0 if not vol["dry"] else 0.0)
    sqs_cf   = min(live_cf * 0.5, 2.0)
    sqs_m1   = (2.0 if (m1_bu or m1_be) else 1.0 if not acf_chop else 0.0)
    sqs_mom  = (2.0 if arne_trend else 1.0 if not arne_wait else 0.0)
    sqs_sfp  = (1.0 if (sfp_bull and direction == "BUY") or
                       (sfp_bear and direction == "SELL") else 0.0)
    sqs_liq  = (1.0 if (liq["swept_l"] and direction == "BUY") or
                       (liq["swept_h"] and direction == "SELL") else 0.0)
    sqs_pdc  = (0.5 if (pdc["discount"] and direction == "BUY") or
                       (pdc["premium"] and direction == "SELL") else 0.0)
    conflict_pen = 1.5 if has_conflict else 0.0
    raw   = sqs_kz + sqs_vol + sqs_cf + sqs_m1 + sqs_mom + sqs_sfp + sqs_liq + sqs_pdc - conflict_pen
    return round(clamp(raw, 0.0, 10.0), 1)

# ═══════════════════════════════════════════════════════════════
# CONTEXT SCORE — v19
# ═══════════════════════════════════════════════════════════════
def calc_ctx(direction, d1_bias, h1_bias, m30_bias, m15_bias,
             vol, liq, sfp_bull, sfp_bear, pdc) -> tuple[int, str]:
    if direction == "BUY":
        score = sum([
            P.use_d1_ctx  and d1_bias  == "BUL",
            P.use_h1_ctx  and h1_bias  == "BUL",
            P.use_m30_ctx and m30_bias == "BUL",
            P.use_m15_ctx and m15_bias == "BUL",
            P.use_vol_ctx and vol["surge"] and not vol["climax"],
            P.use_liq_ctx and liq["swept_l"],
            P.use_sfp_ctx and sfp_bull,
            P.use_pdc_ctx and pdc["discount"],
        ])
    else:
        score = sum([
            P.use_d1_ctx  and d1_bias  == "BER",
            P.use_h1_ctx  and h1_bias  == "BER",
            P.use_m30_ctx and m30_bias == "BER",
            P.use_m15_ctx and m15_bias == "BER",
            P.use_vol_ctx and vol["surge"] and not vol["climax"],
            P.use_liq_ctx and liq["swept_h"],
            P.use_sfp_ctx and sfp_bear,
            P.use_pdc_ctx and pdc["premium"],
        ])
    size = ("FULL" if score >= P.ctx_min_full else
            "HALF" if score >= P.ctx_min_half else "SKIP")
    return score, size

# ═══════════════════════════════════════════════════════════════
# M1 MICROSTRUCTURE
# ═══════════════════════════════════════════════════════════════
def calc_m1_micro(bars_m1: list[Bar]) -> tuple[bool, bool]:
    if len(bars_m1) < 12:
        return False, False
    highs = [b.high for b in bars_m1]
    lows  = [b.low  for b in bars_m1]
    ph    = pivot_high(highs, 3, 3)
    pl    = pivot_low(lows,   3, 3)
    if not ph or not pl:
        return False, False
    last_ph = ph[-1]
    last_pl = pl[-1]
    hi5 = max(highs[-5:])
    lo5 = min(lows[-5:])
    m1_bu = hi5 > last_ph
    m1_be = lo5 < last_pl
    return m1_bu, m1_be

# ═══════════════════════════════════════════════════════════════
# ARNE (ATR Regime Noise Estimator)
# ═══════════════════════════════════════════════════════════════
def calc_arne(bars: list[Bar], atr14: float) -> tuple[bool, bool]:
    atr_ma = sma([atr(bars[max(0,i-14):i+1]) for i in range(len(bars))], 20) or 1.0
    ratio  = atr14 / atr_ma
    closes = [b.close for b in bars]
    e20    = ema(closes, 20)
    _, _, adx_val = dmi(bars[-30:] if len(bars) >= 30 else bars)
    expand  = ratio > 2.0
    noise   = ratio > 1.5 and adx_val < 20.0
    trend   = adx_val > 25.0 and abs(closes[-1] - e20) > atr14 * 0.3 and not expand
    wait = noise or expand if P.aggr_level <= 2 else noise if P.aggr_level == 3 else False
    return wait, trend

# ═══════════════════════════════════════════════════════════════
# ACF (Anti-Chop Filter)
# ═══════════════════════════════════════════════════════════════
def calc_acf(bars_m1: list[Bar], bars_m5: list[Bar]) -> tuple[bool, str]:
    if len(bars_m1) < 20 or len(bars_m5) < 20:
        return False, "OK"
    _, _, adx_m1 = dmi(bars_m1[-20:])
    _, _, adx_m5 = dmi(bars_m5[-20:])
    atr_m1 = atr(bars_m1[-20:])
    atr_m5 = atr(bars_m5[-20:])
    m1_avg = sma([atr(bars_m1[-20:])], 1)
    m5_avg = sma([atr(bars_m5[-20:])], 1)
    m1_rng = adx_m1 < P.adx_chop_thr
    m5_rng = adx_m5 < P.adx_chop_thr
    m1_tgt = atr_m1 < m1_avg * 0.7
    chop   = m1_rng and m5_rng and m1_tgt
    label  = "CHOP!" if chop else "TREND-OK"
    return chop, label

# ═══════════════════════════════════════════════════════════════
# MAIN ANALYSIS ENGINE
# ═══════════════════════════════════════════════════════════════
def analyze(mtf: MTFData) -> Signal:
    sig = Signal()

    bars = mtf.bars_m5
    if len(bars) < 50:
        log.warning("Insufficient M5 bars")
        return sig

    # Current bar
    cb    = bars[-1]
    close = cb.close
    now_wib = cb.ts.astimezone(WIB)
    atr14 = atr(bars[-30:])

    # ── Structures
    h4_str = calc_structure(mtf.bars_h4, 5)
    h1_str = calc_structure(mtf.bars_h1, 5)
    m30_str = calc_structure(mtf.bars_m30, P.m30_bos_len)
    m15_str = calc_structure(mtf.bars_m15, P.m15_bos_len)
    d1_str  = calc_structure(mtf.bars_d1, 5)

    h4_bias  = h4_str["bias"]
    h1_bias  = simple_bias(mtf.bars_h1)
    m30_bias = m30_str["bias"]
    m15_bias = m15_str["bias"]
    d1_bias  = simple_bias(mtf.bars_d1)

    sig.h4_bias    = h4_bias
    sig.h1_bias    = h1_bias
    sig.m30_bias   = m30_bias
    sig.m30_struct = m30_str["bos_struct"]
    sig.m15_bias   = m15_bias
    sig.m15_struct = m15_str["bos_struct"]
    sig.d1_bias    = d1_bias

    # ── PDH/PDL
    pdh = mtf.bars_d1[-1].high  if len(mtf.bars_d1) >= 2 else close + atr14
    pdl = mtf.bars_d1[-1].low   if len(mtf.bars_d1) >= 2 else close - atr14

    # ── VWAP
    vwap, vwap_mature = calc_vwap_daily(bars)

    # ── Kill Zone
    kz_on, kz_nm, kz_quality, kz_q_ok = calc_kz(now_wib)
    sig.kz_name    = kz_nm
    sig.kz_quality = kz_quality

    # ── FVG / OB / Fib
    fvg = calc_fvg(bars, P.fvg_age_max)
    ob  = calc_ob(bars, atr14, P.ob_age_max)
    fib = calc_fib(h4_str, atr14)
    fib = eval_fib(fib, close)

    # ── PDC
    pdc = calc_pdc(h4_str, close)
    sig.pdc_zone = pdc["zone"]

    # ── Liquidity
    liq = calc_liq(bars, atr14)
    sig.liq_status = ("SWEPT-L" if liq["swept_l"] else
                      "SWEPT-H" if liq["swept_h"] else
                      "NEAR-H"  if liq["near_h"]  else
                      "NEAR-L"  if liq["near_l"]  else "OK")

    # ── SFP
    sfp_bull, sfp_bear = calc_sfp(bars, atr14)
    sig.sfp_signal = ("BULL" if sfp_bull else "BEAR" if sfp_bear else "NO")

    # ── Candle
    candle_bull, candle_bear = calc_candle(bars, atr14)

    # ── Volume
    vol = calc_vol(bars, atr14)

    # ── M1 Micro
    m1_bu, m1_be = calc_m1_micro(mtf.bars_m1)

    # ── ACF
    acf_chop, acf_label = calc_acf(mtf.bars_m1, mtf.bars_m5)
    sig.acf_label = acf_label

    # ── ARNE
    arne_wait, arne_trend = calc_arne(bars, atr14)

    # ── MTAM
    mtam_lbl, mtam_sc = calc_mtam(
        mtf.bars_m5, mtf.bars_m15, mtf.bars_h1, mtf.bars_h4, mtf.bars_d1)
    sig.mtam_label = mtam_lbl

    # ── ADR
    adr_pct = calc_adr(mtf.bars_d1, bars)
    sig.adr_pct = round(adr_pct, 1)

    # ── VETO
    veto_chop = acf_chop
    veto_adr  = adr_pct >= P.adr_pct_max
    veto_news = P.news_active
    veto_any  = veto_chop or veto_adr or veto_news
    sig.veto_rsn = ("CHOP" if veto_chop else
                    f"ADR>{P.adr_pct_max:.0f}%" if veto_adr else
                    "NEWS" if veto_news else "PASS")

    # ── Direction candidates
    raw_dir = "BUY" if h4_bias == "BUL" else ("SELL" if h4_bias == "BER" else "NONE")
    if raw_dir == "NONE":
        sig.gate_ok = False
        return sig

    direction = raw_dir

    # ── Conflict
    htf_conflict = ((direction == "BUY"  and h1_bias == "BER") or
                    (direction == "SELL" and h1_bias == "BUL"))
    mtam_conflict = ((direction == "BUY"  and mtam_sc <= -0.6) or
                     (direction == "SELL" and mtam_sc >= 0.6))
    has_conflict = htf_conflict or mtam_conflict

    # ── m15/m30 BOS filter (v19)
    m15_ok = (not P.use_m15_bos or
              m15_bias == direction[:3] or m15_bias == "NEU")
    m30_ok = (not P.use_m30_bos or
              m30_bias == direction[:3] or m30_bias == "NEU")

    # ── At key level
    at_key = ((is_fvg_tap := fvg["bull_tap"] if direction == "BUY" else fvg["bear_tap"]) or
              (ob["bull_valid"] if direction == "BUY" else ob["bear_valid"]) or
              (fib.get("buy") if direction == "BUY" else fib.get("sell")))

    # ── EPS (v18 7-layer)
    eps_score, eps_label = calc_eps(
        h4_bias, h1_bias, ob, fvg, fib, vwap_mature, vwap,
        pdc, m1_bu, m1_be, arne_wait, kz_on, vol["surge"], direction)
    sig.eps_score = eps_score

    # ── SQS
    live_cf = sum([
        (vwap and abs(close - vwap) < atr14 * 0.3),
        (fib["valid"] and fib["fb618"] and abs(close - fib["fb618"]) < atr14 * 0.3),
        ob["bull_valid"] if direction == "BUY" else ob["bear_valid"],
        fvg["bull_fresh"] if direction == "BUY" else fvg["bear_fresh"],
        liq["swept_l"] if direction == "BUY" else liq["swept_h"],
        sfp_bull if direction == "BUY" else sfp_bear,
    ])
    sqs = calc_sqs(kz_nm, kz_quality, vol, sfp_bull, sfp_bear,
                   liq, pdc, m1_bu, m1_be, acf_chop, arne_trend,
                   arne_wait, direction, live_cf, has_conflict)
    sig.sqs_score = sqs

    # ── Context (v19)
    ctx_score, ctx_size = calc_ctx(
        direction, d1_bias, h1_bias, m30_bias, m15_bias,
        vol, liq, sfp_bull, sfp_bear, pdc)
    sig.ctx_score = ctx_score
    sig.ctx_size  = ctx_size

    # ── MAIN GATE — gabungan v18 + v19
    vol_ok   = not vol["climax"]
    candle_ok = candle_bull if direction == "BUY" else candle_bear
    m1_ok     = m1_bu if direction == "BUY" else m1_be
    pdc_ok    = pdc["discount"] if direction == "BUY" else pdc["premium"]
    liq_ok    = (not liq["near_h"]) or liq["swept_l"] if direction == "BUY" else \
                (not liq["near_l"]) or liq["swept_h"]

    gate = (
        h4_bias != "NEU" and
        kz_on and kz_q_ok and
        at_key and
        candle_ok and
        vol_ok and not vol["dry"] and
        not acf_chop and
        not arne_wait and
        not htf_conflict and
        not veto_any and
        m15_ok and m30_ok and
        m1_ok and
        eps_score >= AGGR["eps_go"] and
        sqs >= P.sqs_min and
        pdc_ok and liq_ok and
        ctx_size != "SKIP"
    )
    sig.gate_ok = gate

    if not gate:
        return sig

    # ── ENTRY SELECTION + ORDER TYPE
    if direction == "BUY":
        entry, src, rr, otype = best_entry_buy(
            close, atr14, fvg, ob, fib, vwap, sfp_bull,
            liq, h4_str, h1_str, m30_str, m15_str, pdh, pdl,
            liq["eq_high"], liq["eq_low"])
    else:
        entry, src, rr, otype = best_entry_sell(
            close, atr14, fvg, ob, fib, vwap, sfp_bear,
            liq, h4_str, h1_str, m30_str, m15_str, pdh, pdl,
            liq["eq_high"], liq["eq_low"])

    if rr < P.min_rr:
        sig.gate_ok = False
        return sig

    # ── SL / TP
    if direction == "BUY":
        sl_raw = find_sup(entry, atr14, h4_str, h1_str, m30_str, m15_str,
                          ob, fib, vwap, pdl, liq["eq_low"]) - atr14 * 0.05
        risk   = clamp(entry - sl_raw, atr14 * P.sl_min_atr, atr14 * P.sl_max_atr)
        sl     = entry - risk
        tps    = dpe_buy(entry, risk, atr14, h4_str, h1_str, m30_str, m15_str,
                         ob, fib, vwap, pdh, liq["eq_high"])
    else:
        rs_raw = find_res(entry, atr14, h4_str, h1_str, m30_str, m15_str,
                          ob, fib, vwap, pdh, liq["eq_high"]) + atr14 * 0.05
        risk   = clamp(rs_raw - entry, atr14 * P.sl_min_atr, atr14 * P.sl_max_atr)
        sl     = entry + risk
        tps    = dpe_sell(entry, risk, atr14, h4_str, h1_str, m30_str, m15_str,
                          ob, fib, vwap, pdl, liq["eq_low"])

    sig.direction  = direction
    sig.order_type = otype
    sig.entry      = round(entry, 2)
    sig.sl         = round(sl, 2)
    sig.tp1        = round(tps[0], 2)
    sig.tp2        = round(tps[1], 2)
    sig.tp3        = round(tps[2], 2)
    sig.tp4        = round(tps[3], 2)
    sig.rr         = round(rr, 2)
    sig.risk       = round(risk, 2)
    sig.src        = src

    return sig

# ═══════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════
def fmt_msg(sig: Signal, symbol: str) -> str:
    now_wib = datetime.now(WIB).strftime("%d %b %Y %H:%M WIB")

    if not sig.gate_ok or sig.direction == "NONE":
        return (
            f"🔵 <b>PEMIF v20 UNIFIED</b>\n"
            f"📊 {symbol} | {now_wib}\n"
            f"⏳ <b>WAITING</b>\n\n"
            f"H4: {sig.h4_bias} | H1: {sig.h1_bias} | D1: {sig.d1_bias}\n"
            f"M30: {sig.m30_bias} {sig.m30_struct} | M15: {sig.m15_bias} {sig.m15_struct}\n"
            f"KZ: {sig.kz_name} KZQ:{sig.kz_quality*100:.0f}%\n"
            f"EPS: {sig.eps_score}/7 | SQS: {sig.sqs_score} | CTX: {sig.ctx_score}/8\n"
            f"VETO: {sig.veto_rsn} | ACF: {sig.acf_label}\n"
            f"PDC: {sig.pdc_zone} | LIQ: {sig.liq_status} | SFP: {sig.sfp_signal}"
        )

    dir_emoji  = "🟢 BUY"  if sig.direction == "BUY"  else "🔴 SELL"
    otype_icon = "🔽"      if "LIMIT" in sig.order_type else "🔼"
    otype_desc = (
        "📌 BUY LIMIT — Pasang limit di bawah harga, tunggu retrace"
        if sig.order_type == "BUY LIMIT" else
        "📌 SELL LIMIT — Pasang limit di atas harga, tunggu retrace"
        if sig.order_type == "SELL LIMIT" else
        "📌 BUY STOP — Pasang stop di atas harga, masuk saat breakout"
        if sig.order_type == "BUY STOP" else
        "📌 SELL STOP — Pasang stop di bawah harga, masuk saat breakdown"
    )

    ctx_bar  = "🟩" * sig.ctx_score + "⬜" * (8 - sig.ctx_score)
    sqs_bar  = "🟨" * int(sig.sqs_score) + "⬜" * (10 - int(sig.sqs_score))
    eps_bar  = "🟦" * sig.eps_score + "⬜" * (7 - sig.eps_score)

    return (
        f"{'🟢' if sig.direction=='BUY' else '🔴'} <b>PEMIF v20 UNIFIED — {sig.direction}</b>\n"
        f"📊 {symbol} | {now_wib}\n\n"
        f"{otype_icon} <b>{sig.order_type}</b>\n"
        f"{otype_desc}\n\n"
        f"<b>📍 LEVEL:</b>\n"
        f"  Entry : <code>{sig.entry}</code>  [{sig.src}]\n"
        f"  SL    : <code>{sig.sl}</code>\n"
        f"  TP1   : <code>{sig.tp1}</code>  (RR {sig.rr:.1f}x)\n"
        f"  TP2   : <code>{sig.tp2}</code>\n"
        f"  TP3   : <code>{sig.tp3}</code>\n"
        f"  TP4   : <code>{sig.tp4}</code>  [HARD EXIT]\n"
        f"  Risk  : <code>{sig.risk:.2f}</code> pts\n\n"
        f"<b>📊 SCORE:</b>\n"
        f"  EPS {sig.eps_score}/7  {eps_bar}\n"
        f"  SQS {sig.sqs_score}/10 {sqs_bar}\n"
        f"  CTX {sig.ctx_score}/8  {ctx_bar} [{sig.ctx_size}]\n\n"
        f"<b>🏗 STRUCTURE:</b>\n"
        f"  H4: {sig.h4_bias} | H1: {sig.h1_bias} | D1: {sig.d1_bias}\n"
        f"  M30: {sig.m30_bias} {sig.m30_struct}\n"
        f"  M15: {sig.m15_bias} {sig.m15_struct}\n"
        f"  MTAM: {sig.mtam_label}\n\n"
        f"<b>🔍 CONTEXT:</b>\n"
        f"  KZ: {sig.kz_name} ({sig.kz_quality*100:.0f}%)\n"
        f"  ADR Used: {sig.adr_pct:.1f}%\n"
        f"  PDC: {sig.pdc_zone} | LIQ: {sig.liq_status}\n"
        f"  SFP: {sig.sfp_signal} | ACF: {sig.acf_label}\n"
        f"  VETO: {sig.veto_rsn}"
    )

def send_telegram(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHATID:
        log.warning("Telegram credentials missing — printing to console.")
        print(msg)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id":    TELEGRAM_CHATID,
        "text":       msg,
        "parse_mode": "HTML"
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        log.info("Telegram message sent.")
    except Exception as e:
        log.error(f"Telegram error: {e}")

# ═══════════════════════════════════════════════════════════════
# STATE — anti-spam (simpan ke file JSON)
# ═══════════════════════════════════════════════════════════════
STATE_FILE = "pemif_state.json"

def load_state() -> dict:
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}

def save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

def should_send(sig: Signal, state: dict) -> bool:
    """Kirim hanya jika signal baru (direction/entry berubah)."""
    key = f"{sig.direction}_{sig.entry}_{sig.order_type}"
    last = state.get("last_signal_key", "")
    last_ts_str = state.get("last_ts", "")
    if last == key:
        # Cek apakah sudah lebih dari 4 jam
        if last_ts_str:
            try:
                last_ts = datetime.fromisoformat(last_ts_str)
                elapsed = (datetime.now(timezone.utc) - last_ts).total_seconds()
                if elapsed < 4 * 3600:
                    return False
            except Exception:
                pass
    return True

# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    log.info(f"=== PEMIF v20 UNIFIED starting — {SYMBOL} ===")

    if not TWELVEDATA_KEY:
        log.error("TWELVEDATA_KEY not set.")
        return

    mtf = fetch_all_tf(SYMBOL)

    if not mtf.bars_m5:
        log.error("No M5 data. Aborting.")
        return

    sig = analyze(mtf)

    state = load_state()
    msg   = fmt_msg(sig, SYMBOL)

    log.info(f"Gate: {sig.gate_ok} | Dir: {sig.direction} | "
             f"Type: {sig.order_type} | Entry: {sig.entry} | "
             f"EPS: {sig.eps_score}/7 | SQS: {sig.sqs_score} | "
             f"CTX: {sig.ctx_score}/8 [{sig.ctx_size}]")

    if sig.gate_ok and should_send(sig, state):
        send_telegram(msg)
        state["last_signal_key"] = f"{sig.direction}_{sig.entry}_{sig.order_type}"
        state["last_ts"]         = datetime.now(timezone.utc).isoformat()
        save_state(state)
        log.info("Signal sent and state saved.")
    elif not sig.gate_ok:
        # Kirim status "WAIT" hanya sekali per jam
        last_wait = state.get("last_wait_ts", "")
        send_wait = True
        if last_wait:
            try:
                elapsed = (datetime.now(timezone.utc) -
                           datetime.fromisoformat(last_wait)).total_seconds()
                if elapsed < 3600:
                    send_wait = False
            except Exception:
                pass
        if send_wait:
            send_telegram(msg)
            state["last_wait_ts"] = datetime.now(timezone.utc).isoformat()
            save_state(state)
    else:
        log.info("Duplicate signal — skipped.")

if __name__ == "__main__":
    main()
