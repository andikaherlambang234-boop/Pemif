#!/usr/bin/env python3
"""
PEMIF v20.3 SCALP EDITION — GitHub Actions Signal Bot
UPGRADE dari v20.2:
  [FIX-1]  NY Session: h<0 → h<4  (prime 00:00–04:00 WIB unlocked)
  [FIX-2]  PDH/PDL: bars_d1[-2]   (yesterday, bukan today's forming bar)
  [FIX-3]  TP Cascade: t1<t2<t3<t4 buy, t1>t2>t3>t4 sell — enforced
  [FIX-4]  Bar Close Confirmation: signal hanya dari closed M5 bar
  [FIX-5]  Inside bar dihapus dari candle signal (pola netral)
  [FIX-6]  STOP threshold: ATR×0.1 (was 0.02% absolute)
  [FIX-7]  Displacement BOTH → tidak confirm single direction
  [FIX-8]  SL Sync: best_entry_buy/sell return sl_r konsisten
  [FIX-9]  calc_liq: equal H/L dari 2 pivot historis, bukan avg beda pivot size
  [FIX-10] Pivot length per TF: M15=10, M30=7
  [IMP]    VWAP min 30 M5 bar hari ini (150 menit)
  [IMP]    ABE trigger level ditampilkan di Telegram
  [IMP]    Expiry hint di message
  [CLN]    Ghost params dihapus: use_rsi_div, use_htf_close, use_pinbar,
           use_vol_surge, use_fvg_fresh, use_precision, use_m15_bos,
           use_m30_bos, grace_bars, abe_on, abe_mom
  [CLN]    m15_bos_len/m30_bos_len → pivot_m15/pivot_m30
  [CLN]    bare except → (FileNotFoundError, json.JSONDecodeError)
Timezone: WIB (UTC+7)
"""

import os, json, time, logging, requests
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("PEMIF-v20.3")

# ═══════════════════════════════════════════════════════════════
# PARAMETERS — v20.3 (ghost params removed)
# ═══════════════════════════════════════════════════════════════
@dataclass
class Params:
    aggr_level:      int   = 3
    sl_min_atr:      float = 0.5
    sl_max_atr:      float = 2.0
    rr_tp1:          float = 1.5
    rr_tp2:          float = 3.0
    rr_tp3:          float = 5.0
    rr_tp4:          float = 8.0
    expiry_min:      int   = 100      # display only (20 bars × 5min)
    spread:          float = 0.30
    vol_mult:        float = 1.2
    vol_climax_mult: float = 2.5
    min_rr:          float = 1.5
    fvg_age_max:     int   = 20
    ob_age_max:      int   = 50
    fib_tol:         float = 0.6
    adx_chop_thr:    int   = 15
    adr_pct_max:     float = 90.0
    sess_lon_hr:     int   = 14
    sess_ny_hr:      int   = 20
    kz_asia:         bool  = True
    sqs_min:         float = 5.0
    ctx_min_full:    int   = 3
    ctx_min_half:    int   = 1
    use_d1_ctx:      bool  = True
    use_h1_ctx:      bool  = True
    use_m30_ctx:     bool  = True
    use_m15_ctx:     bool  = True
    use_vol_ctx:     bool  = True
    use_sfp_ctx:     bool  = True
    use_liq_ctx:     bool  = True
    use_pdc_ctx:     bool  = True
    news_active:     bool  = False
    rsi_ob:          float = 72.0
    rsi_os:          float = 28.0
    soft_min:        int   = 4
    disp_atr_mult:   float = 1.5
    # Pivot lengths per TF [FIX-10]
    pivot_h4:        int   = 5
    pivot_h1:        int   = 5
    pivot_m30:       int   = 7    # was 5
    pivot_m15:       int   = 10   # was 5
    # ABE display (level computed, shown in message)
    abe_prog:        float = 0.40
    # Bar close buffer seconds [FIX-4]
    bar_close_buf:   int   = 60

P = Params()

def get_aggr(level: int) -> dict:
    table = {
        1: dict(eps_go=5, kzq_min=0.50, lbl="UC[EPS≥5]"),
        2: dict(eps_go=4, kzq_min=0.25, lbl="CN[EPS≥4]"),
        3: dict(eps_go=4, kzq_min=0.00, lbl="BL[EPS≥4]"),
        4: dict(eps_go=3, kzq_min=0.00, lbl="AG[EPS≥3]"),
        5: dict(eps_go=2, kzq_min=0.00, lbl="UA[EPS≥2]"),
    }
    return table.get(level, table[3])

AGGR = get_aggr(P.aggr_level)

# ═══════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════
@dataclass
class Bar:
    ts: datetime; open: float; high: float; low: float; close: float; volume: float

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
    direction:    str   = "NONE"
    order_type:   str   = "NONE"
    entry:        float = 0.0
    sl:           float = 0.0
    tp1:          float = 0.0
    tp2:          float = 0.0
    tp3:          float = 0.0
    tp4:          float = 0.0
    rr:           float = 0.0
    risk:         float = 0.0
    src:          str   = "-"
    eps_score:    int   = 0
    sqs_score:    float = 0.0
    ctx_score:    int   = 0
    ctx_size:     str   = "SKIP"
    grade:        str   = "---"
    soft_count:   int   = 0
    lot_advice:   str   = "HALF"
    off_kz:       bool  = False
    h4_bias:      str   = "NEU"
    h1_bias:      str   = "NEU"
    m30_bias:     str   = "NEU"
    m30_struct:   str   = "---"
    m15_bias:     str   = "NEU"
    m15_struct:   str   = "---"
    d1_bias:      str   = "NEU"
    kz_name:      str   = "---"
    kz_quality:   float = 0.0
    adr_pct:      float = 0.0
    pdc_zone:     str   = "---"
    liq_status:   str   = "OK"
    sfp_signal:   str   = "NO"
    acf_label:    str   = "OK"
    mtam_label:   str   = "NEU"
    veto_rsn:     str   = "PASS"
    gate_ok:      bool  = False
    disp_ok:      bool  = False
    disp_dir:     str   = "NONE"
    abe_level:    float = 0.0    # [IMP] ABE trigger level
    expiry_min:   int   = 100    # [IMP] display only

# ═══════════════════════════════════════════════════════════════
# UTILS
# ═══════════════════════════════════════════════════════════════
def clamp(v, lo, hi): return max(lo, min(hi, v))

def sma(arr, n):
    if len(arr) < n: return sum(arr)/len(arr) if arr else 0.0
    return sum(arr[-n:])/n

def ema(arr, n):
    if not arr: return 0.0
    k = 2.0/(n+1); e = arr[0]
    for x in arr[1:]: e = x*k + e*(1-k)
    return e

def atr(bars, n=14):
    if len(bars) < 2: return bars[0].high-bars[0].low if bars else 1.0
    trs = []
    for i in range(1, len(bars)):
        trs.append(max(bars[i].high-bars[i].low,
                       abs(bars[i].high-bars[i-1].close),
                       abs(bars[i].low -bars[i-1].close)))
    return sma(trs, n)

def pivot_high(highs, left, right):
    result = []
    for i in range(left, len(highs)-right):
        if all(highs[i]>=h for h in highs[i-left:i]) and \
           all(highs[i]>=h for h in highs[i+1:i+right+1]):
            result.append(highs[i])
    return result

def pivot_low(lows, left, right):
    result = []
    for i in range(left, len(lows)-right):
        if all(lows[i]<=l for l in lows[i-left:i]) and \
           all(lows[i]<=l for l in lows[i+1:i+right+1]):
            result.append(lows[i])
    return result

def dmi(bars, di_len=14, adx_len=14):
    if len(bars) < di_len+2: return 0.0, 0.0, 25.0
    plus_dm, minus_dm, tr_list = [], [], []
    for i in range(1, len(bars)):
        h_diff = bars[i].high-bars[i-1].high
        l_diff = bars[i-1].low-bars[i].low
        plus_dm.append(h_diff if h_diff>l_diff and h_diff>0 else 0.0)
        minus_dm.append(l_diff if l_diff>h_diff and l_diff>0 else 0.0)
        tr_list.append(max(bars[i].high-bars[i].low,
                           abs(bars[i].high-bars[i-1].close),
                           abs(bars[i].low -bars[i-1].close)))
    atr14 = sma(tr_list, di_len) or 1e-9
    diplus  = sma(plus_dm,  di_len)/atr14*100
    diminus = sma(minus_dm, di_len)/atr14*100
    dx = abs(diplus-diminus)/max(diplus+diminus, 1e-9)*100
    return diplus, diminus, dx

def rsi(closes, length=14):
    if len(closes) < length+1: return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i]-closes[i-1]
        gains.append(max(d,0)); losses.append(max(-d,0))
    avg_g = sma(gains[-length:], length)
    avg_l = sma(losses[-length:], length) or 1e-9
    return 100-(100/(1+avg_g/avg_l))

# ═══════════════════════════════════════════════════════════════
# BAR CLOSE CONFIRMATION [FIX-4]
# ═══════════════════════════════════════════════════════════════
def is_bar_closed(bar: Bar, interval_minutes: int, buffer_sec: int = 60) -> bool:
    """True jika bar sudah closed setidaknya buffer_sec yang lalu."""
    now_utc = datetime.now(timezone.utc)
    bar_close_utc = bar.ts + timedelta(minutes=interval_minutes)
    return now_utc >= bar_close_utc + timedelta(seconds=buffer_sec)

# ═══════════════════════════════════════════════════════════════
# DATA FETCHING
# ═══════════════════════════════════════════════════════════════
def fetch_bars(symbol, interval, outputsize=200):
    url = "https://api.twelvedata.com/time_series"
    params = {"symbol":symbol,"interval":interval,"outputsize":outputsize,
              "apikey":TWELVEDATA_KEY,"format":"JSON","order":"ASC"}
    try:
        r = requests.get(url, params=params, timeout=15); r.raise_for_status()
        data = r.json()
        if "values" not in data:
            log.error(f"No values {symbol} {interval}: {data.get('message','?')}"); return []
        bars = []
        for v in data["values"]:
            dt = datetime.fromisoformat(v["datetime"])
            if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
            bars.append(Bar(ts=dt, open=float(v["open"]), high=float(v["high"]),
                            low=float(v["low"]), close=float(v["close"]),
                            volume=float(v.get("volume", 1000))))
        return bars
    except Exception as e:
        log.error(f"fetch_bars {symbol} {interval}: {e}"); return []

def fetch_all_tf(symbol):
    mtf = MTFData()
    for tf, attr in {"1min":"m1","5min":"m5","15min":"m15","30min":"m30",
                     "1h":"h1","4h":"h4","1day":"d1"}.items():
        log.info(f"Fetching {symbol} {tf}...")
        setattr(mtf, f"bars_{attr}", fetch_bars(symbol, tf, OUTPUTSZ))
        time.sleep(0.5)
    return mtf

# ═══════════════════════════════════════════════════════════════
# ANALYSIS FUNCTIONS
# ═══════════════════════════════════════════════════════════════
def calc_vwap_daily(bars):
    """[IMP] min 30 M5 bar hari ini = 150 menit untuk VWAP valid."""
    if not bars: return None, False
    today = bars[-1].ts.astimezone(WIB).date()
    tb = [b for b in bars if b.ts.astimezone(WIB).date()==today]
    if not tb: return None, False
    sv  = sum(((b.high+b.low+b.close)/3)*b.volume for b in tb)
    vol = sum(b.volume for b in tb)
    return (sv/vol if vol>0 else None), len(tb)>=30  # FIX: 15→30

def calc_kz(now_wib):
    """[FIX-1] NY session: h<4 bukan h<0. Handle midnight crossing."""
    h = now_wib.hour
    kz_asia = P.kz_asia and 7<=h<14
    kz_lon  = P.sess_lon_hr<=h<P.sess_lon_hr+8        # 14-22 WIB
    kz_ny   = h>=P.sess_ny_hr or h<4                  # FIX: h<4 (was h<0)
    kz_ovr  = kz_lon and kz_ny                        # overlap 20-22 WIB
    kz_on   = kz_asia or kz_lon or kz_ny

    if kz_ovr:
        nm="OVR"; ep=0.0
    elif kz_ny:
        nm="NY"
        el = h-P.sess_ny_hr if h>=P.sess_ny_hr else h+(24-P.sess_ny_hr)
        ep = min(el/8.0, 1.0)
    elif kz_lon:
        nm="LON"; ep=min((h-P.sess_lon_hr)/8.0, 1.0)
    elif kz_asia:
        nm="ASIA"; ep=min((h-7)/7.0, 1.0)
    else:
        nm="---"; ep=1.0

    kzq = (1.0 if kz_ovr or ep<=0.33 else
           0.75 if ep<=0.67 else
           0.5  if ep<=0.90 else 0.25)
    return kz_on, nm, kzq, (not kz_on) or kzq>=AGGR["kzq_min"]

def calc_structure(bars, pivot_len=5):
    empty = {"bias":"NEU","bos_bull":False,"bos_bear":False,"choch_bull":False,
             "choch_bear":False,"bos_struct":"---","lph":None,"lpl":None,"pph":None,"ppl":None}
    if len(bars) < pivot_len*2+2: return empty
    highs=[b.high for b in bars]; lows=[b.low for b in bars]
    ph = pivot_high(highs,pivot_len,pivot_len)
    pl = pivot_low(lows, pivot_len,pivot_len)
    lph=ph[-1] if len(ph)>=1 else None; pph=ph[-2] if len(ph)>=2 else None
    lpl=pl[-1] if len(pl)>=1 else None; ppl=pl[-2] if len(pl)>=2 else None
    c = bars[-1].close
    bb = lph and pph and c>lph; be = lpl and ppl and c<lpl
    cb = bb and lph and pph and lph<pph; ce = be and lpl and ppl and lpl>ppl
    bias = "BUL" if bb and not be else ("BER" if be and not bb else "NEU")
    s = ("CHoCH↑" if cb else "CHoCH↓" if ce else "BOS↑" if bb else "BOS↓" if be else "---")
    return {"bias":bias,"bos_bull":bb,"bos_bear":be,"choch_bull":cb,"choch_bear":ce,
            "bos_struct":s,"lph":lph,"lpl":lpl,"pph":pph,"ppl":ppl}

def simple_bias(bars, ema_len=20):
    if len(bars)<ema_len: return "NEU"
    closes=[b.close for b in bars]; e=ema(closes,ema_len)
    return "BUL" if closes[-1]>e else "BER"

def calc_fvg(bars, age_max=12):
    if len(bars)<3:
        return {"bull_tap":False,"bear_tap":False,"bull_fresh":False,"bear_fresh":False,
                "bull_mid":None,"bear_mid":None}
    bull_z=[]; bear_z=[]
    for i in range(2,len(bars)):
        if bars[i].low>bars[i-2].high:
            bull_z.append({"top":bars[i].low,"bot":bars[i-2].high,
                           "mid":(bars[i].low+bars[i-2].high)/2,"age":len(bars)-1-i})
        if bars[i].high<bars[i-2].low:
            bear_z.append({"top":bars[i-2].low,"bot":bars[i].high,
                           "mid":(bars[i-2].low+bars[i].high)/2,"age":len(bars)-1-i})
    c=bars[-1].close
    r={"bull_tap":False,"bear_tap":False,"bull_fresh":False,"bear_fresh":False,
       "bull_mid":None,"bear_mid":None}
    bd=1e9
    for z in bull_z[-5:]:
        if z["bot"]<=c<=z["top"]:
            r["bull_tap"]=True
            if z["age"]<=age_max:
                r["bull_fresh"]=True; d=abs(c-z["mid"])
                if d<bd: bd=d; r["bull_mid"]=z["mid"]
    sd=1e9
    for z in bear_z[-5:]:
        if z["bot"]<=c<=z["top"]:
            r["bear_tap"]=True
            if z["age"]<=age_max:
                r["bear_fresh"]=True; d=abs(c-z["mid"])
                if d<sd: sd=d; r["bear_mid"]=z["mid"]
    return r

def calc_ob(bars, atr14, age_max=40):
    if len(bars)<6:
        return {"bull_valid":False,"bear_valid":False,"boh":None,"bol":None,
                "soh":None,"sol":None,"bull_age":0,"bear_age":0}
    thr=atr14*1.2; closes=[b.close for b in bars]
    boh=bol=soh=sol=None; bull_age=bear_age=age_max+1
    for i in range(len(bars)-1,max(0,len(bars)-50),-1):
        if i<1: break
        if closes[i]>closes[i-1] and closes[i]-closes[i-1]>=thr:
            for j in range(i-1,max(0,i-5),-1):
                if bars[j].close<bars[j].open:
                    boh=bars[j].open; bol=bars[j].close; bull_age=len(bars)-1-i; break
            if boh: break
    for i in range(len(bars)-1,max(0,len(bars)-50),-1):
        if i<1: break
        if closes[i]<closes[i-1] and closes[i-1]-closes[i]>=thr:
            for j in range(i-1,max(0,i-5),-1):
                if bars[j].close>bars[j].open:
                    soh=bars[j].close; sol=bars[j].open; bear_age=len(bars)-1-i; break
            if soh: break
    c=bars[-1].close
    return {"bull_valid":boh and bol and bol<=c<=boh and bull_age<=age_max,
            "bear_valid":soh and sol and sol<=c<=soh and bear_age<=age_max,
            "boh":boh,"bol":bol,"soh":soh,"sol":sol,"bull_age":bull_age,"bear_age":bear_age}

def calc_fib(h4_str, atr14):
    fh=h4_str["lph"]; fl=h4_str["lpl"]
    if not fh or not fl or fh<=fl:
        return {"valid":False,"fb618":None,"fb786":None,"fs618":None,"fs786":None,"buy":False,"sell":False}
    rng=fh-fl; tol=atr14*P.fib_tol
    return {"valid":True,"fb618":fh-rng*0.618,"fb786":fh-rng*0.786,
            "fs618":fl+rng*0.618,"fs786":fl+rng*0.786,"tol":tol,"buy":False,"sell":False}

def eval_fib(fib, close):
    if not fib["valid"]: return fib
    t=fib["tol"]
    fib["buy"]  = abs(close-fib["fb618"])<=t or abs(close-fib["fb786"])<=t
    fib["sell"] = abs(close-fib["fs618"])<=t or abs(close-fib["fs786"])<=t
    return fib

def calc_sfp(bars, atr14):
    if len(bars)<12: return False, False
    highs=[b.high for b in bars]; lows=[b.low for b in bars]
    ph=pivot_high(highs,5,5); pl=pivot_low(lows,5,5)
    rh=ph[-1] if ph else None; rl=pl[-1] if pl else None
    b=bars[-1]
    sb = rl and b.low<rl and b.close>rl and (rl-b.low)>=atr14*0.5 and b.close>b.open
    se = rh and b.high>rh and b.close<rh and (b.high-rh)>=atr14*0.5 and b.close<b.open
    return bool(sb), bool(se)

def calc_liq(bars, atr14):
    """[FIX-9] Equal H/L: dua swing high/low historis yang levelnya hampir sama."""
    if len(bars)<12:
        return {"swept_h":False,"swept_l":False,"near_h":False,"near_l":False,
                "eq_high":None,"eq_low":None}
    highs=[b.high for b in bars]; lows=[b.low for b in bars]
    tol=atr14*0.15
    ph = pivot_high(highs, 3, 3)
    pl = pivot_low(lows,  3, 3)

    # Equal high: dua pivot high terakhir yang levelnya hampir sama
    eq_h = None
    if len(ph)>=2 and abs(ph[-1]-ph[-2])<=tol:
        eq_h = (ph[-1]+ph[-2])/2

    # Equal low: dua pivot low terakhir yang levelnya hampir sama
    eq_l = None
    if len(pl)>=2 and abs(pl[-1]-pl[-2])<=tol:
        eq_l = (pl[-1]+pl[-2])/2

    b=bars[-1]
    return {
        "swept_h": bool(eq_h) and b.high>eq_h and b.close<eq_h,
        "swept_l": bool(eq_l) and b.low <eq_l and b.close>eq_l,
        "near_h":  bool(eq_h) and abs(b.close-eq_h)<atr14*0.5,
        "near_l":  bool(eq_l) and abs(b.close-eq_l)<atr14*0.5,
        "eq_high": eq_h, "eq_low": eq_l
    }

def calc_candle(bars, atr14):
    """[FIX-5] Inside bar dihapus — pola netral, bukan directional."""
    if len(bars)<2: return False, False
    b=bars[-1]; b1=bars[-2]; body=abs(b.close-b.open); doji=body<atr14*0.1
    pb_bull = (min(b.open,b.close)-b.low)>=body*2.0 and \
              (b.high-max(b.open,b.close))<=body*0.5 and not doji
    pb_bear = (b.high-max(b.open,b.close))>=body*2.0 and \
              (min(b.open,b.close)-b.low)<=body*0.5 and not doji
    eg_bull = b1.close<b1.open and b.close>b.open and b.close>b1.open and b.open<b1.close
    eg_bear = b1.close>b1.open and b.close<b.open and b.close<b1.open and b.open>b1.close
    # ibar REMOVED [FIX-5]
    return (pb_bull or eg_bull) and not doji, (pb_bear or eg_bear) and not doji

def calc_vol(bars, atr14):
    vols=[b.volume for b in bars]; vsma=sma(vols,20); b=bars[-1]; body=abs(b.close-b.open)
    return {"surge":   b.volume>vsma*P.vol_mult,
            "climax":  b.volume>vsma*P.vol_climax_mult and body<atr14*0.3,
            "dry":     b.volume<vsma*0.5,
            "sma":     vsma}

def calc_pdc(h4_str, close):
    fh=h4_str["lph"]; fl=h4_str["lpl"]
    if not fh or not fl or fh<=fl:
        return {"valid":False,"discount":False,"premium":False,"mid":None,"zone":"---"}
    mid=(fh+fl)/2
    return {"valid":True,"discount":close<mid,"premium":close>mid,"mid":mid,
            "zone":"DISC" if close<mid else ("PREM" if close>mid else "MID")}

def calc_adr(bars_d1, bars_cur):
    if len(bars_d1)<14: return 0.0
    adr_avg=sma([b.high-b.low for b in bars_d1[-14:]],14) or 1e-9
    if not bars_cur: return 0.0
    today=bars_cur[-1].ts.astimezone(WIB).date()
    tb=[b for b in bars_cur if b.ts.astimezone(WIB).date()==today]
    if not tb: return 0.0
    return (max(b.high for b in tb)-min(b.low for b in tb))/adr_avg*100.0

# ═══════════════════════════════════════════════════════════════
# DISPLACEMENT CANDLE FILTER
# ═══════════════════════════════════════════════════════════════
def calc_displacement(bars, atr14, fvg, ob, h4s, liq, lookback=5):
    if len(bars) < lookback+1:
        return False, False, "NONE"
    threshold = atr14 * P.disp_atr_mult
    key_levels_high = []
    key_levels_low  = []
    if ob["bull_valid"]  and ob["bol"] is not None: key_levels_low.append(ob["bol"])
    if ob["bear_valid"]  and ob["soh"] is not None: key_levels_high.append(ob["soh"])
    if fvg["bull_mid"]  is not None: key_levels_low.append(fvg["bull_mid"])
    if fvg["bear_mid"]  is not None: key_levels_high.append(fvg["bear_mid"])
    lph=h4s.get("lph"); lpl=h4s.get("lpl")
    if lph: key_levels_high.append(lph)
    if lpl: key_levels_low.append(lpl)
    eq_h=liq.get("eq_high"); eq_l=liq.get("eq_low")
    if eq_h: key_levels_high.append(eq_h)
    if eq_l: key_levels_low.append(eq_l)
    disp_bull=False; disp_bear=False
    for bar in bars[-lookback:]:
        body=abs(bar.close-bar.open)
        if body<threshold: continue
        if bar.close>bar.open and key_levels_low:
            if bar.close>max(key_levels_low): disp_bull=True
        if bar.close<bar.open and key_levels_high:
            if bar.close<min(key_levels_high): disp_bear=True
    if disp_bull and not disp_bear:   disp_dir="BULL"
    elif disp_bear and not disp_bull: disp_dir="BEAR"
    elif disp_bull and disp_bear:     disp_dir="BOTH"
    else:                              disp_dir="NONE"
    return disp_bull, disp_bear, disp_dir

# ═══════════════════════════════════════════════════════════════
# ENTRY / EXIT HELPERS
# ═══════════════════════════════════════════════════════════════
def find_sup(ep, atr14, h4s, h1s, m30s, m15s, ob, fib, vwap, pdl, eq_low):
    c=[]
    for k in ("lpl","ppl"):
        v=h4s.get(k);
        if v and v<ep: c.append(v)
        v=h1s.get(k);
        if v and v<ep: c.append(v)
    v=m30s.get("lpl");
    if v and v<ep: c.append(v)
    v=m15s.get("lpl");
    if v and v<ep: c.append(v)
    if ob["bull_valid"] and ob["bol"] and ob["bol"]<ep: c.append(ob["bol"]-atr14*0.05)
    if fib["valid"]:
        for k in ("fb786","fb618"):
            v=fib.get(k);
            if v and v<ep: c.append(v)
    if vwap and vwap<ep: c.append(vwap)
    if pdl and pdl<ep: c.append(pdl)
    if eq_low and eq_low<ep: c.append(eq_low)
    return min(c,key=lambda v:ep-v) if c else ep-atr14*1.0

def find_res(ep, atr14, h4s, h1s, m30s, m15s, ob, fib, vwap, pdh, eq_high):
    c=[]
    for k in ("lph","pph"):
        v=h4s.get(k);
        if v and v>ep: c.append(v)
        v=h1s.get(k);
        if v and v>ep: c.append(v)
    v=m30s.get("lph");
    if v and v>ep: c.append(v)
    v=m15s.get("lph");
    if v and v>ep: c.append(v)
    if ob["bear_valid"] and ob["soh"] and ob["soh"]>ep: c.append(ob["soh"]+atr14*0.05)
    if fib["valid"]:
        for k in ("fs786","fs618"):
            v=fib.get(k);
            if v and v>ep: c.append(v)
    if vwap and vwap>ep: c.append(vwap)
    if pdh and pdh>ep: c.append(pdh)
    if eq_high and eq_high>ep: c.append(eq_high)
    return min(c,key=lambda v:v-ep) if c else ep+atr14*1.0

def best_entry_buy(close, atr14, fvg, ob, fib, vwap, sfp_bull,
                   liq, h4s, h1s, m30s, m15s, pdh, pdl, eq_high, eq_low):
    """[FIX-6][FIX-8] ATR-based STOP threshold; return sl_r untuk SL sync."""
    cands=[]
    def _e(ep, nm):
        sl_raw = find_sup(ep,atr14,h4s,h1s,m30s,m15s,ob,fib,vwap,pdl,eq_low) - atr14*0.05
        risk   = clamp(ep-sl_raw, atr14*P.sl_min_atr, atr14*P.sl_max_atr)
        tp     = find_res(ep,atr14,h4s,h1s,m30s,m15s,ob,fib,vwap,pdh,eq_high)
        rr     = (tp-ep)/risk if risk>0 else 0.0
        otype  = "BUY STOP" if ep>close+atr14*0.1 else "BUY LIMIT"  # [FIX-6]
        cands.append((ep, nm, rr, otype, sl_raw))
    if fvg["bull_fresh"] and fvg["bull_mid"]: _e(fvg["bull_mid"],"FVG")
    if ob["bull_valid"] and ob["boh"] and ob["bol"]: _e((ob["boh"]+ob["bol"])/2,"OB")
    if fib["valid"] and fib["fb618"]: _e(fib["fb618"],"F618")
    if fib["valid"] and fib["fb786"]: _e(fib["fb786"],"F786")
    if vwap: _e(vwap,"VWAP")
    if sfp_bull: _e((h4s.get("lpl") or close-atr14)+atr14*0.1,"SFP")
    if liq["swept_l"] and liq["eq_low"]: _e(liq["eq_low"]+atr14*0.1,"LIQ")
    lph=h4s.get("lph")
    if lph and lph>close: _e(lph+atr14*0.1,"BOS-H4")  # [FIX-6] buffer 0.05→0.1
    if cands:
        best=max(cands,key=lambda x:x[2])
        return best[0],best[1],best[2],best[3],best[4]  # entry,src,rr,otype,sl_r
    return close-atr14*0.7, "ATR", 0.0, "BUY LIMIT", close-atr14*1.2

def best_entry_sell(close, atr14, fvg, ob, fib, vwap, sfp_bear,
                    liq, h4s, h1s, m30s, m15s, pdh, pdl, eq_high, eq_low):
    """[FIX-6][FIX-8] ATR-based STOP threshold; return sl_r untuk SL sync."""
    cands=[]
    def _e(ep, nm):
        rs_raw = find_res(ep,atr14,h4s,h1s,m30s,m15s,ob,fib,vwap,pdh,eq_high) + atr14*0.05
        risk   = clamp(rs_raw-ep, atr14*P.sl_min_atr, atr14*P.sl_max_atr)
        sp     = find_sup(ep,atr14,h4s,h1s,m30s,m15s,ob,fib,vwap,pdl,eq_low)
        rr     = (ep-sp)/risk if risk>0 else 0.0
        otype  = "SELL STOP" if ep<close-atr14*0.1 else "SELL LIMIT"  # [FIX-6]
        cands.append((ep, nm, rr, otype, rs_raw))
    if fvg["bear_fresh"] and fvg["bear_mid"]: _e(fvg["bear_mid"],"FVG")
    if ob["bear_valid"] and ob["soh"] and ob["sol"]: _e((ob["soh"]+ob["sol"])/2,"OB")
    if fib["valid"] and fib["fs618"]: _e(fib["fs618"],"F618")
    if fib["valid"] and fib["fs786"]: _e(fib["fs786"],"F786")
    if vwap: _e(vwap,"VWAP")
    if sfp_bear: _e((h4s.get("lph") or close+atr14)-atr14*0.1,"SFP")
    if liq["swept_h"] and liq["eq_high"]: _e(liq["eq_high"]-atr14*0.1,"LIQ")
    lpl=h4s.get("lpl")
    if lpl and lpl<close: _e(lpl-atr14*0.1,"BOS-H4")  # [FIX-6] renamed + buffer 0.05→0.1
    if cands:
        best=max(cands,key=lambda x:x[2])
        return best[0],best[1],best[2],best[3],best[4]  # entry,src,rr,otype,sl_r
    return close+atr14*0.7, "ATR", 0.0, "SELL LIMIT", close+atr14*1.2

def dpe_buy(entry, risk, atr14, h4s, h1s, m30s, m15s, ob, fib, vwap, pdh, eq_high):
    """[FIX-3] TP cascade ascending enforced: t1<t2<t3<t4."""
    r1=find_res(entry,          atr14,h4s,h1s,m30s,m15s,ob,fib,vwap,pdh,eq_high)
    r2=find_res(max(r1,entry)  +atr14*0.1,atr14,h4s,h1s,m30s,m15s,ob,fib,vwap,pdh,eq_high)
    r3=find_res(max(r2,r1)     +atr14*0.1,atr14,h4s,h1s,m30s,m15s,ob,fib,vwap,pdh,eq_high)
    t1=max(r1, entry+risk*P.rr_tp1)
    t2=max(r2, entry+risk*P.rr_tp2, t1+atr14*0.05)
    t3=max(r3, entry+risk*P.rr_tp3, t2+atr14*0.05)
    t4=max(     entry+risk*P.rr_tp4, t3+atr14*0.05)
    return [t-P.spread for t in (t1,t2,t3,t4)]

def dpe_sell(entry, risk, atr14, h4s, h1s, m30s, m15s, ob, fib, vwap, pdl, eq_low):
    """[FIX-3] TP cascade descending enforced: t1>t2>t3>t4."""
    s1=find_sup(entry,          atr14,h4s,h1s,m30s,m15s,ob,fib,vwap,pdl,eq_low)
    s2=find_sup(min(s1,entry)  -atr14*0.1,atr14,h4s,h1s,m30s,m15s,ob,fib,vwap,pdl,eq_low)
    s3=find_sup(min(s2,s1)     -atr14*0.1,atr14,h4s,h1s,m30s,m15s,ob,fib,vwap,pdl,eq_low)
    t1=min(s1, entry-risk*P.rr_tp1)
    t2=min(s2, entry-risk*P.rr_tp2, t1-atr14*0.05)
    t3=min(s3, entry-risk*P.rr_tp3, t2-atr14*0.05)
    t4=min(    entry-risk*P.rr_tp4, t3-atr14*0.05)
    return [t+P.spread for t in (t1,t2,t3,t4)]

# ═══════════════════════════════════════════════════════════════
# SCORING FUNCTIONS
# ═══════════════════════════════════════════════════════════════
def calc_eps(h4b, h1b, ob, fvg, fib, vm, vwap, pdc, m1bu, m1be, aw, kz, vs, d, liq, sfpb, sfpe):
    l1 = (h4b=="BUL" and d=="BUY") or (h4b=="BER" and d=="SELL")
    l2 = (ob["bull_valid"] or ob["bear_valid"] or
          (fvg["bull_fresh"] and d=="BUY") or (fvg["bear_fresh"] and d=="SELL") or
          fib.get("buy") or fib.get("sell"))
    l3 = ((liq["swept_l"] or liq["swept_h"]) or (sfpb if d=="BUY" else sfpe))
    l4 = vm and vwap is not None
    l5 = pdc["valid"] and ((d=="BUY" and pdc["discount"]) or (d=="SELL" and pdc["premium"]))
    l6 = m1bu if d=="BUY" else m1be
    l7 = not aw and (kz or vs)
    sc = sum([l1,l2,l3,l4,l5,l6,l7])
    lbl = {6:"APEX",5:"SNIPER",4:"PRECIS",3:"STAND",2:"MARGN"}.get(sc,"SKIP")
    return sc, lbl

def calc_mtam(bm5, bm15, bh1, bh4, bd1):
    def sc(bars):
        if len(bars)<21: return 0
        closes=[b.close for b in bars]; e=ema(closes,20)
        return 1 if closes[-1]>e else -1
    s=sc(bm5)+sc(bm15)+sc(bh1)+sc(bh4)+sc(bd1); r=s/5.0
    lbl=("STR-B" if r>=0.8 else "MOD-B" if r>=0.4 else
         "STR-S" if r<=-0.8 else "MOD-S" if r<=-0.4 else "NEU")
    return lbl, r

def calc_sqs(kzn, kzq, vol, sfpb, sfpe, liq, pdc, m1bu, m1be,
             chop, trend, wait, d, cf, conflict):
    kz=2.0 if kzn=="OVR" else (1.5 if kzq>=0.75 else (1.0 if kzq>0 else 0.0))
    v=2.0 if vol["surge"] and not vol["climax"] else (1.0 if not vol["dry"] else 0.0)
    c=min(cf*0.5,2.0)
    m=2.0 if m1bu or m1be else (1.0 if not chop else 0.0)
    mo=2.0 if trend else (1.0 if not wait else 0.0)
    sf=1.0 if (sfpb and d=="BUY") or (sfpe and d=="SELL") else 0.0
    lq=1.0 if (liq["swept_l"] and d=="BUY") or (liq["swept_h"] and d=="SELL") else 0.0
    pd=0.5 if (pdc["discount"] and d=="BUY") or (pdc["premium"] and d=="SELL") else 0.0
    return round(clamp(kz+v+c+m+mo+sf+lq+pd-(1.5 if conflict else 0.0),0.0,10.0),1)

def calc_ctx(d, d1b, h1b, m30b, m15b, vol, liq, sfpb, sfpe, pdc):
    if d=="BUY":
        sc=sum([P.use_d1_ctx and d1b=="BUL", P.use_h1_ctx and h1b=="BUL",
                P.use_m30_ctx and m30b=="BUL", P.use_m15_ctx and m15b=="BUL",
                P.use_vol_ctx and vol["surge"] and not vol["climax"],
                P.use_liq_ctx and liq["swept_l"], P.use_sfp_ctx and sfpb,
                P.use_pdc_ctx and pdc["discount"]])
    else:
        sc=sum([P.use_d1_ctx and d1b=="BER", P.use_h1_ctx and h1b=="BER",
                P.use_m30_ctx and m30b=="BER", P.use_m15_ctx and m15b=="BER",
                P.use_vol_ctx and vol["surge"] and not vol["climax"],
                P.use_liq_ctx and liq["swept_h"], P.use_sfp_ctx and sfpe,
                P.use_pdc_ctx and pdc["premium"]])
    return sc, "FULL" if sc>=P.ctx_min_full else ("HALF" if sc>=P.ctx_min_half else "SKIP")

def calc_m1_micro(bars_m1):
    if len(bars_m1)<12: return False, False
    highs=[b.high for b in bars_m1]; lows=[b.low for b in bars_m1]
    ph=pivot_high(highs,3,3); pl=pivot_low(lows,3,3)
    if not ph or not pl: return False, False
    return max(highs[-5:])>ph[-1], min(lows[-5:])<pl[-1]

def calc_arne(bars, atr14):
    if len(bars)<15: return False, False
    atr_vals=[atr(bars[max(0,i-14):i+1]) for i in range(len(bars))]
    atr_ma=sma(atr_vals, min(20,len(atr_vals))) or 1.0
    ratio=atr14/atr_ma
    closes=[b.close for b in bars]; e20=ema(closes,20)
    _,_,adx=dmi(bars[-30:] if len(bars)>=30 else bars)
    expand=ratio>2.0; noise=ratio>1.5 and adx<20.0
    trend=adx>25.0 and abs(closes[-1]-e20)>atr14*0.3 and not expand
    wait=(noise or expand) if P.aggr_level<=2 else (noise if P.aggr_level==3 else False)
    return wait, trend

def calc_acf(bars_m1, bars_m5):
    if len(bars_m1)<20 or len(bars_m5)<20: return False, "OK"
    _,_,a1=dmi(bars_m1[-20:]); _,_,a5=dmi(bars_m5[-20:])
    at1=atr(bars_m1[-20:]); am1=sma([at1],1)
    chop=a1<P.adx_chop_thr and a5<P.adx_chop_thr and at1<am1*0.7
    return chop, "CHOP!" if chop else "TREND-OK"

# ═══════════════════════════════════════════════════════════════
# MAIN ANALYSIS — v20.3
# ═══════════════════════════════════════════════════════════════
def analyze(mtf: MTFData) -> Signal:
    sig = Signal()
    bars = mtf.bars_m5
    if len(bars) < 50: log.warning("Insufficient M5 bars"); return sig

    # [FIX-4] Bar close confirmation
    if not is_bar_closed(bars[-1], 5, P.bar_close_buf):
        sig.veto_rsn = "FORMING-BAR"
        log.info("M5 bar masih forming — skip."); return sig

    cb=bars[-1]; close=cb.close; now_wib=cb.ts.astimezone(WIB); atr14=atr(bars[-30:])

    # [FIX-10] Pivot length per TF
    h4s  = calc_structure(mtf.bars_h4,  P.pivot_h4)
    h1s  = calc_structure(mtf.bars_h1,  P.pivot_h1)
    m30s = calc_structure(mtf.bars_m30, P.pivot_m30)
    m15s = calc_structure(mtf.bars_m15, P.pivot_m15)

    h4b=h4s["bias"]; h1b=simple_bias(mtf.bars_h1)
    m30b=m30s["bias"]; m15b=m15s["bias"]; d1b=simple_bias(mtf.bars_d1)
    sig.h4_bias=h4b; sig.h1_bias=h1b; sig.m30_bias=m30b
    sig.m30_struct=m30s["bos_struct"]; sig.m15_bias=m15b
    sig.m15_struct=m15s["bos_struct"]; sig.d1_bias=d1b

    # [FIX-2] PDH/PDL = previous day bar (index -2)
    pdh = mtf.bars_d1[-2].high if len(mtf.bars_d1)>=2 else close+atr14
    pdl = mtf.bars_d1[-2].low  if len(mtf.bars_d1)>=2 else close-atr14

    vwap,vm=calc_vwap_daily(bars)
    kz_on,kznm,kzq,kzqok=calc_kz(now_wib)
    sig.kz_name=kznm; sig.kz_quality=kzq; sig.off_kz=not kz_on

    # KZ HARD GATE
    if not kz_on:
        sig.veto_rsn="OFF-KZ"
        log.info("OFF-KZ — no signal."); return sig

    fvg=calc_fvg(bars,P.fvg_age_max); ob=calc_ob(bars,atr14,P.ob_age_max)
    fib=eval_fib(calc_fib(h4s,atr14),close)
    pdc=calc_pdc(h4s,close); sig.pdc_zone=pdc["zone"]
    liq=calc_liq(bars,atr14)
    sig.liq_status=("SWEPT-L" if liq["swept_l"] else "SWEPT-H" if liq["swept_h"] else
                    "NEAR-H"  if liq["near_h"]  else "NEAR-L"  if liq["near_l"]  else "OK")
    sfpb,sfpe=calc_sfp(bars,atr14)
    sig.sfp_signal="BULL" if sfpb else ("BEAR" if sfpe else "NO")
    cb_bull,cb_bear=calc_candle(bars,atr14)
    vol=calc_vol(bars,atr14)
    m1bu,m1be=calc_m1_micro(mtf.bars_m1)
    acf_chop,acf_lbl=calc_acf(mtf.bars_m1,mtf.bars_m5); sig.acf_label=acf_lbl
    aw,at=calc_arne(bars,atr14)
    mtam_lbl,mtam_sc=calc_mtam(mtf.bars_m5,mtf.bars_m15,mtf.bars_h1,mtf.bars_h4,mtf.bars_d1)
    sig.mtam_label=mtam_lbl
    adr_pct=calc_adr(mtf.bars_d1,bars); sig.adr_pct=round(adr_pct,1)

    disp_bull,disp_bear,disp_dir=calc_displacement(bars,atr14,fvg,ob,h4s,liq)
    sig.disp_ok=(disp_bull or disp_bear); sig.disp_dir=disp_dir

    veto_chop=acf_chop; veto_adr=adr_pct>=P.adr_pct_max; veto_news=P.news_active
    veto_any=veto_chop or veto_adr or veto_news
    sig.veto_rsn=("CHOP" if veto_chop else
                  f"ADR>{P.adr_pct_max:.0f}%" if veto_adr else
                  "NEWS" if veto_news else "PASS")

    raw_dir="BUY" if h4b=="BUL" else ("SELL" if h4b=="BER" else "NONE")
    if raw_dir=="NONE": return sig
    direction=raw_dir

    htf_c =(direction=="BUY"  and h1b=="BER") or (direction=="SELL" and h1b=="BUL")
    mtam_c=(direction=="BUY"  and mtam_sc<=-0.6) or (direction=="SELL" and mtam_sc>=0.6)
    has_c =htf_c or mtam_c

    eps,eps_lbl=calc_eps(h4b,h1b,ob,fvg,fib,vm,vwap,pdc,m1bu,m1be,aw,kz_on,
                         vol["surge"],direction,liq,sfpb,sfpe)
    sig.eps_score=eps

    live_cf=sum([bool(vwap and abs(close-vwap)<atr14*0.3),
                 bool(fib["valid"] and fib["fb618"] and abs(close-fib["fb618"])<atr14*0.3),
                 ob["bull_valid"]  if direction=="BUY"  else ob["bear_valid"],
                 fvg["bull_fresh"] if direction=="BUY"  else fvg["bear_fresh"],
                 liq["swept_l"]    if direction=="BUY"  else liq["swept_h"],
                 sfpb              if direction=="BUY"  else sfpe])
    sqs=calc_sqs(kznm,kzq,vol,sfpb,sfpe,liq,pdc,m1bu,m1be,acf_chop,at,aw,
                 direction,live_cf,has_c)
    sig.sqs_score=sqs

    ctx_sc,ctx_sz=calc_ctx(direction,d1b,h1b,m30b,m15b,vol,liq,sfpb,sfpe,pdc)
    sig.ctx_score=ctx_sc; sig.ctx_size=ctx_sz

    rsi_val=rsi([b.close for b in bars],14)
    rsi_ok=(rsi_val<=P.rsi_ob if direction=="BUY" else rsi_val>=P.rsi_os)

    candle_ok = cb_bull if direction=="BUY" else cb_bear
    m1_ok     = m1bu   if direction=="BUY" else m1be
    pdc_ok    = pdc["discount"] if direction=="BUY" else pdc["premium"]
    liq_ok    = ((not liq["near_h"]) or liq["swept_l"]) if direction=="BUY" else \
                ((not liq["near_l"]) or liq["swept_h"])
    at_key    = bool((fvg["bull_tap"] if direction=="BUY" else fvg["bear_tap"]) or
                     (ob["bull_valid"] if direction=="BUY" else ob["bear_valid"]) or
                     (fib.get("buy")   if direction=="BUY" else fib.get("sell")))

    soft = {
        "kz":          kz_on and kzq>=AGGR["kzq_min"],
        "at_key":      at_key,
        "candle":      candle_ok,
        "m1":          m1_ok,
        "pdc":         pdc_ok,
        "no_conflict": not htf_c,
        "liq":         liq_ok,
        "no_noise":    not aw,
    }
    sc=sum(soft.values()); sig.soft_count=sc

    # [FIX-7] Displacement: BOTH tidak confirm single direction
    disp_confirmed = (
        (direction=="BUY"  and disp_bull) or
        (direction=="SELL" and disp_bear)
    )
    # disp_dir=="BOTH" → conflicting momentum → tidak confirmed → grade cap
    # disp_dir=="NONE" → tidak ada displacement → tidak confirmed → grade cap (tidak blok)

    # ════════════════════════════════════════
    # MAIN GATE v20.3
    # ════════════════════════════════════════
    gate=(
        h4b != "NEU"          and
        not acf_chop          and
        not veto_any          and
        not vol["climax"]     and
        rsi_ok                and
        kz_on                 and
        eps >= AGGR["eps_go"] and
        sqs >= P.sqs_min      and
        sc  >= P.soft_min
    )
    sig.gate_ok=gate
    if not gate: return sig

    # Grade — disp_confirmed required untuk PRIME
    if disp_confirmed:
        if eps>=5 and sqs>=7.0 and sc>=6 and kzq>=0.75: sig.grade="PRIME"
        elif eps>=4 and sqs>=5.0 and sc>=4:              sig.grade="HIGH"
        else:                                             sig.grade="STANDARD"
    else:
        # No confirmed displacement (NONE or BOTH) → cap at HIGH
        if eps>=5 and sqs>=7.0 and sc>=6 and kzq>=0.75: sig.grade="HIGH"
        else:                                             sig.grade="STANDARD"

    sig.lot_advice=("FULL" if sig.grade=="PRIME" else
                    "FULL" if sig.grade=="HIGH" and kzq>=0.75 else
                    "HALF" if sig.grade=="HIGH" else
                    "HALF" if kzq>=0.50 else "MINI")

    # [FIX-8] Entry + SL sync via sl_r
    if direction=="BUY":
        entry,src,rr,otype,sl_r=best_entry_buy(close,atr14,fvg,ob,fib,vwap,sfpb,liq,
                                                h4s,h1s,m30s,m15s,pdh,pdl,
                                                liq["eq_high"],liq["eq_low"])
        risk=clamp(entry-sl_r, atr14*P.sl_min_atr, atr14*P.sl_max_atr)
        sl=entry-risk
        tps=dpe_buy(entry,risk,atr14,h4s,h1s,m30s,m15s,ob,fib,vwap,pdh,liq["eq_high"])
    else:
        entry,src,rr,otype,sl_r=best_entry_sell(close,atr14,fvg,ob,fib,vwap,sfpe,liq,
                                                  h4s,h1s,m30s,m15s,pdh,pdl,
                                                  liq["eq_high"],liq["eq_low"])
        risk=clamp(sl_r-entry, atr14*P.sl_min_atr, atr14*P.sl_max_atr)
        sl=entry+risk
        tps=dpe_sell(entry,risk,atr14,h4s,h1s,m30s,m15s,ob,fib,vwap,pdl,liq["eq_low"])

    if rr<P.min_rr: sig.gate_ok=False; return sig

    sig.direction=direction; sig.order_type=otype
    sig.entry=round(entry,2); sig.sl=round(sl,2)
    sig.tp1=round(tps[0],2); sig.tp2=round(tps[1],2)
    sig.tp3=round(tps[2],2); sig.tp4=round(tps[3],2)
    sig.rr=round(rr,2); sig.risk=round(risk,2); sig.src=src
    sig.expiry_min=P.expiry_min

    # [IMP] ABE trigger level
    if direction=="BUY":
        sig.abe_level=round(entry+(tps[0]-entry)*P.abe_prog, 2)
    else:
        sig.abe_level=round(entry-(entry-tps[0])*P.abe_prog, 2)

    return sig

# ═══════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════
def fmt_msg(sig: Signal, symbol: str) -> str:
    now_wib=datetime.now(WIB).strftime("%d %b %Y %H:%M WIB")

    if not sig.gate_ok or sig.direction=="NONE":
        kz_info=(f"KZ:{sig.kz_name}({sig.kz_quality*100:.0f}%)"
                 if sig.kz_name!="---" else "OFF-KZ")
        return (f"🔵 <b>PEMIF v20.3 SCALP</b>\n"
                f"📊 {symbol} | {now_wib}\n"
                f"⏳ <b>WAITING</b> — {sig.veto_rsn}\n\n"
                f"H4:{sig.h4_bias} H1:{sig.h1_bias} D1:{sig.d1_bias}\n"
                f"M30:{sig.m30_bias}{sig.m30_struct} M15:{sig.m15_bias}{sig.m15_struct}\n"
                f"{kz_info} | ADR:{sig.adr_pct:.1f}%\n"
                f"EPS:{sig.eps_score}/7 SQS:{sig.sqs_score} SOFT:{sig.soft_count}/8\n"
                f"DISP:{sig.disp_dir} ACF:{sig.acf_label}\n"
                f"PDC:{sig.pdc_zone} LIQ:{sig.liq_status} SFP:{sig.sfp_signal}")

    gi={"PRIME":"🔥","HIGH":"⭐","STANDARD":"📶"}.get(sig.grade,"📶")
    li={"FULL":"🟢","HALF":"🟡","MINI":"🔴"}.get(sig.lot_advice,"🟡")
    oi="🔽" if "LIMIT" in sig.order_type else "🔼"
    od=("📌 BUY LIMIT — Pasang limit di bawah harga"   if sig.order_type=="BUY LIMIT"  else
        "📌 SELL LIMIT — Pasang limit di atas harga"   if sig.order_type=="SELL LIMIT" else
        "📌 BUY STOP — Masuk saat breakout konfirmasi" if sig.order_type=="BUY STOP"   else
        "📌 SELL STOP — Masuk saat breakdown konfirmasi")
    kz_str  = f"{sig.kz_name}({sig.kz_quality*100:.0f}%)"
    disp_tag= f"✅{sig.disp_dir}" if sig.disp_ok else "⬜NO-DISP"
    cb="🟩"*sig.ctx_score  +"⬜"*(8 -sig.ctx_score)
    sb="🟨"*int(sig.sqs_score)+"⬜"*(10-int(sig.sqs_score))
    eb="🟦"*sig.eps_score  +"⬜"*(7 -sig.eps_score)
    ft="🔸"*sig.soft_count +"⬜"*(8 -sig.soft_count)

    return (f"{gi} <b>PEMIF v20.3 — {sig.direction} [{sig.grade}]</b>\n"
            f"📊 {symbol} | {now_wib}\n\n"
            f"{oi} <b>{sig.order_type}</b>\n{od}\n\n"
            f"<b>📍 LEVEL:</b>\n"
            f"  Entry : <code>{sig.entry}</code>  [{sig.src}]\n"
            f"  SL    : <code>{sig.sl}</code>\n"
            f"  TP1   : <code>{sig.tp1}</code>  (RR {sig.rr:.1f}x)\n"
            f"  TP2   : <code>{sig.tp2}</code>\n"
            f"  TP3   : <code>{sig.tp3}</code>\n"
            f"  TP4   : <code>{sig.tp4}</code>  [HARD EXIT]\n"
            f"  Risk  : <code>{sig.risk:.2f}</code> pts\n"
            f"  ABE   : <code>{sig.abe_level}</code>  (→ pindah SL ke entry)\n"
            f"  Expiry: {sig.expiry_min} menit\n\n"
            f"<b>📊 SCORE:</b>\n"
            f"  EPS  {sig.eps_score}/7  {eb}\n"
            f"  SQS  {sig.sqs_score}/10 {sb}\n"
            f"  CTX  {sig.ctx_score}/8  {cb} [{sig.ctx_size}]\n"
            f"  SOFT {sig.soft_count}/8  {ft}\n"
            f"  DISP : {disp_tag}\n\n"
            f"<b>💼 LOT:</b> {li} {sig.lot_advice} LOT\n\n"
            f"<b>🏗 STRUCTURE:</b>\n"
            f"  H4:{sig.h4_bias} H1:{sig.h1_bias} D1:{sig.d1_bias}\n"
            f"  M30:{sig.m30_bias} {sig.m30_struct}\n"
            f"  M15:{sig.m15_bias} {sig.m15_struct}\n"
            f"  MTAM:{sig.mtam_label}\n\n"
            f"<b>🔍 CONTEXT:</b>\n"
            f"  KZ:{kz_str} ADR:{sig.adr_pct:.1f}%\n"
            f"  PDC:{sig.pdc_zone} LIQ:{sig.liq_status}\n"
            f"  SFP:{sig.sfp_signal} ACF:{sig.acf_label}\n"
            f"  VETO:{sig.veto_rsn}")

# ═══════════════════════════════════════════════════════════════
# TELEGRAM SENDER
# ═══════════════════════════════════════════════════════════════
def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHATID:
        print(msg); return
    try:
        r=requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                        json={"chat_id":TELEGRAM_CHATID,"text":msg,"parse_mode":"HTML"},
                        timeout=10)
        r.raise_for_status(); log.info("Sent.")
    except Exception as e: log.error(f"Telegram: {e}")

# ═══════════════════════════════════════════════════════════════
# STATE
# ═══════════════════════════════════════════════════════════════
STATE_FILE="pemif_state.json"

def load_state():
    try:
        with open(STATE_FILE) as f: return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):  # [CLN] bare except removed
        return {}

def save_state(state):
    with open(STATE_FILE,"w") as f: json.dump(state,f)

def should_send(sig, state):
    key=f"{sig.direction}_{sig.entry}_{sig.order_type}"
    if key==state.get("last_signal_key",""):
        ts=state.get("last_ts","")
        if ts:
            try:
                el=(datetime.now(timezone.utc)-datetime.fromisoformat(ts)).total_seconds()
                if el<2*3600: return False
            except: pass
    return True

# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    log.info(f"=== PEMIF v20.3 SCALP — {SYMBOL} ===")
    if not TWELVEDATA_KEY: log.error("TWELVEDATA_KEY not set."); return
    mtf=fetch_all_tf(SYMBOL)
    if not mtf.bars_m5: log.error("No M5 data."); return
    sig=analyze(mtf); state=load_state(); msg=fmt_msg(sig,SYMBOL)
    log.info(f"Gate:{sig.gate_ok} {sig.direction} [{sig.grade}] {sig.order_type} "
             f"Entry:{sig.entry} EPS:{sig.eps_score} SQS:{sig.sqs_score} "
             f"SOFT:{sig.soft_count}/8 DISP:{sig.disp_dir} KZ:{sig.kz_name}")
    if sig.gate_ok and should_send(sig,state):
        send_telegram(msg)
        state["last_signal_key"]=f"{sig.direction}_{sig.entry}_{sig.order_type}"
        state["last_ts"]=datetime.now(timezone.utc).isoformat()
        save_state(state)
    elif not sig.gate_ok:
        lw=state.get("last_wait_ts",""); sw=True
        if lw:
            try:
                el=(datetime.now(timezone.utc)-datetime.fromisoformat(lw)).total_seconds()
                if el<2*3600: sw=False
            except: pass
        if sw:
            send_telegram(msg)
            state["last_wait_ts"]=datetime.now(timezone.utc).isoformat()
            save_state(state)
    else: log.info("Duplicate — skipped.")

if __name__=="__main__":
    main()
