"""
PEMIF v20.4 — Trade Database (SQLite)
Semua trade history, missed trades, dan scan log tersimpan permanen.
"""

import sqlite3
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

log = logging.getLogger("PEMIF-DB")
WIB = timezone(timedelta(hours=7))
DB_PATH = Path("pemif_trades.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Inisialisasi semua tabel database."""
    with get_conn() as conn:
        conn.executescript("""
        -- ──────────────────────────────────────────────
        -- TABEL 1: Scan Log (setiap kali bot jalan)
        -- ──────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS scan_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_utc      TEXT NOT NULL,
            ts_wib      TEXT NOT NULL,
            symbol      TEXT NOT NULL,
            session     TEXT,
            direction   TEXT,
            gate_ok     INTEGER,    -- 0/1
            veto_rsn    TEXT,
            grade       TEXT,
            eps_score   INTEGER,
            sqs_score   REAL,
            ctx_score   INTEGER,
            soft_count  INTEGER,
            confidence  REAL,
            h4_bias     TEXT,
            h1_bias     TEXT,
            m30_bias    TEXT,
            m15_bias    TEXT,
            d1_bias     TEXT,
            has_bos     INTEGER,
            has_fvg     INTEGER,
            has_ob      INTEGER,
            has_liq     INTEGER,
            has_sfp     INTEGER,
            has_disp    INTEGER,
            disp_dir    TEXT,
            pdc_zone    TEXT,
            adr_pct     REAL,
            entry       REAL,
            sl          REAL,
            tp1         REAL,
            tp4         REAL,
            rr          REAL,
            order_type  TEXT,
            src         TEXT
        );

        -- ──────────────────────────────────────────────
        -- TABEL 2: Trade (entry yang benar-benar diambil)
        -- ──────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS trades (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id       INTEGER REFERENCES scan_log(id),
            ts_entry_utc  TEXT NOT NULL,
            ts_exit_utc   TEXT,
            symbol        TEXT NOT NULL,
            session       TEXT,
            direction     TEXT,
            order_type    TEXT,
            entry         REAL,
            sl            REAL,
            tp1           REAL,
            tp2           REAL,
            tp3           REAL,
            tp4           REAL,
            rr            REAL,
            risk          REAL,
            eps_score     INTEGER,
            sqs_score     REAL,
            ctx_score     INTEGER,
            soft_count    INTEGER,
            confidence    REAL,
            grade         TEXT,
            h4_bias       TEXT,
            h1_bias       TEXT,
            m30_bias      TEXT,
            m15_bias      TEXT,
            d1_bias       TEXT,
            has_bos       INTEGER,
            has_fvg       INTEGER,
            has_ob        INTEGER,
            has_liq       INTEGER,
            has_sfp       INTEGER,
            has_disp      INTEGER,
            disp_dir      TEXT,
            pdc_zone      TEXT,
            adr_pct       REAL,
            -- Result (diisi manual atau via /result command)
            result        TEXT,       -- WIN / LOSS / BE / PARTIAL
            exit_price    REAL,
            pnl_r         REAL,       -- dalam satuan R
            duration_min  INTEGER,
            notes         TEXT
        );

        -- ──────────────────────────────────────────────
        -- TABEL 3: Missed Trades
        -- ──────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS missed_trades (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id     INTEGER REFERENCES scan_log(id),
            ts_utc      TEXT NOT NULL,
            symbol      TEXT NOT NULL,
            direction   TEXT,
            veto_rsn    TEXT,
            entry       REAL,
            tp4         REAL,
            confidence  REAL,
            eps_score   INTEGER,
            sqs_score   REAL,
            missing_factors TEXT,   -- JSON list
            -- Diisi saat evaluasi
            outcome     TEXT,       -- TP4_HIT / SL_HIT / UNKNOWN
            max_favorable REAL,     -- pip move kearah trade
            evaluated   INTEGER DEFAULT 0
        );

        -- ──────────────────────────────────────────────
        -- TABEL 4: Daily Stats (cache untuk laporan)
        -- ──────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS daily_stats (
            date_wib    TEXT PRIMARY KEY,
            total_scan  INTEGER DEFAULT 0,
            total_setup INTEGER DEFAULT 0,
            total_entry INTEGER DEFAULT 0,
            total_win   INTEGER DEFAULT 0,
            total_loss  INTEGER DEFAULT 0,
            total_be    INTEGER DEFAULT 0,
            winrate     REAL,
            avg_rr      REAL,
            profit_factor REAL,
            best_session TEXT,
            worst_session TEXT,
            ai_insight  TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts_entry_utc);
        CREATE INDEX IF NOT EXISTS idx_scan_ts   ON scan_log(ts_utc);
        CREATE INDEX IF NOT EXISTS idx_missed_ts ON missed_trades(ts_utc);
        """)
    log.info("Database initialized.")


def log_scan(sig, confidence: float, missing_factors: list) -> int:
    """Catat setiap scan ke scan_log. Return scan_id."""
    now_utc = datetime.now(timezone.utc)
    now_wib = now_utc.astimezone(WIB)

    with get_conn() as conn:
        cur = conn.execute("""
            INSERT INTO scan_log (
                ts_utc, ts_wib, symbol, session, direction, gate_ok,
                veto_rsn, grade, eps_score, sqs_score, ctx_score,
                soft_count, confidence, h4_bias, h1_bias, m30_bias,
                m15_bias, d1_bias, has_bos, has_fvg, has_ob, has_liq,
                has_sfp, has_disp, disp_dir, pdc_zone, adr_pct,
                entry, sl, tp1, tp4, rr, order_type, src
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now_utc.isoformat(), now_wib.isoformat(),
            "XAU/USD", sig.kz_name, sig.direction, int(sig.gate_ok),
            sig.veto_rsn, sig.grade, sig.eps_score, sig.sqs_score,
            sig.ctx_score, sig.soft_count, confidence,
            sig.h4_bias, sig.h1_bias, sig.m30_bias, sig.m15_bias, sig.d1_bias,
            # has_bos: dari m30/m15 struct
            int("BOS" in sig.m30_struct or "BOS" in sig.m15_struct),
            int(sig.src == "FVG"), int(sig.src == "OB"),
            int(sig.liq_status in ("SWEPT-L", "SWEPT-H")),
            int(sig.sfp_signal != "NO"),
            int(sig.disp_ok), sig.disp_dir,
            sig.pdc_zone, sig.adr_pct,
            sig.entry, sig.sl, sig.tp1, sig.tp4, sig.rr,
            sig.order_type, sig.src
        ))
        scan_id = cur.lastrowid

    # Jika WAITING → simpan ke missed_trades untuk evaluasi nanti
    if not sig.gate_ok and sig.direction != "NONE":
        with get_conn() as conn:
            conn.execute("""
                INSERT INTO missed_trades (
                    scan_id, ts_utc, symbol, direction, veto_rsn,
                    entry, tp4, confidence, eps_score, sqs_score, missing_factors
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (
                scan_id, now_utc.isoformat(), "XAU/USD",
                sig.direction, sig.veto_rsn,
                sig.entry, sig.tp4, confidence,
                sig.eps_score, sig.sqs_score,
                json.dumps(missing_factors)
            ))

    return scan_id


def record_trade(sig, scan_id: int, confidence: float) -> int:
    """Simpan trade yang di-fire ke Telegram. Return trade_id."""
    now_utc = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        cur = conn.execute("""
            INSERT INTO trades (
                scan_id, ts_entry_utc, symbol, session, direction,
                order_type, entry, sl, tp1, tp2, tp3, tp4, rr, risk,
                eps_score, sqs_score, ctx_score, soft_count, confidence,
                grade, h4_bias, h1_bias, m30_bias, m15_bias, d1_bias,
                has_bos, has_fvg, has_ob, has_liq, has_sfp, has_disp,
                disp_dir, pdc_zone, adr_pct
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            scan_id, now_utc, "XAU/USD", sig.kz_name, sig.direction,
            sig.order_type, sig.entry, sig.sl, sig.tp1, sig.tp2,
            sig.tp3, sig.tp4, sig.rr, sig.risk,
            sig.eps_score, sig.sqs_score, sig.ctx_score, sig.soft_count,
            confidence, sig.grade,
            sig.h4_bias, sig.h1_bias, sig.m30_bias, sig.m15_bias, sig.d1_bias,
            int("BOS" in sig.m30_struct or "BOS" in sig.m15_struct),
            int(sig.src == "FVG"), int(sig.src == "OB"),
            int(sig.liq_status in ("SWEPT-L", "SWEPT-H")),
            int(sig.sfp_signal != "NO"),
            int(sig.disp_ok), sig.disp_dir,
            sig.pdc_zone, sig.adr_pct
        ))
        return cur.lastrowid


def update_trade_result(trade_id: int, result: str, exit_price: float,
                         pnl_r: float, duration_min: int, notes: str = ""):
    """Update hasil trade (manual via Telegram command atau future webhook)."""
    ts_exit = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute("""
            UPDATE trades SET
                result=?, exit_price=?, pnl_r=?, duration_min=?,
                ts_exit_utc=?, notes=?
            WHERE id=?
        """, (result, exit_price, pnl_r, duration_min, ts_exit, notes, trade_id))
    log.info(f"Trade {trade_id} updated: {result} | PnL={pnl_r:.2f}R")


def get_closed_trades(days: int = 90) -> list:
    """Ambil semua closed trades dalam N hari terakhir."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM trades
            WHERE result IS NOT NULL AND ts_entry_utc >= ?
            ORDER BY ts_entry_utc DESC
        """, (cutoff,)).fetchall()
    return [dict(r) for r in rows]


def get_trades_today() -> list:
    today_wib = datetime.now(WIB).date().isoformat()
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM trades
            WHERE date(ts_entry_utc, '+7 hours') = ?
        """, (today_wib,)).fetchall()
    return [dict(r) for r in rows]


def get_scans_today() -> list:
    today_wib = datetime.now(WIB).date().isoformat()
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM scan_log
            WHERE date(ts_utc, '+7 hours') = ?
        """, (today_wib,)).fetchall()
    return [dict(r) for r in rows]
  """
PEMIF v20.4 — Confidence Engine
Hitung probabilitas 0–100% berdasarkan faktor aktif.
TIDAK mengubah logika entry — hanya ringkasan probabilitas.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class ConfidenceResult:
    score: float          # 0.0 – 100.0
    category: str         # Elite / High Probability / Good / Moderate / Weak
    factor_scores: dict   # breakdown per faktor
    missing: list         # faktor yang belum terpenuhi
    present: list         # faktor yang sudah terpenuhi
    projection: dict      # proyeksi jika faktor X muncul


def calc_confidence(sig, direction: str) -> ConfidenceResult:
    """
    Hitung Confidence Score dari Signal.
    Faktor independen, bobot berdasarkan signifikansi historis ICT/SMC.
    """
    factors = {}

    # ── HTF Alignment (bobot tertinggi) ──────────────────────
    h4_align = (sig.h4_bias == "BUL" and direction == "BUY") or \
               (sig.h4_bias == "BER" and direction == "SELL")
    h1_align = (sig.h1_bias == "BUL" and direction == "BUY") or \
               (sig.h1_bias == "BER" and direction == "SELL")
    d1_align = (sig.d1_bias == "BUL" and direction == "BUY") or \
               (sig.d1_bias == "BER" and direction == "SELL")

    factors["HTF H4 Aligned"]  = (15.0, h4_align)
    factors["HTF D1 Aligned"]  = (8.0,  d1_align)
    factors["HTF H1 Aligned"]  = (7.0,  h1_align)

    # ── Structure ────────────────────────────────────────────
    bos_m30 = "BOS" in sig.m30_struct or "CHoCH" in sig.m30_struct
    bos_m15 = "BOS" in sig.m15_struct or "CHoCH" in sig.m15_struct
    factors["BOS/CHoCH M30"]    = (8.0,  bos_m30)
    factors["BOS/CHoCH M15"]    = (6.0,  bos_m15)

    # ── POI (Point of Interest) ──────────────────────────────
    has_fvg  = sig.src == "FVG"
    has_ob   = sig.src == "OB"
    has_fib  = sig.src in ("F618", "F786")
    factors["FVG Tap"]          = (8.0,  has_fvg)
    factors["Order Block"]      = (7.0,  has_ob)
    factors["Fibonacci"]        = (5.0,  has_fib)

    # ── Liquidity ────────────────────────────────────────────
    liq_swept = sig.liq_status in ("SWEPT-L", "SWEPT-H")
    liq_align = (sig.liq_status == "SWEPT-L" and direction == "BUY") or \
                (sig.liq_status == "SWEPT-H" and direction == "SELL")
    factors["Liquidity Sweep"]  = (10.0, liq_swept and liq_align)

    # ── SFP ──────────────────────────────────────────────────
    sfp_align = (sig.sfp_signal == "BULL" and direction == "BUY") or \
                (sig.sfp_signal == "BEAR" and direction == "SELL")
    factors["SFP Signal"]       = (6.0,  sfp_align)

    # ── Displacement ─────────────────────────────────────────
    disp_aligned = sig.disp_ok and (
        (sig.disp_dir == "BULL" and direction == "BUY") or
        (sig.disp_dir == "BEAR" and direction == "SELL")
    )
    factors["Displacement"]     = (7.0,  disp_aligned)

    # ── Session ──────────────────────────────────────────────
    prime_session = sig.kz_name in ("OVR", "LON", "NY") and sig.kz_quality >= 0.75
    factors["Prime Session"]    = (5.0,  prime_session)

    # ── PDC / VWAP ───────────────────────────────────────────
    pdc_align = (sig.pdc_zone == "DISC" and direction == "BUY") or \
                (sig.pdc_zone == "PREM" and direction == "SELL")
    factors["PDC Zone"]         = (4.0,  pdc_align)

    # ── Context Alignment ────────────────────────────────────
    ctx_full = sig.ctx_size == "FULL"
    ctx_half = sig.ctx_size == "HALF"
    factors["Context FULL"]     = (6.0,  ctx_full)
    factors["Context HALF"]     = (2.0,  ctx_half and not ctx_full)

    # ── Hitung total ─────────────────────────────────────────
    max_possible = sum(w for w, _ in factors.values())
    earned       = sum(w for w, v in factors.values() if v)
    raw_score    = (earned / max_possible) * 100.0 if max_possible > 0 else 0.0

    # Normalisasi ke 0–100
    score = min(round(raw_score, 1), 100.0)

    present = [name for name, (_, active) in factors.items() if active]
    missing = [name for name, (_, active) in factors.items() if not active]

    # Kategori
    if score >= 90:   category = "Elite"
    elif score >= 80: category = "High Probability"
    elif score >= 70: category = "Good"
    elif score >= 60: category = "Moderate"
    else:             category = "Weak"

    # Proyeksi: jika faktor penting muncul, confidence naik berapa?
    projection = {}
    top_missing = sorted(
        [(name, w) for name, (w, active) in factors.items() if not active],
        key=lambda x: -x[1]
    )[:3]
    for name, weight in top_missing:
        new_earned = earned + weight
        new_score  = min(round((new_earned / max_possible) * 100, 1), 100.0)
        projection[name] = new_score

    return ConfidenceResult(
        score=score, category=category,
        factor_scores={n: (w, v) for n, (w, v) in factors.items()},
        missing=missing, present=present, projection=projection
    )
  """
PEMIF v20.4 — AI Auditor
Menjelaskan MENGAPA entry diterima atau ditolak.
TIDAK mengubah logika entry.
"""

from typing import Optional
from ai.confidence import ConfidenceResult


FACTOR_DESCRIPTIONS = {
    "HTF H4 Aligned":    "H4 searah dengan signal",
    "HTF D1 Aligned":    "D1 (Daily) searah dengan signal",
    "HTF H1 Aligned":    "H1 searah dengan signal",
    "BOS/CHoCH M30":     "Struktur M30 konfirmasi (BOS/CHoCH)",
    "BOS/CHoCH M15":     "Struktur M15 konfirmasi (BOS/CHoCH)",
    "FVG Tap":           "Harga menyentuh Fair Value Gap aktif",
    "Order Block":       "Harga berada di Order Block valid",
    "Fibonacci":         "Pullback ke level Fibonacci 61.8/78.6",
    "Liquidity Sweep":   "Liquidity sweep terkonfirmasi",
    "SFP Signal":        "Swing Failure Pattern terdeteksi",
    "Displacement":      "Displacement candle terkonfirmasi",
    "Prime Session":     "Sesi prime (LON/NY/OVR) aktif",
    "PDC Zone":          "Harga di zona Discount/Premium yang tepat",
    "Context FULL":      "Full context alignment (≥3 faktor)",
    "Context HALF":      "Partial context alignment",
}

VETO_DESCRIPTIONS = {
    "FORMING-BAR":   "M5 bar belum closed — tunggu konfirmasi",
    "OFF-KZ":        "Di luar sesi trading (Asia/London/NY)",
    "CHOP":          "Pasar choppy (ADX rendah di M1 dan M5)",
    "NEWS":          "News event aktif — trading diblokir",
    "PASS":          "Tidak ada veto — setup valid",
}


def build_audit_waiting(sig, conf: ConfidenceResult) -> str:
    """Audit message untuk WAITING signal."""
    veto_desc = VETO_DESCRIPTIONS.get(sig.veto_rsn, sig.veto_rsn)
    direction = sig.direction if sig.direction != "NONE" else "?"

    lines = [
        f"🔍 <b>AI AUDITOR — WAITING {direction}</b>",
        f"",
        f"📊 Confidence: <b>{conf.score:.1f}%</b> — {conf.category}",
        f"",
        f"🚫 <b>Veto Aktif:</b> {veto_desc}",
        f"",
        f"✅ <b>Faktor Terpenuhi ({len(conf.present)}):</b>",
    ]
    for f in conf.present:
        lines.append(f"  ✓ {FACTOR_DESCRIPTIONS.get(f, f)}")

    if not conf.present:
        lines.append("  (tidak ada)")

    lines += ["", f"❌ <b>Faktor Belum Terpenuhi ({len(conf.missing)}):</b>"]
    for f in conf.missing:
        lines.append(f"  ✗ {FACTOR_DESCRIPTIONS.get(f, f)}")

    if conf.projection:
        lines += ["", "📈 <b>Proyeksi Confidence jika muncul:</b>"]
        for fname, new_score in conf.projection.items():
            delta = new_score - conf.score
            desc  = FACTOR_DESCRIPTIONS.get(fname, fname)
            lines.append(f"  + {desc}: {new_score:.1f}% (+{delta:.1f}%)")

    # Gate scores summary
    lines += [
        "",
        f"📋 <b>Gate Scores:</b>",
        f"  EPS: {sig.eps_score}/7 | SQS: {sig.sqs_score}/10",
        f"  CTX: {sig.ctx_score}/8 [{sig.ctx_size}] | SOFT: {sig.soft_count}/8",
        f"  DISP: {sig.disp_dir} | ADR: {sig.adr_pct:.1f}%",
    ]

    return "\n".join(lines)


def build_audit_entry(sig, conf: ConfidenceResult, hist_stats: Optional[dict] = None) -> str:
    """Audit message untuk signal ENTRY (gate_ok=True)."""
    lines = [
        f"🔍 <b>AI AUDITOR — {sig.direction} [{sig.grade}]</b>",
        f"",
        f"📊 Confidence: <b>{conf.score:.1f}%</b> — {conf.category}",
        f"",
        f"✅ <b>Faktor Pendukung ({len(conf.present)}):</b>",
    ]
    for f in conf.present:
        lines.append(f"  ✓ {FACTOR_DESCRIPTIONS.get(f, f)}")

    if conf.missing:
        lines += ["", f"⚠️ <b>Faktor Tidak Hadir ({len(conf.missing)}):</b>"]
        for f in conf.missing:
            lines.append(f"  – {FACTOR_DESCRIPTIONS.get(f, f)}")

    # Historical stats jika ada
    if hist_stats and hist_stats.get("total", 0) >= 10:
        wr  = hist_stats.get("winrate", 0)
        tot = hist_stats.get("total", 0)
        avg = hist_stats.get("avg_rr", 0)
        best_sess = hist_stats.get("best_session", "—")
        lines += [
            "",
            f"📚 <b>Historis Setup Serupa:</b>",
            f"  Total trade: {tot}",
            f"  Winrate: {wr:.1f}%",
            f"  Avg RR: {avg:.2f}x",
            f"  Best session: {best_sess}",
        ]
    elif hist_stats:
        lines += ["", f"📚 Historis: {hist_stats.get('total',0)} trade (min 10 untuk insight)"]
    else:
        lines += ["", "📚 Historis: Belum ada data trade selesai"]

    return "\n".join(lines)
  """
PEMIF v20.4 — Learning Engine
Hitung statistik dari trade history.
Hasilkan insight otomatis setelah 50+ closed trades.
TIDAK mengubah parameter strategi.
"""

import logging
from db.trade_db import get_closed_trades

log = logging.getLogger("PEMIF-LEARN")

MIN_TRADES_FOR_INSIGHT = 50
MIN_TRADES_FOR_STAT    = 10


def _winrate(trades: list) -> float:
    if not trades: return 0.0
    wins = sum(1 for t in trades if t.get("result") == "WIN")
    return wins / len(trades) * 100


def _profit_factor(trades: list) -> float:
    gross_profit = sum(t.get("pnl_r", 0) for t in trades if t.get("pnl_r", 0) > 0)
    gross_loss   = abs(sum(t.get("pnl_r", 0) for t in trades if t.get("pnl_r", 0) < 0))
    return round(gross_profit / gross_loss, 2) if gross_loss > 0 else gross_profit


def _avg_rr(trades: list) -> float:
    closed = [t for t in trades if t.get("pnl_r") is not None]
    return round(sum(t["pnl_r"] for t in closed) / len(closed), 2) if closed else 0.0


def calc_factor_winrates(trades: list) -> dict:
    """
    Hitung winrate saat faktor X ada vs tidak ada.
    Return dict: {faktor: {with_wr, without_wr, delta, total_with, total_without}}
    """
    factors = {
        "BOS":          "has_bos",
        "FVG":          "has_fvg",
        "OB":           "has_ob",
        "Liquidity":    "has_liq",
        "SFP":          "has_sfp",
        "Displacement": "has_disp",
    }
    result = {}
    for label, col in factors.items():
        with_f    = [t for t in trades if t.get(col) == 1]
        without_f = [t for t in trades if t.get(col) == 0]
        if len(with_f) < 3: continue
        wr_with    = _winrate(with_f)
        wr_without = _winrate(without_f) if without_f else 0.0
        result[label] = {
            "with_wr":       round(wr_with, 1),
            "without_wr":    round(wr_without, 1),
            "delta":         round(wr_with - wr_without, 1),
            "total_with":    len(with_f),
            "total_without": len(without_f),
        }
    return result


def calc_session_winrates(trades: list) -> dict:
    sessions = {}
    for t in trades:
        s = t.get("session", "---")
        sessions.setdefault(s, []).append(t)
    return {s: {"winrate": round(_winrate(ts), 1), "count": len(ts)}
            for s, ts in sessions.items() if len(ts) >= 3}


def calc_grade_winrates(trades: list) -> dict:
    grades = {}
    for t in trades:
        g = t.get("grade", "---")
        grades.setdefault(g, []).append(t)
    return {g: {"winrate": round(_winrate(ts), 1), "count": len(ts)}
            for g, ts in grades.items() if len(ts) >= 3}


def calc_confidence_buckets(trades: list) -> dict:
    """Winrate per bucket confidence: <60, 60-70, 70-80, 80-90, ≥90."""
    buckets = {"<60": [], "60-70": [], "70-80": [], "80-90": [], "≥90": []}
    for t in trades:
        c = t.get("confidence", 0)
        if c < 60:    buckets["<60"].append(t)
        elif c < 70:  buckets["60-70"].append(t)
        elif c < 80:  buckets["70-80"].append(t)
        elif c < 90:  buckets["80-90"].append(t)
        else:         buckets["≥90"].append(t)
    return {k: {"winrate": round(_winrate(v), 1), "count": len(v)}
            for k, v in buckets.items() if v}


def generate_ai_insights(trades: list) -> list:
    """
    Generate insight otomatis dari data trade.
    Hanya informatif — TIDAK mengubah strategi.
    """
    if len(trades) < MIN_TRADES_FOR_INSIGHT:
        return [f"Insight tersedia setelah {MIN_TRADES_FOR_INSIGHT} closed trades "
                f"(saat ini: {len(trades)})"]

    insights = []
    factor_wr = calc_factor_winrates(trades)
    session_wr = calc_session_winrates(trades)
    grade_wr   = calc_grade_winrates(trades)

    # Factor insights
    for factor, data in sorted(factor_wr.items(), key=lambda x: -x[1]["delta"]):
        d = data["delta"]
        if d >= 15:
            insights.append(
                f"✅ {factor} meningkatkan winrate +{d:.1f}% "
                f"({data['with_wr']:.1f}% vs {data['without_wr']:.1f}%)"
            )
        elif d <= -10:
            insights.append(
                f"⚠️ {factor} berkorelasi negatif ({d:+.1f}%) — evaluasi ulang bobot"
            )
        elif abs(d) <= 3 and data["total_with"] >= 20:
            insights.append(
                f"ℹ️ {factor} dampak minimal ({d:+.1f}%) — pertimbangkan review filter"
            )

    # Session insights
    if session_wr:
        best_sess  = max(session_wr, key=lambda x: session_wr[x]["winrate"])
        worst_sess = min(session_wr, key=lambda x: session_wr[x]["winrate"])
        insights.append(
            f"🏆 Sesi terbaik: {best_sess} ({session_wr[best_sess]['winrate']:.1f}%)"
        )
        if session_wr[worst_sess]["winrate"] < 40:
            insights.append(
                f"🚫 Sesi terlemah: {worst_sess} ({session_wr[worst_sess]['winrate']:.1f}%) "
                f"— pertimbangkan filter"
            )

    # Grade insights
    if "PRIME" in grade_wr:
        g = grade_wr["PRIME"]
        insights.append(f"🔥 Grade PRIME winrate: {g['winrate']:.1f}% dari {g['count']} trade")

    return insights if insights else ["Belum cukup variasi data untuk insight spesifik."]


def get_similar_setup_stats(sig) -> Optional[dict]:
    """
    Cari trade historis dengan setup serupa (direction + grade + session).
    Return statistik winrate setup tersebut.
    """
    try:
        trades = get_closed_trades(days=180)
        similar = [
            t for t in trades
            if t.get("direction") == sig.direction
            and t.get("session") == sig.kz_name
        ]
        if not similar:
            return {"total": 0}

        sessions = {}
        for t in similar:
            s = t.get("session", "---")
            sessions.setdefault(s, []).append(t)

        best_sess = max(sessions, key=lambda x: _winrate(sessions[x])) if sessions else "—"

        return {
            "total":        len(similar),
            "winrate":      round(_winrate(similar), 1),
            "avg_rr":       _avg_rr(similar),
            "best_session": best_sess,
        }
    except Exception as e:
        log.error(f"get_similar_setup_stats: {e}")
        return None
      """
PEMIF v20.4 — Report Engine
Daily Report (00:00 WIB) & Weekly Report (Senin 00:00 WIB)
"""

import logging
from datetime import datetime, timezone, timedelta
from db.trade_db import get_conn
from ai.learning import (
    calc_factor_winrates, calc_session_winrates,
    calc_grade_winrates, calc_confidence_buckets,
    generate_ai_insights, _winrate, _profit_factor, _avg_rr
)

log = logging.getLogger("PEMIF-REPORT")
WIB = timezone(timedelta(hours=7))


def _get_trades_range(start_wib_date: str, end_wib_date: str) -> list:
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM trades
            WHERE date(ts_entry_utc, '+7 hours') >= ?
              AND date(ts_entry_utc, '+7 hours') <= ?
            ORDER BY ts_entry_utc
        """, (start_wib_date, end_wib_date)).fetchall()
    return [dict(r) for r in rows]


def _get_scans_range(start_wib_date: str, end_wib_date: str) -> list:
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM scan_log
            WHERE date(ts_utc, '+7 hours') >= ?
              AND date(ts_utc, '+7 hours') <= ?
        """, (start_wib_date, end_wib_date)).fetchall()
    return [dict(r) for r in rows]


def _get_missed_range(start_wib_date: str, end_wib_date: str) -> list:
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM missed_trades
            WHERE date(ts_utc, '+7 hours') >= ?
              AND date(ts_utc, '+7 hours') <= ?
              AND outcome IS NOT NULL
        """, (start_wib_date, end_wib_date)).fetchall()
    return [dict(r) for r in rows]


def build_daily_report() -> str:
    now_wib  = datetime.now(WIB)
    today    = now_wib.date()
    date_str = today.isoformat()

    trades = _get_trades_range(date_str, date_str)
    scans  = _get_scans_range(date_str, date_str)
    missed = _get_missed_range(date_str, date_str)

    closed  = [t for t in trades if t.get("result") is not None]
    wins    = [t for t in closed if t.get("result") == "WIN"]
    losses  = [t for t in closed if t.get("result") == "LOSS"]
    bes     = [t for t in closed if t.get("result") == "BE"]

    total_setup = sum(1 for s in scans if s.get("gate_ok") == 1)
    wr  = _winrate(closed)
    pf  = _profit_factor(closed)
    avg = _avg_rr(closed)

    sess_wr  = calc_session_winrates(closed) if len(closed) >= 3 else {}
    grade_wr = calc_grade_winrates(closed) if len(closed) >= 3 else {}
    insights = generate_ai_insights(closed)

    best_sess  = max(sess_wr,  key=lambda x: sess_wr[x]["winrate"])  if sess_wr  else "—"
    worst_sess = min(sess_wr,  key=lambda x: sess_wr[x]["winrate"])  if sess_wr  else "—"
    best_grade = max(grade_wr, key=lambda x: grade_wr[x]["winrate"]) if grade_wr else "—"

    missed_success = [m for m in missed if m.get("outcome") == "TP4_HIT"]

    msg = (
        f"📊 <b>PEMIF DAILY INTELLIGENCE REPORT</b>\n"
        f"📅 {today.strftime('%d %B %Y')} (WIB)\n"
        f"{'─' * 30}\n\n"
        f"<b>🔢 OVERVIEW</b>\n"
        f"  Total Scan   : {len(scans)}\n"
        f"  Setup Valid  : {total_setup}\n"
        f"  Entry Fired  : {len(trades)}\n"
        f"  Closed       : {len(closed)}\n\n"
        f"<b>📈 HASIL</b>\n"
        f"  WIN  : {len(wins)}\n"
        f"  LOSS : {len(losses)}\n"
        f"  BE   : {len(bes)}\n"
        f"  Winrate      : {wr:.1f}%\n"
        f"  Avg RR       : {avg:.2f}x\n"
        f"  Profit Factor: {pf:.2f}\n\n"
        f"<b>🏆 SESSION</b>\n"
        f"  Best  : {best_sess}\n"
        f"  Worst : {worst_sess}\n\n"
        f"<b>🎯 GRADE</b>\n"
        f"  Best Grade: {best_grade}\n"
    )

    if missed_success:
        msg += (
            f"\n<b>⚠️ MISSED OPPORTUNITY</b>\n"
            f"  {len(missed_success)} setup ditolak → harga mencapai TP4\n"
        )
        for m in missed_success[:3]:
            msg += f"  • {m['direction']} ditolak [{m['veto_rsn']}]\n"

    msg += f"\n<b>🤖 AI INSIGHT HARIAN</b>\n"
    for ins in insights[:5]:
        msg += f"  {ins}\n"

    return msg


def build_weekly_report() -> str:
    now_wib   = datetime.now(WIB)
    end_date  = now_wib.date()
    start_date = end_date - timedelta(days=6)

    trades = _get_trades_range(start_date.isoformat(), end_date.isoformat())
    scans  = _get_scans_range(start_date.isoformat(), end_date.isoformat())
    closed = [t for t in trades if t.get("result") is not None]
    wins   = [t for t in closed if t.get("result") == "WIN"]
    losses = [t for t in closed if t.get("result") == "LOSS"]

    wr      = _winrate(closed)
    pf      = _profit_factor(closed)
    avg     = _avg_rr(closed)
    sess_wr = calc_session_winrates(closed)
    grade_wr= calc_grade_winrates(closed)
    fac_wr  = calc_factor_winrates(closed)
    conf_bk = calc_confidence_buckets(closed)
    insights= generate_ai_insights(closed)

    best_sess  = max(sess_wr,  key=lambda x: sess_wr[x]["winrate"])  if sess_wr  else "—"
    worst_sess = min(sess_wr,  key=lambda x: sess_wr[x]["winrate"])  if sess_wr  else "—"

    # Best day
    days = {}
    for t in closed:
        d = t.get("ts_entry_utc", "")[:10]
        days.setdefault(d, []).append(t)
    best_day  = max(days, key=lambda x: _winrate(days[x])) if days else "—"
    worst_day = min(days, key=lambda x: _winrate(days[x])) if days else "—"

    msg = (
        f"📊 <b>PEMIF WEEKLY INTELLIGENCE REPORT</b>\n"
        f"📅 {start_date.strftime('%d %b')} – {end_date.strftime('%d %b %Y')} (WIB)\n"
        f"{'─' * 30}\n\n"
        f"<b>🔢 SUMMARY</b>\n"
        f"  Total Scan  : {len(scans)}\n"
        f"  Total Trade : {len(trades)}\n"
        f"  Closed      : {len(closed)}\n"
        f"  WIN / LOSS  : {len(wins)} / {len(losses)}\n"
        f"  Winrate     : {wr:.1f}%\n"
        f"  Avg RR      : {avg:.2f}x\n"
        f"  Profit Factor: {pf:.2f}\n\n"
        f"<b>📅 HARI</b>\n"
        f"  Best  : {best_day}\n"
        f"  Worst : {worst_day}\n\n"
        f"<b>🏆 SESSION</b>\n"
    )
    for s, d in sorted(sess_wr.items(), key=lambda x: -x[1]["winrate"]):
        msg += f"  {s}: {d['winrate']:.1f}% ({d['count']} trade)\n"

    msg += f"\n<b>🎯 GRADE</b>\n"
    for g, d in sorted(grade_wr.items(), key=lambda x: -x[1]["winrate"]):
        msg += f"  {g}: {d['winrate']:.1f}% ({d['count']} trade)\n"

    if conf_bk:
        msg += f"\n<b>📊 CONFIDENCE BUCKET</b>\n"
        for bucket, d in conf_bk.items():
            msg += f"  {bucket}%: WR={d['winrate']:.1f}% ({d['count']} trade)\n"

    if fac_wr:
        top_factors = sorted(fac_wr.items(), key=lambda x: -x[1]["delta"])[:5]
        msg += f"\n<b>🔍 TOP FAKTOR (WR IMPACT)</b>\n"
        for f, d in top_factors:
            sign = "+" if d["delta"] >= 0 else ""
            msg += f"  {f}: {sign}{d['delta']:.1f}% delta\n"

    msg += f"\n<b>🤖 AI RECOMMENDATION MINGGUAN</b>\n"
    for ins in insights[:8]:
        msg += f"  {ins}\n"

    return msg
#!/usr/bin/env python3
"""
PEMIF v20.4 — AI Auditor + Learning Engine
Upgrade dari v20.3 tanpa mengubah logika entry.
"""

import os, json, time, logging, requests
from datetime import datetime, timezone, timedelta

# Import semua dari v20.3 (tidak diubah)
from core.analyzer import analyze, fetch_all_tf, MTFData, Params, Signal
from core.params import P, WIB, SYMBOL

# Import modul baru v20.4
from db.trade_db import init_db, log_scan, record_trade, get_closed_trades
from ai.confidence import calc_confidence
from ai.auditor import build_audit_waiting, build_audit_entry
from ai.learning import generate_ai_insights, get_similar_setup_stats
from reports.report_engine import build_daily_report, build_weekly_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("PEMIF-v20.4")

TELEGRAM_TOKEN  = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHATID = os.environ.get("TELEGRAM_CHATID", "")
TWELVEDATA_KEY  = os.environ.get("TWELVEDATA_KEY", "")

STATE_FILE = "pemif_state.json"


def load_state() -> dict:
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def should_send(sig, state: dict) -> bool:
    key = f"{sig.direction}_{sig.entry}_{sig.order_type}"
    if key == state.get("last_signal_key", ""):
        ts = state.get("last_ts", "")
        if ts:
            try:
                el = (datetime.now(timezone.utc) -
                      datetime.fromisoformat(ts)).total_seconds()
                if el < 2 * 3600:
                    return False
            except Exception:
                pass
    return True


def should_send_wait(state: dict) -> bool:
    lw = state.get("last_wait_ts", "")
    if lw:
        try:
            el = (datetime.now(timezone.utc) -
                  datetime.fromisoformat(lw)).total_seconds()
            if el < 2 * 3600:
                return False
        except Exception:
            pass
    return True


def should_send_daily(state: dict) -> bool:
    """Kirim daily report sekali per hari."""
    today = datetime.now(WIB).date().isoformat()
    return state.get("last_daily_report", "") != today


def should_send_weekly(state: dict) -> bool:
    """Kirim weekly report setiap Senin."""
    now_wib = datetime.now(WIB)
    if now_wib.weekday() != 0:  # 0 = Monday
        return False
    this_week = now_wib.strftime("%Y-W%W")
    return state.get("last_weekly_report", "") != this_week


def send_telegram(msg: str, parse_mode: str = "HTML"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHATID:
        print(msg)
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id":    TELEGRAM_CHATID,
                "text":       msg,
                "parse_mode": parse_mode
            },
            timeout=10
        )
        r.raise_for_status()
        log.info("Telegram sent.")
    except Exception as e:
        log.error(f"Telegram error: {e}")


def fmt_signal_msg(sig: Signal, symbol: str) -> str:
    """Format pesan signal utama (dari v20.3, tidak diubah)."""
    from bot.telegram_bot import fmt_msg  # import dari modul terpisah
    return fmt_msg(sig, symbol)


def build_full_message(sig: Signal, conf_result, hist_stats, symbol: str) -> str:
    """
    Gabungkan signal message (v20.3) + AI Auditor block (v20.4).
    Tidak mengubah signal message inti.
    """
    signal_block = fmt_signal_msg(sig, symbol)

    if sig.gate_ok:
        audit_block = build_audit_entry(sig, conf_result, hist_stats)
    else:
        audit_block = build_audit_waiting(sig, conf_result)

    return f"{signal_block}\n\n{'─' * 30}\n\n{audit_block}"


def main():
    log.info(f"=== PEMIF v20.4 AI AUDITOR — {SYMBOL} ===")

    # Init DB
    init_db()

    if not TWELVEDATA_KEY:
        log.error("TWELVEDATA_KEY not set.")
        return

    state = load_state()

    # ── Scheduled Reports ────────────────────────────────
    if should_send_daily(state):
        try:
            daily_msg = build_daily_report()
            send_telegram(daily_msg)
            state["last_daily_report"] = datetime.now(WIB).date().isoformat()
            log.info("Daily report sent.")
        except Exception as e:
            log.error(f"Daily report failed: {e}")

    if should_send_weekly(state):
        try:
            weekly_msg = build_weekly_report()
            send_telegram(weekly_msg)
            state["last_weekly_report"] = datetime.now(WIB).strftime("%Y-W%W")
            log.info("Weekly report sent.")
        except Exception as e:
            log.error(f"Weekly report failed: {e}")

    # ── Main Analysis ────────────────────────────────────
    from core.analyzer import fetch_all_tf
    mtf = fetch_all_tf(SYMBOL)
    if not mtf.bars_m5:
        log.error("No M5 data.")
        return

    sig = analyze(mtf)

    # ── Confidence Engine ────────────────────────────────
    direction = sig.direction if sig.direction != "NONE" else \
                ("BUY" if sig.h4_bias == "BUL" else "SELL")
    conf_result = calc_confidence(sig, direction)

    # ── Similar Setup Stats ──────────────────────────────
    hist_stats = get_similar_setup_stats(sig)

    # Missing factors untuk missed_trades log
    missing_factors = conf_result.missing

    # ── Log ke Database ──────────────────────────────────
    try:
        scan_id = log_scan(sig, conf_result.score, missing_factors)
    except Exception as e:
        log.error(f"log_scan failed: {e}")
        scan_id = None

    # ── Build Full Message ───────────────────────────────
    msg = build_full_message(sig, conf_result, hist_stats, SYMBOL)

    log.info(
        f"Gate:{sig.gate_ok} {sig.direction} [{sig.grade}] "
        f"Confidence:{conf_result.score:.1f}% [{conf_result.category}] "
        f"EPS:{sig.eps_score} SQS:{sig.sqs_score} SOFT:{sig.soft_count}/8"
    )

    # ── Send / Dedup Logic ───────────────────────────────
    if sig.gate_ok and should_send(sig, state):
        send_telegram(msg)

        # Catat trade ke DB
        if scan_id:
            try:
                trade_id = record_trade(sig, scan_id, conf_result.score)
                log.info(f"Trade recorded: ID={trade_id}")
            except Exception as e:
                log.error(f"record_trade failed: {e}")

        state["last_signal_key"] = f"{sig.direction}_{sig.entry}_{sig.order_type}"
        state["last_ts"] = datetime.now(timezone.utc).isoformat()

    elif not sig.gate_ok and should_send_wait(state):
        send_telegram(msg)
        state["last_wait_ts"] = datetime.now(timezone.utc).isoformat()

    else:
        log.info("Duplicate — skipped.")

    save_state(state)


if __name__ == "__main__":
    main()
