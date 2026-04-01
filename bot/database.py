import sqlite3
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional

DB_PATH = Path(__file__).parent.parent / "data" / "trades.db"


@dataclass
class Trade:
    id: Optional[int] = None
    timestamp: float = 0.0
    market_id: str = ""
    asset: str = ""  # BTC or ETH
    contract_type: str = ""  # 5min or 15min
    side: str = ""  # YES or NO
    direction: str = ""  # UP or DOWN prediction
    entry_price: float = 0.0
    size_usdc: float = 0.0
    edge_pct: float = 0.0
    confidence: float = 0.0
    binance_price: float = 0.0
    poly_implied_price: float = 0.0
    exit_price: Optional[float] = None
    pnl: Optional[float] = None
    resolved: bool = False
    won: Optional[bool] = None
    paper: bool = True


class Database:
    def __init__(self, db_path: Path = DB_PATH):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL NOT NULL,
                    market_id TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    contract_type TEXT NOT NULL,
                    side TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    size_usdc REAL NOT NULL,
                    edge_pct REAL NOT NULL,
                    confidence REAL NOT NULL,
                    binance_price REAL NOT NULL,
                    poly_implied_price REAL NOT NULL,
                    exit_price REAL,
                    pnl REAL,
                    resolved INTEGER DEFAULT 0,
                    won INTEGER,
                    paper INTEGER DEFAULT 1
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL NOT NULL,
                    balance REAL NOT NULL,
                    daily_pnl REAL NOT NULL,
                    total_pnl REAL NOT NULL,
                    win_rate REAL NOT NULL,
                    total_trades INTEGER NOT NULL,
                    open_positions INTEGER NOT NULL
                )
            """)

    def insert_trade(self, trade: Trade) -> int:
        with self._conn() as conn:
            cursor = conn.execute("""
                INSERT INTO trades (timestamp, market_id, asset, contract_type, side,
                    direction, entry_price, size_usdc, edge_pct, confidence,
                    binance_price, poly_implied_price, exit_price, pnl, resolved, won, paper)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade.timestamp or time.time(), trade.market_id, trade.asset,
                trade.contract_type, trade.side, trade.direction, trade.entry_price,
                trade.size_usdc, trade.edge_pct, trade.confidence, trade.binance_price,
                trade.poly_implied_price, trade.exit_price, trade.pnl,
                int(trade.resolved), trade.won, int(trade.paper)
            ))
            return cursor.lastrowid

    def resolve_trade(self, trade_id: int, exit_price: float, pnl: float, won: bool):
        with self._conn() as conn:
            conn.execute("""
                UPDATE trades SET exit_price = ?, pnl = ?, resolved = 1, won = ?
                WHERE id = ?
            """, (exit_price, pnl, int(won), trade_id))

    def get_recent_trades(self, limit: int = 10) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM trades ORDER BY timestamp DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_stats(self) -> dict:
        with self._conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM trades WHERE resolved = 1").fetchone()[0]
            wins = conn.execute("SELECT COUNT(*) FROM trades WHERE won = 1").fetchone()[0]
            total_pnl = conn.execute("SELECT COALESCE(SUM(pnl), 0) FROM trades WHERE resolved = 1").fetchone()[0]
            open_count = conn.execute("SELECT COUNT(*) FROM trades WHERE resolved = 0").fetchone()[0]

            today_start = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            ).timestamp()
            daily_pnl = conn.execute(
                "SELECT COALESCE(SUM(pnl), 0) FROM trades WHERE resolved = 1 AND timestamp >= ?",
                (today_start,)
            ).fetchone()[0]

            return {
                "total_trades": total,
                "wins": wins,
                "win_rate": (wins / total * 100) if total > 0 else 0.0,
                "total_pnl": round(total_pnl, 2),
                "daily_pnl": round(daily_pnl, 2),
                "open_positions": open_count,
            }

    def save_snapshot(self, balance: float, stats: dict):
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO portfolio_snapshots
                    (timestamp, balance, daily_pnl, total_pnl, win_rate, total_trades, open_positions)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                time.time(), balance, stats["daily_pnl"], stats["total_pnl"],
                stats["win_rate"], stats["total_trades"], stats["open_positions"]
            ))

    def get_snapshots(self, limit: int = 288) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM portfolio_snapshots ORDER BY timestamp DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]
