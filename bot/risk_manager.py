import math
import time
import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger("arb.risk")


@dataclass
class RiskState:
    initial_balance: float = 0.0
    current_balance: float = 0.0
    daily_start_balance: float = 0.0
    daily_pnl: float = 0.0
    peak_balance: float = 0.0
    total_exposure: float = 0.0
    kill_switch_active: bool = False
    daily_halt_active: bool = False
    last_daily_reset: float = 0.0


class RiskManager:
    """Position sizing (Kelly) and drawdown protection."""

    def __init__(
        self,
        max_position_pct: float = 0.08,
        daily_loss_limit: float = 0.20,
        kill_switch_drawdown: float = 0.40,
        kelly_fraction: float = 0.5,
    ):
        self.max_position_pct = max_position_pct
        self.daily_loss_limit = daily_loss_limit
        self.kill_switch_drawdown = kill_switch_drawdown
        self.kelly_fraction = kelly_fraction
        self.state = RiskState()

    def initialize(self, balance: float):
        now = time.time()
        self.state.initial_balance = balance
        self.state.current_balance = balance
        self.state.daily_start_balance = balance
        self.state.peak_balance = balance
        self.state.last_daily_reset = now
        logger.info(f"Risk manager initialized with balance: ${balance:.2f}")

    def reset_daily(self):
        self.state.daily_start_balance = self.state.current_balance
        self.state.daily_pnl = 0.0
        self.state.daily_halt_active = False
        self.state.last_daily_reset = time.time()
        logger.info("Daily risk counters reset")

    def can_trade(self) -> tuple[bool, str]:
        if self.state.kill_switch_active:
            return False, "KILL SWITCH ACTIVE — total drawdown exceeded limit"

        if self.state.daily_halt_active:
            return False, "DAILY HALT — daily loss limit exceeded"

        return True, "OK"

    def calculate_position_size(self, edge: float, win_prob: float) -> float:
        """Half-Kelly position sizing capped at max_position_pct."""
        if edge <= 0 or win_prob <= 0.5:
            return 0.0

        # Kelly fraction: f* = (bp - q) / b
        # For binary contracts: b = (1/entry_price - 1), p = win_prob, q = 1 - p
        # Simplified for edge-based: f* = edge / odds
        b = 1.0  # binary payout
        p = win_prob
        q = 1.0 - p
        kelly = (b * p - q) / b
        kelly = max(0.0, kelly)

        # Apply fraction (half-Kelly)
        sized = kelly * self.kelly_fraction

        # Cap at max position
        max_size = self.state.current_balance * self.max_position_pct
        position = min(sized * self.state.current_balance, max_size)

        return round(max(0.0, position), 2)

    def update_after_trade(self, pnl: float):
        self.state.current_balance += pnl
        self.state.daily_pnl += pnl
        self.state.peak_balance = max(self.state.peak_balance, self.state.current_balance)

        # Check daily loss limit
        if self.state.daily_start_balance > 0:
            daily_drawdown = -self.state.daily_pnl / self.state.daily_start_balance
            if daily_drawdown >= self.daily_loss_limit:
                self.state.daily_halt_active = True
                logger.warning(
                    f"DAILY HALT triggered: daily loss {daily_drawdown:.1%} >= {self.daily_loss_limit:.1%}"
                )

        # Check total drawdown kill switch
        if self.state.peak_balance > 0:
            total_drawdown = (self.state.peak_balance - self.state.current_balance) / self.state.peak_balance
            if total_drawdown >= self.kill_switch_drawdown:
                self.state.kill_switch_active = True
                logger.critical(
                    f"KILL SWITCH triggered: drawdown {total_drawdown:.1%} >= {self.kill_switch_drawdown:.1%}"
                )

    def get_state(self) -> dict:
        return {
            "balance": round(self.state.current_balance, 2),
            "daily_pnl": round(self.state.daily_pnl, 2),
            "peak_balance": round(self.state.peak_balance, 2),
            "total_drawdown_pct": round(
                ((self.state.peak_balance - self.state.current_balance) / self.state.peak_balance * 100)
                if self.state.peak_balance > 0 else 0.0, 2
            ),
            "daily_drawdown_pct": round(
                (-self.state.daily_pnl / self.state.daily_start_balance * 100)
                if self.state.daily_start_balance > 0 else 0.0, 2
            ),
            "kill_switch": self.state.kill_switch_active,
            "daily_halt": self.state.daily_halt_active,
            "exposure": round(self.state.total_exposure, 2),
        }
