import time
import math
import logging
from dataclasses import dataclass
from typing import Optional
from collections import deque

logger = logging.getLogger("arb.edge")


@dataclass
class Signal:
    market_id: str
    asset: str
    contract_type: str
    direction: str
    side: str
    edge_pct: float
    confidence: float
    binance_price: float
    poly_implied_price: float
    poly_yes_price: float
    poly_no_price: float
    token_id: str
    seconds_to_expiry: float
    timestamp: float


class EdgeDetector:
    """
    TRUE LATENCY ARBITRAGE — v3

    Instead of trying to estimate "true probability" (Polymarket is better at that),
    we detect when Binance moves BUT Polymarket odds DON'T move in response.

    If BTC drops $25 in 3 seconds and Polymarket's DOWN odds haven't budged,
    those DOWN shares are underpriced RIGHT NOW. Buy them before the odds catch up.

    The edge = how much the odds SHOULD have moved but DIDN'T.
    """

    def __init__(self, min_edge: float = 0.03, min_confidence: float = 0.60):
        self.min_edge = min_edge
        self.min_confidence = min_confidence

        self._prices: dict[str, deque] = {
            "BTC": deque(maxlen=2000),
            "ETH": deque(maxlen=2000),
        }

        # Track Polymarket odds over time to detect staleness
        self._odds_snapshots: deque = deque(maxlen=500)

        self._candle_open: dict[str, float] = {}
        self._last_candle_fetch = 0.0
        self._last_signal_time = 0.0
        self._min_signal_gap = 5.0

    def update_price(self, asset: str, price: float, ts: float):
        self._prices[asset].append((ts, price))

    def record_odds(self, yes_price: float, no_price: float):
        """Called every scan cycle to track how odds evolve."""
        self._odds_snapshots.append((time.time(), yes_price, no_price))

    async def fetch_candle_open(self, asset: str = "BTC"):
        now = time.time()
        if now - self._last_candle_fetch < 5:
            return
        try:
            import httpx
            async with httpx.AsyncClient(timeout=5) as client:
                resp = await client.get(
                    "https://api.binance.com/api/v3/klines",
                    params={"symbol": f"{asset}USDT", "interval": "5m", "limit": 1}
                )
                data = resp.json()
                if data and len(data) > 0:
                    self._candle_open[asset] = float(data[0][1])
                    self._last_candle_fetch = now
        except Exception:
            pass

    def _get_price_at(self, asset: str, seconds_ago: float) -> Optional[float]:
        """Get BTC price from N seconds ago."""
        prices = self._prices.get(asset)
        if not prices:
            return None
        target_ts = prices[-1][0] - seconds_ago
        # Find closest price to target timestamp
        best = None
        for ts, p in reversed(prices):
            if ts <= target_ts:
                best = p
                break
        return best

    def _get_odds_at(self, seconds_ago: float) -> Optional[tuple[float, float]]:
        """Get Polymarket odds from N seconds ago."""
        if not self._odds_snapshots:
            return None
        target_ts = time.time() - seconds_ago
        best = None
        for ts, y, n in reversed(self._odds_snapshots):
            if ts <= target_ts:
                best = (y, n)
                break
        return best

    def detect(self, asset: str, market) -> Optional[Signal]:
        now = time.time()
        seconds_to_expiry = market.end_time - now

        if seconds_to_expiry < 10:
            return None

        if now - self._last_signal_time < self._min_signal_gap:
            return None

        # Record current odds
        self.record_odds(market.yes_price, market.no_price)

        candle_open = self._candle_open.get(asset)
        prices = self._prices.get(asset)
        if not candle_open or not prices or len(prices) < 30:
            return None

        current_price = prices[-1][1]
        diff_from_open = current_price - candle_open

        # === THE CORE LATENCY ARB CHECK ===
        # For each time window: compare how much BTC moved vs how much odds moved
        # If BTC moved a lot but odds barely changed → STALE → opportunity

        best_signal = None
        best_lag_score = 0

        for lookback in [3, 5, 8]:
            price_before = self._get_price_at(asset, lookback)
            odds_before = self._get_odds_at(lookback)

            if price_before is None or odds_before is None:
                continue

            # How much did BTC move in this window?
            price_delta = current_price - price_before
            price_move_dollars = abs(price_delta)

            if price_move_dollars < 8:
                continue  # Not enough movement

            # Direction of the move
            move_dir = "UP" if price_delta > 0 else "DOWN"

            # How much did Polymarket odds move in the same window?
            old_yes, old_no = odds_before
            new_yes, new_no = market.yes_price, market.no_price

            if move_dir == "UP":
                # BTC went up → YES odds should have increased
                odds_delta = new_yes - old_yes
                expected_odds_delta = price_move_dollars / 100.0  # rough: $10 move ≈ 10 cent odds shift
                current_side_price = market.yes_price
                token_id = market.token_id_yes
                side = "YES"
            else:
                # BTC went down → NO odds should have increased
                odds_delta = new_no - old_no
                expected_odds_delta = price_move_dollars / 100.0
                current_side_price = market.no_price
                token_id = market.token_id_no
                side = "NO"

            # THE LAG: how much the odds SHOULD have moved vs how much they DID
            # If BTC moved $30 → expected ~$0.30 odds shift
            # If odds only moved $0.05 → lag of $0.25 → that's our edge
            lag = max(0, expected_odds_delta - odds_delta)

            # Adjust expected delta based on time remaining
            # Less time left → bigger expected odds reaction
            time_mult = max(0.5, min(2.0, (300.0 - seconds_to_expiry) / 150.0))
            lag_adjusted = lag * time_mult

            # Overall direction confidence (from candle open)
            overall_dir = "UP" if diff_from_open > 0 else "DOWN"
            abs_diff = abs(diff_from_open)
            direction_confidence = 0.5 + 0.48 * math.tanh(abs_diff / 25.0) * max(0.3, 1.0 - seconds_to_expiry / 350.0)

            # Log the analysis
            logger.info(
                f"[{lookback}s] BTC ${price_delta:+.0f} (${current_price:.0f}) | "
                f"odds {move_dir}: {odds_delta:+.3f} (expected {expected_odds_delta:+.3f}) | "
                f"LAG={lag:.3f} adj={lag_adjusted:.3f} | "
                f"{side}@{current_side_price:.3f} | "
                f"open_diff=${diff_from_open:+.0f} conf={direction_confidence:.2f} | "
                f"{seconds_to_expiry:.0f}s"
            )

            # FIRE CONDITIONS:
            # 1. Lag is meaningful (odds haven't caught up)
            # 2. Direction is consistent (recent move matches overall position)
            # 3. We can buy the right side at a discount
            if (lag_adjusted >= self.min_edge
                and direction_confidence >= self.min_confidence
                and move_dir == overall_dir  # Don't bet against the trend
                and lag_adjusted > best_lag_score):

                best_lag_score = lag_adjusted
                best_signal = Signal(
                    market_id=market.condition_id,
                    asset=asset,
                    contract_type=market.contract_type,
                    direction=overall_dir,
                    side=side,
                    edge_pct=round(lag_adjusted, 4),
                    confidence=round(direction_confidence, 4),
                    binance_price=current_price,
                    poly_implied_price=current_side_price,
                    poly_yes_price=market.yes_price,
                    poly_no_price=market.no_price,
                    token_id=token_id,
                    seconds_to_expiry=round(seconds_to_expiry, 1),
                    timestamp=now,
                )

        if best_signal:
            self._last_signal_time = now
            logger.info(
                f"🎯 SIGNAL: {asset} {best_signal.direction} → buy {best_signal.side}"
                f"@{best_signal.poly_implied_price:.3f} | "
                f"lag_edge={best_signal.edge_pct:.1%} conf={best_signal.confidence:.1%} | "
                f"{best_signal.seconds_to_expiry:.0f}s left"
            )

        return best_signal
