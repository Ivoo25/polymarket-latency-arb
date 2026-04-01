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
    TRUE LATENCY ARBITRAGE:

    The edge isn't about estimating probabilities better than Polymarket.
    It's about detecting that Binance JUST moved and Polymarket odds
    haven't updated yet (the ~2.7s lag).

    Method:
    1. Track BTC price on Binance tick-by-tick
    2. Track Polymarket odds tick-by-tick
    3. When BTC makes a sharp move, calculate what the odds SHIFT should be
    4. If Polymarket odds haven't shifted yet → they're stale → buy cheap
    """

    def __init__(self, min_edge: float = 0.03, min_confidence: float = 0.60):
        self.min_edge = min_edge
        self.min_confidence = min_confidence

        # Binance price history
        self._prices: dict[str, deque] = {
            "BTC": deque(maxlen=1000),
            "ETH": deque(maxlen=1000),
        }

        # Polymarket odds history (to detect if they're stale)
        self._odds_history: deque = deque(maxlen=200)

        self._candle_open: dict[str, float] = {}
        self._last_candle_fetch = 0.0
        self._last_signal_time = 0.0
        self._min_signal_gap = 5.0  # Max 1 signal per 5 seconds

    def update_price(self, asset: str, price: float, ts: float):
        self._prices[asset].append((ts, price))

    def _record_odds(self, yes_price: float, no_price: float):
        self._odds_history.append((time.time(), yes_price, no_price))

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

    def _get_price_move(self, asset: str, seconds_ago: float) -> Optional[tuple[float, float, float]]:
        """Get price at `seconds_ago` and now. Returns (price_then, price_now, pct_change)."""
        prices = self._prices.get(asset)
        if not prices or len(prices) < 10:
            return None

        now_ts = prices[-1][0]
        cutoff = now_ts - seconds_ago

        # Find price at cutoff
        price_then = None
        for ts, p in prices:
            if ts >= cutoff:
                price_then = p
                break

        if price_then is None or price_then == 0:
            return None

        price_now = prices[-1][1]
        pct = (price_now - price_then) / price_then
        return price_then, price_now, pct

    def _odds_changed_recently(self, seconds: float = 3.0) -> bool:
        """Check if Polymarket odds changed in the last N seconds."""
        if len(self._odds_history) < 2:
            return False

        now = time.time()
        recent = [(t, y, n) for t, y, n in self._odds_history if t >= now - seconds]

        if len(recent) < 2:
            return False

        # Check if yes_price changed
        first_yes = recent[0][1]
        last_yes = recent[-1][1]
        return abs(last_yes - first_yes) > 0.02  # Odds moved >2 cents

    def detect(self, asset: str, market) -> Optional[Signal]:
        now = time.time()
        seconds_to_expiry = market.end_time - now

        if seconds_to_expiry < 10:
            return None

        if now - self._last_signal_time < self._min_signal_gap:
            return None

        # Record current odds
        self._record_odds(market.yes_price, market.no_price)

        candle_open = self._candle_open.get(asset)
        prices = self._prices.get(asset)
        if not candle_open or not prices or len(prices) < 20:
            return None

        current_price = prices[-1][1]
        diff_from_open = current_price - candle_open

        # === LATENCY ARB DETECTION ===
        # Check multiple short windows for sharp moves
        for window in [3, 5, 8, 12]:
            move = self._get_price_move(asset, window)
            if not move:
                continue

            price_then, price_now, pct_change = move
            abs_dollar_move = abs(price_now - price_then)

            # Need a meaningful move in a short window
            if abs_dollar_move < 10:
                continue

            # Direction of the RECENT move (not the overall candle)
            move_direction = "UP" if pct_change > 0 else "DOWN"

            # What the OVERALL direction is (from candle open)
            overall_direction = "UP" if diff_from_open > 0 else "DOWN"

            # After this move, what should the probability be?
            # Key: combine the overall position + the recent momentum
            abs_diff_pct = abs(diff_from_open) / candle_open

            # Time factor: closer to expiry = more certain
            time_factor = max(0.3, min(0.95, 1.0 - (seconds_to_expiry / 300.0)))

            # A $50 move from open with 60s left = very high prob
            # A $10 move from open with 250s left = low prob
            implied_prob = 0.5 + 0.48 * math.tanh(abs_diff_pct * 500 * time_factor)
            implied_prob = min(0.97, implied_prob)

            # What Polymarket currently charges
            if overall_direction == "UP":
                poly_price = market.yes_price
                token_id = market.token_id_yes
                side = "YES"
            else:
                poly_price = market.no_price
                token_id = market.token_id_no
                side = "NO"

            edge = implied_prob - poly_price

            # Check if odds are STALE (haven't reacted to the move)
            odds_moved = self._odds_changed_recently(3.0)

            # Log everything
            stale_tag = "STALE" if not odds_moved else "live"
            logger.info(
                f"[{stale_tag}] BTC=${current_price:.0f} open=${candle_open:.0f} "
                f"diff=${diff_from_open:+.0f} | {window}s move: ${abs_dollar_move:.0f} ({pct_change:+.3%}) | "
                f"implied={implied_prob:.3f} poly={poly_price:.3f} edge={edge:+.1%} | "
                f"UP={market.yes_price:.3f} DN={market.no_price:.3f} | {seconds_to_expiry:.0f}s"
            )

            # FIRE if: edge exists AND odds haven't caught up
            if edge >= self.min_edge and implied_prob >= self.min_confidence:
                # Bonus: if odds are stale, we're even more confident
                confidence = implied_prob
                if not odds_moved and abs_dollar_move >= 20:
                    confidence = min(0.98, confidence + 0.05)

                self._last_signal_time = now
                signal = Signal(
                    market_id=market.condition_id,
                    asset=asset,
                    contract_type=market.contract_type,
                    direction=overall_direction,
                    side=side,
                    edge_pct=round(edge, 4),
                    confidence=round(confidence, 4),
                    binance_price=current_price,
                    poly_implied_price=poly_price,
                    poly_yes_price=market.yes_price,
                    poly_no_price=market.no_price,
                    token_id=token_id,
                    seconds_to_expiry=round(seconds_to_expiry, 1),
                    timestamp=now,
                )

                logger.info(
                    f"🎯 SIGNAL FIRED: {asset} {overall_direction} {side} | "
                    f"edge={edge:.1%} conf={confidence:.1%} | "
                    f"buy {side}@{poly_price:.3f} (true={implied_prob:.3f}) | "
                    f"{seconds_to_expiry:.0f}s left | odds_stale={not odds_moved}"
                )
                return signal

        return None
