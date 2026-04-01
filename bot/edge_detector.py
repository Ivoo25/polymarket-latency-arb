import time
import math
import logging
from dataclasses import dataclass
from typing import Optional

import httpx

logger = logging.getLogger("arb.edge")


@dataclass
class Signal:
    market_id: str
    asset: str
    contract_type: str
    direction: str  # UP or DOWN
    side: str  # YES or NO — what to buy on Polymarket
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
    Detects latency arbitrage: compares Binance real-time BTC price
    against the 5-min candle open to determine direction, then checks
    if Polymarket odds are lagging behind reality.
    """

    def __init__(self, min_edge: float = 0.05, min_confidence: float = 0.85):
        self.min_edge = min_edge
        self.min_confidence = min_confidence
        self._price_history: dict[str, list[tuple[float, float]]] = {
            "BTC": [], "ETH": []
        }
        self.MAX_HISTORY = 600
        self._candle_open: dict[str, float] = {}  # BTC/ETH candle open prices
        self._last_candle_fetch = 0.0

    def update_price(self, asset: str, price: float, ts: float):
        history = self._price_history.get(asset, [])
        history.append((ts, price))
        if len(history) > self.MAX_HISTORY:
            history = history[-self.MAX_HISTORY:]
        self._price_history[asset] = history

    async def fetch_candle_open(self, asset: str = "BTC"):
        """Fetch the opening price of the current 5-min candle from Binance."""
        now = time.time()
        # Only fetch every 5 seconds max
        if now - self._last_candle_fetch < 5:
            return

        try:
            async with httpx.AsyncClient(timeout=5) as client:
                symbol = f"{asset}USDT"
                resp = await client.get(
                    f"https://api.binance.com/api/v3/klines",
                    params={"symbol": symbol, "interval": "5m", "limit": 1}
                )
                data = resp.json()
                if data and len(data) > 0:
                    candle_open = float(data[0][1])
                    self._candle_open[asset] = candle_open
                    self._last_candle_fetch = now
        except Exception as e:
            logger.debug(f"Candle fetch error: {e}")

    def _get_current_price(self, asset: str) -> Optional[float]:
        history = self._price_history.get(asset, [])
        if not history:
            return None
        return history[-1][1]

    def _calculate_momentum(self, asset: str, window_seconds: float = 15.0) -> Optional[float]:
        """Short-window momentum for confidence scoring."""
        history = self._price_history.get(asset, [])
        if len(history) < 5:
            return None
        now = history[-1][0]
        cutoff = now - window_seconds
        window = [(t, p) for t, p in history if t >= cutoff]
        if len(window) < 3:
            return None
        return (window[-1][1] - window[0][1]) / window[0][1]

    def detect(self, asset: str, market) -> Optional[Signal]:
        """
        Core logic: compare current BTC price vs candle open.
        If BTC has moved significantly and Polymarket odds haven't caught up,
        that's our edge.
        """
        now = time.time()
        seconds_to_expiry = market.end_time - now

        # Only trade when <= 120s left (sniper zone) and > 15s (need time to settle)
        if seconds_to_expiry > 120 or seconds_to_expiry < 15:
            return None

        current_price = self._get_current_price(asset)
        candle_open = self._candle_open.get(asset)

        if not current_price or not candle_open or candle_open == 0:
            return None

        # Price difference from candle open
        diff_pct = (current_price - candle_open) / candle_open
        diff_abs = abs(current_price - candle_open)

        # Need meaningful price movement (at least $20 for BTC)
        if diff_abs < 20:
            return None

        # Determine direction based on price reality
        if current_price > candle_open:
            direction = "UP"
            # True probability that BTC will be UP at close (it already IS up)
            # Closer to expiry + bigger move = higher certainty
            time_factor = max(0.3, 1.0 - (seconds_to_expiry / 300.0))
            true_prob = 0.5 + 0.45 * math.tanh(abs(diff_pct) * 300) * time_factor
            poly_price = market.yes_price
            token_id = market.token_id_yes
            side = "YES"
        else:
            direction = "DOWN"
            time_factor = max(0.3, 1.0 - (seconds_to_expiry / 300.0))
            true_prob = 0.5 + 0.45 * math.tanh(abs(diff_pct) * 300) * time_factor
            poly_price = market.no_price
            token_id = market.token_id_no
            side = "NO"

        true_prob = min(0.98, true_prob)

        # Edge = our estimated probability - what Polymarket charges
        edge = true_prob - poly_price

        if edge < self.min_edge:
            return None

        # Momentum confirmation (is price still moving in our direction?)
        momentum = self._calculate_momentum(asset, window_seconds=10)
        if momentum is not None:
            if direction == "UP" and momentum < -0.0001:
                logger.debug(f"Momentum reversal: betting UP but price dropping")
                return None
            if direction == "DOWN" and momentum > 0.0001:
                logger.debug(f"Momentum reversal: betting DOWN but price rising")
                return None

        # Confidence = combination of edge size, time proximity, and price movement
        confidence = min(0.98, true_prob * (1.0 + edge * 0.5))

        if confidence < self.min_confidence:
            return None

        signal = Signal(
            market_id=market.condition_id,
            asset=asset,
            contract_type=market.contract_type,
            direction=direction,
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
            f"SIGNAL: {asset} {direction} | BTC=${current_price:.0f} open=${candle_open:.0f} "
            f"diff=${diff_abs:.0f} ({diff_pct:+.3%}) | "
            f"edge={edge:.2%} conf={confidence:.2%} poly={poly_price:.3f} true={true_prob:.3f} | "
            f"{seconds_to_expiry:.0f}s left"
        )

        return signal
