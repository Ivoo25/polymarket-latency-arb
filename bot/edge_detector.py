import time
import math
import logging
from dataclasses import dataclass
from typing import Optional

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
    """Detects latency arbitrage opportunities between Binance and Polymarket."""

    def __init__(self, min_edge: float = 0.05, min_confidence: float = 0.85):
        self.min_edge = min_edge
        self.min_confidence = min_confidence
        self._price_history: dict[str, list[tuple[float, float]]] = {
            "BTC": [], "ETH": []
        }
        self.MAX_HISTORY = 300  # 5 minutes of second-level data

    def update_price(self, asset: str, price: float, ts: float):
        history = self._price_history.get(asset, [])
        history.append((ts, price))
        # Keep last N entries
        if len(history) > self.MAX_HISTORY:
            history = history[-self.MAX_HISTORY:]
        self._price_history[asset] = history

    def _calculate_momentum(self, asset: str, window_seconds: float = 30.0) -> Optional[float]:
        """Calculate recent price momentum as percentage change over window."""
        history = self._price_history.get(asset, [])
        if len(history) < 10:
            return None

        now = history[-1][0]
        cutoff = now - window_seconds

        window_prices = [(t, p) for t, p in history if t >= cutoff]
        if len(window_prices) < 5:
            return None

        start_price = window_prices[0][1]
        end_price = window_prices[-1][1]

        if start_price == 0:
            return None

        return (end_price - start_price) / start_price

    def _estimate_direction_probability(self, momentum: float, seconds_to_expiry: float) -> tuple[str, float]:
        """
        Estimate probability that price will be higher/lower at expiry
        based on current momentum and time remaining.
        """
        # Strong momentum with little time left = high confidence
        # The closer to expiry, the more predictive current momentum is
        time_factor = max(0.1, min(1.0, 1.0 - (seconds_to_expiry / 900.0)))

        # Absolute momentum strength
        abs_mom = abs(momentum)

        # Base probability from momentum
        if abs_mom < 0.0001:
            return "UP", 0.50

        # Sigmoid-like mapping: stronger momentum = higher probability
        raw_prob = 0.5 + 0.5 * math.tanh(abs_mom * 500 * time_factor)
        raw_prob = min(0.99, max(0.51, raw_prob))

        direction = "UP" if momentum > 0 else "DOWN"
        return direction, raw_prob

    def detect(self, asset: str, market) -> Optional[Signal]:
        """Check if there's an exploitable edge on this market."""
        now = time.time()
        seconds_to_expiry = market.end_time - now

        # Don't trade markets about to expire (< 30s) or too far out (> 14min)
        if seconds_to_expiry < 30 or seconds_to_expiry > 840:
            return None

        momentum = self._calculate_momentum(asset)
        if momentum is None:
            return None

        direction, true_prob = self._estimate_direction_probability(momentum, seconds_to_expiry)

        # What Polymarket currently implies
        if direction == "UP":
            poly_price = market.yes_price  # YES = price goes up
            token_id = market.token_id_yes
            side = "YES"
        else:
            poly_price = market.no_price  # NO = price goes down (or YES on "down" market)
            token_id = market.token_id_no
            side = "NO"

        # Edge = true probability - market price
        edge = true_prob - poly_price

        if edge < self.min_edge:
            return None

        # Confidence combines probability strength and edge size
        confidence = min(0.99, true_prob * (1.0 + edge))

        if confidence < self.min_confidence:
            return None

        current_price = self._price_history[asset][-1][1] if self._price_history[asset] else 0.0

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
            f"SIGNAL: {asset} {direction} | edge={edge:.2%} conf={confidence:.2%} "
            f"| poly={poly_price:.4f} true_prob={true_prob:.4f} | expires in {seconds_to_expiry:.0f}s"
        )

        return signal
