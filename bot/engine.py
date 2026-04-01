import asyncio
import time
import logging
from datetime import datetime, timezone

from .config import Config
from .binance_feed import BinanceFeed
from .polymarket_client import PolymarketClient, Market
from .edge_detector import EdgeDetector, Signal
from .risk_manager import RiskManager
from .database import Database, Trade

logger = logging.getLogger("arb.engine")


class ArbitrageEngine:
    """Main orchestrator: feeds -> edge detection -> risk check -> execution."""

    def __init__(self, config: Config):
        self.config = config
        self.db = Database()
        self.binance = BinanceFeed(config.binance_ws_url)
        self.polymarket = PolymarketClient(
            api_key=config.poly_api_key,
            secret=config.poly_secret,
            passphrase=config.poly_passphrase,
            private_key=config.poly_private_key,
        )
        self.edge_detector = EdgeDetector(
            min_edge=config.min_edge_pct,
            min_confidence=config.min_confidence,
        )
        self.risk = RiskManager(
            max_position_pct=config.max_position_pct,
            daily_loss_limit=config.daily_loss_limit,
            kill_switch_drawdown=config.kill_switch_drawdown,
            kelly_fraction=config.kelly_fraction,
        )

        self._running = False
        self._scan_interval = 10.0  # seconds between market scans (avoid hammering API)
        self._snapshot_interval = 300  # 5 min snapshots
        self._last_snapshot = 0.0
        self._last_daily_reset = 0.0

        # State for dashboard
        self.status = "initializing"
        self.last_signal: dict = {}
        self.trade_count = 0
        self.start_time = 0.0

        # Wire up binance price callback
        self.binance.on_price(self._on_price_update)

    def _on_price_update(self, asset: str, price: float, ts: float):
        self.edge_detector.update_price(asset, price, ts)

    async def start(self, initial_balance: float = 1000.0):
        self.risk.initialize(initial_balance)
        self._running = True
        self.start_time = time.time()
        self.status = "running"

        mode = "PAPER" if not self.config.is_live else "LIVE"
        logger.info(f"Engine starting in {mode} mode with ${initial_balance:.2f}")

        # Run binance feed and trading loop concurrently
        await asyncio.gather(
            self.binance.start(),
            self._trading_loop(),
            self._daily_reset_loop(),
            self._snapshot_loop(),
        )

    async def _trading_loop(self):
        # Wait for binance feed to warm up
        logger.info("Waiting for price feed to warm up...")
        await asyncio.sleep(5)
        self.status = "scanning"

        while self._running:
            try:
                can_trade, reason = self.risk.can_trade()
                if not can_trade:
                    self.status = f"halted: {reason}"
                    await asyncio.sleep(10)
                    continue

                self.status = "scanning"

                # Fetch candle open price for edge detection
                await self.edge_detector.fetch_candle_open("BTC")

                markets = await self.polymarket.get_active_markets()

                for market in markets:
                    if not self._running:
                        break

                    signal = self.edge_detector.detect(market.asset, market)
                    if signal is None:
                        continue

                    await self._execute_signal(signal, market)

                # Scan every 2 seconds — latency arb needs speed
                await asyncio.sleep(2.0)

            except Exception as e:
                logger.error(f"Trading loop error: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def _execute_signal(self, signal: Signal, market: Market):
        can_trade, reason = self.risk.can_trade()
        if not can_trade:
            return

        position_size = self.risk.calculate_position_size(signal.edge_pct, signal.confidence)
        if position_size < 1.0:
            return

        is_paper = not self.config.is_live

        # Record trade
        trade = Trade(
            timestamp=time.time(),
            market_id=signal.market_id,
            asset=signal.asset,
            contract_type=signal.contract_type,
            side=signal.side,
            direction=signal.direction,
            entry_price=signal.poly_implied_price,
            size_usdc=position_size,
            edge_pct=signal.edge_pct,
            confidence=signal.confidence,
            binance_price=signal.binance_price,
            poly_implied_price=signal.poly_implied_price,
            paper=is_paper,
        )

        if is_paper:
            # Paper mode: simulate execution
            trade_id = self.db.insert_trade(trade)
            self.trade_count += 1
            self.last_signal = {
                "asset": signal.asset,
                "direction": signal.direction,
                "edge": signal.edge_pct,
                "confidence": signal.confidence,
                "size": position_size,
                "time": datetime.now(timezone.utc).isoformat(),
            }

            # Simulate resolution after a delay
            asyncio.create_task(self._simulate_resolution(trade_id, signal))
            logger.info(
                f"PAPER TRADE #{trade_id}: {signal.asset} {signal.direction} "
                f"${position_size:.2f} @ edge={signal.edge_pct:.2%}"
            )
        else:
            # Live mode
            order_id = await self.polymarket.place_order(
                token_id=signal.token_id,
                side="BUY",
                size=position_size,
                price=signal.poly_implied_price,
            )
            if order_id:
                trade_id = self.db.insert_trade(trade)
                self.trade_count += 1
                logger.info(f"LIVE TRADE #{trade_id}: order={order_id}")

    async def _simulate_resolution(self, trade_id: int, signal: Signal):
        """Simulate trade resolution for paper mode."""
        # Wait until near contract expiry
        wait_time = max(10, min(signal.seconds_to_expiry - 5, 300))
        await asyncio.sleep(wait_time)

        # In paper mode, simulate based on the edge/confidence
        # Higher confidence = higher win probability (reflects real edge)
        import random
        won = random.random() < signal.confidence

        if won:
            pnl = signal.edge_pct * (signal.poly_implied_price * 100)  # approximate
        else:
            pnl = -signal.poly_implied_price * 10  # loss on the position

        exit_price = 1.0 if won else 0.0
        self.db.resolve_trade(trade_id, exit_price, pnl, won)
        self.risk.update_after_trade(pnl)

    async def _daily_reset_loop(self):
        while self._running:
            now = datetime.now(timezone.utc)
            # Reset at midnight UTC
            if now.hour == 0 and now.minute == 0:
                self.risk.reset_daily()
            await asyncio.sleep(60)

    async def _snapshot_loop(self):
        while self._running:
            await asyncio.sleep(self._snapshot_interval)
            stats = self.db.get_stats()
            self.db.save_snapshot(self.risk.state.current_balance, stats)

    def get_dashboard_state(self) -> dict:
        stats = self.db.get_stats()
        risk_state = self.risk.get_state()
        recent = self.db.get_recent_trades(10)
        uptime = time.time() - self.start_time if self.start_time else 0

        return {
            "status": self.status,
            "mode": "PAPER" if not self.config.is_live else "LIVE",
            "uptime_seconds": round(uptime),
            "balance": risk_state["balance"],
            "daily_pnl": risk_state["daily_pnl"],
            "total_pnl": stats["total_pnl"],
            "win_rate": stats["win_rate"],
            "total_trades": stats["total_trades"],
            "open_positions": stats["open_positions"],
            "kill_switch": risk_state["kill_switch"],
            "daily_halt": risk_state["daily_halt"],
            "total_drawdown_pct": risk_state["total_drawdown_pct"],
            "daily_drawdown_pct": risk_state["daily_drawdown_pct"],
            "last_signal": self.last_signal,
            "recent_trades": recent,
            "prices": {
                "BTC": self.binance.prices.get("BTC", 0),
                "ETH": self.binance.prices.get("ETH", 0),
            },
        }

    def stop(self):
        self._running = False
        self.binance.stop()
        self.status = "stopped"
        logger.info("Engine stopped")
