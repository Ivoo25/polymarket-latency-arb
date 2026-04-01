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
        self._snapshot_interval = 300
        self._last_snapshot = 0.0

        # State for dashboard
        self.status = "initializing"
        self.last_signal: dict = {}
        self.trade_count = 0
        self.start_time = 0.0
        self._traded_windows: set[int] = set()  # Track which 5min windows we already traded

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

        # Resolve any stale OPEN trades from previous runs
        self._resolve_stale_trades()

        await asyncio.gather(
            self.binance.start(),
            self._trading_loop(),
            self._resolution_loop(),
            self._daily_reset_loop(),
            self._snapshot_loop(),
        )

    def _resolve_stale_trades(self):
        """On startup, resolve any OPEN trades from previous runs as losses."""
        stale = self.db.get_open_trades()
        if stale:
            logger.info(f"Resolving {len(stale)} stale OPEN trades from previous run")
            for t in stale:
                # Trades from previous runs — we don't know the result, mark as loss
                self.db.resolve_trade(t["id"], exit_price=0.0, pnl=-t["entry_price"] * 5, won=False)
                self.risk.update_after_trade(-t["entry_price"] * 5)
            logger.info(f"Resolved {len(stale)} stale trades as losses")

    async def _trading_loop(self):
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
                await self.edge_detector.fetch_candle_open("BTC")
                markets = await self.polymarket.get_active_markets()

                for market in markets:
                    if not self._running:
                        break
                    signal = self.edge_detector.detect(market.asset, market)
                    if signal is None:
                        continue
                    await self._execute_signal(signal, market)

                await asyncio.sleep(2.0)

            except Exception as e:
                logger.error(f"Trading loop error: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def _execute_signal(self, signal: Signal, market: Market):
        # Only 1 trade per 5-minute window
        now_ts = int(time.time())
        window_id = (now_ts // 300) * 300
        if window_id in self._traded_windows:
            return

        can_trade, reason = self.risk.can_trade()
        if not can_trade:
            return

        position_size = self.risk.calculate_position_size(signal.edge_pct, signal.confidence)
        if position_size < 1.0:
            return

        is_paper = not self.config.is_live

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
            trade_id = self.db.insert_trade(trade)
            self.trade_count += 1
            self._traded_windows.add(window_id)
            self.last_signal = {
                "asset": signal.asset,
                "direction": signal.direction,
                "edge": signal.edge_pct,
                "confidence": signal.confidence,
                "size": position_size,
                "time": datetime.now(timezone.utc).isoformat(),
            }
            logger.info(
                f"PAPER TRADE #{trade_id}: {signal.asset} {signal.direction} "
                f"${position_size:.2f} @ edge={signal.edge_pct:.2%} (window {window_id})"
            )
        else:
            order_id = await self.polymarket.place_order(
                token_id=signal.token_id,
                side="BUY",
                size=position_size,
                price=signal.poly_implied_price,
            )
            if order_id:
                trade_id = self.db.insert_trade(trade)
                self.trade_count += 1
                self._traded_windows.add(window_id)
                logger.info(f"LIVE TRADE #{trade_id}: order={order_id} (window {window_id})")

    async def _resolution_loop(self):
        """
        Every 30 seconds, check for OPEN trades whose 5-min window has expired.
        Resolve them by checking what BTC actually did (candle close vs open).
        """
        while self._running:
            await asyncio.sleep(30)
            try:
                open_trades = self.db.get_open_trades()
                if not open_trades:
                    continue

                now = time.time()
                btc_price = self.binance.prices.get("BTC", 0)

                for t in open_trades:
                    trade_age = now - t["timestamp"]

                    # Wait at least 5 minutes (300s) + 30s buffer for the contract to settle
                    if trade_age < 330:
                        continue

                    # Determine if trade won: compare current BTC vs the price when trade was placed
                    # The trade bet on a direction — if BTC went that way, it won
                    entry_btc = t["binance_price"]

                    if entry_btc == 0 or btc_price == 0:
                        continue

                    direction = t["direction"]
                    if direction == "UP":
                        won = btc_price > entry_btc
                    else:
                        won = btc_price < entry_btc

                    entry_price = t["entry_price"]
                    size = t["size_usdc"]

                    if won:
                        # Bought at entry_price, settled at $1.00
                        # Profit = (1.0 - entry_price) * shares
                        # shares ≈ size / entry_price
                        shares = size / entry_price if entry_price > 0 else 0
                        pnl = round((1.0 - entry_price) * shares, 2)
                    else:
                        # Settled at $0.00, lost the entire position
                        pnl = round(-size, 2)

                    self.db.resolve_trade(t["id"], exit_price=1.0 if won else 0.0, pnl=pnl, won=won)
                    self.risk.update_after_trade(pnl)

                    result = "WON" if won else "LOST"
                    logger.info(
                        f"RESOLVED #{t['id']}: {t['asset']} {direction} → {result} | "
                        f"entry_btc=${entry_btc:.0f} now=${btc_price:.0f} | "
                        f"pnl=${pnl:+.2f}"
                    )

            except Exception as e:
                logger.error(f"Resolution loop error: {e}", exc_info=True)

    async def _daily_reset_loop(self):
        while self._running:
            now = datetime.now(timezone.utc)
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
        recent = self.db.get_recent_trades(20)
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
