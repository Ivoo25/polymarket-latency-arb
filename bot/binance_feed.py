import asyncio
import json
import time
import logging
from typing import Callable, Optional

import websockets

logger = logging.getLogger("arb.binance")


class BinanceFeed:
    """Real-time BTC/ETH price feed from Binance WebSocket."""

    STREAMS = "btcusdt@trade/ethusdt@trade"

    def __init__(self, ws_url: str = "wss://stream.binance.com:9443"):
        self.ws_url = f"{ws_url}/stream?streams={self.STREAMS}"
        self.prices: dict[str, float] = {"BTC": 0.0, "ETH": 0.0}
        self.last_update: dict[str, float] = {"BTC": 0.0, "ETH": 0.0}
        self._callbacks: list[Callable] = []
        self._running = False
        self._ws = None

    def on_price(self, callback: Callable):
        self._callbacks.append(callback)

    async def start(self):
        self._running = True
        retry_delay = 1
        while self._running:
            try:
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    self._ws = ws
                    retry_delay = 1
                    logger.info("Binance WebSocket connected")
                    async for msg in ws:
                        if not self._running:
                            break
                        self._process_message(msg)
            except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
                logger.warning(f"Binance WS disconnected: {e}. Retrying in {retry_delay}s")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)
            except Exception as e:
                logger.error(f"Binance WS error: {e}")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)

    def _process_message(self, raw: str):
        try:
            data = json.loads(raw)
            payload = data.get("data", {})
            symbol = payload.get("s", "")
            price = float(payload.get("p", 0))
            ts = payload.get("T", 0) / 1000.0

            if symbol == "BTCUSDT":
                asset = "BTC"
            elif symbol == "ETHUSDT":
                asset = "ETH"
            else:
                return

            self.prices[asset] = price
            self.last_update[asset] = ts

            for cb in self._callbacks:
                try:
                    cb(asset, price, ts)
                except Exception as e:
                    logger.error(f"Price callback error: {e}")
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.debug(f"Failed to parse Binance message: {e}")

    def stop(self):
        self._running = False

    def get_price(self, asset: str) -> tuple[float, float]:
        return self.prices.get(asset, 0.0), self.last_update.get(asset, 0.0)
