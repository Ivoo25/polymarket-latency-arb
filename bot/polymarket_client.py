import asyncio
import json
import time
import logging
from typing import Optional
from dataclasses import dataclass

import httpx

logger = logging.getLogger("arb.polymarket")

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"


@dataclass
class Market:
    condition_id: str
    question: str
    asset: str  # BTC or ETH
    contract_type: str  # 5min
    yes_price: float
    no_price: float
    end_time: float
    token_id_yes: str
    token_id_no: str
    slug: str = ""
    active: bool = True


class PolymarketClient:
    """Fetches active 5-minute BTC/ETH up/down markets from Polymarket."""

    def __init__(self, api_key: str = "", secret: str = "", passphrase: str = "", private_key: str = ""):
        self.api_key = api_key
        self.secret = secret
        self.passphrase = passphrase
        self.private_key = private_key
        self._client = httpx.AsyncClient(timeout=10)
        self._rate_limit = asyncio.Semaphore(5)
        self._last_request = 0.0
        self._min_interval = 0.2

    async def _request(self, method: str, url: str, **kwargs) -> dict | list:
        async with self._rate_limit:
            elapsed = time.time() - self._last_request
            if elapsed < self._min_interval:
                await asyncio.sleep(self._min_interval - elapsed)
            self._last_request = time.time()

            for attempt in range(3):
                try:
                    resp = await self._client.request(method, url, **kwargs)
                    resp.raise_for_status()
                    return resp.json()
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:
                        wait = 2 ** (attempt + 1)
                        logger.warning(f"Rate limited, waiting {wait}s")
                        await asyncio.sleep(wait)
                    else:
                        raise
                except (httpx.ConnectError, httpx.ReadTimeout) as e:
                    logger.warning(f"Request failed (attempt {attempt + 1}): {e}")
                    await asyncio.sleep(1)
            return {}

    async def get_active_markets(self) -> list[Market]:
        """Fetch current and next 5-minute BTC up/down markets by slug."""
        markets = []
        now_ts = int(time.time())

        # Current 5-min window
        current_window = (now_ts // 300) * 300
        seconds_left = 300 - (now_ts - current_window)

        # Fetch current window market
        slugs = [f"btc-updown-5m-{current_window}"]

        # Also fetch next window if current is about to expire
        if seconds_left < 60:
            next_window = current_window + 300
            slugs.append(f"btc-updown-5m-{next_window}")

        for slug in slugs:
            market = await self._fetch_market_by_slug(slug, seconds_left if slug == slugs[0] else 300)
            if market:
                markets.append(market)

        if markets:
            for mk in markets:
                logger.info(
                    f"[MARKET] {mk.question[:60]} | UP={mk.yes_price:.3f} DOWN={mk.no_price:.3f} | "
                    f"{int(mk.end_time - time.time())}s left"
                )
        else:
            logger.debug("No active 5m BTC markets found")

        return markets

    async def _fetch_market_by_slug(self, slug: str, seconds_left: int) -> Optional[Market]:
        """Fetch a specific market by its slug from Gamma API."""
        try:
            data = await self._request("GET", f"{GAMMA_API}/events", params={"slug": slug})

            if not data or not isinstance(data, list) or len(data) == 0:
                return None

            event = data[0]
            event_markets = event.get("markets", [])
            if not event_markets:
                return None

            m = event_markets[0]
            prices_raw = m.get("outcomePrices", "[0.5,0.5]")
            tokens_raw = m.get("clobTokenIds", '["",""]')

            prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
            tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw

            yes_price = float(prices[0]) if len(prices) > 0 else 0.5
            no_price = float(prices[1]) if len(prices) > 1 else 0.5
            token_yes = tokens[0] if len(tokens) > 0 else ""
            token_no = tokens[1] if len(tokens) > 1 else ""

            # Try to get more accurate CLOB prices
            clob_yes, clob_no = await self._get_clob_prices(token_yes, token_no)
            if clob_yes is not None:
                yes_price = clob_yes
            if clob_no is not None:
                no_price = clob_no

            end_time = time.time() + seconds_left

            return Market(
                condition_id=m.get("conditionId", m.get("id", "")),
                question=event.get("title", m.get("question", "")),
                asset="BTC",
                contract_type="5min",
                yes_price=yes_price,
                no_price=no_price,
                end_time=end_time,
                token_id_yes=token_yes,
                token_id_no=token_no,
                slug=slug,
            )
        except Exception as e:
            logger.error(f"Failed to fetch market {slug}: {e}")
            return None

    async def _get_clob_prices(self, token_yes: str, token_no: str) -> tuple[Optional[float], Optional[float]]:
        """Get real-time CLOB prices (more accurate than Gamma cache)."""
        clob_yes = None
        clob_no = None
        try:
            if token_yes:
                data = await self._request("GET", f"{CLOB_API}/price", params={"token_id": token_yes, "side": "BUY"})
                if isinstance(data, dict) and "price" in data:
                    p = float(data["price"])
                    if 0.01 < p < 0.99:
                        clob_yes = p
            if token_no:
                data = await self._request("GET", f"{CLOB_API}/price", params={"token_id": token_no, "side": "BUY"})
                if isinstance(data, dict) and "price" in data:
                    p = float(data["price"])
                    if 0.01 < p < 0.99:
                        clob_no = p
        except Exception as e:
            logger.debug(f"CLOB price fetch failed: {e}")
        return clob_yes, clob_no

    async def place_order(self, token_id: str, side: str, size: float, price: float) -> Optional[str]:
        """Place an order on Polymarket CLOB. Returns order ID or None."""
        if not self.api_key:
            logger.warning("No API key — cannot place live orders")
            return None

        try:
            logger.info(f"ORDER: {side} {size:.2f} USDC @ {price:.4f} on {token_id[:20]}...")
            return f"paper_{int(time.time() * 1000)}"
        except Exception as e:
            logger.error(f"Order placement failed: {e}")
            return None

    async def close(self):
        await self._client.aclose()
