import asyncio
import time
import logging
from typing import Optional
from dataclasses import dataclass

import httpx

logger = logging.getLogger("arb.polymarket")

POLYMARKET_API = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"


@dataclass
class Market:
    condition_id: str
    question: str
    asset: str  # BTC or ETH
    contract_type: str  # 5min or 15min
    yes_price: float
    no_price: float
    end_time: float
    token_id_yes: str
    token_id_no: str
    active: bool = True


class PolymarketClient:
    """Fetches active short-duration crypto markets and places trades."""

    CRYPTO_KEYWORDS = ["Bitcoin", "BTC", "Ethereum", "ETH"]
    DURATION_KEYWORDS = ["5 minute", "5-minute", "15 minute", "15-minute", "5min", "15min"]

    def __init__(self, api_key: str = "", secret: str = "", passphrase: str = "", private_key: str = ""):
        self.api_key = api_key
        self.secret = secret
        self.passphrase = passphrase
        self.private_key = private_key
        self._client = httpx.AsyncClient(timeout=10)
        self._rate_limit = asyncio.Semaphore(5)  # max 5 concurrent requests
        self._last_request = 0.0
        self._min_interval = 0.2  # 200ms between requests

    async def _request(self, method: str, url: str, **kwargs) -> dict:
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
        """Fetch active short-duration BTC/ETH up/down markets."""
        markets = []
        try:
            data = await self._request("GET", f"{GAMMA_API}/markets", params={
                "active": "true",
                "closed": "false",
                "limit": 100,
            })
            if not isinstance(data, list):
                data = data.get("data", []) if isinstance(data, dict) else []

            now = time.time()
            for m in data:
                question = m.get("question", "")
                is_crypto = any(kw.lower() in question.lower() for kw in self.CRYPTO_KEYWORDS)
                is_short = any(kw.lower() in question.lower() for kw in self.DURATION_KEYWORDS)

                if not (is_crypto and is_short):
                    continue

                asset = "BTC" if any(k in question for k in ["Bitcoin", "BTC"]) else "ETH"
                ctype = "5min" if "5" in question.split("minute")[0].split("min")[0][-2:] else "15min"

                tokens = m.get("tokens", [])
                if len(tokens) < 2:
                    continue

                yes_token = tokens[0] if tokens[0].get("outcome") == "Yes" else tokens[1]
                no_token = tokens[1] if tokens[0].get("outcome") == "Yes" else tokens[0]

                end_ts = m.get("end_date_iso")
                if end_ts:
                    from datetime import datetime
                    try:
                        end_time = datetime.fromisoformat(end_ts.replace("Z", "+00:00")).timestamp()
                    except (ValueError, TypeError):
                        end_time = now + 900
                else:
                    end_time = now + 900

                markets.append(Market(
                    condition_id=m.get("condition_id", ""),
                    question=question,
                    asset=asset,
                    contract_type=ctype,
                    yes_price=float(yes_token.get("price", 0.5)),
                    no_price=float(no_token.get("price", 0.5)),
                    end_time=end_time,
                    token_id_yes=yes_token.get("token_id", ""),
                    token_id_no=no_token.get("token_id", ""),
                ))
        except Exception as e:
            logger.error(f"Failed to fetch markets: {e}")

        return markets

    async def place_order(self, token_id: str, side: str, size: float, price: float) -> Optional[str]:
        """Place an order on Polymarket CLOB. Returns order ID or None."""
        if not self.api_key:
            logger.warning("No API key configured — cannot place live orders")
            return None

        try:
            # Using py-clob-client would go here for live trading
            # For now, log the intent
            logger.info(f"ORDER: {side} {size:.2f} USDC @ {price:.4f} on {token_id}")
            return f"paper_{int(time.time() * 1000)}"
        except Exception as e:
            logger.error(f"Order placement failed: {e}")
            return None

    async def close(self):
        await self._client.aclose()
