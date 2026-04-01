#!/usr/bin/env python3
"""Polymarket Latency Arbitrage Bot — Entry Point."""

import logging
import sys

import uvicorn

from bot.config import Config


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("data/bot.log", mode="a"),
        ],
    )


def main():
    from pathlib import Path
    Path("data").mkdir(exist_ok=True)

    setup_logging()
    config = Config()

    mode = "PAPER" if not config.is_live else "LIVE"
    print(f"\n{'='*50}")
    print(f"  Polymarket Latency Arbitrage Bot")
    print(f"  Mode: {mode}")
    print(f"  API: http://localhost:{config.api_port}")
    print(f"  Dashboard: http://localhost:{config.dashboard_port}")
    print(f"{'='*50}\n")

    if config.is_live:
        print("  WARNING: LIVE TRADING ENABLED")
        print("  All 3 live flags are set to true")
        print()

    uvicorn.run(
        "bot.api:app",
        host="0.0.0.0",
        port=config.api_port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
