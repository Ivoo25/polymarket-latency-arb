import asyncio
import json
import logging
import time
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from .config import Config
from .engine import ArbitrageEngine

logger = logging.getLogger("arb.api")

DASHBOARD_DIR = Path(__file__).parent.parent / "dashboard"

app = FastAPI(title="Polymarket Latency Arb")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

engine: ArbitrageEngine | None = None
_ws_clients: set[WebSocket] = set()


def get_engine() -> ArbitrageEngine:
    global engine
    if engine is None:
        config = Config()
        engine = ArbitrageEngine(config)
    return engine


@app.on_event("startup")
async def startup():
    eng = get_engine()
    asyncio.create_task(eng.start(initial_balance=1000.0))
    asyncio.create_task(_broadcast_loop())


@app.on_event("shutdown")
async def shutdown():
    if engine:
        engine.stop()
        await engine.polymarket.close()


@app.get("/api/state")
async def get_state():
    eng = get_engine()
    return eng.get_dashboard_state()


@app.get("/api/trades")
async def get_trades(limit: int = 50):
    eng = get_engine()
    return eng.db.get_recent_trades(limit)


@app.get("/api/stats")
async def get_stats():
    eng = get_engine()
    return eng.db.get_stats()


@app.get("/api/risk")
async def get_risk():
    eng = get_engine()
    return eng.risk.get_state()


@app.get("/api/snapshots")
async def get_snapshots(limit: int = 288):
    eng = get_engine()
    return eng.db.get_snapshots(limit)


@app.post("/api/kill-switch")
async def trigger_kill_switch():
    eng = get_engine()
    eng.risk.state.kill_switch_active = True
    eng.status = "halted: manual kill switch"
    return {"status": "kill switch activated"}


@app.post("/api/resume")
async def resume_trading():
    eng = get_engine()
    eng.risk.state.kill_switch_active = False
    eng.risk.state.daily_halt_active = False
    eng.status = "scanning"
    return {"status": "resumed"}


@app.get("/")
async def serve_dashboard():
    return FileResponse(DASHBOARD_DIR / "index.html")


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    _ws_clients.add(ws)
    try:
        while True:
            # Keep connection alive, ignore client messages
            await ws.receive_text()
    except WebSocketDisconnect:
        _ws_clients.discard(ws)
    except Exception:
        _ws_clients.discard(ws)


async def _broadcast_loop():
    """Push state to all connected WebSocket clients every second."""
    global _ws_clients
    while True:
        if _ws_clients and engine:
            state = engine.get_dashboard_state()
            payload = json.dumps(state)
            dead = set()
            for ws in _ws_clients:
                try:
                    await ws.send_text(payload)
                except Exception:
                    dead.add(ws)
            _ws_clients -= dead
        await asyncio.sleep(1)
