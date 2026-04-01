import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    # Polymarket
    poly_api_key: str = field(default_factory=lambda: os.getenv("POLYMARKET_API_KEY", ""))
    poly_secret: str = field(default_factory=lambda: os.getenv("POLYMARKET_SECRET", ""))
    poly_passphrase: str = field(default_factory=lambda: os.getenv("POLYMARKET_PASSPHRASE", ""))
    poly_private_key: str = field(default_factory=lambda: os.getenv("POLYMARKET_PRIVATE_KEY", ""))

    # Binance
    binance_ws_url: str = field(
        default_factory=lambda: os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:9443")
    )

    # Trading mode
    paper_mode: bool = True
    live_flag_1: bool = False
    live_flag_2: bool = False
    live_flag_3: bool = False

    # Risk
    max_position_pct: float = 0.08
    daily_loss_limit: float = 0.20
    kill_switch_drawdown: float = 0.40
    kelly_fraction: float = 0.5
    min_edge_pct: float = 0.05
    min_confidence: float = 0.85

    # Ports
    api_port: int = 8000
    dashboard_port: int = 8080

    # Telegram
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    def __post_init__(self):
        self.paper_mode = os.getenv("PAPER_MODE", "true").lower() == "true"
        self.live_flag_1 = os.getenv("LIVE_FLAG_1", "false").lower() == "true"
        self.live_flag_2 = os.getenv("LIVE_FLAG_2", "false").lower() == "true"
        self.live_flag_3 = os.getenv("LIVE_FLAG_3", "false").lower() == "true"
        self.max_position_pct = float(os.getenv("MAX_POSITION_PCT", "0.08"))
        self.daily_loss_limit = float(os.getenv("DAILY_LOSS_LIMIT", "0.20"))
        self.kill_switch_drawdown = float(os.getenv("KILL_SWITCH_DRAWDOWN", "0.40"))
        self.kelly_fraction = float(os.getenv("KELLY_FRACTION", "0.5"))
        self.min_edge_pct = float(os.getenv("MIN_EDGE_PCT", "0.05"))
        self.min_confidence = float(os.getenv("MIN_CONFIDENCE", "0.85"))
        self.api_port = int(os.getenv("API_PORT", "8000"))
        self.dashboard_port = int(os.getenv("DASHBOARD_PORT", "8080"))
        self.telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")

    @property
    def is_live(self) -> bool:
        return (
            not self.paper_mode
            and self.live_flag_1
            and self.live_flag_2
            and self.live_flag_3
        )
