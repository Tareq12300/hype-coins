import os
import time
import json
import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

import ccxt.async_support as ccxt
import pandas as pd
from ta.momentum import StochRSIIndicator
from ta.trend import MACD
from dotenv import load_dotenv
from telegram import Bot

load_dotenv()

BOT_VERSION = "2026-07-12-multi-1m-5m-10m-15m-30m-45m-4h-score-v4"


def env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip().strip('"').strip("'")


def env_int(name: str, default: int) -> int:
    try:
        return int(float(env_str(name, str(default))))
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(env_str(name, str(default)))
    except Exception:
        return default


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().strip('"').strip("'").lower() in {"1", "true", "yes", "y", "on"}


def env_list(name: str, default: str) -> List[str]:
    return [x.strip() for x in env_str(name, default).split(",") if x.strip()]


TELEGRAM_BOT_TOKEN = env_str("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = env_str("TELEGRAM_CHAT_ID")

TIMEFRAMES = [x.lower() for x in env_list("TIMEFRAMES", "1m,5m,10m,15m,30m,45m")]
# Kept only for backwards compatibility. Each target timeframe now uses the safest source timeframe.
BASE_FETCH_TIMEFRAME = env_str("BASE_FETCH_TIMEFRAME", "1m").lower()
CHECK_INTERVAL = env_int("CHECK_INTERVAL", 60)
CANDLE_LIMIT = env_int("CANDLE_LIMIT", 300)
MAX_FETCH_CANDLES = env_int("MAX_FETCH_CANDLES", 1000)

ENABLE_TIMEFRAME_SCORE = env_bool("ENABLE_TIMEFRAME_SCORE", True)
MIN_SIGNAL_SCORE = env_int("MIN_SIGNAL_SCORE", 70)
FOUR_H_CONFIRMATION_SCORE = env_int("FOUR_H_CONFIRMATION_SCORE", 50)

def parse_timeframe_weights(raw: str) -> Dict[str, int]:
    weights: Dict[str, int] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item or ":" not in item:
            continue
        timeframe, value = item.split(":", 1)
        try:
            weights[timeframe.strip().lower()] = int(float(value.strip()))
        except Exception:
            continue
    return weights

TIMEFRAME_WEIGHTS = parse_timeframe_weights(
    env_str("TIMEFRAME_WEIGHTS", "1m:5,5m:10,10m:15,15m:20,30m:25,45m:30")
)

ENABLE_4H_CONFIRMATION = env_bool("ENABLE_4H_CONFIRMATION", True)
CONFIRMATION_TIMEFRAME = env_str("CONFIRMATION_TIMEFRAME", "4h").lower()
CONFIRMATION_CANDLE_LIMIT = env_int("CONFIRMATION_CANDLE_LIMIT", 300)
USE_CLOSED_4H_CANDLE = env_bool("USE_CLOSED_4H_CANDLE", True)
REQUIRE_4H_STOCH_BULLISH = env_bool("REQUIRE_4H_STOCH_BULLISH", True)
REQUIRE_4H_MACD_BULLISH = env_bool("REQUIRE_4H_MACD_BULLISH", True)
REQUIRE_4H_MACD_ABOVE_ZERO = env_bool("REQUIRE_4H_MACD_ABOVE_ZERO", True)

EXCHANGES = [x.lower() for x in env_list("EXCHANGES", "okx,mexc,gateio,kucoin")]
QUOTE_ASSETS = [x.upper() for x in env_list("QUOTE_ASSETS", "USDT")]
EXCLUDE_SYMBOLS = set(
    x.upper()
    for x in env_list(
        "EXCLUDE_SYMBOLS",
        "BTC/USDT,ETH/USDT,BNB/USDT,SOL/USDT",
    )
)

# لا يتم تحليل هذه العملات أو إرسال أي تنبيه لها.
STABLECOIN_BASES = {
    "USDT", "USDC", "FDUSD", "TUSD", "DAI", "USDE", "USDS",
    "USD0", "RLUSD", "PYUSD", "EURC", "GUSD", "LUSD", "FRAX",
    "BUSD", "CRVUSD", "USTC", "USDD", "SUSD", "USDJ", "CUSD",
    "DOLA", "MIM", "EURS", "EURT", "XAUT", "PAXG",
}

MEMECOIN_BASES = {
    "DOGE", "SHIB", "PEPE", "FLOKI", "BONK", "WIF", "BOME",
    "BRETT", "MOG", "POPCAT", "MEW", "TURBO", "NEIRO",
    "BABYDOGE", "MEME", "PONKE", "MYRO", "SLERF", "WOJAK",
    "LADYS", "ELON", "SAMO", "DEGEN", "COQ", "CHEEMS", "PENGU",
    "TRUMP", "MELANIA", "FARTCOIN", "PNUT", "ACT", "GOAT",
}

LEVERAGED_SUFFIXES = (
    "3L", "3S", "5L", "5S", "BULL", "BEAR", "UP", "DOWN",
)


TOKENIZED_STOCK_KEYWORDS = (
    "tokenized stock",
    "tokenised stock",
    "stock token",
    "tokenized equity",
    "tokenised equity",
    "equity token",
    "xstocks",
    "xstock",
    "real world stock",
    "stock-backed",
    "stock backed",
)

STOCK_TICKERS = {
    "AAPL", "ABBV", "ABT", "ACN", "ADBE", "AMD", "AMAT", "AMGN",
    "AMZN", "ARM", "ASML", "AVGO", "AXP", "BA", "BABA", "BAC",
    "BRK", "CAT", "COIN", "COST", "CRM", "CRWD", "CSCO", "CVX",
    "DIS", "DOW", "GE", "GM", "GOOG", "GOOGL", "GS", "HD",
    "HON", "IBM", "INTC", "JNJ", "JPM", "KO", "LIN", "LLY",
    "LMT", "LOW", "MA", "MCD", "META", "MMM", "MRK", "MSTR",
    "MSFT", "NFLX", "NKE", "NOW", "NVDA", "NVO", "ORCL", "PEP",
    "PFE", "PG", "PLTR", "PYPL", "QCOM", "RDDT", "ROKU", "SBUX",
    "SHOP", "SLB", "SNOW", "SOFI", "SPOT", "T", "TEAM", "TSLA",
    "TSM", "UBER", "UNH", "V", "VZ", "WMT", "XOM", "ZM",
}

TOKENIZED_STOCK_BASES = {
    "RSLB", "RAAPL", "RTSLA", "RNVDA", "RAMZN", "RMSFT", "RMETA",
    "RGOOG", "RGOOGL", "RCOIN", "RMSTR", "RNFLX", "RPLTR", "RAMD",
    "AAPLX", "TSLAX", "NVDAX", "AMZNX", "MSFTX", "METAX",
    "GOOGLX", "GOOGX", "COINX", "MSTRX", "NFLXX", "PLTRX",
}

MAX_SIGNALS_PER_SCAN = env_int("MAX_SIGNALS_PER_SCAN", 100)
SIGNAL_COOLDOWN_HOURS = env_float("SIGNAL_COOLDOWN_HOURS", 6)
MAX_SYMBOLS_PER_EXCHANGE = env_int("MAX_SYMBOLS_PER_EXCHANGE", 0)
MAX_CONCURRENT_REQUESTS = env_int("MAX_CONCURRENT_REQUESTS", 12)
REQUEST_TIMEOUT = env_int("REQUEST_TIMEOUT", 30)

MIN_CANDLE_VOLUME_USDT = env_float("MIN_CANDLE_VOLUME_USDT", 100000)
MIN_VOLUME_INCREASE = env_float("MIN_VOLUME_INCREASE", 2.0)

ENABLE_TARGETS = env_bool("ENABLE_TARGETS", True)
TP1 = env_float("TP1", 10)
TP2 = env_float("TP2", 20)
TP3 = env_float("TP3", 30)
TP4 = env_float("TP4", 40)
TP5 = env_float("TP5", 50)
STOP_LOSS = env_float("STOP_LOSS", 5)


STOCH_RSI_PERIOD = env_int("STOCH_RSI_PERIOD", 14)
STOCH_K = env_int("STOCH_K", 3)
STOCH_D = env_int("STOCH_D", 3)
STOCH_MAX = env_float("STOCH_MAX", 80)
REQUIRE_STOCH_CROSS = env_bool("REQUIRE_STOCH_CROSS", True)

MACD_FAST = env_int("MACD_FAST", 12)
MACD_SLOW = env_int("MACD_SLOW", 26)
MACD_SIGNAL = env_int("MACD_SIGNAL", 9)
REQUIRE_MACD_POSITIVE = env_bool("REQUIRE_MACD_POSITIVE", True)
REQUIRE_MACD_HISTOGRAM_UP = env_bool("REQUIRE_MACD_HISTOGRAM_UP", True)

# false = use latest candle, closer to what you see live on TradingView.
# true = use previous candle, safer because it is closed.
USE_CLOSED_CANDLE = env_bool("USE_CLOSED_CANDLE", False)

ENABLE_TRADINGVIEW = env_bool("ENABLE_TRADINGVIEW", True)
TRADINGVIEW_BASE_URL = env_str("TRADINGVIEW_BASE_URL", "https://www.tradingview.com/chart/")
DISABLE_WEB_PAGE_PREVIEW = env_bool("DISABLE_WEB_PAGE_PREVIEW", True)

STATE_FILE = env_str("STATE_FILE", "data/state.json")

logging.basicConfig(
    level=getattr(logging, env_str("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("ta-stoch-macd-multi-timeframe-bot")


class JsonState:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data: Dict[str, Any] = {}
        self.load()

    def load(self):
        try:
            if self.path.exists():
                self.data = json.loads(self.path.read_text())
            else:
                self.data = {}
        except Exception:
            self.data = {}

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, ensure_ascii=False, indent=2))


def timeframe_to_minutes(timeframe: str) -> int:
    timeframe = timeframe.lower().strip()
    if timeframe.endswith("m"):
        return int(timeframe[:-1])
    raise ValueError(f"Only minute timeframes are supported: {timeframe}")


def aggregate_candles(
    candles: List[List[float]],
    source_timeframe: str,
    target_timeframe: str,
) -> List[List[float]]:
    """Build target-minute OHLCV candles from a compatible source timeframe."""
    base_minutes = timeframe_to_minutes(source_timeframe)
    target_minutes = timeframe_to_minutes(target_timeframe)

    if target_minutes == base_minutes:
        return candles
    if target_minutes < base_minutes or target_minutes % base_minutes != 0:
        raise ValueError(
            f"{target_timeframe} must be a multiple of {source_timeframe}"
        )
    if not candles:
        return []

    bucket_ms = target_minutes * 60_000
    grouped: List[List[float]] = []
    current_bucket = None
    bucket_rows: List[List[float]] = []

    def flush(rows: List[List[float]], bucket_start: int):
        if not rows:
            return
        grouped.append([
            bucket_start,
            float(rows[0][1]),
            max(float(r[2]) for r in rows),
            min(float(r[3]) for r in rows),
            float(rows[-1][4]),
            sum(float(r[5]) for r in rows),
        ])

    for row in candles:
        bucket_start = int(row[0] // bucket_ms * bucket_ms)
        if current_bucket is None:
            current_bucket = bucket_start
        if bucket_start != current_bucket:
            flush(bucket_rows, current_bucket)
            bucket_rows = []
            current_bucket = bucket_start
        bucket_rows.append(row)

    flush(bucket_rows, current_bucket)
    return grouped


def source_timeframe_for(target_timeframe: str) -> str:
    """Use direct exchange candles where possible; synthesize 10m and 45m safely."""
    mapping = {
        "1m": "1m",
        "5m": "5m",
        "10m": "5m",
        "15m": "15m",
        "30m": "30m",
        "45m": "15m",
    }
    return mapping.get(target_timeframe.lower(), target_timeframe.lower())


def fetch_limit_for(source_timeframe: str, target_timeframe: str) -> int:
    source_minutes = timeframe_to_minutes(source_timeframe)
    target_minutes = timeframe_to_minutes(target_timeframe)
    factor = max(1, target_minutes // source_minutes)
    requested = CANDLE_LIMIT * factor + factor * 2
    return min(max(requested, 100), MAX_FETCH_CANDLES)


def analysis_index(candles: List[List[float]]) -> int:
    if USE_CLOSED_CANDLE and len(candles) >= 2:
        return len(candles) - 2
    return len(candles) - 1


def is_bullish_signal(candles: List[List[float]]) -> Optional[Dict[str, float]]:
    minimum_candles = max(MACD_SLOW + MACD_SIGNAL + 20, 80)
    if len(candles) < minimum_candles:
        return None

    df = pd.DataFrame(candles, columns=["time", "open", "high", "low", "close", "volume"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    idx = analysis_index(candles)
    if idx < 60:
        return None

    # ta library returns StochRSI K/D in 0..1, so multiply by 100 to match TradingView display.
    stoch = StochRSIIndicator(
        close=df["close"],
        window=STOCH_RSI_PERIOD,
        smooth1=STOCH_K,
        smooth2=STOCH_D,
        fillna=False,
    )
    df["stoch_k"] = stoch.stochrsi_k() * 100.0
    df["stoch_d"] = stoch.stochrsi_d() * 100.0

    macd_ind = MACD(
        close=df["close"],
        window_fast=MACD_FAST,
        window_slow=MACD_SLOW,
        window_sign=MACD_SIGNAL,
        fillna=False,
    )
    df["macd"] = macd_ind.macd()
    df["macd_signal"] = macd_ind.macd_signal()
    df["hist"] = macd_ind.macd_diff()

    row = df.iloc[idx]
    prev = df.iloc[idx - 1]

    current_volume_usdt = float(row["volume"] * row["close"])
    previous_volume_usdt = float(prev["volume"] * prev["close"])

    if current_volume_usdt < MIN_CANDLE_VOLUME_USDT:
        return None

    volume_ratio = current_volume_usdt / previous_volume_usdt if previous_volume_usdt > 0 else 0.0

    # لا ترسل تنبيه إلا إذا كان فوليوم الشمعة الحالية أكبر من السابقة بحد أدنى محدد.
    # الافتراضي 2.0x ويمكن تغييره من Railway عبر MIN_VOLUME_INCREASE.
    if volume_ratio < MIN_VOLUME_INCREASE:
        return None

    needed_values = [
        row["stoch_k"], row["stoch_d"], prev["stoch_k"], prev["stoch_d"],
        row["macd"], row["macd_signal"], row["hist"], prev["hist"],
    ]
    if any(pd.isna(x) for x in needed_values):
        return None

    stoch_cross = float(prev["stoch_k"]) <= float(prev["stoch_d"]) and float(row["stoch_k"]) > float(row["stoch_d"])
    stoch_ok = float(row["stoch_k"]) <= STOCH_MAX and float(row["stoch_k"]) > float(row["stoch_d"])
    if REQUIRE_STOCH_CROSS:
        stoch_ok = stoch_ok and stoch_cross

    macd_ok = float(row["macd"]) > float(row["macd_signal"])
    if REQUIRE_MACD_POSITIVE:
        macd_ok = macd_ok and float(row["hist"]) > 0
    if REQUIRE_MACD_HISTOGRAM_UP:
        macd_ok = macd_ok and float(row["hist"]) > float(prev["hist"])

    if not (stoch_ok and macd_ok):
        return None

    return {
        "price": float(row["close"]),
        "current_candle_volume_usdt": current_volume_usdt,
        "previous_candle_volume_usdt": previous_volume_usdt,
        "volume_increase_ratio": volume_ratio,
        "stoch_k": float(row["stoch_k"]),
        "stoch_d": float(row["stoch_d"]),
        "prev_stoch_k": float(prev["stoch_k"]),
        "prev_stoch_d": float(prev["stoch_d"]),
        "macd": float(row["macd"]),
        "macd_signal": float(row["macd_signal"]),
        "hist": float(row["hist"]),
        "prev_hist": float(prev["hist"]),
        "candle_time": float(row["time"]),
        "candle_mode": "closed" if USE_CLOSED_CANDLE else "live/current",
    }



def get_4h_confirmation(candles: List[List[float]]) -> Optional[Dict[str, float]]:
    """Return bullish 4H confirmation data, or None when the 4H filter fails."""
    minimum_candles = max(MACD_SLOW + MACD_SIGNAL + 20, 80)
    if len(candles) < minimum_candles:
        return None

    df = pd.DataFrame(candles, columns=["time", "open", "high", "low", "close", "volume"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    stoch = StochRSIIndicator(
        close=df["close"],
        window=STOCH_RSI_PERIOD,
        smooth1=STOCH_K,
        smooth2=STOCH_D,
        fillna=False,
    )
    df["stoch_k"] = stoch.stochrsi_k() * 100.0
    df["stoch_d"] = stoch.stochrsi_d() * 100.0

    macd_ind = MACD(
        close=df["close"],
        window_fast=MACD_FAST,
        window_slow=MACD_SLOW,
        window_sign=MACD_SIGNAL,
        fillna=False,
    )
    df["macd"] = macd_ind.macd()
    df["macd_signal"] = macd_ind.macd_signal()
    df["hist"] = macd_ind.macd_diff()

    idx = len(df) - 2 if USE_CLOSED_4H_CANDLE and len(df) >= 2 else len(df) - 1
    if idx < 1:
        return None

    row = df.iloc[idx]
    needed = [row["stoch_k"], row["stoch_d"], row["macd"], row["macd_signal"], row["hist"]]
    if any(pd.isna(x) for x in needed):
        return None

    stoch_bullish = float(row["stoch_k"]) > float(row["stoch_d"])
    macd_bullish = (
        float(row["macd"]) > float(row["macd_signal"])
        and float(row["hist"]) > 0
    )
    if REQUIRE_4H_MACD_ABOVE_ZERO:
        macd_bullish = macd_bullish and float(row["macd"]) > 0

    if REQUIRE_4H_STOCH_BULLISH and not stoch_bullish:
        return None
    if REQUIRE_4H_MACD_BULLISH and not macd_bullish:
        return None

    return {
        "stoch_k": float(row["stoch_k"]),
        "stoch_d": float(row["stoch_d"]),
        "macd": float(row["macd"]),
        "macd_signal": float(row["macd_signal"]),
        "hist": float(row["hist"]),
        "candle_time": float(row["time"]),
        "candle_mode": "closed" if USE_CLOSED_4H_CANDLE else "live/current",
    }


def format_price(value: float) -> str:
    if value >= 1:
        return f"{value:.6f}"
    if value >= 0.01:
        return f"{value:.8f}"
    return f"{value:.12g}"


TRADINGVIEW_EXCHANGE_CODES = {
    "okx": "OKX",
    "mexc": "MEXC",
    "gateio": "GATEIO",
    "kucoin": "KUCOIN",
}


def build_tradingview_data(exchange_id: str, symbol: str, timeframe: str) -> Tuple[str, str]:
    exchange_code = TRADINGVIEW_EXCHANGE_CODES.get(
        exchange_id.lower().strip(),
        exchange_id.upper().strip(),
    )
    clean_symbol = symbol.split(":")[0].replace("/", "").replace("-", "").upper()
    tv_code = f"{exchange_code}:{clean_symbol}"
    interval = str(timeframe_to_minutes(timeframe))
    separator = "&" if "?" in TRADINGVIEW_BASE_URL else "?"
    tv_url = f"{TRADINGVIEW_BASE_URL}{separator}symbol={tv_code}&interval={interval}"
    return tv_code, tv_url


def build_targets_block(entry_price: float) -> str:
    if not ENABLE_TARGETS:
        return ""

    targets = [
        ("TP1", TP1),
        ("TP2", TP2),
        ("TP3", TP3),
        ("TP4", TP4),
        ("TP5", TP5),
    ]

    lines = ["", "🎯 الأهداف:"]
    for label, percent in targets:
        target_price = entry_price * (1 + percent / 100)
        lines.append(f"{label} (+{percent:.2f}%): {format_price(target_price)}")

    stop_price = entry_price * (1 - STOP_LOSS / 100)
    lines.append("")
    lines.append(f"🛑 وقف الخسارة (-{STOP_LOSS:.2f}%): {format_price(stop_price)}")

    return "\n".join(lines)



def is_tokenized_stock(exchange_id: str, symbol: str, market: Dict[str, Any]) -> bool:
    base_raw = str(market.get("base", "") or "").strip()
    base = base_raw.upper()
    symbol_upper = str(symbol).upper().strip()

    if base in TOKENIZED_STOCK_BASES:
        return True

    searchable_market_data = {
        "id": market.get("id"),
        "symbol": market.get("symbol"),
        "base": market.get("base"),
        "quote": market.get("quote"),
        "type": market.get("type"),
        "subType": market.get("subType"),
        "info": market.get("info", {}),
    }
    market_text = json.dumps(
        searchable_market_data,
        ensure_ascii=False,
        default=str,
    ).lower()

    if any(keyword in market_text for keyword in TOKENIZED_STOCK_KEYWORDS):
        return True


    if base.endswith("X") and base[:-1] in STOCK_TICKERS:
        return True

    if symbol_upper.endswith("/USDT") and base.startswith("R"):
        if base[1:] in STOCK_TICKERS:
            return True

    return False


class BotRunner:
    def __init__(self):
        self.telegram = Bot(token=TELEGRAM_BOT_TOKEN)
        self.state = JsonState(STATE_FILE)
        self.sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    def cooldown_ok(self, key: str) -> bool:
        sent = self.state.data.setdefault("sent", {})
        last = float(sent.get(key, 0) or 0)
        return (time.time() - last) / 3600 >= SIGNAL_COOLDOWN_HOURS

    def mark_sent(self, key: str):
        self.state.data.setdefault("sent", {})[key] = time.time()
        cutoff = time.time() - 30 * 86400
        self.state.data["sent"] = {
            k: v
            for k, v in self.state.data.get("sent", {}).items()
            if float(v or 0) > cutoff
        }
        self.state.save()

    async def send(self, text: str):
        await self.telegram.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=text,
            disable_web_page_preview=DISABLE_WEB_PAGE_PREVIEW,
        )

    def make_exchange(self, exchange_id: str):
        cls = getattr(ccxt, exchange_id)
        return cls({"enableRateLimit": True, "timeout": REQUEST_TIMEOUT * 1000})

    async def get_symbols(self, exchange) -> List[str]:
        markets = await exchange.load_markets()
        symbols: List[str] = []
        exchange_id = str(getattr(exchange, "id", "")).lower().strip()

        excluded_counts = {
            "manual": 0,
            "stablecoin": 0,
            "memecoin": 0,
            "leveraged": 0,
            "tokenized_stock": 0,
        }

        for symbol, market in markets.items():
            if not market.get("active", True):
                continue
            if not market.get("spot", False):
                continue

            normalized_symbol = str(symbol).upper().strip()
            base = str(market.get("base", "") or "").upper().strip()
            quote = str(market.get("quote", "") or "").upper().strip()

            if normalized_symbol in EXCLUDE_SYMBOLS:
                excluded_counts["manual"] += 1
                continue
            if quote not in QUOTE_ASSETS:
                continue
            if ":" in str(symbol):
                continue

            if base in STABLECOIN_BASES:
                excluded_counts["stablecoin"] += 1
                continue

            if base in MEMECOIN_BASES:
                excluded_counts["memecoin"] += 1
                continue

            if base.endswith(LEVERAGED_SUFFIXES):
                excluded_counts["leveraged"] += 1
                continue

            if is_tokenized_stock(exchange_id, symbol, market):
                excluded_counts["tokenized_stock"] += 1
                logger.info("Tokenized stock excluded: %s on %s", symbol, exchange_id)
                continue

            symbols.append(symbol)

        symbols = sorted(set(symbols))

        if MAX_SYMBOLS_PER_EXCHANGE > 0:
            symbols = symbols[:MAX_SYMBOLS_PER_EXCHANGE]

        logger.info(
            "%s filters | manual=%d stable=%d meme=%d leveraged=%d stocks=%d accepted=%d",
            exchange_id,
            excluded_counts["manual"],
            excluded_counts["stablecoin"],
            excluded_counts["memecoin"],
            excluded_counts["leveraged"],
            excluded_counts["tokenized_stock"],
            len(symbols),
        )
        return symbols

    async def analyze_symbol(
        self,
        exchange,
        exchange_id: str,
        symbol: str,
    ) -> List[Tuple[str, str, str, Dict[str, Any]]]:
        async with self.sem:
            raw_results: List[Tuple[str, str, str, Dict[str, Any]]] = []
            try:
                confirmation = None
                if ENABLE_4H_CONFIRMATION:
                    confirmation_candles = await exchange.fetch_ohlcv(
                        symbol,
                        timeframe=CONFIRMATION_TIMEFRAME,
                        limit=CONFIRMATION_CANDLE_LIMIT,
                    )
                    confirmation = get_4h_confirmation(confirmation_candles)
                    if confirmation is None:
                        return raw_results

                source_cache: Dict[str, List[List[float]]] = {}
                for timeframe in TIMEFRAMES:
                    try:
                        source_tf = source_timeframe_for(timeframe)
                        if source_tf not in source_cache:
                            source_cache[source_tf] = await exchange.fetch_ohlcv(
                                symbol,
                                timeframe=source_tf,
                                limit=fetch_limit_for(source_tf, timeframe),
                            )

                        source_candles = source_cache[source_tf]
                        candles = aggregate_candles(source_candles, source_tf, timeframe)
                        if len(candles) > CANDLE_LIMIT:
                            candles = candles[-CANDLE_LIMIT:]

                        result = is_bullish_signal(candles)
                        if result:
                            if confirmation is not None:
                                result["confirmation"] = confirmation
                            raw_results.append((exchange_id, symbol, timeframe, result))
                    except Exception as e:
                        logger.debug(
                            "%s %s %s analysis failed: %s",
                            exchange_id, symbol, timeframe, e,
                        )

                matched_timeframes = [item[2] for item in raw_results]
                timeframe_score = sum(TIMEFRAME_WEIGHTS.get(tf, 0) for tf in matched_timeframes)
                confirmation_score = (
                    FOUR_H_CONFIRMATION_SCORE
                    if ENABLE_4H_CONFIRMATION and confirmation is not None
                    else 0
                )
                total_score = timeframe_score + confirmation_score

                if ENABLE_TIMEFRAME_SCORE and total_score < MIN_SIGNAL_SCORE:
                    logger.debug(
                        "%s %s score %d below minimum %d; matches=%s",
                        exchange_id, symbol, total_score, MIN_SIGNAL_SCORE, matched_timeframes,
                    )
                    return []

                for _, _, _, result in raw_results:
                    result["signal_score"] = total_score
                    result["timeframe_score"] = timeframe_score
                    result["confirmation_score"] = confirmation_score
                    result["matched_timeframes"] = matched_timeframes

                return raw_results
            except Exception as e:
                logger.debug("%s %s fetch failed: %s", exchange_id, symbol, e)
                return []

    def build_message(
        self,
        exchange_id: str,
        symbol: str,
        timeframe: str,
        r: Dict[str, Any],
    ) -> str:
        candle_time = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(r["candle_time"] / 1000))
        score_block = ""
        if ENABLE_TIMEFRAME_SCORE:
            matched = ", ".join(r.get("matched_timeframes", [])) or timeframe
            score_block = f"""

⭐ درجة توافق الفريمات: {int(r.get('signal_score', 0))}
الحد الأدنى المطلوب: {MIN_SIGNAL_SCORE}
الفريمات المتوافقة: {matched}
نقاط الفريمات: {int(r.get('timeframe_score', 0))}
نقاط تأكيد 4H: {int(r.get('confirmation_score', 0))}
""".rstrip()

        confirmation_block = ""
        confirmation = r.get("confirmation")
        if confirmation:
            confirmation_time = time.strftime(
                "%Y-%m-%d %H:%M UTC",
                time.gmtime(confirmation["candle_time"] / 1000),
            )
            confirmation_block = f"""

✅ تأكيد الاتجاه — {CONFIRMATION_TIMEFRAME.upper()}:
نوع الشمعة: {confirmation['candle_mode']}
Stoch RSI K/D: {confirmation['stoch_k']:.2f} / {confirmation['stoch_d']:.2f}
MACD: {confirmation['macd']:.8f}
Signal: {confirmation['macd_signal']:.8f}
Histogram: {confirmation['hist']:.8f}
شمعة التأكيد: {confirmation_time}
""".rstrip()

        tradingview_block = ""
        if ENABLE_TRADINGVIEW:
            tv_code, tv_url = build_tradingview_data(exchange_id, symbol, timeframe)
            tradingview_block = f"""

📊 TradingView:
الكود: {tv_code}
الرابط: {tv_url}
""".rstrip()

        return f"""
🚀 Buy Signal — {timeframe.upper()}

العملة: {symbol}
المنصة: {exchange_id}
الفريم: {timeframe}
نوع الشمعة: {r['candle_mode']}

السعر: {r['price']:.12g}

💰 فوليوم شمعة {timeframe} الحالية:
${r['current_candle_volume_usdt']:,.0f}

💰 فوليوم الشمعة السابقة:
${r['previous_candle_volume_usdt']:,.0f}

🚀 زيادة الفوليوم:
{r['volume_increase_ratio']:.2f}x

🎯 الحد الأدنى للفوليوم:
${MIN_CANDLE_VOLUME_USDT:,.0f}

🎯 أقل زيادة مطلوبة:
{MIN_VOLUME_INCREASE:.2f}x

Stochastic RSI — ta library:
K: {r['stoch_k']:.2f}
D: {r['stoch_d']:.2f}
السابق K/D: {r['prev_stoch_k']:.2f} / {r['prev_stoch_d']:.2f}
✅ تقاطع صاعد

MACD — ta library:
MACD: {r['macd']:.8f}
Signal: {r['macd_signal']:.8f}
Histogram: {r['hist']:.8f}
Prev Hist: {r['prev_hist']:.8f}
✅ MACD إيجابي والهستوجرام يتحسن
{score_block}

{build_targets_block(r['price'])}
{confirmation_block}

شمعة الإشارة: {candle_time}
{tradingview_block}

⚠️ ليست توصية شراء. تحقق من الشارت والسيولة قبل أي قرار.
""".strip()

    async def scan_exchange(self, exchange_id: str) -> int:
        exchange = self.make_exchange(exchange_id)
        sent_count = 0
        try:
            symbols = await self.get_symbols(exchange)
            logger.info("%s symbols: %d", exchange_id, len(symbols))
            tasks = [self.analyze_symbol(exchange, exchange_id, symbol) for symbol in symbols]
            for fut in asyncio.as_completed(tasks):
                if sent_count >= MAX_SIGNALS_PER_SCAN:
                    break
                results = await fut
                if not results:
                    continue

                for ex_id, symbol, timeframe, data in results:
                    if sent_count >= MAX_SIGNALS_PER_SCAN:
                        break

                    base_from_symbol = symbol.split("/")[0].upper().strip()
                    if (
                        base_from_symbol in TOKENIZED_STOCK_BASES
                        or (
                            base_from_symbol.startswith("R")
                            and base_from_symbol[1:] in STOCK_TICKERS
                        )
                        or (
                            base_from_symbol.endswith("X")
                            and base_from_symbol[:-1] in STOCK_TICKERS
                        )
                    ):
                        logger.warning(
                            "Blocked tokenized stock before Telegram send: %s on %s",
                            symbol,
                            ex_id,
                        )
                        continue

                    key = f"{ex_id}:{symbol}:{timeframe}:{data['candle_mode']}"
                    if not self.cooldown_ok(key):
                        continue

                    await self.send(
                        self.build_message(ex_id, symbol, timeframe, data)
                    )
                    self.mark_sent(key)
                    sent_count += 1
                    await asyncio.sleep(0.5)
        finally:
            await exchange.close()
        return sent_count

    async def scan_once(self):
        total = 0
        for exchange_id in EXCHANGES:
            try:
                count = await self.scan_exchange(exchange_id)
                total += count
                logger.info("%s signals sent: %d", exchange_id, count)
            except AttributeError:
                logger.warning("Exchange not supported by ccxt: %s", exchange_id)
            except Exception as e:
                logger.exception("Exchange scan failed %s: %s", exchange_id, e)
        logger.info("Total signals sent this scan: %d", total)

    async def run(self):
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        logger.info("Running bot version: %s", BOT_VERSION)
        await self.send(
            f"✅ Bot started: {BOT_VERSION}\n"
            f"Timeframes: {', '.join(TIMEFRAMES)}\n"
            f"Minimum score: {MIN_SIGNAL_SCORE} | 4H score: {FOUR_H_CONFIRMATION_SCORE}\n"
            "Independent timeframe signals + positive 4H confirmation + TradingView"
        )
        while True:
            try:
                await self.scan_once()
            except Exception as e:
                logger.exception("Main scan error: %s", e)
            await asyncio.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    asyncio.run(BotRunner().run())
