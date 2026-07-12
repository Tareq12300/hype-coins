from __future__ import annotations

import json
import logging
import math
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv
from tradingview_screener import Query

load_dotenv()


# ============================================================
# Helpers
# ============================================================
def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except (TypeError, ValueError):
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)).strip())
    except (TypeError, ValueError):
        return default


def env_csv(name: str, default: str = "", upper: bool = False) -> List[str]:
    values = [x.strip() for x in os.getenv(name, default).split(",") if x.strip()]
    return [x.upper() for x in values] if upper else values


def normalize_symbol(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def now_ts() -> float:
    return time.time()


def finite_number(value: Any) -> Optional[float]:
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def load_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logging.getLogger("bot").warning("تعذر قراءة %s: %s", path, exc)
    return default


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(path)


# ============================================================
# Environment
# ============================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

TIMEFRAME = os.getenv("TIMEFRAME", "4h").strip().lower()
SCAN_INTERVAL = env_int("SCAN_INTERVAL", 60)
CHECK_INTERVAL = env_int("CHECK_INTERVAL", SCAN_INTERVAL)

SIGNAL_ON_OPEN_CANDLE = env_bool("SIGNAL_ON_OPEN_CANDLE", True)
REALTIME_CONFIRMATIONS = max(1, env_int("REALTIME_CONFIRMATIONS", 3))
ONE_SIGNAL_PER_CANDLE = env_bool("ONE_SIGNAL_PER_CANDLE", True)
ALLOW_REPEAT_NEXT_CANDLE = env_bool("ALLOW_REPEAT_NEXT_CANDLE", True)
SIGNAL_COOLDOWN_HOURS = env_float("SIGNAL_COOLDOWN_HOURS", 4)

MARKET_DATA_SOURCES = {
    x.lower() for x in env_csv("MARKET_DATA_SOURCES", "coingecko,coinmarketcap")
}

COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "").strip()
COINGECKO_API_BASE = os.getenv(
    "COINGECKO_API_BASE", "https://api.coingecko.com/api/v3"
).rstrip("/")
CATEGORY_REFRESH_HOURS = env_float("CATEGORY_REFRESH_HOURS", 12)
MAX_COINS_PER_CATEGORY = max(1, env_int("MAX_COINS_PER_CATEGORY", 1000))

COINMARKETCAP_API_KEY = os.getenv("COINMARKETCAP_API_KEY", "").strip()
COINMARKETCAP_API_BASE = os.getenv(
    "COINMARKETCAP_API_BASE", "https://pro-api.coinmarketcap.com"
).rstrip("/")

EXCHANGES = set(env_csv("EXCHANGES", "OKX,BYBIT,BITGET,GATEIO,MEXC,KUCOIN", True))
QUOTE_ASSETS = set(env_csv("QUOTE_ASSETS", "USDT", True))

CATEGORY_IDS = env_csv(
    "CATEGORY_IDS",
    "artificial-intelligence,privacy-coins,decentralized-storage,identity,"
    "internet-of-things-iot,data-management,cloud-computing,"
    "enterprise-solutions",
)
CATEGORY_KEYWORDS = [x.lower() for x in env_csv(
    "CATEGORY_KEYWORDS",
    "artificial intelligence,ai,privacy,identity,storage,cloud,data management,"
    "internet of things,iot,enterprise solution",
)]

MIN_24H_VOLUME_USD = env_float("MIN_24H_VOLUME_USD", 1)
MIN_MARKET_CAP_USD = env_float("MIN_MARKET_CAP_USD", 1)
MAX_MARKET_CAP_USD = env_float("MAX_MARKET_CAP_USD", 0)

ENABLE_CANDLE_VOLUME_FILTER = env_bool("ENABLE_CANDLE_VOLUME_FILTER", True)
MIN_CANDLE_VOLUME_USDT = env_float("MIN_CANDLE_VOLUME_USDT", 1)
CANDLE_VOLUME_AVG_PERIOD = max(1, env_int("CANDLE_VOLUME_AVG_PERIOD", 20))
MIN_VOLUME_RATIO = env_float("MIN_VOLUME_RATIO", 1.0)
REQUIRE_VOLUME_ABOVE_AVERAGE = env_bool("REQUIRE_VOLUME_ABOVE_AVERAGE", True)
REQUIRE_VOLUME_RISING = env_bool("REQUIRE_VOLUME_RISING", True)

REQUIRE_MACD = env_bool("REQUIRE_MACD", True)
REQUIRE_MACD_ABOVE_ZERO = env_bool("REQUIRE_MACD_ABOVE_ZERO", True)
REQUIRE_MACD_ABOVE_SIGNAL = env_bool("REQUIRE_MACD_ABOVE_SIGNAL", True)
REQUIRE_MACD_RISING = env_bool("REQUIRE_MACD_RISING", True)
REQUIRE_HISTOGRAM_POSITIVE = env_bool("REQUIRE_HISTOGRAM_POSITIVE", True)
REQUIRE_HISTOGRAM_RISING = env_bool("REQUIRE_HISTOGRAM_RISING", True)

REQUIRE_STOCH_RSI = env_bool("REQUIRE_STOCH_RSI", True)
REQUIRE_STOCH_CROSS = env_bool("REQUIRE_STOCH_CROSS", True)
REQUIRE_STOCH_RSI_RISING = env_bool("REQUIRE_STOCH_RSI_RISING", True)
REQUIRE_STOCH_RSI_POSITIVE_REC = env_bool("REQUIRE_STOCH_RSI_POSITIVE_REC", True)
MIN_STOCH_RSI_K = env_float("MIN_STOCH_RSI_K", 20)
MAX_STOCH_RSI_K = env_float("MAX_STOCH_RSI_K", 80)

# Price movement filters
MIN_PRICE_CHANGE_4H = env_float("MIN_PRICE_CHANGE_4H", 0)
MAX_PRICE_CHANGE_4H = env_float("MAX_PRICE_CHANGE_4H", 20)

IGNORE_STABLECOINS = env_bool("IGNORE_STABLECOINS", True)
IGNORE_LEVERAGED_TOKENS = env_bool("IGNORE_LEVERAGED_TOKENS", True)
IGNORE_DELISTED = env_bool("IGNORE_DELISTED", True)

MAX_SIGNALS_PER_SCAN = max(1, env_int("MAX_SIGNALS_PER_SCAN", 100))
SEND_STARTUP_MESSAGE = env_bool("SEND_STARTUP_MESSAGE", True)
SEND_SCAN_SUMMARY = env_bool("SEND_SCAN_SUMMARY", False)

ENABLE_TAKE_PROFIT = env_bool("ENABLE_TAKE_PROFIT", True)
TP_LEVELS = [
    env_float("TP1", 5),
    env_float("TP2", 10),
    env_float("TP3", 20),
    env_float("TP4", 35),
    env_float("TP5", 50),
]
STOP_LOSS = env_float("STOP_LOSS", 8)

REQUEST_TIMEOUT = env_int("REQUEST_TIMEOUT", 30)
MAX_RETRIES = max(1, env_int("MAX_RETRIES", 3))
RETRY_DELAY = max(1, env_int("RETRY_DELAY", 5))

STATE_FILE = Path(os.getenv("STATE_FILE", "state.json"))
CATEGORY_CACHE_FILE = Path(os.getenv("CATEGORY_CACHE_FILE", "category_cache.json"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

TF_SUFFIX = {
    "1m": "1",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "2h": "120",
    "3h": "180",
    "4h": "240",
    "1d": "",
    "1w": "1W",
    "1mo": "1M",
}
TF_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
    "3h": 10800,
    "4h": 14400,
    "1d": 86400,
    "1w": 604800,
    "1mo": 2592000,
}

if TIMEFRAME not in TF_SUFFIX:
    raise ValueError(f"TIMEFRAME غير مدعوم: {TIMEFRAME}")

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("bot")
http = requests.Session()
http.headers.update({"User-Agent": "TradingViewCategoryBot/2.0"})


# ============================================================
# Categories configuration
# ============================================================
@dataclass(frozen=True)
class CategoryRule:
    env_name: str
    aliases: Tuple[str, ...]


CATEGORY_RULES: Tuple[CategoryRule, ...] = (
    CategoryRule("ENABLE_AI", ("artificial intelligence", "ai", "ai & big data")),
    CategoryRule("ENABLE_PRIVACY", ("privacy", "privacy coins")),
    CategoryRule("ENABLE_STORAGE", ("storage", "decentralized storage", "file storage")),
    CategoryRule("ENABLE_IDENTITY", ("identity", "digital identity")),
    CategoryRule("ENABLE_CLOUD", ("cloud", "cloud computing", "clouding")),
    CategoryRule("ENABLE_DATA_MANAGEMENT", ("data management", "data availability", "big data")),
    CategoryRule("ENABLE_IOT", ("internet of things", "iot")),
    CategoryRule("ENABLE_ENTERPRISE", ("enterprise", "enterprise solutions")),
    CategoryRule("ENABLE_STOCKS", ("stock", "stocks", "equities")),
    CategoryRule("ENABLE_TOKENIZED_STOCKS", ("tokenized stock", "tokenized stocks", "stock token")),
    CategoryRule("ENABLE_DEPIN", ("depin", "decentralized physical infrastructure")),
    CategoryRule("ENABLE_RWA", ("rwa", "real world asset", "real-world asset")),
    CategoryRule("ENABLE_LAYER1", ("layer 1", "layer-1")),
    CategoryRule("ENABLE_LAYER2", ("layer 2", "layer-2")),
    CategoryRule("ENABLE_DEFI", ("defi", "decentralized finance")),
    CategoryRule("ENABLE_ORACLE", ("oracle", "oracles")),
    CategoryRule("ENABLE_PAYMENTS", ("payments", "payment")),
    CategoryRule("ENABLE_EXCHANGE", ("exchange token", "centralized exchange", "dex")),
    CategoryRule("ENABLE_RESTAKING", ("restaking", "liquid restaking")),
    CategoryRule("ENABLE_ZK", ("zero knowledge", "zk", "zero-knowledge")),
    CategoryRule("ENABLE_GAMING", ("gaming", "gamefi")),
    CategoryRule("ENABLE_SOCIALFI", ("socialfi", "social finance")),
    CategoryRule("ENABLE_METAVERSE", ("metaverse",)),
    CategoryRule("ENABLE_MEME", ("meme", "memecoin", "meme coin")),
)


def enabled_aliases() -> Set[str]:
    aliases: Set[str] = set()
    for rule in CATEGORY_RULES:
        if env_bool(rule.env_name, False):
            aliases.update(a.lower() for a in rule.aliases)
    aliases.update(CATEGORY_KEYWORDS)
    return aliases


ENABLED_ALIASES = enabled_aliases()


# ============================================================
# API clients
# ============================================================
def request_json(
    method: str,
    url: str,
    *,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    data: Optional[dict] = None,
) -> Any:
    last_error: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = http.request(
                method,
                url,
                params=params,
                headers=headers,
                data=data,
                timeout=REQUEST_TIMEOUT,
            )
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", RETRY_DELAY * attempt))
                logger.warning("Rate limit: الانتظار %s ثانية", retry_after)
                time.sleep(retry_after)
                continue
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            logger.warning(
                "فشل الطلب (%s/%s) %s: %s",
                attempt,
                MAX_RETRIES,
                url,
                exc,
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)
    raise RuntimeError(f"فشل الطلب: {url}: {last_error}")


def coingecko_headers() -> Dict[str, str]:
    headers = {"accept": "application/json"}
    if COINGECKO_API_KEY:
        # Demo API endpoint uses x-cg-demo-api-key.
        headers["x-cg-demo-api-key"] = COINGECKO_API_KEY
    return headers


def cmc_headers() -> Dict[str, str]:
    headers = {"Accept": "application/json"}
    if COINMARKETCAP_API_KEY:
        headers["X-CMC_PRO_API_KEY"] = COINMARKETCAP_API_KEY
    return headers


@dataclass
class CoinMeta:
    symbol: str
    name: str
    market_cap: float = 0.0
    volume_24h: float = 0.0
    categories: Set[str] = field(default_factory=set)
    sources: Set[str] = field(default_factory=set)
    stablecoin: bool = False
    inactive: bool = False

    def merge(
        self,
        *,
        name: str,
        market_cap: float,
        volume_24h: float,
        category: str,
        source: str,
        stablecoin: bool = False,
        inactive: bool = False,
    ) -> None:
        if name and (not self.name or self.name == self.symbol):
            self.name = name
        self.market_cap = max(self.market_cap, market_cap)
        self.volume_24h = max(self.volume_24h, volume_24h)
        if category:
            self.categories.add(category)
        self.sources.add(source)
        self.stablecoin = self.stablecoin or stablecoin
        self.inactive = self.inactive or inactive


def category_matches(text: str) -> bool:
    text = text.lower()
    return any(alias in text for alias in ENABLED_ALIASES)


def market_filters_pass(market_cap: float, volume_24h: float) -> bool:
    if volume_24h < MIN_24H_VOLUME_USD:
        return False
    if market_cap < MIN_MARKET_CAP_USD:
        return False
    if MAX_MARKET_CAP_USD > 0 and market_cap > MAX_MARKET_CAP_USD:
        return False
    return True


def add_coin(
    universe: Dict[str, CoinMeta],
    *,
    symbol: Any,
    name: Any,
    market_cap: Any,
    volume_24h: Any,
    category: str,
    source: str,
    stablecoin: bool = False,
    inactive: bool = False,
) -> None:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return

    mc = finite_number(market_cap) or 0.0
    vol = finite_number(volume_24h) or 0.0
    if not market_filters_pass(mc, vol):
        return

    coin = universe.setdefault(
        normalized,
        CoinMeta(symbol=normalized, name=str(name or normalized)),
    )
    coin.merge(
        name=str(name or normalized),
        market_cap=mc,
        volume_24h=vol,
        category=category,
        source=source,
        stablecoin=stablecoin,
        inactive=inactive,
    )


def resolve_coingecko_categories() -> List[str]:
    resolved = list(dict.fromkeys(CATEGORY_IDS))
    try:
        data = request_json(
            "GET",
            f"{COINGECKO_API_BASE}/coins/categories/list",
            headers=coingecko_headers(),
        )
        if isinstance(data, list):
            for item in data:
                category_id = str(item.get("category_id") or "")
                name = str(item.get("name") or "")
                if category_id and category_matches(f"{category_id} {name}"):
                    if category_id not in resolved:
                        resolved.append(category_id)
    except Exception as exc:
        logger.warning("تعذر اكتشاف فئات CoinGecko تلقائيًا: %s", exc)
    return resolved


def fetch_coingecko(universe: Dict[str, CoinMeta]) -> None:
    if "coingecko" not in MARKET_DATA_SOURCES:
        return

    category_ids = resolve_coingecko_categories()
    pages = max(1, math.ceil(MAX_COINS_PER_CATEGORY / 250))

    for category_id in category_ids:
        loaded = 0
        for page in range(1, pages + 1):
            remaining = MAX_COINS_PER_CATEGORY - loaded
            if remaining <= 0:
                break
            per_page = min(250, remaining)
            try:
                data = request_json(
                    "GET",
                    f"{COINGECKO_API_BASE}/coins/markets",
                    headers=coingecko_headers(),
                    params={
                        "vs_currency": "usd",
                        "category": category_id,
                        "order": "volume_desc",
                        "per_page": per_page,
                        "page": page,
                        "sparkline": "false",
                    },
                )
            except Exception as exc:
                logger.warning("CoinGecko category=%s: %s", category_id, exc)
                break

            if not isinstance(data, list) or not data:
                break

            for item in data:
                add_coin(
                    universe,
                    symbol=item.get("symbol"),
                    name=item.get("name"),
                    market_cap=item.get("market_cap"),
                    volume_24h=item.get("total_volume"),
                    category=f"CG:{category_id}",
                    source="coingecko",
                )
            loaded += len(data)
            if len(data) < per_page:
                break
            time.sleep(1.1)


def fetch_cmc(universe: Dict[str, CoinMeta]) -> None:
    if "coinmarketcap" not in MARKET_DATA_SOURCES:
        return
    if not COINMARKETCAP_API_KEY:
        logger.warning("CoinMarketCap مفعّل لكن COINMARKETCAP_API_KEY فارغ")
        return

    try:
        response = request_json(
            "GET",
            f"{COINMARKETCAP_API_BASE}/v1/cryptocurrency/categories",
            headers=cmc_headers(),
            params={"limit": 5000},
        )
    except Exception as exc:
        logger.warning("تعذر جلب فئات CoinMarketCap: %s", exc)
        return

    categories = response.get("data", []) if isinstance(response, dict) else []
    matched: List[Tuple[str, str]] = []
    for item in categories:
        category_id = str(item.get("id") or "")
        name = str(item.get("name") or "")
        title = str(item.get("title") or "")
        description = str(item.get("description") or "")
        if category_id and category_matches(f"{name} {title} {description}"):
            matched.append((category_id, name or title or category_id))

    logger.info("CoinMarketCap categories matched: %s", len(matched))

    for category_id, category_name in matched:
        try:
            detail = request_json(
                "GET",
                f"{COINMARKETCAP_API_BASE}/v1/cryptocurrency/category",
                headers=cmc_headers(),
                params={
                    "id": category_id,
                    "start": 1,
                    "limit": MAX_COINS_PER_CATEGORY,
                    "convert": "USD",
                },
            )
        except Exception as exc:
            logger.warning("CMC category=%s: %s", category_name, exc)
            continue

        payload = detail.get("data", {}) if isinstance(detail, dict) else {}
        coins = payload.get("coins", []) if isinstance(payload, dict) else []
        for item in coins:
            quote = (item.get("quote") or {}).get("USD") or {}
            tags = [str(x).lower() for x in (item.get("tags") or [])]
            add_coin(
                universe,
                symbol=item.get("symbol"),
                name=item.get("name"),
                market_cap=quote.get("market_cap"),
                volume_24h=quote.get("volume_24h"),
                category=f"CMC:{category_name}",
                source="coinmarketcap",
                stablecoin="stablecoin" in tags,
                inactive=not bool(item.get("is_active", 1)),
            )
        time.sleep(0.3)


STABLE_SYMBOLS = {
    "USDT", "USDC", "DAI", "FDUSD", "TUSD", "USDE", "PYUSD", "USDP", "GUSD",
    "FRAX", "LUSD", "EURC", "EURT", "USDJ", "USDD", "USDS", "SUSD", "BUSD",
}
LEVERAGED_SUFFIXES = ("3L", "3S", "5L", "5S", "UP", "DOWN", "BULL", "BEAR")


def filter_universe(universe: Dict[str, CoinMeta]) -> Dict[str, CoinMeta]:
    filtered: Dict[str, CoinMeta] = {}
    for symbol, meta in universe.items():
        if IGNORE_STABLECOINS and (meta.stablecoin or symbol in STABLE_SYMBOLS):
            continue
        if IGNORE_LEVERAGED_TOKENS and symbol.endswith(LEVERAGED_SUFFIXES):
            continue
        if IGNORE_DELISTED and meta.inactive:
            continue
        filtered[symbol] = meta
    return filtered


def load_universe() -> Dict[str, CoinMeta]:
    cached = load_json(CATEGORY_CACHE_FILE, {})
    if (
        cached
        and now_ts() - float(cached.get("updated_at", 0))
        < CATEGORY_REFRESH_HOURS * 3600
    ):
        coins: Dict[str, CoinMeta] = {}
        for symbol, item in cached.get("coins", {}).items():
            coins[symbol] = CoinMeta(
                symbol=symbol,
                name=item.get("name", symbol),
                market_cap=float(item.get("market_cap", 0)),
                volume_24h=float(item.get("volume_24h", 0)),
                categories=set(item.get("categories", [])),
                sources=set(item.get("sources", [])),
                stablecoin=bool(item.get("stablecoin", False)),
                inactive=bool(item.get("inactive", False)),
            )
        return filter_universe(coins)

    universe: Dict[str, CoinMeta] = {}
    fetch_coingecko(universe)
    fetch_cmc(universe)
    universe = filter_universe(universe)

    serializable = {
        symbol: {
            "name": meta.name,
            "market_cap": meta.market_cap,
            "volume_24h": meta.volume_24h,
            "categories": sorted(meta.categories),
            "sources": sorted(meta.sources),
            "stablecoin": meta.stablecoin,
            "inactive": meta.inactive,
        }
        for symbol, meta in universe.items()
    }
    save_json(
        CATEGORY_CACHE_FILE,
        {"updated_at": now_ts(), "coins": serializable},
    )
    return universe


# ============================================================
# TradingView
# ============================================================
def tv_field(name: str, offset: int = 0) -> str:
    suffix = TF_SUFFIX[TIMEFRAME]
    history = f"[{offset}]" if offset else ""
    return f"{name}{history}|{suffix}" if suffix else f"{name}{history}"


def tradingview_columns() -> List[str]:
    columns = [
        "name",
        "description",
        "exchange",
        tv_field("close"),
        tv_field("change"),
        tv_field("volume"),
        tv_field("volume", 1),
        tv_field("MACD.macd"),
        tv_field("MACD.signal"),
        tv_field("MACD.macd", 1),
        tv_field("MACD.signal", 1),
        tv_field("Stoch.RSI.K"),
        tv_field("Stoch.RSI.D"),
        tv_field("Stoch.RSI.K", 1),
        tv_field("Stoch.RSI.D", 1),
        tv_field("Rec.Stoch.RSI"),
    ]
    for offset in range(1, CANDLE_VOLUME_AVG_PERIOD + 1):
        field_name = tv_field("volume", offset)
        if field_name not in columns:
            columns.append(field_name)
    return columns


def fetch_tradingview() -> pd.DataFrame:
    total, frame = (
        Query()
        .set_markets("crypto")
        .select(*tradingview_columns())
        .limit(50000)
        .get_scanner_data(timeout=REQUEST_TIMEOUT)
    )
    logger.info("TradingView rows: %s", total)
    return frame if frame is not None else pd.DataFrame()


def value(row: pd.Series, field_name: str, offset: int = 0) -> Optional[float]:
    return finite_number(row.get(tv_field(field_name, offset)))


def parse_pair(raw_name: Any) -> Optional[Tuple[str, str]]:
    pair = normalize_symbol(raw_name)
    for quote in sorted(QUOTE_ASSETS, key=len, reverse=True):
        if pair.endswith(quote) and len(pair) > len(quote):
            return pair[:-len(quote)], quote
    return None


def candle_id(timestamp: Optional[float] = None) -> int:
    current = timestamp if timestamp is not None else now_ts()
    return int(current // TF_SECONDS[TIMEFRAME])


@dataclass
class Signal:
    key: str
    candle: int
    exchange: str
    base: str
    quote: str
    name: str
    price: float
    change: float
    macd: float
    macd_signal: float
    macd_prev: float
    macd_signal_prev: float
    histogram: float
    histogram_prev: float
    stoch_k: float
    stoch_d: float
    stoch_k_prev: float
    stoch_d_prev: float
    stoch_rec: Optional[float]
    candle_volume_usdt: float
    previous_volume_usdt: float
    average_volume_usdt: float
    volume_ratio: float
    market_cap: float
    volume_24h: float
    categories: List[str]
    sources: List[str]



def build_signal(row: pd.Series, universe: Dict[str, CoinMeta]) -> Optional[Signal]:
    exchange = str(row.get("exchange") or "").upper()
    if EXCHANGES and exchange not in EXCHANGES:
        return None

    pair = parse_pair(row.get("name"))
    if not pair:
        return None
    base, quote = pair

    meta = universe.get(base)
    if not meta:
        return None

    price = value(row, "close")
    change = value(row, "change") or 0.0
    current_base_volume = value(row, "volume")
    previous_base_volume = value(row, "volume", 1)

    macd = value(row, "MACD.macd")
    macd_signal = value(row, "MACD.signal")
    macd_prev = value(row, "MACD.macd", 1)
    macd_signal_prev = value(row, "MACD.signal", 1)

    stoch_k = value(row, "Stoch.RSI.K")
    stoch_d = value(row, "Stoch.RSI.D")
    stoch_k_prev = value(row, "Stoch.RSI.K", 1)
    stoch_d_prev = value(row, "Stoch.RSI.D", 1)
    stoch_rec = value(row, "Rec.Stoch.RSI")

    required = [
        price,
        current_base_volume,
        previous_base_volume,
        macd,
        macd_signal,
        macd_prev,
        macd_signal_prev,
        stoch_k,
        stoch_d,
        stoch_k_prev,
        stoch_d_prev,
    ]
    if any(item is None for item in required):
        return None

    # Filter the current 4H/open-candle price change.
    if change < MIN_PRICE_CHANGE_4H:
        return None
    if MAX_PRICE_CHANGE_4H > 0 and change > MAX_PRICE_CHANGE_4H:
        return None

    # TradingView crypto volume is normally base-asset volume.
    # Convert it to quote/USDT notional using current price.
    current_volume_usdt = current_base_volume * price
    previous_volume_usdt = previous_base_volume * price

    historical_volumes: List[float] = []
    for offset in range(1, CANDLE_VOLUME_AVG_PERIOD + 1):
        base_volume = value(row, "volume", offset)
        if base_volume is not None:
            historical_volumes.append(base_volume * price)

    if not historical_volumes:
        return None
    average_volume_usdt = sum(historical_volumes) / len(historical_volumes)
    volume_ratio = (
        current_volume_usdt / average_volume_usdt
        if average_volume_usdt > 0
        else 0.0
    )

    histogram = macd - macd_signal
    histogram_prev = macd_prev - macd_signal_prev

    if ENABLE_CANDLE_VOLUME_FILTER:
        if current_volume_usdt < MIN_CANDLE_VOLUME_USDT:
            return None
        if REQUIRE_VOLUME_ABOVE_AVERAGE and current_volume_usdt <= average_volume_usdt:
            return None
        if volume_ratio < MIN_VOLUME_RATIO:
            return None
        if REQUIRE_VOLUME_RISING and current_volume_usdt <= previous_volume_usdt:
            return None

    if REQUIRE_MACD:
        if REQUIRE_MACD_ABOVE_ZERO and macd <= 0:
            return None
        if REQUIRE_MACD_ABOVE_SIGNAL and macd <= macd_signal:
            return None
        if REQUIRE_MACD_RISING and macd <= macd_prev:
            return None
        if REQUIRE_HISTOGRAM_POSITIVE and histogram <= 0:
            return None
        if REQUIRE_HISTOGRAM_RISING and histogram <= histogram_prev:
            return None

    if REQUIRE_STOCH_RSI:
        if not (MIN_STOCH_RSI_K <= stoch_k <= MAX_STOCH_RSI_K):
            return None
        if REQUIRE_STOCH_RSI_RISING and stoch_k <= stoch_k_prev:
            return None
        if REQUIRE_STOCH_CROSS:
            # Cross happened on current open candle:
            # previous K <= previous D and current K > current D.
            if not (stoch_k_prev <= stoch_d_prev and stoch_k > stoch_d):
                return None
        if REQUIRE_STOCH_RSI_POSITIVE_REC and (stoch_rec is None or stoch_rec <= 0):
            return None

    current_candle = candle_id()
    key = f"{exchange}:{base}{quote}:{TIMEFRAME}"
    return Signal(
        key=key,
        candle=current_candle,
        exchange=exchange,
        base=base,
        quote=quote,
        name=meta.name,
        price=price,
        change=change,
        macd=macd,
        macd_signal=macd_signal,
        macd_prev=macd_prev,
        macd_signal_prev=macd_signal_prev,
        histogram=histogram,
        histogram_prev=histogram_prev,
        stoch_k=stoch_k,
        stoch_d=stoch_d,
        stoch_k_prev=stoch_k_prev,
        stoch_d_prev=stoch_d_prev,
        stoch_rec=stoch_rec,
        candle_volume_usdt=current_volume_usdt,
        previous_volume_usdt=previous_volume_usdt,
        average_volume_usdt=average_volume_usdt,
        volume_ratio=volume_ratio,
        market_cap=meta.market_cap,
        volume_24h=meta.volume_24h,
        categories=sorted(meta.categories),
        sources=sorted(meta.sources),
    )


# ============================================================
# Signal persistence and Telegram
# ============================================================
def fmt_money(number: float) -> str:
    absolute = abs(number)
    if absolute >= 1_000_000_000:
        return f"${number / 1_000_000_000:.2f}B"
    if absolute >= 1_000_000:
        return f"${number / 1_000_000:.2f}M"
    if absolute >= 1_000:
        return f"${number / 1_000:.2f}K"
    return f"${number:,.2f}"


def fmt_price(number: float) -> str:
    if number >= 1000:
        return f"{number:,.2f}"
    if number >= 1:
        return f"{number:.6f}".rstrip("0").rstrip(".")
    return f"{number:.10f}".rstrip("0").rstrip(".")


def target_price(entry: float, percent: float) -> float:
    return entry * (1 + percent / 100)


def stop_price(entry: float) -> float:
    return entry * (1 - STOP_LOSS / 100)


def signal_message(signal: Signal) -> str:
    mode = "الشمعة المفتوحة" if SIGNAL_ON_OPEN_CANDLE else "الشمعة المغلقة"
    categories = ", ".join(signal.categories[:8]) or "غير متاح"
    sources = ", ".join(signal.sources) or "غير متاح"

    targets = ""
    if ENABLE_TAKE_PROFIT:
        target_lines = [
            f"• TP{i}: <code>{fmt_price(target_price(signal.price, pct))}</code> (+{pct:g}%)"
            for i, pct in enumerate(TP_LEVELS, start=1)
        ]
        target_lines.append(
            f"• SL: <code>{fmt_price(stop_price(signal.price))}</code> (-{STOP_LOSS:g}%)"
        )
        targets = "\n\n🎯 <b>الأهداف</b>\n" + "\n".join(target_lines)

    return (
        f"🚨 <b>إشارة TradingView — {TIMEFRAME}</b>\n"
        f"🟢 الوضع: <b>{mode}</b>\n\n"
        f"🪙 <b>{signal.name} ({signal.base}/{signal.quote})</b>\n"
        f"🏦 المنصة: <b>{signal.exchange}</b>\n"
        f"💰 الدخول التقريبي: <code>{fmt_price(signal.price)}</code>\n"
        f"📈 تغير الفريم: <b>{signal.change:+.2f}%</b>\n"

        "📊 <b>MACD</b>\n"
        f"• MACD: <code>{signal.macd:.8f}</code>\n"
        f"• السابق: <code>{signal.macd_prev:.8f}</code>\n"
        f"• Signal: <code>{signal.macd_signal:.8f}</code>\n"
        f"• Histogram: <code>{signal.histogram:.8f}</code>\n"
        f"• Histogram السابق: <code>{signal.histogram_prev:.8f}</code>\n\n"
        "📉 <b>Stoch RSI</b>\n"
        f"• K: <code>{signal.stoch_k:.2f}</code>\n"
        f"• D: <code>{signal.stoch_d:.2f}</code>\n"
        f"• K السابق: <code>{signal.stoch_k_prev:.2f}</code>\n"
        f"• D السابق: <code>{signal.stoch_d_prev:.2f}</code>\n\n"
        "📦 <b>حجم الشمعة</b>\n"
        f"• الحالي: <b>{fmt_money(signal.candle_volume_usdt)}</b>\n"
        f"• السابق: <b>{fmt_money(signal.previous_volume_usdt)}</b>\n"
        f"• متوسط {CANDLE_VOLUME_AVG_PERIOD}: <b>{fmt_money(signal.average_volume_usdt)}</b>\n"
        f"• النسبة: <b>{signal.volume_ratio:.2f}x</b>\n\n"
        f"🏷 الفئات: <code>{categories}</code>\n"
        f"🌐 المصادر: <code>{sources}</code>\n"
        f"💧 حجم 24 ساعة: <b>{fmt_money(signal.volume_24h)}</b>\n"
        f"🏛 القيمة السوقية: <b>{fmt_money(signal.market_cap)}</b>"
        f"{targets}\n\n"
        "⚠️ تنبيه فني وليس توصية مالية."
    )


def send_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("TELEGRAM_BOT_TOKEN أو TELEGRAM_CHAT_ID غير موجود")
        return False
    try:
        response = http.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return True
    except Exception as exc:
        logger.error("فشل إرسال Telegram: %s", exc)
        return False


def can_send(signal: Signal, state: Dict[str, Any]) -> bool:
    sent = state.setdefault("sent", {})
    record = sent.get(signal.key, {})
    last_time = float(record.get("time", 0))
    last_candle = int(record.get("candle", -1))

    if ONE_SIGNAL_PER_CANDLE and last_candle == signal.candle:
        return False

    if not ALLOW_REPEAT_NEXT_CANDLE and last_time > 0:
        return False

    # When repeats are allowed, a new candle bypasses the generic cooldown.
    if ALLOW_REPEAT_NEXT_CANDLE and last_candle != signal.candle:
        return True

    return now_ts() - last_time >= SIGNAL_COOLDOWN_HOURS * 3600


def process_confirmations(
    candidates: List[Signal],
    state: Dict[str, Any],
) -> List[Signal]:
    confirmation_state = state.setdefault("confirmations", {})
    current_keys = {signal.key for signal in candidates}
    ready: List[Signal] = []

    for signal in candidates:
        record = confirmation_state.get(signal.key, {})
        if int(record.get("candle", -1)) != signal.candle:
            count = 1
        else:
            count = int(record.get("count", 0)) + 1

        confirmation_state[signal.key] = {
            "candle": signal.candle,
            "count": count,
            "updated_at": now_ts(),
        }
        if count >= REALTIME_CONFIRMATIONS:
            ready.append(signal)

    # A failed scan breaks consecutive confirmation.
    for key in list(confirmation_state):
        if key not in current_keys:
            confirmation_state.pop(key, None)

    return ready


def scan_once() -> Tuple[int, int, int]:
    universe = load_universe()
    if not universe:
        raise RuntimeError("لم يتم جلب أي عملة من الفئات")

    frame = fetch_tradingview()
    if frame.empty:
        raise RuntimeError("TradingView لم يرجع بيانات")

    candidates: List[Signal] = []
    for _, row in frame.iterrows():
        try:
            signal = build_signal(row, universe)
            if signal:
                candidates.append(signal)
        except Exception as exc:
            logger.debug("تعذر تحليل صف: %s", exc)

    # Keep the most liquid exchange instance first.
    candidates.sort(
        key=lambda s: (s.volume_ratio, s.candle_volume_usdt),
        reverse=True,
    )

    state = load_json(STATE_FILE, {"sent": {}, "confirmations": {}})
    if SIGNAL_ON_OPEN_CANDLE:
        ready = process_confirmations(candidates, state)
    else:
        # Closed-candle mode uses previous bar values only conceptually.
        # This bot is configured for open-candle mode; no repeated confirmation needed here.
        ready = candidates

    sent_count = 0
    for signal in ready:
        if sent_count >= MAX_SIGNALS_PER_SCAN:
            break
        if not can_send(signal, state):
            continue
        if send_telegram(signal_message(signal)):
            state.setdefault("sent", {})[signal.key] = {
                "time": now_ts(),
                "candle": signal.candle,
            }
            sent_count += 1
            time.sleep(0.25)

    cutoff = now_ts() - 30 * 86400
    state["sent"] = {
        key: record
        for key, record in state.get("sent", {}).items()
        if float(record.get("time", 0)) >= cutoff
    }
    save_json(STATE_FILE, state)

    if SEND_SCAN_SUMMARY:
        send_telegram(
            "🔍 <b>ملخص الفحص</b>\n"
            f"عملات الفئات: <b>{len(universe)}</b>\n"
            f"المطابقات: <b>{len(candidates)}</b>\n"
            f"بعد التأكيد: <b>{len(ready)}</b>\n"
            f"المرسل: <b>{sent_count}</b>"
        )

    logger.info(
        "Universe=%s Candidates=%s Ready=%s Sent=%s",
        len(universe),
        len(candidates),
        len(ready),
        sent_count,
    )
    return len(universe), len(candidates), sent_count


def validate_environment() -> None:
    if not MARKET_DATA_SOURCES:
        raise ValueError("MARKET_DATA_SOURCES فارغ")
    unsupported = MARKET_DATA_SOURCES - {"coingecko", "coinmarketcap"}
    if unsupported:
        raise ValueError(f"مصادر غير مدعومة: {sorted(unsupported)}")
    if MIN_STOCH_RSI_K > MAX_STOCH_RSI_K:
        raise ValueError("MIN_STOCH_RSI_K أكبر من MAX_STOCH_RSI_K")
    if MAX_PRICE_CHANGE_4H > 0 and MIN_PRICE_CHANGE_4H > MAX_PRICE_CHANGE_4H:
        raise ValueError("MIN_PRICE_CHANGE_4H أكبر من MAX_PRICE_CHANGE_4H")
    if CANDLE_VOLUME_AVG_PERIOD > 50:
        logger.warning("CANDLE_VOLUME_AVG_PERIOD مرتفع وقد يجعل طلب TradingView كبيرًا")
    if SIGNAL_ON_OPEN_CANDLE and REALTIME_CONFIRMATIONS < 2:
        logger.warning("الشمعة المفتوحة دون تأكيدات متعددة قد تنتج إشارات متذبذبة")


def main() -> None:
    validate_environment()

    if SEND_STARTUP_MESSAGE:
        send_telegram(
            "✅ <b>تم تشغيل البوت</b>\n"
            f"الفريم: <b>{TIMEFRAME}</b>\n"
            f"الشمعة المفتوحة: <b>{SIGNAL_ON_OPEN_CANDLE}</b>\n"
            f"عدد التأكيدات: <b>{REALTIME_CONFIRMATIONS}</b>\n"
            f"المصادر: <code>{', '.join(sorted(MARKET_DATA_SOURCES))}</code>"
        )

    while True:
        started = datetime.now(timezone.utc)
        logger.info("بدء الفحص: %s", started.isoformat())
        try:
            scan_once()
        except KeyboardInterrupt:
            logger.info("تم إيقاف البوت")
            break
        except Exception:
            logger.exception("فشل الفحص")
        time.sleep(max(10, CHECK_INTERVAL))


if __name__ == "__main__":
    main()
