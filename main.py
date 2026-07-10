import asyncio
import html
import json
import logging
import os
import signal
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, Iterable, List, Optional

import aiohttp
import websockets
from dotenv import load_dotenv


load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("coin-sonar-ws")


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def chunks(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


EXCHANGES = {
    x.strip().lower()
    for x in os.getenv("EXCHANGES", "gateio,mexc,kucoin,bitget").split(",")
    if x.strip()
}
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

WINDOW_SECONDS = env_int("WINDOW_SECONDS", 60)
MIN_CHANGE_15M = env_float("MIN_CHANGE_15M", 0.5)
MIN_CHANGE_1M_CONFIRM = env_float("MIN_CHANGE_1M_CONFIRM", 0.15)
MIN_VOLUME_1M_USDT = env_float("MIN_VOLUME_1M_USDT", 5_000)
MIN_VOLUME_SPIKE_1M = env_float("MIN_VOLUME_SPIKE_1M", 1.5)
MIN_AVERAGE_VOLUME_1M_USDT = env_float("MIN_AVERAGE_VOLUME_1M_USDT", 500)
VOLUME_BASELINE_1M_CANDLES = env_int("VOLUME_BASELINE_1M_CANDLES", 20)
MIN_VOLUME_BASELINE_1M_CANDLES = env_int("MIN_VOLUME_BASELINE_1M_CANDLES", 20)
MIN_TRADES_1M = env_int("MIN_TRADES_1M", 3)
MIN_VOLUME_15M_USDT = env_float("MIN_VOLUME_15M_USDT", 20_000)
MIN_BUY_PERCENT = env_float("MIN_BUY_PERCENT", 55)
MIN_24H_VOLUME_USDT = env_float("MIN_24H_VOLUME_USDT", 250_000)
MAX_24H_CHANGE = env_float("MAX_24H_CHANGE", 30)
ALERT_COOLDOWN_MINUTES = env_int("ALERT_COOLDOWN_MINUTES", 30)
MAX_ALERTS_PER_COIN_PER_HOUR = env_int("MAX_ALERTS_PER_COIN_PER_HOUR", 3)
TICKER_REFRESH_SECONDS = env_int("TICKER_REFRESH_SECONDS", 300)
VOLUME_BASELINE_CANDLES = env_int("VOLUME_BASELINE_CANDLES", 20)
MIN_VOLUME_BASELINE_CANDLES = env_int("MIN_VOLUME_BASELINE_CANDLES", 20)
WHALE_TRADE_USDT = env_float("WHALE_TRADE_USDT", 25_000)
MIN_SIGNAL_SCORE = env_int("MIN_SIGNAL_SCORE", 70)
MIN_AVERAGE_VOLUME_USDT = env_float("MIN_AVERAGE_VOLUME_USDT", 5000)
MIN_VOLUME_SPIKE = env_float("MIN_VOLUME_SPIKE", 1.5)
MIN_TRADES_15M = env_int("MIN_TRADES_15M", 10)
MAX_WS_START_DELAY_SECONDS = env_int("MAX_WS_START_DELAY_SECONDS", 120)
PRIMARY_TIMEFRAME = os.getenv("PRIMARY_TIMEFRAME", "15m").strip().lower()
CONFIRM_TIMEFRAME = os.getenv("CONFIRM_TIMEFRAME", "1m").strip().lower()
CANDLE_LIMIT = env_int("CANDLE_LIMIT", 25)
CANDLE_REQUEST_COOLDOWN_SECONDS = env_int("CANDLE_REQUEST_COOLDOWN_SECONDS", 10)
SHOW_TRADINGVIEW_LINK = os.getenv("SHOW_TRADINGVIEW_LINK", "true").lower() == "true"
STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").lower() == "true"
MEXC_SKIP_INVALID_KLINES = os.getenv("MEXC_SKIP_INVALID_KLINES", "true").lower() == "true"

BINANCE_BATCH = env_int("BINANCE_STREAMS_PER_CONNECTION", 180)
GATE_BATCH = env_int("GATE_SYMBOLS_PER_CONNECTION", 300)
MEXC_BATCH = env_int("MEXC_SYMBOLS_PER_CONNECTION", 25)
KUCOIN_TOPIC_BATCH = min(env_int("KUCOIN_SYMBOLS_PER_TOPIC", 100), 100)
KUCOIN_TOPICS_PER_CONNECTION = env_int("KUCOIN_TOPICS_PER_CONNECTION", 20)
BITGET_SYMBOLS_PER_CONNECTION = min(env_int("BITGET_SYMBOLS_PER_CONNECTION", 45), 50)

EXCLUDED_BASES = {
    x.strip().upper()
    for x in os.getenv(
        "EXCLUDED_BASES",
        "USDC,FDUSD,TUSD,DAI,USDP,EUR,EURT,USDE,USD1,BUSD,PAX"
    ).split(",")
    if x.strip()
}


@dataclass
class Trade:
    exchange: str
    symbol: str
    price: float
    quantity: float
    side: str
    timestamp_ms: int

    @property
    def quote_value(self) -> float:
        return self.price * self.quantity


@dataclass
class HourAccumulator:
    hour_start_ms: int
    first_trade_ms: int = 0
    total_volume: float = 0.0
    buy_volume: float = 0.0
    trade_count: int = 0
    max_trade_value: float = 0.0

    def add(self, trade: Trade) -> None:
        if self.first_trade_ms == 0:
            self.first_trade_ms = trade.timestamp_ms
        value = trade.quote_value
        self.total_volume += value
        if trade.side == "buy":
            self.buy_volume += value
        self.trade_count += 1
        if value > self.max_trade_value:
            self.max_trade_value = value



def _read_varint(data: bytes, pos: int):
    value = 0
    shift = 0
    while pos < len(data):
        b = data[pos]
        pos += 1
        value |= (b & 0x7F) << shift
        if not (b & 0x80):
            return value, pos
        shift += 7
        if shift > 70:
            raise ValueError("Invalid protobuf varint")
    raise ValueError("Unexpected end of protobuf data")


def _parse_fields(data: bytes):
    """Parse basic protobuf wire fields into (field_number, wire_type, value)."""
    pos = 0
    fields = []
    while pos < len(data):
        key, pos = _read_varint(data, pos)
        field_no = key >> 3
        wire_type = key & 0x07

        if wire_type == 0:
            value, pos = _read_varint(data, pos)
        elif wire_type == 1:
            value = data[pos:pos + 8]
            pos += 8
        elif wire_type == 2:
            size, pos = _read_varint(data, pos)
            value = data[pos:pos + size]
            pos += size
        elif wire_type == 5:
            value = data[pos:pos + 4]
            pos += 4
        else:
            raise ValueError(f"Unsupported protobuf wire type: {wire_type}")

        fields.append((field_no, wire_type, value))
    return fields


def decode_mexc_aggre_deals(raw: bytes):
    """
    Decode MEXC PushDataV3ApiWrapper without generated protobuf files.
    Returns (symbol, deals), where each deal is (price, quantity, trade_type, time_ms).
    """
    symbol = ""
    body = None

    for field_no, wire_type, value in _parse_fields(raw):
        if field_no == 3 and wire_type == 2:
            symbol = value.decode("utf-8", errors="ignore")
        elif field_no == 314 and wire_type == 2:
            body = value

    if not symbol or body is None:
        return "", []

    deals = []
    for field_no, wire_type, value in _parse_fields(body):
        if field_no != 1 or wire_type != 2:
            continue

        price = ""
        quantity = ""
        trade_type = 0
        timestamp = 0

        for item_no, item_wire, item_value in _parse_fields(value):
            if item_no == 1 and item_wire == 2:
                price = item_value.decode("utf-8", errors="ignore")
            elif item_no == 2 and item_wire == 2:
                quantity = item_value.decode("utf-8", errors="ignore")
            elif item_no == 3 and item_wire == 0:
                trade_type = int(item_value)
            elif item_no == 4 and item_wire == 0:
                timestamp = int(item_value)

        if price and quantity:
            deals.append((price, quantity, trade_type, timestamp))

    return symbol, deals


class Telegram:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def send(self, text: str) -> None:
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning("Telegram variables missing. Alert:\n%s", text)
            return
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        async with self.session.post(
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
        ) as response:
            body = await response.text()
            if response.status >= 400:
                raise RuntimeError(f"Telegram {response.status}: {body}")


class Detector:
    def __init__(self, telegram: Telegram):
        self.telegram = telegram
        self.trades: Dict[str, Deque[Trade]] = defaultdict(deque)
        self.last_alert: Dict[str, float] = {}
        self.hourly_alerts: Dict[str, Deque[float]] = defaultdict(deque)
        self.vol24h: Dict[str, float] = {}
        self.change24h: Dict[str, float] = {}
        self.lock = asyncio.Lock()
        self.candle_cache: Dict[str, tuple] = {}
        self.hour_accumulators: Dict[str, HourAccumulator] = {}
        self.minute_accumulators: Dict[str, HourAccumulator] = {}
        self.invalid_candle_symbols: set[str] = set()
        self.logged_invalid_candle_symbols: set[str] = set()

    @staticmethod
    def key(exchange: str, symbol: str) -> str:
        return f"{exchange}:{symbol}"

    def update_ticker(self, exchange: str, symbol: str, volume: float, change: float) -> None:
        key = self.key(exchange, symbol)
        self.vol24h[key] = max(volume, 0.0)
        self.change24h[key] = change

    async def fetch_official_candles(self, exchange: str, symbol: str, timeframe: str):
        cache_key = f"{self.key(exchange, symbol)}:{timeframe}"
        if cache_key in self.invalid_candle_symbols:
            return []

        now = time.time()
        cached = self.candle_cache.get(cache_key)
        if cached and now - cached[0] < CANDLE_REQUEST_COOLDOWN_SECONDS:
            return cached[1]

        if timeframe not in ("1m", "15m"):
            raise ValueError("Supported timeframes are 1m and 15m only")

        session = self.telegram.session
        candles = []

        gate_interval = timeframe
        mexc_interval = timeframe
        kucoin_type = "1min" if timeframe == "1m" else "15min"
        bitget_granularity = "1min" if timeframe == "1m" else "15min"

        limit = max(
            CANDLE_LIMIT,
            VOLUME_BASELINE_1M_CANDLES + 2 if timeframe == "1m"
            else VOLUME_BASELINE_CANDLES + 2,
        )

        try:
            if exchange == "gateio":
                url = "https://api.gateio.ws/api/v4/spot/candlesticks"
                params = {"currency_pair": symbol, "interval": gate_interval, "limit": limit}
                async with session.get(url, params=params) as r:
                    r.raise_for_status()
                    rows = await r.json()
                for x in rows:
                    candles.append({
                        "ts": int(float(x[0])) * 1000,
                        "open": float(x[5]),
                        "high": float(x[3]),
                        "low": float(x[4]),
                        "close": float(x[2]),
                        "base_volume": float(x[6]) if len(x) > 6 else 0.0,
                        "quote_volume": float(x[1]),
                    })

            elif exchange == "mexc":
                url = "https://api.mexc.com/api/v3/klines"
                params = {"symbol": symbol, "interval": mexc_interval, "limit": limit}
                async with session.get(url, params=params) as r:
                    if r.status == 400 and MEXC_SKIP_INVALID_KLINES:
                        self.invalid_candle_symbols.add(cache_key)
                        if cache_key not in self.logged_invalid_candle_symbols:
                            body = await r.text()
                            log.info(
                                "MEXC skipped unsupported %s kline symbol %s: HTTP 400 %s",
                                timeframe,
                                symbol,
                                body[:200],
                            )
                            self.logged_invalid_candle_symbols.add(cache_key)
                        return []
                    r.raise_for_status()
                    rows = await r.json()
                for x in rows:
                    candles.append({
                        "ts": int(x[0]),
                        "open": float(x[1]),
                        "high": float(x[2]),
                        "low": float(x[3]),
                        "close": float(x[4]),
                        "base_volume": float(x[5]),
                        "quote_volume": float(x[7]),
                    })

            elif exchange == "kucoin":
                url = "https://api.kucoin.com/api/v1/market/candles"
                params = {"symbol": symbol, "type": kucoin_type}
                async with session.get(url, params=params) as r:
                    r.raise_for_status()
                    payload = await r.json()
                for x in payload.get("data", [])[:limit]:
                    candles.append({
                        "ts": int(x[0]) * 1000,
                        "open": float(x[1]),
                        "high": float(x[3]),
                        "low": float(x[4]),
                        "close": float(x[2]),
                        "base_volume": float(x[5]),
                        "quote_volume": float(x[6]),
                    })

            elif exchange == "bitget":
                url = "https://api.bitget.com/api/v2/spot/market/candles"
                params = {
                    "symbol": symbol,
                    "granularity": bitget_granularity,
                    "limit": str(limit),
                }
                async with session.get(url, params=params) as r:
                    r.raise_for_status()
                    payload = await r.json()
                if payload.get("code") != "00000":
                    raise RuntimeError(f"Bitget candle error: {payload}")
                for x in payload.get("data", []):
                    candles.append({
                        "ts": int(x[0]),
                        "open": float(x[1]),
                        "high": float(x[2]),
                        "low": float(x[3]),
                        "close": float(x[4]),
                        "base_volume": float(x[5]),
                        "quote_volume": float(x[6]),
                    })

            elif exchange == "binance":
                url = "https://api.binance.com/api/v3/klines"
                params = {"symbol": symbol, "interval": timeframe, "limit": limit}
                async with session.get(url, params=params) as r:
                    r.raise_for_status()
                    rows = await r.json()
                for x in rows:
                    candles.append({
                        "ts": int(x[0]),
                        "open": float(x[1]),
                        "high": float(x[2]),
                        "low": float(x[3]),
                        "close": float(x[4]),
                        "base_volume": float(x[5]),
                        "quote_volume": float(x[7]),
                    })

            candles.sort(key=lambda c: c["ts"])
            self.candle_cache[cache_key] = (now, candles)
            return candles

        except Exception as exc:
            if cache_key not in self.invalid_candle_symbols:
                log.warning(
                    "%s %s official %s candles failed: %s",
                    exchange,
                    symbol,
                    timeframe,
                    exc,
                )
            return []


    @staticmethod
    def official_candle_stats(
        candles,
        timeframe_ms: int,
        baseline_candles: int,
        min_baseline_candles: int,
    ):
        if len(candles) < min_baseline_candles + 1:
            return None

        now_ms = int(time.time() * 1000)
        current_start = (now_ms // timeframe_ms) * timeframe_ms
        current = None
        completed = []

        for candle in candles:
            if candle["ts"] == current_start:
                current = candle
            elif candle["ts"] < current_start:
                completed.append(candle)

        if current is None:
            return None

        baseline = completed[-baseline_candles:]
        if len(baseline) < min_baseline_candles:
            return None

        avg_quote_volume = sum(c["quote_volume"] for c in baseline) / len(baseline)
        if avg_quote_volume <= 0:
            return None

        current_volume = current["quote_volume"]
        elapsed_seconds = max((now_ms - current["ts"]) / 1000, 1)
        timeframe_seconds = timeframe_ms / 1000
        elapsed_fraction = min(elapsed_seconds / timeframe_seconds, 1.0)

        raw_spike = current_volume / avg_quote_volume
        projected_volume = current_volume / elapsed_fraction
        pace_spike = projected_volume / avg_quote_volume

        return {
            "current": current,
            "baseline_count": len(baseline),
            "avg_quote_volume": avg_quote_volume,
            "current_quote_volume": current_volume,
            "volume_spike": raw_spike,
            "volume_change": (raw_spike - 1) * 100,
            "pace_volume_spike": pace_spike,
            "pace_volume_change": (pace_spike - 1) * 100,
            "projected_volume": projected_volume,
            "elapsed_minutes": elapsed_seconds / 60,
            "change": (
                ((current["close"] / current["open"]) - 1) * 100
                if current["open"] else 0.0
            ),
        }



    async def add_trade(self, trade: Trade) -> None:
        if trade.price <= 0 or trade.quantity <= 0:
            return

        now_ms = int(time.time() * 1000)
        retention_seconds = max(15 * 60, WINDOW_SECONDS)
        cutoff = now_ms - retention_seconds * 1000
        key = self.key(trade.exchange, trade.symbol)

        async with self.lock:
            window = self.trades[key]
            window.append(trade)
            while window and window[0].timestamp_ms < cutoff:
                window.popleft()

            if len(window) < 2:
                return

            current_15m_start_ms = (now_ms // 900_000) * 900_000
            current_1m_start_ms = (now_ms // 60_000) * 60_000

            acc15 = self.hour_accumulators.get(key)
            if acc15 is None or acc15.hour_start_ms != current_15m_start_ms:
                acc15 = HourAccumulator(hour_start_ms=current_15m_start_ms)
                self.hour_accumulators[key] = acc15

            acc1 = self.minute_accumulators.get(key)
            if acc1 is None or acc1.hour_start_ms != current_1m_start_ms:
                acc1 = HourAccumulator(hour_start_ms=current_1m_start_ms)
                self.minute_accumulators[key] = acc1

            if trade.timestamp_ms >= current_15m_start_ms:
                acc15.add(trade)
            if trade.timestamp_ms >= current_1m_start_ms:
                acc1.add(trade)

            buy_pct_1m_1m = (
                acc1.buy_volume / acc1.total_volume * 100
                if acc1.total_volume > 0 else 0.0
            )
            ws_start_delay_1m = (
                (acc1.first_trade_ms - current_1m_start_ms) / 1000
                if acc1.first_trade_ms else 999999
            )

            if ws_start_delay_1m > MAX_WS_START_DELAY_SECONDS:
                return
            if acc1.trade_count < MIN_TRADES_1M:
                return
            if buy_pct_1m_1m < MIN_BUY_PERCENT:
                return

            candles_15m, candles_1m = await asyncio.gather(
                self.fetch_official_candles(trade.exchange, trade.symbol, PRIMARY_TIMEFRAME),
                self.fetch_official_candles(trade.exchange, trade.symbol, CONFIRM_TIMEFRAME),
            )

            official15 = self.official_candle_stats(
                candles_15m,
                timeframe_ms=900_000,
                baseline_candles=VOLUME_BASELINE_CANDLES,
                min_baseline_candles=MIN_VOLUME_BASELINE_CANDLES,
            )
            official1 = self.official_candle_stats(
                candles_1m,
                timeframe_ms=60_000,
                baseline_candles=VOLUME_BASELINE_1M_CANDLES,
                min_baseline_candles=MIN_VOLUME_BASELINE_1M_CANDLES,
            )
            if not official15 or not official1:
                return

            change_15m = official15["change"]
            volume_15m = official15["current_quote_volume"]
            avg_volume_15m = official15["avg_quote_volume"]
            spike_15m = official15["volume_spike"]
            spike_15m_pct = official15["volume_change"]
            pace_spike_15m = official15["pace_volume_spike"]
            projected_15m = official15["projected_volume"]
            elapsed_15m = official15["elapsed_minutes"]

            change_1m = official1["change"]
            volume_1m = official1["current_quote_volume"]
            avg_volume_1m = official1["avg_quote_volume"]
            spike_1m = official1["volume_spike"]
            spike_1m_pct = official1["volume_change"]
            pace_spike_1m = official1["pace_volume_spike"]
            projected_1m = official1["projected_volume"]
            elapsed_1m = official1["elapsed_minutes"]

            last_price = official1["current"]["close"]

            if change_15m < MIN_CHANGE_15M:
                return
            if volume_15m < MIN_VOLUME_15M_USDT:
                return
            if avg_volume_15m < MIN_AVERAGE_VOLUME_USDT:
                return
            if spike_15m < MIN_VOLUME_SPIKE:
                return

            if change_1m < MIN_CHANGE_1M_CONFIRM:
                return
            if volume_1m < MIN_VOLUME_1M_USDT:
                return
            if avg_volume_1m < MIN_AVERAGE_VOLUME_1M_USDT:
                return
            if spike_1m < MIN_VOLUME_SPIKE_1M:
                return

            # Validate the 1m WebSocket total against the official open 1m candle.
            ws_1m_ratio = (
                acc1.total_volume / volume_1m
                if volume_1m > 0 else 0.0
            )
            if ws_1m_ratio < 0.80 or ws_1m_ratio > 1.20:
                return

            volume24 = self.vol24h.get(key, 0.0)
            change24 = self.change24h.get(key, 0.0)
            if volume24 and volume24 < MIN_24H_VOLUME_USDT:
                return
            if change24 > MAX_24H_CHANGE:
                return

            now = time.time()
            last_alert = self.last_alert.get(key, 0.0)
            if now - last_alert < ALERT_COOLDOWN_MINUTES * 60:
                return

            hourly = self.hourly_alerts[key]
            while hourly and now - hourly[0] > 3600:
                hourly.popleft()
            if len(hourly) >= MAX_ALERTS_PER_COIN_PER_HOUR:
                return

            self.last_alert[key] = now
            hourly.append(now)

            sell_volume = max(acc1.total_volume - acc1.buy_volume, 0.0)
            net_buy_flow = acc1.buy_volume - sell_volume
            max_trade_value = acc1.max_trade_value
            whale_detected = max_trade_value >= WHALE_TRADE_USDT

            # Transparent heuristic score. It is not a guaranteed success probability.
            score = 0
            reasons = []

            price_points = min(max(change_15m / 3.0 * 20, 0), 20)
            score += price_points
            if change_15m >= 1.0:
                reasons.append("✅ اختراق سعري سريع")

            if official15["baseline_count"] >= MIN_VOLUME_BASELINE_CANDLES and spike_15m > 0:
                spike_points = min(spike_15m / 5.0 * 25, 25)
                score += spike_points
                if spike_15m >= 3:
                    reasons.append("✅ فوليوم أعلى من المتوسط بأكثر من 3x")
                elif spike_15m >= 2:
                    reasons.append("✅ تضخم ملحوظ في الفوليوم")
            else:
                score += 5

            buy_points = min(max((buy_pct_1m - 50) / 20 * 20, 0), 20)
            score += buy_points
            if buy_pct_1m >= 60:
                reasons.append("✅ ضغط شراء قوي")
            elif buy_pct_1m >= MIN_BUY_PERCENT:
                reasons.append("✅ المشترون متفوقون")

            confirm_points = 0
            confirm_points += min(max(change_1m / 1.0 * 5, 0), 5)
            confirm_points += min(max(spike_1m / 4.0 * 5, 0), 5)
            score += confirm_points
            reasons.append("✅ تأكيد فريم الدقيقة تحقق")

            liquidity_points = min(volume24 / 10_000_000 * 15, 15)
            score += liquidity_points
            if volume24 >= 5_000_000:
                reasons.append("✅ سيولة يومية مرتفعة")

            momentum_points = 0
            if change_15m > 0:
                momentum_points += 3
            if change_1m > 0:
                momentum_points += 3
            if net_buy_flow > 0:
                momentum_points += 2
            score += momentum_points

            if whale_detected:
                score = min(score + 5, 100)
                reasons.append(f"🐋 صفقة كبيرة: {max_trade_value:,.0f} USDT")

            score = int(round(min(max(score, 0), 100)))
            if score < MIN_SIGNAL_SCORE:
                return

            if score >= 90:
                score_icon, score_label = "🟢", "ممتازة"
            elif score >= 80:
                score_icon, score_label = "🔵", "قوية جدًا"
            elif score >= 70:
                score_icon, score_label = "🟡", "جيدة"
            else:
                score_icon, score_label = "⚪", "عادية"

            # Heuristic estimate only; clearly labeled as estimated.
            continuation_probability = int(round(min(max(45 + score * 0.45, 50), 90)))
            correction_probability = 100 - continuation_probability

            base = trade.symbol.replace("_", "-").split("-")[0]
            pair = trade.symbol.replace("_", "").replace("-", "")
            exchange_title = {
                "binance": "Binance",
                "gateio": "Gate",
                "mexc": "MEXC",
                "kucoin": "KuCoin",
                "bitget": "Bitget",
            }.get(trade.exchange, trade.exchange)

            tv_exchange = {
                "binance": "BINANCE",
                "gateio": "GATEIO",
                "mexc": "MEXC",
                "kucoin": "KUCOIN",
                "bitget": "BITGET",
            }.get(trade.exchange, trade.exchange.upper())
            tv_symbol = pair
            tradingview_url = f"https://www.tradingview.com/chart/?symbol={tv_exchange}:{tv_symbol}"

            reasons_text = "\n".join(reasons[:6]) if reasons else "✅ تحققت شروط التنبيه الأساسية"
            baseline_text_15m = (
                f"📈 ارتفاع فوليوم 15m: <b>{spike_15m_pct:+,.0f}%</b>\n"
                f"🔥 Volume Spike 15m: <b>{spike_15m:.2f}x</b>\n"
                f"⏱ المتوقع عند الإغلاق 15m: <b>{pace_spike_15m:.2f}x</b>\n"
                f"📏 متوسط 15m: <b>{avg_volume_15m:,.0f} USDT</b> "
                f"({official15['baseline_count']} شموع مغلقة)\n"
            )
            baseline_text_1m = (
                f"📈 ارتفاع فوليوم 1m: <b>{spike_1m_pct:+,.0f}%</b>\n"
                f"🔥 Volume Spike 1m: <b>{spike_1m:.2f}x</b>\n"
                f"⏱ المتوقع عند الإغلاق 1m: <b>{pace_spike_1m:.2f}x</b>\n"
                f"📏 متوسط 1m: <b>{avg_volume_1m:,.0f} USDT</b> "
                f"({official1['baseline_count']} شموع مغلقة)\n"
            )

            chart_text = f"\n📈 <a href=\"{tradingview_url}\">فتح الشارت على TradingView</a>\n" if SHOW_TRADINGVIEW_LINK else "\n"

            message = (
                "🚀 <b>HYPE COINS BOT — 1m + 15m</b>\n\n"
                f"{score_icon} <b>Signal Score: {score}/100 — {score_label}</b>\n\n"
                f"🪙 <b>{html.escape(base)}</b> | <code>{html.escape(pair)}</code>\n"
                f"🏦 المنصة: <b>{exchange_title}</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                f"💰 السعر: <code>{last_price:.12g}</code> USDT\n\n"
                "⚡ <b>تأكيد الفريمات</b>\n"
                f"• 1m: <b>{change_1m:+.2f}%</b> ✅\n"
                f"• 15m: <b>{change_15m:+.2f}%</b> ✅\n"
                f"• 24h: <b>{change24:+.2f}%</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "💵 <b>شمعة 15m المفتوحة — الفريم الأساسي</b>\n"
                f"• الفوليوم الحالي: <b>{volume_15m:,.0f} USDT</b>\n"
                f"• مضى من الشمعة: <b>{elapsed_15m:.1f} دقيقة</b>\n"
                f"• المتوقع عند الإغلاق: <b>{projected_15m:,.0f} USDT</b>\n"
                f"{baseline_text_15m}"
                "━━━━━━━━━━━━━━━━━━\n"
                "⚡ <b>شمعة 1m المفتوحة — تأكيد الدخول</b>\n"
                f"• الفوليوم الحالي: <b>{volume_1m:,.0f} USDT</b>\n"
                f"• مضى من الشمعة: <b>{elapsed_1m:.1f} دقيقة</b>\n"
                f"• المتوقع عند الإغلاق: <b>{projected_1m:,.0f} USDT</b>\n"
                f"{baseline_text_1m}"
                "━━━━━━━━━━━━━━━━━━\n"
                "🟢 <b>ضغط الشراء — الدقيقة الحالية</b>\n"
                f"• Buy Volume: <b>{acc1.buy_volume:,.0f} USDT</b>\n"
                f"• Sell Volume: <b>{sell_volume:,.0f} USDT</b>\n"
                f"• Buy Ratio: <b>{buy_pct_1m:.1f}%</b>\n"
                f"• Net Buy Flow: <b>{net_buy_flow:+,.0f} USDT</b>\n"
                + (f"• 🐋 أكبر صفقة: <b>{max_trade_value:,.0f} USDT</b>\n" if whale_detected else "")
                + "━━━━━━━━━━━━━━━━━━\n"
                "📊 <b>السوق</b>\n"
                f"• تداول 24h: <b>{volume24:,.0f} USDT</b>\n"
                f"• صفقات الدقيقة: <b>{acc1.trade_count:,}</b>\n"
                f"• تغطية WebSocket للدقيقة: <b>{ws_1m_ratio * 100:.1f}%</b>\n"
                f"• تنبيهات الساعة: <b>{len(hourly)}</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "🧾 <b>أسباب الإشارة</b>\n"
                f"{reasons_text}\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "🎯 <b>تقدير الحركة</b>\n"
                f"• استمرار الصعود: <b>{continuation_probability}%</b>\n"
                f"• احتمال التصحيح: <b>{correction_probability}%</b>\n"
                "⚠️ <i>تقدير حسابي، وليس ضمانًا للربح.</i>\n"
                f"{chart_text}"
                f"🕒 {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())} UTC"
            )



        try:
            await self.telegram.send(message)
            log.info("Alert sent: %s %s", trade.exchange, trade.symbol)
        except Exception:
            log.exception("Telegram alert failed")


async def retry_loop(name: str, runner, stop: asyncio.Event) -> None:
    delay = 3
    while not stop.is_set():
        try:
            await runner()
            delay = 3
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("%s disconnected: %s; reconnecting in %ss", name, exc, delay)
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, 60)


class BinanceAdapter:
    REST = "https://api.binance.com"
    WS = "wss://stream.binance.com:9443/stream?streams="

    def __init__(self, session, detector, stop):
        self.session, self.detector, self.stop = session, detector, stop
        self.symbols: List[str] = []

    async def load_symbols_and_tickers(self):
        async with self.session.get(f"{self.REST}/api/v3/exchangeInfo") as r:
            if r.status == 451:
                raise RuntimeError("Binance blocked this Railway region (HTTP 451)")
            r.raise_for_status()
            data = await r.json()
        self.symbols = [
            s["symbol"] for s in data["symbols"]
            if s.get("status") == "TRADING"
            and s.get("quoteAsset") == "USDT"
            and s.get("baseAsset", "").upper() not in EXCLUDED_BASES
            and s.get("isSpotTradingAllowed", True)
        ]
        await self.refresh_tickers()
        log.info("Binance: %d symbols", len(self.symbols))

    async def refresh_tickers(self):
        async with self.session.get(f"{self.REST}/api/v3/ticker/24hr") as r:
            r.raise_for_status()
            rows = await r.json()
        allowed = set(self.symbols)
        for x in rows:
            if x.get("symbol") not in allowed:
                continue
            self.detector.update_ticker(
                "binance", x["symbol"],
                float(x.get("quoteVolume") or 0),
                float(x.get("priceChangePercent") or 0),
            )

    async def ticker_loop(self):
        while not self.stop.is_set():
            try:
                await self.refresh_tickers()
            except Exception as exc:
                log.warning("Binance ticker refresh: %s", exc)
            await asyncio.sleep(TICKER_REFRESH_SECONDS)

    async def connection(self, symbols: List[str]):
        streams = "/".join(f"{s.lower()}@aggTrade" for s in symbols)
        async with websockets.connect(
            self.WS + streams, ping_interval=20, ping_timeout=20,
            close_timeout=5, max_queue=100_000
        ) as ws:
            async for raw in ws:
                msg = json.loads(raw)["data"]
                price, qty = float(msg["p"]), float(msg["q"])
                # m=true means buyer is maker, therefore aggressive/taker side is sell.
                side = "sell" if msg.get("m") else "buy"
                await self.detector.add_trade(
                    Trade("binance", msg["s"], price, qty, side, int(msg["T"]))
                )

    async def run(self):
        await self.load_symbols_and_tickers()
        tasks = [
            asyncio.create_task(retry_loop(
                f"binance-{i}", lambda b=batch: self.connection(b), self.stop
            ))
            for i, batch in enumerate(chunks(self.symbols, BINANCE_BATCH))
        ]
        tasks.append(asyncio.create_task(self.ticker_loop()))
        await asyncio.gather(*tasks)


class GateAdapter:
    REST = "https://api.gateio.ws/api/v4"
    WS = "wss://api.gateio.ws/ws/v4/"

    def __init__(self, session, detector, stop):
        self.session, self.detector, self.stop = session, detector, stop
        self.symbols: List[str] = []

    async def load_symbols_and_tickers(self):
        async with self.session.get(f"{self.REST}/spot/currency_pairs") as r:
            r.raise_for_status()
            rows = await r.json()
        self.symbols = [
            x["id"] for x in rows
            if x.get("quote") == "USDT"
            and x.get("base", "").upper() not in EXCLUDED_BASES
            and x.get("trade_status") == "tradable"
        ]
        await self.refresh_tickers()
        log.info("Gate: %d symbols", len(self.symbols))

    async def refresh_tickers(self):
        async with self.session.get(f"{self.REST}/spot/tickers") as r:
            r.raise_for_status()
            rows = await r.json()
        allowed = set(self.symbols)
        for x in rows:
            symbol = x.get("currency_pair")
            if symbol not in allowed:
                continue
            self.detector.update_ticker(
                "gateio", symbol,
                float(x.get("quote_volume") or 0),
                float(x.get("change_percentage") or 0),
            )

    async def ticker_loop(self):
        while not self.stop.is_set():
            try:
                await self.refresh_tickers()
            except Exception as exc:
                log.warning("Gate ticker refresh: %s", exc)
            await asyncio.sleep(TICKER_REFRESH_SECONDS)

    async def connection(self, symbols: List[str]):
        async with websockets.connect(
            self.WS, ping_interval=20, ping_timeout=20,
            close_timeout=5, max_queue=100_000
        ) as ws:
            await ws.send(json.dumps({
                "time": int(time.time()),
                "channel": "spot.trades",
                "event": "subscribe",
                "payload": symbols,
            }))
            async for raw in ws:
                msg = json.loads(raw)
                if msg.get("channel") != "spot.trades" or msg.get("event") != "update":
                    continue
                x = msg["result"]
                await self.detector.add_trade(Trade(
                    "gateio", x["currency_pair"],
                    float(x["price"]), float(x["amount"]),
                    x["side"], int(float(x.get("create_time_ms", time.time() * 1000))),
                ))

    async def run(self):
        await self.load_symbols_and_tickers()
        tasks = [
            asyncio.create_task(retry_loop(
                f"gate-{i}", lambda b=batch: self.connection(b), self.stop
            ))
            for i, batch in enumerate(chunks(self.symbols, GATE_BATCH))
        ]
        tasks.append(asyncio.create_task(self.ticker_loop()))
        await asyncio.gather(*tasks)


class MexcAdapter:
    REST = "https://api.mexc.com"
    WS = "wss://wbs-api.mexc.com/ws"

    def __init__(self, session, detector, stop):
        self.session, self.detector, self.stop = session, detector, stop
        self.symbols: List[str] = []

    async def load_symbols_and_tickers(self):
        async with self.session.get(f"{self.REST}/api/v3/exchangeInfo") as r:
            r.raise_for_status()
            data = await r.json()
        self.symbols = []
        for s in data["symbols"]:
            symbol = s.get("symbol", "")
            base = s.get("baseAsset", "").upper()

            if s.get("status") not in ("1", "ENABLED"):
                continue
            if s.get("quoteAsset") != "USDT":
                continue
            if base in EXCLUDED_BASES:
                continue
            if not s.get("isSpotTradingAllowed", True):
                continue
            if "(" in symbol or ")" in symbol:
                continue

            self.symbols.append(symbol)
        await self.refresh_tickers()
        log.info("MEXC: %d symbols", len(self.symbols))

    async def refresh_tickers(self):
        async with self.session.get(f"{self.REST}/api/v3/ticker/24hr") as r:
            r.raise_for_status()
            rows = await r.json()
        allowed = set(self.symbols)
        for x in rows:
            symbol = x.get("symbol")
            if symbol not in allowed:
                continue
            self.detector.update_ticker(
                "mexc", symbol,
                float(x.get("quoteVolume") or 0),
                float(x.get("priceChangePercent") or 0),
            )

    async def ticker_loop(self):
        while not self.stop.is_set():
            try:
                await self.refresh_tickers()
            except Exception as exc:
                log.warning("MEXC ticker refresh: %s", exc)
            await asyncio.sleep(TICKER_REFRESH_SECONDS)

    async def ping_loop(self, ws):
        while True:
            await asyncio.sleep(20)
            await ws.send(json.dumps({"method": "PING"}))

    async def connection(self, symbols: List[str]):
        async with websockets.connect(
            self.WS, ping_interval=None, close_timeout=5,
            max_queue=100_000, max_size=8 * 1024 * 1024
        ) as ws:
            params = [
                f"spot@public.aggre.deals.v3.api.pb@100ms@{s}"
                for s in symbols
            ]
            await ws.send(json.dumps({"method": "SUBSCRIPTION", "params": params}))
            pinger = asyncio.create_task(self.ping_loop(ws))
            try:
                async for raw in ws:
                    if isinstance(raw, str):
                        continue
                    symbol, deals = decode_mexc_aggre_deals(raw)
                    if not symbol or not deals:
                        continue
                    for price, quantity, trade_type, timestamp in deals:
                        side = "buy" if trade_type == 1 else "sell"
                        await self.detector.add_trade(Trade(
                            "mexc", symbol, float(price), float(quantity),
                            side, int(timestamp),
                        ))
            finally:
                pinger.cancel()

    async def run(self):
        await self.load_symbols_and_tickers()
        tasks = [
            asyncio.create_task(retry_loop(
                f"mexc-{i}", lambda b=batch: self.connection(b), self.stop
            ))
            for i, batch in enumerate(chunks(self.symbols, MEXC_BATCH))
        ]
        tasks.append(asyncio.create_task(self.ticker_loop()))
        await asyncio.gather(*tasks)


class KucoinAdapter:
    REST = "https://api.kucoin.com"

    def __init__(self, session, detector, stop):
        self.session, self.detector, self.stop = session, detector, stop
        self.symbols: List[str] = []

    async def load_symbols_and_tickers(self):
        async with self.session.get(f"{self.REST}/api/v2/symbols") as r:
            r.raise_for_status()
            data = await r.json()
        self.symbols = [
            s["symbol"] for s in data["data"]
            if s.get("enableTrading")
            and s.get("quoteCurrency") == "USDT"
            and s.get("baseCurrency", "").upper() not in EXCLUDED_BASES
        ]
        await self.refresh_tickers()
        log.info("KuCoin: %d symbols", len(self.symbols))

    async def refresh_tickers(self):
        async with self.session.get(f"{self.REST}/api/v1/market/allTickers") as r:
            r.raise_for_status()
            data = await r.json()
        allowed = set(self.symbols)
        for x in data["data"]["ticker"]:
            symbol = x.get("symbol")
            if symbol not in allowed:
                continue
            last = float(x.get("last") or 0)
            volume = float(x.get("vol") or 0) * last
            change = float(x.get("changeRate") or 0) * 100
            self.detector.update_ticker("kucoin", symbol, volume, change)

    async def ticker_loop(self):
        while not self.stop.is_set():
            try:
                await self.refresh_tickers()
            except Exception as exc:
                log.warning("KuCoin ticker refresh: %s", exc)
            await asyncio.sleep(TICKER_REFRESH_SECONDS)

    async def get_ws_endpoint(self):
        async with self.session.post(f"{self.REST}/api/v1/bullet-public") as r:
            r.raise_for_status()
            data = (await r.json())["data"]
        server = data["instanceServers"][0]
        url = f'{server["endpoint"]}?token={data["token"]}&connectId={uuid.uuid4().hex}'
        return url, int(server.get("pingInterval", 18000)) / 1000

    async def connection(self, topic_groups: List[List[str]]):
        url, ping_seconds = await self.get_ws_endpoint()
        async with websockets.connect(
            url, ping_interval=None, close_timeout=5, max_queue=100_000
        ) as ws:
            for group in topic_groups:
                topic = "/market/match:" + ",".join(group)
                await ws.send(json.dumps({
                    "id": str(int(time.time() * 1000)),
                    "type": "subscribe",
                    "topic": topic,
                    "response": True,
                }))

            async def pinger():
                while True:
                    await asyncio.sleep(max(ping_seconds * 0.8, 5))
                    await ws.send(json.dumps({
                        "id": str(int(time.time() * 1000)),
                        "type": "ping",
                    }))

            ping_task = asyncio.create_task(pinger())
            try:
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("type") != "message" or not msg.get("topic", "").startswith("/market/match:"):
                        continue
                    x = msg["data"]
                    await self.detector.add_trade(Trade(
                        "kucoin", x["symbol"], float(x["price"]), float(x["size"]),
                        x["side"], int(x["time"]) // 1_000_000,
                    ))
            finally:
                ping_task.cancel()

    async def run(self):
        await self.load_symbols_and_tickers()
        topic_groups = list(chunks(self.symbols, KUCOIN_TOPIC_BATCH))
        connection_groups = list(chunks(topic_groups, KUCOIN_TOPICS_PER_CONNECTION))
        tasks = [
            asyncio.create_task(retry_loop(
                f"kucoin-{i}", lambda g=group: self.connection(g), self.stop
            ))
            for i, group in enumerate(connection_groups)
        ]
        tasks.append(asyncio.create_task(self.ticker_loop()))
        await asyncio.gather(*tasks)


class BitgetAdapter:
    REST = "https://api.bitget.com"
    WS = "wss://ws.bitget.com/v2/ws/public"

    def __init__(self, session, detector, stop):
        self.session, self.detector, self.stop = session, detector, stop
        self.symbols: List[str] = []

    async def load_symbols_and_tickers(self):
        async with self.session.get(f"{self.REST}/api/v2/spot/public/symbols") as r:
            r.raise_for_status()
            payload = await r.json()
        if payload.get("code") != "00000":
            raise RuntimeError(f"Bitget symbols error: {payload}")

        self.symbols = [
            x["symbol"] for x in payload.get("data", [])
            if x.get("status") == "online"
            and x.get("quoteCoin") == "USDT"
            and x.get("baseCoin", "").upper() not in EXCLUDED_BASES
        ]
        await self.refresh_tickers()
        log.info("Bitget: %d symbols", len(self.symbols))

    async def refresh_tickers(self):
        async with self.session.get(f"{self.REST}/api/v2/spot/market/tickers") as r:
            r.raise_for_status()
            payload = await r.json()
        if payload.get("code") != "00000":
            raise RuntimeError(f"Bitget tickers error: {payload}")

        allowed = set(self.symbols)
        for x in payload.get("data", []):
            symbol = x.get("symbol")
            if symbol not in allowed:
                continue
            volume24 = float(x.get("usdtVolume") or x.get("quoteVolume") or 0)
            change24 = float(x.get("change24h") or 0) * 100
            self.detector.update_ticker("bitget", symbol, volume24, change24)

    async def ticker_loop(self):
        while not self.stop.is_set():
            try:
                await self.refresh_tickers()
            except Exception as exc:
                log.warning("Bitget ticker refresh: %s", exc)
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=TICKER_REFRESH_SECONDS)
            except asyncio.TimeoutError:
                pass

    async def ping_loop(self, ws):
        while True:
            await asyncio.sleep(25)
            await ws.send("ping")

    async def connection(self, symbols: List[str]):
        async with websockets.connect(
            self.WS,
            ping_interval=None,
            close_timeout=5,
            max_queue=100_000,
            max_size=8 * 1024 * 1024,
        ) as ws:
            await ws.send(json.dumps({
                "op": "subscribe",
                "args": [
                    {"instType": "SPOT", "channel": "trade", "instId": symbol}
                    for symbol in symbols
                ],
            }))

            pinger = asyncio.create_task(self.ping_loop(ws))
            try:
                async for raw in ws:
                    if raw == "pong":
                        continue
                    msg = json.loads(raw)

                    if msg.get("event") == "error":
                        raise RuntimeError(
                            f"Bitget subscription error: {msg.get('code')} {msg.get('msg')}"
                        )

                    arg = msg.get("arg", {})
                    if arg.get("channel") != "trade":
                        continue

                    symbol = arg.get("instId", "")
                    for x in msg.get("data", []):
                        side = str(x.get("side") or "").lower()
                        if side not in ("buy", "sell"):
                            continue
                        await self.detector.add_trade(Trade(
                            "bitget",
                            symbol,
                            float(x.get("price") or 0),
                            float(x.get("size") or 0),
                            side,
                            int(x.get("ts") or time.time() * 1000),
                        ))
            finally:
                pinger.cancel()
                await asyncio.gather(pinger, return_exceptions=True)

    async def run(self):
        await self.load_symbols_and_tickers()
        tasks = [
            asyncio.create_task(retry_loop(
                f"bitget-{i}",
                lambda batch=batch: self.connection(batch),
                self.stop,
            ))
            for i, batch in enumerate(chunks(self.symbols, BITGET_SYMBOLS_PER_CONNECTION))
        ]
        tasks.append(asyncio.create_task(self.ticker_loop()))
        await asyncio.gather(*tasks)


async def main():
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        telegram = Telegram(session)
        detector = Detector(telegram)

        if STARTUP_MESSAGE:
            await telegram.send(
                "✅ <b>Hype Coins Bot V12 — 1m + 15m</b>\n"
                f"المنصات: <b>{', '.join(sorted(EXCHANGES))}</b>\n"
                f"الفريم الأساسي: <b>{PRIMARY_TIMEFRAME}</b>\n"
                f"فريم التأكيد: <b>{CONFIRM_TIMEFRAME}</b>\n"
                f"أقل تغير 15m: <b>{MIN_CHANGE_15M}%</b>\n"
                f"أقل تغير 1m: <b>{MIN_CHANGE_1M_CONFIRM}%</b>\n"
                f"أقل Volume Spike 15m: <b>{MIN_VOLUME_SPIKE:.2f}x</b>\n"
                f"أقل Volume Spike 1m: <b>{MIN_VOLUME_SPIKE_1M:.2f}x</b>"
            )


        adapters = []
        if "binance" in EXCHANGES:
            adapters.append(("binance-main", BinanceAdapter(session, detector, stop).run))
        if "gateio" in EXCHANGES or "gate" in EXCHANGES:
            adapters.append(("gate-main", GateAdapter(session, detector, stop).run))
        if "mexc" in EXCHANGES:
            adapters.append(("mexc-main", MexcAdapter(session, detector, stop).run))
        if "kucoin" in EXCHANGES:
            adapters.append(("kucoin-main", KucoinAdapter(session, detector, stop).run))
        if "bitget" in EXCHANGES:
            adapters.append(("bitget-main", BitgetAdapter(session, detector, stop).run))

        tasks = [
            asyncio.create_task(retry_loop(name, run, stop))
            for name, run in adapters
        ]
        await stop.wait()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
