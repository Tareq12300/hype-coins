import asyncio
import logging
import os
import signal
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Tuple

import ccxt.async_support as ccxt
import httpx
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("coin-sonar")


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


EXCHANGES = [
    x.strip().lower()
    for x in os.getenv("EXCHANGES", "binance,gateio,mexc,kucoin").split(",")
    if x.strip()
]
POLL_SECONDS = env_int("POLL_SECONDS", 15)
MIN_CHANGE_1M = env_float("MIN_CHANGE_1M", 0.7)
MIN_VOLUME_1M_USDT = env_float("MIN_VOLUME_1M_USDT", 50_000)
MIN_BUY_PERCENT = env_float("MIN_BUY_PERCENT", 55)
MIN_24H_VOLUME_USDT = env_float("MIN_24H_VOLUME_USDT", 250_000)
MAX_24H_CHANGE = env_float("MAX_24H_CHANGE", 30)
COOLDOWN_MINUTES = env_int("ALERT_COOLDOWN_MINUTES", 30)
MAX_ALERTS_PER_COIN_PER_HOUR = env_int("MAX_ALERTS_PER_COIN_PER_HOUR", 3)
MAX_CANDIDATES_PER_SCAN = env_int("MAX_CANDIDATES_PER_SCAN", 25)
FETCH_TRADES_LIMIT = env_int("FETCH_TRADES_LIMIT", 1000)
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").lower() == "true"

STABLE_BASES = {
    x.strip().upper()
    for x in os.getenv(
        "EXCLUDED_BASES",
        "USDC,FDUSD,TUSD,DAI,USDP,EUR,EURT,USDE,USD1,BUSD,PAX"
    ).split(",")
    if x.strip()
}


@dataclass
class Snapshot:
    ts: float
    price: float


class Telegram:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.client = httpx.AsyncClient(timeout=20)

    async def send(self, text: str) -> None:
        if not self.token or not self.chat_id:
            log.warning("Telegram credentials are missing; alert printed only:\n%s", text)
            return
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        response = await self.client.post(url, json=payload)
        response.raise_for_status()

    async def close(self) -> None:
        await self.client.aclose()


class ExchangeScanner:
    def __init__(self, exchange_id: str, telegram: Telegram):
        exchange_cls = getattr(ccxt, exchange_id)
        self.exchange = exchange_cls({
            "enableRateLimit": True,
            "timeout": 20_000,
            "options": {"defaultType": "spot"},
        })
        self.exchange_id = exchange_id
        self.telegram = telegram
        self.history: Dict[str, Deque[Snapshot]] = defaultdict(lambda: deque(maxlen=30))
        self.last_alert: Dict[str, float] = {}
        self.alerts_hour: Dict[str, Deque[float]] = defaultdict(deque)
        self.active_symbols: List[str] = []

    async def initialize(self) -> None:
        markets = await self.exchange.load_markets()
        self.active_symbols = [
            symbol
            for symbol, market in markets.items()
            if market.get("spot")
            and market.get("active", True)
            and market.get("quote") == "USDT"
            and market.get("base", "").upper() not in STABLE_BASES
        ]
        log.info("%s: loaded %d active USDT spot pairs", self.exchange_id, len(self.active_symbols))

    @staticmethod
    def ticker_price(ticker: dict) -> Optional[float]:
        value = ticker.get("last") or ticker.get("close")
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def quote_volume(ticker: dict) -> float:
        value = ticker.get("quoteVolume")
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                pass
        base_volume = ticker.get("baseVolume")
        last = ticker.get("last")
        try:
            return float(base_volume or 0) * float(last or 0)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def percentage(ticker: dict) -> float:
        try:
            return float(ticker.get("percentage") or 0)
        except (TypeError, ValueError):
            return 0.0

    def price_ago(self, symbol: str, seconds: int, now: float) -> Optional[float]:
        points = self.history[symbol]
        cutoff = now - seconds
        eligible = [point for point in points if point.ts <= cutoff]
        return eligible[-1].price if eligible else None

    def can_alert(self, symbol: str, now: float) -> bool:
        last = self.last_alert.get(symbol, 0)
        if now - last < COOLDOWN_MINUTES * 60:
            return False

        hourly = self.alerts_hour[symbol]
        while hourly and now - hourly[0] > 3600:
            hourly.popleft()
        return len(hourly) < MAX_ALERTS_PER_COIN_PER_HOUR

    async def analyze_trades(self, symbol: str, now_ms: int) -> Tuple[float, float, float, int]:
        """Returns total quote volume, buy quote volume, buy percentage, trade count."""
        since = now_ms - 60_000
        trades = await self.exchange.fetch_trades(
            symbol,
            since=since,
            limit=FETCH_TRADES_LIMIT,
        )
        total = 0.0
        buys = 0.0
        count = 0
        for trade in trades:
            ts = trade.get("timestamp")
            if ts is None or ts < since:
                continue
            price = float(trade.get("price") or 0)
            amount = float(trade.get("amount") or 0)
            cost = trade.get("cost")
            quote = float(cost) if cost is not None else price * amount
            if quote <= 0:
                continue
            total += quote
            if str(trade.get("side", "")).lower() == "buy":
                buys += quote
            count += 1
        buy_pct = (buys / total * 100) if total > 0 else 0.0
        return total, buys, buy_pct, count

    async def fetch_all_tickers(self) -> Dict[str, dict]:
        try:
            tickers = await self.exchange.fetch_tickers()
            return {s: t for s, t in tickers.items() if s in self.active_symbols}
        except Exception as exc:
            log.warning("%s fetch_tickers failed: %s", self.exchange_id, exc)
            return {}

    async def scan_once(self) -> None:
        now = time.time()
        now_ms = int(now * 1000)
        tickers = await self.fetch_all_tickers()
        if not tickers:
            return

        candidates = []
        for symbol, ticker in tickers.items():
            price = self.ticker_price(ticker)
            if not price or price <= 0:
                continue

            self.history[symbol].append(Snapshot(now, price))
            old_price = self.price_ago(symbol, 60, now)
            if not old_price:
                continue

            change_1m = (price / old_price - 1) * 100
            volume_24h = self.quote_volume(ticker)
            change_24h = self.percentage(ticker)

            if (
                change_1m >= MIN_CHANGE_1M
                and volume_24h >= MIN_24H_VOLUME_USDT
                and change_24h <= MAX_24H_CHANGE
                and self.can_alert(symbol, now)
            ):
                candidates.append((change_1m, symbol, price, volume_24h, change_24h))

        candidates.sort(reverse=True)
        for change_1m, symbol, price, volume_24h, change_24h in candidates[:MAX_CANDIDATES_PER_SCAN]:
            try:
                volume_1m, buy_volume, buy_pct, trade_count = await self.analyze_trades(symbol, now_ms)
            except Exception as exc:
                log.warning("%s %s fetch_trades failed: %s", self.exchange_id, symbol, exc)
                continue

            if volume_1m < MIN_VOLUME_1M_USDT or buy_pct < MIN_BUY_PERCENT:
                continue

            sell_volume = max(volume_1m - buy_volume, 0)
            base = symbol.split("/")[0]
            exchange_name = self.exchange.name
            message = (
                f"🚀 <b>ارتفاع مفاجئ</b>\n\n"
                f"🪙 <b>{base}</b> | <code>{symbol.replace('/', '')}</code>\n"
                f"🏦 المنصة: <b>{exchange_name}</b>\n\n"
                f"💰 السعر: <code>{price:.12g}</code>\n"
                f"⚡ تغير آخر دقيقة: <b>+{change_1m:.2f}%</b>\n"
                f"📈 تغير 24 ساعة: <b>{change_24h:+.2f}%</b>\n\n"
                f"💵 تداول آخر دقيقة: <b>{volume_1m:,.0f} USDT</b>\n"
                f"🟢 عمليات الشراء: <b>{buy_volume:,.0f} USDT</b> ({buy_pct:.1f}%)\n"
                f"🔴 عمليات البيع: <b>{sell_volume:,.0f} USDT</b>\n"
                f"🔄 عدد الصفقات المقروءة: <b>{trade_count:,}</b>\n\n"
                f"📊 تداول 24 ساعة: <b>{volume_24h:,.0f} USDT</b>\n"
                f"🔔 تنبيهات العملة خلال الساعة: <b>{len(self.alerts_hour[symbol]) + 1}</b>"
            )
            await self.telegram.send(message)
            self.last_alert[symbol] = now
            self.alerts_hour[symbol].append(now)
            log.info("%s alert sent for %s", self.exchange_id, symbol)

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                if not self.active_symbols:
                    await self.initialize()
                await self.scan_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("%s scanner cycle failed", self.exchange_id)
                await asyncio.sleep(10)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=POLL_SECONDS)
            except asyncio.TimeoutError:
                pass

    async def close(self) -> None:
        await self.exchange.close()


async def main() -> None:
    telegram = Telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    scanners = []
    for exchange_id in EXCHANGES:
        if not hasattr(ccxt, exchange_id):
            log.error("Unsupported CCXT exchange id: %s", exchange_id)
            continue
        scanners.append(ExchangeScanner(exchange_id, telegram))

    if not scanners:
        raise RuntimeError("No valid exchanges configured.")

    if SEND_STARTUP_MESSAGE:
        await telegram.send(
            "✅ <b>Coin Sonar Bot started</b>\n"
            f"المنصات: <b>{', '.join(EXCHANGES)}</b>\n"
            f"شرط ارتفاع الدقيقة: <b>{MIN_CHANGE_1M}%</b>\n"
            f"أقل تداول للدقيقة: <b>{MIN_VOLUME_1M_USDT:,.0f} USDT</b>\n"
            f"أقل نسبة شراء: <b>{MIN_BUY_PERCENT}%</b>"
        )

    tasks = [asyncio.create_task(scanner.run(stop_event)) for scanner in scanners]
    await stop_event.wait()

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await asyncio.gather(*(scanner.close() for scanner in scanners), return_exceptions=True)
    await telegram.close()


if __name__ == "__main__":
    asyncio.run(main())
