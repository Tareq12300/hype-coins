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

from proto.PushDataV3ApiWrapper_pb2 import PushDataV3ApiWrapper

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
    for x in os.getenv("EXCHANGES", "binance,gateio,mexc,kucoin").split(",")
    if x.strip()
}
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

WINDOW_SECONDS = env_int("WINDOW_SECONDS", 60)
MIN_CHANGE_1M = env_float("MIN_CHANGE_1M", 0.7)
MIN_VOLUME_1M_USDT = env_float("MIN_VOLUME_1M_USDT", 50_000)
MIN_BUY_PERCENT = env_float("MIN_BUY_PERCENT", 55)
MIN_24H_VOLUME_USDT = env_float("MIN_24H_VOLUME_USDT", 250_000)
MAX_24H_CHANGE = env_float("MAX_24H_CHANGE", 30)
ALERT_COOLDOWN_MINUTES = env_int("ALERT_COOLDOWN_MINUTES", 30)
MAX_ALERTS_PER_COIN_PER_HOUR = env_int("MAX_ALERTS_PER_COIN_PER_HOUR", 3)
TICKER_REFRESH_SECONDS = env_int("TICKER_REFRESH_SECONDS", 300)
STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").lower() == "true"

BINANCE_BATCH = env_int("BINANCE_STREAMS_PER_CONNECTION", 180)
GATE_BATCH = env_int("GATE_SYMBOLS_PER_CONNECTION", 300)
MEXC_BATCH = env_int("MEXC_SYMBOLS_PER_CONNECTION", 25)
KUCOIN_TOPIC_BATCH = min(env_int("KUCOIN_SYMBOLS_PER_TOPIC", 100), 100)
KUCOIN_TOPICS_PER_CONNECTION = env_int("KUCOIN_TOPICS_PER_CONNECTION", 20)

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

    @staticmethod
    def key(exchange: str, symbol: str) -> str:
        return f"{exchange}:{symbol}"

    def update_ticker(self, exchange: str, symbol: str, volume: float, change: float) -> None:
        key = self.key(exchange, symbol)
        self.vol24h[key] = max(volume, 0.0)
        self.change24h[key] = change

    async def add_trade(self, trade: Trade) -> None:
        if trade.price <= 0 or trade.quantity <= 0:
            return

        now_ms = int(time.time() * 1000)
        cutoff = now_ms - WINDOW_SECONDS * 1000
        key = self.key(trade.exchange, trade.symbol)

        async with self.lock:
            window = self.trades[key]
            window.append(trade)
            while window and window[0].timestamp_ms < cutoff:
                window.popleft()

            if len(window) < 2:
                return

            first_price = window[0].price
            last_price = window[-1].price
            if first_price <= 0:
                return

            change = (last_price / first_price - 1) * 100
            if change < MIN_CHANGE_1M:
                return

            total = sum(t.quote_value for t in window)
            if total < MIN_VOLUME_1M_USDT:
                return

            buys = sum(t.quote_value for t in window if t.side == "buy")
            buy_pct = buys / total * 100 if total else 0
            if buy_pct < MIN_BUY_PERCENT:
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

            sell_volume = max(total - buys, 0.0)
            base = trade.symbol.replace("_", "-").split("-")[0]
            pair = trade.symbol.replace("_", "").replace("-", "")
            exchange_title = {
                "binance": "Binance",
                "gateio": "Gate",
                "mexc": "MEXC",
                "kucoin": "KuCoin",
            }.get(trade.exchange, trade.exchange)

            message = (
                "🚀 <b>ارتفاع مفاجئ</b>\n\n"
                f"🪙 <b>{html.escape(base)}</b> | <code>{html.escape(pair)}</code>\n"
                f"🏦 المنصة: <b>{exchange_title}</b>\n\n"
                f"💰 السعر: <code>{last_price:.12g}</code>\n"
                f"⚡ تغير آخر {WINDOW_SECONDS} ثانية: <b>+{change:.2f}%</b>\n"
                f"📈 تغير 24 ساعة: <b>{change24:+.2f}%</b>\n\n"
                f"💵 تداول الفترة: <b>{total:,.0f} USDT</b>\n"
                f"🟢 عمليات الشراء: <b>{buys:,.0f} USDT</b> ({buy_pct:.1f}%)\n"
                f"🔴 عمليات البيع: <b>{sell_volume:,.0f} USDT</b>\n"
                f"🔄 الصفقات المستلمة: <b>{len(window):,}</b>\n\n"
                f"📊 تداول 24 ساعة: <b>{volume24:,.0f} USDT</b>\n"
                f"🔔 تنبيهات العملة خلال الساعة: <b>{len(hourly)}</b>"
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
        self.symbols = [
            s["symbol"] for s in data["symbols"]
            if s.get("status") in ("1", "ENABLED")
            and s.get("quoteAsset") == "USDT"
            and s.get("baseAsset", "").upper() not in EXCLUDED_BASES
            and s.get("isSpotTradingAllowed", True)
        ]
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
                    wrapper = PushDataV3ApiWrapper()
                    wrapper.ParseFromString(raw)
                    if not wrapper.HasField("publicAggreDeals"):
                        continue
                    symbol = wrapper.symbol
                    for deal in wrapper.publicAggreDeals.deals:
                        side = "buy" if deal.tradeType == 1 else "sell"
                        await self.detector.add_trade(Trade(
                            "mexc", symbol, float(deal.price), float(deal.quantity),
                            side, int(deal.time),
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
                "✅ <b>Coin Sonar WebSocket V2 started</b>\n"
                f"المنصات: <b>{', '.join(sorted(EXCHANGES))}</b>\n"
                f"نافذة القياس: <b>{WINDOW_SECONDS} ثانية</b>\n"
                f"أقل تغير: <b>{MIN_CHANGE_1M}%</b>\n"
                f"أقل تداول: <b>{MIN_VOLUME_1M_USDT:,.0f} USDT</b>\n"
                f"أقل نسبة شراء: <b>{MIN_BUY_PERCENT}%</b>"
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
