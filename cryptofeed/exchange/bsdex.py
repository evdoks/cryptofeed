from cryptofeed.symbols import Symbols
from typing import Tuple, Dict, List, Callable
from decimal import Decimal
import json
import logging
import time

from sortedcontainers import SortedDict as sd

from cryptofeed.connection import AsyncConnection
from cryptofeed.defines import BSDEX, TICKER, BID, ASK, L2_BOOK
from cryptofeed.feed import Feed

LOG = logging.getLogger('feedhandler')
BOOK_DELTA_MAX_DEPTH = 100


class Bsdex(Feed):
    id = BSDEX
    symbol_endpoint = 'https://api-public.bsdex.de/v1/markets'  # non-existing endpoint

    def __init__(self, **kwargs):
        super().__init__(
            address='wss://api.bsdex.de/consumer/ws',
            **kwargs)
        self.__reset()

    def connect(self) -> List[Tuple[AsyncConnection, Callable[[None], None], Callable[[str, float], None]]]:
        ret = []
        for channel in self.subscription:
            ret.append(self._connect_builder(f"{self.address}?access_token={self.config.bsdex.access_token}",
                                             None))

        return ret

    def __reset(self):
        pass

    def _parse_symbol_data(cls, data: dict,
                           symbol_separator: str) -> Tuple[Dict, Dict]:
        return {'BTC-EUR': 'btc-eur'}, {}

    def symbol_mapping(cls, symbol_separator='-', refresh=False) -> Dict:
        syms, info = cls._parse_symbol_data({}, symbol_separator)
        Symbols.set(cls.id, syms, info)
        return syms

    async def _ticker(self, msg):
        """
        {
            "chan_name": "quote",
            "subchan_name": "btc-eur",
            "type": "data",
            "data": {
                "buy_price": "48649.27",
                "buy_volume": "0.01",
                "sell_price": "48701.74",
                "sell_volume": "0.01",
                "market": "btc-eur"
            }
        }
        """
        timestamp = time.time()
        await self.callback(
            TICKER,
            feed=self.id,
            symbol=msg['subchan_name'].upper(),
            bid=Decimal(msg['data']['sell_price']),
            ask=Decimal(msg['data']['buy_price']),
            timestamp=timestamp,
            receipt_timestamp=timestamp,
        )

    async def _book(self, msg):
        """
        {
            "chan_name": "orderbook",
            "subchan_name": "btc-eur",
            "type": "data",
            "data": [
                {
                "price": "0.2",
                "side": "buy",
                "size": "1",
                "market": "btc-eur"
                },
                {
                "price": "1003512.3",
                "side": "sell",
                "size": "0.15",
                "market": "btc-eur"
                }
            ]
        }
        """
        timestamp = time.time()
        delta = {BID: [], ASK: []}
        pair = msg['subchan_name'].upper()
        if pair not in self.l2_book:
            self.l2_book[pair] = {
                BID:
                sd({
                    Decimal(m['price']): Decimal(m['size'])
                    for m in msg['data'] if m['side'] == 'buy'
                }),
                ASK:
                sd({
                    Decimal(m['price']): Decimal(m['size'])
                    for m in msg['data'] if m['side'] == 'sell'
                })
            }
        else:
            for m in msg['data']:
                side = False
                if m['side'] == 'buy':
                    side = BID
                elif m['side'] == 'sell':
                    side = ASK
                if side:
                    price = Decimal(m['price'])
                    size = Decimal(m['size'])
                    if size == 0:
                        if price in self.l2_book[pair][side]:
                            del self.l2_book[pair][side][price]
                            delta[side].append((price, 0))
                    else:
                        delta[side].append((price, size))
                        self.l2_book[pair][side][price] = size

        await self.book_callback(self.l2_book[pair], L2_BOOK, pair, False,
                                 delta, timestamp, timestamp)

    async def subscribe(self, conn: AsyncConnection, options=None):
        self.__reset()
        client_id = 0
        for chan, symbols in self.subscription.items():
            params = {}
            for symbol in symbols:
                if chan == "orderbook":
                    params = {"keep_alive_when_offline": True}
                client_id += 1
                await conn.write(
                    json.dumps({
                        "type": "subscribe",
                        "chan_name": f"{chan}",
                        "subchan_name": f"{symbol}",
                        "params": params
                    }))

    async def message_handler(self, msg, conn, timestamp):
        msg = json.loads(msg, parse_float=Decimal)

        if 'ping' in msg:
            await conn.write(json.dumps({'pong': msg['ping']}))
        elif msg['type'] == 'online':
            return
        elif msg['type'] == 'subscribed':
            return
        elif 'chan_name' in msg and msg['chan_name'] == 'quote' and msg[
                'type'] == 'data':
            await self._ticker(msg)
        elif 'chan_name' in msg and msg['chan_name'] == 'orderbook' and msg[
                'type'] == 'data':
            await self._book(msg)
        elif 'chan_name' in msg and msg['chan_name'] == 'orderbook' and msg[
                'type'] == 'offline':
            # clear orderbook after orderbook channel gets offline
            self.l2_book.pop(msg['subchan_name'].upper(), None)
            LOG.info("%s: Channel %s is online", self.id, msg['chan_name'])
        elif msg['type'] == 'offline':
            LOG.warning("%s: Channel %s is offline", self.id, msg['chan_name'])
        elif msg['type'] == 'online':
            LOG.info("%s: Channel %s is online", self.id, msg['chan_name'])
        else:
            LOG.warning("%s: Invalid message type %s", self.id, msg)
