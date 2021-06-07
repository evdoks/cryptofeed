from cryptofeed.symbols import Symbols
from typing import Tuple, Dict
from decimal import Decimal
import json
import logging

from cryptofeed.connection import AsyncConnection
from cryptofeed.defines import BSDEX, TICKER
from cryptofeed.feed import Feed


LOG = logging.getLogger('feedhandler')


class Bsdex(Feed):
    id = BSDEX
    symbol_endpoint = 'https://api-public.bsdex.de/v1/markets'      # non-existing endpoint

    def __init__(self, **kwargs):
        super().__init__('wss://api.bsdex.de/consumer/ws?access_token=4Mxa789dwcJAw5ONvMzYwnjrIqWlkREC', **kwargs)
        self.__reset()

    def __reset(self):
        pass

    def _parse_symbol_data(cls, data: dict, symbol_separator: str) -> Tuple[Dict, Dict]:
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
        await self.callback(TICKER,
                            feed=self.id,
                            symbol=msg['subchan_name'].upper(),
                            bid=Decimal(msg['data']['sell_price']),
                            ask=Decimal(msg['data']['buy_price']),
                            timestamp=None,
                            receipt_timestamp=None
                            )

    async def subscribe(self, conn: AsyncConnection):
        self.__reset()
        client_id = 0
        for chan, symbols in self.subscription.items():
            for symbol in symbols:
                client_id += 1
                await conn.write(json.dumps(
                    {
                        "type": "subscribe",
                        "chan_name": f"{chan}",
                        "subchan_name": f"{symbol}"
                    }
                ))

    async def message_handler(self, msg, conn, timestamp):
        msg = json.loads(msg, parse_float=Decimal)
        if 'chan_name' in msg and msg['chan_name'] == 'quote':
            if 'type' in msg and msg['type'] == 'subscribed':
                return
            elif 'type' in msg and msg['type'] == 'data':
                await self._ticker(msg)
        else:
            LOG.warning("%s: Invalid message type %s", self.id, msg)
