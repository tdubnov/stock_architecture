from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from datetime import timedelta
import pandas as pd
from backtrader.feed import DataBase
from backtrader import date2num, num2date
from backtrader.utils.py3 import queue, with_metaclass
import backtrader as bt
from ts.client import TradeStationClient
from Tradestation_python_api.ts.client import TradeStationClient
from helper import create_logger
import sys
import firebase_admin
from firebase_admin import credentials, firestore
#logger = create_logger(file=f'{sys.argv[1].replace(" ", "_")}_log.log')

import schedule
from copy import deepcopy
from time import sleep
from math import exp
import pandas as pd
import numpy as np
import threading
import requests
import datetime
import logging
import socket
import json
from pqdm.threads import pqdm
from multiprocess import Pool

from config import clients
from tradestation import TradeStation
from helper import CustomErrorHandler, market_open_regular_hours, market_open_after_hours, \
    datetime64_to_date, Budget, get_equity
from email_helper import send_email

from constants import TRADE_HISTORY_COLUMNS, TIMEZONE, HOLIDAYS, FAILED_STATUSES, ALIVE_STATUSES, \
    ORDER_HISTORY_COLUMNS, FILLED_STATUSES, PARTIALLY_FILLED_STATUSES





class TradeStationData(bt.feed.DataBase):


    def __init__(self, client: 'Paper', paper: bool = True,
                 interval: int = 1, unit: str = 'Minute', session: str = 'USEQPreAndPost'):

        super(TradeStationData, self).__init__()

        self.symbol = symbol

        if paper:
            client = 'Paper'

        self.client = client
        self.trade_station = TradeStation(client=client, symbols=self.symbol,
                                          paper=paper, interval=interval, unit=unit, session=session)
        

    def start(self):
        #logger.addHandler(self.handler)


        catchable_signals = set(signal.Signals) - {signal.CTRL_C_EVENT, signal.CTRL_BREAK_EVENT}
        for sig in catchable_signals:
            signal.signal(sig, )



    def _time_to_open(self):

        now = datetime.datetime.now(tz=datetime.timezone.utc)
        if not np.is_busday(now.date(), holidays=HOLIDAYS):
            next_date = datetime64_to_date(np.busday_offset(now.date(), 0, roll='forward', holidays=HOLIDAYS))
        elif now > self.trade_station.nyse.schedule(start_date=now.date(), end_date=now.date()).market_close[0]:
            next_date = datetime64_to_date(np.busday_offset(now.date(), 1, holidays=HOLIDAYS))
        else:
            next_date = now.date()

        next_opening_time = self.trade_station.nyse.schedule(start_date=next_date, end_date=next_date).market_open[0]
        time_till_open = next_opening_time - now - datetime.timedelta(minutes=1)

        return time_till_open

    def _load(self, symbol):

        while True:
            try:
                time_till_open = self._time_to_open()
                if time_till_open.total_seconds() > 0:
                    #logger.info(f'Stream quotes thread sleeping for {time_till_open}')
                    sleep(time_till_open.total_seconds())

                with self.trade_station.ts_client.stream_quotes(list(symbol)) as stream:
                    if stream.status_code != 200:
                        raise Exception(f"Cannot stream quotes (HTTP {stream.status_code}): {stream.text}")

                    print('Stream quotes started')
                    for line in stream.iter_lines():
                        # str_line = line.decode('utf-8')
                        if not line or line == b'':
                            continue

                        decoded_line = json.loads(line)
                        if any([param not in decoded_line for param in ["Symbol", "TradeTime", "Close"]]):
                            continue

                        symbol = decoded_line['Symbol']
                        curr_time = pd.Timestamp(decoded_line['TradeTime']).to_pydatetime().astimezone(TIMEZONE)

                        close = float(decoded_line['Close'])
                        curr_price = round(close, 2)
                        print(f'New price at {curr_time} - {symbol}: ${curr_price}')

                        self.lines.close[0] = curr_price
                        self.lines.datetime[0] = bt.date2num(pd.to_datetime(curr_time))
                        self.lines.high[0] = float(decoded_line['High'])
                        self.lines.low[0] = float(decoded_line['Low'])
                        self.lines.volume[0] = float(decoded_line['TotalVolume'])

                        return True
            except requests.exceptions.ChunkedEncodingError:
                #logger.warning(f'Stream quotes chunked encoding error')
                return

            except Exception:
                #logger.exception(f"Exception in stream quotes")
                return

            print('Stream quotes stopped')
            return True

    def stop(self):
        pass

    def islive(self):
        return True

