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
logger = create_logger(file=f'{sys.argv[1].replace(" ", "_")}_log.log')

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
        details = clients[client]
        self.socket = socket.socket()  # instantiate
        self.trade_station = TradeStation(client=client, symbols=details['Symbols'],
                                          paper=paper, interval=interval, unit=unit, session=session)

    def start(self):
        details = clients[self.client]
        self.socket.bind(('localhost', details['Port']))
        self.socket.connect(('localhost', 5000))  # connect to the server
        logger.addHandler(self.handler)


        catchable_signals = set(signal.Signals) - {signal.CTRL_C_EVENT, signal.CTRL_BREAK_EVENT}
        for sig in catchable_signals:
            signal.signal(sig, )

        stream_bars_threads = []
        bar_thread = threading.Thread(target=self.stream_bars, args=(symbol, ))
        bar_thread.daemon = False
        stream_bars_threads.append(bar_thread)

        stream_quotes_thread = threading.Thread(target=self.stream_quotes)
        monitor_thread = threading.Thread(target=self.monitor_broker)
        telegram_bot_thread = threading.Thread(target=self.listen_to_server)

        stream_quotes_thread.daemon = False
        monitor_thread.daemon = False
        telegram_bot_thread.daemon = False

        for thread in stream_bars_threads:
            thread.start()
        stream_quotes_thread.start()

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
                    logger.info(f'Stream quotes thread sleeping for {time_till_open}')
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
                logger.warning(f'Stream quotes chunked encoding error')
                return

            except Exception:
                logger.exception(f"Exception in stream quotes")
                return

            print('Stream quotes stopped')
                return True

    def stop(self):
        pass

    def islive(self):
        return True


class MyStrategy(bt.Strategy):


    params = (
        ('symbol', 'a')
        )
    qty_bought = 0
    qty_sold = 0
    threshold_keep = 0
    MyStrategy.budget = Budget()
    MyStrategy.temp_trade_history = {}
    MyStrategy.temp_order_history = {}
    MyStrategy.trade_history = pd.DataFrame(columns=TRADE_HISTORY_COLUMNS + ["latest_update"])
    MyStrategy.sell_trades = pd.DataFrame(columns=TRADE_HISTORY_COLUMNS + ["latest_update"])
    MyStrategy.order_history = pd.DataFrame(columns=ORDER_HISTORY_COLUMNS)
    def __init__(self):
        self.trade_station = TradeStation(client='Paper', paper=True)

    
    def check_buy(self):
        import random
        quantity = random.randint(0, 3)
        threshold = random.uniform(0, 1/3)

        if quantity == 0:
            return 0
        logger.info(f'Attempting to buy {quantity} shares of {symbol} at ${self.data.close}/share')
        response = self.trade_station.submit_market_order(symbol=symbol, qty=quantity, order_type="BUY")
        # response = self.trade_station.submit_limit_order(symbol=symbol, limit_price=curr_price,
        #                                                  qty=quantity, order_type="BUY")
        if response:

            order_id = response["OrderID"]

            status = "purchase_ordered"
            MyStrategy.budget.update_remaining_budget(-self.data.close*quantity, self.trade_station)

            # Update local and db copies of order history
            MyStrategy.order_history.loc[order_id] = [symbol, quantity, "buy", None, self.data.datetime.time(0), None, self.data.close, status]


            # Update local and db copies of trade history
            # symbol, quantity, sell_threshold, purchase_time,
            # sold_time, purchase_price, sold_price, status, latest_update
            MyStrategy.trade_history.loc[order_id] = [symbol, quantity, threshold, self.data.datetime.time(0),
                                                None, curr_price, 0, status, False, self.data.datetime.time(0)]

        return quantity

    def check_sell(self):
        if not market_open_regular_hours():
            return

        open_trades = MyStrategy.trade_history[(MyStrategy.trade_history.symbol == symbol) & (MyStrategy.trade_history.status == "own")]

        if open_trades.empty:
            return

        sell_prices = (1 + open_trades.sell_threshold) * open_trades.purchase_price

        new_trades_above_threshold = open_trades.loc[(sell_prices <= self.data.close) & ~open_trades.above_threshold]
        for trade_id in new_trades_above_threshold.index:
            MyStrategy.trade_history.loc[trade_id, 'above_threshold'] = True

        MyStrategy.sell_trades = open_trades.loc[MyStrategy.trade_history.above_threshold]

        print(f'Checking to sell {symbol}'
              f'\n\t current price: {self.data.close}, trade cutoffs: {sell_prices}')

        self.handle_sell()

    def handle_sell(self):
        if MyStrategy.sell_trades.empty:
            return

        qty = MyStrategy.sell_trades.quantity.sum()

        logger.info(f'Selling {qty} shares of {symbol} at ${self.data.close}/share')
        response = self.trade_station.submit_market_order(symbol=symbol, qty=qty, order_type="SELL")

        total_sell_quantity = int(MyStrategy.sell_trades.quantity.sum())
        if response:
            order_id = response["OrderID"]

            for trade_id in MyStrategy.sell_trades.index:
                self.trade_history.at[trade_id, "status"] = "sell_ordered"
                self.trade_history.at[trade_id, "sold_time"] = self.data.datetime.time(0)
                self.trade_history.at[trade_id, "latest_update"] = self.data.datetime.time(0)
                self.trade_history.at[trade_id, "sold_price"] = self.data.close


            MyStrategy.order_history.loc[order_id] = [symbol, total_sell_quantity, "sell", list(sell_trades.index),
                                                self.data.datetime.time(0), None, self.data.close, "ordered"]

    def next(self):
        print('date:{},close:{}'.format(self.data.datetime.time(0), self.data.close))
        if not market_open_after_hours():
            return

        quantity_bought = self.check_buy()

        if quantity_bought == 0:
            self.check_sell()

if __name__ == '__main__':

    symbols =[]
    cerebro = bt.Cerebro()
    cerebro.addstrategy(MyStrategy)

    for s in symbols:
        data = TradeStationData(s)
        sleep(0.001)
        cerebro.adddata(data)
        cerebro.addstrategy(MyStrategy, symbol = s)

    cerebro.run()
