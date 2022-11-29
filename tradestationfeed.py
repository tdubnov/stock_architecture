from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from datetime import timedelta
import pandas as pd
from backtrader.feed import DataBase
from backtrader import date2num, num2date
from backtrader.utils.py3 import queue, with_metaclass
import backtrader as bt
from Tradestation_python_api.ts.client import TradeStationClient
from helper import create_logger
import sys
import firebase_admin
from firebase_admin import credentials, firestore, initialize_app
#logger = create_logger(file=f'{sys.argv[1].replace(" ", "_")}_log.log')
import signal
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

from DataClass import TradeStationData



class MyStrategy(bt.Strategy):
    params = (
        ('symbol', None),
        ('details', clients['Paper'])
    )

    def __init__(self, args):

        details = self.p.details
        symbol = self.params.symbol
        print(details, symbol)
        #details = clients['Paper']
        self.trade_client = TradeStationClient(
            username=details['Username'],
            client_id=details['ClientId'],
            client_secret=details['Secret'],
            redirect_uri="http://localhost",
            version=details['Version'],
            paper_trading=True
        )

        self.trade_station = TradeStation(client='Paper', symbols=details['Symbols'],
                                          paper=True, interval=1, unit='Minute', session='USEQPreAndPost')

        self.account_name = [account['Name'] for account in self.trade_client.get_accounts(details['Username']) if account['TypeDescription'] == details['AccountType']][0]
        cred = credentials.Certificate('./firestore_key.json')
        initialize_app(cred)

        self.db = firestore.client().collection(self.trade_station.account_name)
        self.trade_history = pd.DataFrame(columns=TRADE_HISTORY_COLUMNS + ["latest_update"])
        self.temp_trade_history = {}
        #pqdm(self.db.list_documents(), self.get_document_info, n_jobs=10)
        self.trade_history = pd.DataFrame.from_dict(self.temp_trade_history, orient='index', columns=TRADE_HISTORY_COLUMNS + ["latest_update"])
        self.trade_history = self.trade_history.sort_values(by='purchase_time')

    def next(self):

        print('date:{},close:{}'.format(self.data.datetime.time(0), self.data.close[0]))

        import random
        quantity = random.randint(0, 3)
        threshold = random.uniform(0, 1/3)

        if not market_open_after_hours():
            print('Closed')
            return

        else: 
            print('Open')
            if quantity == 0:

                open_trades = self.trade_history[(self.trade_history.symbol == symbol) & (self.trade_history.status == "own")]


                if open_trades.empty:
                    return

                sell_prices = (1 + open_trades.sell_threshold) * open_trades.purchase_price

                new_trades_above_threshold = open_trades.loc[(sell_prices <= self.data.close[0]) & ~open_trades.above_threshold]
                for trade_id in new_trades_above_threshold.index:
                    self.trade_history.loc[trade_id, 'above_threshold'] = True
                    self.db.document(symbol).collection("trade_history").document(trade_id).set(
                        {"above_threshold": True}, merge=True)

                sell_trades = open_trades.loc[self.trade_history.above_threshold]

                if sell_trades.empty:

                    return

                qty = sell_trades.quantity.sum()

                self.trade_client.place_order(account_key=self.account_name, symbol=symbol, trade_action='SELL',
                                                  quantity=qty, order_type="Market", duration="DAY")
                self.sell()



            else:

                self.trade_client.place_order(account_key=self.account_name, symbol=symbol, trade_action='BUY',
                                                      quantity=quantity, order_type="Market", duration="DAY")
                self.buy()



if __name__ == '__main__':
  
  symbols =['GOOGL']
  cerebro = bt.Cerebro() 

  for s in symbols:
    strat_params = {'symbol': s, 'details': clients['Paper']}
    data = TradeStationData(strat_params, symbol = s, details = clients['Paper'])
    sleep(0.001)
    cerebro.adddata(data)
    cerebro.addstrategy(MyStrategy, strat_params, symbol = s, details = clients['Paper'])

  cerebro.run()
