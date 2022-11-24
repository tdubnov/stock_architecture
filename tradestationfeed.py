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

from DataClass import TradeStationData



class MyStrategy(bt.Strategy):
    params = (
        ('symbol', 'a')
        )

    def __init__(self):


        details = clients['Paper']
        self.trade_client = TradeStationClient(
            username=details['Username'],
            client_id=details['ClientId'],
            client_secret=details['Secret'],
            redirect_uri="http://localhost",
            version=details['Version'],
            paper_trading=paper
        )
        self.account_name = [account['Name'] for account in self.ts_client.get_accounts(details['Username'])
                             if account['TypeDescription'] == details['AccountType']][0]

    def next(self):

        print('date:{},close:{}'.format(self.data.datetime.time(0), self.data.close[0]))

        import random
        quantity = random.randint(0, 3)
        threshold = random.uniform(0, 1/3)

        if not market_open_after_hours():

            return

        else: 

            if quantity == 0:

                self.trade_client.place_order(account_key=self.account_name, symbol=symbol, trade_action='SELL',
                                                  quantity=qty, order_type="Market", duration="DAY")



            else:

                self.trade_client.place_order(account_key=self.account_name, symbol=symbol, trade_action='BUY',
                                                      quantity=quantity, order_type="Market", duration="DAY")



if __name__ == '__main__':
  
  symbols =[]
  cerebro = bt.Cerebro() 

  for s in symbols:
      data = TradeStationData(s)
      sleep(0.001)
      cerebro.adddata(data)
      cerebro.addstrategy(MyStrategy, symbol = s)

  cerebro.run()
