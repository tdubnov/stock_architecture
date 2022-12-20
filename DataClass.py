from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from datetime import timedelta, time
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
import pytz
from pqdm.threads import pqdm
from multiprocess import Pool
import pandas_market_calendars as calendars
from config import clients
from tradestation import TradeStation
from helper import CustomErrorHandler, market_open_regular_hours, market_open_after_hours, \
    datetime64_to_date, Budget, get_equity
from email_helper import send_email

from constants import TRADE_HISTORY_COLUMNS, TIMEZONE, HOLIDAYS, FAILED_STATUSES, ALIVE_STATUSES, \
    ORDER_HISTORY_COLUMNS, FILLED_STATUSES, PARTIALLY_FILLED_STATUSES





class TradeStationData(bt.feed.DataBase):
    params = (
        ('symbol', 'a'),
        ('details', clients['Paper']),
        )


    def __init__(self, args):

        super(TradeStationData, self).__init__()

        details = self.p.details
        self.symbol = self.p.symbol
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

        self.budget = Budget()
        self.account_name = [account['Name'] for account in self.trade_client.get_accounts(details['Username']) if account['TypeDescription'] == details['AccountType']][0]

        if not firebase_admin._apps:

            cred = credentials.Certificate('./firestore_key.json')
            initialize_app(cred)

        self.db = firestore.client().collection(self.trade_station.account_name)
        self.trade_history = pd.DataFrame(columns=TRADE_HISTORY_COLUMNS + ["latest_update"])
        self.temp_trade_history = {}
        self.temp_order_history = {}
        self.trade_history = pd.DataFrame(columns=TRADE_HISTORY_COLUMNS + ["latest_update"])
        self.order_history = pd.DataFrame(columns=ORDER_HISTORY_COLUMNS)

    #def start(self):
    #    catchable_signals = set(signal.Signals) - {signal.CTRL_C_EVENT, signal.CTRL_BREAK_EVENT}
    #    for sig in catchable_signals:
    #        signal.signal(sig, )

    def read_db(self):
        print('Reading db')
        budget_info = self.db.document("budget").get().to_dict()
        self.budget.max_budget = budget_info["max_budget"]
        self.budget.remaining_budget = budget_info["remaining_budget"]
        self.budget.starting_budget = budget_info["starting_budget"]

        pqdm(self.db.list_documents(), self.get_document_info, n_jobs=10)
        self.trade_history = pd.DataFrame.from_dict(self.temp_trade_history, orient='index', columns=TRADE_HISTORY_COLUMNS + ["latest_update"])
        self.order_history = pd.DataFrame.from_dict(self.temp_order_history, orient='index', columns=ORDER_HISTORY_COLUMNS)
        print(f'Finished reading db:'            
              f'\n\nTrade history from DB:'
              f'\n{self.trade_history.to_string()}'
                
              f'\n\nOrder history from DB:'
              f'\n{self.order_history.to_string()}')
        self.trade_history = self.trade_history.sort_values(by='purchase_time')

    def get_document_info(self, document):
        if document.id == 'budget':  # or document.id not in symbols:
            return

        for trade in document.collection("trade_history").list_documents():
            info = trade.get().to_dict()
            quantity = info["quantity"]
            sell_threshold = info["sell_threshold"]
            purchase_time = info["purchase_time"].astimezone(TIMEZONE)
            sold_time = info["sold_time"].astimezone(TIMEZONE) if "sold_time" in info else None
            latest_update = info["latest_update"].astimezone(TIMEZONE)

            purchase_price = info["purchase_filled_price"]
            sold_price = info.get("sold_filled_price", 0)
            status = info["status"]
            above_threshold = info.get("above_threshold", False)
            try:
                row = {'symbol': document.id, 'quantity': quantity, 'sell_threshold': sell_threshold,
                       'purchase_time': purchase_time, 'sold_time': sold_time, 'purchase_price': purchase_price,
                       'sold_price': sold_price, 'status': status, 'above_threshold': above_threshold,
                       'latest_update': latest_update}
                self.temp_trade_history[trade.id] = row

            except Exception as e:
                print(f"Error reading Trade History of {document.id}:{trade.id}")


        for order in document.collection("order_history").list_documents():
            try:
                info = order.get().to_dict()
                quantity = info["quantity"]
                order_type = info["type"]
                opened_time = None if "opened_time" not in info else info["opened_time"].astimezone(TIMEZONE)
                closed_time = None if "closed_time" not in info else info["closed_time"].astimezone(TIMEZONE)
                trade_ids = info.get("trade_ids", [])
                price = info["filled_price"]
                status = info["status"]

                row = {'symbol': document.id, 'quantity': quantity, 'type': order_type, 'trade_ids': trade_ids,
                       'opened_time': opened_time, 'closed_time': closed_time, 'price': price, 'status': status}
                self.temp_order_history[order.id] = row

            except Exception as e:
                print(f"Error reading Order History of {document.id}:{order.id}")


    def time_to_open(self):

        now = datetime.datetime.now(tz=datetime.timezone.utc)
        if not np.is_busday(now.date(), holidays=HOLIDAYS):
            next_date = datetime64_to_date(np.busday_offset(now.date(), 0, roll='forward', holidays=HOLIDAYS))

        elif now > self.trade_station.nyse.schedule(start_date=now.date(), end_date=now.date()).market_close[0]:
            next_date = datetime64_to_date(np.busday_offset(now.date(), 1, holidays=HOLIDAYS))

        else:
            next_date = now.date()

        next_opening_time = self.trade_station.nyse.schedule(start_date=next_date, end_date=next_date).market_open[0]
        next_opening_time = next_opening_time.replace(tzinfo=None)
        now = now.replace(tzinfo=None)
        time_till_open = next_opening_time- now - datetime.timedelta(minutes=1)
        return time_till_open

    def _load(self):

        try:
            time_till_open = self.time_to_open()
            
            if time_till_open.total_seconds()>0:
                #logger.info(f'Stream quotes thread sleeping for {time_till_open}')
                sleep(time_till_open.total_seconds())

            with self.trade_station.ts_client.stream_quotes([self.symbol]) as stream:
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

                    print(curr_price, bt.date2num(pd.to_datetime(curr_time)), float(decoded_line['High']), float(decoded_line['Low']), float(decoded_line['Volume']))
                    self.lines.close[0] = curr_price
                    self.lines.datetime[0] = bt.date2num(pd.to_datetime(curr_time))
                    self.lines.high[0] = float(decoded_line['High'])
                    self.lines.low[0] = float(decoded_line['Low'])
                    self.lines.volume[0] = float(decoded_line['Volume'])
                    print(curr_price, bt.date2num(pd.to_datetime(curr_time)), float(decoded_line['High']), float(decoded_line['Low']), float(decoded_line['Volume']))

                    return True
        except requests.exceptions.ChunkedEncodingError:
            #logger.warning(f'Stream quotes chunked encoding error')
            print('Stream quotes chunked encoding error')
            return None

        except Exception:
            #logger.exception(f"Exception in stream quotes")
            print('Stream quotes chunked encoding error')
            return None

        print('Stream quotes stopped')
        return None

    def stop(self):
        pass

    def haslivedata(self):
         return True

    def islive(self):
        return True
