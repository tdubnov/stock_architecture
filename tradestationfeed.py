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

        if not firebase_admin._apps:
            cred = credentials.Certificate('./firestore_key.json')
            initialize_app(cred)
        self.db = firestore.client().collection(self.trade_station.account_name)

        self.budget = Budget()
        self.trade_history = pd.DataFrame(columns=TRADE_HISTORY_COLUMNS + ["latest_update"])
        self.temp_trade_history = {}
        self.temp_order_history = {}
        self.trade_history = pd.DataFrame(columns=TRADE_HISTORY_COLUMNS + ["latest_update"])
        self.order_history = pd.DataFrame(columns=ORDER_HISTORY_COLUMNS)
        #self.read_db()


    def read_db(self):
        print('Reading db')
        budget_info = self.db.document("budget").get().to_dict()
        ###There is a problem in this line###
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



    def synchronize_broker_with_db(self):
        self.trade_station.get_account_updates()

        for order_id, curr_order in sorted(self.trade_station.orders.items(), key=lambda k_v: k_v[1]["OpenedTime"]):
            order_type = curr_order["Type"]
            symbol = curr_order["Symbol"]

            opened_time = curr_order["OpenedTime"]
            closed_time = curr_order["ClosedTime"]

            filled_price = curr_order["FilledPrice"]
            limit_price = curr_order["LimitPrice"]
            quantity = curr_order["Quantity"]
            status = curr_order["Status"]

            if status in FAILED_STATUSES:
                order_status = "failed"
            elif status in ALIVE_STATUSES:
                order_status = "received"
            elif status in PARTIALLY_FILLED_STATUSES:
                order_status = "partially_filled"
            elif status in FILLED_STATUSES:
                order_status = "filled"
            else:
                order_status = f"INVALID_STATUS: {status}"

            symbol_latest_status = list(self.db.document(symbol).collection("trade_history").list_documents())[-1].get().to_dict()['status']
            if order_id in self.order_history.index and order_status == self.order_history.at[order_id, "status"] and symbol_latest_status != 'sell_ordered':
                continue
            if order_status == "filled" and symbol_latest_status != 'sold':
                self.order_history.loc[order_id] = [symbol, quantity, order_type, [], openedTime, closedTime, filled_price, order_status]
                self.db.document(symbol).collection("order_history").document(order_id).set(
                    {"quantity": quantity, "type": order_type.lower(),
                     "filled_price": filled_price, "limit_price": limit_price,
                     "opened_time": opened_time, "closed_time": closed_time, "status": order_status}, merge=True)

            if order_type == "Sell":
                try:
                    trade_ids = self.order_history.at[order_id, "trade_ids"]
                except Exception:
                    continue

                if order_status == "failed":
                    trade_status = "own"
                elif order_status == "received":
                    trade_status = "sell_order_received"
                elif order_status == "partially_filled":
                    trade_status = "partially_sold"
                    
                elif order_status == "filled":
                    trade_status = "sold"
                    total_profit = 0
                    for trade_id in trade_ids:
                        profit = ((filled_price - self.trade_history.loc[trade_id].purchase_price)
                                  * self.trade_history.loc[trade_id].quantity)
                        total_profit += profit

                        trade = self.trade_history.loc[trade_id]
                        profit = (filled_price - trade.purchase_price)*trade.quantity
                        bdays = np.busday_count(datetime64_to_date(trade.purchase_time), datetime64_to_date(closedTime), holidays=HOLIDAYS)

                    # If order in trade_history then we already subtracted the cost of the purchase and we need to
                    # add the money gained from selling
                    if not self.order_history.at[order_id, 'status']:
                        self.budget.update_remaining_budget(update=total_profit, ts_client=self.trade_station)
                    else:
                        self.budget.update_remaining_budget(update=filled_price * quantity, ts_client=self.trade_station)
                    self.budget.increase_max_budget(increase=total_profit)

                else:
                    trade_status = f"INVALID_SELL_STATUS: {order_status}"

                # Update local and db copy of trade history
                for trade_id in trade_ids:
                    self.trade_history.at[trade_id, "status"] = trade_status
                    self.trade_history.at[trade_id, "sold_time"] = closedTime
                    self.trade_history.at[trade_id, "latest_update"] = closedTime
                    self.trade_history.at[trade_id, "sold_price"] = filled_price
                    self.db.document(symbol).collection("trade_history").document(trade_id).set(
                        {"sold_filled_price": filled_price, "sold_limit_price": limit_price,
                         "sold_time": closedTime, "status": trade_status, "latest_update": closedTime}, merge=True)

            else:  # Buy order
                if order_id not in self.trade_history.index:
                    self.trade_history.loc[order_id] = [symbol, quantity, MIN_THRESHOLD, closedTime,  # symbol, quantity, sell_threshold, purchase_time,
                                                        None, filled_price, 0, None, False, closedTime]
                    self.db.document(symbol).collection("trade_history").document(order_id).set(
                        {"quantity": quantity, "purchase_filled_price": filled_price,
                         "purchase_limit_price": limit_price,
                         "purchase_time": closedTime, "status": None, "sell_threshold": min_threshold,
                         "latest_update": closedTime}, merge=True)

                # # When server restarts, order_changes will contain all orders that we will skip
                # if closedTime < self.trade_history.at[order_id, "latest_update"]:
                #     continue

                if order_status == "failed":
                    trade_status = "purchase_failed"

                    # If we subtracted the filled price on the initial budget than add it back
                    if self.order_history.at[order_id, "status"]:
                        self.budget.update_remaining_budget(quantity * limit_price, self.trade_station)
                elif order_status == "received":
                    trade_status = "purchase_order_received"
                    print(f'Buy order (OrderID: {order_id}) received')
                elif order_status == "partially_filled":
                    trade_status = "partially_purchased"
                    print(f'Buy order (OrderID: {order_id}) partially filled\n' \
                                       f'DID NOT IMPLEMENT THIS CASE BECAUSE TAMMUZ SAID IT WOULD NOT HAPPEN\n' \
                                       f'THINGS WERE NOT SUPPOSED TO BE LIKE THIS AHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH')
                elif order_status == "filled":
                    trade_status = "own"

                    # If order already exists in trade history then we already subtracted the filled price
                    # from the budget and we need to add back the difference between the filled and limit price
                    if self.order_history.at[order_id, "status"]:
                        self.budget.update_remaining_budget(quantity * (limit_price - filled_price), self.trade_station)
                    else:
                        self.budget.update_remaining_budget(-quantity * filled_price, self.trade_station)

                    # Send Email
                    threshold = self.trade_history.loc[order_id].sell_threshold
                    transaction_cost = quantity * filled_price

                else:
                    trade_status = f"INVALID_BUY_STATUS: {order_status}"
                    print(f'Buy order (OrderID: {order_id}) status is INVALID: {order_status}!')

                # Update local and db copy of trade history
                self.trade_history.at[order_id, "status"] = trade_status
                self.trade_history.at[order_id, "purchase_price"] = filled_price
                self.trade_history.at[order_id, "purchase_time"] = closedTime
                self.trade_history.at[order_id, "latest_update"] = closedTime
                self.db.document(symbol).collection("trade_history").document(order_id).set(
                    {"purchase_filled_price": filled_price, "purchase_limit_price": limit_price,
                     "purchase_time": closedTime, "status": trade_status, "latest_update": closedTime}, merge=True)

            # Update local and db copy of orders, update db budget, send telegram message
            self.order_history.at[order_id, "status"] = order_status
            self.order_history.at[order_id, "price"] = filled_price
            self.order_history.at[order_id, "opened_time"] = openedTime
            self.order_history.at[order_id, "closed_time"] = closedTime
            self.db.document(symbol).collection("order_history").document(order_id).set(
                {"filled_price": filled_price, "status": order_status, "opened_time": openedTime,
                 "closed_time": closedTime}, merge=True)
            self.db.document("budget").set(
                {"max_budget": self.budget.max_budget,
                 "remaining_budget": self.budget.remaining_budget}, merge=True)

        # Check if trade history is de-synced
        open_trades = self.trade_history[self.trade_history.status.isin(["own", "sell_ordered", "sell_order_received"])]
        quantity_per_symbol = open_trades.groupby('symbol').quantity.sum()
        for symbol, tracked_quantity in quantity_per_symbol.items():
            try:
                actual_quantity = self.trade_station.positions[symbol]['Quantity']
                if actual_quantity != tracked_quantity:
                    print(f"DB is de-synced with TradeStation\n"
                                                   f"DB says we own {tracked_quantity} shares of {symbol} "
                                                   f"but we actually own {actual_quantity} shares")
            except Exception:
                print(f"symbol {symbol} not found, likely no longer tracked ")

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

                response = self.trade_client.place_order(account_key=self.account_name, symbol=symbol, trade_action='SELL',
                                                  quantity=qty, order_type="Market", duration="DAY")

                if response:
                    order_id = response["OrderID"]

                    for trade_id in sell_trades.index:
                        self.trade_history.at[trade_id, "status"] = "sell_ordered"
                        self.trade_history.at[trade_id, "sold_time"] = self.data.datetime.time(0)
                        self.trade_history.at[trade_id, "latest_update"] = self.data.datetime.time(0)
                        self.trade_history.at[trade_id, "sold_price"] = self.data.close[0]

                        self.db.document(symbol).collection("trade_history").document(trade_id).set(
                            {"sold_filled_price": 0, "sold_limit_price": self.data.close[0],
                             "sold_time": self.data.datetime.time(0), "status": "sell_ordered",
                             "latest_update": self.data.datetime.time(0)}, merge=True)

                    self.order_history.loc[order_id] = [symbol, qty, "sell", list(sell_trades.index),
                                                        self.data.datetime.time(0), None, self.data.close[0], "ordered"]
                    self.db.document(symbol).collection("order_history").document(order_id).set(
                        {"quantity": qty, "type": "sell", "trade_ids": list(sell_trades.index),
                         "filled_price": 0, 'limit_price': self.data.close[0], "time": self.data.datetime.time(0),
                         "status": "ordered"}, merge=True)
                self.sell()



            else:

                response = self.trade_client.place_order(account_key=self.account_name, symbol=symbol, trade_action='BUY',
                                                      quantity=quantity, order_type="Market", duration="DAY")

                if response:

                    order_id = response["OrderID"]

                    status = "purchase_ordered"
                    self.budget.update_remaining_budget(-self.data.close[0]*quantity, self.trade_station)
                    self.db.document("budget").set(
                        {"remaining_budget": self.budget.remaining_budget}, merge=True)

                    # Update local and db copies of order history
                    self.order_history.loc[order_id] = [symbol, quantity, "buy", None, self.data.datetime.time(0), None, self.data.close[0], status]
                    self.db.document(symbol).collection("order_history").document(order_id).set(
                        {"quantity": quantity, "type": "buy", "filled_price": 0, 'limit_price': self.data.close[0],
                         "time": self.data.datetime.time(0), "status": status}, merge=True)

                    # Update local and db copies of trade history
                    # symbol, quantity, sell_threshold, purchase_time,
                    # sold_time, purchase_price, sold_price, status, latest_update
                    self.trade_history.loc[order_id] = [symbol, quantity, threshold, self.data.datetime.time(0),
                                                        None, self.data.close[0], 0, status, False, self.data.datetime.time(0)]
                    self.db.document(symbol).collection("trade_history").document(order_id).set(
                        {"quantity": quantity, "purchase_filled_price": 0, "purchase_limit_price": self.data.close[0],
                         "purchase_time": self.data.datetime.time(0), "status": status, "sell_threshold": threshold,
                         "latest_update": self.data.datetime.time(0), "above_threshold": False}, merge=True)
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
