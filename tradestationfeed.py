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
import yfinance as yf
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
import random

from config import clients
from tradestation import TradeStation
from helper import CustomErrorHandler, market_open_regular_hours, market_open_after_hours, \
    datetime64_to_date, Budget, get_equity
from email_helper import send_email
from os.path import exists

from constants import TRADE_HISTORY_COLUMNS, TIMEZONE, HOLIDAYS, FAILED_STATUSES, ALIVE_STATUSES, \
    ORDER_HISTORY_COLUMNS, FILLED_STATUSES, PARTIALLY_FILLED_STATUSES

from DataClass import YData
import telegram_integration

UNDERLINE_START = '<u>'
UNDERLINE_END = '</u>'
TELEGRAM_INDENT = "   "


symboll = ""
socket = socket.socket()
vari = False
var = False
trd_hist = None
ord_hist = None
class MyStrategy(bt.Strategy):

    def __init__(self, *args):

        global vari, var, trd_hist, ord_hist, socket

        details = clients['Paper']
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

        if not vari:
            self.temp_trade_history = {}
            self.temp_order_history = {}
            self.trade_history = pd.DataFrame(columns=TRADE_HISTORY_COLUMNS + ["latest_update"])
            self.order_history = pd.DataFrame(columns=ORDER_HISTORY_COLUMNS)
            self.read_db()
            trd_hist = self.trade_history
            ord_hist = self.order_history
            vari = True

        else:
            #self.temp_trade_history = {}
            #self.temp_order_history = {}
            self.trade_history = trd_hist
            self.order_history = ord_hist

        self.tn  = telegram_integration.TelegramNotification(self.order_history, self.trade_history, self.db, var, socket)
        var = True

        signal.signal(signal.SIGINT, self.stop_trader)

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

    def save_update(self):
        trd_hist = self.trade_history
        ord_hist = self.order_history

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

    @classmethod
    def ret_symbol(self):
        return self.symbol

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
                self.order_history.loc[order_id] = [symbol, quantity, order_type, [], opened_time, closed_time, filled_price, order_status]
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
                    telegram_message = f'Sell order (OrderID: {order_id}) failed'

                elif order_status == "received":
                    trade_status = "sell_order_received"
                    telegram_message = f'Sell order (OrderID: {order_id}) received'

                elif order_status == "partially_filled":
                    trade_status = "partially_sold"
                    telegram_message = f'Sell order (OrderID: {order_id}) partially filled\n' \
                                       f'DID NOT IMPLEMENT THIS CASE BECAUSE TAMMUZ SAID IT WOULD NOT HAPPEN\n' \
                                       f'THINGS WERE NOT SUPPOSED TO BE LIKE THIS AHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH'
                    
                elif order_status == "filled":
                    trade_status = "sold"
                    subject = f'Sell order (OrderID: {order_id}) filled'
                    message = f'Sold {quantity} shares of {symbol} at {closed_time} for ${filled_price}/share' \
                              f'- Total: ${round(filled_price * quantity, 2)}'

                    total_profit = 0
                    for trade_id in trade_ids:
                        profit = ((filled_price - self.trade_history.loc[trade_id].purchase_price)
                                  * self.trade_history.loc[trade_id].quantity)
                        total_profit += profit

                        trade = self.trade_history.loc[trade_id]
                        profit = (filled_price - trade.purchase_price)*trade.quantity
                        bdays = np.busday_count(datetime64_to_date(trade.purchase_time), datetime64_to_date(closed_time), holidays=HOLIDAYS)
                        message += f'\n\nClosed trade: {trade_id}' \
                                   f'\n\t - Quantity: {trade.quantity}' \
                                   f'\n\t - Purchase Price: {trade.purchase_price}' \
                                   f'\n\t - Purchase Time: {trade.purchase_time}' \
                                   f'\n\t - B-Days Open: {bdays}' \
                                   f'\n\t - Return: ${round(profit, 2)}/ ' \
                                   f'{round(100 * (filled_price/trade.purchase_price - 1), 2)}%' \
                                   f'\n\t - Profit prop to budget: {100 * round(profit/self.budget.max_budget, 4)}%'

                    # If order in trade_history then we already subtracted the cost of the purchase and we need to
                    # add the money gained from selling
                    if not self.order_history.at[order_id, 'status']:
                        self.budget.update_remaining_budget(update=total_profit, ts_client=self.trade_station)
                    else:
                        self.budget.update_remaining_budget(update=filled_price * quantity, ts_client=self.trade_station)
                    self.budget.increase_max_budget(increase=total_profit)
                    telegram_message = f'{subject}\n\n{message}'

                else:
                    trade_status = f"INVALID_SELL_STATUS: {order_status}"
                    telegram_message = f"Received INVALID_SELL_STATUS: {order_status} for order {order_id}"

                # Update local and db copy of trade history
                for trade_id in trade_ids:
                    self.trade_history.at[trade_id, "status"] = trade_status
                    self.trade_history.at[trade_id, "sold_time"] = closed_time
                    self.trade_history.at[trade_id, "latest_update"] = closed_time
                    self.trade_history.at[trade_id, "sold_price"] = filled_price
                    self.db.document(symbol).collection("trade_history").document(trade_id).set(
                        {"sold_filled_price": filled_price, "sold_limit_price": limit_price,
                         "sold_time": closed_time, "status": trade_status, "latest_update": closed_time}, merge=True)


            else:  # Buy order
                if order_id not in self.trade_history.index:
                    MIN_THRESHOLD = 0
                    self.trade_history.loc[order_id] = [symbol, quantity, MIN_THRESHOLD, closed_time,  # symbol, quantity, sell_threshold, purchase_time,
                                                        None, filled_price, 0, None, False, closed_time]
                    self.db.document(symbol).collection("trade_history").document(order_id).set(
                        {"quantity": quantity, "purchase_filled_price": filled_price,
                         "purchase_limit_price": limit_price,
                         "purchase_time": closed_time, "status": None, "sell_threshold": MIN_THRESHOLD,
                         "latest_update": closed_time}, merge=True)

                # # When server restarts, order_changes will contain all orders that we will skip
                # if closedTime < self.trade_history.at[order_id, "latest_update"]:
                #     continue

                if order_status == "failed":
                    trade_status = "purchase_failed"
                    telegram_message = f'Buy order {order_id} failed'

                    # If we subtracted the filled price on the initial budget than add it back
                    if self.order_history.at[order_id, "status"]:
                        self.budget.update_remaining_budget(quantity * limit_price, self.trade_station)
                elif order_status == "received":
                    trade_status = "purchase_order_received"
                    telegram_message = f'Buy order (OrderID: {order_id}) received'
                    print(f'Buy order (OrderID: {order_id}) received')
                elif order_status == "partially_filled":
                    trade_status = "partially_purchased"
                    telegram_message = f'Buy order (OrderID: {order_id}) partially filled\n' \
                                       f'DID NOT IMPLEMENT THIS CASE BECAUSE TAMMUZ SAID IT WOULD NOT HAPPEN\n' \
                                       f'THINGS WERE NOT SUPPOSED TO BE LIKE THIS AHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH'
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
                    subject = f'Buy order (OrderID: {order_id}) filled'
                    message = f'- Bought {quantity} shares of {symbol} at {closed_time} for ${filled_price}/share\n' \
                              f'- Total cost: ${round(transaction_cost, 2)}\n' \
                              f'- Expected return: ${round(threshold * transaction_cost, 2)}' \
                              f'/{round(100 * threshold, 3)}%'

                    telegram_message = f'{subject}\n{message}'

                else:
                    trade_status = f"INVALID_BUY_STATUS: {order_status}"
                    telegram_message = f'Buy order (OrderID: {order_id}) status is INVALID: {order_status}!'

                # Update local and db copy of trade history
                self.trade_history.at[order_id, "status"] = trade_status
                self.trade_history.at[order_id, "purchase_price"] = filled_price
                self.trade_history.at[order_id, "purchase_time"] = closed_time
                self.trade_history.at[order_id, "latest_update"] = closed_time
                self.db.document(symbol).collection("trade_history").document(order_id).set(
                    {"purchase_filled_price": filled_price, "purchase_limit_price": limit_price,
                     "purchase_time": closed_time, "status": trade_status, "latest_update": closed_time}, merge=True)

            # Update local and db copy of orders, update db budget, send telegram message
            self.order_history.at[order_id, "status"] = order_status
            self.order_history.at[order_id, "price"] = filled_price
            self.order_history.at[order_id, "opened_time"] = opened_time
            self.order_history.at[order_id, "closed_time"] = closed_time
            self.db.document(symbol).collection("order_history").document(order_id).set(
                {"filled_price": filled_price, "status": order_status, "opened_time": opened_time,
                 "closed_time": closed_time}, merge=True)
            self.db.document("budget").set(
                {"max_budget": self.budget.max_budget,
                 "remaining_budget": self.budget.remaining_budget}, merge=True)
            self.tn.send_telegram_message(message=telegram_message, order=True)

        # Check if trade history is de-synced
        open_trades = self.trade_history[self.trade_history.status.isin(["own", "sell_ordered", "sell_order_received"])]
        quantity_per_symbol = open_trades.groupby('symbol').quantity.sum()
        for symbol, tracked_quantity in quantity_per_symbol.items():
            try:
                actual_quantity = self.trade_station.positions[symbol]['Quantity']
                if actual_quantity != tracked_quantity:
                    self.tn.send_telegram_message(message=f"DB is de-synced with TradeStation\n"
                                                   f"DB says we own {tracked_quantity} shares of {symbol} "
                                                   f"but we actually own {actual_quantity} shares")
                    return
            except Exception:
                print(f"symbol {symbol} not found, likely no longer tracked ")
                return
        self.save_update()       


    def next(self):


        timee = firestore.SERVER_TIMESTAMP
        time_till_open = self.time_to_open()
            
        if time_till_open.total_seconds()>0:
            #logger.info(f'Stream quotes thread sleeping for {time_till_open}')
            print(f'Sleeping for {time_till_open.total_seconds()}')
            print('Not market open after hours')
            sleep(time_till_open.total_seconds())


        #if not market_open_after_hours():

        else:

            for i, d in enumerate(self.datas):

                symbol = d._name

                print('symbol:{}, date:{}, close:{}'.format(symbol, d.datetime.date(0), d.close[0]))

                quantity = random.randint(0, 3)
                threshold = random.uniform(0, 1/3)
                if quantity == 0:

                    open_trades = self.trade_history[(self.trade_history.symbol == symbol) & (self.trade_history.status == "own")]

                    if open_trades.empty:
                        return

                    sell_prices = (1 + open_trades.sell_threshold) * open_trades.purchase_price

                    new_trades_above_threshold = open_trades.loc[(sell_prices <= d.close[0]) & ~open_trades.above_threshold]

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
                        order_id = response['Orders'][0]['OrderID']
                        response1 = response['Orders'][0]
                        self.tn.send_telegram_message(message=f'Sent order ({order_id}): {response1["Message"][12:]}', order=True)


                        for trade_id in sell_trades.index:
                            self.trade_history.at[trade_id, "status"] = "sell_ordered"
                            self.trade_history.at[trade_id, "sold_time"] = d.datetime.date(0)
                            self.trade_history.at[trade_id, "latest_update"] = d.datetime.date(0)
                            self.trade_history.at[trade_id, "sold_price"] = d.close[0]

                            self.db.document(symbol).collection("trade_history").document(trade_id).set(
                                {"sold_filled_price": 0, "sold_limit_price": d.close[0],
                                 "sold_time": timee, "status": "sell_ordered",
                                 "latest_update": timee}, merge=True)

                        self.order_history.loc[order_id] = [symbol, qty, "sell", list(sell_trades.index),
                                                            d.datetime.date(0), None, d.close[0], "ordered"]

                        self.db.document(symbol).collection("order_history").document(order_id).set(
                            {"quantity": int(qty), "type": "sell", "trade_ids": list(sell_trades.index),
                             "filled_price": 0, 'limit_price': d.close[0], "time": timee,
                             "status": "ordered"}, merge=True)
                        sell_trades = open_trades.loc[self.trade_history.above_threshold]
                    self.sell()



                else:

                    response = self.trade_client.place_order(account_key=self.account_name, symbol=symbol, trade_action='BUY',
                                                          quantity=quantity, order_type="Market", duration="DAY")

                    if response:
                        order_id = response['Orders'][0]['OrderID']
                        response1 = response['Orders'][0]
                        self.tn.send_telegram_message(message=f'Sent order ({order_id}): {response1["Message"][12:]}', order=True)

                        status = "purchase_ordered"
                        self.budget.update_remaining_budget(-d.close[0]*quantity, self.trade_station)
                        self.db.document("budget").set(
                            {"remaining_budget": self.budget.remaining_budget}, merge=True)

                        # Update local and db copies of order history
                        self.order_history.loc[order_id] = [symbol, quantity, "buy", None, d.datetime.date(0), None, d.close[0], status]
                        self.db.document(symbol).collection("order_history").document(order_id).set(
                            {"quantity": quantity, "type": "buy", "filled_price": 0, 'limit_price': d.close[0],
                             "time": timee, "status": status}, merge=True)

                        # Update local and db copies of trade history
                        # symbol, quantity, sell_threshold, purchase_time,
                        # sold_time, purchase_price, sold_price, status, latest_update
                        self.trade_history.loc[order_id] = [symbol, quantity, threshold, d.datetime.date(0),
                                                            None, d.close[0], 0, status, False, d.datetime.date(0)]
                        self.db.document(symbol).collection("trade_history").document(order_id).set(
                            {"quantity": quantity, "purchase_filled_price": 0, "purchase_limit_price": d.close[0],
                             "purchase_time": timee, "status": status, "sell_threshold": threshold,
                             "latest_update": timee, "above_threshold": False}, merge=True)
                    self.buy()
        self.save_update()

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

    def stop_trader(self, a ,b):
        print('Stopping Backtrader')
        self.env.runstop()


if __name__ == '__main__':

    symbols = list(clients['Paper']['Symbols'])

    dict_ind = {}
    dict_rc = {}

    for s in symbols:
        dict_rc[s] = 0
        if exists(s + '.csv'):
          tmp_data = pd.read_csv(s + '.csv')
          dict_ind[s] = len(tmp_data)
        else:
          dict_ind[s] = 0

    cerebro = bt.Cerebro(maxcpus=2)
    cerebro.addstrategy(MyStrategy)

    #cerebro.addwriter(bt.WriterFile, csv=True)

    #cerebro.broker.setcash(234560.0)
    #cerebro.broker.setcommission(commission=0.001)

    for s in symbols:
        strat_params = {'symbol': s}
        symboll = s
        #, symbol = s, details = clients['Paper']
        data = YData(symbol = s, dict_rc = dict_rc, dict_ind = dict_ind)
        data.add_ind
        #data = bt.feeds.PandasData(dataname=data, name = s)
        sleep(0.001)
        cerebro.adddata(data, name=s)
        #cerebro.addanalyzer(bt.analyzers.SharpeRatio)

        #cerebro.broker.getvalue()

    cerebro.run()
    #cerebro.plot()
