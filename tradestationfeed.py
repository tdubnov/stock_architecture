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

class Datas():
    params = (
        ('symbol', 'a')
        )
    cred = credentials.Certificate("/home/ubuntu/Documents/stock_architecture-new/firestore_key.json")
    firebase_admin.initialize_app(cred)

    def __init__(self):
        self.budget = Budget()
        self.temp_trade_history = {}
        self.temp_order_history = {}
        self.trade_station = TradeStation(client='Paper', paper=True)
        self.trade_history = pd.DataFrame(columns=TRADE_HISTORY_COLUMNS + ["latest_update"])
        self.sell_trades = pd.DataFrame(columns=TRADE_HISTORY_COLUMNS + ["latest_update"])
        self.order_history = pd.DataFrame(columns=ORDER_HISTORY_COLUMNS)
        self.db = firestore.client().collection(self.trade_station.account_name)
        self.read_db()


    def check_buy(self):
        import random
        quantity = random.randint(0, 3)
        threshold = random.uniform(0, 1/3)

        if quantity == 0:
            return 0

        response = self.trade_station.submit_market_order(symbol=symbol, qty=quantity, order_type="BUY")

        if response:

            order_id = response["OrderID"]

            status = "purchase_ordered"
            self.budget.update_remaining_budget(-self.data.close*quantity, self.trade_station)

            self.order_history.loc[order_id] = [symbol, quantity, "buy", None, self.data.datetime.time(0), None, self.data.close, status]

            self.trade_history.loc[order_id] = [symbol, quantity, threshold, self.data.datetime.time(0),
                                                None, curr_price, 0, status, False, self.data.datetime.time(0)]

        return quantity

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

                    # send_email(subject=subject, message=message)
                    telegram_message = f'{subject}\n\n{message}'
                else:
                    trade_status = f"INVALID_SELL_STATUS: {order_status}"
                    telegram_message = f"Received INVALID_SELL_STATUS: {order_status} for order {order_id}"

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
                    telegram_message = f'Buy order {order_id} failed'

                    # If we subtracted the filled price on the initial budget than add it back
                    if self.order_history.at[order_id, "status"]:
                        self.budget.update_remaining_budget(quantity * limit_price, self.trade_station)
                elif order_status == "received":
                    trade_status = "purchase_order_received"
                    telegram_message = f'Buy order (OrderID: {order_id}) received'
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
                    message = f'- Bought {quantity} shares of {symbol} at {closedTime} for ${filled_price}/share\n' \
                              f'- Total cost: ${round(transaction_cost, 2)}\n' \
                              f'- Expected return: ${round(threshold * transaction_cost, 2)}' \
                              f'/{round(100 * threshold, 3)}%'

                    # send_email(subject=subject, message=message)
                    telegram_message = f'{subject}\n{message}'
                else:
                    trade_status = f"INVALID_BUY_STATUS: {order_status}"
                    telegram_message = f'Buy order (OrderID: {order_id}) status is INVALID: {order_status}!'

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
            self.send_telegram_message(message=telegram_message, order=True)

        # Check if trade history is de-synced
        open_trades = self.trade_history[self.trade_history.status.isin(["own", "sell_ordered", "sell_order_received"])]
        quantity_per_symbol = open_trades.groupby('symbol').quantity.sum()
        for symbol, tracked_quantity in quantity_per_symbol.items():
            try:
                actual_quantity = self.trade_station.positions[symbol]['Quantity']
                if actual_quantity != tracked_quantity:
                    self.send_telegram_message(message=f"DB is de-synced with TradeStation\n"
                                                   f"DB says we own {tracked_quantity} shares of {symbol} "
                                                   f"but we actually own {actual_quantity} shares")
            except Exception:
                print(f"symbol {symbol} not found, likely no longer tracked ")


    def check_sell(self):

        if not market_open_regular_hours():
            return

        open_trades = self.trade_history[(self.trade_history.symbol == symbol) & (self.trade_history.status == "own")]

        if open_trades.empty:
            return

        sell_prices = (1 + open_trades.sell_threshold) * open_trades.purchase_price

        new_trades_above_threshold = open_trades.loc[(sell_prices <= self.data.close) & ~open_trades.above_threshold]
        for trade_id in new_trades_above_threshold.index:
            self.trade_history.loc[trade_id, 'above_threshold'] = True

        self.sell_trades = open_trades.loc[self.trade_history.above_threshold]

        print(f'Checking to sell {symbol}'
              f'\n\t current price: {self.data.close}, trade cutoffs: {sell_prices}')


        self.handle_sell()


    def handle_sell(self):
        if self.sell_trades.empty:
            return

        qty = self.sell_trades.quantity.sum()

        #logger.info(f'Selling {qty} shares of {symbol} at ${self.data.close}/share')
        response = self.trade_station.submit_market_order(symbol=symbol, qty=qty, order_type="SELL")

        total_sell_quantity = int(self.sell_trades.quantity.sum())
        if response:
            order_id = response["OrderID"]

            for trade_id in self.sell_trades.index:
                self.trade_history.at[trade_id, "status"] = "sell_ordered"
                self.trade_history.at[trade_id, "sold_time"] = self.data.datetime.time(0)
                self.trade_history.at[trade_id, "latest_update"] = self.data.datetime.time(0)
                self.trade_history.at[trade_id, "sold_price"] = self.data.close


            self.order_history.loc[order_id] = [symbol, total_sell_quantity, "sell", list(sell_trades.index),
                                                self.data.datetime.time(0), None, self.data.close, "ordered"]
          
    


class MyStrategy(bt.Strategy):
    params = (
        ('symbol', 'a')
        )

    def __init__(self):


    def next(self):
        print('date:{},close:{}'.format(self.data.datetime.time(0), self.data.close[0]))
        if not market_open_after_hours():
            return

        quantity_bought = Datas.check_buy()

        if quantity_bought == 0:
            Datas.check_sell()

if __name__ == '__main__':
  
  symbols =[]
  cerebro = bt.Cerebro() 

  for s in symbols:
      data = TradeStationData(s)
      sleep(0.001)
      cerebro.adddata(data)
      cerebro.addstrategy(MyStrategy, symbol = s)

  cerebro.run()
