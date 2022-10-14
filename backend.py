from helper import create_logger
import sys
logger = create_logger(file=f'{sys.argv[1].replace(" ", "_")}_log.log')

import firebase_admin
from firebase_admin import credentials, firestore, initialize_app
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

from config import clients
import atexit
import signal

UNDERLINE_START = '<u>'
UNDERLINE_END = '</u>'
TELEGRAM_INDENT = "   "

logging.getLogger("urllib3").setLevel(logging.WARNING)  # Suppress useless logs to warning


class Backend:
    def __init__(self, client: str, paper: bool = True,
                 interval: int = 1, unit: str = 'Minute', session: str = 'USEQPreAndPost'):
        if paper:
            client = 'Paper'

        self.client = client
        details = clients[client]

        self.socket = socket.socket()  # instantiate
        self.trade_station = TradeStation(client=client, symbols=details['Symbols'],
                                          paper=paper, interval=interval, unit=unit, session=session)

        if not firebase_admin._apps:
            cred = credentials.Certificate('./firestore_key.json')
            initialize_app(cred)
        self.db = firestore.client().collection(self.trade_station.account_name)

        self.budget = Budget()
        self.temp_trade_history = {}
        self.temp_order_history = {}
        self.trade_history = pd.DataFrame(columns=TRADE_HISTORY_COLUMNS + ["latest_update"])
        self.order_history = pd.DataFrame(columns=ORDER_HISTORY_COLUMNS)
        self.read_db()

        self.last_telegram_update = None
        self.handler = CustomErrorHandler(self.send_telegram_message)

    def start(self):
        details = clients[self.client]
        self.socket.bind(('localhost', details['Port']))
        self.socket.connect(('localhost', 5000))  # connect to the server
        logger.addHandler(self.handler)

        def handle_exit(**kwargs):
            self.send_telegram_message(message=f"Server Stopped Running: {kwargs}")

        atexit.register(handle_exit)
        catchable_signals = set(signal.Signals) - {signal.CTRL_C_EVENT, signal.CTRL_BREAK_EVENT}
        for sig in catchable_signals:
            signal.signal(sig, handle_exit)

        stream_bars_threads = []
        for symbol in self.trade_station.symbols():
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
        # monitor_thread.start()
        telegram_bot_thread.start()
        self.send_telegram_message(message=f"{UNDERLINE_START}Server Started Running{UNDERLINE_END}")

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
                logger.exception(f"Error reading Trade History of {document.id}:{trade.id}")

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
                logger.exception(f"Error reading Order History of {document.id}:{order.id}")

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
                    message = f'Sold {quantity} shares of {symbol} at {closedTime} for ${filled_price}/share' \
                              f'- Total: ${round(filled_price * quantity, 2)}'

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

    def model_update_call_back(self, values):
        self.model.update_call_back(values)

        symbol, update_date, params = values
        num_bins, distance_n = params['num_bins'], params['distance_n']
        training_length, training_freq = params['training_length'], params['training_freq']

        self.db.document(symbol).collection("model_parameter_history").document(str(update_date)).set(
            {"num_bins": num_bins, "distance_n": distance_n,
             "training_freq": training_freq, "training_length": training_length}, merge=True)

    def invest_retrain_call_back(self, values):
        self.model.invest.retrain_call_back(values)

        symbol, retrain_date, threshold, alpha, beta = values
        if not self.first_invest_train_date.get(symbol):
            self.first_invest_train_date[symbol] = retrain_date

        self.db.document(symbol).collection("invest_parameter_history").document(str(retrain_date)).set(
            {"threshold": threshold, "alpha": alpha, "beta": beta}, merge=True)

    def update_params(self):
        while True:
            try:
                logger.info('Update thread checking if any hyper-parameters need to be updated')
                self.model.invest.simulation.budget.max_budget = self.budget.max_budget
                self.model.invest.simulation.budget.remaining_budget = self.budget.remaining_budget

                # Check we need to update params
                latest_date = self.trade_station.get_latest_date_with_data()

                symbols_to_update_model_params = set()
                for symbol in self.trade_station.symbols():
                    last_model_update_date = self.model.last_update_date.get(symbol)
                    days_since_last_update = np.busday_count(last_model_update_date, latest_date, holidays=HOLIDAYS)\
                        if last_model_update_date else float('inf')
                    if days_since_last_update > MODEL_RETRAINING_FREQ:
                        symbols_to_update_model_params.add(symbol)

                # Save parameters immediately after training finishes in case of a crash
                if symbols_to_update_model_params:
                    message = f"Updating {symbols_to_update_model_params} model parameters"
                    self.send_telegram_message(message=message)
                    self.model.update_params(update_date=latest_date, symbols=symbols_to_update_model_params,
                                             call_back=self.model_update_call_back)

                symbols_to_update_invest_params = set()
                for symbol in self.trade_station.symbols():
                    threshold_update_date = self.model.invest.last_train_date.get(symbol)
                    days_since_last_update = np.busday_count(threshold_update_date, latest_date, holidays=HOLIDAYS) \
                        if threshold_update_date else float('inf')
                    if days_since_last_update > self.model.invest.training_freq[symbol]:
                        symbols_to_update_invest_params.add(symbol)

                # Save parameters immediately after training finishes in case of a crash
                if len(symbols_to_update_invest_params) != 0:
                    message = f"Updating {symbols_to_update_invest_params} invest parameters"
                    self.send_telegram_message(message=message)
                    self.model.invest.retrain(symbols=symbols_to_update_invest_params, retrain_date=latest_date,
                                              call_back=self.invest_retrain_call_back)

                next_day = datetime64_to_date(np.busday_offset(latest_date, 1, holidays=HOLIDAYS))
                next_closing_time = self.trade_station.nyse.schedule(start_date=next_day,
                                                                     end_date=next_day).market_close[0]
                now = datetime.datetime.now(tz=datetime.timezone.utc)
                if next_closing_time > now:
                    time_till_next = next_closing_time - now
                    logger.info(f'Update thread sleeping for {time_till_next}')
                    sleep(time_till_next.total_seconds())

            except Exception:
                logger.exception(f"Monitor Broker Crashed")

    def trade(self, symbol, curr_price, curr_time):
        if not market_open_after_hours():
            return

        quantity_bought = self.check_buy(symbol=symbol, curr_price=curr_price, curr_time=curr_time)

        if quantity_bought == 0:
            self.check_sell(symbol=symbol, curr_price=curr_price, curr_time=curr_time)

    def check_buy(self, symbol: str, curr_price: float, curr_time: datetime.datetime):
        import random
        quantity = random.randint(0, 3)
        threshold = random.uniform(0, 1/3)

        if quantity == 0:
            return 0
        logger.info(f'Attempting to buy {quantity} shares of {symbol} at ${curr_price}/share')
        response = self.trade_station.submit_market_order(symbol=symbol, qty=quantity, order_type="BUY")
        # response = self.trade_station.submit_limit_order(symbol=symbol, limit_price=curr_price,
        #                                                  qty=quantity, order_type="BUY")
        if response:

            order_id = response["OrderID"]
            self.send_telegram_message(message=f'Sent order ({order_id}): {response["Message"][12:]}', order=True)

            status = "purchase_ordered"
            self.budget.update_remaining_budget(-curr_price*quantity, self.trade_station)
            self.db.document("budget").set(
                {"remaining_budget": self.budget.remaining_budget}, merge=True)

            # Update local and db copies of order history
            self.order_history.loc[order_id] = [symbol, quantity, "buy", None, curr_time, None, curr_price, status]
            self.db.document(symbol).collection("order_history").document(order_id).set(
                {"quantity": quantity, "type": "buy", "filled_price": 0, 'limit_price': curr_price,
                 "time": curr_time, "status": status}, merge=True)

            # Update local and db copies of trade history
            # symbol, quantity, sell_threshold, purchase_time,
            # sold_time, purchase_price, sold_price, status, latest_update
            self.trade_history.loc[order_id] = [symbol, quantity, threshold, curr_time,
                                                None, curr_price, 0, status, False, curr_time]
            self.db.document(symbol).collection("trade_history").document(order_id).set(
                {"quantity": quantity, "purchase_filled_price": 0, "purchase_limit_price": curr_price,
                 "purchase_time": curr_time, "status": status, "sell_threshold": threshold,
                 "latest_update": curr_time, "above_threshold": False}, merge=True)

        return quantity

    def check_sell(self, symbol: str, curr_price: float, curr_time: datetime.datetime):
        if not market_open_regular_hours():
            return

        open_trades = self.trade_history[(self.trade_history.symbol == symbol) & (self.trade_history.status == "own")]

        if open_trades.empty:
            return

        sell_prices = (1 + open_trades.sell_threshold) * open_trades.purchase_price

        new_trades_above_threshold = open_trades.loc[(sell_prices <= curr_price) & ~open_trades.above_threshold]
        for trade_id in new_trades_above_threshold.index:
            self.trade_history.loc[trade_id, 'above_threshold'] = True
            self.db.document(symbol).collection("trade_history").document(trade_id).set(
                {"above_threshold": True}, merge=True)

        sell_trades = open_trades.loc[self.trade_history.above_threshold]

        print(f'Checking to sell {symbol}'
              f'\n\t current price: {curr_price}, trade cutoffs: {sell_prices}')

        self.handle_sell(sell_trades, curr_price, curr_time)

    def handle_sell(self, sell_trades, curr_price, curr_time):
        if sell_trades.empty:
            return

        symbol = sell_trades.iloc[0].symbol
        qty = sell_trades.quantity.sum()

        logger.info(f'Selling {qty} shares of {symbol} at ${curr_price}/share')
        response = self.trade_station.submit_market_order(symbol=symbol, qty=qty, order_type="SELL")

        total_sell_quantity = int(sell_trades.quantity.sum())
        if response:
            order_id = response["OrderID"]
            self.send_telegram_message(message=f'Sent order ({order_id}): {response["Message"][12:]}', order=True)

            for trade_id in sell_trades.index:
                self.trade_history.at[trade_id, "status"] = "sell_ordered"
                self.trade_history.at[trade_id, "sold_time"] = curr_time
                self.trade_history.at[trade_id, "latest_update"] = curr_time
                self.trade_history.at[trade_id, "sold_price"] = curr_price

                self.db.document(symbol).collection("trade_history").document(trade_id).set(
                    {"sold_filled_price": 0, "sold_limit_price": curr_price,
                     "sold_time": curr_time, "status": "sell_ordered",
                     "latest_update": curr_time}, merge=True)

            self.order_history.loc[order_id] = [symbol, total_sell_quantity, "sell", list(sell_trades.index),
                                                curr_time, None, curr_price, "ordered"]
            self.db.document(symbol).collection("order_history").document(order_id).set(
                {"quantity": total_sell_quantity, "type": "sell", "trade_ids": list(sell_trades.index),
                 "filled_price": 0, 'limit_price': curr_price, "time": curr_time,
                 "status": "ordered"}, merge=True)

    def monitor_broker(self):
        while True:
            try:
                time_till_open = self._time_to_open()
                if time_till_open.total_seconds() > 0:
                    logger.info(f'Monitor broker thread sleeping for {time_till_open}')
                    sleep(time_till_open.total_seconds())

                print('Monitor Broker Running')
                while True:
                    self.synchronize_broker_with_db()
                    sleep(1.5)

            except Exception:
                logger.exception(f"Monitor Broker Crashed")

    def stream_bars(self, symbol: str):
        while True:
            try:
                time_till_open = self._time_to_open()
                if time_till_open.total_seconds() > 0:
                    logger.info(f'Stream {symbol} bars thread sleeping for {time_till_open}')
                    sleep(time_till_open.total_seconds())

                with self.trade_station.ts_client.stream_bars(symbol=symbol, interval=self.trade_station.interval,
                                                              unit=self.trade_station.unit, bars_back=1,
                                                              session_template=self.trade_station.session) as stream:
                    if stream.status_code != 200:
                        raise Exception(f"Cannot stream {symbol} bars (HTTP {stream.status_code}): {stream.text}")

                    print(f'Stream {symbol} bars started')
                    for line in stream.iter_lines():
                        if not line or line == b'':
                            continue

                        decoded_line = json.loads(line)
                        if any([param not in decoded_line for param in ["TimeStamp", "Close"]]):
                            continue

                        dt = datetime.datetime.fromisoformat(decoded_line['TimeStamp']
                                                             .replace('Z', '+00:00')).astimezone(TIMEZONE)
                        close = float(decoded_line['Close'])

                        if dt in self.trade_station.prices[symbol]:
                            continue

                        start = dt if self.trade_station.prices[symbol].empty \
                            else self.trade_station.prices[symbol].index[-1]
                        _, _, time_steps = self.trade_station.handle_dates(
                            symbol=symbol, start=start, end=dt, interval=self.trade_station.interval, unit=self.trade_station.unit,
                            session=self.trade_station.session)

                        difference = pd.Series(index=time_steps.difference(self.trade_station.prices[symbol].index),
                                               dtype=float)
                        self.trade_station.prices[symbol] = pd.concat([self.trade_station.prices[symbol], difference])\
                            .sort_index().fillna(method='ffill')
                        self.trade_station.prices[symbol].loc[dt] = close

            except requests.exceptions.ChunkedEncodingError:
                logger.warning(f'Stream {symbol} bars chunked encoding error')

            except Exception:
                logger.exception(f"Exception in stream {symbol} bars")

            print(f'Stream {symbol} bars stopped')

    def stream_quotes(self):
        while True:
            try:
                time_till_open = self._time_to_open()
                if time_till_open.total_seconds() > 0:
                    logger.info(f'Stream quotes thread sleeping for {time_till_open}')
                    sleep(time_till_open.total_seconds())

                with self.trade_station.ts_client.stream_quotes(list(self.trade_station.symbols())) as stream:
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

                        self.trade(symbol=symbol, curr_price=curr_price, curr_time=curr_time)

            except requests.exceptions.ChunkedEncodingError:
                logger.warning(f'Stream quotes chunked encoding error')

            except Exception:
                logger.exception(f"Exception in stream quotes")

            print('Stream quotes stopped')

    def send_telegram_message(self, message, order: bool = False):
        self.socket.sendall(f'{order}|{message}'.encode())

    def listen_to_server(self):
        while True:
            try:
                message = str(self.socket.recv(1024).decode())
                if message == '/hello_world':
                    response = self.hello_world()
                elif message == '/get_budget':
                    response = self.get_budget()
                elif message == '/get_balance':
                    response = self.get_balance()
                elif message == '/get_orders':
                    response = self.get_orders()
                elif message == '/get_positions':
                    response = self.get_positions()
                elif message == '/get_updates':
                    response = self.get_updates()
                elif message == '/get_errors':
                    response = self.get_errors()
                elif message == '/error_acknowledged':
                    response = self.error_acknowledged()
                elif message == '/get_performance_graph':
                    response = self.get_performance_graph()
                else:
                    response = "Invalid request"

                self.send_telegram_message(message=response)

            except Exception:
                logger.exception('Hit listen exception')

    def hello_world(self):
        return "Hello World!"

    def get_errors(self):
        return f"Errors: {self.handler.errors}"

    def error_acknowledged(self):
        self.handler.error_thread_alive = False
        self.handler.errors.clear()
        return "Thank you UwU"

    def get_updates(self):
        try:
            self.synchronize_broker_with_db()

            if not self.last_telegram_update:
                self.last_telegram_update = {"budget": Budget(),
                                             "balance": {}, "positions": {}}

                message = "Since this is the first request, this is the entire state:"
            else:
                message = "Here are the updates:"

            # Print updates to budget
            if self.last_telegram_update["budget"] != self.budget:
                budget_left = self.budget.remaining_budget / self.budget.max_budget
                message += f"\n\n\nBudget updates:" \
                           f"\n- Max budget: ${self.budget.max_budget}" \
                           f"\n- Remaining budget: ${round(self.budget.remaining_budget, 2)}" \
                           f"\n- Budget in Use: {round(100 * (1 - budget_left), 2)}%"

                self.last_telegram_update["budget"] = self.budget.deep_copy()

            # Print updates to balance
            if self.last_telegram_update["balance"] != self.trade_station.balance:
                message += f"\n\n\nBalance updates:"
                updates = {key: value for key, value in self.trade_station.balance.items()
                           if self.last_telegram_update["balance"].get(key) != value}
                for key, value in updates.items():
                    if isinstance(value, (int, float)):
                        value = round(value, 2)
                    message += f'\n- {key}: {value}'
                self.last_telegram_update["balance"] = deepcopy(self.trade_station.balance)

            # Print updates to positions
            symbols_changed = set()
            if self.last_telegram_update["positions"] != self.trade_station.positions:
                message += f"\n\n\nPosition updates:"
                positions = {symbol: position for symbol, position in self.trade_station.positions.items()
                             if position['Quantity'] != 0}
                symbols_changed = set(positions.keys())

                updates = {symbol: {key: value for key, value in position.items() if
                                    self.last_telegram_update["positions"].get(symbol, {}).get(key) != value}
                           for symbol, position in positions.items()
                           if self.last_telegram_update["positions"].get(symbol) != position}

                for symbol, position_update in sorted(updates.items()):
                    if not self.model.last_update_date.get(symbol):
                        continue

                    message += f'\n\n- Symbol: {symbol}'

                    if any(key in position_update for key in ["UnrealizedPL($)", "UnrealizedPL(%)",
                                                              "UnrealizedPL(qty)"]):
                        pl_total = round(position_update.pop("UnrealizedPL($)", 0), 2)
                        pl_percent = round(position_update.pop("UnrealizedPL(%)", 0), 2)
                        pl_qty = round(position_update.pop("UnrealizedPL(qty)", 0), 2)
                        message += f'\n\t Unrealized PL($/%/qty): {pl_total}/{pl_percent}/{pl_qty}'

                    for key, value in position_update.items():
                        if isinstance(value, (int, float)):
                            value = round(value, 2)
                        message += f'\n\t {key}: {value}'

                    momentum, _ = self.calculate_momentum(symbol=symbol, timedelta=datetime.timedelta(minutes=30))
                    message += f'\n\t Momentum: {momentum:.2E}'

                self.last_telegram_update["positions"] = deepcopy(self.trade_station.positions)

            # Send updates to momentum
            message += "\n\n\nMomentum Updates:"
            momentum_symbols = {symbol for symbol in self.trade_station.symbols() if
                                self.model.invest.last_train_date.get(symbol) and symbol not in symbols_changed}
            for symbol in sorted(momentum_symbols):
                momentum, _ = self.calculate_momentum(symbol=symbol, timedelta=datetime.timedelta(minutes=30))
                message += f"\n-{symbol}: {momentum:.2E}"

            return message

        except Exception as exception:
            logger.exception(f"Exception in get_updates")
            return f"Hit exception in get_updates: {exception}"

    def get_budget(self):
        try:
            max_budget = self.budget.max_budget
            remaining_budget = self.budget.remaining_budget
            budget_left = remaining_budget/max_budget
            return f"Max budget: ${max_budget}\n" \
                   f"Remaining budget: ${round(remaining_budget, 2)}\n" \
                   f"Budget in Use: {round(100 * (1 - budget_left), 2)}%"

        except Exception as exception:
            logger.exception(f"Exception in get_budget")
            return f"Hit exception in get_budget: {exception}"

    def get_balance(self):
        try:
            self.synchronize_broker_with_db()

            message = 'Balance:'
            for key, value in self.trade_station.balance.items():
                if isinstance(value, (int, float)):
                    value = round(value, 2)
                message += f'\n\t {key}: {value}'

            return message

        except Exception as exception:
            logger.exception(f"Exception in get_balance")
            return f"Hit exception in get_balance: {exception}"

    def get_orders(self):
        try:
            self.synchronize_broker_with_db()

            message = 'Orders:'
            for order_id, order in self.trade_station.orders.items():
                message += f'\n\n\t Id: {order_id}'
                for key, value in order.items():
                    if isinstance(value, (int, float)):
                        value = round(value, 2)
                    message += f'\n\t {key}: {value}'
            return message

        except Exception:
            logger.exception(f"Exception in get_orders")
            return "Hit exception in get_orders: {exception}"

    def get_positions(self):
        try:
            self.synchronize_broker_with_db()

            message = 'Positions:'
            positions = {symbol: position for symbol, position in self.trade_station.positions.items()
                         if position['Quantity'] != 0}
            for symbol, position in sorted(positions.items()):
                message += f'\n\n\t Symbol: {symbol}'

                if any(key in position for key in ["UnrealizedPL($)", "UnrealizedPL(%)", "UnrealizedPL(qty)"]):
                    pl_total = round(position.pop("UnrealizedPL($)", 0), 2)
                    pl_percent = round(position.pop("UnrealizedPL(%)", 0), 2)
                    pl_qty = round(position.pop("UnrealizedPL(qty)", 0), 2)
                    message += f'\n\t Unrealized PL($/%/qty): {pl_total}/{pl_percent}/{pl_qty}'

                for key, value in position.items():
                    if isinstance(value, (int, float)):
                        value = round(value, 2)
                    message += f'\n\t {key}: {value}'

            return message

        except Exception as exception:
            logger.exception(f"Exception in get_positions")
            return f"Hit exception in get_positions: {exception}"

    def get_performance_graph(self):
        try:
            start = self.trade_history.purchase_time.min()
            end = datetime.datetime.now(tz=TIMEZONE)

            all_trading_prices = pd.DataFrame()
            for symbol in set(self.trade_history.symbol):
                all_trading_prices[symbol] = self.trade_station.collect_prices(symbol, start, end, 1, 'Daily', 'Default')

            liquidated_balance = get_equity(self.trade_history, self.budget.starting_budget, all_trading_prices)
            spy_data = self.trade_station.collect_prices('SPY', start, end, 1, 'Daily', 'Default')

            import plotly.graph_objects as go
            fig = go.Figure()
            spy_pct = spy_data / spy_data.iloc[0] - 1
            balance_pct = liquidated_balance / liquidated_balance.iloc[0] - 1
            fig = fig.add_trace(go.Scatter(x=spy_pct.index, y=spy_pct.values, name='SPY'))
            fig = fig.add_trace(go.Scatter(x=balance_pct.index, y=balance_pct.values, name='Balance'))
            fig.write_image(f"{self.client}_performance_graph.png")

            return "send_performance_graph"

        except Exception as exception:
            logger.exception(f"Exception in get_performance_graph")
            return f"Hit exception in get_performance_graph: {exception}"


if __name__ == '__main__':
    backend = Backend(client=sys.argv[1], paper=False)
    backend.start()
