from helper import CustomErrorHandler
from helper import create_logger, Budget, get_equity
import sys
import socket
import logging
from copy import deepcopy
from datetime import datetime
import pandas as pd
from config import clients
import threading

from tradestation import TradeStation

## Give path to logs file 
logger = create_logger(file="path_file")
from constants import TIMEZONE
import tradestationfeed

UNDERLINE_START = '<u>'
UNDERLINE_END = '</u>'
TELEGRAM_INDENT = "   "

class TelegramNotification(object):
    
    def __init__(self, orders_hist, trade_hist, db,  var, socket):
        self.order_history = orders_hist
        self.trade_history = trade_hist
        self.socket = socket
        self.last_telegram_update = None
        self.db = db
        self.handler = CustomErrorHandler(self.send_telegram_message)
        details = clients['Paper']
        if var == False:
            self.socket.bind(('localhost', details['Port']))
            self.socket.connect(('localhost', 5000))
            telegram_bot_thread = threading.Thread(target=self.listen_to_server)
            telegram_bot_thread.daemon = False
            telegram_bot_thread.start()
            self.send_telegram_message(message=f"{UNDERLINE_START}Server Started Running{UNDERLINE_END}")
        self.trade_station = TradeStation(client='Paper', symbols=details['Symbols'],
                                          paper=True, interval=1, unit='Minute', session='USEQPreAndPost')

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
                    response =self.get_positions()
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
                    return
            except Exception:
                print(f"symbol {symbol} not found, likely no longer tracked ")
                return

    def get_errors(self):

        return f"Errors: {self.handler.errors}"

    def error_acknowledged(self):
        self.handler.error_thread_alive = False
        self.handler.errors.clear()
        return "Thank you UwU"
        
    def send_telegram_message(self, message, order: bool = False):

        self.socket.sendall(f'{order}|{message}'.encode())

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
            fig.write_image("Paper_performance_graph.png")

            return "send_performance_graph"

        except Exception as exception:
            logger.exception(f"Exception in get_performance_graph")
            return f"Hit exception in get_performance_graph: {exception}"

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