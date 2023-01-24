from helper import CustomErrorHandler
from helper import create_logger, Budget, get_equity
import sys
import socket
import logging
from copy import deepcopy
from datetime import datetime
import pandas as pd
from tradestationfeed import MyStrategy
from constants import TIMEZONE
import telegram_integration


class ServerUpdate:
    params = (
        ('symbol', 'GOOGL'),
    )
    
    def __init__(self):
        tf = MyStrategy()
        tn  = telegram_integration.TelegramNotification()
        symbol = self.p.symbol
        
    def get_updates(self):
    	
        try:
            tf.synchronize_broker_with_db()

            if not tn.last_telegram_update:
                tn.last_telegram_update = {"budget": Budget(),
                                             "balance": {}, "positions": {}}

                message = "Since this is the first request, this is the entire state:"
            else:
                message = "Here are the updates:"

            # Print updates to budget
            if tn.last_telegram_update["budget"] != tn.budget:
                budget_left = tn.budget.remaining_budget / tn.budget.max_budget
                message += f"\n\n\nBudget updates:" \
                           f"\n- Max budget: ${tn.budget.max_budget}" \
                           f"\n- Remaining budget: ${round(tn.budget.remaining_budget, 2)}" \
                           f"\n- Budget in Use: {round(100 * (1 - budget_left), 2)}%"

                tn.last_telegram_update["budget"] = tn.budget.deep_copy()

            # Print updates to balance
            if tn.last_telegram_update["balance"] != tf.trade_station.balance:
                message += f"\n\n\nBalance updates:"
                updates = {key: value for key, value in tf.trade_station.balance.items()
                           if tn.last_telegram_update["balance"].get(key) != value}
                for key, value in updates.items():
                    if isinstance(value, (int, float)):
                        value = round(value, 2)
                    message += f'\n- {key}: {value}'
                tn.last_telegram_update["balance"] = deepcopy(tf.trade_station.balance)

            # Print updates to positions
            symbols_changed = set()
            if tn.last_telegram_update["positions"] != tf.trade_station.positions:
                message += f"\n\n\nPosition updates:"
                positions = {symbol: position for symbol, position in tf.trade_station.positions.items()
                             if position['Quantity'] != 0}
                symbols_changed = set(positions.keys())

                updates = {symbol: {key: value for key, value in position.items() if
                                    tn.last_telegram_update["positions"].get(symbol, {}).get(key) != value}
                           for symbol, position in positions.items()
                           if tn.last_telegram_update["positions"].get(symbol) != position}

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

                tn.last_telegram_update["positions"] = deepcopy(tf.trade_station.positions)

            # Send updates to momentum
            message += "\n\n\nMomentum Updates:"
            momentum_symbols = {symbol for symbol in tf.trade_station.symbols() if
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
            max_budget = tf.budget.max_budget
            remaining_budget = tf.budget.remaining_budget
            budget_left = remaining_budget/max_budget
            return f"Max budget: ${max_budget}\n" \
                   f"Remaining budget: ${round(remaining_budget, 2)}\n" \
                   f"Budget in Use: {round(100 * (1 - budget_left), 2)}%"

        except Exception as exception:
            logger.exception(f"Exception in get_budget")
            return f"Hit exception in get_budget: {exception}"

    def get_balance(self):

        try:
            tf.synchronize_broker_with_db()

            message = 'Balance:'
            for key, value in tf.trade_station.balance.items():
                if isinstance(value, (int, float)):
                    value = round(value, 2)
                message += f'\n\t {key}: {value}'

            return message

        except Exception as exception:
            logger.exception(f"Exception in get_balance")
            return f"Hit exception in get_balance: {exception}"

    def get_orders(self):

        try:
            tf.synchronize_broker_with_db()

            message = 'Orders:'
            for order_id, order in tf.trade_station.orders.items():
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
            tf.synchronize_broker_with_db()

            message = 'Positions:'
            positions = {symbol: position for symbol, position in tf.trade_station.positions.items()
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
            start = tf.trade_history.purchase_time.min()
            end = datetime.datetime.now(tz=TIMEZONE)

            all_trading_prices = pd.DataFrame()
            for symbol in set(tf.trade_history.symbol):
                all_trading_prices[symbol] = tf.trade_station.collect_prices(symbol, start, end, 1, 'Daily', 'Default')

            liquidated_balance = get_equity(tf.trade_history, tf.budget.starting_budget, all_trading_prices)
            spy_data = tf.trade_station.collect_prices('SPY', start, end, 1, 'Daily', 'Default')

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

        tf.db.document(symbol).collection("model_parameter_history").document(str(update_date)).set(
            {"num_bins": num_bins, "distance_n": distance_n,
             "training_freq": training_freq, "training_length": training_length}, merge=True)

    def invest_retrain_call_back(self, values):
        self.model.invest.retrain_call_back(values)

        symbol, retrain_date, threshold, alpha, beta = values
        if not self.first_invest_train_date.get(symbol):
            self.first_invest_train_date[symbol] = retrain_date

        tf.db.document(symbol).collection("invest_parameter_history").document(str(retrain_date)).set(
            {"threshold": threshold, "alpha": alpha, "beta": beta}, merge=True)

    def update_params(self):
        while True:
            try:
                logger.info('Update thread checking if any hyper-parameters need to be updated')
                self.model.invest.simulation.budget.max_budget = tf.budget.max_budget
                self.model.invest.simulation.budget.remaining_budget = tf.budget.remaining_budget

                # Check we need to update params
                latest_date = tf.trade_station.get_latest_date_with_data()

                symbols_to_update_model_params = set()
                for symbol in tf.trade_station.symbols():
                    last_model_update_date = self.model.last_update_date.get(symbol)
                    days_since_last_update = np.busday_count(last_model_update_date, latest_date, holidays=HOLIDAYS)\
                        if last_model_update_date else float('inf')
                    if days_since_last_update > MODEL_RETRAINING_FREQ:
                        symbols_to_update_model_params.add(symbol)

                # Save parameters immediately after training finishes in case of a crash
                if symbols_to_update_model_params:
                    message = f"Updating {symbols_to_update_model_params} model parameters"
                    tn.send_telegram_message(message=message)
                    self.model.update_params(update_date=latest_date, symbols=symbols_to_update_model_params,
                                             call_back=self.model_update_call_back)

                symbols_to_update_invest_params = set()
                for symbol in tf.trade_station.symbols():
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
                next_closing_time = tf.trade_station.nyse.schedule(start_date=next_day,
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
                time_till_open = tf._time_to_open()
                if time_till_open.total_seconds() > 0:
                    logger.info(f'Monitor broker thread sleeping for {time_till_open}')
                    sleep(time_till_open.total_seconds())

                print('Monitor Broker Running')
                while True:
                    tf.synchronize_broker_with_db()
                    sleep(1.5)

            except Exception:
                logger.exception(f"Monitor Broker Crashed")