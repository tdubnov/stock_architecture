import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
from time import sleep
import threading
import logging
import os
from concurrent_log_handler import ConcurrentRotatingFileHandler

from constants import LOG_LEVEL, HOLIDAYS, TIMEZONE, TRADE_HISTORY_COLUMNS, MAX_TIME

experiment_length = 20


class CustomErrorHandler(logging.StreamHandler):
    def __init__(self, send_telegram_message):
        super().__init__()
        self.errors = []
        self.error_thread_alive = False
        self.send_telegram_message = send_telegram_message

    def error_overload(self):
        while self.error_thread_alive:
            self.send_telegram_message(
                message=r'Hit 5 or more errors in the last 15 minutes. Please notice me Senpai '
                        r'and respond \error_acknowledged')
            sleep(15 * 60)
        # self.error_thread_alive = False

    def emit(self, record):
        if record.levelno in [logging.ERROR, logging.CRITICAL]:
            self.errors.append(record)

            curr_time = datetime.now()
            for error in self.errors:
                error_time = datetime.fromisoformat(error.asctime.replace(',', ':'))
                if curr_time - error_time > timedelta(minutes=15):
                    self.errors.remove(error)

            if len(self.errors) >= 5 and not self.error_thread_alive:
                self.error_thread_alive = True
                error_thread = threading.Thread(target=self.error_overload)
                error_thread.daemon = False
                error_thread.start()

        super().emit(record)


def create_logger(file: str):
    logger = logging.getLogger()
    if logger.handlers:
        return logger

    # Use an absolute path to prevent file rotation trouble.
    logfile = os.path.abspath(file)
    logger.setLevel(LOG_LEVEL)

    # Rotate log after reaching 512K, keep 5 old copies.
    file_handler = ConcurrentRotatingFileHandler(
        logfile, mode="a", maxBytes=512 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setLevel(LOG_LEVEL)

    # create also handler for displaying output in the stdout
    ch = logging.StreamHandler()
    ch.setLevel(LOG_LEVEL)

    formatter = logging.Formatter(
        '[%(asctime)s | %(module)s:%(lineno)d | %(levelname)s | %(processName)s] %(message)s'
    )

    # add formatter to ch
    ch.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(file_handler)

    return logger


# Wrapper class for int
class Budget:
    def __init__(self, starting_budget: float = -1):
        self.starting_budget = starting_budget
        self.max_budget = starting_budget
        self.remaining_budget = starting_budget
        self.budget_date = None

    def increase_max_budget(self, increase: float):
        self.max_budget += increase

    def update_remaining_budget(self, update: float, ts_client=None):
        if ts_client:
            balance = ts_client.get_balances()
            if balance:
                self.remaining_budget = balance["BuyingPower"]
            else:
                self.remaining_budget += update
        else:
            self.remaining_budget += update

    def __eq__(self, other):
        if not isinstance(other, Budget):
            return False

        return (self.starting_budget == other.starting_budget) & (self.max_budget == other.max_budget) & \
               (self.remaining_budget == other.remaining_budget)

    def deep_copy(self):
        copy = Budget()
        copy.starting_budget = self.starting_budget
        copy.max_budget = self.max_budget
        copy.remaining_budget = self.remaining_budget
        return copy


def get_open_trades(trade_history: pd.DataFrame, curr_time: pd.Timestamp) -> pd.DataFrame:
    return trade_history.loc[(trade_history.purchase_time <= curr_time) &
                             ((trade_history.status == 'own') | (trade_history.sold_time > curr_time))]


def get_money_in_market_series(trade_history: pd.DataFrame) -> pd.Series:
    money_in_market_series = pd.Series(dtype=float)
    for index, row in trade_history.iterrows():
        open_trades_at_purchase = get_open_trades(trade_history=trade_history, curr_time=row.purchase_time)
        money_in_market_series.loc[row.purchase_time] = sum(open_trades_at_purchase.purchase_price *
                                                            open_trades_at_purchase.quantity)

        if row.status == 'sold':
            open_trades_at_sold = get_open_trades(trade_history=trade_history, curr_time=row.sold_time)
            money_in_market_series.loc[row.sold_time] = sum(open_trades_at_sold.purchase_price *
                                                            open_trades_at_sold.quantity)
    return money_in_market_series.sort_index()


def get_remaining_budget_series(trade_history: pd.DataFrame, starting_budget: float) -> pd.Series:
    remaining_budget = pd.Series(dtype=float)
    remaining_budget.loc[0] = starting_budget
    purchase_times = trade_history.purchase_time
    sold_times = trade_history.sold_time[trade_history.status == 'sold'].dropna().drop_duplicates().sort_values()
    key_times = purchase_times.append(sold_times, ignore_index=True).drop_duplicates().sort_values()
    for curr_time in key_times:
        # curr_time = curr_time.to_pydatetime()
        purchases_at_curr_time = trade_history[trade_history.purchase_time == curr_time]
        cost_of_purchases = (purchases_at_curr_time.purchase_price * purchases_at_curr_time.quantity).sum()
        sells_at_curr_time = trade_history[trade_history.sold_time == curr_time]
        income_of_sells = (sells_at_curr_time.sold_price * sells_at_curr_time.quantity).sum()
        remaining_budget.loc[curr_time] = remaining_budget.iloc[-1] - cost_of_purchases + income_of_sells

    return remaining_budget.iloc[1:]


def get_max_budget_series(trade_history: pd.DataFrame, starting_budget: float) -> pd.Series:
    max_budget = pd.Series(dtype=float)
    max_budget.loc[trade_history.purchase_time.min()] = starting_budget
    sold_times = trade_history.sold_time[trade_history.status == 'sold'].dropna().drop_duplicates().sort_values()
    for curr_time in sold_times:
        sells_at_curr_time = trade_history[trade_history.sold_time == curr_time]
        profit_of_sells = sum((sells_at_curr_time.sold_price - sells_at_curr_time.purchase_price)
                              * sells_at_curr_time.quantity)
        max_budget.loc[curr_time] = max_budget.iloc[-1] + profit_of_sells

    return max_budget


# def get_liquidated_balance_series(trade_history: pd.DataFrame, starting_budget: float,
#                                   all_trading_prices: pd.DataFrame) -> pd.Series:
#     closed_profits = pd.Series(dtype=float)
#     closed_profits.loc[all_trading_prices.index[0]] = 0
#
#     sold_times = trade_history[trade_history.status == 'sold'].sold_time.drop_duplicates().sort_values()
#     for curr_time in sold_times:
#         closed_trades = trade_history.loc[trade_history.sold_time == curr_time]
#         profit = sum((closed_trades.sold_price - closed_trades.purchase_price) * closed_trades.quantity)
#         closed_profits.loc[curr_time] = closed_profits.iloc[-1] + profit
#
#     key_times = trade_history.purchase_time.append(sold_times, ignore_index=True).drop_duplicates().sort_values()
#     open_trades_series = pd.Series(dtype='object')
#     open_trades_series.loc[all_trading_prices.index[0]] = pd.DataFrame(columns=TRADE_HISTORY_COLUMNS)
#     for curr_time in key_times:
#         open_df = trade_history.loc[(trade_history.purchase_time <= curr_time) &
#                                     ((trade_history.status == 'own') | (trade_history.sold_time > curr_time))]
#         open_trades_series.loc[curr_time] = open_df
#
#     balance = pd.Series(dtype=float)
#     balance.loc[all_trading_prices.index[0]] = starting_budget
#     for curr_time, prices in all_trading_prices.iterrows():
#         closed_profit_index = closed_profits.index.get_indexer([curr_time], method='ffill')[0]
#         profit = closed_profits[closed_profit_index]
#
#         open_trades_index = open_trades_series.index.get_indexer([curr_time], method='ffill')[0]
#         open_trades = open_trades_series[open_trades_index]
#         cost_of_open_trades = (open_trades.quantity*open_trades.purchase_price).sum()
#         liquidated_value = 0 if open_trades.empty \
#             else open_trades.apply(lambda x: x.quantity*prices[x.symbol], axis=1).sum()
#
#         balance.loc[curr_time] = starting_budget + profit + liquidated_value - cost_of_open_trades
#
#     return balance


def get_symbol_liquidated_value_series(trade_history: pd.DataFrame, symbol: str,
                                       trading_prices: pd.Series) -> pd.Series:
    trade_history = trade_history[trade_history.symbol == symbol]

    purchase_times = trade_history.purchase_time
    sold_times = trade_history.sold_time[trade_history.sold_time != MAX_TIME].dropna().drop_duplicates().sort_values()
    key_times = purchase_times.append(sold_times, ignore_index=True).drop_duplicates().sort_values()
    open_trades_series = pd.Series(dtype='object')
    open_trades_series.loc[trading_prices.index[0]] = pd.DataFrame(columns=TRADE_HISTORY_COLUMNS)
    for curr_time in key_times:
        # curr_time = curr_time.to_pydatetime()
        open_df = trade_history.loc[(trade_history.purchase_time <= curr_time) &
                                    ((trade_history.status == 'own') | (trade_history.sold_time > curr_time))]
        open_trades_series.loc[curr_time] = open_df

    open_value = pd.Series(dtype=float)
    open_value.loc[trading_prices.index[0]] = 0
    for curr_time, price in trading_prices.iteritems():
        # curr_time = curr_time.to_pydatetime()

        open_trades_index = open_trades_series.index.get_indexer([curr_time], method='ffill')[0]
        open_trades = open_trades_series[open_trades_index]
        liquidated_value = open_trades.quantity.sum()*price

        open_value.loc[curr_time] = liquidated_value

    return open_value


def get_realized_profit(trade_history: pd.DataFrame) -> pd.Series:
    realized_profit_series = pd.Series(dtype=float)
    realized_profit_series.loc[trade_history.purchase_time.min()] = 0

    sold_times = trade_history.sold_time[trade_history.status == 'sold'].dropna().drop_duplicates().sort_values()
    for curr_time in sold_times:
        closed_trades = trade_history.loc[trade_history.sold_time == curr_time]
        profit = ((closed_trades.sold_price - closed_trades.purchase_price)
                  * closed_trades.quantity).sum()
        realized_profit_series.loc[curr_time] = realized_profit_series.iloc[-1] + profit

    return realized_profit_series


def get_equity(trade_history: pd.DataFrame, starting_budget: float, all_trading_prices: pd.DataFrame) -> pd.Series:
    start_time = min(all_trading_prices.index[0], trade_history.purchase_time.min()) - timedelta(minutes=1)
    closed_profits = pd.Series(dtype=float)
    closed_profits.loc[start_time] = 0

    sold_times = trade_history.sold_time[trade_history.status == 'sold'].dropna().drop_duplicates().sort_values()
    for curr_time in sold_times:
        closed_trades = trade_history.loc[trade_history.sold_time == curr_time]
        profit = sum((closed_trades.sold_price - closed_trades.purchase_price) * closed_trades.quantity)
        closed_profits.loc[curr_time] = closed_profits.iloc[-1] + profit

    key_times = pd.concat([trade_history.purchase_time, sold_times]).drop_duplicates().sort_values()
    open_trades_series = pd.Series(dtype='object')
    open_trades_series.loc[start_time] = pd.DataFrame(columns=TRADE_HISTORY_COLUMNS)
    for curr_time in key_times:
        open_trades = get_open_trades(trade_history=trade_history, curr_time=curr_time)
        open_trades_series.loc[curr_time] = open_trades

    balance = pd.Series(dtype=float)
    balance.loc[start_time] = 0
    for curr_time, prices in all_trading_prices.iterrows():
        closed_profit_index = closed_profits.index.get_indexer([curr_time], method='ffill')[0]
        profit = closed_profits[closed_profit_index]

        open_trades_index = open_trades_series.index.get_indexer([curr_time], method='ffill')[0]
        open_trades = open_trades_series[open_trades_index]
        cost_of_open_trades = (open_trades.quantity*open_trades.purchase_price).sum()
        liquidated_value = 0 if open_trades.empty \
            else open_trades.apply(lambda x: x.quantity*prices[x.symbol], axis=1).sum()

        balance.loc[curr_time] = profit + liquidated_value - cost_of_open_trades

    return balance + starting_budget


def get_market_value(trade_history: pd.DataFrame, all_trading_prices: pd.DataFrame) -> pd.Series:
    sold_times = trade_history.sold_time[trade_history.status == 'sold'].dropna().drop_duplicates().sort_values()
    key_times = trade_history.purchase_time.append(sold_times, ignore_index=True).drop_duplicates().sort_values()
    open_trades_series = pd.Series(dtype='object')
    open_trades_series.loc[all_trading_prices.index[0]] = pd.DataFrame(columns=TRADE_HISTORY_COLUMNS)
    for curr_time in key_times:
        open_trades = get_open_trades(trade_history=trade_history, curr_time=curr_time)
        open_trades_series.loc[curr_time] = open_trades

    market_value = pd.Series(dtype=float)
    market_value.loc[all_trading_prices.index[0]] = 0
    for curr_time, prices in all_trading_prices.iterrows():
        open_trades_index = open_trades_series.index.get_indexer([curr_time], method='ffill')[0]
        open_trades = open_trades_series[open_trades_index]
        value = 0 if open_trades.empty else open_trades.apply(lambda x: x.quantity*prices[x.symbol], axis=1).sum()

        market_value.loc[curr_time] = value

    return market_value


# def get_symbol_liquidated_value_series(trade_history: pd.DataFrame, symbol: str,
#                                        trading_prices: pd.Series) -> pd.Series:
#     symbol_trade_history = trade_history[trade_history.symbol == symbol]
#
#     purchase_times = symbol_trade_history.purchase_time
#     sold_times = trade_history.sold_time[trade_history.sold_time == 'sold'].dropna().drop_duplicates().sort_values()
#     key_times = purchase_times.append(sold_times, ignore_index=True).drop_duplicates().sort_values()
#     open_trades_series = pd.Series(dtype='object')
#     open_trades_series.loc[trading_prices.index[0]] = pd.DataFrame(columns=TRADE_HISTORY_COLUMNS)
#     for curr_time in key_times:
#         # curr_time = curr_time.to_pydatetime()
#         open_trades = get_open_trades(trade_history=trade_history, curr_time=curr_time)
#         open_trades_series.loc[curr_time] = open_trades
#
#     open_value = pd.Series(dtype=float)
#     open_value.loc[trading_prices.index[0]] = 0
#     for curr_time, price in trading_prices.iteritems():
#         # curr_time = curr_time.to_pydatetime()
#
#         open_trades_index = open_trades_series.index.get_indexer([curr_time], method='ffill')[0]
#         open_trades = open_trades_series[open_trades_index]
#         liquidated_value = open_trades.quantity.sum()*price
#
#         open_value.loc[curr_time] = liquidated_value
#
#     return open_value


def datetime64_to_date(datetime64: np.datetime64):
    return pd.Timestamp(datetime64).to_pydatetime().date()


def market_open_at_time(dt: datetime):
    if not np.is_busday(dt.date(), holidays=HOLIDAYS):
        return False

    if time(hour=9, minute=30) <= dt.time() <= time(hour=20):
        return True

    return False


def market_open_regular_hours():
    now = datetime.now(tz=TIMEZONE)
    if not np.is_busday(now.date(), holidays=HOLIDAYS):
        return False

    if time(hour=9, minute=30) <= now.time() <= time(hour=16):
        return True

    return False


def market_open_after_hours():
    now = datetime.now(tz=TIMEZONE)
    if not np.is_busday(now.date(), holidays=HOLIDAYS):
        return False

    if time(hour=9, minute=30) <= now.time() <= time(hour=20):
        return True

    return False


def negative_average(x):
    return -np.average(x)


def positive_momentum(prev_momentum, current_momentum):
    return prev_momentum < 0 < current_momentum

def negative_momentum(prev_momentum, current_momentum):
    return prev_momentum > 0 > current_momentum

# def await_market_open(self):
    #     """ Return false if market is open, else sleep until market opens and return true
    #     """
    #
    #     if self.market_open():
    #         return True
    #
    #     logging.info("Waiting for market to open...")
    #     while not self.api.get_clock().is_open:
    #         clock = self.api.get_clock()
    #         opening_time = clock.next_open.tz_convert(UTC)
    #         current_time = clock.timestamp.tz_convert(UTC)
    #         time_to_open = opening_time - current_time
    #         logging.info(f'Time until market opens: {time_to_open.components}')
    #         sleep_time = min(time_to_open.seconds + 1, 30*60)
    #         logging.info(f'Sleep for {sleep_time} seconds')
    #         time.sleep(sleep_time)    # Sleep 30 minutes or timeToOpen
    #
    #     logging.info("Market opened")
    #     return False
