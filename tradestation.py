from helper import create_logger
logger = create_logger(file='debug_log.log')

from Tradestation_python_api.ts.client import TradeStationClient
import pandas as pd
import numpy as np
from datetime import datetime, date, time, timedelta
from time import sleep
import pandas_market_calendars as calendars
import logging
import json

from helper import datetime64_to_date
from constants import TIMEZONE, NUMBER_OF_QUERY_ATTEMPTS, HOLIDAYS, ORDER_STATUS_LEVELS
from config import clients

logging.getLogger("urllib3").setLevel(logging.WARNING)


class TradeStation:
    def __init__(self, client: str = None, paper: bool = True, symbols: set = None, interval: int = 1,
                 unit: str = "Minute", session: str = "USEQPreAndPost", verify_symbols: bool = False):
        if paper:
            client = 'Paper'

        if not symbols:
            symbols = set()

        details = clients[client]
        self.ts_client = TradeStationClient(
            username=details['Username'],
            client_id=details['ClientId'],
            client_secret=details['Secret'],
            redirect_uri="http://localhost",
            version=details['Version'],
            paper_trading=paper
        )

        self.account_name = [account['Name'] for account in self.ts_client.get_accounts(details['Username'])
                             if account['TypeDescription'] == details['AccountType']][0]

        self.interval = interval
        self.unit = unit
        self.session = session

        self.nyse = calendars.get_calendar("NYSE")
        if session == "USEQPre":
            self.nyse.change_time("market_open", time(8, 0))
        elif session == "USEQPost":
            self.nyse.change_time("market_close", time(20, 0))
        elif session == "USEQPreAndPost":
            self.nyse.change_time("market_open", time(8, 0))
            self.nyse.change_time("market_close", time(20, 0))
        elif session == "Default":
            pass
        else:
            raise Exception(f"Invalid session template ({session})")

        self.symbol_details = self.get_symbol_details(list(symbols))
        self.prices = {}

        if verify_symbols:
            self.verify_symbols(symbols=symbols)
        else:
            for symbol in symbols:
                self.prices[symbol] = pd.Series(dtype=float, index=pd.DatetimeIndex(data=[], tz=TIMEZONE))

        self.balance = {}
        self.orders = {}
        self.positions = {symbol: {'Quantity': 0} for symbol in self.symbols()}

    def symbols(self):
        return set(self.prices)

    def handle_dates(self, symbol: str, start: datetime or date,
                     end: datetime or date, interval: int, unit: str, session: str = None):
        start_date = start if type(start) is date else start.date()
        end_date = end if type(end) is date else end.date()

        if unit == 'Minute':
            unit = 'Min'
        elif unit == 'Daily':
            unit = 'D'
        elif unit == 'Weekly':
            unit = 'W'
        elif unit == 'Monthly':
            unit = 'M'

        if symbol in self.symbol_details:
            asset_type = self.symbol_details[symbol]['AssetType']
        else:
            asset_type = self.get_symbol_details(symbols=[symbol])[symbol]['AssetType']

        if asset_type == 'STOCK':
            if not np.is_busday(start_date, holidays=HOLIDAYS):
                start = datetime64_to_date(np.busday_offset(start_date, 0, roll='forward', holidays=HOLIDAYS))

            if not np.is_busday(end_date, holidays=HOLIDAYS):
                end = datetime64_to_date(np.busday_offset(end_date, 0, roll='backward', holidays=HOLIDAYS))

        if type(start) is date:
            start = TIMEZONE.localize(datetime.combine(date=start, time=time.min))
        else:
            start = start.astimezone(TIMEZONE)

        if type(end) is date:
            end = TIMEZONE.localize(datetime.combine(date=end, time=time.max))
        else:
            end = end.astimezone(TIMEZONE)

        if asset_type == 'STOCK':
            nyse = calendars.get_calendar("NYSE")
            if session == "USEQPre":
                nyse.change_time("market_open", time(8, 0))
            elif session == "USEQPost":
                nyse.change_time("market_close", time(20, 0))
            elif session == "USEQPreAndPost":
                nyse.change_time("market_open", time(8, 0))
                nyse.change_time("market_close", time(20, 0))
            elif session == "Default":
                pass
            else:
                raise Exception(f"Invalid session template ({session})")

            schedule = nyse.schedule(start_date=start.date(), end_date=end.date())
            date_range = calendars.date_range(schedule=schedule, frequency=f'{interval}{unit}').tz_convert(TIMEZONE)

        elif asset_type == 'CRYPTO':
            date_range = pd.date_range(start=start, end=end, freq=f'{interval}{unit}')

        else:
            raise Exception(f'{symbol} is a {asset_type}')

        if start > date_range[-1]:
            raise Exception(f'Start time is invalid: start = {start}, end = {end}')
        if end < date_range[0]:
            raise Exception(f'End time is invalid: start = {start}, end = {end}')

        start_index = date_range.get_indexer([start], method='bfill')[0]
        end_index = date_range.get_indexer([end], method='ffill')[0]
        start = date_range[start_index]
        end = date_range[end_index]
        time_steps = date_range[start_index: end_index + 1]

        return start, end, time_steps

    def collect_prices(self, symbol: str, start: datetime or date, end: datetime or date,
                       interval: int, unit: str, session: str):

        series = pd.Series(dtype=float, index=pd.DatetimeIndex(data=[], tz=TIMEZONE))
        start, end, time_steps = self.handle_dates(symbol=symbol, start=start, end=end,
                                                   interval=interval, unit=unit, session=session)
        print(f'Collecting prices from TradeStation {symbol}-{interval}{unit}:({start}, {end})')

        curr_start = start
        while len(time_steps) - time_steps.get_indexer([curr_start])[0] > 57600:
            temp_end_index = time_steps.get_indexer([curr_start])[0] + 57600 - 1
            temp_end = time_steps[temp_end_index]

            query = self._query_prices(symbol=symbol, start=curr_start, end=temp_end, interval=interval,
                                       unit=unit, session=session)
            series = pd.concat([series, query]).sort_index()
            curr_start = time_steps[temp_end_index + 1]

        query = self._query_prices(symbol=symbol, start=curr_start, end=end, interval=interval,
                                   unit=unit, session=session)
        series = pd.concat([series, query]).sort_index()

        # TODO delete because self.prices should already now have any holes
        percent = len(time_steps.difference(series.index)) / len(time_steps) * 100
        if percent > 10.0:
            logger.warning(f'Missing {percent}% of expected data {symbol}:({start}, {end}) in collect_prices')

        series = series.reindex(time_steps, method='nearest')
        return series

    def _query_prices(self, symbol: str, start: datetime or date, end: datetime or date, interval: int,
                      unit: str, session: str):
        start, end, time_steps = self.handle_dates(symbol=symbol, start=start, end=end,
                                                   interval=interval, unit=unit, session=session)
        print(f'Making query to TradeStation {symbol}-{interval}{unit}:({start}, {end})')

        series = pd.Series(dtype=float, index=pd.DatetimeIndex(data=[], tz=TIMEZONE))
        if unit == 'Minute':
            td = timedelta(minutes=interval)
        elif unit == 'Hourly':
            td = timedelta(hours=interval)
        elif unit == 'Daily':
            td = timedelta(days=interval)
        else:
            raise Exception('Did not implement case when unit is Weekly or Monthly,')

        query_start = end + td
        i = 0
        while i < NUMBER_OF_QUERY_ATTEMPTS:
            try:
                for bar in self.ts_client.get_bars(symbol=symbol, interval=interval, unit=unit, start_date=query_start,
                                                   bars_back=len(time_steps), session_template=session)['Bars']:
                    dt = datetime.fromisoformat(bar['TimeStamp'].replace('Z', '+00:00')).astimezone(TIMEZONE)
                    series.loc[dt] = float(bar['Close'])

                series = series.replace(0, np.nan).dropna().sort_index()

                percent = len(time_steps.difference(series.index)) / len(time_steps) * 100
                if percent > 10.0:
                    logger.warning(f'Missing {percent}% of expected data {symbol}-{interval}{unit}:({start}, {end})')

                series = series.reindex(time_steps, method='nearest')
                return series

            except json.decoder.JSONDecodeError:
                logger.warning(f'JSON Decode failure querying TradeStation {symbol}-{interval}{unit}:({start}, {end}): '
                               f'attempt {i + 1}/{NUMBER_OF_QUERY_ATTEMPTS}')

            except Exception as exception:
                logger.warning(f'Failed querying TradeStation {symbol}-{interval}{unit}:({start}, {end}): '
                               f'attempt {i + 1}/{NUMBER_OF_QUERY_ATTEMPTS}', exc_info=exception)

            i += 1
            sleep(15)

        logger.error(f'ALL TradeStation query attempts failed for {symbol}-{interval}{unit}:({start}, {end})')
        return pd.Series(dtype=float, index=pd.DatetimeIndex(data=[], tz=TIMEZONE))

    def get_key_prices(self, symbol: str, start: datetime or date, end: datetime or date):
        """
        Get closing prices from start (inclusive) to end (inclusive)
        :param symbol:
        :param start:
        :param end:
        :return:
        """
        start, end, time_steps = self.handle_dates(symbol=symbol, start=start, end=end,
                                                   interval=self.interval, unit=self.unit, session=self.session)
        print(f'Getting key prices for {symbol}:({start}, {end})')
        if start > end:
            return pd.Series(dtype=float)

        if start not in self.prices[symbol].index or end not in self.prices[symbol].index:
            query_start = start
            query_end = end
            if not self.prices[symbol].empty:
                if start < self.prices[symbol].index[0]:
                    query_end = self.prices[symbol].index[0]
                else:
                    query_start = self.prices[symbol].index[-1]

            if query_start > query_end:
                print(f'Weird output {start}, {end}')

            collected_prices = self.collect_prices(symbol=symbol, start=query_start, end=query_end,
                                                   interval=self.interval, unit=self.unit, session=self.session)
            new_prices = pd.concat([self.prices[symbol], collected_prices]).sort_index()
            duplicates = new_prices.index.duplicated(keep='first')
            self.prices[symbol] = new_prices.loc[~duplicates]

        # fix bug when second side of operand is being computed a price is added causing a size mismatch
        prices = self.prices[symbol].copy()
        prices = prices.loc[(start <= prices.index) & (prices.index <= end)]

        # TODO delete because self.prices should already fill in holes
        percent = len(time_steps.difference(prices.index)) / len(time_steps) * 100
        if percent > 10.0:
            logger.warning(f'Missing {percent}% of expected data {symbol}:({start}, {end}) in get_key_prices')

        prices = prices.reindex(time_steps, method='nearest')
        return prices

    def verify_symbols(self, symbols: set):
        """
        verifies that the symbols can be properly queried in TradeStation
        :param symbols:
        :return:
        """
        logger.info(f'Collecting valid symbols from {symbols}')

        latest_date = self.get_latest_date_with_data()

        for symbol in symbols:
            try:
                self.prices[symbol] = pd.Series(dtype=float, index=pd.DatetimeIndex(data=[], tz=TIMEZONE))
                prices = self.collect_prices(symbol=symbol, start=latest_date, end=latest_date, interval=self.interval,
                                             unit=self.unit, session=self.session)

                if prices.isnull().iloc[-1]:
                    raise Exception('Collected no historical price data')

            except Exception as error:
                self.prices.pop(symbol, None)
                logger.error(f"Error finding {symbol}: {error.args}")

        logger.info(f'Finished collecting valid symbols')

    def get_symbol_details(self, symbols: list) -> dict:
        if len(symbols) == 0:
            return dict()

        for i in range(NUMBER_OF_QUERY_ATTEMPTS):
            try:
                query = self.ts_client.get_symbol_details(symbols)
                if len(query["Errors"]) != 0:
                    raise Exception(query["Errors"])

                return {e.pop('Symbol'): e for e in query['Symbols']}

            except Exception as exception:
                logger.warning(f'Failed getting symbol details: attempt {i}/{NUMBER_OF_QUERY_ATTEMPTS}',
                               exc_info=exception)
                sleep(5)

        logger.error('Get symbol details failed')

    def get_balances(self):
        for i in range(NUMBER_OF_QUERY_ATTEMPTS):
            try:
                query = self.ts_client.get_balances(account_keys=[self.account_name])
                if len(query["Errors"]) != 0:
                    raise Exception(query["Errors"])
                query = query["Balances"][0]

                balance = dict()
                balance["AccountID"] = query.get("AccountID")
                balance["BuyingPower"] = float(query.get("BuyingPower"))
                balance["CashBalance"] = float(query.get("CashBalance"))
                balance["Equity"] = float(query.get("Equity", 0))
                balance["MarketValue"] = float(query.get("MarketValue", 0))
                balance["TodaysPL"] = float(query.get("TodaysProfitLoss", 0))
                # balance["CostOfPositions"] = float(query.get("BalanceDetail", {}).get("CostOfPositions", 0))
                # TODO get cost of positions like above
                balance['TotalCost'] = sum([position['TotalCost'] for position in self.get_positions().values()])

                balance["RealizedPL"] = float(query.get("BalanceDetail", {}).get("RealizedProfitLoss", 0))
                balance["UnrealizedPL"] = float(query.get("BalanceDetail", {}).get("UnrealizedProfitLoss", 0))

                bod_query = self.ts_client.get_balances_bod(account_keys=[self.account_name])
                if len(bod_query["Errors"]) != 0:
                    raise Exception(bod_query["Errors"])
                bod_query = bod_query["BODBalances"][0]

                balance["BODEquity"] = float(bod_query.get("BalanceDetail", {}).get("Equity", 0))

                return balance

            except Exception as exception:
                logger.warning(f'Failed getting balance: attempt {i}/{NUMBER_OF_QUERY_ATTEMPTS}', exc_info=exception)
                sleep(5)

        logger.error('Get account balance failed')
        return None

    def get_orders(self):
        for i in range(NUMBER_OF_QUERY_ATTEMPTS):
            try:
                query = self.ts_client.get_historical_orders(account_keys=[self.account_name], since=14)
                if len(query["Errors"]) != 0:
                    raise Exception(query["Errors"])
                order_list = query["Orders"]

                query = self.ts_client.get_orders(account_keys=[self.account_name])
                if len(query["Errors"]) != 0:
                    raise Exception(query["Errors"])
                order_list = query["Orders"]

                orders = {}
                for q in order_list:
                    order = dict()
                    # if "GoodTillDate" in q:
                    #     order["GoodTillTime"] = datetime.fromisoformat(q["GoodTillDate"]
                    #                                                    .replace('Z', '+00:00')).astimezone(TIMEZONE)
                    if "OpenedDateTime" in q:
                        order["OpenedTime"] = datetime.fromisoformat(q["OpenedDateTime"]
                                                                     .replace('Z', '+00:00')).astimezone(TIMEZONE)
                    else:
                        order["OpenedTime"] = None

                    if "ClosedDateTime" in q:
                        order["ClosedTime"] = datetime.fromisoformat(q["ClosedDateTime"]
                                                                     .replace('Z', '+00:00')).astimezone(TIMEZONE)
                    else:
                        order["ClosedTime"] = None

                    if "ClosedTime" in order:
                        order["TimeStamp"] = order["ClosedTime"]
                    elif "OpenedTime" in order:
                        order["TimeStamp"] = order["OpenedTime"]
                    else:
                        order["TimeStamp"] = order["GoodTillTime"]

                    order["FilledPrice"] = float(q.get("FilledPrice", 0))
                    order["LimitPrice"] = float(q.get("LimitPrice", 0))
                    order["Status"] = q.get("StatusDescription")
                    order["StopPrice"] = float(q.get("StopPrice", 0))
                    order["Symbol"] = q["Legs"][0]["Symbol"]
                    order["Quantity"] = int(q["Legs"][0]["QuantityOrdered"])
                    order["Type"] = q["Legs"][0]["BuyOrSell"]

                    orders[q.get("OrderID")] = order

                return orders

            except Exception as exception:
                logger.warning(f'Failed getting orders: attempt {i}/{NUMBER_OF_QUERY_ATTEMPTS}', exc_info=exception)
                sleep(5)

        logger.error('Get account orders failed')
        return None

    def get_positions(self):
        for i in range(NUMBER_OF_QUERY_ATTEMPTS):
            try:
                query = self.ts_client.get_positions(account_keys=[self.account_name])
                if len(query["Errors"]) != 0:
                    raise Exception(query["Errors"])

                query = query["Positions"]
                positions = {}

                for element in query:
                    pos = dict()
                    pos["AveragePrice"] = float(element.get("AveragePrice", 0))
                    pos["LatestPrice"] = float(element.get("Last", 0))
                    pos["MarketValue"] = float(element.get("MarketValue", 0))
                    pos["Quantity"] = int(element.get("Quantity", 0))
                    pos["TodaysUnrealizedPL($)"] = float(element.get("TodaysProfitLoss", 0))
                    pos["TotalCost"] = float(element.get("TotalCost", 0))
                    pos["UnrealizedPL($)"] = float(element.get("UnrealizedProfitLoss", 0))
                    pos["UnrealizedPL(%)"] = float(element.get("UnrealizedProfitLossPercent", 0))
                    pos["UnrealizedPL(qty)"] = float(element.get("UnrealizedProfitLossQty", 0))

                    positions[element.get('Symbol')] = pos

                return positions

            except Exception as exception:
                logger.warning(f'Failed getting positions: attempt {i}/{NUMBER_OF_QUERY_ATTEMPTS}', exc_info=exception)
                sleep(5)

        logger.error('Get account positions failed')
        return None

    def get_account_updates(self):
        """
        Updates balance, orders, and positions
        :return: updates to orders
        """

        # Dealing with balance
        self.balance = self.get_balances()

        # Dealing with orders
        orders = self.get_orders()

        # Get orders that status have changed and sort by timestamp
        # order_changes = [(order["TimeStamp"], order_id) for order_id, order in orders.items()
        #     if order_id not in self.orders or
        #     (
        #         ORDER_STATUS_LEVELS[order["Status"]] >= ORDER_STATUS_LEVELS[self.orders[order_id]["Status"]]
        #         and order['TimeStamp'] > self.orders[order_id]['TimeStamp']
        #      )
        #     ]
        # order_changes.sort()
        # changed_ids = [change[1] for change in order_changes]
        self.orders = orders

        # Dealing with positions
        self.positions = self.get_positions()
        self.positions.update({symbol: {'Quantity': 0} for symbol in self.symbols() if symbol not in self.positions})

        # return changed_ids

    def submit_market_order(self, symbol: str, qty: int, order_type: str):
        if qty <= 0:
            logger.warning(f"Quantity is 0: {order_type} order of {symbol} did not follow through")
            return None

        try:
            if order_type == "SELL" and self.positions[symbol]['Quantity'] < qty:
                logger.error(f"Order failed: trying to sell {qty} shares of {symbol} but only "
                             f"{self.positions[symbol]['Quantity']} shares owned")
                return None

            response = self.ts_client.place_order(account_key=self.account_name, symbol=symbol, trade_action=order_type,
                                                  quantity=qty, order_type="Market", duration="DAY")
            if "Errors" in response:
                logger.warning(f"{order_type} {qty} shares of {symbol} order failed "
                               f"\n\tREASON: {response['Errors']}")
                return None
            else:
                logger.info(f"{order_type} {qty} shares of {symbol} ordered")
                return response["Orders"][0]

        except Exception as exception:
            logger.exception(f"{order_type} {qty} shares of {symbol} order failed", exc_info=exception)
            return None

    def submit_limit_order(self, symbol: str, limit_price: float, qty: int, order_type: str):
        if qty <= 0:
            logger.warning(f"Quantity is 0: {order_type} order of {symbol} did not follow through")
            return None

        try:
            if order_type == "SELL" and self.positions[symbol]['Quantity'] < qty:
                logger.error(f"Order failed: trying to sell {qty} shares of {symbol} but only "
                             f"{self.positions[symbol]['Quantity']} shares owned")
                return None

            response = self.ts_client.place_order(account_key=self.account_name, symbol=symbol, trade_action=order_type,
                                                  quantity=qty, order_type="Limit", duration="DYP",
                                                  LimitPrice=str(limit_price))
            if "Errors" in response:
                logger.warning(f"{order_type} {qty} shares of {symbol} order failed "
                               f"\n\tREASON: {response['Errors']}")
                return None
            else:
                logger.info(f"{order_type} {qty} shares of {symbol} ordered")
                return response["Orders"][0]

        except Exception as exception:
            logger.exception(f"{order_type} {qty} shares of {symbol} order failed", exc_info=exception)
            return None

    def get_latest_date_with_data(self) -> date:
        now = datetime.now(tz=TIMEZONE)
        latest_date = now.date()

        if not np.is_busday(latest_date, holidays=HOLIDAYS):
            latest_date = datetime64_to_date(np.busday_offset(latest_date, 0, roll='backward', holidays=HOLIDAYS))

        elif now < self.nyse.schedule(start_date=now.date(), end_date=now.date()).market_close[0]:
            latest_date = datetime64_to_date(np.busday_offset(latest_date, -1, holidays=HOLIDAYS))

        return latest_date


def main():
    trade_station = TradeStation(client='Paper', paper=True)
    print(trade_station.get_balances())
    for symbol, details in trade_station.get_positions().items():
        print(symbol)
        print(details)

    for order, details in trade_station.get_orders().items():
        print(order)
        print(details)


if __name__ == '__main__':
    main()
