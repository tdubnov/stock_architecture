import pytz
from datetime import time, datetime
from pandas_market_calendars import get_calendar
import pandas as pd
import logging
import math

LOG_LEVEL = logging.DEBUG
TIMEZONE = pytz.timezone("US/Eastern")

OPENING_TIME = time(hour=9, minute=30)
CLOSING_TIME = time(hour=16)
HOLIDAYS = get_calendar("NYSE").holidays().holidays

MAX_TIME = pd.Timestamp.max
MIN_TIME = TIMEZONE.localize(datetime(2, 1, 1))  # pd.Timestamp.min

NUMBER_OF_QUERY_ATTEMPTS = 5
TIMEFRAME = "Minute"

TRADE_HISTORY_COLUMNS = ["symbol", "quantity", "sell_threshold", "purchase_time",
                         "sold_time", "purchase_price", "sold_price", "status", "above_threshold"]
ORDER_HISTORY_COLUMNS = ["symbol", "quantity", "type", "trade_ids", "opened_time", "closed_time", "price", "status"]
# SIMULATION_COLUMNS = {SYMBOL: "", QUANTITY: int(), VIRTUAL_PRICE: float(), SELL_THRESHOLD: float(),
#                       PURCHASE_TIME: datetime.now(), SOLD_TIME: datetime.now(), PURCHASE_PRICE: float(),
#                       SOLD_PRICE: float(), STATUS: ""}

FAILED_STATUSES = ["Broken", "Canceled", "Expired", "Rejected", "Trade Server Canceled", "Unsent", "UROut"]
ALIVE_STATUSES = ["Sent", "Sending", "Received", "Queued", "Alive", "Cancel Pending"]
FILLED_STATUSES = ["Filled", "Too Late To Cancel", "Cancel Rejected"]
PARTIALLY_FILLED_STATUSES = ["Partially Filled (UROut)", "Partially Filled (Alive)"]

ORDER_STATUS_LEVELS = {
    'Sent': 1,
    'Cancel Sent': 1,
    'Replace Sent': 1,

    'Condition Met': 2,
    'OSO Order': 2,
    'Received': 2,
    'Partial Fill (Alive)': 2,
    'Queued': 2,

    'Broken': 3,
    'Cancelled': 3,
    'Expired': 3,
    'Filled': 3,
    'Partial Fill (UROut)': 3,
    'Too Late to Cancel': 3,
    'UROut': 3,
    'Rejected': 3,
    'Replaced': 3,
    'Trade Server Canceled': 3,
    'Cancel Request Rejected': 3,
    'Suspended': 3
}