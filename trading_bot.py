import os
import time
import datetime as dt
import math
import gzip
import json
import pandas as pd
import threading
import asyncio
import websockets  # pip install websockets
import numpy as np
import talib  # For technical indicators
import logging
from collections import defaultdict
import aiohttp  # For asynchronous HTTP calls
import requests  # For synchronous calls

# Upstox SDK imports
from upstox_client.configuration import Configuration
from upstox_client.api_client import ApiClient
from upstox_client.rest import ApiException
from upstox_client.api.market_quote_api import MarketQuoteApi
from upstox_client.api.websocket_api import WebsocketApi as WSApi

# Protobuf-generated classes from MarketDataFeedV3.proto
import MarketDataFeedV3_pb2 as pb

# ----------------------------------------------------------------------------
# LOGGING CONFIGURATION
# ----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# ----------------------------------------------------------------------------
# GLOBAL DATA STRUCTURES
# ----------------------------------------------------------------------------
live_data = defaultdict(dict)      # Latest tick per instrument (from WebSocket)
local_history = defaultdict(list)   # Aggregated tick/candle history per instrument

trade_open = False
current_trade = {}

stop_bot = False  # Flag to fully stop the bot

# Strategy/Indicator settings
CANDLE_INTERVAL = '3Min'  # Desired candle resolution
ATR_PERIOD = 14
RVOL_PERIOD = 20
breakeven_offset = 2

# Upstox credentials (replace with your actual tokens/keys)
ACCESS_TOKEN = "YOUR_ACCESS_TOKEN_HERE"
API_KEY = "62c659ea-8004-48ef-8f7f-8c86f9d73995"

# Instrument file details
INSTRUMENT_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
LOCAL_INSTRUMENT_FILE = r"C:\Users\trading_bot\NSE.json.gz"

# Unsubscription time (24-hour format)
UNSUBSCRIBE_TIME = dt.time(15, 14)

# Strike filtering parameters – 10 strikes below ATM + ATM + 10 strikes above ATM (total 21 strikes)
INTERVAL = 50
STRIKES_BELOW = 10
STRIKES_ABOVE = 10

force_unsubscribe_flag = False  # For graceful shutdown

# Dictionary: instrument_key -> {friendly_name, option_type}
instrument_names = {}

# ----------------------------------------------------------------------------
# ASYNC RETRY DECORATOR
# ----------------------------------------------------------------------------
async def async_retry(coro_func, retries=3, delay=1, backoff=2, *args, **kwargs):
    attempt = 0
    while attempt < retries:
        try:
            return await coro_func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Error in {coro_func.__name__}: {e} (attempt {attempt+1}). Args: {args}, Kwargs: {kwargs}")
            await asyncio.sleep(delay)
            delay *= backoff
            attempt += 1
    raise Exception(f"{coro_func.__name__} failed after {retries} attempts.")

# ----------------------------------------------------------------------------
# HOLIDAY API FUNCTIONS
# ----------------------------------------------------------------------------
def is_trading_day(date_to_check: dt.date) -> bool:
    """
    Queries Upstox's holiday API for a specific date.
    If the holiday data indicates a 'TRADING_HOLIDAY', then the day is not a trading day.
    """
    url = f"https://api.upstox.com/v2/market/holidays/{date_to_check.strftime('%Y-%m-%d')}"
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Accept': 'application/json'
    }
    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            holidays = data.get("data", [])
            # If any holiday in the response is a TRADING_HOLIDAY, return False.
            for holiday in holidays:
                if holiday.get("holiday_type") == "TRADING_HOLIDAY":
                    return False
            return True
        else:
            logging.error(f"Holiday API error for {date_to_check}: {resp.status_code} {resp.text}")
            # Fallback: assume trading day
            return True
    except Exception as e:
        logging.error(f"Exception in holiday API for {date_to_check}: {e}")
        return True

def get_last_trading_day():
    """
    Determines the most recent trading day by checking the holiday API.
    Tries up to 7 days back.
    """
    candidate_day = dt.datetime.now().date() - dt.timedelta(days=1)
    for _ in range(7):
        if is_trading_day(candidate_day):
            return candidate_day
        candidate_day -= dt.timedelta(days=1)
    return candidate_day

# ----------------------------------------------------------------------------
# HISTORICAL BACKFILL
# ----------------------------------------------------------------------------
def historical_backfill(instrument_keys):
    """
    For each instrument in instrument_keys, fetch historical 1-minute candle data for the last trading day.
    Then, aggregate the 1-minute candles into 3-minute candles and store them in local_history.
    """
    last_trading_day = get_last_trading_day()
    start_time = dt.datetime.combine(last_trading_day, dt.time(9, 15))
    end_time = dt.datetime.combine(last_trading_day, dt.time(15, 30))
    
    url = "https://api.upstox.com/v2/historical"  # Verify with latest docs
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Accept': 'application/json'
    }
    
    for instrument in instrument_keys:
        params = {
            "instrument_key": instrument,
            "interval": "1minute",
            "from_date": start_time.strftime("%Y-%m-%d"),
            "to_date": end_time.strftime("%Y-%m-%d")
        }
        try:
            resp = requests.get(url, params=params, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                candles = data.get("data", {}).get("candles", [])
                if candles and len(candles) > 0:
                    df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
                    df['datetime'] = pd.to_datetime(df['timestamp'])
                    df.set_index('datetime', inplace=True)
                    for col in ["open", "high", "low", "close", "volume", "oi"]:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    candles_3min = df.resample("3Min").agg({
                        "open": "first",
                        "high": "max",
                        "low": "min",
                        "close": "last",
                        "volume": "sum",
                        "oi": "last"
                    }).dropna()
                    local_history[instrument] = candles_3min.reset_index().to_dict("records")
                    logging.info(f"Historical backfill for {instrument} complete with {len(local_history[instrument])} candles from {last_trading_day}.")
                else:
                    logging.error(f"No candle data in historical response for {instrument}. Params: {params}. Response: {resp.text}")
            else:
                logging.error(f"Historical data fetch failed for {instrument}: {resp.status_code}. Params: {params}. Response: {resp.text}")
        except Exception as e:
            logging.error(f"Error in historical backfill for {instrument}: {e}. Params: {params}")

# ----------------------------------------------------------------------------
# HELPER FUNCTIONS: INSTRUMENT DATA & MISC
# ----------------------------------------------------------------------------
def download_instrument_file(url, local_path):
    logging.info(f"Downloading instrument file from {url}...")
    resp = requests.get(url, stream=True)
    if resp.status_code == 200:
        with open(local_path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        logging.info(f"File downloaded and saved to {local_path}")
    else:
        raise Exception(f"Download failed with status code {resp.status_code}")

def get_current_index_level():
    configuration = Configuration()
    configuration.access_token = ACCESS_TOKEN
    api_client = ApiClient(configuration)
    market_quote_api = MarketQuoteApi(api_client)
    symbol = "NSE_INDEX|Nifty 50"
    api_version = "2.0"
    try:
        response = market_quote_api.ltp(symbol, api_version)
        logging.debug(f"LTP API Response: {response}")
        if hasattr(response, "to_dict"):
            response_dict = response.to_dict()
        else:
            response_dict = response.__dict__
        if "data" in response_dict:
            for inst_data in response_dict["data"].values():
                if "last_price" in inst_data:
                    ltp = inst_data["last_price"]
                    logging.info(f"Fetched Nifty 50 Level via REST: {ltp}")
                    return float(ltp)
            raise AttributeError("No 'last_price' found in response data.")
        else:
            if "ltp" in response_dict:
                ltp = response_dict["ltp"]
            elif "last_traded_price" in response_dict:
                ltp = response_dict["last_traded_price"]
            else:
                raise AttributeError("Response lacks 'ltp' or 'last_traded_price'.")
            logging.info(f"Fetched Nifty 50 Level via REST: {ltp}")
            return float(ltp)
    except ApiException as e:
        logging.error(f"Error fetching current index level: {e}")
        raise e

def round_to_nearest_50(level):
    return round(level / INTERVAL) * INTERVAL

def load_instruments(file_path):
    instruments = []
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        content = f.read().strip()
        try:
            data = json.loads(content)
            if isinstance(data, list):
                instruments = data
            elif isinstance(data, dict):
                instruments.append(data)
        except json.JSONDecodeError:
            f.seek(0)
            for line in f:
                line = line.strip()
                if line:
                    try:
                        record = json.loads(line)
                        if isinstance(record, list):
                            instruments.extend(record)
                        elif isinstance(record, dict):
                            instruments.append(record)
                    except json.JSONDecodeError as e:
                        logging.error(f"Error decoding JSON: {e}")
    return instruments

def parse_expiry(expiry_val):
    if isinstance(expiry_val, (int, float)):
        return dt.datetime.fromtimestamp(expiry_val / 1000.0)
    elif isinstance(expiry_val, str):
        try:
            return dt.datetime.strptime(expiry_val, "%d-%b-%Y")
        except ValueError:
            return None
    return None

def filter_nifty50_options(instruments):
    filtered = []
    for rec in instruments:
        if not isinstance(rec, dict):
            continue
        segment = rec.get("segment", "")
        trading_symbol = rec.get("trading_symbol", "").upper()
        if segment == "NSE_FO" and "NIFTY" in trading_symbol and "BANKNIFTY" not in trading_symbol:
            expiry_dt = parse_expiry(rec.get("expiry"))
            if expiry_dt:
                rec["expiry_dt"] = expiry_dt
                filtered.append(rec)
    return filtered

def get_next_closest_expiry(options):
    now = dt.datetime.now()
    future_expiries = [rec["expiry_dt"] for rec in options if rec["expiry_dt"] > now]
    return min(future_expiries) if future_expiries else None

def filter_by_expiry(options, expiry_date):
    return [rec for rec in options if rec["expiry_dt"] == expiry_date]

def filter_by_strike_range(options, atm):
    lower_bound = atm - (STRIKES_BELOW * INTERVAL)
    upper_bound = atm + (STRIKES_ABOVE * INTERVAL)
    return [rec for rec in options if lower_bound <= rec.get("strike_price", 0) <= upper_bound]

def wait_until(target_time):
    while dt.datetime.now().time() < target_time and not stop_bot:
        time.sleep(10)

# ----------------------------------------------------------------------------
# PROTOBUF DECODING & LIVE FEED HANDLING
# ----------------------------------------------------------------------------
def decode_protobuf(buffer):
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response

def handle_decoded_feed(feed_response):
    """
    Decodes Protobuf messages (full mode) and stores the latest tick in live_data.
    """
    for instrument_key, feed in feed_response.feeds.items():
        if feed.HasField("fullFeed"):
            full_feed = feed.fullFeed
            if full_feed.HasField("marketFF"):
                market_ff = full_feed.marketFF
                ltp = market_ff.ltpc.ltp
                ltt = market_ff.ltpc.ltt
                oi = market_ff.oi
                iv = market_ff.iv
                atp = market_ff.atp
                vtt = market_ff.vtt
                delta = market_ff.optionGreeks.delta if market_ff.optionGreeks else None
                live_data[instrument_key] = {
                    "ltp": ltp,
                    "ltt": ltt,
                    "oi": oi,
                    "iv": iv,
                    "atp": atp,
                    "vtt": vtt,
                    "delta": delta,
                }
                friendly_info = instrument_names.get(instrument_key, {})
                friendly_name = friendly_info.get("friendly_name", instrument_key)
                logging.info(f"Decoded {friendly_name}: LTP={ltp}, OI={oi}, IV={iv}, ATP={atp}, vtt={vtt}, delta={delta}")
            elif full_feed.HasField("indexFF"):
                index_ff = full_feed.indexFF
                ltp = index_ff.ltpc.ltp
                ltt = index_ff.ltpc.ltt
                live_data[instrument_key] = {"ltp": ltp, "ltt": ltt}
                friendly_info = instrument_names.get(instrument_key, {})
                friendly_name = friendly_info.get("friendly_name", instrument_key)
                logging.info(f"Decoded {friendly_name}: LTP={ltp}, LTT={ltt}")
        elif feed.HasField("ltpc"):
            ltpc_data = feed.ltpc
            live_data[instrument_key] = {"ltp": ltpc_data.ltp, "ltt": ltpc_data.ltt}
            friendly_info = instrument_names.get(instrument_key, {})
            friendly_name = friendly_info.get("friendly_name", instrument_key)
            logging.info(f"Decoded {friendly_name}: LTP={ltpc_data.ltp}, LTT={ltpc_data.ltt}")

# ----------------------------------------------------------------------------
# WEBSOCKET LISTENER FOR LIVE DATA
# ----------------------------------------------------------------------------
async def websocket_listener(redirect_url, instrument_keys):
    while not stop_bot and not force_unsubscribe_flag:
        try:
            async with websockets.connect(redirect_url, open_timeout=10) as websocket:
                logging.info("Connected to WebSocket endpoint.")
                subscribe_message = {
                    "guid": "unique_guid_123",
                    "method": "sub",
                    "data": {
                        "mode": "full",
                        "instrumentKeys": instrument_keys
                    }
                }
                await websocket.send(json.dumps(subscribe_message).encode("utf-8"))
                logging.info("Subscription message sent.")
                while not force_unsubscribe_flag and not stop_bot:
                    try:
                        raw_message = await asyncio.wait_for(websocket.recv(), timeout=5)
                        if isinstance(raw_message, (bytes, bytearray)):
                            decoded = decode_protobuf(raw_message)
                            handle_decoded_feed(decoded)
                        else:
                            logging.debug(f"Received non-binary data: {raw_message}")
                    except asyncio.TimeoutError:
                        continue
        except Exception as e:
            logging.error(f"WebSocket connection error: {e}")
            await asyncio.sleep(5)

def subscribe_options(instrument_keys):
    configuration = Configuration()
    configuration.access_token = ACCESS_TOKEN
    api_client = ApiClient(configuration)
    ws_api = WSApi(api_client)
    redirect_response = ws_api.get_market_data_feed_authorize_v3()
    if hasattr(redirect_response, "to_dict"):
        redirect_data = redirect_response.to_dict()
    else:
        redirect_data = redirect_response.__dict__
    try:
        redirect_url = redirect_data["data"]["authorized_redirect_uri"]
    except KeyError:
        raise Exception("Redirect URL not found in the authorization response.")
    logging.info(f"Obtained WebSocket redirect URL: {redirect_url}")
    def start_listener():
        asyncio.run(websocket_listener(redirect_url, instrument_keys))
    listener_thread = threading.Thread(target=start_listener, daemon=True)
    listener_thread.start()
    return listener_thread

def unsubscribe_options(listener_thread, instrument_keys):
    force_unsubscribe()
    listener_thread.join(timeout=10)
    logging.info("Unsubscribed from market data feed.")

# ----------------------------------------------------------------------------
# ORDER & TRADE MANAGEMENT
# ----------------------------------------------------------------------------
async def check_api_response(session, url, headers, method, **kwargs):
    async with session.request(method, url, headers=headers, **kwargs) as response:
        try:
            res_json = await response.json()
        except Exception as e:
            logging.error(f"Response not in JSON format: {e}. Response text: {response.text}")
            return None
        if response.status != 200:
            logging.error(f"HTTP Error {response.status}: {res_json.get('errors')}. Params: {kwargs.get('params', {})}. Full Response: {response.text}")
            return None
        if "errors" in res_json:
            for error in res_json["errors"]:
                logging.error(f"API Error {error.get('errorCode')}: {error.get('message')}. Params: {kwargs.get('params', {})}")
            return None
        return res_json

async def check_order_status(order_id):
    url = 'https://api.upstox.com/v2/order/details'
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {ACCESS_TOKEN}'}
    params = {'order_id': order_id}
    timeout = 30
    start_time = time.time()
    async with aiohttp.ClientSession() as session:
        while time.time() - start_time < timeout and not stop_bot:
            res_json = await async_retry(check_api_response, 3, 1, 2, session, url, headers, "GET", params=params)
            if res_json and res_json.get("data", {}).get("status") == "FILLED":
                logging.info(f"Order {order_id} is filled.")
                return True
            await asyncio.sleep(1)
    logging.info(f"Order {order_id} not filled within timeout.")
    return False

async def place_order(order_details, use_slicing=False):
    url = 'https://api-hft.upstox.com/v3/order/place'
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {ACCESS_TOKEN}',
    }
    order_details.update({
        "quantity": 75,
        "product": "INTRADAY",
        "validity": "DAY",
        "price": 0,
        "trigger_price": 0,
        "is_amo": False,
        "slice": use_slicing
    })
    async with aiohttp.ClientSession() as session:
        res_json = await async_retry(check_api_response, 3, 1, 2, session, url, headers, "POST", json=order_details)
        if res_json and "order_id" in res_json:
            order_id = res_json["order_id"]
            logging.info(f"Entry order placed. Order ID: {order_id}")
            if await check_order_status(order_id):
                return order_id
    return None

async def place_tp_order(trade):
    url = 'https://api-hft.upstox.com/v3/order/place'
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {ACCESS_TOKEN}',
    }
    tp_order = {
        "instrument_token": trade["instrument"],
        "order_type": "LIMIT",
        "transaction_type": "SELL",
        "tag": "TP_AlgoTrade",
        "quantity": 75,
        "product": "INTRADAY",
        "validity": "DAY",
        "price": trade["target_profit"],
        "trigger_price": 0,
        "is_amo": False,
        "slice": False
    }
    async with aiohttp.ClientSession() as session:
        res_json = await async_retry(check_api_response, 3, 1, 2, session, url, headers, "POST", json=tp_order)
        if res_json and "order_id" in res_json:
            tp_order_id = res_json["order_id"]
            logging.info(f"TP order placed. Order ID: {tp_order_id}")
            if await check_order_status(tp_order_id):
                return tp_order_id
    return None

async def place_sl_order(trade, stop_price):
    url = 'https://api-hft.upstox.com/v3/order/place'
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {ACCESS_TOKEN}',
    }
    sl_order = {
        "instrument_token": trade["instrument"],
        "order_type": "SL-M",
        "transaction_type": "SELL",
        "tag": "SL_AlgoTrade",
        "quantity": 75,
        "product": "INTRADAY",
        "validity": "DAY",
        "price": 0,
        "trigger_price": stop_price,
        "is_amo": False,
        "slice": False
    }
    async with aiohttp.ClientSession() as session:
        res_json = await async_retry(check_api_response, 3, 1, 2, session, url, headers, "POST", json=sl_order)
        if res_json and "order_id" in res_json:
            sl_order_id = res_json["order_id"]
            logging.info(f"SL order placed. Order ID: {sl_order_id}")
            if await check_order_status(sl_order_id):
                return sl_order_id
    return None

async def update_stop_loss(trade, new_sl):
    url = 'https://api-hft.upstox.com/v3/order/modify'
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {ACCESS_TOKEN}'
    }
    data = {
        'order_id': trade.get("sl_order_id"),
        'quantity': 75,
        'validity': 'DAY',
        'price': 0,
        'order_type': 'SL-M',
        'disclosed_quantity': 0,
        'trigger_price': new_sl
    }
    async with aiohttp.ClientSession() as session:
        res_json = await async_retry(check_api_response, 3, 1, 2, session, url, headers, "PUT", json=data)
        if res_json:
            friendly = instrument_names.get(trade['instrument'], {}).get("friendly_name", trade["instrument"])
            logging.info(f"SL order modified for {friendly}. New SL: {new_sl:.2f}")
            trade["current_sl"] = new_sl
            return True
    return False

async def get_available_margin(segment='SEC'):
    url = f'https://api.upstox.com/v2/user/get-funds-and-margin?segment={segment}'
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {ACCESS_TOKEN}'}
    async with aiohttp.ClientSession() as session:
        res_json = await async_retry(check_api_response, 3, 1, 2, session, url, headers, "GET")
        if res_json:
            available_margin = res_json["data"].get("available_margin", 0)
            logging.info(f"Available margin for {segment}: {available_margin}")
            return float(available_margin)
    return 0

# ----------------------------------------------------------------------------
# CANDLE-BUILDING & INDICATOR CALCULATIONS
# ----------------------------------------------------------------------------
def build_candles(ticks):
    if not ticks:
        return None
    if not isinstance(ticks, list):
        logging.warning("Skipping candle build because ticks is not a list.")
        return None
    if not all(isinstance(t, dict) for t in ticks):
        logging.warning("Skipping candle build because not all items in ticks are dicts.")
        return None
    try:
        # If data is already aggregated as candles, return as DataFrame
        sample = ticks[0]
        if all(key in sample for key in ["open", "high", "low", "close"]):
            df = pd.DataFrame(ticks)
            if "datetime" in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'])
                df.set_index('datetime', inplace=True)
            return df
        # Otherwise, assume tick data with 'ltt' and 'ltp'
        df = pd.DataFrame(ticks)
        df['datetime'] = pd.to_datetime(df['ltt'], unit='ms')
        df.set_index('datetime', inplace=True)
        df['ltp'] = pd.to_numeric(df['ltp'], errors='coerce')
        if 'volume' in df.columns and df['volume'].astype(float).sum() > 0:
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        elif 'vtt' in df.columns:
            df['vtt'] = pd.to_numeric(df['vtt'], errors='coerce')
            df['volume'] = df['vtt'].diff().fillna(0)
        else:
            df['volume'] = 0
        ohlcv = df['ltp'].resample(CANDLE_INTERVAL).ohlc()
        vol = df['volume'].resample(CANDLE_INTERVAL).sum()
        ohlcv['volume'] = vol
        ohlcv.dropna(inplace=True)
        logging.info(f"Built {len(ohlcv)} candles from tick data for this instrument.")
        return ohlcv
    except Exception as e:
        logging.error(f"Error building candles: {e}")
        return None

def calculate_emas(candle_df, span_short=9, span_long=21):
    try:
        close_prices = candle_df['close'].values
        if len(close_prices) < span_long:
            logging.warning("Not enough data for EMA calculation.")
            return None, None
        ema_short = talib.EMA(close_prices, timeperiod=span_short)
        ema_long = talib.EMA(close_prices, timeperiod=span_long)
        return ema_short, ema_long
    except Exception as e:
        logging.error(f"Error calculating EMAs: {e}")
        return None, None

def compute_atr(candle_df, period=ATR_PERIOD):
    try:
        high = candle_df['high'].values
        low = candle_df['low'].values
        close = candle_df['close'].values
        atr_array = talib.ATR(high, low, close, timeperiod=period)
        return atr_array[-1]
    except Exception as e:
        logging.error(f"Error computing ATR: {e}")
        return None

def calculate_rvol(candle_df, period=RVOL_PERIOD):
    if candle_df is None or candle_df.empty:
        return None
    try:
        current_vol = candle_df['volume'].iloc[-1]
        avg_vol = candle_df['volume'].rolling(window=period, min_periods=1).mean().iloc[-1]
        return current_vol / (avg_vol + 1e-9)
    except Exception as e:
        logging.error(f"Error calculating RVOL: {e}")
        return None

# ----------------------------------------------------------------------------
# TRADE CONDITIONS & EXECUTION
# ----------------------------------------------------------------------------
async def check_trade_conditions():
    global trade_open, current_trade

    # India VIX Filter: Only proceed if India VIX >= 13.5
    india_vix_key = "NSE_INDEX|India VIX"
    if india_vix_key in live_data:
        india_vix_value = live_data[india_vix_key].get("ltp")
        if india_vix_value is None or india_vix_value < 13.5:
            logging.info(f"India VIX ({india_vix_value}) is below threshold 13.5. Skipping trade conditions.")
            return

    for instrument, ticks in local_history.items():
        if instrument == india_vix_key:
            continue

        candles = build_candles(ticks)
        if candles is None or candles.empty:
            continue

        premium = candles['close'].iloc[-1]
        friendly_info = instrument_names.get(instrument, {})
        friendly_name = friendly_info.get("friendly_name", instrument)
        option_type = friendly_info.get("option_type", None)

        logging.debug(f"[DEBUG] Checking {friendly_name} | Last candle close: {premium:.2f}")

        if trade_open:
            logging.debug(f"[DEBUG] Trade already open; skipping {friendly_name}")
            continue

        ema_short, ema_long = calculate_emas(candles)
        if ema_short is None or ema_long is None:
            continue
        logging.debug(f"[DEBUG] {friendly_name} => EMA short={ema_short[-1]:.2f}, EMA long={ema_long[-1]:.2f}")
        if ema_short[-1] <= ema_long[-1]:
            logging.debug(f"[DEBUG] EMA crossover not met for {friendly_name}")
            continue

        rvol = calculate_rvol(candles)
        if rvol is None or rvol < 1.5:
            logging.debug(f"[DEBUG] RVOL not met for {friendly_name} (RVOL={rvol if rvol else 0:.2f})")
            continue

        last_tick = ticks[-1]
        oi = last_tick.get("oi", 0)
        logging.debug(f"[DEBUG] {friendly_name} => OI={oi}")
        if oi < 150000:
            logging.debug(f"[DEBUG] OI not met for {friendly_name} (OI={oi})")
            continue

        delta = last_tick.get("delta", None)
        if option_type == "CE" and (delta is None or delta < 0.3):
            logging.debug(f"[DEBUG] Delta filter not met for {friendly_name} (CE delta={delta})")
            continue
        if option_type == "PE" and (delta is None or delta > -0.3):
            logging.debug(f"[DEBUG] Delta filter not met for {friendly_name} (PE delta={delta})")
            continue

        atr = compute_atr(candles)
        if atr is None:
            logging.debug(f"[DEBUG] ATR calculation failed for {friendly_name}")
            continue

        margin_required = premium * 75 + 100
        margin_available = await get_available_margin()
        if margin_available < margin_required:
            logging.info(f"Insufficient margin for {friendly_name}: Required = {margin_required:.2f}, Available = {margin_available:.2f}")
            continue

        breakeven_price = premium + breakeven_offset
        target_profit = premium + 1.5 * atr

        logging.info(f"Trade conditions met for {friendly_name}. BUY signal generated.")
        logging.info(f"Trade details: Entry={premium:.2f}, Initial SL={premium - atr:.2f}, Breakeven Price={breakeven_price:.2f}, TP={target_profit:.2f}")

        entry_order_id = await place_order({
            "instrument_token": instrument,
            "order_type": "MARKET",
            "transaction_type": "BUY",
            "tag": "AlgoTrade"
        }, use_slicing=False)

        if entry_order_id:
            trade_open = True
            current_trade = {
                "instrument": instrument,
                "entry_price": premium,
                "atr": atr,
                "target_profit": target_profit,
                "current_sl": premium - atr
            }
            tp_order_id = await place_tp_order(current_trade)
            if tp_order_id:
                logging.info(f"TP order confirmed for {friendly_name}.")
            else:
                logging.error(f"TP order failed for {friendly_name}.")

            sl_order_id = await place_sl_order(current_trade, premium - atr)
            if sl_order_id:
                current_trade["sl_order_id"] = sl_order_id
                logging.info(f"SL order confirmed for {friendly_name}.")
            else:
                logging.error(f"SL order failed for {friendly_name}.")

            asyncio.create_task(trade_manager(current_trade))
            break

async def trade_manager(trade):
    global trade_open, current_trade
    instrument = trade["instrument"]
    friendly_info = instrument_names.get(instrument, {})
    friendly_name = friendly_info.get("friendly_name", instrument)
    entry_price = trade["entry_price"]
    target_profit = trade["target_profit"]
    trailing_gap = breakeven_offset

    logging.info(f"Trade initiated for {friendly_name}: Entry={entry_price:.2f}, SL={trade['current_sl']:.2f}, TP={target_profit:.2f}")

    while trade_open and not stop_bot:
        await asyncio.sleep(1)
        ticks = local_history[instrument]
        candles = build_candles(ticks)
        if candles is None or candles.empty:
            continue

        current_price = candles['close'].iloc[-1]
        logging.debug(f"[TRADE MANAGER] {friendly_name} price: {current_price:.2f}")

        if current_price >= entry_price + breakeven_offset:
            candidate_sl = current_price - trailing_gap
            if candidate_sl > trade["current_sl"]:
                logging.info(f"[TRADE MANAGER] Trailing SL update for {friendly_name}: from {trade['current_sl']:.2f} to {candidate_sl:.2f}")
                await update_stop_loss(trade, candidate_sl)

        if current_price >= target_profit:
            pl = (current_price - entry_price) * 75
            logging.info(f"[TRADE MANAGER] TP hit for {friendly_name}. Exiting trade. P/L: {pl:.2f}")
            break
        if current_price <= trade["current_sl"]:
            pl = (current_price - entry_price) * 75
            logging.info(f"[TRADE MANAGER] SL hit for {friendly_name}. Exiting trade. P/L: {pl:.2f}")
            break

    final_price = current_price
    pl = (final_price - entry_price) * 75
    logging.info(f"Trade for {friendly_name} closed. Entry: {entry_price:.2f}, Exit: {final_price:.2f}, P/L: {pl:.2f}")
    trade_open = False
    current_trade = {}

# ----------------------------------------------------------------------------
# DATA PROCESSOR (MAIN LOOP)
# ----------------------------------------------------------------------------
async def data_processor():
    global stop_bot
    while not stop_bot:
        await asyncio.sleep(1)
        logging.debug("data_processor loop running...")

        # Update local_history from live_data
        for instrument, tick_data in live_data.items():
            if instrument not in local_history:
                local_history[instrument] = []
            if local_history[instrument]:
                last_tick = local_history[instrument][-1]
                if tick_data.get("ltt") != last_tick.get("ltt"):
                    local_history[instrument].append(tick_data)
            else:
                local_history[instrument].append(tick_data)
            if len(local_history[instrument]) > 200:
                local_history[instrument] = local_history[instrument][-200:]
        await check_trade_conditions()

    logging.info("Data processor loop stopped due to stop signal.")

# ----------------------------------------------------------------------------
# SUBSCRIPTION MANAGER
# ----------------------------------------------------------------------------
def subscription_manager():
    logging.info("Downloading instrument file...")
    try:
        download_instrument_file(INSTRUMENT_URL, LOCAL_INSTRUMENT_FILE)
    except Exception as e:
        logging.error(f"Error downloading instrument file: {e}")
        return

    current_index = get_current_index_level()
    atm = round_to_nearest_50(current_index)
    logging.info(f"Current Nifty 50 Level: {current_index:.2f}, Rounded ATM Strike: {atm}")

    instruments = load_instruments(LOCAL_INSTRUMENT_FILE)
    logging.info(f"Total instruments loaded: {len(instruments)}")

    nifty_options = filter_nifty50_options(instruments)
    logging.info(f"Total Nifty 50 options found: {len(nifty_options)}")

    next_expiry = get_next_closest_expiry(nifty_options)
    if not next_expiry:
        logging.error("No future expiry found for Nifty 50 options.")
        return
    logging.info(f"Next closest expiry: {next_expiry.strftime('%d-%b-%Y')}")

    options_next_expiry = filter_by_expiry(nifty_options, next_expiry)
    logging.info(f"Options with expiry {next_expiry.strftime('%d-%b-%Y')}: {len(options_next_expiry)} before strike filtering.")

    options_in_range = filter_by_strike_range(options_next_expiry, atm)

    for rec in options_in_range:
        ikey = rec["instrument_key"]
        strike_price = rec.get("strike_price", 0)
        t_symbol = rec.get("trading_symbol", "Unknown")
        if "CE" in t_symbol:
            option_type = "CE"
        elif "PE" in t_symbol:
            option_type = "PE"
        else:
            option_type = "FUT"
        instrument_names[ikey] = {
            "friendly_name": f"{t_symbol} (Strike={strike_price} {option_type})",
            "option_type": option_type
        }

    instrument_keys = [rec["instrument_key"] for rec in options_in_range]

    # Add India VIX instrument
    india_vix_key = "NSE_INDEX|India VIX"
    instrument_keys.append(india_vix_key)
    instrument_names[india_vix_key] = {"friendly_name": "India VIX", "option_type": "INDEX"}

    logging.info(f"Found {len(instrument_keys)} instruments to subscribe:")
    for key in instrument_keys:
        friendly = instrument_names.get(key, {}).get("friendly_name", key)
        logging.info(f"  -> {friendly}")

    # Historical backfill using holiday API to ensure a valid trading day is used
    historical_backfill(instrument_keys)

    listener_thread = subscribe_options(instrument_keys)
    logging.info(f"Subscribed. Waiting until unsubscription time: {UNSUBSCRIBE_TIME.strftime('%H:%M')}")
    wait_until(UNSUBSCRIBE_TIME)
    unsubscribe_options(listener_thread, instrument_keys)
    logging.info("Subscription management complete for today.")

# ----------------------------------------------------------------------------
# MAIN ENTRY POINT
# ----------------------------------------------------------------------------
def run_trading_bot():
    override_thread = threading.Thread(target=override_listener, daemon=True)
    override_thread.start()

    subscription_thread = threading.Thread(target=subscription_manager, daemon=True)
    subscription_thread.start()

    asyncio.run(data_processor())

if __name__ == "__main__":
    run_trading_bot()
