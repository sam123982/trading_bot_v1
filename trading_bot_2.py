"""
Trading Bot v1 – Improved MACD Bollinger RVOL Strategy v2
Author: [Your Name]
Description:
    A production-level trading bot for NSE options (Nifty 50) that receives real-time tick data,
    aggregates it into 1-minute OHLC candles, and calculates indicators (MACD, Bollinger Bands,
    SMA, ATR, RVOL) once 50 candles are available. It then manages trades using an ATR-based stop loss,
    trailing stop, and breakeven logic. The bot uses WebSocket for market data (decoded using Protobuf)
    and REST API calls for order management.
    
This code uses asyncio for I/O-bound tasks and multithreading for control flows.
"""

# ============================================================================
# Imports
# ============================================================================
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
import requests  # For synchronous HTTP calls

# Upstox SDK imports
from upstox_client.configuration import Configuration
from upstox_client.api_client import ApiClient
from upstox_client.rest import ApiException
from upstox_client.api.market_quote_api import MarketQuoteApi
from upstox_client.api.websocket_api import WebsocketApi as WSApi

# Protobuf-generated classes from MarketDataFeedV3.proto
import MarketDataFeedV3_pb2 as pb

# ============================================================================
# Logging Configuration
# ============================================================================
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# ============================================================================
# Global Variables and Strategy Parameters
# ============================================================================
live_data = defaultdict(dict)      # Latest tick data per instrument
local_history = defaultdict(list)   # Tick history used to build candles

trade_open = False
current_trade = {}

stop_bot = False               # Global shutdown flag
force_unsubscribe_flag = False # Flag to signal WebSocket unsubscribe

# We want 1-minute candles
CANDLE_INTERVAL = "1Min"

# -------------------------------
# API Credentials and URLs (Replace with actual credentials)
# -------------------------------
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIzR0M1OFUiLCJqdGkiOiI2N2QyNDJmZDM5MTI2NzJhNDlmZGVmZmQiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaWF0IjoxNzQxODMyOTU3LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NDE5MDMyMDB9.lHhO4O0n5D_ICXX0KTjEIEZil-GQk3PJZLSfeZUWiCA"
API_KEY = "62c659ea-8004-48ef-8f7f-8c86f9d73995"

INSTRUMENT_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
LOCAL_INSTRUMENT_FILE = r"C:\Users\trading_bot\NSE.json.gz"

# Unsubscription time (24-hour format)
UNSUBSCRIBE_TIME = dt.time(15, 14)

# -------------------------------
# Instrument and Strike Parameters
# -------------------------------
INTERVAL = 50
STRIKES_BELOW = 10
STRIKES_ABOVE = 10

# -------------------------------
# Strategy Parameters: Improved MACD Bollinger RVOL Strategy v2
# -------------------------------
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

LENGTH_BB = 20
MULT_BB = 2.0

RVOL_THRESHOLD = 1.5

TREND_LENGTH = 50
# Require 50 one-minute candles before indicator calculation
MIN_CANDLES_REQUIRED = 50

ATR_PERIOD = 14
TP_MULTIPLIER = 2.0         # Target Profit = Entry + 2 * ATR
SL_MULTIPLIER = 0.75        # Initial SL = Entry - 0.75 * ATR
BREAKEVEN_OFFSET = 2.0      # Breakeven threshold = Entry + Rs.2
TRAILING_STOP_MULT = 1.5    # Trailing stop offset = current price - (1.5 * ATR)

MIN_OI = 150000           # Minimum acceptable open interest

INDIA_VIX_KEY = "NSE_INDEX|India VIX"
MIN_INDIA_VIX = 13.5

# Mapping for instrument details (instrument_key -> {"friendly_name": str, "option_type": str})
instrument_names = {}

# ============================================================================
# Override Listener (for graceful shutdown)
# ============================================================================
def override_listener():
    """
    Listens for user input to stop the trading bot.
    Typing 'exit' sets the global stop_bot flag to True.
    """
    global stop_bot
    while not stop_bot:
        try:
            user_input = input("Type 'exit' to stop the trading bot: ")
            if user_input.strip().lower() == "exit":
                stop_bot = True
                logging.info("Stop signal received. Stopping trading bot.")
                break
        except Exception as e:
            logging.error(f"Error in override_listener: {e}")
            break

# ============================================================================
# Protobuf Decoding Functions (as in original code)
# ============================================================================
def decode_protobuf(buffer):
    """
    Decodes a binary buffer into a FeedResponse object using Protobuf.
    """
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response

def handle_decoded_feed(feed_response):
    """
    Processes the FeedResponse and updates live_data.
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

# ============================================================================
# Order Manager Module (REST API Calls)
# ============================================================================
async def async_retry(coro_func, retries=3, delay=1, backoff=2, *args, **kwargs):
    attempt = 0
    while attempt < retries:
        try:
            return await coro_func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Error in {coro_func.__name__}: {e} (attempt {attempt+1}).")
            await asyncio.sleep(delay)
            delay *= backoff
            attempt += 1
    raise Exception(f"{coro_func.__name__} failed after {retries} attempts.")

async def check_api_response(session, url, headers, method, **kwargs):
    async with session.request(method, url, headers=headers, **kwargs) as response:
        try:
            res_json = await response.json()
        except Exception as e:
            logging.error(f"Response not in JSON format: {e}. Response text: {await response.text()}")
            return None
        if response.status != 200:
            logging.error(f"HTTP Error {response.status}: {res_json.get('errors')}.")
            return None
        if "errors" in res_json:
            for error in res_json["errors"]:
                logging.error(f"API Error {error.get('errorCode')}: {error.get('message')}.")
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

# ============================================================================
# Indicators Module
# ============================================================================
def calculate_macd(close_series):
    macd, signal, hist = talib.MACD(close_series, fastperiod=MACD_FAST, slowperiod=MACD_SLOW, signalperiod=MACD_SIGNAL)
    return macd, signal, hist

def calculate_bollinger(close_series):
    middle_band = talib.SMA(close_series, timeperiod=LENGTH_BB)
    stdev = talib.STDDEV(close_series, timeperiod=LENGTH_BB)
    lower_band = middle_band - (MULT_BB * stdev)
    return middle_band, lower_band

def calculate_sma(close_series, period):
    return talib.SMA(close_series, timeperiod=period)

def is_price_rising(close_series):
    if len(close_series) < 3:
        return False
    return close_series.iloc[-1] > close_series.iloc[-2] and close_series.iloc[-2] > close_series.iloc[-3]

# ============================================================================
# Data Handler Module (Candle Building & Synthetic Tick Generation)
# ============================================================================
def build_candles(ticks):
    if not ticks or not isinstance(ticks, list):
        return None
    try:
        df = pd.DataFrame(ticks)
        if 'ltt' not in df.columns or 'ltp' not in df.columns:
            logging.warning("Tick data missing required fields ('ltt' or 'ltp').")
            return None
        df['datetime'] = pd.to_datetime(df['ltt'], unit='ms')
        df.set_index('datetime', inplace=True)
        df['ltp'] = pd.to_numeric(df['ltp'], errors='coerce')
        if 'volume' in df.columns:
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        else:
            df['volume'] = 0
        ohlcv = df['ltp'].resample(CANDLE_INTERVAL).ohlc()
        volume_series = df['volume'].resample(CANDLE_INTERVAL).sum()
        ohlcv['volume'] = volume_series
        ohlcv.dropna(inplace=True)
        logging.info(f"Built {len(ohlcv)} candles from tick data.")
        return ohlcv
    except Exception as e:
        logging.error(f"Error building candles: {e}")
        return None

# ============================================================================
# Trade Logic Module (Conditions and Management)
# ============================================================================
async def check_trade_conditions():
    global trade_open, current_trade

    if INDIA_VIX_KEY in live_data:
        india_vix_price = live_data[INDIA_VIX_KEY].get("ltp")
        if india_vix_price is None or india_vix_price < MIN_INDIA_VIX:
            logging.info(f"India VIX ({india_vix_price}) below threshold ({MIN_INDIA_VIX}). Skipping trades.")
            return

    for instrument, ticks in local_history.items():
        if instrument == INDIA_VIX_KEY:
            continue

        # Build candles from tick data
        candles = build_candles(ticks)
        if candles is None or candles.empty or len(candles) < MIN_CANDLES_REQUIRED:
            continue

        current_close = candles['close'].iloc[-1]
        last_tick = ticks[-1]
        oi = last_tick.get("oi", 0)
        delta = last_tick.get("delta", None)
        option_type = instrument_names.get(instrument, {}).get("option_type", None)

        if oi < MIN_OI:
            logging.debug(f"{instrument}: OI {oi} below minimum {MIN_OI}.")
            continue

        if option_type == "CE" and (delta is None or delta < 0.3):
            logging.debug(f"{instrument}: CE delta {delta} insufficient.")
            continue
        if option_type == "PE" and (delta is None or delta > -0.3):
            logging.debug(f"{instrument}: PE delta {delta} insufficient.")
            continue

        close_series = candles['close']

        macd, signal, _ = calculate_macd(close_series)
        if macd is None or signal is None or len(macd) < 2:
            continue
        if not (macd.iloc[-1] > signal.iloc[-1] and macd.iloc[-2] <= signal.iloc[-2]):
            logging.debug(f"{instrument}: MACD crossover condition not met.")
            continue

        _, lower_band = calculate_bollinger(close_series)
        if close_series.iloc[-1] <= lower_band.iloc[-1]:
            logging.debug(f"{instrument}: Price {close_series.iloc[-1]:.2f} below lower Bollinger band {lower_band.iloc[-1]:.2f}.")
            continue

        volume_series = candles['volume']
        avg_volume = volume_series.rolling(window=LENGTH_BB, min_periods=1).mean().iloc[-1]
        current_volume = volume_series.iloc[-1]
        rvol = current_volume / (avg_volume + 1e-9)
        if rvol < RVOL_THRESHOLD:
            logging.debug(f"{instrument}: RVOL {rvol:.2f} below threshold {RVOL_THRESHOLD}.")
            continue

        trend_sma = calculate_sma(close_series, TREND_LENGTH)
        if close_series.iloc[-1] <= trend_sma.iloc[-1]:
            logging.debug(f"{instrument}: Price {close_series.iloc[-1]:.2f} not above trend SMA {trend_sma.iloc[-1]:.2f}.")
            continue

        if not is_price_rising(close_series):
            logging.debug(f"{instrument}: Price action (rising candles) condition not met.")
            continue

        atr_values = talib.ATR(candles['high'].values, candles['low'].values, close_series.values, timeperiod=ATR_PERIOD)
        if atr_values is None or len(atr_values) == 0:
            continue
        atr_value = atr_values[-1]
        if atr_value <= 0:
            continue

        entry_price = current_close
        initial_sl = entry_price - (SL_MULTIPLIER * atr_value)
        target_profit = entry_price + (TP_MULTIPLIER * atr_value)
        breakeven_threshold = entry_price + BREAKEVEN_OFFSET

        logging.info(f"Trade conditions met for {instrument}: Entry={entry_price:.2f}, Initial SL={initial_sl:.2f}, TP={target_profit:.2f}")

        margin_required = entry_price * 75 + 100
        margin_available = await get_available_margin()
        if margin_available < margin_required:
            logging.info(f"Insufficient margin for {instrument}: Required={margin_required:.2f}, Available={margin_available:.2f}")
            continue

        entry_order_id = await place_order({
            "instrument_token": instrument,
            "order_type": "MARKET",
            "transaction_type": "BUY",
            "tag": "AlgoTrade"
        }, use_slicing=False)
        if not entry_order_id:
            continue

        trade_open = True
        current_trade = {
            "instrument": instrument,
            "entry_price": entry_price,
            "atr": atr_value,
            "target_profit": target_profit,
            "current_sl": initial_sl,
            "breakeven_threshold": breakeven_threshold
        }
        tp_order_id = await place_tp_order(current_trade)
        if tp_order_id:
            logging.info(f"TP order confirmed for {instrument}.")
        else:
            logging.error(f"TP order failed for {instrument}.")
        sl_order_id = await place_sl_order(current_trade, initial_sl)
        if sl_order_id:
            current_trade["sl_order_id"] = sl_order_id
            logging.info(f"SL order confirmed for {instrument}.")
        else:
            logging.error(f"SL order failed for {instrument}.")
        
        asyncio.create_task(trade_manager(current_trade))
        break

async def trade_manager(trade):
    global trade_open, current_trade
    instrument = trade["instrument"]
    entry_price = trade["entry_price"]
    target_profit = trade["target_profit"]
    breakeven_level = trade["breakeven_threshold"]

    logging.info(f"Trade initiated for {instrument}: Entry={entry_price:.2f}, Initial SL={trade['current_sl']:.2f}, TP={target_profit:.2f}")
    current_price = entry_price

    while trade_open and not stop_bot:
        await asyncio.sleep(1)
        candles = build_candles(local_history[instrument])
        if candles is None or candles.empty:
            continue
        current_price = candles['close'].iloc[-1]
        logging.debug(f"[Trade Manager] {instrument} current price: {current_price:.2f}")

        if current_price >= breakeven_level:
            if trade["current_sl"] < breakeven_level:
                logging.info(f"[Trade Manager] {instrument}: Price reached breakeven. Setting SL to {breakeven_level:.2f}.")
                await update_stop_loss(trade, breakeven_level)
        if current_price > breakeven_level:
            candidate_sl = current_price - (TRAILING_STOP_MULT * trade["atr"])
            candidate_sl = max(candidate_sl, breakeven_level)
            if candidate_sl > trade["current_sl"]:
                logging.info(f"[Trade Manager] {instrument}: Trailing SL update from {trade['current_sl']:.2f} to {candidate_sl:.2f}.")
                await update_stop_loss(trade, candidate_sl)

        if current_price <= trade["current_sl"]:
            profit_loss = (current_price - entry_price) * 75
            logging.info(f"[Trade Manager] {instrument}: SL hit. Exiting trade. P/L: {profit_loss:.2f}")
            break
        if current_price >= target_profit:
            profit_loss = (current_price - entry_price) * 75
            logging.info(f"[Trade Manager] {instrument}: TP hit. Exiting trade. P/L: {profit_loss:.2f}")
            break

    final_price = current_price
    profit_loss = (final_price - entry_price) * 75
    logging.info(f"Trade for {instrument} closed. Entry: {entry_price:.2f}, Exit: {final_price:.2f}, P/L: {profit_loss:.2f}")
    trade_open = False
    current_trade = {}

# ============================================================================
# WebSocket Listener Module (Live Data Feed)
# ============================================================================
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
                while not stop_bot and not force_unsubscribe_flag:
                    try:
                        raw_message = await asyncio.wait_for(websocket.recv(), timeout=5)
                        if isinstance(raw_message, (bytes, bytearray)):
                            feed_response = decode_protobuf(raw_message)
                            handle_decoded_feed(feed_response)
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
    global force_unsubscribe_flag
    force_unsubscribe_flag = True
    listener_thread.join(timeout=10)
    logging.info("Unsubscribed from market data feed.")

# ============================================================================
# Subscription Manager Module (Instrument File & WebSocket Subscription)
# ============================================================================
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

def filter_nifty50_options(instruments):
    """
    Filters instruments to select only NSE options for Nifty 50.
    Uses segment ("NSE_FO" or "NFO"), underlying_symbol containing "NIFTY", and excludes BANKNIFTY.
    """
    filtered = []
    for rec in instruments:
        if not isinstance(rec, dict):
            continue
        segment = rec.get("segment", "") or rec.get("exchange", "")
        t_symbol = (rec.get("tradingsymbol") or rec.get("trading_symbol") or "Unknown").upper()
        underlying_symbol = rec.get("underlying_symbol", "").upper()
        if segment in ["NSE_FO", "NFO"] and "NIFTY" in underlying_symbol and "BANKNIFTY" not in t_symbol:
            expiry_val = rec.get("expiry")
            expiry_dt = None
            if isinstance(expiry_val, (int, float)):
                expiry_dt = dt.datetime.fromtimestamp(expiry_val / 1000.0)
            elif isinstance(expiry_val, str):
                try:
                    expiry_dt = dt.datetime.strptime(expiry_val, "%d-%b-%Y")
                except Exception:
                    expiry_dt = None
            if expiry_dt:
                rec["expiry_dt"] = expiry_dt
                filtered.append(rec)
    return filtered

def get_next_closest_expiry(options):
    now = dt.datetime.now()
    future_expiries = [rec["expiry_dt"] for rec in options if rec.get("expiry_dt") and rec["expiry_dt"] > now]
    return min(future_expiries) if future_expiries else None

def filter_by_expiry(options, expiry_date):
    return [rec for rec in options if rec.get("expiry_dt") == expiry_date]

def filter_by_strike_range(options, atm):
    lower_bound = atm - (STRIKES_BELOW * INTERVAL)
    upper_bound = atm + (STRIKES_ABOVE * INTERVAL)
    return [rec for rec in options if lower_bound <= rec.get("strike_price", 0) <= upper_bound]

def round_to_nearest_50(level):
    return round(level / INTERVAL) * INTERVAL

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
                    logging.info(f"Fetched Nifty 50 Level: {ltp}")
                    return float(ltp)
            raise AttributeError("No 'last_price' in response data.")
        else:
            if "ltp" in response_dict:
                ltp = response_dict["ltp"]
            elif "last_traded_price" in response_dict:
                ltp = response_dict["last_traded_price"]
            else:
                raise AttributeError("Response missing 'ltp' or 'last_traded_price'.")
            logging.info(f"Fetched Nifty 50 Level: {ltp}")
            return float(ltp)
    except ApiException as e:
        logging.error(f"Error fetching index level: {e}")
        raise e

def subscription_manager():
    logging.info("Starting subscription manager...")
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
        ikey = rec.get("instrument_key")
        strike_price = rec.get("strike_price", 0)
        t_symbol = (rec.get("tradingsymbol") or rec.get("trading_symbol") or "Unknown").upper()
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

    instrument_keys = [rec.get("instrument_key") for rec in options_in_range]

    instrument_keys.append(INDIA_VIX_KEY)
    instrument_names[INDIA_VIX_KEY] = {"friendly_name": "India VIX", "option_type": "INDEX"}

    logging.info(f"Found {len(instrument_keys)} instruments to subscribe:")
    for key in instrument_keys:
        friendly = instrument_names.get(key, {}).get("friendly_name", key)
        logging.info(f"  -> {friendly}")

    listener_thread = subscribe_options(instrument_keys)
    logging.info(f"Subscribed. Waiting until unsubscription time: {UNSUBSCRIBE_TIME.strftime('%H:%M')}")
    while dt.datetime.now().time() < UNSUBSCRIBE_TIME and not stop_bot:
        time.sleep(10)
    unsubscribe_options(listener_thread, instrument_keys)
    logging.info("Subscription management complete for today.")

# ============================================================================
# Data Processor: Update Local History and Generate Synthetic Ticks
# ============================================================================
async def data_processor():
    SYNTHETIC_TICK_INTERVAL = 5  # seconds before generating a synthetic tick if no new tick arrives
    last_tick_time = {}
    while not stop_bot:
        await asyncio.sleep(1)
        current_time = dt.datetime.now()

        for instrument, tick_data in live_data.items():
            if instrument not in last_tick_time:
                last_tick_time[instrument] = current_time

            if local_history[instrument]:
                last_record = local_history[instrument][-1]
                if tick_data.get("ltt") != last_record.get("ltt"):
                    local_history[instrument].append(tick_data)
                    last_tick_time[instrument] = current_time
            else:
                local_history[instrument].append(tick_data)
                last_tick_time[instrument] = current_time

            elapsed = (current_time - last_tick_time[instrument]).total_seconds()
            if elapsed >= SYNTHETIC_TICK_INTERVAL:
                synthetic_tick = tick_data.copy()
                synthetic_tick["ltt"] = int(current_time.timestamp() * 1000)
                local_history[instrument].append(synthetic_tick)
                logging.debug(f"Generated synthetic tick for {instrument} at {current_time}")
                last_tick_time[instrument] = current_time

            if len(local_history[instrument]) > 200:
                local_history[instrument] = local_history[instrument][-200:]
        await check_trade_conditions()

    logging.info("Data processor loop stopped due to stop signal.")

# ============================================================================
# WebSocket Listener Module (Live Data Feed)
# ============================================================================
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
                while not stop_bot and not force_unsubscribe_flag:
                    try:
                        raw_message = await asyncio.wait_for(websocket.recv(), timeout=5)
                        if isinstance(raw_message, (bytes, bytearray)):
                            feed_response = decode_protobuf(raw_message)
                            handle_decoded_feed(feed_response)
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
    global force_unsubscribe_flag
    force_unsubscribe_flag = True
    listener_thread.join(timeout=10)
    logging.info("Unsubscribed from market data feed.")

# ============================================================================
# Main Execution: Run Subscription Manager and Data Processor
# ============================================================================
def run_trading_bot():
    override_thread = threading.Thread(target=override_listener, daemon=True)
    override_thread.start()

    subscription_thread = threading.Thread(target=subscription_manager, daemon=True)
    subscription_thread.start()

    asyncio.run(data_processor())

if __name__ == "__main__":
    run_trading_bot()
