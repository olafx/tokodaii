from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime as dt, timedelta as td
import numpy as np
from tokodaii.bybit.api import API
from tokodaii.bybit.utils import CANDLES_PER_CALL
from tokodaii.utils import time, dataframe
from tokodaii.data import KLINE_TYPES, KLINE_COLUMNS

'''
Raw API kline output, still in the form of strings.
'''
def get_raw(api:API, category:str, symbol:str, dt_initial:dt) -> dict[str, np.ndarray]:
  start_ms, end_ms = time.unix_ms(dt_initial), time.unix_ms(dt_initial+td(minutes=CANDLES_PER_CALL-1)) # [start_ms, end_ms]
  params = {'category':category, 'symbol':symbol, 'interval':'1', 'start':str(start_ms), 'end':str(end_ms), 'limit':str(CANDLES_PER_CALL)}
  response, _ = api.GET('/v5/market/kline', params)
  return response['result']['list']

'''
Convert raw API kline output into a dataframe.
'''
def from_api(data) -> dict[str, np.ndarray]:
  data = np.array(data, order='F')
  return {col:data[:,i] for i, col in enumerate(KLINE_COLUMNS)}

'''
Process API kline data. `df` should already have the right column names, as
defined by `tokodaii.data.KLINE_COLUMNS`, because the API does not return these
columns, so they're assigned anyway. Flips by default because the API does not
return chronologically.
'''
def process(df:dict[str, np.ndarray], flip=True):
  dataframe.as_type(df, col_type=KLINE_TYPES, copy=False)
  df['start time'] *= 10**6 # to ns
  if flip:
    for col in df.keys(): df[col] = df[col][::-1]

'''
Find the earliest date of available API kline data. ByBit's data on this is
sometimes missing, or at least has been missing in the past, or is incorrect.
Uses the bisection method, taking around 12 iterations.
'''
def get_earliest(api:API, category:str, symbol:str, initial:dt, final:dt) -> dt:
  while True:
    middle = time.strip_time(initial+(final-initial)/2)
    if initial == middle: break
    if len(get_raw(api, category, symbol, middle)) == 0: initial = middle
    else: final = middle
  return final
def get_all_earliest(api:API, category:str, symbols:list[str], initial:dt, final:dt, n_threads:int=1) -> dict[str, dt]:
  if n_threads == 1: return {symbol:get_earliest(api, category, symbol, initial, final) for symbol in symbols}
  else:
    earliest = {}
    def task(symbol): earliest[symbol] = get_earliest(api, category, symbol, initial, final)
    with ThreadPoolExecutor(n_threads) as tpe: wait([tpe.submit(task, symbol) for symbol in symbols])
    return earliest
