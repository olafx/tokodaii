from datetime import datetime as dt
from tokodaii.bybit.api import API
from tokodaii.bybit.utils import SYMBOLS_PER_CALL
from tokodaii.utils import time

def get_time(api:API) -> dt:
  response, _ = api.GET('/v5/market/time')
  return time.from_unix_ns(int(response['result']['timeNano']))

def get_symbols(api:API, category:str) -> set[str]:
  symbols = set()
  params = {'category':category, 'limit':SYMBOLS_PER_CALL}
  while True:
    response, _ = api.GET('/v5/market/instruments-info', params)
    new_symbols = {e['symbol'] for e in response['result']['list']}
    if symbols & new_symbols: # if true, this is the last one
      symbols |= new_symbols
      break
    symbols |= new_symbols
    params = {'category':category, 'limit':SYMBOLS_PER_CALL, 'cursor':response['result']['nextPageCursor']}
  return symbols
