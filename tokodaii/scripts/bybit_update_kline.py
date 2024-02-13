'''
Update kline data from the ByBit API.

Limitations:
- Only gets data of symbols that are currently (!) available on the exchange.
- Can't deal with symbols that are removed, then readded later; might fail in
  unexpected ways.
- Files are not chronologically written, but the program assumes the last local
  file chronologically was the last one.
'''

import argparse
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime as dt, timedelta as td
import numpy as np
from tokodaii.bybit.api import API
from tokodaii.bybit.utils import kline_api, api, DT_EARLIEST, CANDLES_PER_CALL
from tokodaii.data import storage, SUBS, KLINE_CATEGORIES
from tokodaii.utils import dataframe, time

CANDLES_PER_THREAD = 10**4 # multiple days

def args():
  # `VALID_CATEGORIES` is independent of whether it's on testnet or not.
  VALID_CATEGORIES = KLINE_CATEGORIES['ByBit']['API_kline']+['all']
  parser = argparse.ArgumentParser(prog='bybit_update_kline', description='Update kline data from the ByBit API.')
  parser.add_argument('category', metavar='category', choices=VALID_CATEGORIES, help=', '.join(KLINE_CATEGORIES['ByBit']['API_kline'])+', or all')
  parser.add_argument('symbol', metavar='symbol', help='any individual symbol, or all')
  parser.add_argument('--tn', action=argparse.BooleanOptionalAction, default=False, help='use testnet')
  parser.add_argument('--v', action=argparse.BooleanOptionalAction, default=False, help='be verbose')
  parser.add_argument('--j', type=int, default=16, help='number of threads (default: 16)')
  return parser.parse_args()

def get_earliest(sub:str, category:str, symbols:set[str], now:dt, n_threads:int=1, verbose:bool=False) -> dict[str, dt]:
  earliest = {}
  symbols_local = storage.read_folders(storage.path(sub, category))
  if symbols_old := symbols&set(symbols_local):
    if verbose: print(f'reading earliest data of {len(symbols_old)} old symbol(s) in {category}')
    earliest |= {symbol:time.from_str_date(storage.read_filenames(storage.path(sub, category, symbol), fmt='fea')[-1])+td(days=1) for symbol in symbols_old}
  if symbols_new := symbols-set(symbols_local):
    if verbose: print(f'finding earliest data of {len(symbols_new)} new symbol(s) in {category}')
    earliest |= kline_api.get_all_earliest(api_, category, sorted(symbols_new), DT_EARLIEST, time.strip_time(now), n_threads)
  earliest = dict(sorted(earliest.items(), key=lambda x: x[0]))
  if verbose: print('got earliest data')
  return earliest

def create_tasks(earliest:dict[str, dt], now:dt, candles_per_thread:int) -> list[tuple[str, dt, dt]]:
  tasks = []
  for symbol in earliest.keys():
    l, end = earliest[symbol], time.strip_time(now)
    if l < end:
      while True:
        r = time.strip_time(l+td(minutes=candles_per_thread))
        tasks += [(symbol, l, r)] # [l, r)
        if r >= end:
          tasks[-1] = (symbol, l, end)
          break
        l = r
  return tasks

def execute_tasks(api:API, sub:str, category:str, tasks:list[tuple[str, dt, dt]], n_threads:int=1, verbose:bool=False):
  def task(symbol:str, start:dt, end:dt): # [start, end)
    df = kline_api.from_api(np.concatenate(
      [np.array(kline_api.get_raw(api, category, symbol, start+td(minutes=i*CANDLES_PER_CALL)))[::-1]
      for i in range(-((end-start)//td(minutes=-CANDLES_PER_CALL)))]))
    kline_api.process(df, flip=False)
    first = time.from_unix_ns(df['start time'][0])
    offset = (first-start)//td(minutes=1)
    end_first = 24*60-offset
    df_day = dataframe.empty_like(df)
    for col in df.keys(): df_day[col] = df[col][:end_first]
    storage.write_feather(storage.path(sub, category, symbol, f'{time.dt_to_str_date(start)}.fea'), df_day)
    for i in range(1, (end-start)//td(days=1)):
      for col in df.keys(): df_day[col] = df[col][end_first+24*60*(i-1):end_first+24*60*i]
      storage.write_feather(storage.path(sub, category, symbol, f'{time.dt_to_str_date(start+td(days=i))}.fea'), df_day)
    if verbose: print(f'completed {symbol} [{time.dt_to_str_date_hm(start)}, {time.dt_to_str_date_hm(end)})')
  if n_threads == 1:
    for t in tasks: task(*t)
  else:
    with ThreadPoolExecutor(n_threads) as tpe: wait([tpe.submit(task, *t) for t in tasks])

def update(api_:API, category:str, symbol:str, now:dt, n_threads, verbose):
  sub = SUBS[api_.exchange]['API_kline']
  for category in KLINE_CATEGORIES[api_.exchange]['API_kline'] if category == 'all' else [category]:
    if symbol == 'all':
      symbols = api.get_symbols(api_, category)
      if verbose: print(f'read {len(symbols)} symbol(s) in {category}')
    else: symbols = {symbol}
    earliest = get_earliest(sub, category, symbols, now, n_threads, verbose)
    tasks = create_tasks(earliest, now, CANDLES_PER_THREAD)
    if verbose: print(f'starting {len(tasks)} task(s)')
    execute_tasks(api_, sub, category, tasks, n_threads, verbose)

if __name__ == '__main__':

  args = args()
  if args.category == 'all': assert args.symbol == 'all'
  api_ = API(use_testnet=args.tn)

  now = api.get_time(api_)
  if args.v: print(f'server time {time.dt_to_str_date_hms_us(now)}')
  update(api_, args.category, args.symbol, now, args.j, args.v)
