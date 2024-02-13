'''
Deal with local kline data.
'''

from datetime import datetime as dt
from datetime import timedelta as td
import numpy as np
from tokodaii.data import storage, DATA_TYPES, SUBS
from tokodaii.utils import time, dataframe

'''
Returns local kline data in [`start`, `end`) with a resolution `step`.
'''
def get(exchange:str, source:str, category:str, symbol:str, start:dt, end:dt, step:td=td(minutes=1)) -> dict[str, np.ndarray]:

  path_base = (SUBS[exchange][source], category, symbol)
  is_simple = DATA_TYPES[exchange][source][category] == 'kline_simple'

  n_days = (end.date()-start.date())//td(days=1)
  dates = [start.date()+td(days=i) for i in range(n_days)]

  if n_days == 1:
    df = storage.read_feather(storage.path(*path_base, f'{dates[0]}.fea'))
    i_start = np.searchsorted(df['start time'] >= time.unix_ns(start), True)
    i_end = np.searchsorted(df['start time'] > time.unix_ns(end), True)
    for col in df.keys(): df[col] = df[col][i_start:i_end]
  else:
    df_first = storage.read_feather(storage.path(*path_base, f'{dates[0]}.fea'))
    df_last = storage.read_feather(storage.path(*path_base, f'{dates[-1]}.fea'))
    i_start = np.searchsorted(df_first['start time'] >= time.unix_ns(start), True)
    i_end = np.searchsorted(df_first['start time'] > time.unix_ns(end), True)
    for col in df_first.keys(): df_first[col] = df_first[col][i_start:]
    for col in df_last.keys(): df_last[col] = df_last[col][:i_end]
    df = dataframe.concat([df_first]+[storage.read_feather(storage.path(*path_base, f'{date}.fea')) for date in dates[1:-1]]+[df_last])

  step_data = td(microseconds=(df['start time'][1]-df['start time'][0])/10**3)
  scale = step//step_data
  if scale == 1: return df
  else:
    n = len(df[col])
    n_new = n//scale
    for col in ('start time', 'open price', 'close price'): df[col] = df[col][:n_new*scale:scale]
    if not is_simple:
      for col in ('volume', 'turnover'): df[col] = df[col][:n_new*scale].reshape((n_new, scale)).sum(axis=1)
    df['high price'] = df['high price'][:n_new*scale].reshape((n_new, scale)).max(axis=1)
    df['low price'] = df['low price'][:n_new*scale].reshape((n_new, scale)).min(axis=1)
    return df
