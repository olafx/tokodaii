'''
Dealing with historical data from public.bybit.com.
'''

from html.parser import HTMLParser
import requests
import numpy as np
from tokodaii.utils import dataframe
from tokodaii.data import KLINE_SIMPLE_COLUMNS, KLINE_SIMPLE_TYPES, TRADE_COLUMNS, TRADE_TYPES, DATA_TYPES

URL = 'https://public.bybit.com'
CATEGORY_COLS = {
  'trading':['timestamp', 'symbol', 'side', 'size', 'price', 'tickDirection', 'trdMatchID', 'grossValue', 'homeNotional', 'foreignNotional'],
  'premium_index':['start_at', 'symbol', 'period', 'open', 'high', 'low', 'close'],
  'spot_index':['start_at', 'symbol', 'period', 'open', 'high', 'low', 'close']}
CATEGORY_COLS_KEEP = {
  'trading':['timestamp', 'side', 'size', 'price'],
  'premium_index':['start_at', 'open', 'high', 'low', 'close'],
  'spot_index':['start_at', 'open', 'high', 'low', 'close']}
FILENAME_EXTRA = {'trading':'', 'premium_index':'_premium_index', 'spot_index':'_index_price'}

'''
Get historical data. If a _v2 file is needed, it is understood that '_v2' is
included in the date. Implicit data (useless) is ignored. This data is otherwise
unprocessed, not ready to use.
'''
def get(category:str, symbol:str, date:str) -> dict[str, np.ndarray]:
  return dataframe.from_csv(f'{URL}/{category}/{symbol}/{symbol}{date}{FILENAME_EXTRA[category]}.csv.gz', usecols=CATEGORY_COLS_KEEP[category], engine='pyarrow')

'''
Read the categories from public.bybit.com. This is not intended to be used to
get a list of the categories that can be dealt with, use
`tokodaii.data.CATEGORIES` for that.
'''
def read_categories() -> list[str]:
  blacklist, categories = ['kline_for_metatrader4'], []
  class Parser(HTMLParser):
    def __init__(self):
      super().__init__()
      self.reset()
      self.interested = False
    def handle_starttag(self, tag, attrs):
      self.interested = tag == 'a'
    def handle_data(self, data):
      if self.interested:
        categories.append(data[:-1])
        self.interested = False
  Parser().feed(requests.get(URL).text)
  return [c for c in categories if c not in blacklist]

'''
Read the symbols in a category from public.bybit.com.
'''
def read_symbols(category:str) -> list[str]:
  symbols = []
  class Parser(HTMLParser):
    def __init__(self):
      super().__init__()
      self.reset()
      self.interested = False
    def handle_starttag(self, tag, attrs):
      self.interested = tag == 'a'
    def handle_data(self, data):
      if self.interested:
        symbols.append(data[:-1])
        self.interested = False
  Parser().feed(requests.get(f'{URL}/{category}').text)
  return symbols

'''
Read the dates from a symbol from public.bybit.com. `from_date` itself is
ignored, i.e. it's supposed to be the last date already obtained.
'''
def read_dates(category:str, symbol:str) -> list[str]:
  dates = []
  class Parser(HTMLParser):
    def __init__(self):
      super().__init__()
      self.reset()
      self.interested = False
    def handle_starttag(self, tag, attrs):
      self.interested = tag == 'a'
    def handle_data(self, data):
      if self.interested:
        # Sometimes v2 files get uploaded. Then there is an associated
        # uncompressed file that needs to be ignored.
        if data.endswith('.csv.gz'):
          match category:
            case 'trading': date_del = -7
            case 'premium_index': date_del = -7-14
            case 'spot_index': date_del = -7-12
          dates.append(data[len(symbol):date_del])
        self.interested = False
  Parser().feed(requests.get(f'{URL}/{category}/{symbol}').text)
  return dates

'''
Process historical data.
'''
def process(df:dict[str, np.ndarray], category:str, return_chronological=True):
  match DATA_TYPES['ByBit']['historical'][category]:
    case 'trade':
      time = TRADE_COLUMNS[0]
      df['price'] *= 2*(df['side'] == 'Sell')-1
      del df['side']
      dataframe.rename(df, from_to=dict(zip(list(df.keys()), TRADE_COLUMNS)))
      df[time] *= 10**3 # to ms first, due to inadequate precision
      dataframe.as_type(df, col_type=TRADE_TYPES, copy=False)
      df[time] *= 10**6 # to ns
    case 'kline_simple':
      time = KLINE_SIMPLE_COLUMNS[0]
      dataframe.rename(df, from_to=dict(zip(list(df.keys()), KLINE_SIMPLE_COLUMNS)))
      df[time] *= 10**9 # to ns
      dataframe.as_type(df, col_type=KLINE_SIMPLE_TYPES, copy=False)      
  # ByBit has changed chronology before. This dataset is large, avoid numpy manipulations.
  i = 1
  while df[time][i] == df[time][0]: i += 1
  chronological = df[time][i] > df[time][0]
  if chronological != return_chronological:
    for col in df.keys(): df[col] = df[col][::-1]
