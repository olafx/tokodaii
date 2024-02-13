'''
Data related standards.
'''

from pathlib import Path

EXCHANGES = ['ByBit', 'ByBit_testnet']

# All kline data and individual trade data should be of this form.
KLINE_COLUMNS = ['start time', 'open price', 'high price', 'low price', 'close price', 'volume', 'turnover']
KLINE_TYPES = {'start time':'int64', 'open price':'float32', 'high price':'float32', 'low price':'float32', 'close price':'float32', 'volume':'float64', 'turnover':'float64'}
KLINE_SIMPLE_COLUMNS = ['start time', 'open price', 'high price', 'low price', 'close price']
KLINE_SIMPLE_TYPES = {'start time':'int64', 'open price':'float32', 'high price':'float32', 'low price':'float32', 'close price':'float32'}
# The time at which a symbol was added to an exchange is stored.
KLINE_EARLIEST_COLUMNS = ['symbol', 'earliest']
KLINE_EARLIEST_TYPES = {'symbol':'object', 'earliest':'int64'}
# The side is encoded in the size: if size is negative, it means buy.
TRADE_COLUMNS = ['time', 'size', 'price']
TRADE_TYPES = {'time':'int64', 'size':'float32', 'price':'float32'}

# Various data sources and categories within those sources.
SOURCES = {
  'ByBit':
    ['API_kline', 'historical'],
  'ByBit_testnet':
    ['API_kline']}
CATEGORIES = {
  'ByBit':{
    'API_kline':
      ['linear', 'inverse', 'spot'],
    'historical':
      ['trading', 'premium_index', 'spot_index']},
  'ByBit_testnet':{
    'API_kline':
      ['linear', 'inverse', 'spot']}}
# Each source and category gets a data type, as defined above.
DATA_TYPES = {
  'ByBit':{
    'API_kline':{
      'linear':'kline', 'inverse':'kline', 'spot':'kline'},
    'historical':{
      'trading':'trade', 'premium_index':'kline_simple', 'spot_index':'kline_simple'}},
  'ByBit_testnet':{
    'API_kline':{
      'linear':'kline', 'inverse':'kline', 'spot':'kline'}}}

# Stuff that is kline. This includes simple kline also.
KLINE_SOURCES = {
  'ByBit':['API_kline', 'historical'],
  'ByBit_testnet':['API_kline']}
KLINE_CATEGORIES = {
  'ByBit':{
    'API_kline':['linear', 'inverse', 'spot'],
    'historical':['premium_index', 'spot_index']},
  'ByBit_testnet':{
    'API_kline':['linear', 'inverse', 'spot']}}

# Sub is the subfolder of the tokodaii data folder where a source is stored.
SUBS = {
  'ByBit':{
    'API_kline':Path('ByBit', 'API', 'kline'),
    'historical':Path('ByBit', 'historical')},
  'ByBit_testnet':{
    'API_kline':Path('ByBit_testnet', 'API', 'kline')}}
