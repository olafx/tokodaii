'''
Update historical data from public.bybit.com.
'''

import argparse
from concurrent.futures import ThreadPoolExecutor, wait
from tokodaii.bybit.utils import historical
from tokodaii.data import storage, SUBS, CATEGORIES

def args():
  VALID_CATEGORIES = CATEGORIES['ByBit']['historical']+['all']
  parser = argparse.ArgumentParser(prog='bybit_update_historical', description='Update historical data from public.bybit.com.')
  parser.add_argument('category', metavar='category', choices=VALID_CATEGORIES, help=', '.join(CATEGORIES['ByBit']['historical'])+', or all')
  parser.add_argument('symbol', metavar='symbol', help='any individual symbol, or all')
  parser.add_argument('--v', action=argparse.BooleanOptionalAction, default=False, help='be verbose')
  parser.add_argument('--j', type=int, default=4, help='number of threads (default: 4)')
  return parser.parse_args()

def update(category:str, symbol:str, n_threads:int=1, verbose:bool=False):
  sub = SUBS['ByBit']['historical']
  for category in CATEGORIES['ByBit']['historical'] if category == 'all' else [category]:
    for symbol in historical.read_symbols(category) if symbol == 'all' else [symbol]:
      dates_bybit = historical.read_dates(category, symbol)
      dates_local = storage.read_filenames(storage.path(sub, category, symbol), fmt='fea')
      dates = sorted(list(set(dates_bybit)-set(dates_local)))
      if verbose: print(f'{category}/{symbol} missing {len(dates)}/{len(dates_bybit)}')
      def task(date:str):
        df = historical.get(category, symbol, date)
        historical.process(df, category)
        storage.write_feather(storage.path(SUBS['ByBit']['historical'], category, symbol, f'{date}.fea'), df)
        if verbose: print(f'got {category}/{symbol}/{date}')
      with ThreadPoolExecutor(n_threads) as tpe: wait([tpe.submit(task, date) for date in dates])

if __name__ == '__main__':

  args = args()
  if args.category == 'all': assert args.symbol == 'all'
  update(args.category, args.symbol, args.j, args.v)
