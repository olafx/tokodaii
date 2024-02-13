'''
Rewrite local data as Excel files to be human readable, for convenience.
'''

import os
import argparse
import pandas as pd
from tokodaii.data import storage, SOURCES, CATEGORIES, SUBS, EXCHANGES

def args():
  parser = argparse.ArgumentParser(prog='to_excel', description='Make a .xlsx copy of locally stored data.')
  parser.add_argument('exchange', metavar='exchange', help=f'exchange in {EXCHANGES}')
  parser.add_argument('source', metavar='source', help=f'source in {SOURCES}')
  parser.add_argument('category', metavar='category', help=f'category in {CATEGORIES}')
  parser.add_argument('symbol', metavar='symbol')
  parser.add_argument('date', metavar='date', help='date (yyyy-mm-dd)')
  return parser.parse_args()

if __name__ == '__main__':

  args = args()
  exchange, source, category, symbol, date = args.exchange, args.source, args.category, args.symbol, args.date

  path = storage.path(SUBS[exchange][source], category, symbol, f'{date}.fea')
  pd.read_feather(path).to_excel(f'{exchange}_{source}_{category}_{symbol}_{args.date}.xlsx')
