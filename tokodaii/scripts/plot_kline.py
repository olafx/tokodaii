'''
Use local kline data to make basic kline plots.
'''

import argparse
from datetime import timedelta as td
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
from tokodaii.data.utils import kline
from tokodaii.utils import time, rounding
from tokodaii.data import KLINE_SOURCES, KLINE_CATEGORIES, EXCHANGES

N = 4096

def args():
  parser = argparse.ArgumentParser(prog='plot_kline', description='Plot locally stored kline data.')
  parser.add_argument('exchange', metavar='exchange', choices=EXCHANGES, help=f'exchange in {EXCHANGES}')
  parser.add_argument('source', metavar='source', help=f'source in {KLINE_SOURCES}')
  parser.add_argument('category', metavar='category', help=f'category in {KLINE_CATEGORIES}')
  parser.add_argument('symbol', metavar='symbol')
  parser.add_argument('start', metavar='start', help='start date (yyyy-mm-dd) (inclusive)')
  parser.add_argument('end', metavar='end', help='end date in (yyyy-mm-dd) (exclusive)')
  parser.add_argument('--s', action=argparse.BooleanOptionalAction, default=False, help='save the plot instead')
  return parser.parse_args()

if __name__ == '__main__':

  args = args()
  exchange, source, category, symbol = args.exchange, args.source, args.category, args.symbol
  start, end, s = args.start, args.end, args.s

  start, end = time.from_str_date(start), time.from_str_date(end)
  step = rounding.up((end-start)/N, td(minutes=1))
  df = kline.get(exchange, source, category, symbol, start, end, step)
  times = df['start time'].astype('datetime64[ns]')
  date_fmt = time.FMT_date_hm if end-start < td(days=5) else time.FMT_date

  plt.rcParams['figure.figsize'] = (10, 5)
  plt.gca().xaxis.set_major_formatter(DateFormatter(date_fmt))
  plt.gcf().autofmt_xdate()
  plt.plot(times, (df['high price']+df['low price'])/2, c='black', label='hi-lo avg')
  plt.xlim(times[0], times[-1])
  plt.ylabel(symbol, size=18)
  plt.grid()
  plt.legend(fontsize=18)
  plt.tight_layout()
  if s: plt.savefig(f'{source}_{category}_{symbol}_{start}_{end}.pdf')
  else: plt.show()
