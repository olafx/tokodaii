'''
The config is mostly mutable, with some exceptions like the limits; can't
guarantee safety when guard limits are tightened.
'''

import os
import json
import tokodaii

PATH = tokodaii.PATH/'config'

DEFAULT_BYBIT_API_LIMITS = [{'time':5, 'time tol':.5, 'count':120, 'count tol':10}]
DEFAULT_BYBIT_WS_LIMITS = [{'time':5*60, 'time tol':30, 'count':500, 'count tol':50}]

# This is initialized in `tokodaii.__init__`.
config:dict = None

def exists() -> bool:
  return os.path.isfile(PATH)

def read() -> dict:
  with open(PATH, 'r') as fp: return json.load(fp)

def write(config:dict):
  with open(PATH, 'w') as fp: json.dump(config, fp, indent=4)

def default() -> dict:
  ret = {}
  for e in ['ByBit', 'ByBit_testnet']:
    ret[e] = {'API':{}, 'WS':{}}
    ret[e]['API']['limits'] = DEFAULT_BYBIT_API_LIMITS
    ret[e]['keys and secrets'] = []
    ret[e]['WS']['limits'] = DEFAULT_BYBIT_WS_LIMITS
  ret['data'] = {
    'storage path':str(tokodaii.PATH/'storage'),
    'processing':{'compressor':'zstd', 'compression level':1}}
  return ret
