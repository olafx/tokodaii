from time import time_ns, sleep
import json
import hmac
import requests
import urllib3
from tokodaii.auto.guard import Guard
from tokodaii.config import config

# The requests contain no unencrypted private information, so we'll speed up
# requests by not verifying the SSL certificate each time.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

guards = dict(zip(['mn', 'tn'], [Guard(f'ByBit_API{a}', config['ByBit']['API']['limits']) for a in ['', '_tn']]))

'''
A generic ByBit API call using the guard. If allow_sleep, it will guarantee the
request can be made and sleep if need be. (This is intended to be threaded.) If
not allow_sleep, GET and POST will return a wait time as well as the response
json and headers.
'''
class API():

  def __init__(self, use_testnet=False, key_secret_i=0, window_ms=5000, allow_sleep=True):
    if use_testnet:
      self.exchange = 'ByBit_testnet'
      self.guard = guards['tn']
      self.url = 'https://api-testnet.bybit.com'
    else:
      self.exchange = 'ByBit'
      self.guard = guards['mn']
      self.url = 'https://api.bybit.com'
    self.key, self.secret = config[self.exchange]['keys and secrets'][key_secret_i]
    self.window_ms = window_ms
    self.allow_sleep = allow_sleep

  # What form `params` is in depends on whether the request is GET or POST.
  def _authenticate(self, params:str, time_ms:int=None) -> dict:
    if time_ms is None: time_ms = time_ns()//10**6
    sign = hmac.new(bytes(self.secret, 'utf-8'), bytes(f'{time_ms}{self.key}{self.window_ms}{params}', 'utf-8'), digestmod='sha256').hexdigest()
    return {'X-BAPI-SIGN':str(sign), 'X-BAPI-API-KEY':self.key, 'X-BAPI-SIGN-TYPE':'2', 'X-BAPI-TIMESTAMP':str(time_ms), 'X-BAPI-RECV-WINDOW':str(self.window_ms), 'Content-Type':'application/json'}

  def _check_and_return(self, response:requests.Response):
    assert response.ok
    response_json = response.json()
    if ret_code := response_json['retCode']:
      print(f'{self.exchange}: API return code: {ret_code}')
    assert ret_code == 0
    return (response_json, response.headers) if self.allow_sleep else (0, response_json, response.headers)

  def GET(self, endpoint, params=None, private=False, time_ms:int=None):
    if wait := self.guard.request():
      if self.allow_sleep: sleep(wait)
      else: return wait, None, None
    params = '' if params is None else '&'.join([f'{k}={v}' for k, v in params.items()])
    headers = self._authenticate(params, time_ms) if private else None
    response = requests.get(self.url+endpoint+'?'+params, verify=False, headers=headers)
    return self._check_and_return(response)

  def POST(self, endpoint, params=None, private=False, time_ms:int=None):
    if wait := self.guard.request():
      if self.allow_sleep: sleep(wait)
      else: return wait, None, None
    params = json.dumps(params)
    headers = self._authenticate(params, time_ms) if private else None
    response = requests.post(self.url+endpoint, verify=False, headers=headers, data=params)
    return self._check_and_return(response)
