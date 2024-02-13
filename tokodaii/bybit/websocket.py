from time import time_ns, monotonic_ns, sleep
from uuid import uuid4
from threading import Thread
import json
import hmac
import websocket
from tokodaii.auto.guard import Guard
from tokodaii.config import config

WS_CHANNELS = ['private', 'linear', 'option', 'spot']
guards =\
  dict(zip(WS_CHANNELS, [Guard(f'ByBit_WS_{c}', config['ByBit']['WS']['limits']) for c in WS_CHANNELS])) |\
  dict(zip([f'{c}_tn' for c in WS_CHANNELS], [Guard(f'ByBit_WS_{c}_tn', config['ByBit']['WS']['limits']) for c in WS_CHANNELS]))

'''
A wrapper that acts as a ByBit websocket. Doesn't deal with errors, so use
`on_error`. For simplicity, the constructor will always wait if the guard says
it should, so the constructor can't fail, but may sleep, in the highly unlikely
event of a websocket limit being reached.
'''
class WebSocket():

  def __init__(self, channel, use_testnet=False, key_secret_i=0, ping_interval_s=10, ping_timeout_s=5, *args, **kwargs):
    wait = guards[channel+('_tn' if use_testnet else '')].request()
    if wait != 0: sleep(wait)
    self.exchange = f'ByBit{"_testnet" if use_testnet else ""}'
    self.key, self.secret = config[self.exchange]['keys and secrets'][key_secret_i]
    url = f'wss://stream{"-testnet" if use_testnet else ""}.bybit.com/v5/{"public/" if channel != "private" else ""}'
    self.ws = websocket.WebSocketApp(url=url+channel, *args, **kwargs)
    self.ws_th = Thread(target=lambda: self.ws.run_forever(ping_interval=ping_interval_s, ping_timeout=ping_timeout_s), daemon=True)
    self.ws_th.start()

  '''
  Force the websocket to be connected. This is useful because sending messages
  before it's properly connected will fail.
  '''
  def connected(self, monotonic_ms:int=None, connect_recheck_delay_ms=50, connect_attempt_s=1) -> bool:
    if monotonic_ms is None: monotonic_ms = monotonic_ns()//10**6
    while self.ws.sock is None or not self.ws.sock.connected:
      if monotonic_ns()//10**6-monotonic_ms > connect_attempt_s*10**3: return False
      sleep(connect_recheck_delay_ms/10**3)
    return True

  def authenticate(self, time_ms:int=None) -> int:
    if time_ms is None: time_ms = time_ns()//10**6
    expires_ms = time_ms+10**3
    sign = hmac.new(bytes(self.secret, 'utf-8'), bytes(f'GET/realtime{expires_ms}', 'utf-8'), digestmod='sha256').hexdigest()
    self.ws.send(json.dumps({'req_id':str(id := uuid4()), 'op':'auth', 'args':[self.key, expires_ms, str(sign)]}))
    return id

  def _send(self, op, topics:dict) -> int:
    self.ws.send(json.dumps({'req_id':str(id := uuid4()), 'op':op, 'args':topics}))
    return id

  def subscribe(self, topics:dict) -> int:
    return self._send('subscribe', topics)

  def unsubscribe(self, topics:dict) -> int:
    return self._send('unsubscribe', topics)
