'''
A guard against API limits. The idea is to use this system to efficiently work
on the edge of the API limits, for exchanges where the API limit is not
explicitly returned after every request. It's safe even when opening and closing
a program on a timescale smaller than the API limit timescale. But it does not
protect if there are multiple instances, i.e. it's not global, let alone global
on the IP address. So it is to be used with caution still.
'''

from collections import deque
from time import time_ns as time_ns_
import numpy as np
from tokodaii import PATH

'''
Guards have important data that must be written at exit. We can't rely on
their destructors to do this because Python has issues destroying objects in
the right order at exit. So we must keep them together, and deal with this
list on exit. Even if we manually destroy a guard, it should not be removed from
this list.
'''
guards = []

'''
Each exchange has a guard, as defined in `tokodaii.config.config`. These should
not be manually constructed, they are automatically constructed while an
exchange, is imported, e.g. on importing `tokodaii.bybit`. The guard object is
defined by its limits, which are given for each exchange in
`tokodaii.config.config`. The guard keeps a history (implemented as a `deque`)
of past requests and future reserved requests. The guard will write its history
to local storage on exit. Requests made through guard can be reserved, or not.
If reserved, guard can't fail, and will return some amount of time you should
wait before making the request. No 2nd check-in is needed, it is safe to make
the request, but it must be approximately at that time, and it is assumed that
you indeed make it. If not reserved, guard can deny a request. The intended use
case is to reserve it, since this is overall the faster solution.
'''
class Guard():

  def __init__(self, name, limits):
    self.name = name
    self.path = PATH/f'guard_{name}.npy'
    self.limits = sorted(limits, key=lambda d:d['time'])
    self.history = self.read() if self.exists() else self._create_deque()
    guards.append(self)

  def _create_deque(self) -> deque:
    history_maxlen = max(l['time']*l['count'] for l in self.limits)
    return deque(maxlen=history_maxlen)

  def exists(self) -> bool:
    return self.path.is_file()

  def read(self) -> deque:
    history = self._create_deque()
    for e in np.load(self.path): history.append(e)
    return history

  def write(self):
    self._remove_old_requests(time_ns_())
    if len(self.history) != 0:
      np.save(self.path, np.array(self.history, dtype='int64, int16'))

  def _remove_old_requests(self, time_ns:int):
    # The last limit has the longest time interval.
    t_max = self.limits[-1]['time']+self.limits[-1]['time tol']
    while self.history and time_ns-self.history[-1][0] > t_max*10**9: self.history.pop()

  def request(self, n_requests:int=1, reserve:bool=True) -> int:
    self._remove_old_requests(time_ns := time_ns_())
    # Number of requests made during the counting process, wait time for each of
    # the limits, and which limits have been broken.
    n, wait, limit_broken = 0, [0]*len(self.limits), [False]*len(self.limits)
    # Most future request.
    t0 = time_ns if len(self.history) == 0 else max(time_ns, self.history[0][0])
    # Conceptually add the current request, and iterate to the past.
    for t, reqs in self.history:
      n += reqs
      # Which limits are broken if we were to add this request?
      for j in range(len(self.limits)):
        limit = self.limits[j]
        if not limit_broken[j]:
          if n+n_requests+limit['time']*limit['count tol'] >= limit['time']*limit['count']:
            wait[j] = limit['time']+limit['time tol']-(t0-t)/10**9
            limit_broken[j] = True
      # Because we're conceptually in the future, there may be requests that are
      # too old to care about, despite having removed old requests.
      if (t0-t)/10**9 > self.limits[-1]['time']+self.limits[-1]['time tol']: break
    wait = max(wait)
    if reserve or wait == 0: self.history.appendleft((t0+wait, n_requests))
    return wait
