def down(x, res=1):
  return round(x//res)*res

def up(x, res=1):
  return -round(-x//res)*res

def to_zero(x, res=1):
  return round(x//res)*res if x > 0 else -round(-x//res)*res

def to_infty(x, res=1):
  return -round(-x//res)*res if x > 0 else round(x//res)*res

def nearest(x, res=1):
  return round(x/res)*res if x > 0 else -round(-x/res)*res
