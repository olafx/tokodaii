from pathlib import Path
import os, atexit

# Various globals are assigned here, which must be done in a specific order.

PATH = Path.home()/'.tokodaii'

# Set up the config.
from tokodaii import config
if config.exists():
  config.config = config.read()
else:
  os.makedirs(PATH, exist_ok=True)
  config.config = config.default()
  config.write(config.config)

# Guard states need to be manually written at exit because of Python's issues
# with destroying objects in the wrong order at exit.
def _exiter():
  from tokodaii.auto.guard import guards
  for guard in guards: guard.write()
atexit.register(_exiter)
