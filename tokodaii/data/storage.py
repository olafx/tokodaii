'''
Deals with local storage for dataframes, defining how they are stored. They're
all stored in compressed feather files.
'''

from typing import Any
import os
from pathlib import Path
import numpy as np
from tokodaii.config import config
from tokodaii.utils import dataframe

def path(*args) -> Path:
  return Path(config['data']['storage path'], *args)

'''
Read filenames. If `fmt` is specified, only read filenames of that filetype. The
filetype is not included in the returned filenames.
'''
def read_filenames(path:Path, fmt:str=None) -> list[str]:
  if os.path.exists(path):
    if fmt: return sorted([a.name[:-len(fmt)-1] for a in os.scandir(path) if a.is_file() and a.name.endswith(f'.{fmt}')])
    else: return sorted([a.name for a in os.scandir(path) if a.is_file()])
  else:
    return []

def read_folders(path:Path) -> list[str]:
  if os.path.exists(path):
    return sorted([a.name for a in os.scandir(path) if a.is_dir()])
  else: return []

def write_feather(filename:Path, df:dict[str, Any]):
  os.makedirs(filename.parent, exist_ok=True)
  dataframe.to_feather(filename, df, version=2, compression=config['data']['processing']['compressor'], compression_level=config['data']['processing']['compression level'])

def read_feather(filename:Path, *args, **kwargs) -> dict[str, np.ndarray]:
  return dataframe.from_feather(filename, *args, **kwargs)
