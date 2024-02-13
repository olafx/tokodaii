from typing import Any
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow.feather as feather
import pyarrow as pa

def from_pd(df:pd.DataFrame) -> dict[str, np.ndarray]:
  return {col:df[col].to_numpy() for col in df.columns}

def from_csv(*args, **kwargs) -> dict[str, np.ndarray]:
  return from_pd(pd.read_csv(*args, **kwargs))

def from_feather(filename:Path, *args, **kwargs) -> dict[str, np.ndarray]:
  return from_pd(feather.read_feather(filename, *args, **kwargs))

def to_feather(filename:Path, df:dict[str, Any], *args, **kwargs):
  feather.write_feather(pa.table(df), filename, *args, **kwargs)

def rename(df:dict[str, Any], from_to: dict[str, str]):
  for col in from_to.keys(): df[from_to[col]] = df.pop(col)

def as_type(df:dict[str, np.ndarray], col_type: dict[str, Any], *args, **kwargs):
  for col in df.keys(): df[col] = df[col].astype(col_type[col], *args, **kwargs)

def concat(dfs:list[dict[str, np.ndarray]]) -> dict[str, np.ndarray]:
  return {col:np.concatenate([df[col] for df in dfs]) for col in dfs[0].keys()}

def empty(cols:list[str]) -> dict[str, Any]:
  return {col:None for col in cols}

def empty_numpy(col_type:dict[str, Any]) -> dict[str, np.ndarray]:
  return {col:np.array([], dtype=col_type[col]) for col in col_type.keys()}

def empty_like(df:dict[str, Any]) -> dict[str, Any]:
  return empty(list(df.keys()))

def is_empty(df:dict[str, Any], check_all:bool=False):
  for col in df.keys():
    if df[col]: return False
  return True
