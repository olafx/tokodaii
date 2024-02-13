'''
Deal with time conversions. Timestamps are in UTC.
'''

from datetime import datetime as dt, timezone
utc = timezone.utc

FMT_date = '%Y-%m-%d'
FMT_date_hm = '%Y-%m-%d %H:%M'
FMT_date_hms = '%Y-%m-%d %H:%M:%S'
FMT_date_hms_us = '%Y-%m-%d %H:%M:%S.%f'

def unix(datetime:dt) -> float:
  return datetime.timestamp()
def unix_s(datetime:dt) -> int:
  return int(datetime.timestamp())
def unix_ms(datetime:dt) -> int:
  return int(datetime.timestamp()*10**3)
def unix_us(datetime:dt) -> int:
  return int(datetime.timestamp()*10**6)
def unix_ns(datetime:dt) -> int:
  return int(datetime.timestamp()*10**9)

def from_unix(time_unix:float) -> dt:
  return dt.utcfromtimestamp(time_unix).replace(tzinfo=utc)
def from_unix_s(time_unix_s:int) -> dt:
  return dt.utcfromtimestamp(time_unix_s).replace(tzinfo=utc)
def from_unix_ms(time_unix_ms:int) -> dt:
  return dt.utcfromtimestamp(time_unix_ms/10**3).replace(tzinfo=utc)
def from_unix_us(time_unix_us:int) -> dt:
  return dt.utcfromtimestamp(time_unix_us/10**6).replace(tzinfo=utc)
def from_unix_ns(time_unix_ns:int) -> dt:
  return dt.utcfromtimestamp(time_unix_ns/10**9).replace(tzinfo=utc)

def from_str_date(datetime:str) -> dt:
  return dt.strptime(datetime, FMT_date).replace(tzinfo=utc)

def dt_to_str_date(datetime:dt) -> str:
  return datetime.strftime(FMT_date)
def dt_to_str_date_hm(datetime:dt) -> str:
  return datetime.strftime(FMT_date_hm)
def dt_to_str_date_hms(datetime:dt) -> str:
  return datetime.strftime(FMT_date_hms)
def dt_to_str_date_hms_us(datetime:dt) -> str:
  return datetime.strftime(FMT_date_hms_us)

def strip_time(datetime:dt) -> dt:
  return datetime.replace(hour=0, minute=0, second=0, microsecond=0)
