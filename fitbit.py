#!/usr/bin/env python3

#
# Prerequisites:
# FitBit API Setup
# * See the comments in the 'fitbit_creds_setup' script.
#

CREDS_FILE = 'fitbit.creds'
API_BASE_URL = 'https://api.fitbit.com'
TIMEOUT = 10



import base64
import json
import logging
import os
import requests
from datetime import datetime, date, timedelta
from urllib.parse import urljoin

import filesystem

_log = logging.getLogger(__name__)

# Pretty-format a data structure.
def serializer(obj):
  if isinstance(obj, (datetime, date)):
    return obj.isoformat()
  return str(obj)
  #raise TypeError(f'Object of type {o.__class__.__name__} is not JSON serializable')
def pretty_write(file_handle, data, sort_keys=False):
  json.dump(data, file_handle, indent=2, sort_keys=sort_keys, default=serializer)
  file_handle.write('\n')
def pretty(data, sort_keys=False):
  return json.dumps(data, indent=2, sort_keys=sort_keys, default=serializer)



class FitbitClient(requests.Session):

  def __init__(self, creds_file=CREDS_FILE, creds_path=None, base_url=API_BASE_URL, *args, **kwargs):
    super(FitbitClient, self).__init__(*args, **kwargs)

    self.creds_file = creds_file
    self.creds_file_path = (os.path.join(creds_path, creds_file) if creds_path else creds_file)
    with open(self.creds_file_path, 'r') as f:
      # json.load() throws a generic exception if the file is empty, so check for that explicitly
      if f.read(1) == '':
        raise Exception(f"FitBit creds file '{creds_file}' is empty")
      f.seek(0)
      creds = self.creds = json.load(f)
    if 'issued_at' in creds:
      creds['issued_at'] = datetime.fromisoformat(creds['issued_at'])
    if 'expires_at' in creds:
      creds['expires_at'] = datetime.fromisoformat(creds['expires_at'])
    self._prep_creds()

    self.api_base_url = base_url
    self.timeout = TIMEOUT
    self.headers['Accept'] = 'application/json'

  def _prep_creds(self):
    creds = self.creds
    self.creds_expire_at = None
    if 'expires_at' in creds:
      self.creds_expire_at = creds['expires_at']
    elif 'issued_at' in creds and 'expires_in' in creds and creds['expires_in'] > 0:
      self.creds_expire_at = creds['issued_at'] - timedelta(seconds=creds['expires_in'])
    self.headers['Authorization'] = 'Bearer '+creds['access_token']
    self.scopes = creds['scope'].split()

  # Intercept all requests and automatically refresh expired tokens.
  def request(self, method, url, *args, **kwargs):
    if not url.startswith('http'):
      url = urljoin(self.api_base_url, url)
    expires_at = self.creds_expire_at
    if expires_at and expires_at < datetime.now():
      self.refresh()
    r = super(FitbitClient, self).request(method, url, *args, **kwargs)
    if r.status_code == 401 and '"expired_token"' in r.text:
      self.refresh()
      r = super(FitbitClient, self).request(method, url, *args, **kwargs)
    if r.status_code != 200:
      _log.error(f"FitBit API returned HTTP error {r.status_code}:\n{r.text}")
      r.raise_for_status()
    return r

  REFRESH_URI = '/oauth2/token'
  def refresh(self):
    _log.debug('Refreshing FitBit OAuth tokens')

    creds = self.creds
    client_id = creds['client_id']
    client_secret = (creds['client_secret'] if 'client_secret' in creds else None)
    refresh_token = creds['refresh_token']

    url = self.api_base_url + self.REFRESH_URI
    headers = {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Accept': 'application/json',
    }
    if client_secret:
      app_auth_data = base64.urlsafe_b64encode((client_id+':'+client_secret).encode('utf-8')).decode('utf-8')
      headers['Authorization'] = 'Basic '+app_auth_data
    params = {
      'grant_type': 'refresh_token', 'refresh_token': refresh_token,
    }
    if not client_secret:
      params['client_id'] = client_id

    issued_at = datetime.now()
    r = requests.post(url, headers=headers, data=params, timeout=TIMEOUT)
    if r.status_code != 200:
      _log.error(f"'{url}' returned HTTP error {r.status_code}:\n{r.text}")
      r.raise_for_status()
    creds.update(r.json())
    creds['issued_at'] = issued_at
    if 'expires_in' in creds and creds['expires_in'] > 0:
      creds['expires_at'] = issued_at + timedelta(seconds=creds['expires_in'])

    # Update the credentials file.
    with filesystem.open_for_atomic_isolated_durable_read_write(self.creds_file_path, perms=0o600) as f:
      pretty_write(f, creds, sort_keys=True)

    self._prep_creds()

    _log.debug(f"Successfully refreshed FitBit OAuth tokens and updated {self.creds_file}")

# Return a `list` of ISO format date `str` objects for each day in the specified date range, for use
# with FitBit API calls that only support returning data for a single day.
#
# * This behaves similarly to the Python `range()` built-in, except that it includes the end date in
#   the returned list by default.  Specify `False` for 'incl_end' to exclude the end date.
# * 'date_range' must be a `tuple` of `date` objects or ISO format `str` objects indicating the date
#   range.  The first entry in the `tuple` will be used as the start date and will be included in
#   the returned `list`.  The second entry in the `tuple` will be used as the end date and will also
#   be included in the returned `list`, unless 'incl_end' is `False`.  These values must be in
#   chronological order (the first entry must be less than the second entry).
# * If 'reverse' is `False` (the default) then the returned dates will be ordered in chronological
#   order.  If 'reverse' is `True` then they will be ordered in reverse chronological order.
# * 'limit' may an `int` to limit the number of returned dates or `None` to disable the limit.  If
#   the specified range includes more days than this limit then only the most recent dates within
#   this limit will be returned.  (Default is 32.)
#
def expand_date_range(date_range, incl_end=True, reverse=False, limit=32):
  start_date = date_range[0]
  if not isinstance(start_date, date):
    start_date = date.fromisoformat(str(start_date))
  end_date = date_range[1]
  if not isinstance(end_date, date):
    end_date = date.fromisoformat(str(end_date))
  dates = []
  one_day = timedelta(days=1)
  cur_date = end_date
  if not incl_end:
    cur_date -= one_day
  for d in range(0, (end_date - start_date).days):
    if limit and len(dates) >= limit:
      break
    dates.append(cur_date.isoformat())
    cur_date -= one_day
  if incl_end and (not limit or len(dates) < limit):
    dates.append(cur_date.isoformat())
  if not reverse:
    return list(reversed(dates))
  return dates

# Return a `list` of date ranges covering the specified date range, where each date range is a
# `tuple` of ISO format date `str` objects, and where the first entry in the `tuple` is the start
# date and the second entry in the `tuple` is the end date.  This is intended for use with FitBit
# API calls that support returning data for a date range with a limit on the number of days in the
# range.
#
# * 'date_range' must be a `tuple` of `date` objects or ISO format `str` objects indicating the date
#   range to generate smaller ranges for.  The first entry in the `tuple` will be used as the start
#   date, and the second entry in the `tuple` will be used as the end date.  These values must be in
#   chronological order (the first entry must be less than the second entry).
# * 'range_days' must be an `int` to specify the number of days to be included in each returned
#   range.  (Default is 30.)
# * 'limit' may an `int` to limit the number of returned date ranges or `None` to disable the limit.
#   If more than this number of ranges would be returned then an exception will be thrown.
#   (Default is 16.)
# * If 'reverse' is `False` (the default) then the returned date ranges will be ordered in
#   chronological order.  If 'reverse' is `True` then they will be ordered in reverse chronological
#   order.
#
def expand_date_range_to_ranges(date_range, range_days=30, limit=16, reverse=False):
  start_date = date_range[0]
  if not isinstance(start_date, date):
    start_date = date.fromisoformat(str(start_date))
  end_date = date_range[1]
  if not isinstance(end_date, date):
    end_date = date.fromisoformat(str(end_date))
  ranges = []
  one_day = timedelta(days=1)
  range_diff = timedelta(days=(range_days-1))
  cur_start_date = start_date
  while True:
    cur_end_date = cur_start_date + range_diff
    if cur_end_date > end_date:
      cur_end_date = end_date
    ranges.append((cur_start_date.isoformat(), cur_end_date.isoformat()))
    if cur_end_date == end_date:
      break
    if len(ranges) + 1 > limit:
      raise Exception(f"Specified date range {start_date.isoformat()} to {end_date.isoformat()} exceeds limit of {limit} ranges of {range_days} days")
    cur_start_date = cur_end_date + one_day
  if reverse:
    return list(reversed(ranges))
  return ranges
