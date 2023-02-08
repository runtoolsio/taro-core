import re
import secrets
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import Enum

from dateutil import relativedelta

# Produced by ChatGPT - seems correct
ISO_DATE_TIME_PATTERN = re.compile(
    r'\b(\d{4}-\d{2}-\d{2}(?:T|\s)\d{2}:\d{2}:\d{2}(?:\.\d{3})?(?:Z|[+-]\d{2}:\d{2})?)\b')


def unique_timestamp_hex(random_suffix_length=4):
    return secrets.token_hex(random_suffix_length) + format(int(datetime.utcnow().timestamp() * 1000000), 'x')[::-1]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def str_to_datetime(str_ts):
    if not str_ts:
        return None

    sep = "T" if "T" in str_ts else " "
    dec = ".%f" if "." in str_ts else ""
    zone = "%z" if any(1 for z in ('z', 'Z', '+') if z in str_ts) else ""

    return datetime.strptime(str_ts, "%Y-%m-%d" + sep + "%H:%M:%S" + dec + zone)


def format_timedelta(td):
    mm, ss = divmod(td.seconds, 60)
    hh, mm = divmod(mm, 60)
    s = "%02d:%02d:%02d" % (hh, mm, ss)
    if td.days:
        def plural(n):
            return n, abs(n) != 1 and "s" or ""

        s = ("%d day%s, " % plural(td.days)) + s
    if td.microseconds:
        s = s + (".%06d" % td.microseconds)[:-3]
        # s = s + ("%f" % (td.microseconds / 1000000))[1:-3]
    return s


def str_to_seconds(val):
    value = float(val[:-1])
    unit = val[-1].lower()

    if unit == 's':
        return value
    if unit == 'm':
        return value * 60
    if unit == 'h':
        return value * 60 * 60
    if unit == 'd':
        return value * 60 * 60 * 24

    raise ValueError("Unknown unit: " + unit)


def parse_iso8601_duration(duration):
    match = re.match(r'P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)W)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?', duration)
    if not match:
        raise ValueError('Invalid duration: ' + duration)
    years = int(match.group(1)) if match.group(1) else 0
    months = int(match.group(2)) if match.group(2) else 0
    weeks = int(match.group(3)) if match.group(3) else 0
    days = int(match.group(4)) if match.group(4) else 0
    hours = int(match.group(5)) if match.group(5) else 0
    minutes = int(match.group(6)) if match.group(6) else 0
    seconds = int(match.group(7)) if match.group(7) else 0
    return relativedelta.relativedelta(years=years, months=months, weeks=weeks, days=days, hours=hours, minutes=minutes,
                                       seconds=seconds).normalized()


def datetime_to_str(td):
    if td is None:
        return None
    return td.isoformat()


def format_dt_ms_local_tz(dt, null=''):
    if not dt:
        return null

    return dt.astimezone().replace(tzinfo=None).isoformat(sep=' ', timespec='milliseconds')


def format_time_ms_local_tz(dt):
    if not dt:
        return 'N/A'

    return dt.astimezone().strftime('%H:%M:%S.%f')[:-3]


class DateTimeFormat(Enum):
    DATE_TIME_MS_LOCAL_ZONE = (format_dt_ms_local_tz,)
    TIME_MS_LOCAL_ZONE = (format_time_ms_local_tz,)
    NONE = (lambda dt: None,)

    def __call__(self, *args, **kwargs):
        return self.value[0](*args, **kwargs)


class TimePeriod(ABC):

    @property
    @abstractmethod
    def start_date(self):
        pass

    @property
    @abstractmethod
    def end_date(self):
        pass
