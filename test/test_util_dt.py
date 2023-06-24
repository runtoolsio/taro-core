import datetime

from taro.util import parse


def test_parse_full_tz():
    parsed = parse('2023-04-23 13:00:00+01:00')
    assert parsed.tzinfo is not None

def test_parse_full_utc():
    parsed = parse('2023-04-23 13:00:00Z')
    assert parsed.tzinfo is not None

def test_parse_short_tz():
    parsed = parse('2023-04-23 13:00+01:00')
    assert parsed.tzinfo is not None

def test_parse_date():
    parsed = parse('2023-04-23')
    assert isinstance(parsed, datetime.date)
