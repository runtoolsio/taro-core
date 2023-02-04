from taro.util import KVParser


def test_default_parse():
    kv = KVParser()
    parsed = kv.parse("noise here.. event=[downloaded and stored] count=[10] noise there.. unit=files")
    assert parsed == {"event": "downloaded and stored", "count": "10", "unit": "files"}


def test_prefix_parse():
    kv = KVParser(prefix='prefixed_')
    parsed = kv.parse("field=value")
    assert parsed == {"prefixed_field": "value"}


def test_trim_parse():
    kv = KVParser(trim_key='/|', trim_value='\\')
    parsed = kv.parse("/field/=\\value\\")
    assert parsed == {"field": "value"}
    parsed = kv.parse("|field|=value")
    assert parsed == {"field": "value"}
