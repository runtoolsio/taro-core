from taro.util import KVParser


def test_default():
    kv = KVParser()
    parsed = kv.parse("noise here.. event=[downloaded and stored] count=[10] noise there.. unit=files")
    assert parsed == {"event": "downloaded and stored", "count": "10", "unit": "files"}


def test_prefix():
    kv = KVParser(prefix='prefixed_')
    parsed = kv.parse("field=value")
    assert parsed == {"prefixed_field": "value"}


def test_trim():
    kv = KVParser(trim_key='/|', trim_value='\\')
    parsed = kv.parse("/field/=\\value\\")
    assert parsed == {"field": "value"}
    parsed = kv.parse("|field|=value")
    assert parsed == {"field": "value"}


def test_exclude_keys():
    kv = KVParser(field_split="&", exclude_keys={"k1"})
    parsed = kv.parse("k1=v1&k2=v2")
    assert parsed == {"k2": "v2"}


def test_alias():
    kv = KVParser(value_split=":", aliases={'k1': 'key1'})
    parsed = kv.parse("k1:value1 key2:value2")
    assert parsed == {"key1": "value1", "key2": "value2"}
