from taro.util import KVParser


def test_default_parse():
    kv = KVParser()
    parsed = kv.parse("noise here.. event=[downloaded and stored] count=[10] noise there.. unit=files")
    assert parsed == {"event": "downloaded and stored", "count": "10", "unit": "files"}
