from tarotools.taro.jobs.inst import IntervalCriteria, LifecycleEvent


def test_interval_utc_conversion():
    c = IntervalCriteria.to_utc(LifecycleEvent.CREATED, from_val='2023-11-10T09:00+02:00', to_val=None)
    assert c.from_dt.hour == 7
