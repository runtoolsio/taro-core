from operator import eq

from taro.jobs.job import JobInstanceID as jid


def test_full_match():
    sut = jid('job_id', 'instance_id')

    assert sut.matches('job_id@instance_id', matching_strategy=eq)
    assert not sut.matches('job_id@instance', matching_strategy=eq)
    assert not sut.matches('job@instance_id', matching_strategy=eq)


def test_match():
    sut = jid('job_id', 'instance_id')

    assert sut.matches('job_id', matching_strategy=eq)
    assert sut.matches('instance_id', matching_strategy=eq)
    assert not sut.matches('job', matching_strategy=eq)
    assert not sut.matches('instance', matching_strategy=eq)


def test_individual_id_match():
    sut = jid('job_id', 'instance_id')

    assert sut.matches('job_id@', matching_strategy=eq)
    assert sut.matches('@instance_id', matching_strategy=eq)
    assert not sut.matches('@job_id', matching_strategy=eq)
    assert not sut.matches('instance_id@', matching_strategy=eq)
