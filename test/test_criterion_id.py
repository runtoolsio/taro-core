from operator import eq

from tarotools.taro.criteria import JobRunIdCriterion


def test_full_match():
    pattern = 'job_id@instance_id'
    sut = JobRunIdCriterion.parse_pattern(pattern, strategy=eq)

    assert sut.matches(('job_id', 'instance_id'))
    assert not sut.matches(('job_id', 'instance'))
    assert not sut.matches(('job', 'instance_id'))


def test_match():
    pattern = 'identifier'
    sut = JobRunIdCriterion.parse_pattern(pattern, strategy=eq)

    assert sut.matches(('identifier', 'any_instance_id'))
    assert sut.matches(('any_job_id', 'identifier'))
    assert not sut.matches(('job', 'any_instance_id'))
    assert not sut.matches(('any_job_id', 'instance'))


def test_individual_id_match():
    job_id_pattern = 'job_id@'
    instance_id_pattern = '@instance_id'

    sut_job_id = JobRunIdCriterion.parse_pattern(job_id_pattern, strategy=eq)
    sut_instance_id = JobRunIdCriterion.parse_pattern(instance_id_pattern, strategy=eq)

    assert sut_job_id.matches(('job_id', 'any_instance_id'))
    assert sut_instance_id.matches(('any_job_id', 'instance_id'))
    assert not sut_job_id.matches(('any_job_id', 'job_id'))
    assert not sut_instance_id.matches(('instance_id', 'any_instance_id'))
