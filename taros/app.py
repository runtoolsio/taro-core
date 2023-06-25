import json
from collections import Counter
from urllib.parse import quote

from bottle import response, Bottle, request

import taro.client
from taro import util
from taro.jobs import persistence
from taro.jobs import repo
from taro.jobs.execution import ExecutionState, ExecutionStateFlag
from taro.jobs.inst import InstanceMatchingCriteria, IDMatchingCriteria, IntervalCriteria, LifecycleEvent, StateCriteria
from taro.jobs.job import JobMatchingCriteria
from taro.jobs.persistence import SortCriteria, PersistenceDisabledError
from taro.util import MatchingStrategy
from taros.httputil import http_error, query_digit, query, query_multi

api = Bottle()


@api.route('/instances')
def instances(links=None):
    include = query_multi('include', default='active', allowed=('active', 'finished', 'all'))
    limit = query_digit('limit', default=-1)
    offset = query_digit('offset', default=0)
    if offset and ('all' in include or len(include) > 1):
        raise http_error(422, "Query parameter 'offset' cannot be used for both active and finished instances")
    job_limit = query_digit('job_limit', default=-1)
    if limit >= 0 and job_limit >= 0:
        raise http_error(422, "Query parameters 'limit' and 'job_limit' cannot be used together")
    order = query('order', default='desc', allowed=('asc', 'desc'), aliases={'ascending': 'asc', 'descending': 'desc'})
    asc = (order == 'asc')

    request.jobs = repo.read_jobs()  # Move to create request ctx function if more data in the ctx

    try:
        instance_match = _instance_match()
    except NoJobMatchesException:
        return create_instances_response([], links)

    job_instances = []
    if 'finished' in include or 'all' in include:
        sort = query('sort', default='created', allowed=[c.name.lower() for c in SortCriteria])
        try:
            job_instances = persistence.read_instances(
                instance_match, SortCriteria[sort.upper()], asc=asc, limit=limit, offset=offset)
        except PersistenceDisabledError:
            raise http_error(409, "Persistence is not enabled")
    if 'active' in include or 'all' in include:
        if query('sort'):
            raise http_error(422, "Query parameter 'sort' can be used only with query parameter 'finished'")
        active_instances = taro.client.read_jobs_info(instance_match).responses
        job_instances = list(util.sequence_view(
            job_instances + active_instances,
            sort_key=lambda j: j.lifecycle.changed_at(ExecutionState.CREATED),
            asc=asc,
            limit=limit,
            offset=offset,
            filter_=job_limiter(job_limit)))

    return create_instances_response(job_instances, links)


def create_instances_response(job_instances, links=None):
    links = links or {"self": "/instances", "jobs": "/jobs"}
    response.content_type = 'application/hal+json'
    embedded = {"instances": [resource_instance(i) for i in job_instances],
                "jobs": [resource_job(i) for i in jobs_filter(request.jobs, job_instances)]}
    return to_json(resource({}, links=links, embedded=embedded))


def _instance_match():
    ids = query_multi('id')
    if ids:
        id_criteria = [IDMatchingCriteria.parse_pattern(i, MatchingStrategy.PARTIAL) for i in ids]
    else:
        id_criteria = []

    from_str = query('from')
    to_str = query('to')
    if from_str or to_str:
        try:
            interval_criteria = IntervalCriteria.to_utc(LifecycleEvent.CREATED, from_str, to_str)
        except ValueError as e:
            raise http_error(422, f"Invalid date or date-time value: {e}")
    else:
        interval_criteria = None

    flags_str = query_multi('flag')
    try:
        flag_groups = [{ExecutionStateFlag[flag_str.upper()]} for flag_str in flags_str]
    except KeyError as e:
        raise http_error(422, f"Invalid flag: {e}, allowed values are {[f.name.lower() for f in ExecutionStateFlag]}")
    state_criteria = StateCriteria(flag_groups=flag_groups)

    return InstanceMatchingCriteria(id_criteria, interval_criteria, state_criteria, _matched_jobs())


def _matched_jobs():
    req_jobs = query_multi('job')
    req_job_properties = query_multi('job_property')
    if not req_job_properties:
        return req_jobs

    jobs_by_properties = _find_jobs_by_properties(req_job_properties)
    if not jobs_by_properties and not req_jobs:
        raise NoJobMatchesException

    return set(req_jobs).union(jobs_by_properties)


def _find_jobs_by_properties(req_job_properties):
    properties = {}
    for req_job_prop in req_job_properties:
        try:
            name, value = req_job_prop.rsplit(':', maxsplit=1)
            properties[name] = value
        except ValueError:
            raise http_error(422, "Query parameter 'job_property' must be in format name:value but was "
                             + req_job_prop)

    job_criteria = JobMatchingCriteria(properties=properties, property_match_strategy=MatchingStrategy.PARTIAL)
    return {j.id for j in job_criteria.matched(request.jobs)}


def job_limiter(limit):
    if limit == -1:
        return lambda _: True

    c = Counter()

    def filter_(job_info):
        c[job_info.job_id] += 1
        return c[job_info.job_id] <= limit

    return filter_


@api.route('/instances/<inst>')
def instance(inst):
    if "@" not in inst:
        raise http_error(404, "Instance not found")

    match_criteria = InstanceMatchingCriteria.parse_pattern(inst, MatchingStrategy.EXACT)
    job_instances, _ = taro.client.read_jobs_info(match_criteria)

    if not job_instances:
        try:
            job_instances = persistence.read_instances(match_criteria)
        except PersistenceDisabledError:
            raise http_error(404, "Instance not found")

    if not job_instances:
        raise http_error(404, "Instance not found")

    response.content_type = 'application/hal+json'
    return to_json(resource_instance(job_instances[0]))  # TODO How to handle duplicated instances?


@api.route('/jobs')
def jobs():
    embedded = {"jobs": [resource_job(i) for i in repo.read_jobs()]}
    response.content_type = 'application/hal+json'
    return to_json(resource({}, links={"self": "/jobs", "instances": "/instances"}, embedded=embedded))


@api.route('/jobs/<job_id>')
def jobs(job_id):
    job = repo.read_job(job_id)
    if not job:
        raise http_error(404, "Instance not found")

    embedded = resource_job(job)
    response.content_type = 'application/hal+json'
    return to_json(embedded)

@api.route('/jobs/<job_id>/instances')
def instances_of_job(job_id):
    request.query.replace('job', job_id)
    links = {"self": f"/jobs/{job_id}/instances", "jobs": f"/jobs/{job_id}"}
    return instances(links)


def jobs_filter(jobs_, instances_):
    return [j for j in jobs_ if j.id in [i.job_id for i in instances_]]  # TODO replace with criteria


def resource(props, *, links=None, embedded=None):
    res = {}
    if links:
        res["_links"] = links
    if embedded:
        res["_embedded"] = embedded
    res.update(props)
    return res


def resource_job(job):
    return resource(
        {"id": job.id, "properties": job.properties},
        links={"self": "/jobs/" + quote(job.id), "instances": "/jobs/" + quote(job.id) + "/instances"}
    )


def resource_instance(job_inst):
    return resource(
        job_inst.to_dict(),
        links={"self": "/instances/" + quote(repr(job_inst.id)),
               "job": "/jobs/" + quote(job_inst.job_id)}
    )


def to_json(d):
    return json.dumps(d, indent=2)


class NoJobMatchesException(Exception):
    pass


def start(host, port, reload=False):
    api.run(host=host, port=port, debug=True, reloader=reload)


if __name__ == '__main__':
    taro.load_defaults()
    start('localhost', 8000, True)
