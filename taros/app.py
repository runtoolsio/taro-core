import json
from collections import Counter
from urllib.parse import quote

from bottle import response, Bottle

import taro.client
from taro import util
from taro.jobs import persistence
from taro.jobs import repo
from taro.jobs.execution import ExecutionState
from taro.jobs.inst import InstanceMatchingCriteria
from taro.jobs.job import JobMatchingCriteria
from taro.jobs.persistence import SortCriteria, PersistenceDisabledError
from taro.util import MatchingStrategy
from taros.httputil import http_error, query_digit, query, query_multi

api = Bottle()


@api.route('/instances')
def instances():
    include = query_multi('include', default='active', allowed=('active', 'finished', 'all'))
    limit = query_digit('limit', default=-1)
    job_limit = query_digit('job_limit', default=-1)
    if limit >= 0 and job_limit >= 0:
        raise http_error(412, "Query parameters 'limit' and 'job_limit' cannot be used together")
    order = query('order', default='desc', allowed=('asc', 'desc'), aliases={'ascending': 'asc', 'descending': 'desc'})
    asc = (order == 'asc')

    try:
        instance_match = _instance_match()
    except ValueError:
        return create_instances_response([])

    job_instances = []
    if 'finished' in include or 'all' in include:
        sort = query('sort', default='created', allowed=[c.name.lower() for c in SortCriteria])
        try:
            job_instances = persistence.read_instances(instance_match, SortCriteria[sort.upper()], asc=asc, limit=limit)
        except PersistenceDisabledError:
            raise http_error(409, "Persistence is not enabled")
    if 'active' in include or 'all' in include:
        if query('sort'):
            raise http_error(412, "Query parameter 'sort' can be used only with query parameter 'finished'")
        active_instances = taro.client.read_jobs_info(instance_match).responses
        job_instances = list(util.sequence_view(
            job_instances + active_instances,
            sort_key=lambda j: j.lifecycle.changed_at(ExecutionState.CREATED),
            asc=asc,
            limit=limit,
            filter_=job_limiter(job_limit)))

    return create_instances_response(job_instances)


def create_instances_response(job_instances):
    response.content_type = 'application/hal+json'
    embedded = {"instances": [resource_job_info(i) for i in job_instances],
                "jobs": [job_to_resource(i) for i in jobs_filter(repo.read_jobs(), job_instances)]}
    return to_json(resource({}, links={"self": "/instances", "jobs": "/jobs"}, embedded=embedded))


def _instance_match():
    return InstanceMatchingCriteria(jobs=_matched_jobs())


def _matched_jobs():
    req_jobs = query_multi('job')
    req_job_properties = query_multi('job_property')
    if not req_job_properties:
        return req_jobs

    jobs_by_properties = _find_jobs_by_properties(req_job_properties)
    if not jobs_by_properties and not req_jobs:
        raise ValueError

    return set(req_jobs).union(jobs_by_properties)


def _find_jobs_by_properties(req_job_properties):
    properties = {}
    for req_job_prop in req_job_properties:
        try:
            name, value = req_job_prop.rsplit(':', maxsplit=1)
            properties[name] = value
        except ValueError:
            raise http_error(412, "Query parameter 'job_property' must be in format name:value but was "
                             + req_job_prop)

    job_criteria = JobMatchingCriteria(properties=properties, property_match_strategy=MatchingStrategy.PARTIAL)
    return {j.id for j in job_criteria.matched(repo.read_jobs())}


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
    jobs_info, _ = taro.client.read_jobs_info(match_criteria)
    if not jobs_info:
        raise http_error(404, "Instance not found")

    response.content_type = 'application/hal+json'
    return to_json(resource_job_info(jobs_info[0]))  # TODO Ensure always only 1


@api.route('/jobs')
def jobs():
    embedded = {"jobs": [job_to_resource(i) for i in repo.read_jobs()]}
    response.content_type = 'application/hal+json'
    return to_json(resource({}, links={"self": "/jobs", "instances": "/instances"}, embedded=embedded))


@api.route('/jobs/<job_id>')
def jobs(job_id):
    job = repo.read_job(job_id)
    if not job:
        raise http_error(404, "Instance not found")

    embedded = job_to_resource(job)
    response.content_type = 'application/hal+json'
    return to_json(embedded)


def job_to_resource(job):
    return resource({"id": job.id, "properties": job.properties}, links={"self": "/jobs/" + quote(job.id)})


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


def resource_job_info(job_info):
    return resource(job_info.to_dict(),
                    links={"self": "/instances/" + quote(job_info.instance_id),
                           "jobs": "/jobs/" + quote(job_info.job_id)}
                    )


def to_json(d):
    return json.dumps(d, indent=2)


def start(host, port, reload=False):
    api.run(host=host, port=port, debug=True, reloader=reload)


if __name__ == '__main__':
    taro.load_defaults()
    start('localhost', 8000, True)
