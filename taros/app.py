import json

from bottle import route, run, request, response

import taro.client
import taro.jobs.repo as Jobs
from taro import dto, util
from taro.jobs import persistence
from taro.jobs.execution import ExecutionState
from taro.jobs.persistence import SortCriteria
from taros.httputil import http_error, query_digit, query


@route('/instances')
def instances():
    limit = query_digit('limit', default=-1)
    order = query('order', default='desc', allowed=('asc', 'desc'), aliases={'ascending': 'asc', 'descending': 'desc'})
    asc = order == 'asc'

    if request.GET.get('finished') is not None:  # Check if `finished` query param without value is present
        sort = query('sort', default='created', allowed=[c.name.lower() for c in SortCriteria])
        if not persistence.is_enabled():
            raise http_error(409, "Persistence is not enabled in the config file")
        jobs_info = persistence.read_jobs(sort=SortCriteria[sort.upper()], asc=asc, limit=limit)
    else:
        if query('sort'):
            raise http_error(412, "Query parameter 'sort' can be used only with query parameter 'finished'")
        jobs_info = list(util.sequence_view(
            taro.client.read_jobs_info()[0],
            sort_key=lambda j: j.lifecycle.changed(ExecutionState.CREATED),
            asc=asc,
            limit=limit))

    response.content_type = 'application/hal+json'
    embedded = {"instances": [resource_job_info(i) for i in jobs_info], 
                "jobs": [job_to_rescource(i) for i in jobs_filter(Jobs.get_all_jobs(), jobs_info)]}
    return to_json(resource({}, links={"self": "/instances", "jobs": "/jobs"}, embedded=embedded))


@route('/instances/<inst>')
def instance(inst):
    jobs_info, _ = taro.client.read_jobs_info(job_instance=inst)
    if not jobs_info:
        raise http_error(404, "Instance not found")

    response.content_type = 'application/hal+json'
    return to_json(resource_job_info(jobs_info[0]))


@route('/jobs')
def jobs():
    embedded = {"jobs": [job_to_rescource(i) for i in Jobs.get_all_jobs()]}
    response.content_type = 'application/hal+json'
    return to_json(resource({}, links={"self": "/jobs", "instances": "/instances"}, embedded=embedded)) 


@route('/jobs/<job_id>')
def jobs(job_id):
    job = Jobs.get_job(job_id)
    if not job:
        raise http_error(404, "Instance not found")

    embedded = job_to_rescource(job)
    response.content_type = 'application/hal+json'
    return to_json(embedded)


def job_to_rescource(job):
    return resource({"properties": job.properties},links={"self": "/jobs/" + job.job_id}) 


def jobs_filter(jobs, instances):   
    return [j for j in jobs if j.job_id in [i.job_id for i in instances]]


def resource(props, *, links=None, embedded=None):
    res = {}
    if links:
        res["_links"] = links
    if embedded:
        res["_embedded"] = embedded
    res.update(props)
    return res


def resource_job_info(job_info):
    return resource(dto.to_info_dto(job_info), links={"self": "/instances/" + job_info.instance_id, "jobs": "/jobs/" + job_info.job_id})


def to_json(d):
    return json.dumps(d, indent=2)


def start(host, port):
    run(host=host, port=port, debug=True, reloader=False)


if __name__ == '__main__':
    start('localhost', 8000)
