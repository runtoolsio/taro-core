from bottle import route, run, request

from taro import client, dto, persistence, cnf, util, ExecutionState
from taro.taros.httputil import http_error, query_digit, query


@route('/instances')
def instances():
    limit = query_digit('limit', default=-1)
    order = query('order', default='desc', allowed=('asc', 'desc'), aliases={'ascending': 'asc', 'descending': 'desc'})
    asc = order == 'asc'

    if request.GET.get('finished') is not None:
        if not persistence.init():
            raise http_error(409, "Persistence is not enabled in the config file")
        jobs_info = persistence.read_jobs(asc=asc, limit=limit)
    else:
        jobs_info = util.sequence_view(
            client.read_jobs_info(),
            sort_key=lambda j: j.lifecycle.changed(ExecutionState.CREATED),
            asc=asc,
            limit=limit)
    embedded = {"instances": [resource_job_info(i) for i in jobs_info]}
    return resource({}, links={"self": "/instances"}, embedded=embedded)


@route('/instances/<inst>')
def instance(inst):
    jobs_info = client.read_jobs_info(instance=inst)
    if not jobs_info:
        raise http_error(404, "Instance not found")
    return resource_job_info(jobs_info[0])


def resource(props, *, links=None, embedded=None):
    res = {}
    if links:
        res["_links"] = links
    if embedded:
        res["_embedded"] = embedded
    res.update(props)
    return res


def resource_job_info(job_info):
    return resource(dto.to_info_dto(job_info), links={"self": "/instances/" + job_info.instance_id})


cnf.init(None)

run(host='localhost', port=8080, debug=True, reloader=True)
