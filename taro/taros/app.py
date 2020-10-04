from bottle import route, run, HTTPError, request

from taro import client, dto, persistence, cnf


@route('/instances')
def instances():
    limit = request.query.limit or -1
    if not limit.isdigit():
        raise http_error(412, 'Limit param must be number')

    if request.GET.get('finished') is not None:
        if not persistence.init():
            raise http_error(409, "Persistence is not enabled in the config file")
        jobs_info = persistence.read_jobs(limit=limit)
    else:
        jobs_info = client.read_jobs_info()
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


def http_error(status, message):
    return HTTPError(status=status, body='{"message": "' + message + '"}')


cnf.init(None)

run(host='localhost', port=8080, debug=True, reloader=True)
