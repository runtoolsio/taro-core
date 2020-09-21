from bottle import route, run, HTTPError

from taro import client, dto


@route('/instances')
def instances():
    embedded = {"instances": [resource_job_info(i) for i in client.read_jobs_info()]}
    return resource({}, links={"self": "/instances"}, embedded=embedded)


@route('/instances/<inst>')
def instance(inst):
    jobs_info = client.read_jobs_info(instance=inst)
    if not jobs_info:
        http_error(404, "Instance not found")
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
    raise HTTPError(status=404, body='{"message": "' + message + '"}')


run(host='localhost', port=8080, debug=True, reloader=True)
