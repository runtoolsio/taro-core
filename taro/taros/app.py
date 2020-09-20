from bottle import route, run, HTTPError

from taro import client, dto


@route('/instances')
def instances():
    embedded = {"instances": [resource(dto.to_info_dto(i), links={"self": "/instances/" + i.instance_id})
                              for i in client.read_jobs_info()]}
    return resource({}, links={"self": "/instances"}, embedded=embedded)


@route('/instances/<inst>')
def instance(inst):
    jobs_info = client.read_jobs_info(instance=inst)
    if not jobs_info:
        http_error(404, "Instance not found")


def resource(props, *, links=None, embedded=None):
    res = {}
    if links:
        res["_links"] = links
    if embedded:
        res["_embedded"] = embedded
    res.update(props)
    return res


def http_error(status, message):
    raise HTTPError(status=404, body='{"message": "' + message + '"}')


run(host='localhost', port=8080, debug=True, reloader=True)
