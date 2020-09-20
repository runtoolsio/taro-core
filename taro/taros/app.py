from bottle import route, run

from taro import client, dto


@route('/instances')
def instances():
    embedded = {"instances": [resource(dto.to_info_dto(i), links={"self": "/instances/" + i.instance_id})
                              for i in client.read_jobs_info()]}
    return resource({}, links={"self": "/instances"}, embedded=embedded)


def resource(props, *, links=None, embedded=None):
    res = {}
    if links:
        res["_links"] = links
    if embedded:
        res["_embedded"] = embedded
    res.update(props)
    return res


run(host='localhost', port=8080, debug=True, reloader=True)
