from bottle import route, run, response

from taro import client, dto


@route('/instances')
def hello():
    return {
        "_links": {
            "self": "/instances"
        },
        "_embedded": {
            "instances": [dto.to_info_dto(i) for i in client.read_jobs_info()]
        }
    }


run(host='localhost', port=8080, debug=True, reloader=True)
