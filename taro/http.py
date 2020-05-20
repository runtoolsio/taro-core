import json
import sys

import urllib3
import yaql as yaql
from yaql.language.exceptions import YaqlException

from taro import util


def run(trigger_url, trigger_body, monitor_url, is_running, status):
    http = urllib3.PoolManager()
    headers = {'Content-type': 'application/json'}
    resp = http.request('POST', trigger_url, headers=headers, body=trigger_body)
    resp_body = resp.data.decode("utf-8")
    print(resp_body)

    if resp.status >= 300:
        print(f'HTTP trigger non-2xx code: {resp.status}', file=sys.stderr)
        exit(1)

    if not monitor_url:
        print(f'Job completed with status code: {resp.status}')
        return

    resp_body_obj = util.wrap_namespace(json.loads(resp_body))
    res_monitor_url = monitor_url.format(resp_body=resp_body_obj)

    engine = yaql.factory.YaqlFactory().create()
    is_running_exp = engine(is_running or 'false')

    while True:
        mon_resp = http.request('GET', res_monitor_url)
        mon_resp_body = mon_resp.data.decode("utf-8")
        ctx = yaql.create_context()
        ctx['status'] = mon_resp.status
        ctx['resp_body'] = json.loads(mon_resp_body) if mon_resp.headers[
                                                            'Content-Type'] == 'application/json' else mon_resp_body
        if is_running_exp.evaluate(context=ctx):
            if status:
                try:
                    print(engine(status).evaluate(context=ctx))
                except YaqlException as e:
                    print(mon_resp_body)
                    print("Invalid status expression: " + str(e), file=sys.stderr)
            else:
                print(mon_resp_body)
        else:
            break
