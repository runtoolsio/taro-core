import sys
import urllib3
import json

from taro import util


def run(trigger_url, trigger_body, monitor_url):
    http = urllib3.PoolManager()
    headers = {'Content-type': 'application/json'}
    resp = http.request('POST', trigger_url, headers=headers, body=trigger_body)
    resp_body = resp.data.decode("utf-8")
    print(resp_body, file=sys.stderr)

    if resp.status >= 300:
        print(f'HTTP trigger non-2xx code: {resp.status}', file=sys.stderr)
        exit(1)

    if not monitor_url:
        print(f'Job completed with status code: {resp.status}')
        return

    resp_body_obj = util.wrap_namespace(json.loads(resp_body))
    res_monitor_url = monitor_url.format(resp_body=resp_body_obj)
    mon_resp = http.request('GET', res_monitor_url)
    print(res_monitor_url)
    print(mon_resp.status)


def _ctx():
    return {'UTC_NOW': util.utc_now()}
