import sys
import urllib3

from taro import util


def run(trigger_url, trigger_body):
    http = urllib3.PoolManager()
    headers = {'Content-type': 'application/json'}
    resp = http.request('POST', trigger_url, headers=headers, body=trigger_body)
    if resp.status >= 300:
        print(resp.data.decode("utf-8"), file=sys.stderr)
        print(f'HTTP trigger non-2xx code: {resp.status}', file=sys.stderr)
        exit(1)

    print(resp.read())


def _ctx():
    return {'UTC_NOW': util.utc_now()}
