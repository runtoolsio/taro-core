from taro import ExecutionState
from test.taro_test_util import run_app_as_process_and_wait, run_app_as_process, run_app, StateWaiter


def test_serial():
    run_app_as_process_and_wait('exec -mc --id j1 --pending p echo me first',
                                wait_for=ExecutionState.PENDING, daemon=True)
    waiter = StateWaiter()
    run_app_as_process('exec -mc --id j1 --serial echo waiting is over', state_queue=waiter.state_queue)
    waiter.wait_for_state(ExecutionState.QUEUED, timeout=1)
    run_app('release --pending p')
    waiter.wait_for_state(ExecutionState.COMPLETED, timeout=1)
