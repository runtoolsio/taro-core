from taro.execution import ExecutionError, ExecutionState
from taro.job import JobInstanceData


def job_instance(inst):
    if inst.exec_error:
        exec_error = {"message": inst.exec_error.message, "state": inst.exec_error.exec_state.name}
    else:
        exec_error = None

    return {"job_id": inst.job_id, "instance_id": inst.instance_id, "state": inst.state.name, "exec_error": exec_error}


def to_job_instance_data(as_dict):
    if as_dict['exec_error']:
        exec_error = ExecutionError(as_dict['exec_error']['message'], ExecutionState[as_dict['exec_error']['state']])
    else:
        exec_error = None

    return JobInstanceData(as_dict['job_id'], as_dict['instance_id'], ExecutionState[as_dict['state']], exec_error)
