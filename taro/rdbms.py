import logging
from collections import OrderedDict

from taro import util
from taro.execution import ExecutionState, ExecutionError
from taro.job import ExecutionStateObserver, JobInstanceData

log = logging.getLogger(__name__)


def _to_job_instance(t):
    states = [ExecutionState[name] for name in t[5].split(",")]
    exec_state = next(state for state in states if state.is_executing())

    def dt_for_state(state):
        ts = None
        if state == ExecutionState.CREATED:
            ts = t[2]
        elif state == exec_state:
            ts = t[3]
        elif state.is_terminal():
            ts = t[4]
        return util.dt_from_utc_str(ts, is_iso=False) if ts else None

    state_changes = OrderedDict((state, dt_for_state(state)) for state in states)
    exec_error = ExecutionError(t[6], states[-1]) if t[6] else None  # TODO more data
    return JobInstanceData(t[0], t[1], state_changes, None, exec_error)


class Persistence(ExecutionStateObserver):

    def __init__(self, connection):
        self._conn = connection
        self._check_tables_exist()

    def _check_tables_exist(self):
        c = self._conn.cursor()
        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='history' ''')
        if c.fetchone()[0] != 1:
            c.execute('''CREATE TABLE history
                         (job_id text,
                         instance_id text,
                         created timestamp,
                         executed timestamp,
                         finished timestamp,
                         states text,
                         error text)
                         ''')
            log.debug('event=[table_created] table=[history]')
            self._conn.commit()

    def read_finished(self):
        c = self._conn.execute("SELECT * FROM history ORDER BY finished ASC")
        return [_to_job_instance(row) for row in c.fetchall()]

    def notify(self, job_instance):
        if job_instance.state.is_terminal():
            self._conn.execute(
                "INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?)",
                (job_instance.job_id,
                 job_instance.instance_id,
                 job_instance.state_changes[ExecutionState.CREATED],
                 next(changed for state, changed in job_instance.state_changes.items() if state.is_executing()),
                 job_instance.state_changes[job_instance.state],
                 ",".join([state.name for state in job_instance.state_changes.keys()]),
                 job_instance.exec_error.message if job_instance.exec_error else None
                 ))
            self._conn.commit()
