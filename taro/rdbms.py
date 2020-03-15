import logging

from taro.execution import ExecutionState, ExecutionError
from taro.job import ExecutionStateObserver, JobInstanceData

log = logging.getLogger(__name__)


class Persistence(ExecutionStateObserver):

    def __init__(self, connection):
        self._conn = connection
        self._check_tables_exist()

    def _check_tables_exist(self):
        c = self._conn.cursor()
        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='history' ''')
        if c.fetchone()[0] != 1:
            c.execute('''CREATE TABLE history
                         (job_id text, instance_id text, created timestamp, finished timestamp, states text, error text)
                         ''')
            log.debug('event=[table_created] table=[history]')
            self._conn.commit()

    def read_finished(self):
        c = self._conn.execute("SELECT * FROM history ORDER BY finished DESC")
        return [self._to_job_instance(row) for row in c.fetchall()]

    def notify(self, job_instance):
        if job_instance.state.is_terminal():
            self._conn.execute(
                "INSERT INTO history VALUES (?, ?, ?, ?, ?, ?)",
                (job_instance.job_id,
                 job_instance.instance_id,
                 job_instance.state_changes[ExecutionState.CREATED],
                 job_instance.state_changes[job_instance.state],
                 ",".join([state.name for state in job_instance.state_changes.keys()]),
                 job_instance.exec_error.message if job_instance.exec_error else None
                 ))
            self._conn.commit()

    def _to_job_instance(self, t):
        state_names = t[4].split(",")

        def dt_for_state(i):
            if i == 0:
                return t[2]
            elif i == len(state_names) - 1:
                return t[3]
            else:
                return None

        state_changes = {ExecutionState[name]: dt_for_state(i) for i, name in enumerate(state_names)}
        exec_error = ExecutionError(t[5], ExecutionState[state_names[-1]]) if t[5] else None
        return JobInstanceData(t[0], t[1], state_changes, None, exec_error)
