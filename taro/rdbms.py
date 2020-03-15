import logging

from taro.execution import ExecutionState
from taro.job import ExecutionStateObserver

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

    def notify(self, job_instance):
        if job_instance.state.is_terminal():
            self._conn.execute(
                "INSERT INTO history VALUES (?, ?, ?, ?, ?, ?)",
                (job_instance.job_id,
                 job_instance.instance_id,
                 job_instance.state_changes[ExecutionState.CREATED],
                 job_instance.state_changes[job_instance.state],
                 ".".join([state.name for state in job_instance.state_changes.keys()]),
                 job_instance.exec_error.message if job_instance.exec_error else None
                 ))
