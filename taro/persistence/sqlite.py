import logging
from typing import List

from taro import util
from taro.execution import ExecutionState, ExecutionError, ExecutionLifecycle
from taro.job import JobInfo, DisabledJob, Warn

log = logging.getLogger(__name__)


def _to_job_info(t):
    states = [ExecutionState[name] for name in t[5].split(",")]
    exec_state = next((state for state in states if state.is_executing()), None)

    def dt_for_state(state):
        ts = None
        if state == ExecutionState.CREATED:
            ts = t[2]
        elif state == exec_state:
            ts = t[3]
        elif state.is_terminal():
            ts = t[4]
        return util.dt_from_utc_str(ts, is_iso=False) if ts else None

    lifecycle = ExecutionLifecycle(*((state, dt_for_state(state)) for state in states))
    warnings = [Warn(w_id, {}) for w_id in t[7].split(',') if w_id]  # Escape ','
    exec_error = ExecutionError(t[7], states[-1]) if t[8] else None  # TODO more data
    return JobInfo(t[0], t[1], lifecycle, t[6], warnings, exec_error)


# TODO indices
class SQLite:

    def __init__(self, connection):
        self._conn = connection

    def check_tables_exist(self):
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
                         result text,
                         warnings text,
                         error text)
                         ''')
            log.debug('event=[table_created] table=[history]')
            self._conn.commit()

        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='disabled_jobs' ''')
        if c.fetchone()[0] != 1:
            c.execute('''CREATE TABLE disabled_jobs
                        (job_id text,
                        regex integer,
                        created timestamp,
                        expires timestamp)
                        ''')
            log.debug('event=[table_created] table=[disabled_jobs]')
            self._conn.commit()

    def read_jobs(self, *, chronological) -> List[JobInfo]:
        c = self._conn.execute("SELECT * FROM history ORDER BY finished " + ("ASC" if chronological else "DESC"))
        return [_to_job_info(row) for row in c.fetchall()]

    def store_job(self, job_info):
        self._conn.execute(
            "INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (job_info.job_id,
             job_info.instance_id,
             job_info.lifecycle.changed(ExecutionState.CREATED),
             job_info.lifecycle.execution_started(),
             job_info.lifecycle.last_changed(),
             ",".join([state.name for state in job_info.lifecycle.states()]),
             job_info.status,
             ",".join((w.id for w in job_info.warnings)),
             job_info.exec_error.message if job_info.exec_error else None,
             )
        )
        self._conn.commit()

    def add_disabled_jobs(self, disabled_jobs):
        added = []
        for j in disabled_jobs:
            res = self._conn.execute(
                "INSERT INTO disabled_jobs SELECT ?, ?, ?, ? WHERE NOT EXISTS(SELECT 1 FROM disabled_jobs WHERE job_id = ?)",
                (j.job_id, j.regex, j.created, j.expires, j.job_id))
            if res.rowcount:
                added.append(j)
        self._conn.commit()
        return added

    def remove_disabled_jobs(self, job_ids):
        for job_id in job_ids:
            c = self._conn.execute("DELETE FROM disabled_jobs WHERE job_id = (?)", (job_id,))
            print(c)
            print(type(c))

    def read_disabled_jobs(self):
        c = self._conn.execute("SELECT * FROM disabled_jobs ")
        return [DisabledJob(row[0],
                            row[1],
                            util.dt_from_utc_str(row[2], is_iso=False),
                            util.dt_from_utc_str(row[3], is_iso=False))
                for row in c.fetchall()]

    def close(self):
        self._conn.close()
