import datetime
import json
import logging
import sqlite3
from datetime import timezone
from typing import List

from taro import util, cfg, paths
from taro.execution import ExecutionState, ExecutionError, ExecutionLifecycle
from taro.job import JobInfo, DisabledJob
from taro.persistence import SortCriteria

log = logging.getLogger(__name__)


def create_persistence():
    db_con = sqlite3.connect(cfg.persistence_database or str(paths.sqlite_db_path(True)))
    sqlite_ = SQLite(db_con)
    sqlite_.check_tables_exist()
    return sqlite_


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
                         finished timestamp,
                         state_changed text,
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

    def read_jobs(self, *, sort, asc, limit) -> List[JobInfo]:
        def sort_exp():
            if sort == SortCriteria.CREATED:
                return 'created'
            if sort == SortCriteria.FINISHED:
                return 'finished'
            if sort == SortCriteria.TIME:
                return "julianday(finished) - julianday(created)"
            raise ValueError(sort)

        c = self._conn.execute("SELECT * FROM history "
                               + "ORDER BY " + sort_exp() + (" ASC" if asc else " DESC")
                               + " LIMIT ?",
                               (limit,))

        def to_job_info(t):
            state_changes = ((ExecutionState[state], datetime.datetime.fromtimestamp(changed, tz=timezone.utc))
                             for state, changed in json.loads(t[4]))
            lifecycle = ExecutionLifecycle(*state_changes)
            warnings = json.loads(t[6]) if t[6] else dict()
            exec_error = ExecutionError(t[7], lifecycle.state()) if t[7] else None  # TODO more data
            return JobInfo(t[0], t[1], lifecycle, t[5], warnings, exec_error)

        return [to_job_info(row) for row in c.fetchall()]

    def store_job(self, job_info):
        self._conn.execute(
            "INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (job_info.job_id,
             job_info.instance_id,
             job_info.lifecycle.changed(ExecutionState.CREATED),
             job_info.lifecycle.last_changed(),
             json.dumps(
                 [(state.name, int(changed.timestamp())) for state, changed in job_info.lifecycle.state_changes()]),
             job_info.status,
             json.dumps(job_info.warnings),
             job_info.exec_error.message if job_info.exec_error else None,
             )
        )
        self._conn.commit()

    def add_disabled_jobs(self, disabled_jobs):
        added = []
        for j in disabled_jobs:
            c = self._conn.execute(
                "INSERT INTO disabled_jobs SELECT ?, ?, ?, ? "
                "WHERE NOT EXISTS(SELECT 1 FROM disabled_jobs WHERE job_id = ?)",
                (j.job_id, j.regex, j.created, j.expires, j.job_id))
            if c.rowcount:
                added.append(j)
        self._conn.commit()
        return added

    def remove_disabled_jobs(self, job_ids):
        removed = []
        for job_id in job_ids:
            c = self._conn.execute("DELETE FROM disabled_jobs WHERE job_id = (?)", (job_id,))
            if c.rowcount:
                removed.append(job_id)
        self._conn.commit()
        return removed

    def read_disabled_jobs(self):
        c = self._conn.execute("SELECT * FROM disabled_jobs ")
        return [DisabledJob(row[0],
                            row[1],
                            util.dt_from_utc_str(row[2], is_iso=False),
                            util.dt_from_utc_str(row[3], is_iso=False))
                for row in c.fetchall()]

    def close(self):
        self._conn.close()
