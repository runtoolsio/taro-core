import datetime
import json
import logging
import sqlite3
from datetime import timezone
from typing import List

from taro import cfg, paths, JobInstanceID
from taro.jobs.execution import ExecutionState, ExecutionError, ExecutionLifecycle
from taro.jobs.job import JobInfo
from taro.jobs.persistence import SortCriteria

log = logging.getLogger(__name__)


def create_persistence():
    db_con = sqlite3.connect(cfg.persistence_database or str(paths.sqlite_db_path(True)))
    sqlite_ = SQLite(db_con)
    sqlite_.check_tables_exist()  # TODO execute only in taro.auto_init()
    return sqlite_


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
                         error text,
                         user_params text,
                         parameters text)
                         ''')
            c.execute('''CREATE INDEX job_id_index ON history (job_id)''')
            c.execute('''CREATE INDEX instance_id_index ON history (instance_id)''')
            c.execute('''CREATE INDEX finished_index ON history (finished)''')
            log.debug('event=[table_created] table=[history]')
            self._conn.commit()

    def read_jobs(self, *, ids, sort, asc, limit, last) -> List[JobInfo]:
        def sort_exp():
            if sort == SortCriteria.CREATED:
                return 'created'
            if sort == SortCriteria.FINISHED:
                return 'finished'
            if sort == SortCriteria.TIME:
                return "julianday(finished) - julianday(created)"
            raise ValueError(sort)

        statement = "SELECT * FROM history"

        if ids:
            conditions = ["job_id LIKE \"%{id}%\" OR instance_id = \"{id}\"".format(id=id_) for id_ in ids]
            statement += " WHERE ({conditions})".format(conditions=" OR ".join(conditions))
        if last:
            statement += " GROUP BY job_id HAVING ROWID = max(ROWID) "

        c = self._conn.execute(statement
                               + " ORDER BY " + sort_exp() + (" ASC" if asc else " DESC")
                               + " LIMIT ?",
                               (limit,))

        def to_job_info(t):
            state_changes = ((ExecutionState[state], datetime.datetime.fromtimestamp(changed, tz=timezone.utc))
                             for state, changed in json.loads(t[4]))
            lifecycle = ExecutionLifecycle(*state_changes)
            warnings = json.loads(t[6]) if t[6] else dict()
            exec_error = ExecutionError(t[7], lifecycle.state) if t[7] else None  # TODO more data
            user_params = json.loads(t[8]) if t[8] else dict()
            parameters = json.loads(t[9]) if t[9] else tuple()
            return JobInfo(JobInstanceID(t[0], t[1]), lifecycle, t[5], warnings, exec_error, parameters, **user_params)

        return [to_job_info(row) for row in c.fetchall()]

    def clean_up(self, max_records, max_age):
        if max_records >= 0:
            self._max_rows(max_records)
        if max_age:
            self._delete_old_jobs(max_age)

    def _max_rows(self, limit):
        c = self._conn.execute("SELECT COUNT(*) FROM history")
        count = c.fetchone()[0]
        if count > limit:
            self._conn.execute(
                "DELETE FROM history WHERE rowid not in (SELECT rowid FROM history ORDER BY finished DESC LIMIT (?))",
                (limit,))
            self._conn.commit()

    def _delete_old_jobs(self, max_age):
        self._conn.execute("DELETE FROM history WHERE finished < (?)",
                           ((datetime.datetime.now(tz=timezone.utc) - max_age),))
        self._conn.commit()

    def store_job(self, job_info):
        self._conn.execute(
            "INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (job_info.job_id,
             job_info.instance_id,
             job_info.lifecycle.changed(ExecutionState.CREATED),
             job_info.lifecycle.last_changed,
             json.dumps(
                 [(state.name, int(changed.timestamp())) for state, changed in job_info.lifecycle.state_changes]),
             job_info.status,
             json.dumps(job_info.warnings),
             job_info.exec_error.message if job_info.exec_error else None,
             json.dumps(job_info.user_params),
             json.dumps(job_info.parameters)
             )
        )
        self._conn.commit()

    def remove_job(self, id_):
        self._conn.execute("DELETE FROM history WHERE job_id = (?) or instance_id = (?)", (id_, id_,))
        self._conn.commit()

    def close(self):
        self._conn.close()
