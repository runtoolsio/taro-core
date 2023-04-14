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
from taro.jobs.track import TrackedTaskInfo
from taro.util import MatchingStrategy

log = logging.getLogger(__name__)


def create_persistence():
    db_con = sqlite3.connect(cfg.persistence_database or str(paths.sqlite_db_path(True)))
    sqlite_ = SQLite(db_con)
    sqlite_.check_tables_exist()  # TODO execute only setup?
    return sqlite_


def _build_where_clause(instance_match):
    if not instance_match or not instance_match.id_matching_criteria:
        return ""

    criteria = instance_match.id_matching_criteria
    conditions = []
    for c in criteria:
        op = 'AND' if c.match_both_ids else 'OR'
        if c.strategy == MatchingStrategy.PARTIAL:
            conditions.append("job_id GLOB \"*{jid}*\" {op} instance_id GLOB \"*{iid}*\""
                              .format(jid=c.job_id, iid=c.instance_id, op=op))
        elif c.strategy == MatchingStrategy.FN_MATCH:
            conditions.append("job_id GLOB \"{jid}\" {op} instance_id GLOB \"{iid}\""
                              .format(jid=c.job_id, iid=c.instance_id, op=op))
        elif c.strategy == MatchingStrategy.EXACT:
            if not c.instance_id:
                conditions.append(f"job_id = \"{c.job_id}\"")  # TODO proper impl
            else:
                conditions.append("job_id = \"{jid}\" {op} instance_id = \"{iid}\""
                                  .format(jid=c.job_id, iid=c.instance_id, op=op))
        else:
            raise ValueError(f"Matching strategy {criteria.strategy} is not supported")

    return " WHERE ({conditions})".format(conditions=" OR ".join(conditions))


class SQLite:

    def __init__(self, connection):
        self._conn = connection

    def check_tables_exist(self):
        # Old versions:
        # `ALTER TABLE history RENAME COLUMN parameters TO user_params;`
        # `ALTER TABLE history ADD COLUMN parameters text;`
        c = self._conn.cursor()
        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='history' ''')
        if c.fetchone()[0] != 1:
            c.execute('''CREATE TABLE history
                         (job_id text,
                         instance_id text,
                         created timestamp,
                         finished timestamp,
                         state_changed text,
                         tracking text,
                         result text,
                         error_output text,
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

    def read_jobs(self, instance_match=None, sort=SortCriteria.CREATED, *, asc=True, limit=-1, last=False)\
            -> List[JobInfo]:
        def sort_exp():
            if sort == SortCriteria.CREATED:
                return 'created'
            if sort == SortCriteria.FINISHED:
                return 'finished'
            if sort == SortCriteria.TIME:
                return "julianday(finished) - julianday(created)"
            raise ValueError(sort)

        statement = "SELECT * FROM history"
        statement += _build_where_clause(instance_match)

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
            tracking = TrackedTaskInfo.from_dict(json.loads(t[5])) if t[5] else None
            error_output = json.loads(t[7]) if t[7] else tuple()
            warnings = json.loads(t[8]) if t[8] else dict()
            exec_error = ExecutionError.from_dict(json.loads(t[9])) if t[9] else None
            user_params = json.loads(t[10]) if t[10] else dict()
            parameters = tuple((tuple(x) for x in json.loads(t[11]))) if t[11] else tuple()
            return JobInfo(JobInstanceID(t[0], t[1]), lifecycle, tracking, t[6], error_output, warnings, exec_error,
                           parameters, **user_params)

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
            "INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (job_info.job_id,
             job_info.instance_id,
             job_info.lifecycle.changed(ExecutionState.CREATED),
             job_info.lifecycle.last_changed,
             json.dumps(
                 [(state.name, float(changed.timestamp())) for state, changed in job_info.lifecycle.state_changes]),
             json.dumps(job_info.tracking.to_dict(include_empty=False)) if job_info.tracking else None,
             job_info.status,
             json.dumps(job_info.error_output) if job_info.error_output else None,
             json.dumps(job_info.warnings) if job_info.warnings else None,
             json.dumps(job_info.exec_error.to_dict(include_empty=False)) if job_info.exec_error else None,
             json.dumps(job_info.user_params) if job_info.user_params else None,
             json.dumps(job_info.parameters) if job_info.parameters else None
             )
        )
        self._conn.commit()

    def remove_jobs(self, instance_match):
        where_clause = _build_where_clause(instance_match)
        if not where_clause:
            raise ValueError("No rows to remove")
        self._conn.execute("DELETE FROM history" + where_clause)
        self._conn.commit()

    def close(self):
        self._conn.close()
