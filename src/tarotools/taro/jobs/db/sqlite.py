"""
Persistence storage implementation using SQLite. See `tarotools.taro.persistence` module doc for much more details.
"""

import datetime
import json
import logging
import sqlite3
from datetime import timezone
from typing import List

from tarotools.taro import cfg
from tarotools.taro import paths
from tarotools.taro.jobs.execution import ExecutionState, ExecutionError, ExecutionLifecycle, ExecutionPhase, Flag, \
    Phase
from tarotools.taro.jobs.inst import (InstanceStateObserver, JobInst, JobInstances, JobInstanceID, LifecycleEvent,
                                      JobInstanceMetadata)
from tarotools.taro.jobs.job import JobStats
from tarotools.taro.jobs.persistence import SortCriteria
from tarotools.taro.jobs.track import TrackedTaskInfo
from tarotools.taro.util import MatchingStrategy, format_dt_sql, parse_dt_sql

log = logging.getLogger(__name__)


def create_persistence():
    db_con = sqlite3.connect(cfg.persistence_database or str(paths.sqlite_db_path(True)))
    sqlite_ = SQLite(db_con)
    sqlite_.check_tables_exist()
    return sqlite_


def _build_where_clause(instance_match, alias=''):
    # TODO Post fetch filter for criteria not supported in WHERE (instance parameters, etc.)
    if not instance_match:
        return ""

    if alias and not alias.endswith('.'):
        alias = alias + "."

    job_conditions = [f'{alias}job_id = "{j}"' for j in instance_match.job_ids]

    id_conditions = []
    for c in instance_match.id_criteria:
        if c.strategy == MatchingStrategy.ALWAYS_TRUE:
            id_conditions.clear()
            break
        if c.strategy == MatchingStrategy.ALWAYS_FALSE:
            id_conditions = ['1=0']
            break

        conditions = []
        op = ' AND ' if c.match_both_ids else ' OR '
        if c.job_id:
            if c.strategy == MatchingStrategy.PARTIAL:
                conditions.append(f'{alias}job_id GLOB "*{c.job_id}*"')
            elif c.strategy == MatchingStrategy.FN_MATCH:
                conditions.append(f'{alias}job_id GLOB "{c.job_id}"')
            elif c.strategy == MatchingStrategy.EXACT:
                conditions.append(f'{alias}job_id = "{c.job_id}"')
            else:
                raise ValueError(f"Matching strategy {c.strategy} is not supported")
        if c.instance_id:
            if c.strategy == MatchingStrategy.PARTIAL:
                conditions.append(f'{alias}instance_id GLOB "*{c.instance_id}*"')
            elif c.strategy == MatchingStrategy.FN_MATCH:
                conditions.append(f'{alias}instance_id GLOB "{c.instance_id}"')
            elif c.strategy == MatchingStrategy.EXACT:
                conditions.append(f'{alias}instance_id = "{c.instance_id}"')
            else:
                raise ValueError(f"Matching strategy {c.strategy} is not supported")

        id_conditions.append(op.join(conditions))

    int_criteria = instance_match.interval_criteria
    int_conditions = []
    for c in int_criteria:
        if c.event == LifecycleEvent.CREATED:
            e = f'{alias}created'
        elif c.event == LifecycleEvent.ENDED:
            e = f'{alias}ended'
        else:
            continue

        conditions = []
        if c.from_dt:
            conditions.append(f"{e} >= '{format_dt_sql(c.from_dt)}'")
        if c.to_dt:
            if c.include_to:
                conditions.append(f"{e} <= '{format_dt_sql(c.to_dt)}'")
            else:
                conditions.append(f"{e} < '{format_dt_sql(c.to_dt)}'")

        int_conditions.append("(" + " AND ".join(conditions) + ")")

    state_conditions = []
    if instance_match.state_criteria:
        if instance_match.phases and (Phase.TERMINAL not in instance_match.phases or len(instance_match.phases) > 1):
            raise ValueError("Phase matching was requested but it is not supported: " + str(instance_match.phases))
        if instance_match.state_criteria.warning:
            state_conditions.append(f"{alias}warnings IS NOT NULL")
        if flag_groups := instance_match.state_criteria.flag_groups:
            states = ",".join(f"'{s.name}'" for group in flag_groups
                              for s in ExecutionState.get_states_by_flags(*group))
            state_conditions.append(f"{alias}terminal_state IN ({states})")

    all_conditions_list = (job_conditions, id_conditions, int_conditions, state_conditions)
    all_conditions_str = ["(" + " OR ".join(c_list) + ")" for c_list in all_conditions_list if c_list]

    return " WHERE {conditions}".format(conditions=" AND ".join(all_conditions_str))


class SQLite(InstanceStateObserver):

    def __init__(self, connection):
        self._conn = connection

    def new_instance_state(self, job_inst: JobInst, previous_state, new_state, changed):
        if new_state.in_phase(Phase.TERMINAL):
            self.store_instances(job_inst)

    def check_tables_exist(self):
        # Version 4
        c = self._conn.cursor()
        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='history' ''')
        if c.fetchone()[0] != 1:
            c.execute('''CREATE TABLE history
                         (job_id text,
                         instance_id text,
                         created timestamp,
                         ended timestamp,
                         exec_time real,
                         state_changes text,
                         terminal_state text,
                         tracking text,
                         result text,
                         error_output text,
                         warnings text,
                         error text,
                         user_params text,
                         parameters text,
                         misc text)
                         ''')
            c.execute('''CREATE INDEX job_id_index ON history (job_id)''')
            c.execute('''CREATE INDEX instance_id_index ON history (instance_id)''')
            c.execute('''CREATE INDEX ended_index ON history (ended)''')  # TODO created + exec_time idx too
            log.debug('event=[table_created] table=[history]')
            self._conn.commit()

    def read_instances(self, instance_match=None, sort=SortCriteria.ENDED,
                       *, asc=True, limit=-1, offset=-1, last=False) \
            -> JobInstances:
        def sort_exp():
            if sort == SortCriteria.CREATED:
                return 'h.created'
            if sort == SortCriteria.ENDED:
                return 'h.ended'
            if sort == SortCriteria.TIME:
                return "julianday(h.ended) - julianday(h.created)"
            raise ValueError(sort)

        statement = "SELECT * FROM history h"
        statement += _build_where_clause(instance_match, alias='h')

        if last:
            statement += " GROUP BY h.job_id HAVING ROWID = max(ROWID) "

        statement += " ORDER BY " + sort_exp() + (" ASC" if asc else " DESC") + " LIMIT ? OFFSET ?"

        log.debug("event=[executing_query] statement=[%s]", statement)
        c = self._conn.execute(statement, (limit, offset))

        def to_job_info(t):
            state_changes = ((ExecutionState[state], datetime.datetime.fromtimestamp(changed, tz=timezone.utc))
                             for state, changed in json.loads(t[5]))
            lifecycle = ExecutionLifecycle(*state_changes)
            tracking = TrackedTaskInfo.from_dict(json.loads(t[7])) if t[7] else None
            status = t[8]
            error_output = json.loads(t[9]) if t[9] else tuple()
            warnings = json.loads(t[10]) if t[10] else dict()
            exec_error = ExecutionError.from_dict(json.loads(t[11])) if t[11] else None
            user_params = json.loads(t[12]) if t[12] else dict()
            parameters = tuple((tuple(x) for x in json.loads(t[13]))) if t[13] else tuple()
            pending_group = json.loads(t[14]).get("pending_group") if t[14] else None
            metadata = JobInstanceMetadata(JobInstanceID(t[0], t[1]), parameters, user_params, pending_group)

            return JobInst(metadata, lifecycle, tracking, status, error_output, warnings, exec_error)

        return JobInstances((to_job_info(row) for row in c.fetchall()))

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
                "DELETE FROM history WHERE rowid not in (SELECT rowid FROM history ORDER BY ended DESC LIMIT (?))",
                (limit,))
            self._conn.commit()

    def _delete_old_jobs(self, max_age):
        self._conn.execute("DELETE FROM history WHERE ended < (?)",
                           ((datetime.datetime.now(tz=timezone.utc) - max_age),))
        self._conn.commit()

    def read_stats(self, instance_match=None) -> List[JobStats]:
        where = _build_where_clause(instance_match, alias='h')
        failure_states = ",".join([f"'{s.name}'" for s in ExecutionState.get_states_by_flags(Flag.FAILURE)])
        sql = f'''
            SELECT
                h.job_id,
                count(h.job_id) AS "count",
                min(created) AS "first_created",
                max(created) AS "last_created",
                min(h.exec_time) AS "fastest_time",
                avg(h.exec_time) AS "average_time",
                max(h.exec_time) AS "slowest_time",
                last.exec_time AS "last_time",
                last.terminal_state AS "last_state",
                COUNT(CASE WHEN h.terminal_state IN ({failure_states}) THEN 1 ELSE NULL END) AS failed,
                COUNT(h.warnings) AS warnings
            FROM
                history h
            INNER JOIN
                (SELECT job_id, exec_time, terminal_state FROM history h {where} GROUP BY job_id HAVING ROWID = max(ROWID)) AS last
                ON h.job_id = last.job_id
            {where}
            GROUP BY
                h.job_id
        '''
        c = self._conn.execute(sql)

        def to_job_stats(t):
            job_id = t[0]
            count = t[1]
            first_at = parse_dt_sql(t[2])
            last_at = parse_dt_sql(t[3])
            fastest = datetime.timedelta(seconds=t[4]) if t[4] else None
            average = datetime.timedelta(seconds=t[5]) if t[5] else None
            slowest = datetime.timedelta(seconds=t[6]) if t[6] else None
            last_time = datetime.timedelta(seconds=t[7]) if t[7] else None
            last_state = ExecutionState[t[8]] if t[8] else ExecutionState.UNKNOWN
            failed_count = t[9]
            warn_count = t[10]

            return JobStats(
                job_id, count, first_at, last_at, fastest, average, slowest, last_time, last_state, failed_count,
                warn_count
            )

        return [to_job_stats(row) for row in c.fetchall()]

    def store_instances(self, *job_inst):
        def to_tuple(j):
            return (j.job_id,
                    j.instance_id,
                    format_dt_sql(j.lifecycle.created_at),
                    format_dt_sql(j.lifecycle.last_changed_at),
                    round(j.lifecycle.execution_time.total_seconds(), 3) if j.lifecycle.execution_time else None,
                    json.dumps(
                        [(state.name, float(changed.timestamp())) for state, changed in j.lifecycle.state_changes]),
                    j.lifecycle.state.name if j.lifecycle.state.in_phase(
                        ExecutionPhase.TERMINAL) else ExecutionState.UNKNOWN.name,
                    json.dumps(j.tracking.to_dict(include_empty=False)) if j.tracking else None,
                    j.status,
                    json.dumps(j.error_output) if j.error_output else None,
                    json.dumps(j.warnings) if j.warnings else None,
                    json.dumps(j.exec_error.to_dict(include_empty=False)) if j.exec_error else None,
                    json.dumps(j.metadata.user_params) if j.metadata.user_params else None,
                    json.dumps(j.metadata.parameters) if j.metadata.parameters else None,
                    json.dumps({"pending_group": j.metadata.pending_group}) if j.metadata.pending_group else None
                    )

        jobs = [to_tuple(j) for j in job_inst]
        self._conn.executemany(
            "INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            jobs
        )
        self._conn.commit()

    def remove_instances(self, instance_match):
        where_clause = _build_where_clause(instance_match)
        if not where_clause:
            raise ValueError("No rows to remove")
        self._conn.execute("DELETE FROM history" + where_clause)
        self._conn.commit()

    def close(self):
        self._conn.close()
