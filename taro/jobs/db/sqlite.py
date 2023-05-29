import datetime
import json
import logging
import sqlite3
from datetime import timezone

from taro import cfg, paths, JobInstanceID
from taro.jobs.execution import ExecutionState, ExecutionError, ExecutionLifecycle, ExecutionPhase
from taro.jobs.job import JobInfo, JobInfoList, LifecycleEvent, JobInstanceMetadata
from taro.jobs.persistence import SortCriteria
from taro.jobs.track import TrackedTaskInfo
from taro.util import MatchingStrategy, format_dt_sql

log = logging.getLogger(__name__)


def create_persistence():
    db_con = sqlite3.connect(cfg.persistence_database or str(paths.sqlite_db_path(True)))
    sqlite_ = SQLite(db_con)
    sqlite_.check_tables_exist()  # TODO execute only setup?
    return sqlite_


def _build_where_clause(instance_match):
    if not instance_match:
        return ""

    id_criteria = instance_match.id_criteria
    id_conditions = []
    for c in id_criteria:
        if c.strategy == MatchingStrategy.ALWAYS_TRUE:
            id_conditions.clear()
            break

        conditions = []
        op = ' AND ' if c.match_both_ids else ' OR '
        if c.job_id:
            if c.strategy == MatchingStrategy.PARTIAL:
                conditions.append(f'job_id GLOB "*{c.job_id}*"')
            elif c.strategy == MatchingStrategy.FN_MATCH:
                conditions.append(f'job_id GLOB "{c.job_id}"')
            elif c.strategy == MatchingStrategy.EXACT:
                conditions.append(f'job_id = "{c.job_id}"')
            else:
                raise ValueError(f"Matching strategy {id_criteria.strategy} is not supported")
        if c.instance_id:
            if c.strategy == MatchingStrategy.PARTIAL:
                conditions.append(f'instance_id GLOB "*{c.instance_id}*"')
            elif c.strategy == MatchingStrategy.FN_MATCH:
                conditions.append(f'instance_id GLOB "{c.instance_id}"')
            elif c.strategy == MatchingStrategy.EXACT:
                conditions.append(f'instance_id = "{c.instance_id}"')
            else:
                raise ValueError(f"Matching strategy {id_criteria.strategy} is not supported")

        id_conditions.append(op.join(conditions))

    int_criteria = instance_match.interval_criteria
    int_conditions = []
    for c in int_criteria:
        if c.event == LifecycleEvent.CREATED:
            e = 'created'
        elif c.event == LifecycleEvent.ENDED:
            e = 'ended'
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
        if instance_match.state_criteria.warning:
            state_conditions.append("warnings IS NOT NULL")
        if flag_groups := instance_match.state_criteria.flag_groups:
            states = ",".join(f"'{s.name}'" for group in flag_groups
                                            for s in ExecutionState.get_states_by_flags(*group))
            state_conditions.append(f"terminal_state IN ({states})")

    all_conditions_list = (id_conditions, int_conditions, state_conditions)
    all_conditions_str = ["(" + " OR ".join(c_list) + ")" for c_list in all_conditions_list if c_list]

    return " WHERE {conditions}".format(conditions=" AND ".join(all_conditions_str))


class SQLite:

    def __init__(self, connection):
        self._conn = connection

    def check_tables_exist(self):
        # Version 2
        c = self._conn.cursor()
        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='history' ''')
        if c.fetchone()[0] != 1:
            c.execute('''CREATE TABLE history
                         (job_id text,
                         instance_id text,
                         created timestamp,
                         ended timestamp,
                         state_changes text,
                         terminal_state text,
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
            c.execute('''CREATE INDEX ended_index ON history (ended)''')  # TODO created idx too?
            log.debug('event=[table_created] table=[history]')
            self._conn.commit()

    def read_jobs(self, instance_match=None, sort=SortCriteria.ENDED, *, asc=True, limit=-1, last=False) \
            -> JobInfoList:
        def sort_exp():
            if sort == SortCriteria.CREATED:
                return 'created'
            if sort == SortCriteria.ENDED:
                return 'ended'
            if sort == SortCriteria.TIME:
                return "julianday(ended) - julianday(created)"
            raise ValueError(sort)

        statement = "SELECT * FROM history"
        statement += _build_where_clause(instance_match)

        if last:
            statement += " GROUP BY job_id HAVING ROWID = max(ROWID) "

        print(statement)

        c = self._conn.execute(statement
                               + " ORDER BY " + sort_exp() + (" ASC" if asc else " DESC")
                               + " LIMIT ?",
                               (limit,))

        def to_job_info(t):
            state_changes = ((ExecutionState[state], datetime.datetime.fromtimestamp(changed, tz=timezone.utc))
                             for state, changed in json.loads(t[4]))
            lifecycle = ExecutionLifecycle(*state_changes)
            tracking = TrackedTaskInfo.from_dict(json.loads(t[6])) if t[6] else None
            error_output = json.loads(t[8]) if t[8] else tuple()
            warnings = json.loads(t[9]) if t[9] else dict()
            exec_error = ExecutionError.from_dict(json.loads(t[10])) if t[10] else None
            user_params = json.loads(t[11]) if t[11] else dict()
            parameters = tuple((tuple(x) for x in json.loads(t[12]))) if t[12] else tuple()
            metadata = JobInstanceMetadata(JobInstanceID(t[0], t[1]), parameters, user_params, None) # TODO
            return JobInfo(metadata, lifecycle, tracking, t[7], error_output, warnings, exec_error)

        return JobInfoList((to_job_info(row) for row in c.fetchall()))

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

    def store_job(self, *job_info):
        def to_tuple(j):
            return (j.job_id,
                    j.instance_id,
                    format_dt_sql(j.lifecycle.created_at),
                    format_dt_sql(j.lifecycle.last_changed_at),
                    json.dumps(
                        [(state.name, float(changed.timestamp())) for state, changed in j.lifecycle.state_changes]),
                    j.lifecycle.state.name if j.lifecycle.state.in_phase(ExecutionPhase.TERMINAL) else ExecutionState.UNKNOWN.name,
                    json.dumps(j.tracking.to_dict(include_empty=False)) if j.tracking else None,
                    j.status,
                    json.dumps(j.error_output) if j.error_output else None,
                    json.dumps(j.warnings) if j.warnings else None,
                    json.dumps(j.exec_error.to_dict(include_empty=False)) if j.exec_error else None,
                    json.dumps(j.metadata.user_params) if j.metadata.user_params else None,
                    json.dumps(j.metadata.parameters) if j.metadata.parameters else None,
                    )

        jobs = [to_tuple(j) for j in job_info]
        self._conn.executemany(
            "INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            jobs
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
