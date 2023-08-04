BEGIN TRANSACTION;

CREATE TABLE temp AS SELECT
    job_id,
    instance_id,
    CASE
        WHEN created LIKE '%+00:00' THEN SUBSTR(created, 1, LENGTH(created) - 6)
        ELSE created
    END AS created,
    CASE
        WHEN ended LIKE '%+00:00' THEN SUBSTR(ended, 1, LENGTH(ended) - 6)
        ELSE ended
    END AS ended,
    exec_time,
    state_changes,
    terminal_state,
    tracking,
    result,
    error_output,
    warnings,
    error,
    user_params,
    parameters,
    misc
FROM history;

DROP TABLE history;

ALTER TABLE temp RENAME TO history;

COMMIT;
