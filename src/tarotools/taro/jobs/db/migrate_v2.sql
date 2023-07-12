BEGIN TRANSACTION;

-- Create new table with the desired structure
CREATE TABLE history_new
(
    job_id text,
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
    parameters text
);

-- Copy data from old table to new table and set 'UNKNOWN' for terminal_state
INSERT INTO history_new
    (job_id, instance_id, created, ended, state_changes, terminal_state, tracking, result, error_output, warnings, error, user_params, parameters)
SELECT
    job_id, instance_id, created, ended, state_changed, 'UNKNOWN', tracking, result, error_output, warnings, error, user_params, parameters
FROM history;

-- Drop old table
DROP TABLE history;

-- Rename new table to old table
ALTER TABLE history_new RENAME TO history;

COMMIT;
