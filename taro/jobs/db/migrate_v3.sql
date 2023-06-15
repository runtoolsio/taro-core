BEGIN TRANSACTION;

ALTER TABLE history ADD COLUMN misc TEXT;

ALTER TABLE history RENAME TO old_history;

CREATE TABLE history
(
  job_id text,
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
  misc text
);

INSERT INTO history
(
  job_id,
  instance_id,
  created,
  ended,
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
)
SELECT
  job_id,
  instance_id,
  created,
  ended,
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
FROM old_history;

DROP TABLE old_history;

COMMIT;
