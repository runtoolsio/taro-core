# taro
Tool for monitoring and managing jobs and processes.
Created for managing cron jobs scheduled on many remote instances from a [single client application](https://github.com/taro-suite/taroc).

## Terminology
### Job
Job is a definition of a unit of work or unit of execution. A job is identified by its job ID.
A typical example of a job is an entry in crontab.

### Job Instance
A concrete execution of a job.

*Job instance is often called simply a 'job' (when it doesn't cause any confusion)*

## Main Features
### Single point monitoring
#### Unified logging for all jobs
Execution events of all jobs are logged into a single log file.


## Commands
### Execute command managed by taro
```commandline
taro exec {command} [args..]
```

### Release pending execution
A job can be executed in pending mode which suspends the execution before the job is actually started.
In such case the instance is waiting in the PENDING state to be released by `release` command.
This mode is enabled by using `--pending` option which requires one argument for a "latch" value.
This value must be provided to the `release` command as an argument. Any job waiting for the same latch is released.
```commandline
taro exec --pending latch1 echo finally released
sleep 5
taro release latch1
```

### Job (process) status
You can list all running jobs (job instances) with `ps` command. This will display also more information about running jobs
like execution start timestamp, execution time, status and others.
```commandline
taro ps
```

### Stop instance
An instance can be stopped if it correctly handles the termination signal.
```commandline
taro stop {job-or-instance-id}
```

### Listen
A running job is primarily monitored by observing changes in its state. Each state change triggers an event which can be monitored
by 'listen' command.

*Note: Transition from `NONE` to `CREATED` state is not currently visible by this command. This may change in the future.*
```commandline
taro listen
```

### Wait
This command will wait for a state transition and then terminates. This can be used to execute an action when a job reaches certain state. 

*Note: Transition from `NONE` to `CREATED` state is not currently visible by this command. This may change in the future.*
```commandline
taro wait completed && echo -ne '\007' # beep on completion
```

### Tail
Unless output is bypassed by `-b` argument the last lines of job output can be displayed by `tail` command.
```commandline
taro tail
``` 
The output can be followed by using `-f` option.
```commandline
taro tail -f
```

### History
If persistence is enabled then information about every job execution is stored when the execution is finished.
The history of all job execution can be displayed by `history` command (or its alias `hist`).
```commandline
taro hist
```

### Config
#### Show
Shows path and content of the config file:
```commandline
taro config show
```

#### Path
Shows path to the config file. This is handy in cases where path to the config is used as an argument:
```commandline
vim `taro config path`
```

### Disable
Jobs can be disabled. When disabled job is executed it goes through these states: `None` -> `Created` -> `Disabled`.
It means that disabled job is not started and it terminates in `disabled` state instead.

This feature is mainly useful for temporary disabling of scheduled jobs as an alternative for simply commenting out of crontab entries.
Doing it this way has advantage of jobs stored in the history, processed by plugins, etc.
This in general helps to make disabling more visible and harder to forget re-enabling.

*Note: To use this feature persistence must be enabled in the config file.*
```commandline
taro disable job-to-disable1 job-to-disable2
```
A group of jobs can be disabled using regular expression with `-regex` option.

### List Disabled
Disabled jobs can be showed by this command.
```commandline
taro list-disabled
```

### Enable
Disabled jobs can be re-enabled by this command.
```commandline
taro enable disabled-job1 disabled-job2
```

### Hostinfo
Hostinfo is a description of host where taro is installed. It consists of list of parameters where each parameter has its name and value.
This command will display hostinfo parameters:
```commandline
taro hostinfo
```

## Premature termination
### General
A process can be terminated either by stop or interrupt operations.
When a process is stopped its termination (final) state is set to STOPPED.
When a process is interrupted its termination (final) state is set to INTERRUPTED which is a failure state.
Choosing one of these options a user can decided whether the termination is a failure or not.

### Exec command
A child process executed with `exec` command is stopped or interrupted by sending kill signal.
Received SIGTERM (normal kill) or SIGINT (Ctrl+C) signals trigger interrupt operation.
It is expected for terminated child process to stop executing otherwise this process will continue waiting for the child.
