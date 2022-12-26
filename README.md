# taro

Tool for monitoring and managing jobs and processes. Created for managing cron jobs scheduled on many remote instances
from [single client application](https://github.com/taro-suite/taroc).

## Similar or related projects
[Pueue](https://github.com/nukesor/pueue)  
[MProcs](https://github.com/pvolok/mprocs)  

## Main Features

### REST API

Taro jobs can be controlled by REST API provided by server component named `taros` (taro server) which is included
with `taro` installation.

### Client Application

CLI client [taroc](https://github.com/taro-suite/taroc) (taro client) communicates with multiple hosts in parallel using
SSH.

### Unified logging

Execution events are logged into a single log file.

### Access to stdout and stderr

Taro reads job output by default and makes it easily accessible.

### History

History of job execution is stored in SQLite database.

### Execution Events Notification

Execution events are listenable both locally and remotely.

### Plugins

Plugin infrastructure is provided for adding custom extensions.

## Terminology

### Job

Job is a definition of a unit of work or unit of execution. A job is identified by its job ID. A typical example of a
job is an entry in crontab.

### Job Instance

A concrete execution of a job.

*Job instance is often called simply a 'job' (when it doesn't cause any confusion)*

## Commands

### Execute command managed by taro

```commandline
taro exec command [args..]
```

### Job (process) status

You can list all running jobs (job instances) with `ps` command. This will display also more information about running
jobs like execution start timestamp, execution time, status and others.

```commandline
taro ps
```

### Release pending execution

A job can be executed in pending mode which suspends the execution before the job is actually started. In such case the
instance is waiting in the PENDING state to be released by `release` command. This mode is enabled by using `--pending`
option which requires one argument for a "latch" value. This value must be provided to the `release` command as an
argument. Any job waiting for the same latch is released.

```commandline
# Server
taro exec --pending latch1 echo finally released
# Client
taroc release latch1
```

### Stop instance

An instance can be stopped if it correctly handles the termination signal.

```commandline
taro stop {job-or-instance-id}
```

### Listen

A running job is primarily monitored by observing changes in its state. Each state change triggers an event which can be
monitored by 'listen' command.

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

TODO:

### Disable

Jobs can be disabled. When disabled job is executed it goes through these states: `None` -> `Created` -> `Disabled`. It
means that disabled job is not started, and it terminates in `disabled` state instead.

This feature is mainly useful for temporary disabling of scheduled jobs as an alternative for simply commenting out of
crontab entries. Doing it this way has advantage of jobs are stored in the history, processed by plugins, etc.

### List Disabled

Disabled jobs can be showed by this command. TODO:

```commandline
taro config list-disabled 
```

### Hostinfo

Hostinfo is a description of host where taro is installed. It consists of list of parameters where each parameter has
its name and value. This command will display hostinfo parameters:

```commandline
taro hostinfo
```

## Configuration File

### Lookup

1. Current working directory
2. `$XDG_CONFIG_HOME` (env variable reading to be implemented yet)
3. `/etc/taro` (considering change to /etc/xdg)

## Premature termination

### General

A process can be terminated either by stop or interrupt operations. When a process is stopped its termination (final)
state is set to STOPPED. When a process is interrupted its termination (final) state is set to INTERRUPTED which is a
failure state. Choosing one of these options a user can decided whether the termination is a failure or not.

### Exec command

A child process executed with `exec` command is stopped or interrupted by sending kill signal.
Received SIGTERM (normal kill) or SIGINT (Ctrl+C) signals trigger interrupt operation.
It is expected for terminated child process to stop executing otherwise this process will continue waiting for the child.
