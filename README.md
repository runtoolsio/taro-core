# taro
Job management library

## Inter-process communication
https://docs.python.org/3/library/xmlrpc.html

## Feature priority
### Mandatory
1. ~~Logging - normal to stdout and errors to stderr (https://docs.python.org/3/library/logging.handlers.html)~~
2. ~~(Config)~~
3. Current executing jobs
4. Progress of currently executing jobs
5. SNS notifications
6. Execution history
7. Log rotation

### Optional
- job stopping
- job timeout
- job post checks

## Implementation notes

### Color Print
termcolor module
https://github.com/tartley/colorama
https://github.com/erikrose/blessings
https://github.com/timofurrer/colorful

### Table Print
https://stackoverflow.com/questions/9535954/printing-lists-as-tabular-data

### Misc

Logging config: https://stackoverflow.com/questions/14058453/making-python-loggers-output-all-messages-to-stdout-in-addition-to-log-file

STDOUT to file: https://stackoverflow.com/questions/4965159/how-to-redirect-output-with-subprocess-in-python

Reading subprocess stdout:
https://zaiste.net/realtime_output_from_shell_command_in_python/

https://stackoverflow.com/questions/13398261/how-to-run-a-subprocess-with-python-wait-for-it-to-exit-and-get-the-full-stdout
https://stackoverflow.com/questions/19961052/what-is-the-difference-if-i-dont-use-stdout-subprocess-pipe-in-subprocess-popen
https://stackoverflow.com/questions/803265/getting-realtime-output-using-subprocess

Log files location:
https://superuser.com/questions/1293842/where-should-userspecific-application-log-files-be-stored-in-gnu-linux?rq=1
https://stackoverflow.com/questions/25897836/where-should-i-write-a-user-specific-log-file-to-and-be-xdg-base-directory-comp

Bin location:
https://unix.stackexchange.com/questions/36871/where-should-a-local-executable-be-placed

Default ranger config:
/usr/lib/python3.7/site-packages/ranger/config/rc.conf

User specific site-packages:
/home/stan/.local/lib/python3.7/site-packages