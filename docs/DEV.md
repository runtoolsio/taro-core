# Development Notes

## Termination / Signals
### SIGTERM handler
Python does not register a handler for the SIGTERM signal. That means that the system will take the default action.
On linux, the default action for a SIGTERM is to terminate the process:
 - the process will simply not be allocated any more time slices during which it can execute code
 - the process's memory and other resources (open files, network sockets, etc...) will be released back to the rest of the system

### sys.exit()
Signal handler suspends current execution in the main thread and executes the handler code. When an exception is raised from the handler
the exception is propagated back in the main thread stack. An observed traceback:
```
...
  File "/usr/lib/python3.8/subprocess.py", line 1804, in _wait
    (pid, sts) = self._try_wait(0)
  File "/usr/lib/python3.8/subprocess.py", line 1762, in _try_wait
    (pid, sts) = os.waitpid(self.pid, wait_flags)
  File "/home/stan/Projects/taro-suite/taro/taro/term.py", line 15, in terminate << handler code
    sys.exit()
SystemExit
```

## Colour Print
### VT100 Colours
https://github.com/prompt-toolkit/python-prompt-toolkit/blob/master/prompt_toolkit/output/vt100.py

## Resources
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