## Terminology

### Resources
https://www.kolpackov.net/projects/c++/utility/Utility-1.2.2/Documentation/CommandLine/Terminology.xhtml
https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap12.html#tag_12_01

### CLI Components

#### Arguments
Each string in a command line array is referred to as `argument`.
First argument usually contains a string that refers to an executable.

#### Command
Command is usually a word, or a single letter that represents a command to the program logic.
Other terms for command include action and function.

> Even though '--help' is usually considered to be an option semantically it is a command.

#### Options
Option consists of option name and optionally one or more option values (option-arguments). Options are usually optional.
Non-optional options are usually better represented by commands or operands. 
Option without a value is always optional and represents an option with implied binary value (e.g. {0, 1} or {false, true} etc.).
Such option is sometimes called flag.

##### Types
Option can be associated with a program or a command.
Thus the concept of option can be further refined to `program option` and `command option`.
Program option alters behavior of the program as a whole while command option is only affecting particular command.
Options are usually listed in alphabetical order unless this wOptions are usually listed in alphabetical order unless this would make the utility description more confusing.ould make the utility description more confusing.


#### Operands
Operand usually represents an input value or a parameter. Operands can be mandatory or optional.

## Examples
### Git
```
git [--version] [--help] [-C <path>] [-c <name>=<value>]
           [--exec-path[=<path>]] [--html-path] [--man-path] [--info-path]
           [-p | --paginate | -P | --no-pager] [--no-replace-objects] [--bare]
           [--git-dir=<path>] [--work-tree=<path>] [--namespace=<name>]
           <command> [<args>]
```
