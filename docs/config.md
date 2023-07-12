# Configuration

The configuration consists of lower case attributes in the [cfg module](taro/cfg.py).
This is a recommended pattern for sharing configuration across modules described in [official python documentation](https://docs.python.org/3/faq/programming.html#how-do-i-share-global-variables-across-modules).
The upper case constants of the module contain initial values for the configuration attributes and represent minimal configuration.

## Current configuration
Normally when a taro command is executed (except the `config` command) the configuration file located in corresponding XDG directory is loaded
and its content overrides attributes of the [cfg module](taro/cfg.py).
The content of the current config file can be printed by this command:

`taro config show`

## Default configuration
Default configuration is located in the default config file. This file is copied to corresponding XDG directory when taro is first initialized after the installation.
A user can then edit the file to change the [current configuration](#current-configuration). The command line `-dc` option can be specified to use the original default configuration.
The content of the default config file can be printed by this command:

`taro config show -dc`

## Minimal configuration
This configuration is used when `-mc` command line option is specified. In such case no configuration file is loaded to override attributes of the [cfg module](taro/cfg.py).
Therefore, the values of the configuration attributes remains same as they were initialized from the constants in the module.
Such configuration is very defensive and disables most configurable features.

### Usage
This configuration is used mainly during testing and development.

## Setting config attributes manually
Each configuration attribute can be set manually using `--set` command line option. In such case a value loaded from the config file for this attribute (if any) is ignored.

### Examples
#### Disable persistence for an execution
`taro exec --set persistance_enabled=False ./will_not_be_stored_in_db.sh`

#### Disable logging
`taro exec --set log_mode=disabled echo There is no logging now`

## Configuration Attributes
| Attribute               | Config File             | Default Value | Minimal Value | Values                                  | Note                                                                                             |
|-------------------------|-------------------------|---------------|---------------|-----------------------------------------|--------------------------------------------------------------------------------------------------|
 | log_mode                | log.mode                | enabled       | disabled      | enabled, disabled, propagate            ||
 | log_stdout_level        | log.stdout.level        | warn          | off           | off, debug, info, warn, error, critical ||
 | log_file_level          | log.file.level          | info          | off           | off, debug, info, warn, error, critical ||
 | log_file_path           | log.file.path           | {none}        | {none}        | Full path for the log file              | When none is set the directory is resolved according to XDG spec and the file name is `taro.log` |
 | persistence_enabled     | persistence.enabled     | true          | false         | Boolean values (1, 0, on, off, etc.)    ||
 | persistence_type        | persistence.type        | sqlite        | sqlite        | sqlite                                  | Only SQLite is supported for now                                                                 |
 | persistence_max_age     | persistence.max_age     | {none}        | {none}        | ISO 8601 duration format                |                                                                                                  |
 | persistence_max_records | persistence.max_records | -1            | -1            | -1, 0, positive integer                 | -1 value disables max records feature                                                            |
 | persistence_database    | persistence.database    | {none}        | {none}        | Full path for the sqlite db file        | When none is set the directory is resolved according to XDG spec and the file name is `jobs.db`  |
 | plugins                 | plugins                 | []            | []            | List of plugin names                    |                                                                                                  |
 | default_action          | default_action          | --help        | --help        | Command and optionally arguments        |                                                                                                  |
