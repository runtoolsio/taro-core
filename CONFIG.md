# Configuration

The configuration consists of lower case attributes of the [cfg module](taro/cfg.py).
This is a recommended pattern for sharing configuration across modules described in [official python documentation](https://docs.python.org/3/faq/programming.html#how-do-i-share-global-variables-across-modules).
The upper case constants of the module contain initial values for the configuration attributes and represent minimal configuration.

## Current configuration
Normally when a taro command is executed (except the `config` command) the configuration file located in corresponding XDG directory is loaded
and its content overrides attributes of the [cfg module](taro/cfg.py).
The content of the current config file can be printed by this command: `taro config show`

## Default configuration
Default configuration is located in the default config file. This file is copied to corresponding XDG directory when taro is first initialized after installation.
A user can then edit the file to change the [current configuration](#current-configuration). The command line `-dc` option can be specified to use the original default configuration.
The content of the default config file can be printed by this command: `taro config show -dc`

## Minimal configuration
This configuration is used when `-mc` command line option is specified. In such case no configuration file is loaded to override attributes of the [cfg module](taro/cfg.py).
Therefore, the values of the configuration attributes remains same as they were initialized from the constants in the module.
Such configuration is very defensive and disables most configurable features.

### Usage
This configuration is used mainly during testing and development.

## Setting config attributes manually
Each configuration attribute can be set manually using `--set` command line option. In such case a value loaded from the config file for this attribute (if any) is ignored.

### Example
#### Disable persistence for an execution
`taro exec --set persistance_enabled=False ./will_not_be_stored_in_db.sh`

#### Disable logging
`taro exec --set log_mode=disabled echo There is no logging now`
