# Configuration

The configuration consists of lower case attributes of [cfg module](taro/cfg.py).
This is a recommended pattern for sharing configuration across modules described in [official python documentation](https://docs.python.org/3/faq/programming.html#how-do-i-share-global-variables-across-modules).
The upper case constants of the module contain initial values for the configuration attributes and represents minimal configuration.
Normally when taro is initialized a configuration file is loaded and its content overrides attributes of the [cfg module](taro/cfg.py).

## Minimal configuration
This configuration is set by using `-mc` option. When it's used then no configuration file is loaded to override attributes of the [cfg module](taro/cfg.py).
Therefore, the values of the configuration attributes remains same as they were initialized from the constants in the module.
Such configuration is very defensive and disables most configurable features.

### Usage
This configuration is used mainly during testing and development.