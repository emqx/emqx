Added a new `resource_opts.health_check_timeout` configuration to all Connectors, Actions and Sources, with default value of 60 seconds.  If a health check takes more than this to return a response, the Connector/Action/Source will be deemed `disconnected`.

Note: since the default is 60 seconds, this means that if a Connector/Action/Source previously could take more than that to return a healthy response, now it'll be deemed disconnected in such situations.
