The deprecated `$q/...` and `$s/...` subscription prefixes have been removed.

Use `$queue/<name>/...` for message queues and `$stream/<name>/...` for streams. The short prefixes are now treated as regular MQTT topic filters.

Message queues and streams created before 6.1.1 have no name of their own. The Dashboard and the REST API show them with the name `/<topic-filter>`. Clients could reach them only through the `$q/...` and `$s/...` prefixes, so clients can no longer subscribe to them.

To delete all such message queues and streams, run the following command on any one node of the cluster:

```
emqx eval 'io:format("~p~n", [{emqx_mq_registry:delete_legacy(), emqx_streams_registry:delete_legacy()}]).'
```

The command prints the number of deleted message queues and the number of deleted streams, for example `{2,1}`. A second run prints `{0,0}`.

Deleting a message queue or a stream also deletes its stored messages. To keep one of them, create a new message queue or stream with a name and the same topic filter before you run the command.
