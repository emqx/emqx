The deprecated `$q/...` and `$s/...` subscription prefixes have been removed.

Use `$queue/<name>/...` for message queues and `$stream/<name>/...` for streams. The short prefixes are now treated as regular MQTT topic filters.

Message queues and streams created before 6.1.1 have no name of their own. The Dashboard and the REST API show them with the name `/<topic-filter>`. Clients could reach them only through the `$q/...` and `$s/...` prefixes, so clients can no longer subscribe to them.

To delete all such message queues and streams, run the following command on any one node of the cluster:

```
emqx eval 'io:format("~p~n", [{emqx_mq_registry:delete_legacy(), emqx_streams_registry:delete_legacy()}]).'
```

The command prints the number of deleted message queues and the number of deleted streams, for example `{2,1}`. A second run prints `{0,0}`.

Deleting a message queue or a stream also deletes its stored messages. The command does not move these messages to a different message queue or stream.

To continue to collect messages for the same topic filter, create a message queue or stream with a name and that topic filter. A new message queue or stream collects only the messages that are published after you create it.
