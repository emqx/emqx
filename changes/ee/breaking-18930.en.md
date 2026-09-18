The node-local channel-info table no longer stores a client's will message, its CONNECT
properties, or its subscription list. This keeps the per-client entry at a fixed size instead
of one that grows with the CONNECT packet and with the number of subscriptions, and the entry
is rewritten on every SUBACK and UNSUBACK.

Clients and the REST API are not affected: the removed fields were never returned by the API.
Plugins that read the table through `emqx_cm:get_chan_info/1,2`, `emqx_cm:lookup_client/1` or
`emqx_mgmt:lookup_client/2` no longer see `will_msg`, `conninfo.conn_props` or
`session.subscriptions` in the returned map. Use `emqx_broker:subscriptions/1` for a client's
subscriptions, the `client.connected` / `client.ping` hooks for its CONNECT properties, or
`emqx_connection:info/1` (or `emqx_ws_connection:info/1`) with the channel pid, which still
returns the complete map.
