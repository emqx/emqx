%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(__EMQX_BRIDGE_ZEROBUS_HRL__).
-define(__EMQX_BRIDGE_ZEROBUS_HRL__, true).

-define(CONNECTOR_TYPE, zerobus).
-define(CONNECTOR_TYPE_BIN, <<"zerobus">>).

-define(ACTION_TYPE, zerobus).
-define(ACTION_TYPE_BIN, <<"zerobus">>).

-define(PARSE_SERVER_OPTS, #{supported_schemes => ["http", "https"]}).

-define(META_TAB, emqx_bridge_zerobus_meta).
-define(META_STREAM_KEY(ACTIONRESID, IDX), {ACTIONRESID, IDX, stream}).
-define(META_ROW(KEY, VAL), {KEY, VAL}).

-define(acked, acked).
-define(action_res_id, action_res_id).
-define(callers, callers).
-define(catalog, catalog).
-define(client_pool, client_pool).
-define(continue, continue).
-define(descriptor, descriptor).
-define(idx, idx).
-define(last_acked_seq, last_acked_seq).
-define(last_seen_seq, last_seen_seq).
-define(message_type, message_type).
-define(n_restarts, n_restarts).
-define(opts, opts).
-define(pool, pool).
-define(proto, proto).
-define(record, record).
-define(recv_handle, recv_handle).
-define(recv_pool, recv_pool).
-define(reopen, reopen).
-define(request_ttl, request_ttl).
-define(schema, schema).
-define(seq, seq).
-define(serde_name, serde_name).
-define(stream, stream).
-define(stream_closed, stream_closed).
-define(table, table).
-define(table_fqn, table_fqn).
-define(undefined, undefined).
-define(writer, writer).

%% END ifndef(__EMQX_BRIDGE_ZEROBUS_HRL__)
-endif.
