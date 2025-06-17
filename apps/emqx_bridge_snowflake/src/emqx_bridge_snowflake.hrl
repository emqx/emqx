%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(__EMQX_BRIDGE_SNOWFLAKE_HRL__).
-define(__EMQX_BRIDGE_SNOWFLAKE_HRL__, true).

-define(CONNECTOR_TYPE, snowflake).
-define(CONNECTOR_TYPE_BIN, <<"snowflake">>).

-define(ACTION_TYPE, snowflake).
-define(ACTION_TYPE_BIN, <<"snowflake">>).

-define(SERVER_OPTS, #{
    default_port => 443
}).

-define(AGGREG_SUP, emqx_bridge_snowflake_sup).

-define(streaming, streaming).
-define(aggregated, aggregated).

-define(action_res_id, action_res_id).
-define(append_rows_path, append_rows_path).
-define(append_rows_path_template, append_rows_path_template).
-define(channel, channel).
-define(channel_name, channel_name).
-define(connect_timeout, connect_timeout).
-define(database, database).
-define(generate_jwt_fn, generate_jwt_fn).
-define(health_check_timeout, health_check_timeout).
-define(hostname, hostname).
-define(id, id).
-define(jwt_config, jwt_config).
-define(max_inactive, max_inactive).
-define(max_retries, max_retries).
-define(mode, mode).
-define(open_channel_path, open_channel_path).
-define(open_channel_path_template, open_channel_path_template).
-define(pipe, pipe).
-define(request_ttl, request_ttl).
-define(schema, schema).
-define(setup, setup).
-define(setup_pool_id, setup_pool_id).
-define(setup_pool_state, setup_pool_state).
-define(write, write).
-define(write_pool_id, write_pool_id).
-define(write_state_tab, write_state_tab).

%% END ifndef(__EMQX_BRIDGE_SNOWFLAKE_HRL__)
-endif.
