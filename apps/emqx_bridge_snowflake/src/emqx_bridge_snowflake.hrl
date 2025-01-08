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

%% END ifndef(__EMQX_BRIDGE_SNOWFLAKE_HRL__)
-endif.
