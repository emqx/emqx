%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(__EMQX_BRIDGE_COUCHBASE_HRL__).
-define(__EMQX_BRIDGE_COUCHBASE_HRL__, true).

-define(CONNECTOR_TYPE, couchbase).
-define(CONNECTOR_TYPE_BIN, <<"couchbase">>).

-define(ACTION_TYPE, couchbase).
-define(ACTION_TYPE_BIN, <<"couchbase">>).

-define(SERVER_OPTIONS, #{
    default_port => 8093
}).

%% END ifndef(__EMQX_BRIDGE_COUCHBASE_HRL__)
-endif.
