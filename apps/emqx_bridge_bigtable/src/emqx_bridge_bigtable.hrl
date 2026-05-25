%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(__EMQX_BRIDGE_BIGTABLE_HRL__).
-define(__EMQX_BRIDGE_BIGTABLE_HRL__, true).

-define(CONNECTOR_TYPE, bigtable).
-define(CONNECTOR_TYPE_BIN, <<"bigtable">>).

-define(ACTION_TYPE, bigtable).
-define(ACTION_TYPE_BIN, <<"bigtable">>).

%% Used by WIF authn
-define(TOKEN_TAB, emqx_bridge_bigtable_tokens).

%% Used by attached service account authn
%% Stores _responses_ to token requests (`{ok, Token} | {error, term()}`)
-define(SA_TOKEN_RESP_TAB, emqx_bridge_bigtable_sa_token).
-define(SA_SERVER_REF, emqx_bridge_bigtable_token_cache).

%% END ifndef(__EMQX_BRIDGE_BIGTABLE_HRL__)
-endif.
