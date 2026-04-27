%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(__EMQX_BRIDGE_BIGQUERY_HRL__).
-define(__EMQX_BRIDGE_BIGQUERY_HRL__, true).

-define(CONNECTOR_TYPE, bigquery).
-define(CONNECTOR_TYPE_BIN, <<"bigquery">>).

-define(ACTION_TYPE, bigquery).
-define(ACTION_TYPE_BIN, <<"bigquery">>).

%% Used by WIF authn
-define(TOKEN_TAB, emqx_bridge_bigquery_tokens).

%% Used by attached service account authn
%% Stores _responses_ to token requests (`{ok, Token} | {error, term()}`)
-define(SA_TOKEN_RESP_TAB, emqx_bridge_bigquery_sa_token).
-define(SA_SERVER_REF, emqx_bridge_bigquery_token_cache).

%% END ifndef(__EMQX_BRIDGE_BIGQUERY_HRL__)
-endif.
