%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(__EMQX_BRIDGE_GCP_PUBSUB_HRL__).
-define(__EMQX_BRIDGE_GCP_PUBSUB_HRL__, true).

%% Used by WIF authn
-define(TOKEN_TAB, emqx_bridge_gcp_pubsub_tokens).

%% Used by attached service account authn
%% Stores _responses_ to token requests (`{ok, Token} | {error, term()}`)
-define(SA_TOKEN_RESP_TAB, emqx_bridge_gcp_pubsub_sa_token).
-define(SA_SERVER_REF, emqx_bridge_gcp_pubsub_token_cache).

-endif.
