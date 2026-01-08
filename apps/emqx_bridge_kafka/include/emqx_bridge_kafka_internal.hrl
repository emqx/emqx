%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_BRIDGE_KAFKA_INTERNAL_HRL).
-define(EMQX_BRIDGE_KAFKA_INTERNAL_HRL, true).

%% Stores _responses_ to token requests (`{ok, Token} | {error, term()}`)
-define(TOKEN_RESP_TAB, emqx_bridge_kafka_token).

-endif.
