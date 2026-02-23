%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_registry).

%% API
-export([
    ensure_card_schema_registered/0
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include("emqx_a2a_registry_internal.hrl").

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

ensure_card_schema_registered() ->
    case emqx_schema_registry:get_serde(?A2A_SCHEMA_REGISTRY_SERDE_NAME) of
        {ok, _} ->
            ok;
        {error, not_found} ->
            ok = emqx_schema_registry:build_serdes([
                {?A2A_SCHEMA_REGISTRY_SERDE_NAME, #{
                    type => json,
                    source => agent_card_schema_source()
                }}
            ])
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

agent_card_schema_path() ->
    filename:join([code:lib_dir(emqx_a2a_registry), "priv", "agent_card_schema.json"]).

agent_card_schema_source() ->
    {ok, Source} = file:read_file(agent_card_schema_path()),
    Source.
