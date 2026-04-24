%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka_token_cache).

%% API
-export([
    start_link/0,

    create_tables/0,
    get_or_refresh/2,
    get_or_refresh/3,
    unregister/1,
    clear_cache/0,
    clear_cache/1
]).

-include("emqx_bridge_kafka_internal.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    Opts = #{table => ?TOKEN_RESP_TAB},
    emqx_connector_jwt_token_cache:start_link({local, ?MODULE}, Opts).

create_tables() ->
    emqx_connector_jwt_token_cache:create_tables(?TOKEN_RESP_TAB).

get_or_refresh(ClientId, RefreshFn) ->
    emqx_connector_jwt_token_cache:get_or_refresh(?MODULE, ?TOKEN_RESP_TAB, ClientId, RefreshFn).

get_or_refresh(ClientId, RefreshFn, Opts) ->
    emqx_connector_jwt_token_cache:get_or_refresh(
        ?MODULE, ?TOKEN_RESP_TAB, ClientId, RefreshFn, Opts
    ).

unregister(ClientId) ->
    emqx_connector_jwt_token_cache:unregister(?MODULE, ClientId).

%% For debug/test/manual ops
clear_cache() ->
    emqx_connector_jwt_token_cache:clear_cache(?TOKEN_RESP_TAB).

%% For debug/test/manual ops
clear_cache(ClientId) ->
    emqx_connector_jwt_token_cache:clear_cache(?TOKEN_RESP_TAB, ClientId).
