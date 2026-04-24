%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_bigquery_token_cache).

%% API
-export([
    start_link/0,
    child_spec/0,

    create_tables/0,
    get_or_refresh/2,
    get_or_refresh/3,
    unregister/1,
    clear_cache/0,
    clear_cache/1
]).

-include("emqx_bridge_bigquery.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    Opts = #{table => ?SA_TOKEN_RESP_TAB},
    emqx_connector_jwt_token_cache:start_link({local, ?MODULE}, Opts).

child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => 5_000,
        type => worker,
        modules => [?MODULE, emqx_connector_jwt_token_cache]
    }.

create_tables() ->
    emqx_connector_jwt_token_cache:create_tables(?SA_TOKEN_RESP_TAB).

get_or_refresh(ClientId, RefreshFn) ->
    emqx_connector_jwt_token_cache:get_or_refresh(?MODULE, ?SA_TOKEN_RESP_TAB, ClientId, RefreshFn).

get_or_refresh(ClientId, RefreshFn, Opts) ->
    emqx_connector_jwt_token_cache:get_or_refresh(
        ?MODULE, ?SA_TOKEN_RESP_TAB, ClientId, RefreshFn, Opts
    ).

unregister(ClientId) ->
    emqx_connector_jwt_token_cache:unregister(?MODULE, ClientId).

%% For debug/test/manual ops
clear_cache() ->
    emqx_connector_jwt_token_cache:clear_cache(?SA_TOKEN_RESP_TAB).

%% For debug/test/manual ops
clear_cache(ClientId) ->
    emqx_connector_jwt_token_cache:clear_cache(?SA_TOKEN_RESP_TAB, ClientId).
