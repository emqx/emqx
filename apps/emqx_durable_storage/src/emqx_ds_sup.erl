%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_sup).

-behaviour(supervisor).

%% API:
-export([start_link/0]).

%% behaviour callbacks:
-export([init/1]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(SUP, ?MODULE).

%%================================================================================
%% API funcions
%%================================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?SUP}, ?MODULE, []).

%%================================================================================
%% behaviour callbacks
%%================================================================================

-dialyzer({nowarn_function, init/1}).
init([]) ->
    %% TODO: technically, we don't need rocksDB for the alternative
    %% backends. But right now we have any:
    Children =
        case mria:rocksdb_backend_available() of
            true -> [meta(), storage_layer_sup()];
            false -> []
        end,
    SupFlags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },
    {ok, {SupFlags, Children}}.

%%================================================================================
%% Internal functions
%%================================================================================

meta() ->
    #{
        id => emqx_ds_replication_layer_meta,
        start => {emqx_ds_replication_layer_meta, start_link, []},
        restart => permanent,
        type => worker,
        shutdown => 5000
    }.

storage_layer_sup() ->
    #{
        id => local_store_shard_sup,
        start => {emqx_ds_storage_layer_sup, start_link, []},
        restart => permanent,
        type => supervisor,
        shutdown => infinity
    }.
