%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_sup).

-behaviour(supervisor).

%% API:
-export([start_link/0, ensure_workers/0]).

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
    supervisor:start_link({local, ?SUP}, ?MODULE, top).

-spec ensure_workers() -> ok.
ensure_workers() ->
    ChildSpec = #{
        id => workers_sup,
        restart => temporary,
        type => supervisor,
        start => {supervisor, start_link, [?MODULE, workers]}
    },
    case supervisor:start_child(?SUP, ChildSpec) of
        {ok, _} ->
            ok;
        {error, already_present} ->
            ok;
        {error, {already_started, _}} ->
            ok
    end.

%%================================================================================
%% behaviour callbacks
%%================================================================================

-dialyzer({nowarn_function, init/1}).
init(top) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 10,
        period => 1
    },
    {ok, {SupFlags, []}};
init(workers) ->
    %% TODO: technically, we don't need rocksDB for the alternative
    %% backends. But right now we have any:
    Children = [meta(), storage_layer_sup()],
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
