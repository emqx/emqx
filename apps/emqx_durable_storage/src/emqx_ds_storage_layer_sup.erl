%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_storage_layer_sup).

-behaviour(supervisor).

%% API:
-export([start_link/0, start_shard/1, stop_shard/1]).

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

-spec start_shard(emqx_ds:shard()) -> supervisor:startchild_ret().
start_shard(Shard) ->
    supervisor:start_child(?SUP, shard_child_spec(Shard)).

-spec stop_shard(emqx_ds:shard()) -> ok | {error, _}.
stop_shard(Shard) ->
    ok = supervisor:terminate_child(?SUP, Shard),
    ok = supervisor:delete_child(?SUP, Shard).

%%================================================================================
%% behaviour callbacks
%%================================================================================

init([]) ->
    Children = [],
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    {ok, {SupFlags, Children}}.

%%================================================================================
%% Internal functions
%%================================================================================

-spec shard_child_spec(emqx_ds:shard()) -> supervisor:child_spec().
shard_child_spec(Shard) ->
    #{
        id => Shard,
        start => {emqx_ds_storage_layer, start_link, [Shard]},
        shutdown => 5_000,
        restart => permanent,
        type => worker
    }.
