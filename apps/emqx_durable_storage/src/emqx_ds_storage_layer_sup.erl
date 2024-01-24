%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_storage_layer_sup).

-behaviour(supervisor).

%% API:
-export([start_link/0, start_shard/2, stop_shard/1, ensure_shard/2]).

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
    supervisor:start_link(?MODULE, []).

-spec start_shard(emqx_ds_storage_layer:shard_id(), emqx_ds:create_db_opts()) ->
    supervisor:startchild_ret().
start_shard(Shard, Options) ->
    supervisor:start_child(?SUP, shard_child_spec(Shard, Options)).

-spec stop_shard(emqx_ds_storage_layer:shard_id()) -> ok | {error, _}.
stop_shard(Shard) ->
    ok = supervisor:terminate_child(?SUP, Shard),
    ok = supervisor:delete_child(?SUP, Shard).

-spec ensure_shard(emqx_ds_storage_layer:shard_id(), emqx_ds_storage_layer:options()) ->
    ok | {error, _Reason}.
ensure_shard(Shard, Options) ->
    case start_shard(Shard, Options) of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

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

-spec shard_child_spec(emqx_ds_storage_layer:shard_id(), emqx_ds:create_db_opts()) ->
    supervisor:child_spec().
shard_child_spec(Shard, Options) ->
    #{
        id => Shard,
        start => {emqx_ds_storage_layer, start_link, [Shard, Options]},
        shutdown => 5_000,
        restart => permanent,
        type => worker
    }.
