%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_builtin_raft_liveness).
-moduledoc """
This per-db server caches information about the remote optimistic
transaction processes. It is responsible for setting and resetting
shard readiness status. It tracks state of all shards, with or without
local replicas.

NOTE: this module monitors optimistic leader processes on remote nodes
and receives up notifications from them. But since establishing the
global order of events during leadership takever can be challenging,
the logic here relies on simple polling. Events serve as mere hints to
speed up change detection, but their content is ignored.
""".

-behaviour(gen_server).

%% API:
-export([start_link/1, notify_shard_up/2]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([do_notify_shard_up_v1/3]).

-export_type([]).

-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type shard_info() ::
    #{
        pid := pid(),
        monitor := reference()
    }.

-record(s, {
    db :: emqx_ds:db(),
    shards = #{} :: #{emqx_ds:shard() => shard_info() | undefined},
    monitors = #{} :: #{reference() => emqx_ds:shard()}
}).

-type s() :: #s{}.

-define(name(DB), {n, l, {?MODULE, DB}}).
-define(via(DB), {via, gproc, ?name(DB)}).

-record(cast_shard_up, {shard :: emqx_ds:shard()}).
-record(to_poll, {}).

-define(poll_interval, 1_000).

%%================================================================================
%% API functions
%%================================================================================

-spec start_link(emqx_ds:db()) -> {ok, pid()}.
start_link(DB) ->
    gen_server:start_link(?via(DB), ?MODULE, DB, []).

-spec notify_shard_up(emqx_ds:db(), emqx_ds:shard()) -> ok.
notify_shard_up(DB, Shard) ->
    emqx_ds_builtin_raft_liveness_proto_v1:multicast_shard_up(nodes(), DB, Shard, -1).

%%================================================================================
%% Internal exports
%%================================================================================

%% RPC target
-spec do_notify_shard_up_v1(emqx_ds:db(), emqx_ds:shard(), integer()) -> ok.
do_notify_shard_up_v1(DB, Shard, _LeaderTerm) ->
    %% NOTE: leader term can be used in the future to serialize
    %% updates and have a smarter implementation (e.g. avoid
    %% dependence on `global'?).
    gen_server:cast(?via(DB), #cast_shard_up{shard = Shard}).

%%================================================================================
%% behavior callbacks
%%================================================================================

init(DB) ->
    process_flag(trap_exit, true),
    S = #s{db = DB},
    {ok, check_on_all(S)}.

handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(#cast_shard_up{shard = Shard}, S) ->
    {noreply, check_on_shard(Shard, S)};
handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(#to_poll{}, S) ->
    {noreply, check_on_all(S)};
handle_info({'DOWN', MRef, process, _, _}, S = #s{monitors = Monitors}) ->
    case Monitors of
        #{MRef := Shard} ->
            {noreply, check_on_shard(Shard, S)};
        _ ->
            {noreply, S}
    end;
handle_info({'EXIT', _, shutdown}, S) ->
    {stop, shutdown, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, #s{db = DB}) ->
    lists:foreach(
        fun(Shard) ->
            emqx_ds:set_shard_ready(DB, Shard, false)
        end,
        emqx_ds:list_shards(DB)
    ).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

-spec check_on_all(s()) -> s().
check_on_all(S0 = #s{db = DB}) ->
    erlang:send_after(?poll_interval, self(), #to_poll{}),
    lists:foldl(
        fun(Shard, S) ->
            check_on_shard(Shard, S)
        end,
        S0,
        emqx_ds:list_shards(DB)
    ).

-spec check_on_shard(emqx_ds:shard(), s()) -> s().
check_on_shard(Shard, S = #s{db = DB, shards = Shards}) ->
    Current = emqx_ds_builtin_raft:lookup_global_otx_leader(DB, Shard),
    case maps:get(Shard, Shards, undefined) of
        #{pid := Current} when is_pid(Current) ->
            %% No change, up:
            S;
        _ when is_pid(Current) ->
            %% Shard went up or leader changed:
            on_up(Shard, Current, on_down(Shard, S));
        #{pid := _} ->
            %% Shard went down:
            on_down(Shard, S);
        undefined ->
            %% No change, down:
            S
    end.

-spec on_up(emqx_ds:shard(), pid(), s()) -> s().
on_up(Shard, Current, S = #s{db = DB, shards = Shards, monitors = Monitors}) when is_pid(Current) ->
    MRef = monitor(process, Current),
    emqx_ds:set_shard_ready(DB, Shard, true),
    S#s{
        shards = Shards#{Shard => #{pid => Current, monitor => MRef}},
        monitors = Monitors#{MRef => Shard}
    }.

-spec on_down(emqx_ds:shard(), s()) -> s().
on_down(Shard, S = #s{db = DB, shards = Shards0, monitors = Monitors0}) ->
    case maps:take(Shard, Shards0) of
        {#{monitor := MRef}, Shards} ->
            demonitor(MRef),
            Monitors = maps:remove(MRef, Monitors0),
            emqx_ds:set_shard_ready(DB, Shard, false),
            S#s{
                shards = Shards,
                monitors = Monitors
            };
        error ->
            S
    end.
