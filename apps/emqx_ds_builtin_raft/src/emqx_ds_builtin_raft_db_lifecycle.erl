%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Auxiliary process that manages various aspects of DB lifecycle.
%% * Starting and stopping leader-tied process trees.
%% * Parallel shard termination on DB shutdown.
-module(emqx_ds_builtin_raft_db_lifecycle).

-include_lib("snabbkaffe/include/trace.hrl").

-export([start_link/1]).

-export([
    async_start_leader_sup/2,
    async_stop_leader_sup/2
]).

-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(name(DB), {n, l, {?MODULE, DB}}).
-define(via(DB), {via, gproc, ?name(DB)}).

%% Async requests:
-record(start_leader_sup, {db :: emqx_ds:db(), shard :: emqx_ds:shard()}).
-record(stop_leader_sup, {db :: emqx_ds:db(), shard :: emqx_ds:shard()}).

%%

-spec start_link(emqx_ds:db()) -> {ok, pid()}.
start_link(DB) ->
    gen_server:start_link(?via(DB), ?MODULE, DB, []).

-doc "Order startup of a shard leader supervisor, asynchronously".
-spec async_start_leader_sup(emqx_ds:db(), emqx_ds:shard()) -> ok.
async_start_leader_sup(DB, Shard) ->
    gen_server:cast(?via(DB), #start_leader_sup{db = DB, shard = Shard}).

-doc "Order stopping of a shard leader supervisor, asynchronously".
-spec async_stop_leader_sup(emqx_ds:db(), emqx_ds:shard()) -> ok.
async_stop_leader_sup(DB, Shard) ->
    gen_server:cast(?via(DB), #stop_leader_sup{db = DB, shard = Shard}).

%%

-type state() :: #{db := emqx_ds:db()}.

-spec init(emqx_ds:db()) -> {ok, state()}.
init(DB) ->
    _ = erlang:process_flag(trap_exit, true),
    {ok, #{db => DB}, hibernate}.

-spec handle_call(_Call, _From, state()) -> {reply, ignored, state()}.
handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

-spec handle_cast(_Cast, state()) -> {noreply, state()}.
handle_cast(#start_leader_sup{db = DB, shard = Shard}, State) ->
    ?tp_span(
        debug,
        dsrepl_start_otx_leader,
        #{db => DB, shard => Shard},
        emqx_ds_builtin_raft_db_sup:start_shard_leader_sup(DB, Shard)
    ),
    {noreply, State};
handle_cast(#stop_leader_sup{db = DB, shard = Shard}, State) ->
    ?tp_span(
        debug,
        dsrepl_stop_otx_leader,
        #{db => DB, shard => Shard},
        emqx_ds_builtin_raft_db_sup:stop_shard_leader_sup(DB, Shard)
    ),
    {noreply, State};
handle_cast(_Cast, State) ->
    {noreply, State}.

-spec handle_info(_Cast, state()) -> {noreply, state()}.
handle_info(_Cast, State) ->
    {noreply, State}.

-spec terminate(_Reason, state()) -> _Ok.
terminate(_Reason, #{db := DB}) ->
    stop_shards(DB).

%%

-doc """
Stop all local DB shards.
Shards are asked to stop all at once, so that all required teardown routines
(e.g. `emqx_ds_builtin_raft_shard:prep_stop_server/2`) are running in parallel.
""".
stop_shards(DB) ->
    Shards = emqx_ds_builtin_raft_db_sup:which_shards(DB),
    Requests = [
        %% Stop shard supervisors directly, w/o involvement of `shards_sup`:
        proc_lib:spawn_link(gen_server, stop, [SupPid, shutdown, infinity])
     || Shard <- Shards,
        SupPid <- [emqx_ds_builtin_raft_db_sup:shard_sup({DB, Shard})],
        is_pid(SupPid)
    ],
    lists:foreach(
        fun(Pid) ->
            receive
                {'EXIT', Pid, _Reason} -> ok
            end
        end,
        Requests
    ).
