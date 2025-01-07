%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_persistent_session_ds_gc_worker).

-behaviour(gen_server).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include("session_internals.hrl").

%% API
-export([
    start_link/0,
    check_session/1,
    check_session_after/2,
    session_last_alive_at/2
]).

%% `gen_server' API
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% call/cast/info records
-record(gc, {}).
-record(check_session, {id :: emqx_persistent_session_ds:id()}).

%%--------------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec check_session(emqx_persistent_session_ds:id()) -> ok.
check_session(SessionId) ->
    gen_server:cast(?MODULE, #check_session{id = SessionId}).

-spec check_session_after(emqx_persistent_session_ds:id(), pos_integer()) -> ok.
check_session_after(SessionId, Time0) ->
    #{bump_interval := BumpInterval} = gc_context(),
    Time = max(Time0, BumpInterval),
    _ = erlang:send_after(Time, ?MODULE, #check_session{id = SessionId}),
    ok.

-spec session_last_alive_at(
    pos_integer(), emqx_persistent_session_ds_node_heartbeat_worker:epoch_id() | undefined
) -> pos_integer().
session_last_alive_at(LastAliveAt, undefined) ->
    LastAliveAt;
session_last_alive_at(LastAliveAt, NodeEpochId) ->
    case emqx_persistent_session_ds_node_heartbeat_worker:get_last_alive_at(NodeEpochId) of
        undefined ->
            LastAliveAt;
        NodeLastAliveAt ->
            max(
                LastAliveAt,
                NodeLastAliveAt + emqx_config:get([durable_sessions, heartbeat_interval])
            )
    end.

%%--------------------------------------------------------------------------------
%% `gen_server' API
%%--------------------------------------------------------------------------------

init(_Opts) ->
    ensure_gc_timer(),
    State = #{},
    {ok, State}.

handle_call(_Call, _From, State) ->
    {reply, error, State}.

handle_cast(#check_session{id = SessionId}, State) ->
    do_check_session(SessionId),
    {noreply, State};
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(#gc{}, State) ->
    try_gc(),
    ensure_gc_timer(),
    {noreply, State};
handle_info(#check_session{id = SessionId}, State) ->
    do_check_session(SessionId),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------------------

ensure_gc_timer() ->
    Timeout = emqx_config:get([durable_sessions, session_gc_interval]),
    _ = erlang:send_after(Timeout, self(), #gc{}),
    ok.

try_gc() ->
    %% Only cores should run GC.
    CoreNodes = mria_membership:running_core_nodelist(),
    Res = global:trans(
        {?MODULE, self()},
        fun() -> ?tp_span(debug, ds_session_gc, #{}, run_gc()) end,
        CoreNodes,
        %% Note: we set retries to 1 here because, in rare occasions, GC might start at the
        %% same time in more than one node, and each one will abort the other.  By allowing
        %% one retry, at least one node will (hopefully) get to enter the transaction and
        %% the other will abort.  If GC runs too fast, both nodes might run in sequence.
        %% But, in that case, GC is clearly not too costly, and that shouldn't be a problem,
        %% resource-wise.
        _Retries = 1
    ),
    case Res of
        aborted ->
            ?tp(ds_session_gc_lock_taken, #{}),
            ok;
        ok ->
            ok
    end.

now_ms() ->
    erlang:system_time(millisecond).

run_gc() ->
    NowMs = now_ms(),
    SessionCounts0 = init_epoch_session_counters(NowMs),
    SessionCounts = gc_loop(
        gc_context(), SessionCounts0, emqx_persistent_session_ds_state:make_session_iterator()
    ),
    ok = clenup_inactive_epochs(SessionCounts).

gc_context() ->
    gc_context(now_ms()).

gc_context(NowMs) ->
    GCInterval = emqx_config:get([durable_sessions, session_gc_interval]),
    BumpInterval = emqx_config:get([durable_sessions, heartbeat_interval]),
    SafetyMargin = BumpInterval * 3,
    #{
        min_last_alive => NowMs - SafetyMargin,
        bump_interval => BumpInterval,
        gc_interval => GCInterval
    }.

gc_loop(GCContext, SessionCounts0, It0) ->
    GCBatchSize = emqx_config:get([durable_sessions, session_gc_batch_size]),
    case emqx_persistent_session_ds_state:session_iterator_next(It0, GCBatchSize) of
        {[], _It} ->
            SessionCounts0;
        {Sessions, It} ->
            SessionCounts1 = lists:foldl(
                fun({SessionId, Metadata}, SessionCountsAcc) ->
                    do_gc(GCContext, SessionCountsAcc, SessionId, Metadata)
                end,
                SessionCounts0,
                Sessions
            ),
            gc_loop(GCContext, SessionCounts1, It)
    end.

do_gc(GCContext, SessionId, Metadata) ->
    do_gc(GCContext, _NoEpochCounters = #{}, SessionId, Metadata).

do_gc(#{min_last_alive := MinLastAlive}, SessionCounts, SessionId, Metadata) ->
    #{
        ?last_alive_at := SessionLastAliveAt,
        ?node_epoch_id := NodeEpochId,
        ?expiry_interval := EI,
        ?will_message := MaybeWillMessage,
        ?clientinfo := ClientInfo
    } = Metadata,
    LastAliveAt = session_last_alive_at(SessionLastAliveAt, NodeEpochId),
    IsExpired = LastAliveAt + EI < MinLastAlive,
    case
        should_send_will_message(
            MaybeWillMessage, ClientInfo, IsExpired, LastAliveAt, MinLastAlive
        )
    of
        {true, PreparedMessage} ->
            _ = emqx_broker:publish(PreparedMessage),
            ok = emqx_persistent_session_ds_state:clear_will_message_now(SessionId),
            ?tp(session_gc_published_will_msg, #{id => SessionId, msg => PreparedMessage}),
            ok;
        false ->
            ok
    end,
    case IsExpired of
        true ->
            emqx_persistent_session_ds:destroy_session(SessionId),
            ?tp(debug, ds_session_gc_cleaned, #{
                session_id => SessionId,
                last_alive_at => LastAliveAt,
                expiry_interval => EI,
                min_last_alive => MinLastAlive
            }),
            SessionCounts;
        false ->
            inc_epoch_session_count(SessionCounts, NodeEpochId)
    end.

should_send_will_message(undefined, _ClientInfo, _IsExpired, _LastAliveAt, _MinLastAlive) ->
    false;
should_send_will_message(WillMsg, ClientInfo, IsExpired, LastAliveAt, MinLastAlive) ->
    WillDelayIntervalS = emqx_channel:will_delay_interval(WillMsg),
    WillDelayInterval = timer:seconds(WillDelayIntervalS),
    PastWillDelay = LastAliveAt + WillDelayInterval < MinLastAlive,
    case PastWillDelay orelse IsExpired of
        true ->
            case emqx_channel:prepare_will_message_for_publishing(ClientInfo, WillMsg) of
                {ok, PreparedMessage} ->
                    {true, PreparedMessage};
                {error, _} ->
                    false
            end;
        false ->
            false
    end.

do_check_session(SessionId) ->
    case emqx_persistent_session_ds_state:print_session(SessionId) of
        #{metadata := Metadata} ->
            do_gc(gc_context(), SessionId, Metadata);
        _ ->
            ok
    end.

init_epoch_session_counters(NowMs) ->
    maps:from_keys(
        emqx_persistent_session_ds_node_heartbeat_worker:inactive_epochs(NowMs), 0
    ).

inc_epoch_session_count(SessionCounts, NodeEpochId) when
    is_map_key(NodeEpochId, SessionCounts)
->
    maps:update_with(NodeEpochId, fun(X) -> X + 1 end, 1, SessionCounts);
inc_epoch_session_count(SessionCounts, _NodeEpochId) ->
    SessionCounts.

clenup_inactive_epochs(SessionCounts) ->
    ?tp(debug, clenup_inactive_epochs, #{
        session_counts => SessionCounts
    }),
    EmptyInactiveEpochIds = [NodeEpochId || {NodeEpochId, 0} <- maps:to_list(SessionCounts)],
    ok = emqx_persistent_session_ds_node_heartbeat_worker:delete_epochs(EmptyInactiveEpochIds).
