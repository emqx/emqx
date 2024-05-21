%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("stdlib/include/qlc.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-include("emqx_persistent_session_ds.hrl").

%% API
-export([
    start_link/0,
    check_session/1,
    check_session_after/2
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
        fun() -> ?tp_span(debug, ds_session_gc, #{}, start_gc()) end,
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

start_gc() ->
    #{
        min_last_alive := MinLastAlive,
        min_last_alive_will_msg := MinLastAliveWillMsg
    } = gc_context(),
    gc_loop(
        MinLastAlive, MinLastAliveWillMsg, emqx_persistent_session_ds_state:make_session_iterator()
    ).

gc_context() ->
    GCInterval = emqx_config:get([durable_sessions, session_gc_interval]),
    BumpInterval = emqx_config:get([durable_sessions, heartbeat_interval]),
    TimeThreshold = max(GCInterval, BumpInterval) * 3,
    NowMS = now_ms(),
    #{
        min_last_alive => NowMS - TimeThreshold,
        %% For will messages, we don't need to be so strict as session GC (GC interval is
        %% of the order of ~ 10 minutes by default, bump interval ~ 100 ms), otherwise
        %% most will be sent very late.
        min_last_alive_will_msg => NowMS - BumpInterval * 5,
        time_threshold => TimeThreshold,
        bump_interval => BumpInterval,
        gc_interval => GCInterval
    }.

gc_loop(MinLastAlive, MinLastAliveWillMsg, It0) ->
    GCBatchSize = emqx_config:get([durable_sessions, session_gc_batch_size]),
    case emqx_persistent_session_ds_state:session_iterator_next(It0, GCBatchSize) of
        {[], _It} ->
            ok;
        {Sessions, It} ->
            [
                do_gc(SessionId, MinLastAliveWillMsg, MinLastAlive, Metadata)
             || {SessionId, Metadata} <- Sessions
            ],
            gc_loop(MinLastAlive, MinLastAliveWillMsg, It)
    end.

do_gc(SessionId, MinLastAliveWillMsg, MinLastAlive, Metadata) ->
    #{
        ?last_alive_at := LastAliveAt,
        ?expiry_interval := EI,
        ?will_message := MaybeWillMessage,
        ?clientinfo := ClientInfo
    } = Metadata,
    IsExpired = LastAliveAt + EI < MinLastAlive,
    case
        should_send_will_message(
            MaybeWillMessage, ClientInfo, IsExpired, LastAliveAt, MinLastAliveWillMsg
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
            });
        false ->
            ok
    end.

should_send_will_message(
    undefined = _WillMsg, _ClientInfo, _IsExpired, _LastAliveAt, _MinLastAliveWillMsg
) ->
    false;
should_send_will_message(WillMsg, ClientInfo, IsExpired, LastAliveAt, MinLastAliveWillMsg) ->
    WillDelayIntervalS = emqx_channel:will_delay_interval(WillMsg),
    WillDelayInterval = timer:seconds(WillDelayIntervalS),
    PastWillDelay = LastAliveAt + WillDelayInterval < MinLastAliveWillMsg,
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
            #{
                min_last_alive := MinLastAlive,
                min_last_alive_will_msg := MinLastAliveWillMsg
            } = gc_context(),
            do_gc(SessionId, MinLastAliveWillMsg, MinLastAlive, Metadata);
        _ ->
            ok
    end.
