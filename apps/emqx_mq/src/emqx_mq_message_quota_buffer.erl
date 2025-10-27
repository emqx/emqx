%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_message_quota_buffer).

-moduledoc """
This module is used for buffering quota updates for the message queues and flushing them to the DS periodically.
""".

-include_lib("../gen_src/MessageQueue.hrl").
-include("emqx_mq_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(gen_server).

%% API
-export([
    start/0,
    stop/0,
    add/4,
    flush/0
]).

%% Internal API
-export([
    connect/1,
    do_add/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(POOL_NAME, ?MODULE).
-define(AUTO_RECONNECT_INTERVAL, 2).

-type update() :: emqx_mq_message_quota_index:update().

-record(add_updates, {
    mq_handle :: emqx_mq_types:mq_handle(),
    shard :: emqx_ds:shard(),
    generation :: emqx_ds:generation(),
    updates :: list(update())
}).
-record(flush, {}).

-record(st, {
    buffer :: #{
        emqx_ds:slab() => #{
            emqx_mq_types:mqid() => #{
                mq_handle := emqx_mq_types:mq_handle(),
                updates := list(update())
            }
        }
    },
    buffer_size :: non_neg_integer(),
    flush_tref :: reference() | undefined,
    worker_id :: term()
}).

-spec start() -> ok.
start() ->
    Options = [
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL},
        {pool_size, pool_size()},
        {pool_type, hash}
    ],
    {ok, _} = ecpool:start_sup_pool(?POOL_NAME, ?MODULE, Options),
    ok.

-spec stop() -> ok.
stop() ->
    ecpool:stop_sup_pool(?POOL_NAME).

-spec start_link(#{worker_id => term()}) -> {ok, pid()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

-spec add(emqx_ds:generation(), emqx_ds:shard(), emqx_mq_types:mq_handle(), list(update())) -> ok.
add(Generation, Shard, MQHandle, Updates) ->
    AddUpdates = #add_updates{
        generation = Generation,
        shard = Shard,
        mq_handle = MQHandle,
        updates = Updates
    },
    Key = pool_key(Shard, MQHandle),
    ecpool:pick_and_do({?POOL_NAME, Key}, {?MODULE, do_add, [AddUpdates]}, no_handover).

-spec flush() -> ok.
flush() ->
    {_, Workers} = lists:unzip(ecpool:workers(?POOL_NAME)),
    Clients = lists:filtermap(
        fun(Worker) ->
            case ecpool_worker:client(Worker) of
                {ok, BufferPid} ->
                    {true, BufferPid};
                {error, _} ->
                    false
            end
        end,
        Workers
    ),
    emqx_utils:pforeach(
        fun(BufferPid) ->
            gen_server:call(BufferPid, #flush{})
        end,
        Clients,
        infinity
    ).

%%--------------------------------------------------------------------
%% ecpool callbacks
%%--------------------------------------------------------------------

connect(Opts) ->
    WorkerId = proplists:get_value(ecpool_worker_id, Opts),
    start_link(#{worker_id => WorkerId}).

do_add(BufferPid, AddUpdates) ->
    gen_server:cast(BufferPid, AddUpdates).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([#{worker_id := WorkerId}]) ->
    erlang:process_flag(trap_exit, true),
    {ok, #st{buffer = #{}, flush_tref = undefined, buffer_size = 0, worker_id = WorkerId}}.

handle_info(#flush{}, State0) ->
    State1 = flush(State0),
    State = ensure_flush_timer(State1),
    {noreply, State};
handle_info(Info, State) ->
    ?tp(warning, mq_message_quota_buffer_info, #{info => Info}),
    {noreply, State}.

handle_cast(#add_updates{} = Insert, State0) ->
    State1 = add_to_buffer(Insert, State0),
    State2 = maybe_flush(State1),
    State = ensure_flush_timer(State2),
    {noreply, State};
handle_cast(Info, State) ->
    ?tp(warning, mq_message_quota_buffer_cast, #{info => Info}),
    {noreply, State}.

handle_call(#flush{}, _From, State0) ->
    State1 = flush(State0),
    State = ensure_flush_timer(State1),
    {reply, ok, State};
handle_call(Info, _From, State) ->
    ?tp(warning, mq_message_quota_buffer_call, #{info => Info}),
    {reply, {error, {unknown_call, Info}}, State}.

terminate(_Reason, State) ->
    _ = flush(State),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

maybe_flush(State = #st{buffer_size = BufferSize}) ->
    case BufferSize >= buffer_max_size() of
        true ->
            flush(State);
        false ->
            State
    end.

ensure_flush_timer(State = #st{flush_tref = undefined, buffer_size = 0}) ->
    State;
ensure_flush_timer(State = #st{flush_tref = undefined, buffer_size = _BufferSize}) ->
    TRef = erlang:send_after(buffer_flush_interval(), self(), #flush{}),
    State#st{flush_tref = TRef};
ensure_flush_timer(State = #st{flush_tref = TRef, buffer_size = 0}) ->
    _ = emqx_utils:cancel_timer(TRef),
    State#st{flush_tref = undefined};
ensure_flush_timer(State) ->
    State.

add_to_buffer(#add_updates{} = Insert, State = #st{buffer = Buffer0, buffer_size = BufferSize}) ->
    #add_updates{
        mq_handle = MQHandle,
        shard = Shard,
        generation = Generation,
        updates = NewUpdates
    } = Insert,
    Slab = {Shard, Generation},
    SlabBuffer0 = maps:get(Slab, Buffer0, #{}),
    MqId = emqx_mq_prop:id(MQHandle),
    MQBuffer0 = maps:get(MqId, SlabBuffer0, #{
        mq_handle => MQHandle,
        updates => []
    }),
    Updates0 = maps:get(updates, MQBuffer0, []),
    Updates = lists:reverse(NewUpdates) ++ Updates0,
    MQBuffer = MQBuffer0#{updates => Updates},
    SlabBuffer = SlabBuffer0#{MqId => MQBuffer},
    Buffer = Buffer0#{Slab => SlabBuffer},
    State#st{buffer = Buffer, buffer_size = BufferSize + 1}.

flush(State = #st{buffer = Buffer}) ->
    ok = update_metrics(State),
    ok = maps:foreach(
        fun({Shard, Generation}, SlabBuffer) ->
            UpdatesByMQ = lists:map(
                fun(#{mq_handle := MQHandle, updates := Updates}) ->
                    {MQHandle, lists:reverse(Updates)}
                end,
                maps:values(SlabBuffer)
            ),
            emqx_mq_message_db:flush_quota_index(Shard, Generation, UpdatesByMQ)
        end,
        Buffer
    ),
    State#st{buffer = #{}, buffer_size = 0}.

pool_key(Shard, MQHandle) ->
    {Shard, emqx_mq_prop:id(MQHandle)}.

update_metrics(#st{worker_id = WorkerId}) ->
    {message_queue_len, InboxSize} = process_info(self(), message_queue_len),
    ok = emqx_mq_metrics:set_quota_buffer_inbox_size(WorkerId, InboxSize).

pool_size() ->
    emqx:get_config([mq, quota, buffer_pool_size]).

buffer_max_size() ->
    emqx:get_config([mq, quota, buffer_max_size]).

buffer_flush_interval() ->
    round(emqx:get_config([mq, quota, buffer_flush_interval]) * (0.9 + 0.2 * rand:uniform())).
