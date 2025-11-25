%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota_buffer).

-moduledoc """
This module is used for buffering quota updates for the message queues and flushing them periodically.
The quota updates are buffered by a composite key (tx_key, key).

The flush is triggered by the queue size or by a timer.

During a flush, for each unique tx_key a transaction is opened via quota_buffer_flush_transaction/2 callback.
Then for each key a quota index
* is read via quota_buffer_flush_tx_read/2,
* is updated with the buffered updates
* is persisted via quota_buffer_flush_tx_write/3 callback.

So the `tx_key` is something for which a single transaction is desired to be opened,
like a slab in case of DS storage.

The (`tx_key`, `key`) pair is something that uniquely identifies quota index,
like (slab, mq_topic) in case of MQs in DS storage.
""".

-include("../emqx_mq_internal.hrl").
-include("emqx_mq_quota.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(gen_server).

%% API
-export([
    start/2,
    stop/1,
    add/6,
    flush/1
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

-type message_update_info() :: #{
    message := emqx_types:message(),
    message_size := non_neg_integer()
}.
-type index_opts() :: emqx_mq_quota_index:opts().
-type index() :: emqx_mq_quota_index:t().
-type name() :: ecpool:pool_name().
-type tx_key() :: term().
-type key() :: term().

-type options() :: #{
    pool_size := pos_integer(),
    cbm := module()
}.

%%--------------------------------------------------------------------
%% emqx_mq_quota_buffer behaviour callbacks
%%--------------------------------------------------------------------

%% The main responsibility of the callbacks is to restore and persist quota index from/to the underlying storage.

-callback quota_buffer_flush_transaction(tx_key(), fun(() -> ok)) -> ok.
-callback quota_buffer_flush_tx_read(tx_key(), key(), index_opts()) -> index() | undefined.
-callback quota_buffer_flush_tx_write(tx_key(), key(), index()) -> ok.
-callback quota_buffer_max_size() -> pos_integer().
-callback quota_buffer_flush_interval() -> non_neg_integer().
-callback quota_buffer_notify_queue_size(emqx_metrics_worker:worker_id(), non_neg_integer()) -> ok.

-define(AUTO_RECONNECT_INTERVAL, 2).

-record(add_updates, {
    quota_index_opts :: emqx_mq_quota_index:opts(),
    tx_key :: tx_key(),
    key :: key(),
    updates :: list(emqx_mq_quota_index:update())
}).
-record(flush, {}).

-record(st, {
    buffer :: #{
        tx_key() => #{
            key() => #{
                quota_index_opts := emqx_mq_quota_index:opts(),
                updates := list(emqx_mq_quota_index:update())
            }
        }
    },
    buffer_size :: non_neg_integer(),
    flush_tref :: reference() | undefined,
    worker_id :: term(),
    cbm :: module()
}).

-spec start(name(), options()) -> ok.
start(Name, Options) ->
    PoolSize = maps:get(pool_size, Options, ?DEFAULT_QUOTA_BUFFER_POOL_SIZE),
    PoolOptions = [
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL},
        {pool_size, PoolSize},
        {pool_type, hash},
        {options, Options}
    ],
    {ok, _} = ecpool:start_sup_pool(Name, ?MODULE, PoolOptions),
    ok.

-spec stop(name()) -> ok.
stop(Name) ->
    ecpool:stop_sup_pool(Name).

-spec start_link(#{worker_id => term()}) -> {ok, pid()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

-spec add(
    name(),
    tx_key(),
    key(),
    index_opts(),
    message_update_info() | undefined,
    message_update_info()
) -> ok.
add(Name, TxKey, Key, QuotaIndexOpts, OldMessageInfo, NewMessageInfo) ->
    AddUpdates = #add_updates{
        tx_key = TxKey,
        key = Key,
        quota_index_opts = QuotaIndexOpts,
        updates = quota_index_updates(OldMessageInfo, NewMessageInfo)
    },
    PoolKey = {TxKey, Key},
    ecpool:pick_and_do({Name, PoolKey}, {?MODULE, do_add, [AddUpdates]}, no_handover).

-spec flush(ecpool:pool_name()) -> ok.
flush(Name) ->
    {_, Workers} = lists:unzip(ecpool:workers(Name)),
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
    Options = proplists:get_value(options, Opts),
    start_link(Options#{worker_id => WorkerId}).

do_add(BufferPid, AddUpdates) ->
    gen_server:cast(BufferPid, AddUpdates).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([#{worker_id := WorkerId, cbm := CBM}]) ->
    erlang:process_flag(trap_exit, true),
    {ok, #st{
        buffer = #{},
        flush_tref = undefined,
        buffer_size = 0,
        worker_id = WorkerId,
        cbm = CBM
    }}.

handle_info(#flush{}, State0) ->
    State1 = do_flush(State0),
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
    State1 = do_flush(State0),
    State = ensure_flush_timer(State1),
    {reply, ok, State};
handle_call(Info, _From, State) ->
    ?tp(warning, mq_message_quota_buffer_call, #{info => Info}),
    {reply, {error, {unknown_call, Info}}, State}.

terminate(_Reason, State) ->
    _ = do_flush(State),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

maybe_flush(State = #st{buffer_size = BufferSize}) ->
    case BufferSize >= buffer_max_size(State) of
        true ->
            do_flush(State);
        false ->
            State
    end.

ensure_flush_timer(State = #st{flush_tref = undefined, buffer_size = 0}) ->
    State;
ensure_flush_timer(State = #st{flush_tref = undefined, buffer_size = _BufferSize}) ->
    TRef = erlang:send_after(buffer_flush_interval(State), self(), #flush{}),
    State#st{flush_tref = TRef};
ensure_flush_timer(State = #st{flush_tref = TRef, buffer_size = 0}) ->
    _ = emqx_utils:cancel_timer(TRef),
    State#st{flush_tref = undefined};
ensure_flush_timer(State) ->
    State.

add_to_buffer(#add_updates{} = Insert, State = #st{buffer = Buffer0, buffer_size = BufferSize}) ->
    #add_updates{
        tx_key = TxKey,
        key = Key,
        quota_index_opts = QuotaIndexOpts,
        updates = NewUpdates
    } = Insert,
    TxKeyBuffer0 = maps:get(TxKey, Buffer0, #{}),
    KeyBuffer0 = maps:get(Key, TxKeyBuffer0, #{
        quota_index_opts => QuotaIndexOpts,
        updates => []
    }),
    Updates0 = maps:get(updates, KeyBuffer0, []),
    Updates = lists:reverse(NewUpdates) ++ Updates0,
    KeyBuffer = KeyBuffer0#{updates := Updates, quota_index_opts := QuotaIndexOpts},
    TxKeyBuffer = TxKeyBuffer0#{Key => KeyBuffer},
    Buffer = Buffer0#{TxKey => TxKeyBuffer},
    State#st{buffer = Buffer, buffer_size = BufferSize + 1}.

do_flush(State = #st{buffer = Buffer}) ->
    ok = notify_queue_size(State),
    ok = maps:foreach(
        fun(TxKey, TxKeyBuffer) ->
            UpdatesByKey = lists:map(
                fun({Key, #{quota_index_opts := QuotaIndexOpts, updates := Updates}}) ->
                    {Key, QuotaIndexOpts, lists:reverse(Updates)}
                end,
                maps:to_list(TxKeyBuffer)
            ),
            flush_quota_index(State, TxKey, UpdatesByKey)
        end,
        Buffer
    ),
    State#st{buffer = #{}, buffer_size = 0}.

notify_queue_size(#st{worker_id = WorkerId, cbm = CBM}) ->
    {message_queue_len, InboxSize} = process_info(self(), message_queue_len),
    ok = CBM:quota_buffer_notify_queue_size(WorkerId, InboxSize).

buffer_max_size(#st{cbm = CBM}) ->
    CBM:quota_buffer_max_size().

buffer_flush_interval(#st{cbm = CBM}) ->
    CBM:quota_buffer_flush_interval().

quota_index_updates(OldMessageInfo, #{message := NewMessage, message_size := NewMessageSize}) ->
    %% Old record to delete from the index
    OldUpdates =
        case OldMessageInfo of
            undefined ->
                [];
            #{message := OldMessage, message_size := OldMessageSize} ->
                [
                    ?QUOTA_INDEX_UPDATE(
                        message_timestamp_us(OldMessage),
                        -OldMessageSize,
                        -1
                    )
                ]
        end,
    %% Add new record to the index
    NewUpdate = ?QUOTA_INDEX_UPDATE(
        message_timestamp_us(NewMessage),
        NewMessageSize,
        1
    ),
    OldUpdates ++ [NewUpdate].

message_timestamp_us(Message) ->
    Ms = emqx_message:timestamp(Message),
    erlang:convert_time_unit(Ms, millisecond, microsecond).

flush_quota_index(#st{cbm = CBM} = State, TxKey, UpdatesByKey) ->
    CBM:quota_buffer_flush_transaction(TxKey, fun() ->
        lists:foreach(
            fun({Key, QuotaIndexOpts, Updates}) ->
                tx_update_index(State, TxKey, Key, QuotaIndexOpts, Updates)
            end,
            UpdatesByKey
        )
    end).

tx_update_index(_State, _TxKey, _Key, _QuotaIndexOpts, []) ->
    ok;
tx_update_index(State, TxKey, Key, QuotaIndexOpts, Updates) ->
    Index0 =
        case tx_read_index(State, TxKey, Key, QuotaIndexOpts) of
            undefined ->
                IndexStartTsUs = lists:min(
                    lists:map(fun(?QUOTA_INDEX_UPDATE(TsUs, _, _)) -> TsUs end, Updates)
                ),
                emqx_mq_quota_index:new(
                    QuotaIndexOpts, IndexStartTsUs
                );
            Idx ->
                Idx
        end,
    Index = emqx_mq_quota_index:apply_updates(Index0, Updates),
    tx_write_index(State, TxKey, Key, Index).

tx_read_index(#st{cbm = CBM}, TxKey, Key, QuotaIndexOpts) ->
    CBM:quota_buffer_flush_tx_read(TxKey, Key, QuotaIndexOpts).

tx_write_index(#st{cbm = CBM}, TxKey, Key, Index) ->
    CBM:quota_buffer_flush_tx_write(TxKey, Key, Index).
