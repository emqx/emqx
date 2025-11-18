%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_message_quota_buffer).

-moduledoc """
This module is used for buffering quota updates for the message queues and flushing them to the DS periodically.
""".

-include("emqx_mq_internal.hrl").
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

-define(AUTO_RECONNECT_INTERVAL, 2).

-type update() :: emqx_mq_message_quota_index:update().

-record(add_updates, {
    quota_index_opts :: emqx_mq_message_quota_index:opts(),
    tx_key :: emqx_mq_quota:tx_key(),
    key :: emqx_mq_quota:key(),
    updates :: list(update())
}).
-record(flush, {}).

-record(st, {
    buffer :: #{
        emqx_mq_quota:tx_key() => #{
            emqx_mq_quota:key() => #{
                quota_index_opts := emqx_mq_message_quota_index:opts(),
                updates := list(update())
            }
        }
    },
    buffer_size :: non_neg_integer(),
    flush_tref :: reference() | undefined,
    worker_id :: term(),
    options :: emqx_mq_quota:options()
}).

-spec start(ecpool:pool_name(), emqx_mq_quota:options()) -> ok.
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

-spec stop(ecpool:pool_name()) -> ok.
stop(Name) ->
    ecpool:stop_sup_pool(Name).

-spec start_link(#{worker_id => term()}) -> {ok, pid()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

-spec add(
    ecpool:pool_name(),
    emqx_mq_quota:tx_key(),
    emqx_mq_quota:key(),
    emqx_mq_quota:index_opts(),
    emqx_maybe:option(emqx_mq_quota:message_update_info()),
    emqx_mq_quota:message_update_info()
) -> ok.
add(Name, TxKey, Key, QuotaIndexOpts, OldMessageInfo, NewMessageInfo) ->
    AddUpdates = #add_updates{
        tx_key = TxKey,
        key = Key,
        quota_index_opts = QuotaIndexOpts,
        updates = quota_index_updates(OldMessageInfo, NewMessageInfo)
    },
    PoolKey = pool_key(TxKey, Key),
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

init([#{worker_id := WorkerId} = Options]) ->
    erlang:process_flag(trap_exit, true),
    {ok, #st{
        buffer = #{},
        flush_tref = undefined,
        buffer_size = 0,
        worker_id = WorkerId,
        options = Options
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
        quota_index_opts := QuotaIndexOpts,
        updates := []
    }),
    Updates0 = maps:get(updates, KeyBuffer0, []),
    Updates = lists:reverse(NewUpdates) ++ Updates0,
    KeyBuffer = KeyBuffer0#{updates => Updates},
    TxKeyBuffer = TxKeyBuffer0#{Key => KeyBuffer},
    Buffer = Buffer0#{TxKey => TxKeyBuffer},
    State#st{buffer = Buffer, buffer_size = BufferSize + 1}.

do_flush(State = #st{buffer = Buffer}) ->
    % ok = update_metrics(State),
    ok = maps:foreach(
        fun(TxKey, TxKeyBuffer) ->
            UpdatesByKey = lists:map(
                fun(#{key := Key, quota_index_opts := QuotaIndexOpts, updates := Updates}) ->
                    {Key, QuotaIndexOpts, lists:reverse(Updates)}
                end,
                maps:values(TxKeyBuffer)
            ),
            flush_quota_index(State, TxKey, UpdatesByKey)
        end,
        Buffer
    ),
    State#st{buffer = #{}, buffer_size = 0}.

pool_key(Shard, Topic) ->
    {Shard, Topic}.

update_metrics(#st{worker_id = WorkerId}) ->
    {message_queue_len, InboxSize} = process_info(self(), message_queue_len),
    ok = emqx_mq_metrics:set_quota_buffer_inbox_size(WorkerId, InboxSize).

buffer_max_size(St) ->
    option_value(buffer_max_size, St).

buffer_flush_interval(St) ->
    FlushInterval = option_value(buffer_flush_interval, St),
    round(FlushInterval * (0.9 + 0.2 * rand:uniform())).

option_value(Key, #st{options = Options}) ->
    case Options of
        #{Key := ValueFn} when is_function(ValueFn, 0) ->
            ValueFn();
        #{Key := Value} ->
            Value
    end.

quota_index_updates(OldMessageInfo, #{message := NewMessage, message_size := NewMessageSize}) ->
    %% Old record to delete from the index
    OldUpdates =
        case OldMessage of
            undefined ->
                [];
            #{message := OldMessage, message_size := OldMessageSize} ->
                [
                    ?QUOTA_INDEX_UPDATE(
                        message_timestamp_us(OldMessage),
                        -byte_size(OldMessageBin),
                        -1
                    )
                ]
        end,
    %% Add new record to the index
    NewUpdate = ?QUOTA_INDEX_UPDATE(
        message_timestamp_us(Message),
        MessageSize,
        1
    ),
    OldUpdates ++ [NewUpdate].

message_timestamp_us(Message) ->
    Ms = emqx_message:timestamp(Message),
    erlang:convert_time_unit(Ms, millisecond, microsecond).

flush_quota_index(State, TxKey, UpdatesByKey) ->
    TxFun = option_value(tx_fun, State),
    % {Shard, Generation} = Slab,
    % TxBaseOpts = option_value(persist_base_tx_opts, State),
    % TxOpts = TxBaseOpts#{
    %     shard => Shard,
    %     generation => Generation,
    %     sync => true
    % },
    TxFun(TxKey, fun() ->
        lists:foreach(
            fun({Key, QuotaIndexOpts, Updates}) ->
                tx_update_index(State, TxKey, Key, QuotaIndexOpts, Updates)
            end,
            UpdatesByKey
        )
    end).
    % {Time, Result} = timer:tc(fun() -> emqx_ds:trans(TxOpts, TxFun) end),
    % case Result of
    %     {atomic, _Serial, ok} ->
    %         OnPersistOk = option_value(on_persist_ok, State),
    %         ok = OnPersistOk(Time);
    %     {error, IsRecoverable, Reason} ->
    %         Reason = {IsRecoverable, Reason},
    %         OnPersistError = option_value(on_persist_error, State),
    %         ok = OnPersistError(Reason),
    %         {error, Reason}
    % end.

tx_update_index(_State, _TxKey, _Key, _QuotaIndexOpts, []) ->
    ok;
tx_update_index(State, TxKey, Key, QuotaIndexOpts, Updates) ->
    Index0 =
        case tx_read_index(State, TxKey, Key, QuotaIndexOpts) of
            undefined ->
                IndexStartTsUs = lists:min(
                    lists:map(fun(?QUOTA_INDEX_UPDATE(TsUs, _, _)) -> TsUs end, Updates)
                ),
                emqx_mq_message_quota_index:new(
                    QuotaIndexOpts, IndexStartTsUs
                );
            Index ->
                Index
        end,
    Index1 = emqx_mq_message_quota_index:apply_updates(Index0, Updates),
    tx_write_index(Topic, Index1).

tx_read_index(State, TxKey, Key, QuotaIndexOpts) ->
    ReadFun = option_value(read_fun, State),
    case ReadFun(TxKey, Key) of
        undefined ->
            undefined;
        IndexBin ->
            emqx_mq_message_quota_index:decode(QuotaIndexOpts, IndexBin)
    end.

tx_write_index(State, TxKey, Key, Index) ->
    WriteFun = option_value(write_fun, State),
    WriteFun(TxKey, Key, emqx_mq_message_quota_index:encode(Index)).