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

%% @doc Buffer servers are responsible for collecting batches from the
%% local processes, sharding and repackaging them.
-module(emqx_ds_buffer).
-feature(maybe_expr, enable).

-behaviour(gen_server).

%% API:
-export([start_link/4, store_batch/3, shard_of_operation/3]).
-export([ls/0]).

%% behavior callbacks:
-export([init/1, format_status/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([]).

-export_type([]).

-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("typerefl/include/types.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type size_limit() :: pos_integer() | infinity.
-reflect_type([size_limit/0]).

-define(name(DB, SHARD), {n, l, {?MODULE, DB, SHARD}}).
-define(via(DB, SHARD), {via, gproc, ?name(DB, SHARD)}).
-define(flush, flush).

-define(cbm(DB), {?MODULE, DB}).

-record(enqueue_req, {
    operations :: [emqx_ds:operation()],
    sync :: boolean(),
    n_operations :: non_neg_integer(),
    payload_bytes :: non_neg_integer()
}).

-callback init_buffer(emqx_ds:db(), _Shard, _Options) -> {ok, _State}.

-callback flush_buffer(emqx_ds:db(), _Shard, [emqx_ds:operation()], State) ->
    {State, ok | {error, recoverable | unrecoverable, _}}.

-callback shard_of_operation(emqx_ds:db(), emqx_ds:operation(), topic | clientid, _Options) ->
    _Shard.

-callback buffer_config
    (emqx_ds:db(), _Shard, _State, batch_size | batch_bytes) ->
        {ok, size_limit()} | undefined;
    (emqx_ds:db(), _Shard, _State, flush_interval) ->
        {ok, pos_integer()} | undefined.

-optional_callbacks([buffer_config/4]).

%%================================================================================
%% API functions
%%================================================================================

-spec ls() -> [{emqx_ds:db(), _Shard}].
ls() ->
    MS = {{?name('$1', '$2'), '_', '_'}, [], [{{'$1', '$2'}}]},
    gproc:select({local, names}, [MS]).

-spec start_link(module(), _CallbackOptions, emqx_ds:db(), _ShardId) ->
    {ok, pid()}.
start_link(CallbackModule, CallbackOptions, DB, Shard) ->
    gen_server:start_link(
        ?via(DB, Shard), ?MODULE, [CallbackModule, CallbackOptions, DB, Shard], []
    ).

-spec store_batch(emqx_ds:db(), [emqx_ds:operation()], emqx_ds:message_store_opts()) ->
    emqx_ds:store_batch_result().
store_batch(DB, Operations, Opts) ->
    Sync = maps:get(sync, Opts, true),
    %% Usually we expect all messages in the batch to go into the
    %% single shard, so this function is optimized for the happy case.
    case shards_of_batch(DB, Operations) of
        [{Shard, {NOps, NBytes}}] ->
            %% Happy case:
            enqueue_call_or_cast(
                ?via(DB, Shard),
                #enqueue_req{
                    operations = Operations,
                    sync = Sync,
                    n_operations = NOps,
                    payload_bytes = NBytes
                }
            );
        _Shards ->
            %% Use a slower implementation for the unlikely case:
            repackage_messages(DB, Operations, Sync)
    end.

-spec shard_of_operation(emqx_ds:db(), emqx_ds:operation(), clientid | topic) -> _Shard.
shard_of_operation(DB, Operation, ShardBy) ->
    {CBM, Options} = persistent_term:get(?cbm(DB)),
    CBM:shard_of_operation(DB, Operation, ShardBy, Options).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s, {
    callback_module :: module(),
    callback_state :: term(),
    db :: emqx_ds:db(),
    shard :: _ShardId,
    metrics_id :: emqx_ds_builtin_metrics:shard_metrics_id(),
    n_retries = 0 :: non_neg_integer(),
    %% FIXME: Currently max_retries is always 0, because replication
    %% layer doesn't guarantee idempotency. Retrying would create
    %% duplicate messages.
    max_retries = 0 :: non_neg_integer(),
    n = 0 :: non_neg_integer(),
    n_bytes = 0 :: non_neg_integer(),
    tref :: undefined | reference(),
    queue :: queue:queue(emqx_ds:operation()),
    pending_replies = [] :: [gen_server:from()],
    oldest_message_timestamp :: integer() | undefined
}).

init([CBM, CBMOptions, DB, Shard]) ->
    process_flag(trap_exit, true),
    process_flag(message_queue_data, off_heap),
    logger:update_process_metadata(#{domain => [emqx, ds, buffer, DB]}),
    MetricsId = emqx_ds_builtin_metrics:shard_metric_id(DB, Shard),
    ok = emqx_ds_builtin_metrics:init_for_shard(MetricsId),
    {ok, CallbackS} = CBM:init_buffer(DB, Shard, CBMOptions),
    S = #s{
        callback_module = CBM,
        callback_state = CallbackS,
        db = DB,
        shard = Shard,
        metrics_id = MetricsId,
        queue = queue:new()
    },
    persistent_term:put(?cbm(DB), {CBM, CBMOptions}),
    {ok, S}.

format_status(Status) ->
    maps:map(
        fun
            (state, #s{db = DB, shard = Shard, queue = Q}) ->
                #{
                    db => DB,
                    shard => Shard,
                    queue => queue:len(Q)
                };
            (_, Val) ->
                Val
        end,
        Status
    ).

handle_call(
    #enqueue_req{
        operations = Operations,
        sync = Sync,
        n_operations = NOps,
        payload_bytes = NBytes
    },
    From,
    S0 = #s{pending_replies = Replies0}
) ->
    S = S0#s{pending_replies = [From | Replies0]},
    {noreply, enqueue(Sync, Operations, NOps, NBytes, S)};
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(
    #enqueue_req{
        operations = Operations,
        sync = Sync,
        n_operations = NOps,
        payload_bytes = NBytes
    },
    S
) ->
    {noreply, enqueue(Sync, Operations, NOps, NBytes, S)};
handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(?flush, S) ->
    {noreply, flush(S)};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, S = #s{db = DB}) ->
    _ = flush(S),
    persistent_term:erase(?cbm(DB)),
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

enqueue(
    Sync,
    Ops,
    BatchSize,
    BatchBytes,
    S0 = #s{n = NOps0, n_bytes = NBytes0, queue = Q0, oldest_message_timestamp = OldestTS0}
) ->
    %% At this point we don't split the batches, even when they aren't
    %% atomic. It wouldn't win us anything in terms of memory, and
    %% EMQX currently feeds data to DS in very small batches, so
    %% granularity should be fine enough.
    NMax = get_config(S0, batch_size),
    NBytesMax = get_config(S0, batch_bytes),
    NMsgs = NOps0 + BatchSize,
    NBytes = NBytes0 + BatchBytes,
    case (NMsgs >= NMax orelse NBytes >= NBytesMax) andalso (NOps0 > 0) of
        true ->
            %% Adding this batch would cause buffer to overflow. Flush
            %% it now, and retry:
            S1 = flush(S0),
            enqueue(Sync, Ops, BatchSize, BatchBytes, S1);
        false ->
            %% The buffer is empty, we enqueue the atomic batch in its
            %% entirety:
            {Q1, OldestTS} = lists:foldl(fun enqueue_op/2, {Q0, OldestTS0}, Ops),
            S1 = S0#s{n = NMsgs, n_bytes = NBytes, queue = Q1, oldest_message_timestamp = OldestTS},
            case NMsgs >= NMax orelse NBytes >= NBytesMax of
                true ->
                    flush(S1);
                false ->
                    ensure_timer(S1)
            end
    end.

enqueue_op(Msg = #message{timestamp = TS}, {Q, OldestTS}) ->
    {
        queue:in(Msg, Q),
        min(OldestTS, TS)
    };
enqueue_op(Op, {Q, OldestTS}) ->
    {
        queue:in(Op, Q),
        OldestTS
    }.

-define(COOLDOWN_MIN, 1000).
-define(COOLDOWN_MAX, 5000).

flush(S) ->
    do_flush(cancel_timer(S)).

do_flush(S0 = #s{n = 0}) ->
    S0;
do_flush(
    S0 = #s{
        callback_module = CBM,
        callback_state = CallbackS0,
        queue = Q,
        pending_replies = Replies,
        db = DB,
        shard = Shard,
        metrics_id = Metrics,
        n_retries = Retries,
        max_retries = MaxRetries,
        oldest_message_timestamp = OTS
    }
) ->
    Messages = queue:to_list(Q),
    T0 = erlang:monotonic_time(microsecond),
    {CallbackS, Result} = CBM:flush_buffer(DB, Shard, Messages, CallbackS0),
    S = S0#s{callback_state = CallbackS},
    T1 = erlang:monotonic_time(microsecond),
    emqx_ds_builtin_metrics:observe_buffer_flush_time(Metrics, T1 - T0),
    case Result of
        ok ->
            %% Report metrics and events:
            emqx_ds_builtin_metrics:inc_buffer_batches(Metrics),
            emqx_ds_builtin_metrics:inc_buffer_messages(Metrics, S#s.n),
            emqx_ds_builtin_metrics:inc_buffer_bytes(Metrics, S#s.n_bytes),
            case is_integer(OTS) of
                true ->
                    Latency = erlang:system_time(millisecond) - OTS,
                    emqx_ds_builtin_metrics:observe_buffer_latency(Metrics, Latency);
                false ->
                    ok
            end,
            ?tp(
                emqx_ds_buffer_flush,
                #{db => DB, shard => Shard, batch => Messages}
            ),
            %% Unblock clients:
            lists:foreach(fun(From) -> gen_server:reply(From, ok) end, Replies),
            erlang:garbage_collect(),
            S#s{
                n = 0,
                n_bytes = 0,
                queue = queue:new(),
                pending_replies = [],
                oldest_message_timestamp = undefined
            };
        {error, recoverable, Err} when Retries < MaxRetries ->
            %% Note: this is a hot loop, so we report error messages
            %% with `debug' level to avoid wiping the logs. Instead,
            %% error the detection must rely on the metrics. Debug
            %% logging can be enabled for the particular egress server
            %% via logger domain.
            ?tp(
                debug,
                emqx_ds_buffer_flush_retry,
                #{db => DB, shard => Shard, reason => Err}
            ),
            %% Retry sending the batch:
            emqx_ds_builtin_metrics:inc_buffer_batches_retry(Metrics),
            erlang:garbage_collect(),
            %% We block the gen_server until the next retry.
            BlockTime = ?COOLDOWN_MIN + rand:uniform(?COOLDOWN_MAX - ?COOLDOWN_MIN),
            timer:sleep(BlockTime),
            S#s{n_retries = Retries + 1};
        Err ->
            ?tp(
                debug,
                emqx_ds_buffer_flush_failed,
                #{db => DB, shard => Shard, batch => Messages, error => Err}
            ),
            emqx_ds_builtin_metrics:inc_buffer_batches_failed(Metrics),
            Reply =
                case Err of
                    {error, _, _} -> Err;
                    {timeout, ServerId} -> {error, recoverable, {timeout, ServerId}};
                    _ -> {error, unrecoverable, Err}
                end,
            lists:foreach(
                fun(From) -> gen_server:reply(From, Reply) end, Replies
            ),
            erlang:garbage_collect(),
            S#s{
                n = 0,
                n_bytes = 0,
                queue = queue:new(),
                pending_replies = [],
                n_retries = 0,
                oldest_message_timestamp = undefined
            }
    end.

-spec shards_of_batch(emqx_ds:db(), [emqx_ds:operation()]) ->
    [{_ShardId, {NMessages, NBytes}}]
when
    NMessages :: non_neg_integer(),
    NBytes :: non_neg_integer().
shards_of_batch(DB, Batch) ->
    maps:to_list(
        lists:foldl(
            fun(Operation, Acc) ->
                %% TODO: sharding strategy must be part of the DS DB schema:
                Shard = shard_of_operation(DB, Operation, clientid),
                Size = payload_size(Operation),
                maps:update_with(
                    Shard,
                    fun({N, S}) ->
                        {N + 1, S + Size}
                    end,
                    {1, Size},
                    Acc
                )
            end,
            #{},
            Batch
        )
    ).

repackage_messages(DB, Batch, Sync) ->
    Batches = lists:foldl(
        fun(Operation, Acc) ->
            Shard = shard_of_operation(DB, Operation, clientid),
            Size = payload_size(Operation),
            maps:update_with(
                Shard,
                fun({N, S, Msgs}) ->
                    {N + 1, S + Size, [Operation | Msgs]}
                end,
                {1, Size, [Operation]},
                Acc
            )
        end,
        #{},
        Batch
    ),
    maps:fold(
        fun(Shard, {NOps, ByteSize, RevOperations}, ErrAcc) ->
            Err = enqueue_call_or_cast(
                ?via(DB, Shard),
                #enqueue_req{
                    operations = lists:reverse(RevOperations),
                    sync = Sync,
                    n_operations = NOps,
                    payload_bytes = ByteSize
                }
            ),
            compose_errors(ErrAcc, Err)
        end,
        ok,
        Batches
    ).

enqueue_call_or_cast(To, Req = #enqueue_req{sync = true}) ->
    gen_server:call(To, Req, infinity);
enqueue_call_or_cast(To, Req = #enqueue_req{sync = false}) ->
    gen_server:cast(To, Req).

compose_errors(ErrAcc, ok) ->
    ErrAcc;
compose_errors(ok, Err) ->
    Err;
compose_errors({error, recoverable, _}, {error, unrecoverable, Err}) ->
    {error, unrecoverable, Err};
compose_errors(ErrAcc, _Err) ->
    ErrAcc.

ensure_timer(S = #s{tref = undefined}) ->
    Interval = get_config(S, flush_interval),
    Tref = erlang:send_after(Interval, self(), ?flush),
    S#s{tref = Tref};
ensure_timer(S) ->
    S.

cancel_timer(S = #s{tref = undefined}) ->
    S;
cancel_timer(S = #s{tref = TRef}) ->
    _ = erlang:cancel_timer(TRef),
    S#s{tref = undefined}.

%% @doc Return approximate size of the MQTT message (it doesn't take
%% all things into account, for example headers and extras)
payload_size(#message{payload = P, topic = T}) ->
    size(P) + size(T);
payload_size({_OpName, _}) ->
    0.

get_config(#s{db = DB, callback_module = Mod, callback_state = State, shard = Shard}, Item) ->
    maybe
        true ?= erlang:function_exported(Mod, buffer_config, 4),
        {ok, Val} ?= Mod:buffer_config(DB, Shard, State, Item),
        Val
    else
        _ ->
            case Item of
                batch_size ->
                    application:get_env(emqx_durable_storage, egress_batch_size, 1000);
                batch_bytes ->
                    application:get_env(emqx_durable_storage, egress_batch_bytes, infinity);
                flush_interval ->
                    application:get_env(emqx_durable_storage, egress_flush_interval, 100)
            end
    end.
