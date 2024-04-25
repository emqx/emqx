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

%% @doc Egress servers are responsible for proxing the outcoming
%% `store_batch' requests towards EMQX DS shards.
%%
%% They re-assemble messages from different local processes into
%% fixed-sized batches, and introduce centralized channels between the
%% nodes. They are also responsible for maintaining backpressure
%% towards the local publishers.
%%
%% There is (currently) one egress process for each shard running on
%% each node, but it should be possible to have a pool of egress
%% servers, if needed.
-module(emqx_ds_replication_layer_egress).

-behaviour(gen_server).

%% API:
-export([start_link/2, store_batch/3]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([]).

-export_type([]).

-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(via(DB, Shard), {via, gproc, {n, l, {?MODULE, DB, Shard}}}).
-define(flush, flush).

-record(enqueue_req, {
    messages :: [emqx_types:message()],
    sync :: boolean(),
    atomic :: boolean(),
    n_messages :: non_neg_integer(),
    payload_bytes :: non_neg_integer()
}).

%%================================================================================
%% API functions
%%================================================================================

-spec start_link(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) -> {ok, pid()}.
start_link(DB, Shard) ->
    gen_server:start_link(?via(DB, Shard), ?MODULE, [DB, Shard], []).

-spec store_batch(emqx_ds:db(), [emqx_types:message()], emqx_ds:message_store_opts()) ->
    emqx_ds:store_batch_result().
store_batch(DB, Messages, Opts) ->
    Sync = maps:get(sync, Opts, true),
    Atomic = maps:get(atomic, Opts, false),
    %% Usually we expect all messages in the batch to go into the
    %% single shard, so this function is optimized for the happy case.
    case shards_of_batch(DB, Messages) of
        [{Shard, {NMsgs, NBytes}}] ->
            %% Happy case:
            enqueue_call_or_cast(
                ?via(DB, Shard),
                #enqueue_req{
                    messages = Messages,
                    sync = Sync,
                    atomic = Atomic,
                    n_messages = NMsgs,
                    payload_bytes = NBytes
                }
            );
        [_, _ | _] when Atomic ->
            %% It's impossible to commit a batch to multiple shards
            %% atomically
            {error, unrecoverable, atomic_commit_to_multiple_shards};
        _Shards ->
            %% Use a slower implementation for the unlikely case:
            repackage_messages(DB, Messages, Sync)
    end.

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s, {
    db :: emqx_ds:db(),
    shard :: emqx_ds_replication_layer:shard_id(),
    metrics_id :: emqx_ds_builtin_metrics:shard_metrics_id(),
    n_retries = 0 :: non_neg_integer(),
    %% FIXME: Currently max_retries is always 0, because replication
    %% layer doesn't guarantee idempotency. Retrying would create
    %% duplicate messages.
    max_retries = 0 :: non_neg_integer(),
    n = 0 :: non_neg_integer(),
    n_bytes = 0 :: non_neg_integer(),
    tref :: undefined | reference(),
    queue :: queue:queue(emqx_types:message()),
    pending_replies = [] :: [gen_server:from()]
}).

init([DB, Shard]) ->
    process_flag(trap_exit, true),
    process_flag(message_queue_data, off_heap),
    logger:update_process_metadata(#{domain => [emqx, ds, egress, DB]}),
    MetricsId = emqx_ds_builtin_metrics:shard_metric_id(DB, Shard),
    ok = emqx_ds_builtin_metrics:init_for_shard(MetricsId),
    S = #s{
        db = DB,
        shard = Shard,
        metrics_id = MetricsId,
        queue = queue:new()
    },
    {ok, S}.

handle_call(
    #enqueue_req{
        messages = Msgs,
        sync = Sync,
        atomic = Atomic,
        n_messages = NMsgs,
        payload_bytes = NBytes
    },
    From,
    S0 = #s{pending_replies = Replies0}
) ->
    S = S0#s{pending_replies = [From | Replies0]},
    {noreply, enqueue(Sync, Atomic, Msgs, NMsgs, NBytes, S)};
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(
    #enqueue_req{
        messages = Msgs,
        sync = Sync,
        atomic = Atomic,
        n_messages = NMsgs,
        payload_bytes = NBytes
    },
    S
) ->
    {noreply, enqueue(Sync, Atomic, Msgs, NMsgs, NBytes, S)};
handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(?flush, S) ->
    {noreply, flush(S)};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

enqueue(
    Sync,
    Atomic,
    Msgs,
    BatchSize,
    BatchBytes,
    S0 = #s{n = NMsgs0, n_bytes = NBytes0, queue = Q0}
) ->
    %% At this point we don't split the batches, even when they aren't
    %% atomic. It wouldn't win us anything in terms of memory, and
    %% EMQX currently feeds data to DS in very small batches, so
    %% granularity should be fine enough.
    NMax = application:get_env(emqx_durable_storage, egress_batch_size, 1000),
    NBytesMax = application:get_env(emqx_durable_storage, egress_batch_bytes, infinity),
    NMsgs = NMsgs0 + BatchSize,
    NBytes = NBytes0 + BatchBytes,
    case (NMsgs >= NMax orelse NBytes >= NBytesMax) andalso (NMsgs0 > 0) of
        true ->
            %% Adding this batch would cause buffer to overflow. Flush
            %% it now, and retry:
            S1 = flush(S0),
            enqueue(Sync, Atomic, Msgs, BatchSize, BatchBytes, S1);
        false ->
            %% The buffer is empty, we enqueue the atomic batch in its
            %% entirety:
            Q1 = lists:foldl(fun queue:in/2, Q0, Msgs),
            S1 = S0#s{n = NMsgs, n_bytes = NBytes, queue = Q1},
            case NMsgs >= NMax orelse NBytes >= NBytesMax of
                true ->
                    flush(S1);
                false ->
                    ensure_timer(S1)
            end
    end.

-define(COOLDOWN_MIN, 1000).
-define(COOLDOWN_MAX, 5000).

flush(S) ->
    do_flush(cancel_timer(S)).

do_flush(S0 = #s{n = 0}) ->
    S0;
do_flush(
    S = #s{
        queue = Q,
        pending_replies = Replies,
        db = DB,
        shard = Shard,
        metrics_id = Metrics,
        n_retries = Retries,
        max_retries = MaxRetries
    }
) ->
    Messages = queue:to_list(Q),
    T0 = erlang:monotonic_time(microsecond),
    Result = emqx_ds_replication_layer:ra_store_batch(DB, Shard, Messages),
    T1 = erlang:monotonic_time(microsecond),
    emqx_ds_builtin_metrics:observe_egress_flush_time(Metrics, T1 - T0),
    case Result of
        ok ->
            emqx_ds_builtin_metrics:inc_egress_batches(Metrics),
            emqx_ds_builtin_metrics:inc_egress_messages(Metrics, S#s.n),
            emqx_ds_builtin_metrics:inc_egress_bytes(Metrics, S#s.n_bytes),
            ?tp(
                emqx_ds_replication_layer_egress_flush,
                #{db => DB, shard => Shard, batch => Messages}
            ),
            lists:foreach(fun(From) -> gen_server:reply(From, ok) end, Replies),
            erlang:garbage_collect(),
            S#s{
                n = 0,
                n_bytes = 0,
                queue = queue:new(),
                pending_replies = []
            };
        {timeout, ServerId} when Retries < MaxRetries ->
            %% Note: this is a hot loop, so we report error messages
            %% with `debug' level to avoid wiping the logs. Instead,
            %% error the detection must rely on the metrics. Debug
            %% logging can be enabled for the particular egress server
            %% via logger domain.
            ?tp(
                debug,
                emqx_ds_replication_layer_egress_flush_retry,
                #{db => DB, shard => Shard, reason => timeout, server_id => ServerId}
            ),
            %% Retry sending the batch:
            emqx_ds_builtin_metrics:inc_egress_batches_retry(Metrics),
            erlang:garbage_collect(),
            %% We block the gen_server until the next retry.
            BlockTime = ?COOLDOWN_MIN + rand:uniform(?COOLDOWN_MAX - ?COOLDOWN_MIN),
            timer:sleep(BlockTime),
            S#s{n_retries = Retries + 1};
        Err ->
            ?tp(
                debug,
                emqx_ds_replication_layer_egress_flush_failed,
                #{db => DB, shard => Shard, error => Err}
            ),
            emqx_ds_builtin_metrics:inc_egress_batches_failed(Metrics),
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
                n_retries = 0
            }
    end.

-spec shards_of_batch(emqx_ds:db(), [emqx_types:message()]) ->
    [{emqx_ds_replication_layer:shard_id(), {NMessages, NBytes}}]
when
    NMessages :: non_neg_integer(),
    NBytes :: non_neg_integer().
shards_of_batch(DB, Messages) ->
    maps:to_list(
        lists:foldl(
            fun(Message, Acc) ->
                %% TODO: sharding strategy must be part of the DS DB schema:
                Shard = emqx_ds_replication_layer:shard_of_message(DB, Message, clientid),
                Size = payload_size(Message),
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
            Messages
        )
    ).

repackage_messages(DB, Messages, Sync) ->
    Batches = lists:foldl(
        fun(Message, Acc) ->
            Shard = emqx_ds_replication_layer:shard_of_message(DB, Message, clientid),
            Size = payload_size(Message),
            maps:update_with(
                Shard,
                fun({N, S, Msgs}) ->
                    {N + 1, S + Size, [Message | Msgs]}
                end,
                {1, Size, [Message]},
                Acc
            )
        end,
        #{},
        Messages
    ),
    maps:fold(
        fun(Shard, {NMsgs, ByteSize, RevMessages}, ErrAcc) ->
            Err = enqueue_call_or_cast(
                ?via(DB, Shard),
                #enqueue_req{
                    messages = lists:reverse(RevMessages),
                    sync = Sync,
                    atomic = false,
                    n_messages = NMsgs,
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
    Interval = application:get_env(emqx_durable_storage, egress_flush_interval, 100),
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
    size(P) + size(T).
