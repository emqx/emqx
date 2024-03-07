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

-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(via(DB, Shard), {via, gproc, {n, l, {?MODULE, DB, Shard}}}).
-define(flush, flush).

-type message() :: emqx_types:message().

-record(enqueue_req, {
    message :: message(),
    sync :: boolean(),
    auto_assign_timestamps :: boolean()
}).
-record(enqueue_atomic_req, {
    batch :: [message()],
    sync :: boolean(),
    auto_assign_timestamps :: boolean()
}).

-type bucket() :: auto_ts | no_auto_ts.
-type store_opts() :: #{
    auto_assign_timestamps := boolean()
}.
-type msg_or_atomic_batch() :: {atomic, pos_integer(), [message()]} | message().

%%================================================================================
%% API functions
%%================================================================================

-spec start_link(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) -> {ok, pid()}.
start_link(DB, Shard) ->
    gen_server:start_link(?via(DB, Shard), ?MODULE, [DB, Shard], []).

-spec store_batch(emqx_ds:db(), [emqx_types:message()], emqx_ds:message_store_opts()) ->
    ok.
store_batch(DB, Messages, Opts) ->
    Sync = maps:get(sync, Opts, true),
    AutoAssignTimestamps = maps:get(auto_assign_timestamps, Opts, true),
    case maps:get(atomic, Opts, false) of
        false ->
            lists:foreach(
                fun(Message) ->
                    Shard = emqx_ds_replication_layer:shard_of_message(DB, Message, clientid),
                    gen_server:call(
                        ?via(DB, Shard),
                        #enqueue_req{
                            message = Message,
                            sync = Sync,
                            auto_assign_timestamps = AutoAssignTimestamps
                        },
                        infinity
                    )
                end,
                Messages
            );
        true ->
            maps:foreach(
                fun(Shard, Batch) ->
                    gen_server:call(
                        ?via(DB, Shard),
                        #enqueue_atomic_req{
                            batch = Batch,
                            sync = Sync,
                            auto_assign_timestamps = AutoAssignTimestamps
                        },
                        infinity
                    )
                end,
                maps:groups_from_list(
                    fun(Message) ->
                        emqx_ds_replication_layer:shard_of_message(DB, Message, clientid)
                    end,
                    Messages
                )
            )
    end.

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s, {
    db :: emqx_ds:db(),
    shard :: emqx_ds_replication_layer:shard_id(),
    n = 0 :: non_neg_integer(),
    tref :: reference(),
    buckets = #{} :: #{
        _Bucket ::
            term() =>
                #{
                    msgs := [emqx_types:message()],
                    pending_replies := [gen_server:from()],
                    n := non_neg_integer()
                }
    }
}).
-type s() :: #s{}.

init([DB, Shard]) ->
    process_flag(trap_exit, true),
    process_flag(message_queue_data, off_heap),
    S = #s{
        db = DB,
        shard = Shard,
        tref = start_timer()
    },
    {ok, S}.

handle_call(#enqueue_req{message = Msg, sync = Sync, auto_assign_timestamps = AutoTS}, From, S) ->
    do_enqueue(From, Sync, AutoTS, Msg, S);
handle_call(
    #enqueue_atomic_req{batch = Batch, sync = Sync, auto_assign_timestamps = AutoTS}, From, S
) ->
    Len = length(Batch),
    do_enqueue(From, Sync, AutoTS, {atomic, Len, Batch}, S);
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(?flush, S) ->
    {noreply, do_flush(S)};
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

-define(COOLDOWN_MIN, 1000).
-define(COOLDOWN_MAX, 5000).

do_flush(S = #s{n = 0}) ->
    S#s{tref = start_timer()};
do_flush(S = #s{buckets = Buckets}) ->
    do_flush1(maps:to_list(Buckets), S).

do_flush1([] = _Buckets, S) ->
    S#s{
        n = 0,
        buckets = #{},
        tref = start_timer()
    };
do_flush1(
    [{Bucket, #{msgs := Messages, pending_replies := Replies}} | Rest],
    S = #s{db = DB, shard = Shard}
) ->
    StoreOpts = bucket_to_store_opts(Bucket),
    case emqx_ds_replication_layer:ra_store_batch(DB, Shard, lists:reverse(Messages), StoreOpts) of
        ok ->
            ?tp(
                emqx_ds_replication_layer_egress_flush,
                #{db => DB, shard => Shard, batch => Messages}
            ),
            lists:foreach(fun(From) -> gen_server:reply(From, ok) end, Replies),
            true = erlang:garbage_collect(),
            ok;
        Error ->
            true = erlang:garbage_collect(),
            ?tp(
                warning,
                emqx_ds_replication_layer_egress_flush_failed,
                #{db => DB, shard => Shard, reason => Error}
            ),
            Cooldown = ?COOLDOWN_MIN + rand:uniform(?COOLDOWN_MAX - ?COOLDOWN_MIN),
            ok = timer:sleep(Cooldown),
            %% Since we drop the entire batch here, we at least reply callers with an
            %% error so they don't hang indefinitely in the `gen_server' call with
            %% `infinity' timeout.
            lists:foreach(fun(From) -> gen_server:reply(From, {error, Error}) end, Replies)
    end,
    do_flush1(Rest, S).

do_enqueue(From, Sync, AutoTS, MsgOrBatch, S0 = #s{n = N}) ->
    NMax = application:get_env(emqx_durable_storage, egress_batch_size, 1000),
    Bucket = bucket(AutoTS),
    S1 = add_to_bucket(Bucket, MsgOrBatch, S0),
    %% TODO: later we may want to delay the reply until the message is
    %% replicated, but it requies changes to the PUBACK/PUBREC flow to
    %% allow for async replies. For now, we ack when the message is
    %% _buffered_ rather than stored.
    %%
    %% Otherwise, the client would freeze for at least flush interval,
    %% or until the buffer is filled.
    S2 = add_pending_or_reply(Bucket, Sync, From, S1),
    S =
        case N >= NMax of
            true ->
                _ = erlang:cancel_timer(S2#s.tref),
                do_flush(S2);
            false ->
                S2
        end,
    %% TODO: add a backpressure mechanism for the server to avoid
    %% building a long message queue.
    {noreply, S}.

start_timer() ->
    Interval = application:get_env(emqx_durable_storage, egress_flush_interval, 100),
    erlang:send_after(Interval, self(), ?flush).

-spec bucket(boolean()) -> bucket().
bucket(AutoTS) ->
    case AutoTS of
        true -> auto_ts;
        false -> no_auto_ts
    end.

-spec bucket_to_store_opts(bucket()) -> store_opts().
bucket_to_store_opts(auto_ts) ->
    #{auto_assign_timestamps => true};
bucket_to_store_opts(no_auto_ts) ->
    #{auto_assign_timestamps => false}.

-spec add_to_bucket(bucket(), msg_or_atomic_batch(), s()) -> s().
add_to_bucket(Bucket, {atomic, NumMsgs, Msgs}, S0 = #s{n = N0, buckets = Buckets0}) ->
    Buckets =
        maps:update_with(
            Bucket,
            fun(#{msgs := Msgs0, n := M0} = Previous) ->
                Previous#{
                    msgs := Msgs ++ Msgs0,
                    n := M0 + NumMsgs
                }
            end,
            #{msgs => Msgs, n => NumMsgs, pending_replies => []},
            Buckets0
        ),
    S0#s{n = N0 + NumMsgs, buckets = Buckets};
add_to_bucket(Bucket, Msg, S0 = #s{n = N0, buckets = Buckets0}) ->
    Buckets =
        maps:update_with(
            Bucket,
            fun(#{msgs := Msgs0, n := M0} = Previous) ->
                Previous#{
                    msgs := [Msg | Msgs0],
                    n := M0 + 1
                }
            end,
            #{msgs => [Msg], n => 1, pending_replies => []},
            Buckets0
        ),
    S0#s{n = N0 + 1, buckets = Buckets}.

-spec add_pending_or_reply(bucket(), boolean(), gen_server:from(), s()) -> s().
add_pending_or_reply(_Bucket, _Sync = false, From, S) ->
    gen_server:reply(From, ok),
    S;
add_pending_or_reply(Bucket, _Sync = true, From, S0) ->
    #s{buckets = Buckets0} = S0,
    Buckets =
        maps:update_with(
            Bucket,
            fun(Previous = #{pending_replies := Replies}) ->
                Previous#{pending_replies := [From | Replies]}
            end,
            Buckets0
        ),
    S0#s{buckets = Buckets}.
