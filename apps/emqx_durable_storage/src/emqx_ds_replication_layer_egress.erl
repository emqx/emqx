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

-record(enqueue_req, {message :: emqx_types:message(), sync :: boolean()}).
-record(enqueue_atomic_req, {batch :: [emqx_types:message()], sync :: boolean()}).

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
    case maps:get(atomic, Opts, false) of
        false ->
            lists:foreach(
                fun(Message) ->
                    Shard = emqx_ds_replication_layer:shard_of_message(DB, Message, clientid),
                    gen_server:call(
                        ?via(DB, Shard),
                        #enqueue_req{
                            message = Message,
                            sync = Sync
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
                            sync = Sync
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
    batch = [] :: [emqx_types:message()],
    pending_replies = [] :: [gen_server:from()]
}).

init([DB, Shard]) ->
    process_flag(trap_exit, true),
    process_flag(message_queue_data, off_heap),
    S = #s{
        db = DB,
        shard = Shard,
        tref = start_timer()
    },
    {ok, S}.

handle_call(#enqueue_req{message = Msg, sync = Sync}, From, S) ->
    do_enqueue(From, Sync, Msg, S);
handle_call(#enqueue_atomic_req{batch = Batch, sync = Sync}, From, S) ->
    Len = length(Batch),
    do_enqueue(From, Sync, {atomic, Len, Batch}, S);
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

do_flush(S = #s{batch = []}) ->
    S#s{tref = start_timer()};
do_flush(
    S = #s{batch = Messages, pending_replies = Replies, db = DB, shard = Shard}
) ->
    case emqx_ds_replication_layer:ra_store_batch(DB, Shard, lists:reverse(Messages)) of
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
    S#s{
        n = 0,
        batch = [],
        pending_replies = [],
        tref = start_timer()
    }.

do_enqueue(From, Sync, MsgOrBatch, S0 = #s{n = N, batch = Batch, pending_replies = Replies}) ->
    NMax = application:get_env(emqx_durable_storage, egress_batch_size, 1000),
    S1 =
        case MsgOrBatch of
            {atomic, NumMsgs, Msgs} ->
                S0#s{n = N + NumMsgs, batch = Msgs ++ Batch};
            Msg ->
                S0#s{n = N + 1, batch = [Msg | Batch]}
        end,
    %% TODO: later we may want to delay the reply until the message is
    %% replicated, but it requies changes to the PUBACK/PUBREC flow to
    %% allow for async replies. For now, we ack when the message is
    %% _buffered_ rather than stored.
    %%
    %% Otherwise, the client would freeze for at least flush interval,
    %% or until the buffer is filled.
    S2 =
        case Sync of
            true ->
                S1#s{pending_replies = [From | Replies]};
            false ->
                gen_server:reply(From, ok),
                S1
        end,
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
