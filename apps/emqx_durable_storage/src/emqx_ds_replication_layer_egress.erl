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

-include("emqx_ds_replication_layer.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(via(DB, Shard), {via, gproc, {n, l, {?MODULE, DB, Shard}}}).
-define(flush, flush).

-record(enqueue_req, {message :: emqx_types:message(), sync :: boolean()}).

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
    lists:foreach(
        fun(Message) ->
            Shard = emqx_ds_replication_layer:shard_of_message(DB, Message, clientid),
            gen_server:call(?via(DB, Shard), #enqueue_req{message = Message, sync = Sync})
        end,
        Messages
    ).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s, {
    db :: emqx_ds:db(),
    shard :: emqx_ds_replication_layer:shard_id(),
    leader :: node(),
    n = 0 :: non_neg_integer(),
    tref :: reference(),
    batch = [] :: [emqx_types:message()],
    pending_replies = [] :: [gen_server:from()]
}).

init([DB, Shard]) ->
    process_flag(trap_exit, true),
    process_flag(message_queue_data, off_heap),
    %% TODO: adjust leader dynamically
    {ok, Leader} = emqx_ds_replication_layer_meta:shard_leader(DB, Shard),
    S = #s{
        db = DB,
        shard = Shard,
        leader = Leader,
        tref = start_timer()
    },
    {ok, S}.

handle_call(#enqueue_req{message = Msg, sync = Sync}, From, S) ->
    do_enqueue(From, Sync, Msg, S);
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

do_flush(S = #s{batch = []}) ->
    S#s{tref = start_timer()};
do_flush(
    S = #s{batch = Messages, pending_replies = Replies, db = DB, shard = Shard, leader = Leader}
) ->
    Batch = #{?tag => ?BATCH, ?batch_messages => lists:reverse(Messages)},
    ok = emqx_ds_proto_v2:store_batch(Leader, DB, Shard, Batch, #{}),
    [gen_server:reply(From, ok) || From <- lists:reverse(Replies)],
    ?tp(emqx_ds_replication_layer_egress_flush, #{db => DB, shard => Shard}),
    erlang:garbage_collect(),
    S#s{
        n = 0,
        batch = [],
        pending_replies = [],
        tref = start_timer()
    }.

do_enqueue(From, Sync, Msg, S0 = #s{n = N, batch = Batch, pending_replies = Replies}) ->
    NMax = application:get_env(emqx_durable_storage, egress_batch_size, 1000),
    S1 = S0#s{n = N + 1, batch = [Msg | Batch]},
    S2 =
        case N >= NMax of
            true ->
                _ = erlang:cancel_timer(S0#s.tref),
                do_flush(S1);
            false ->
                S1
        end,
    %% TODO: later we may want to delay the reply until the message is
    %% replicated, but it requies changes to the PUBACK/PUBREC flow to
    %% allow for async replies. For now, we ack when the message is
    %% _buffered_ rather than stored.
    %%
    %% Otherwise, the client would freeze for at least flush interval,
    %% or until the buffer is filled.
    S =
        case Sync of
            true ->
                S2#s{pending_replies = [From | Replies]};
            false ->
                gen_server:reply(From, ok),
                S2
        end,
    %% TODO: add a backpressure mechanism for the server to avoid
    %% building a long message queue.
    {noreply, S}.

start_timer() ->
    Interval = application:get_env(emqx_durable_storage, egress_flush_interval, 100),
    erlang:send_after(Interval, self(), ?flush).
