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
-module(emqx_ds_beamformer).

-behavior(gen_server).

%% API:
-export([]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([start_link/2]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

-record(req_poll, {
                   node :: node(),
                   it :: emqx_ds_storage_layer:iterator(),
                   opts :: emqx_ds:poll_opts(),
                   deadline :: integer()
}).

%%================================================================================
%% API functions
%%================================================================================

-spec start_link(emqx_ds_storage_layer:shard_id(), integer()) -> {ok, pid()}.
start_link(ShardId, Name) ->
    gen_server:start_link(?MODULE, [ShardId, Name], []).

-spec poll(node(), emqx_ds_beam:return_addr(_ItKey), emqx_ds_storage_layer:shard_id(), emqx_ds_storage_layer:iterator(), emqx_ds:poll_opts()) -> ok.
poll(Node, ReturnAddr, Shard, Iterator, Opts = #{timeout := Timeout}) ->
    Req = #req_poll{ node = Node,
                     it = Iterator,
                     opts = Opts,
                     deadline = erlang:monotonic_time(millisecond) + Timeout
                   },
    Worker = gproc_pool:pick_worker(emqx_ds_beamformer_sup:pool(Shard), pick(Iterator)),
    %% Using call to enqueue request for backpressure.
    gen_server:call(Worker, Req, Timeout).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s, {
    shard,
    name,
    pending
}).

-record(pending, {
    key, iterator, node, opts, deadline
}).

init([ShardId, Name]) ->
    process_flag(trap_exit, true),
    Pool = emqx_ds_pollers:pool(ShardId),
    gproc_pool:add_worker(Pool, Name),
    gproc_pool:connect_worker(Pool, Name),
    Tab = ets:new(pending_polls, [duplicate_bag, private, {keypos, #pending.key}]),
    S = #s{shard = ShardId, name = N, pending = Tab},
    timer:send_interval(50, doit),
    {ok, S}.

handle_call(#req_poll{node = Node, it = It, opts = Opts, deadline = Deadline}, _From, S) ->
    %% TODO: drop requests past deadline immediately:
    {Stream, NextKey, _Timestamp} = emqx_ds_storage_layer:unpack_iterator(It),
    Pending = #pending{
                 key = {Stream, NextKey},
                 iterator = It,
                 node = Node,
                 opts = Opts,
                 deadline = Deadline
                },
    ets:insert(S#s.pending, Pending),
    {reply, ok, S};
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(doit, S = #s{shard = Shard, pending = Pending}) ->
    fulfill_pending(Shard, Pending),
    {noreply, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, #s{shard = ShardId, name = Name}) ->
    Pool = emqx_ds_pollers:pool(ShardId),
    gproc_pool:disconnect_worker(Pool, Name),
    gproc_pool:remove_worker(Pool, Name),
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

fulfill_pending(Shard, Pending) ->
    case ets:first(Pending) of
        '$end_of_table' ->
            ok;
        {Stream, MsgKey} ->
            %% The function MUST destructively consume all requests
            %% matching stream and MsgKey to avoid infinite loop:
            fulfill_pending(Shard, Pending, Stream, MsgKey),
            fulfill_pending(Shard, Pending)
    end.

fulfill_pending(Shard, Pending, Stream, MsgKey) ->
    Batch = emqx_ds_storage_layer:stream_scan(Shard, Stream, MsgKey, 100).

getter(PendingTab, Stream) ->
    fun(MsgKey) ->
            Pendings = ets:take(PendingTab, {Stream, MsgKey}),
            [ || #pending{iterator = It0, node = Node} <- Pendings]
    end.

pick(Iterator) ->
    {Stream, _Key, Timestamp} = emqx_ds_storage_layer:unpack_iterator(Iterator),
    %% Try to maximize likelyhood of sending similar iterators to the
    %% same worker:
    {Stream, Timestamp div 10_000}.
