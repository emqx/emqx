%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This type of beamformer worker takes care of fulfilling the
%% poll requests on committed data.
-module(emqx_ds_beamformer_catchup).

-behavior(gen_server).

%% API:
-export([start_link/4, enqueue/3]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([]).

-export_type([]).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-include("emqx_ds_beamformer.hrl").
-include("emqx_ds.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-record(s, {
    module :: module(),
    metrics_id,
    shard,
    name,
    queue :: ets:tid(),
    pending_request_limit :: non_neg_integer(),
    batch_size :: non_neg_integer()
}).

-type s() :: #s{}.

-define(fulfill_loop, beamformer_fulfill_loop).
-define(housekeeping_loop, housekeeping_loop).

%% Type of the key in the queue table:
-type queue_key() :: {_Stream, emqx_ds_beamformer:event_topic_filter(), emqx_ds:message_key()}.

%%================================================================================
%% API functions
%%================================================================================

-spec enqueue(_Shard, emqx_ds_beamformer:poll_req(), timeout()) -> ok.
enqueue(Shard, Req, Timeout) ->
    ?tp(debug, beamformer_enqueue, #{req_id => Req#poll_req.req_id, queue => catchup}),
    emqx_ds_beamformer:enqueue(emqx_ds_beamformer_sup:catchup_pool(Shard), Req, Timeout).

-spec start_link(module(), _Shard, integer(), emqx_ds_beamformer:opts()) -> {ok, pid()}.
start_link(Mod, ShardId, Name, Opts) ->
    gen_server:start_link(?MODULE, [Mod, ShardId, Name, Opts], []).

%%================================================================================
%% behavior callbacks
%%================================================================================

init([CBM, ShardId, Name, _Opts]) ->
    process_flag(trap_exit, true),
    Pool = emqx_ds_beamformer_sup:catchup_pool(ShardId),
    gproc_pool:add_worker(Pool, Name),
    gproc_pool:connect_worker(Pool, Name),
    S = #s{
        module = CBM,
        shard = ShardId,
        metrics_id = emqx_ds_beamformer:shard_metrics_id(ShardId),
        name = Name,
        queue = queue_new(),
        pending_request_limit = emqx_ds_beamformer:cfg_pending_request_limit(),
        batch_size = emqx_ds_beamformer:cfg_batch_size()
    },
    self() ! ?housekeeping_loop,
    {ok, S}.

handle_call(Req = #poll_req{}, _From, S0) ->
    {Reply, S} = do_enqueue(Req, S0),
    {reply, Reply, S};
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(Req = #poll_req{}, S0) ->
    {_Reply, S} = do_enqueue(Req, S0),
    {noreply, S};
handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(?fulfill_loop, S0) ->
    put(?fulfill_loop, false),
    S = fulfill(S0),
    {noreply, S};
handle_info(?housekeeping_loop, S0 = #s{metrics_id = Metrics, queue = Queue}) ->
    %% Reload configuration according from environment variables:
    S = S0#s{
        batch_size = emqx_ds_beamformer:cfg_batch_size(),
        pending_request_limit = emqx_ds_beamformer:cfg_pending_request_limit()
    },
    emqx_ds_beamformer:cleanup_expired(catchup, Metrics, Queue),
    erlang:send_after(emqx_ds_beamformer:cfg_housekeeping_interval(), self(), ?housekeeping_loop),
    {noreply, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, #s{shard = ShardId, name = Name}) ->
    Pool = emqx_ds_beamformer_sup:catchup_pool(ShardId),
    gproc_pool:disconnect_worker(Pool, Name),
    gproc_pool:remove_worker(Pool, Name),
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

do_enqueue(Req, S = #s{queue = PendingTab, metrics_id = Metrics}) ->
    PQLen = ets:info(PendingTab, size),
    emqx_ds_builtin_metrics:set_pendingq_len(Metrics, PQLen),
    case PQLen >= S#s.pending_request_limit of
        true ->
            emqx_ds_builtin_metrics:inc_poll_requests_dropped(Metrics, 1),
            Reply = {error, recoverable, too_many_requests},
            {Reply, S};
        false ->
            queue_push(S#s.queue, Req),
            ensure_fulfill_loop(),
            {ok, S}
    end.

fulfill(S = #s{queue = Queue}) ->
    %% debug_pending(S),
    case find_older_request(Queue, 100) of
        undefined ->
            S;
        {Stream, TopicFilter, StartKey} ->
            ?tp(emqx_ds_beamformer_fulfill_old, #{
                stream => Stream, topic => TopicFilter, start_key => StartKey
            }),
            %% The function MUST destructively consume all requests
            %% matching stream and MsgKey to avoid infinite loop:
            do_fulfill(S, Stream, TopicFilter, StartKey),
            ensure_fulfill_loop(),
            S
    end.

do_fulfill(
    S = #s{
        shard = Shard,
        module = CBM,
        queue = OldQueue,
        batch_size = BatchSize
    },
    Stream,
    TopicFilter,
    StartKey
) ->
    %% Here we only group requests with exact match of the topic
    %% filter:
    GetF = queue_pop(OldQueue, Stream, TopicFilter),
    Result = emqx_ds_beamformer:scan_stream(CBM, Shard, Stream, TopicFilter, StartKey, BatchSize),
    OnNomatch = enqueue_nomatch(S),
    form_beams(S, GetF, OnNomatch, StartKey, Result).

%% This function checks pending requests in the `FromTab' and either
%% dispatches them as a beam or passes them to `OnNomatch' callback if
%% there's nothing to dispatch.
-spec form_beams(
    s(),
    fun((emqx_ds:message_key()) -> [Req]),
    fun(([Req]) -> _),
    emqx_ds:message_key(),
    emqx_ds_beamformer:stream_scan_return()
) -> boolean() when Req :: emqx_ds_beamformer:poll_req().
form_beams(S, GetF, OnNomatch, StartKey, {ok, EndKey, Batch}) ->
    do_form_beams(S, GetF, OnNomatch, StartKey, EndKey, Batch);
form_beams(#s{metrics_id = Metrics}, GetF, _OnNomatch, StartKey, Result) ->
    Pack =
        case Result of
            Err = {error, _, _} -> Err;
            {ok, end_of_stream} -> end_of_stream
        end,
    MatchReqs = GetF(StartKey),
    %% Report metrics:
    NFulfilled = length(MatchReqs),
    NFulfilled > 0 andalso
        begin
            emqx_ds_builtin_metrics:inc_poll_requests_fulfilled(Metrics, NFulfilled),
            emqx_ds_builtin_metrics:observe_sharing(Metrics, NFulfilled)
        end,
    %% Pack requests into beams and send out:
    emqx_ds_beamformer:send_out_term(Pack, MatchReqs),
    NFulfilled > 0.

-spec do_form_beams(
    s(),
    fun((emqx_ds:message_key()) -> [Req]),
    fun(([Req]) -> _),
    emqx_ds:message_key(),
    emqx_ds:message_key(),
    [{emqx_ds:message_key(), emqx_types:message()}]
) -> boolean() when Req :: emqx_ds_beamformer:poll_req().
do_form_beams(
    #s{metrics_id = Metrics, module = CBM, shard = Shard},
    GetF,
    OnNomatch,
    StartKey,
    EndKey,
    Batch
) ->
    %% Find iterators that match the start message of the batch (to
    %% handle iterators freshly created by `emqx_ds:make_iterator'):
    Candidates0 = GetF(StartKey),
    %% Search for iterators where `last_seen_key' is equal to key of
    %% any message in the batch:
    {Candidates, BeamMaker} =
        process_batch(
            GetF,
            Batch,
            Candidates0,
            emqx_ds_beamformer:beams_init()
        ),
    %% Find what poll requests _actually_ have data in the batch. It's
    %% important not to send empty batches to the consumers, so they
    %% don't come back immediately, creating a busy loop:
    MatchReqs = emqx_ds_beamformer:beams_matched_requests(BeamMaker),
    NoMatchReqs = Candidates -- MatchReqs,
    ?tp(emqx_ds_beamformer_form_beams, #{match => MatchReqs, no_match => NoMatchReqs}),
    %% Report metrics:
    NFulfilled = length(MatchReqs),
    NFulfilled > 0 andalso
        begin
            emqx_ds_builtin_metrics:inc_poll_requests_fulfilled(Metrics, NFulfilled),
            emqx_ds_builtin_metrics:observe_sharing(Metrics, NFulfilled)
        end,
    %% Place unmatched requests back to the queue:
    OnNomatch(EndKey, NoMatchReqs),
    %% Pack requests into beams and serve them:
    UpdateIterator = fun(Iterator, NextKey) ->
        emqx_ds_beamformer:update_iterator(CBM, Shard, Iterator, NextKey)
    end,
    ok = emqx_ds_beamformer:beams_conclude(UpdateIterator, EndKey, BeamMaker),
    NFulfilled > 0.

process_batch(_GetF, [], Candidates, Beams) ->
    {Candidates, Beams};
process_batch(GetF, [{Key, Msg} | Rest], Candidates0, Beams0) ->
    Candidates = GetF(Key) ++ Candidates0,
    MatchingReqs = lists:filter(
        fun(#poll_req{msg_matcher = Matcher}) ->
            Matcher(Key, Msg)
        end,
        Candidates
    ),
    Beams = emqx_ds_beamformer:beams_add({Key, Msg}, MatchingReqs, Beams0),
    process_batch(GetF, Rest, Candidates, Beams).

enqueue_nomatch(#s{shard = Shard}) ->
    fun(EndKey, NoMatch) ->
        lists:foreach(
            fun(Req0 = #poll_req{}) ->
                Req = Req0#poll_req{start_key = EndKey},
                emqx_ds_beamformer_rt:enqueue(Shard, Req, 0)
            end,
            NoMatch
        )
    end.

%% It's always worth trying to fulfill the oldest requests first,
%% because they have a better chance of producing a batch that
%% overlaps with other pending requests.
%%
%% This function implements a heuristic that tries to find such poll
%% request. It simply compares the keys (and nothing else) within a
%% small sample of pending polls, and picks request with the smallest
%% key as the starting point.
-spec find_older_request(ets:tid(), pos_integer()) -> queue_key().
find_older_request(Tab, SampleSize) ->
    MS = {{'$1', '_'}, [], ['$1']},
    case ets:select(Tab, [MS], SampleSize) of
        '$end_of_table' ->
            undefined;
        {[Fst | Rest], _Cont} ->
            %% Find poll request key with the minimum start_key (3rd
            %% element):
            lists:foldl(
                fun(E, Acc) ->
                    case element(3, E) < element(3, Acc) of
                        true -> E;
                        false -> Acc
                    end
                end,
                Fst,
                Rest
            )
    end.

-spec ensure_fulfill_loop() -> ok.
ensure_fulfill_loop() ->
    case get(?fulfill_loop) of
        true ->
            ok;
        _ ->
            put(?fulfill_loop, true),
            self() ! ?fulfill_loop,
            ok
    end.

queue_new() ->
    ets:new(old_polls, [duplicate_bag, private, {keypos, 1}]).

queue_push(
    Queue, Req = #poll_req{req_id = Ref, stream = Stream, topic_filter = TF, start_key = Key}
) ->
    ?tp(beamformer_push_replay, #{req_id => Ref}),
    ets:insert(Queue, {{Stream, TF, Key}, Req}).

%% queue_lookup(Queue, Stream, TopicFilter) ->
%%     fun(MsgKey) ->
%%         [Req || {_, Req} <- ets:lookup(Queue, {Stream, TopicFilter, MsgKey})]
%%     end.

queue_pop(OldQueue, Stream, TopicFilter) ->
    fun(MsgKey) ->
        [Req || {_, Req} <- ets:take(OldQueue, {Stream, TopicFilter, MsgKey})]
    end.
