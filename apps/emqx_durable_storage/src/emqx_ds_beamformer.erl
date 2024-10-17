%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This process is responsible for processing async poll requests
%% from the consumers. It is designed as a generic, drop-in "frontend"
%% that can be integrated into any DS backend (builtin or external).
%%
%% It serves as a pool for such requests, limiting the number of
%% queries running in parallel. In addition, it tries to group
%% "coherent" poll requests together, so they can be fulfilled as a
%% group ("coherent beam").
%%
%% By "coherent" we mean requests to scan overlapping key ranges of
%% the same DS stream. Grouping requests helps to reduce the number of
%% storage queries and conserve throughput of the EMQX backplane
%% network. This should help in the situations when the majority of
%% clients are up to date and simply wait for the new data.
%%
%% Beamformer works as following:
%%
%% - Initially, requests are added to the "pending" queue.
%%
%% - Beamformer process spins in a "fulfill loop" that takes requests
%% from the pending queue one at a time, and tries to fulfill them
%% normally by quering the storage.
%%
%% - When storage returns a non-empty batch, an unrecoverable error or
%% `end_of_stream', beamformer searches for pending poll requests that
%% may be coherent with the current one. All matching requests are
%% then packed into "beams" (one per destination node) and sent out
%% accodingly.
%%
%% - If the query returns an empty batch, beamformer moves the request
%% to the "wait" queue. Poll requests just linger there until they
%% time out, or until beamformer receives a matching stream event from
%% the storage.
%%
%% The storage backend can send events to the beamformer by calling
%% `shard_event' function.
%%
%% Storage event processing logic is following: if beamformer finds
%% waiting poll requests matching the event, it queries the storage
%% for a batch of data. If batch is non-empty, requests are served
%% exactly as described above. If batch is empty again, request is
%% moved back to the wait queue.

%% WARNING: beamformer makes some implicit assumptions about the
%% storage layout:
%%
%% - For each topic filter and stream, there's a bijection between
%% iterator and the message key
%%
%% - Quering a stream with non-wildcard topic-filter is equivalent to
%% quering it with a wildcard topic filter and dropping messages in
%% postprocessing, e.g.:
%%
%% ```
%% next("foo/bar", StartTime) ==
%%   filter(λ msg. msg.topic == "foo/bar",
%%         next("#", StartTime))
%% '''
-module(emqx_ds_beamformer).

%% API:
-export([poll/5, shard_event/2]).
-export([unpack_iterator/3, update_iterator/4, scan_stream/6, high_watermark/3, fast_forward/4]).
-export([beams_init/0, beams_add/3, beams_conclude/3, beams_matched_requests/1, split/1]).
-export([shard_metrics_id/1, enqueue/3, send_out_term/2, cleanup_expired/3]).
-export([cfg_pending_request_limit/0, cfg_batch_size/0, cfg_housekeeping_interval/0]).

%% internal exports:
-export([do_dispatch/1]).

-export_type([
    opts/0,
    poll_req/0, poll_req/2,
    beam/2, beam/0,
    return_addr/1,
    unpack_iterator_result/1,
    event_topic/0,
    stream_scan_return/0,
    iterator_type/0,
    beam_maker/0
]).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-include("emqx_ds.hrl").
-include("emqx_ds_beamformer.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

%% `event_topic' and `event_topic_filter' types are structurally (but
%% not semantically) equivalent to their `emqx_ds' counterparts.
%%
%% These types are only used for matching of events against waiting
%% poll requests. Values of these types are never exposed outside.
%% Hence the backend can, for example, compress topics used in the
%% events and iterators.
-type event_topic() :: emqx_ds:topic().
-type event_topic_filter() :: emqx_ds:topic_filter().

-type opts() :: #{
    n_workers := non_neg_integer()
}.

%% Request:

-type return_addr(ItKey) :: {reference(), ItKey}.

-type poll_req_id() :: reference().

-type poll_req(ItKey, Iterator) ::
    #poll_req{
        req_id :: poll_req_id(),
        stream :: _Stream,
        topic_filter :: event_topic_filter(),
        start_key :: emqx_ds:message_key(),
        node :: node(),
        return_addr :: return_addr(ItKey),
        it :: Iterator,
        msg_matcher :: match_messagef(),
        opts :: emqx_ds:poll_opts(),
        deadline :: integer(),
        hops :: non_neg_integer()
    }.

-type poll_req() :: poll_req(_, _).

%% Response:

-type dispatch_mask() :: bitstring().

-record(beam, {iterators, pack, misc = #{}}).

-opaque beam(ItKey, Iterator) ::
    #beam{
        iterators :: [{return_addr(ItKey), Iterator}],
        pack ::
            [{emqx_ds:message_key(), dispatch_mask(), emqx_types:message()}]
            | end_of_stream
            | emqx_ds:error(_),
        misc :: #{}
    }.

-type beam() :: beam(_ItKey, _Iterator).

-type stream_scan_return() ::
    {ok, emqx_ds:message_key(), [{emqx_ds:message_key(), emqx_types:message()}]}
    | {ok, end_of_stream}
    | emqx_ds:error(_).

-type iterator_type() :: committed | committed_fresh | future.

%% Beam constructor:

-type per_node_requests() :: #{poll_req_id() => poll_req()}.

-type dispatch_matrix() :: #{{poll_req_id(), non_neg_integer()} => _}.

-type filtered_batch() :: [{non_neg_integer(), {emqx_ds:message_key(), emqx_types:message()}}].

-record(beam_maker, {
    n = 0 :: non_neg_integer(),
    reqs = #{} :: #{node() => per_node_requests()},
    matrix = #{} :: dispatch_matrix(),
    msgs = [] :: filtered_batch()
}).

-opaque beam_maker() :: #beam_maker{}.

%%================================================================================
%% Callbacks
%%================================================================================

-type match_messagef() :: fun(
    (emqx_ds:message_key(), emqx_ds:topic(), emqx_types:message()) -> boolean()
).

-type unpack_iterator_result(Stream) :: #{
    stream := Stream,
    topic_filter := event_topic_filter(),
    last_seen_key := emqx_ds:message_key(),
    message_matcher := match_messagef(),
    type => iterator_type()
}.

-callback unpack_iterator(_Shard, _Iterator) ->
    unpack_iterator_result(_Stream) | emqx_ds:error(_).

-callback update_iterator(_Shard, Iterator, emqx_ds:message_key()) ->
    emqx_ds:make_iterator_result(Iterator).

-callback scan_stream(
    _Shard, _Stream, event_topic_filter(), emqx_ds:message_key(), _BatchSize :: non_neg_integer()
) ->
    stream_scan_return().

-callback high_watermark(_Shard, _Stream) -> {ok, emqx_ds:message_key()} | emqx_ds:error().

-callback fast_forward(_Shard, Iterator, emqx_ds:key()) ->
    {ok, Iterator} | {ok, end_of_stream} | emqx_ds:error().

%%================================================================================
%% API functions
%%================================================================================

%% @doc Submit a poll request
-spec poll(node(), return_addr(_ItKey), _Shard, _Iterator, emqx_ds:poll_opts()) ->
    ok.
poll(Node, ReturnAddr, Shard, Iterator, Opts = #{timeout := Timeout}) ->
    CBM = emqx_ds_beamformer_sup:cbm(Shard),
    case CBM:unpack_iterator(Shard, Iterator) of
        #{
            stream := Stream,
            topic_filter := TF,
            last_seen_key := DSKey,
            message_matcher := MsgMatcher
        } ->
            Deadline = erlang:monotonic_time(millisecond) + Timeout,
            ReqId = make_ref(),
            ?tp(beamformer_poll, #{
                req_id => ReqId,
                shard => Shard,
                key => DSKey,
                timeout => Timeout,
                deadline => Deadline
            }),
            %% Make a request:
            Req = #poll_req{
                req_id = ReqId,
                stream = Stream,
                topic_filter = TF,
                start_key = DSKey,
                node = Node,
                return_addr = ReturnAddr,
                it = Iterator,
                opts = Opts,
                deadline = Deadline,
                msg_matcher = MsgMatcher
            },
            emqx_ds_builtin_metrics:inc_poll_requests(shard_metrics_id(Shard), 1),
            emqx_ds_beamformer_rt:enqueue(Shard, Req, Timeout);
        Err = {error, _, _} ->
            Beam = #beam{
                iterators = [{ReturnAddr, Iterator}],
                pack = Err
            },
            send_out(Node, Beam)
    end.

%% @doc This internal API notifies the beamformer that new data is
%% available for reading in the storage.
%%
%% Backends should call this function after committing data to the
%% storage, so waiting long poll requests can be fulfilled.
%%
%% Types of arguments to this function are dependent on the backend.
%%
%% - `_Shard': most DS backends have some notion of shard. Beamformer
%% uses this fact to partition poll requests, but otherwise it treats
%% shard as an opaque value.
%%
%% - `_Stream': backend- and layout-specific type of stream.
%% Beamformer uses exact matching on streams when it searches for
%% similar requests and when it matches events. Otherwise it's an
%% opaque type.
%%
%% - `event_topic()': When beamformer receives stream events, it
%% selects waiting events with matching stream AND
%% `event_topic_filter'.
-spec shard_event(_Shard, [{_Stream, emqx_ds:mduessage_key()}]) -> ok.
shard_event(Shard, Events) ->
    emqx_ds_beamformer_rt:shard_event(Shard, Events).

%% @doc Split beam into individual batches
-spec split(beam(ItKey, Iterator)) -> [{ItKey, emqx_ds:next_result(Iterator)}].
split(#beam{iterators = Its, pack = end_of_stream}) ->
    [{ItKey, {ok, end_of_stream}} || {ItKey, _Iter} <- Its];
split(#beam{iterators = Its, pack = {error, _, _} = Err}) ->
    [{ItKey, Err} || {ItKey, _Iter} <- Its];
split(#beam{iterators = Its, pack = Pack}) ->
    split(Its, Pack, 0, []).

-spec send_out_term({ok, end_of_stream} | emqx_ds:error(_), [poll_req()]) -> ok.
send_out_term(Term, Reqs) ->
    ReqsByNode = maps:groups_from_list(
        fun(#poll_req{node = Node}) -> Node end,
        fun(#poll_req{return_addr = RAddr, it = It}) ->
            {RAddr, It}
        end,
        Reqs
    ),
    %% Pack requests into beams and serve them:
    maps:foreach(
        fun(Node, Its) ->
            Beam = #beam{
                pack = Term,
                iterators = Its
            },
            send_out(Node, Beam)
        end,
        ReqsByNode
    ).

-spec beams_init() -> beam_maker().
beams_init() ->
    #beam_maker{}.

-spec beams_add(
    {emqx_ds:message_key(), emqx_types:message()},
    [poll_req()],
    beam_maker()
) -> beam_maker().
beams_add(_, [], S = #beam_maker{n = N}) ->
    %% This message was not matched by any poll request, so we don't
    %% pack it into the beam:
    S#beam_maker{n = N + 1};
beams_add(Msg, NewReqs, S = #beam_maker{n = N, reqs = Reqs0, matrix = Matrix0, msgs = Msgs}) ->
    {Reqs, Matrix} = lists:foldl(
        fun(Req = #poll_req{node = Node, req_id = Ref}, {Reqs1, Matrix1}) ->
            Reqs2 = maps:update_with(
                Node,
                fun(A) -> A#{Ref => Req} end,
                #{Ref => Req},
                Reqs1
            ),
            Matrix2 = Matrix1#{{Ref, N} => []},
            {Reqs2, Matrix2}
        end,
        {Reqs0, Matrix0},
        NewReqs
    ),
    S#beam_maker{
        n = N + 1,
        reqs = Reqs,
        matrix = Matrix,
        msgs = [{N, Msg} | Msgs]
    }.

-spec beams_matched_requests(beam_maker()) -> [poll_req()].
beams_matched_requests(#beam_maker{reqs = Reqs}) ->
    maps:fold(
        fun(_Node, NodeReqs, Acc) ->
            maps:values(NodeReqs) ++ Acc
        end,
        [],
        Reqs
    ).

-spec beams_conclude(
    fun((Iterator, emqx_ds:message_key()) -> Iterator),
    emqx_ds:message_key(),
    beam_maker()
) -> ok.
beams_conclude(UpdateIterator, NextKey, #beam_maker{reqs = Reqs, matrix = Matrix, msgs = Msgs}) ->
    maps:foreach(
        fun(Node, NodeReqs) ->
            Beam = pack(UpdateIterator, NextKey, Matrix, Msgs, NodeReqs),
            send_out(Node, Beam)
        end,
        Reqs
    ).

shard_metrics_id({DB, Shard}) ->
    emqx_ds_builtin_metrics:shard_metric_id(DB, Shard).

%% Dynamic config (currently it's global for all DBs):

cfg_pending_request_limit() ->
    application:get_env(emqx_durable_storage, poll_pending_request_limit, 100_000).

cfg_batch_size() ->
    application:get_env(emqx_durable_storage, poll_batch_size, 100).

cfg_housekeeping_interval() ->
    application:get_env(emqx_durable_storage, beamformer_housekeeping_interval, 1000).

%%================================================================================
%% behavior callback wrappers
%%================================================================================

-spec unpack_iterator(module(), _Shard, _Iterator) ->
    unpack_iterator_result(_Stream) | emqx_ds:error().
unpack_iterator(Mod, Shard, It) ->
    Mod:unpack_iterator(Shard, It).

-spec update_iterator(module(), _Shard, _It, emqx_ds:message_key()) ->
    emqx_ds:make_iterator_result().
update_iterator(Mod, Shard, It, Key) ->
    Mod:update_iterator(Shard, It, Key).

-spec scan_stream(
    module(), _Shard, _Stream, event_topic_filter(), emqx_ds:message_key(), non_neg_integer()
) ->
    stream_scan_return().
scan_stream(Mod, Shard, Stream, TopicFilter, StartKey, BatchSize) ->
    Mod:scan_stream(Shard, Stream, TopicFilter, StartKey, BatchSize).

-spec high_watermark(module(), _Shard, _Stream) -> {ok, emqx_ds:message_key()} | emqx_ds:error().
high_watermark(Mod, Shard, Stream) ->
    Mod:high_watermark(Shard, Stream).

-spec fast_forward(module(), _Shard, Iterator, emqx_ds:message_key()) ->
    {ok, Iterator} | {error, unrecoverable, has_data} | emqx_ds:error().
fast_forward(Mod, Shard, It, Key) ->
    Mod:fast_forward(Shard, It, Key).

%%================================================================================
%% Internal exports
%%================================================================================

enqueue(_, Req = #poll_req{hops = Hops}, _) when Hops > 3 ->
    %% It seems that the poll request is being bounced between the
    %% queues. After certain number of such hops we drop it to break
    %% the infinite loop.
    %%
    %% Normally, the poll request is bounced between the queues in the
    %% following cases:
    %%
    %% 1. The request was submitted to the RT queue, but its
    %% `start_key' is older than the high watermark for the stream. In
    %% this case RT queue will move it to the catchup queue.
    %%
    %% 2. Catchup queue tried to fulfill the poll request, but got an
    %% empty batch. In this case, it moves the poll request to the RT
    %% queue.
    %%
    %% Normally, the second case should not lead to the first one,
    %% since catchup queue updates the start key before sending it to
    %% the RT queue. However, the DS backend MAY misbehave and fail to
    %% return the end key corresponding to the empty batch equal to
    %% the high watermark.
    %%
    %% This is the probable cause of this warning. To avoid it the
    %% backend must ensure the following property:
    %%
    %% If `scan_stream' callback returns `{ok, EndKey, []}', then
    %% `EndKey' must be greater or equal to the `high_watermark' for
    %% the stream at the moment.
    ?tp(warning, beamformer_enqueue_max_hops, #{req => Req}),
    ok;
enqueue(Pool, Req0 = #poll_req{stream = Stream, hops = Hops}, Timeout) ->
    Req = Req0#poll_req{hops = Hops + 1},
    %% Currently we implement backpressure by ignoring transient
    %% errors (gen_server timeouts, `too_many_requests'), and just
    %% letting poll requests expire at the higher level. This should
    %% hold back the caller.
    Worker = gproc_pool:pick_worker(Pool, Stream),
    case Timeout of
        0 ->
            %% Avoid deadlock due to calling self:
            gen_server:cast(Worker, Req);
        _ ->
            try gen_server:call(Worker, Req, Timeout) of
                ok -> ok;
                {error, recoverable, too_many_requests} -> ok
            catch
                exit:{timeout, _} ->
                    ok
            end
    end.

%% @doc RPC target: split the beam and dispatch replies to local
%% consumers.
-spec do_dispatch(beam()) -> ok.
do_dispatch(Beam = #beam{}) ->
    lists:foreach(
        fun({{Alias, ItKey}, Result}) ->
            Alias ! #poll_reply{ref = Alias, userdata = ItKey, payload = Result}
        end,
        split(Beam)
    ).

%% @doc Semi-generic function for cleaning expired poll requests from
%% an ETS table that is organized as a collection of 2-tuples, where
%% `#poll_req{}' is the second element:
%%
%% `{_Key, #poll_req{...}}'
cleanup_expired(QueueType, Metrics, Table) ->
    Now = erlang:monotonic_time(millisecond),
    maybe_report_expired(QueueType, Table, Now),
    MS = {{'_', #poll_req{_ = '_', deadline = '$1'}}, [{'<', '$1', Now}], [true]},
    NDeleted = ets:select_delete(Table, [MS]),
    emqx_ds_builtin_metrics:inc_poll_requests_expired(Metrics, NDeleted),
    ok.

%%================================================================================
%% Internal functions
%%================================================================================

%% In TEST profile we want to emit trace events for expired poll
%% reqests. In prod we just silently drop them.
-ifndef(TEST).
maybe_report_expired(_QueueType, _Tabl, _Now) ->
    ok.
-else.
maybe_report_expired(QueueType, Table, Now) ->
    MS = {{'_', #poll_req{_ = '_', deadline = '$1', req_id = '$2'}}, [{'<', '$1', Now}], ['$2']},
    [
        ?tp(beamformer_poll_expired, #{req_id => ReqId, queue => QueueType})
     || ReqId <- ets:select(Tabl, [MS])
    ],
    ok.
-endif.

-spec pack(
    fun((Iterator, emqx_ds:message_key()) -> Iterator),
    emqx_ds:message_key(),
    dispatch_matrix(),
    filtered_batch(),
    per_node_requests()
) -> beam(_ItKey, _Iterator).
pack(UpdateIterator, NextKey, Matrix, Msgs, Reqs) ->
    {Refs, UpdatedIterators} =
        maps:fold(
            fun(_, #poll_req{req_id = Ref, it = It0, return_addr = RAddr}, {AccRefs, Acc}) ->
                ?tp(beamformer_fulfilled, #{req_id => Ref}),
                {ok, It} = UpdateIterator(It0, NextKey),
                {[Ref | AccRefs], [{RAddr, It} | Acc]}
            end,
            {[], []},
            Reqs
        ),
    Pack = do_pack(lists:reverse(Refs), Matrix, Msgs, []),
    #beam{
        iterators = lists:reverse(UpdatedIterators),
        pack = Pack
    }.

do_pack(_Refs, _Matrix, [], Acc) ->
    %% Messages in the `beam_maker' record are reversed, so we don't
    %% have to reverse them here:
    Acc;
do_pack(Refs, Matrix, [{N, {MsgKey, Msg}} | Msgs], Acc) ->
    %% FIXME: it can include messages that are not needed for the
    %% node:
    DispatchMask = lists:foldl(
        fun(Ref, Bin) ->
            case Matrix of
                #{{Ref, N} := _} ->
                    <<Bin/bitstring, 1:1>>;
                #{} ->
                    <<Bin/bitstring, 0:1>>
            end
        end,
        <<>>,
        Refs
    ),
    do_pack(Refs, Matrix, Msgs, [{MsgKey, DispatchMask, Msg} | Acc]).

split([], _Pack, _N, Acc) ->
    Acc;
split([{ItKey, It} | Rest], Pack, N, Acc0) ->
    Msgs = [
        {MsgKey, Msg}
     || {MsgKey, Mask, Msg} <- Pack,
        check_mask(N, Mask)
    ],
    case Msgs of
        [] -> logger:warning("Empty batch ~p", [ItKey]);
        _ -> ok
    end,
    Acc = [{ItKey, {ok, It, Msgs}} | Acc0],
    split(Rest, Pack, N + 1, Acc).

-spec check_mask(non_neg_integer(), dispatch_mask()) -> boolean().
check_mask(N, Mask) ->
    <<_:N, Val:1, _/bitstring>> = Mask,
    Val =:= 1.

send_out(Node, Beam) ->
    ?tp(debug, beamformer_out, #{
        dest_node => Node,
        beam => Beam
    }),
    %% gen_rpc currently doesn't optimize local casts:
    case node() of
        Node ->
            erlang:spawn(?MODULE, do_dispatch, [Beam]),
            ok;
        _ ->
            emqx_ds_beamsplitter_proto_v1:dispatch(Node, Beam)
    end.

%%================================================================================
%% Tests
%%================================================================================

-ifdef(TEST).

chec_mask_test_() ->
    [
        ?_assert(check_mask(0, <<1:1>>)),
        ?_assertNot(check_mask(0, <<0:1>>)),
        ?_assertNot(check_mask(5, <<0:10>>)),

        ?_assert(check_mask(0, <<255:8>>)),
        ?_assert(check_mask(7, <<255:8>>)),

        ?_assertNot(check_mask(7, <<0:8, 1:1, 0:10>>)),
        ?_assert(check_mask(8, <<0:8, 1:1, 0:10>>)),
        ?_assertNot(check_mask(9, <<0:8, 1:1, 0:10>>))
    ].

pack_test_() ->
    UpdateIterator = fun(It0, NextKey) ->
        {ok, setelement(2, It0, NextKey)}
    end,
    Raddr = raddr,
    NextKey = <<"42">>,
    Req1 = #poll_req{
        req_id = make_ref(),
        return_addr = Raddr,
        it = {it1, <<"0">>}
    },
    Req2 = #poll_req{
        req_id = make_ref(),
        return_addr = Raddr,
        it = {it2, <<"1">>}
    },
    Req3 = #poll_req{
        req_id = make_ref(),
        return_addr = Raddr,
        it = {it3, <<"1">>}
    },
    %% Messages:
    M1 = {<<"1">>, #message{id = <<"1">>}},
    M2 = {<<"2">>, #message{id = <<"2">>}},
    [
        ?_assertMatch(
            [],
            maps:to_list(
                pack_test_helper(
                    UpdateIterator,
                    NextKey,
                    [
                        {M1, []},
                        {M2, []}
                    ]
                )
            )
        ),
        ?_assertMatch(
            #{
                undefined :=
                    #beam{
                        iterators = [
                            {Raddr, {it1, NextKey}}, {Raddr, {it2, NextKey}}
                        ],
                        pack = [
                            {<<"1">>, <<1:1, 0:1>>, #message{id = <<"1">>}},
                            {<<"2">>, <<0:1, 1:1>>, #message{id = <<"2">>}}
                        ]
                    }
            },
            pack_test_helper(
                UpdateIterator,
                NextKey,
                [
                    {M1, [Req1]},
                    {M2, [Req2]}
                ]
            )
        ),
        ?_assertMatch(
            #{
                undefined :=
                    #beam{
                        iterators = [
                            {Raddr, {it1, NextKey}}, {Raddr, {it2, NextKey}}
                        ],
                        pack = [
                            {<<"2">>, <<1:1, 1:1>>, #message{id = <<"2">>}}
                        ]
                    }
            },
            pack_test_helper(
                UpdateIterator,
                NextKey,
                [
                    {M1, []},
                    {M2, [Req1, Req2]}
                ]
            )
        ),
        ?_assertMatch(
            #{
                undefined :=
                    #beam{
                        iterators = [
                            {Raddr, {it1, NextKey}},
                            {Raddr, {it2, NextKey}},
                            {RAddr, {it3, NextKey}}
                        ],
                        pack = [
                            {<<"1">>, <<1:1, 0:1, 1:1>>, #message{id = <<"1">>}},
                            {<<"2">>, <<0:1, 1:1, 0:1>>, #message{id = <<"2">>}}
                        ]
                    }
            },
            pack_test_helper(
                UpdateIterator,
                NextKey,
                [
                    {M1, [Req1, Req3]},
                    {M2, [Req2]}
                ]
            )
        )
    ].

pack_test_helper(UpdateIterator, NextKey, L) ->
    BeamMaker = lists:foldl(
        fun({Message, Reqs}, Acc) ->
            beams_add(Message, Reqs, Acc)
        end,
        beams_init(),
        L
    ),
    #beam_maker{reqs = Reqs, matrix = Matrix, msgs = Msgs} = BeamMaker,
    maps:map(
        fun(Node, NodeReqs) ->
            pack(UpdateIterator, NextKey, Matrix, Msgs, NodeReqs)
        end,
        Reqs
    ).

split_test_() ->
    M1 = {<<"1">>, #message{id = <<"1">>}},
    M2 = {<<"2">>, #message{id = <<"2">>}},
    M3 = {<<"3">>, #message{id = <<"3">>}},
    Its = [{<<"it1">>, it1}, {<<"it2">>, it2}],
    Beam1 = #beam{
        iterators = Its,
        pack = [
            {<<"1">>, <<1:1, 0:1>>, element(2, M1)},
            {<<"2">>, <<0:1, 1:1>>, element(2, M2)},
            {<<"3">>, <<1:1, 1:1>>, element(2, M3)}
        ]
    },
    [{<<"it1">>, Result1}, {<<"it2">>, Result2}] = lists:sort(split(Beam1)),
    [
        ?_assertMatch(
            {ok, it1, [M1, M3]},
            Result1
        ),
        ?_assertMatch(
            {ok, it2, [M2, M3]},
            Result2
        )
    ].

-endif.
