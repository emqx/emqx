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

%% @doc This process is responsible for serving async poll requests
%% from the consumers.
%%
%% It serves as a pool for such requests, limiting the number of
%% queries running in parallel. Additionally, it tries to group
%% "coherent" poll requests together, so they can be served as a group
%% ("coherent beam").
%%
%% Here, by "coherent" we mean requests to scan overlapping key ranges
%% of a DS stream. Grouping requests helps to limit the number of
%% storage queries and conserve throughput of the EMQX backplane
%% network.
%%
%% Beamformer works as following:
%%
%% - Initially, requests are added to the "pending" queue.
%%
%% - Beamformer process spins in a "fulfill loop" that takes requests
%% from the pending queue one at a time, and tries to fulfill them
%% normally by quering the storage.
%%
%% - If storage returns a non-empty batch, beamformer then searches
%% for pending poll requests that may be coherent with the current
%% one. All matching requests are then packed into "beams" (one per
%% destination node) and sent out accodingly.
%%
%% - If the query returns an empty batch, beamformer moves the request
%% to the "wait" queue. Poll requests just linger there until they
%% time out, or until beamformer receives a matching stream event from
%% the storage.
%%
%% Storage event processing logic is following: if beamformer finds
%% waiting poll requests matching the event, it queries the storage
%% for a batch of data. If batch is non-empty, requests are served
%% exactly as described above. If batch is empty again, request is
%% moved back to the wait queue.

%% WARNING: beamformer makes some implicit assumptions about the
%% storage layout:
%%
%% - There's a bijection between iterator position and the message key
%%
%% - Message keys in the stream are monotonic
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

-behaviour(gen_server).

%% API:
-export([poll/5, shard_event/2]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([start_link/4, do_dispatch/1]).

-export_type([opts/0, beam/2, beam/0, return_addr/1, unpack_iterator_result/1]).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-include("emqx_ds.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-type opts() :: #{
    n_workers := non_neg_integer(),
    pending_request_limit => non_neg_integer()
}.

-type match_messagef() :: fun((emqx_ds:message_key(), emqx_types:message()) -> boolean()).

%% Request:

-type return_addr(ItKey) :: {reference(), ItKey}.

-record(poll_req, {
    key,
    %% Node from where the poll request originates:
    node,
    %% Information about the process that created the request:
    return_addr,
    %% Iterator:
    it,
    msg_matcher,
    opts,
    deadline
}).

-type poll_req(ItKey, Iterator) ::
    #poll_req{
        key :: {_Stream, emqx_ds:message_key()},
        node :: node(),
        return_addr :: return_addr(ItKey),
        it :: Iterator,
        msg_matcher :: match_messagef(),
        opts :: emqx_ds:poll_opts(),
        deadline :: integer()
    }.

%% Response:

-type dispatch_mask() :: bitstring().

-record(beam, {iterators, pack, misc}).

-opaque beam(ItKey, Iterator) ::
    #beam{
        iterators :: [{return_addr(ItKey), Iterator}],
        pack ::
            [{emqx_ds:message_key(), dispatch_mask(), emqx_types:message()}]
            | end_of_stream
            | emqx_ds:error(),
        misc :: #{}
    }.

-type beam() :: beam(_ItKey, _Iterator).

-type stream_scan_return() ::
    {ok, emqx_ds:message_key(), [{emqx_ds:message_key(), emqx_types:message()}]}
    | {ok, end_of_stream}
    | emqx_ds:error().

-record(s, {
    module :: module(),
    metrics_id,
    shard,
    name,
    pending_queue :: ets:tid(),
    pending_request_limit :: non_neg_integer(),
    wait_queue :: ets:tid(),
    is_spinning = false :: boolean()
}).

-type s() :: #s{}.

-record(shard_event, {
    updated_streams :: list()
}).

-type unpack_iterator_result(Stream) :: #{
    stream := Stream,
    last_seen_key := emqx_ds:message_key(),
    timestamp := emqx_ds:timestamp_us(),
    matcher := match_messagef()
}.

-define(fulfill_loop, fulfill_loop).
-define(housekeeping_loop, housekeeping_loop).

%%================================================================================
%% Callbacks
%%================================================================================

-callback unpack_iterator(_Shard, _Iterator) ->
    unpack_iterator_result(_Stream) | undefined.

%% -callback update_iterator(_Shard, Iterator, emqx_ds:message_key()) ->
%%     emqx_ds:make_iterator_result(Iterator).

-callback scan_stream(_Shard, _Stream, emqx_ds:message_key(), non_neg_integer()) ->
    stream_scan_return().

%%================================================================================
%% API functions
%%================================================================================

-spec poll(node(), return_addr(_ItKey), _Shard, _Iterator, emqx_ds:poll_opts()) ->
    ok.
poll(Node, ReturnAddr, Shard, Iterator, Opts = #{timeout := Timeout}) ->
    CBM = emqx_ds_beamformer_sup:cbm(Shard),
    #{stream := Stream, last_seen_key := DSKey, timestamp := Timestamp, matcher := Matcher} = CBM:unpack_iterator(
        Shard, Iterator
    ),
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    logger:debug(#{
        msg => poll, shard => Shard, key => DSKey, timeout => Timeout, deadline => Deadline
    }),
    %% Try to maximize likelyhood of sending similar iterators to the
    %% same worker:
    Token = {Stream, Timestamp div 10_000_000},
    Worker = gproc_pool:pick_worker(
        emqx_ds_beamformer_sup:pool(Shard),
        Token
    ),
    %% Make request:
    Req = #poll_req{
        key = {Stream, DSKey},
        node = Node,
        return_addr = ReturnAddr,
        it = Iterator,
        opts = Opts,
        deadline = Deadline,
        msg_matcher = Matcher
    },
    emqx_ds_builtin_metrics:inc_poll_requests(shard_metrics_id(Shard), 1),
    %% Currently we implement backpressure by ignoring transient
    %% errors (gen_server timeouts, `too_many_requests'), and just
    %% letting poll requests expire at the higher level. This should
    %% hold back the caller.
    try gen_server:call(Worker, Req, Timeout) of
        ok -> ok;
        {error, recoverable, too_many_requests} -> ok
    catch
        exit:timeout ->
            ok
    end.

shard_event(Shard, Streams) ->
    Workers = gproc_pool:active_workers(emqx_ds_beamformer_sup:pool(Shard)),
    lists:foreach(
        fun({_, Pid}) ->
            Pid ! #shard_event{updated_streams = Streams}
        end,
        Workers
    ).

%%================================================================================
%% behavior callbacks
%%================================================================================

init([CBM, ShardId, Name, Opts]) ->
    process_flag(trap_exit, true),
    Pool = emqx_ds_beamformer_sup:pool(ShardId),
    gproc_pool:add_worker(Pool, Name),
    gproc_pool:connect_worker(Pool, Name),
    PendingTab = ets:new(pending_polls, [duplicate_bag, private, {keypos, #poll_req.key}]),
    WaitingTab = ets:new(pending_polls, [duplicate_bag, private, {keypos, #poll_req.key}]),
    S = #s{
        module = CBM,
        shard = ShardId,
        metrics_id = shard_metrics_id(ShardId),
        name = Name,
        pending_queue = PendingTab,
        wait_queue = WaitingTab,
        pending_request_limit = maps:get(pending_request_limit, Opts, 10_000)
    },
    self() ! ?housekeeping_loop,
    {ok, S}.

handle_call(Req = #poll_req{}, _From, S = #s{pending_queue = PendingTab, wait_queue = WaitingTab}) ->
    %% TODO: it should be possible to drop requests past deadline
    %% immediately.
    NQueued = ets:info(PendingTab, size) + ets:info(WaitingTab, size),
    case NQueued >= S#s.pending_request_limit of
        true ->
            Reply = {error, recoverable, too_many_requests},
            {reply, Reply, S};
        false ->
            ets:insert(S#s.pending_queue, Req),
            {reply, ok, start_fulfill_loop(S)}
    end;
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(#shard_event{updated_streams = Streams}, S) ->
    logger:debug("Stream event ~p", [Streams]),
    {noreply, maybe_fulfill_waiting(S, Streams)};
handle_info(?fulfill_loop, S0) ->
    S1 = S0#s{is_spinning = false},
    S = fulfill_pending(S1),
    {noreply, S};
handle_info(?housekeeping_loop, S0) ->
    S = cleanup(S0),
    erlang:send_after(1000, self(), ?housekeeping_loop),
    {noreply, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, #s{shard = ShardId, name = Name}) ->
    Pool = emqx_ds_beamformer_sup:pool(ShardId),
    gproc_pool:disconnect_worker(Pool, Name),
    gproc_pool:remove_worker(Pool, Name),
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link(module(), _Shard, integer(), opts()) -> {ok, pid()}.
start_link(Mod, ShardId, Name, Opts) ->
    gen_server:start_link(?MODULE, [Mod, ShardId, Name, Opts], []).

%% @doc RPC target: split the beam and dispatch replies to local
%% consumers.
-spec do_dispatch(beam()) -> ok.
do_dispatch(Beam = #beam{}) ->
    %% TODO: optimize by avoiding the intermediate list. Split may be
    %% organized in a fold-like fashion.
    lists:foreach(
        fun({{Alias, ItKey}, Result}) ->
            Alias ! #poll_reply{ref = Alias, userdata = ItKey, payload = Result}
        end,
        split(Beam)
    ).

%%================================================================================
%% Internal functions
%%================================================================================

-spec start_fulfill_loop(s()) -> s().
start_fulfill_loop(S = #s{is_spinning = true}) ->
    S;
start_fulfill_loop(S = #s{is_spinning = false}) ->
    self() ! ?fulfill_loop,
    S#s{is_spinning = true}.

-spec cleanup(s()) -> s().
cleanup(S = #s{pending_queue = PendingTab, wait_queue = WaitingTab, metrics_id = Metrics}) ->
    do_cleanup(Metrics, PendingTab),
    do_cleanup(Metrics, WaitingTab),
    %% erlang:garbage_collect(),
    S.

do_cleanup(Metrics, Tab) ->
    Now = erlang:monotonic_time(millisecond),
    MS = {#poll_req{_ = '_', deadline = '$1'}, [{'<', '$1', Now}], [true]},
    NDeleted = ets:select_delete(Tab, [MS]),
    emqx_ds_builtin_metrics:inc_poll_requests_expired(Metrics, NDeleted).

-spec fulfill_pending(s()) -> s().
fulfill_pending(S = #s{pending_queue = PendingTab}) ->
    %% debug_pending(S),
    case find_older_key(PendingTab, 100) of
        undefined ->
            S;
        {Stream, StartKey} ->
            logger:debug("Fulfilling ~p ~p", [Stream, StartKey]),
            %% The function MUST destructively consume all requests
            %% matching stream and MsgKey to avoid infinite loop:
            do_fulfill_pending(S, Stream, StartKey),
            start_fulfill_loop(S)
    end.

do_fulfill_pending(
    S = #s{shard = Shard, module = CBM, pending_queue = PendingTab, wait_queue = WaitingTab},
    Stream,
    StartKey
) ->
    case CBM:scan_stream(Shard, Stream, StartKey, 100) of
        {ok, EndKey, Batch} ->
            form_beams(S, PendingTab, WaitingTab, Stream, StartKey, EndKey, Batch)
    end.

maybe_fulfill_waiting(S, []) ->
    S;
maybe_fulfill_waiting(
    S = #s{wait_queue = WaitingTab, module = CBM, shard = Shard},
    [Stream | Rest]
) ->
    %% debug_waiting(Stream, S),
    case oldest_key_for_stream(WaitingTab, Stream, 5) of
        undefined ->
            maybe_fulfill_waiting(S, Rest);
        StartKey ->
            %% logger:warning("Waiting keys ~p", [WaitingKeys]),
            case CBM:scan_stream(Shard, Stream, StartKey, 1000) of
                {ok, EndKey, Batch} ->
                    form_beams(S, WaitingTab, WaitingTab, Stream, StartKey, EndKey, Batch)
            end,
            maybe_fulfill_waiting(S, Rest)
    end.

%% It's always worth to try fulfilling the oldest requests first,
%% because they have a better chance of producing a batch that
%% overlaps with other pending requests.
%%
%% Below is one very shoddy attempt to find the oldest key for the
%% stream. It finds the lowest key in a somewhat random sample. But
%% since we use a `duplicate_bag', it may require a full table scan?
oldest_key_for_stream(Tab, Stream, SampleSize) ->
    MS = {#poll_req{_ = '_', key = {Stream, '$1'}}, [], ['$1']},
    case ets:select(Tab, [MS], SampleSize) of
        '$end_of_table' ->
            undefined;
        {Keys, _Cont} ->
            lists:min(Keys)
    end.

%% Same as above. Heuristic that tries to find a poll request that has
%% a better chance to overlap with others.
%%
%% TODO: This might negatively impact latency, since oldest requests
%% presumably will be served first.
find_older_key(Tab, SampleSize) ->
    MS = #poll_req{_ = '_', key = '$1'},
    case ets:match(Tab, MS, SampleSize) of
        '$end_of_table' ->
            undefined;
        {L, _Cont} ->
            hd(lists:min(L))
    end.

%% @doc Split beam into individual batches
-spec split(beam(ItKey, Iterator)) -> [{ItKey, emqx_ds:next_result(Iterator)}].
split(#beam{iterators = Its, pack = end_of_stream}) ->
    [{ItKey, {ok, end_of_stream}} || {ItKey, _Iter} <- Its];
split(#beam{iterators = Its, pack = {error, _, _} = Err}) ->
    [{ItKey, Err} || {ItKey, _Iter} <- Its];
split(#beam{iterators = Its, pack = Pack}) ->
    split(Its, Pack, 0, []).

%% This function checks pending requests in the `FromTab' and either
%% dispatches them as a beam or moves them to the `ToTab' if there's
%% nothing to dispatch.
%%
%% So when we process new poll requests, `FromTab' = `new_requests'
%% and `ToTab' = `waiting_requests'. When we process waiting poll
%% requests, both `FromTab' and `ToTab' = `waiting_requests' (i.e. we
%% kick long polls that didn't produce new messages back to the wait
%% queue).
-spec form_beams(
    s(),
    ets:tid(),
    ets:tid(),
    _Stream,
    emqx_ds:message_key(),
    emqx_ds:message_key(),
    [{emqx_ds:message_key(), emqx_types:message()}]
) ->
    #{node() => beam(_ItKey, _Iterator)}.
form_beams(S = #s{metrics_id = Metrics}, FromTab, ToTab, Stream, StartKey, EndKey, Batch) ->
    GetF = fun(MsgKey) ->
        ets:take(FromTab, {Stream, MsgKey})
    end,
    %% Find iterators that match the start message of the batch (to
    %% handle iterators freshly created by `emqx_ds:make_iterator'):
    Candidates0 = GetF(StartKey),
    %% Search for iterators where `last_seen_key' is equal to key of
    %% any message in the batch:
    Candidates = lists:foldl(
        fun({Key, _Msg}, Acc) ->
            GetF(Key) ++ Acc
        end,
        Candidates0,
        Batch
    ),
    %% Find what poll requests _actually_ have data in the batch. It's
    %% important not to send empty batches to the consumers, so they
    %% don't come back immediately, creating a busy loop:
    {MatchReqs, NoMatchReqs} = filter_candidates(Candidates, Batch),
    %% Report metrics:
    NFulfilled = length(MatchReqs),
    NFulfilled > 0 andalso
        begin
            emqx_ds_builtin_metrics:inc_poll_requests_fulfilled(Metrics, NFulfilled),
            emqx_ds_builtin_metrics:observe_sharing(Metrics, NFulfilled)
        end,
    %% Move unmatched requests to `ToTab':
    ets:insert(ToTab, NoMatchReqs),
    %% Split matched requests by destination node:
    ReqsByNode =
        lists:foldl(
            fun(Req = #poll_req{node = Node}, Acc) ->
                maps:update_with(
                    Node,
                    fun(L) -> [Req | L] end,
                    [Req],
                    Acc
                )
            end,
            #{},
            MatchReqs
        ),
    %% Pack requests into beams and serve them:
    maps:foreach(
        fun(Node, Reqs) ->
            Beam = pack(S, EndKey, Reqs, Batch),
            send_out(Node, Beam)
        end,
        ReqsByNode
    ).

-spec pack(
    s(),
    emqx_ds:message_key(),
    [{ItKey, Iterator}],
    stream_scan_return()
) -> beam(ItKey, Iterator).
pack(S, NextKey, Reqs, Batch) ->
    #s{module = CBM, shard = Shard} = S,
    Pack = [{Key, mk_mask(Reqs, Elem), Msg} || Elem = {Key, Msg} <- Batch],
    UpdatedIterators =
        lists:map(
            fun(#poll_req{it = It0, return_addr = RAddr}) ->
                {ok, It} = CBM:update_iterator(Shard, It0, NextKey),
                {RAddr, It}
            end,
            Reqs
        ),
    #beam{
        iterators = UpdatedIterators,
        pack = Pack,
        misc = #{}
    }.

split([], _Pack, _N, Acc) ->
    Acc;
split([{ItKey, It} | Rest], Pack, N, Acc0) ->
    Msgs = [
        {MsgKey, Msg}
     || {MsgKey, Mask, Msg} <- Pack,
        is_member(N, Mask)
    ],
    case Msgs of
        [] -> logger:warning("Empty batch ~p", [ItKey]);
        _ -> ok
    end,
    Acc = [{ItKey, {ok, It, Msgs}} | Acc0],
    split(Rest, Pack, N + 1, Acc).

-spec is_member(non_neg_integer(), dispatch_mask()) -> boolean().
is_member(N, Mask) ->
    <<_:N, Val:1, _/bitstring>> = Mask,
    Val =:= 1.

-spec mk_mask([poll_req(_ItKey, _Iterator)], {emqx_ds:message_key(), emqx_types:message()}) ->
    dispatch_mask().
mk_mask(Reqs, Elem) ->
    mk_mask(Reqs, Elem, <<>>).

mk_mask([], _Elem, Acc) ->
    Acc;
mk_mask([#poll_req{msg_matcher = Matcher} | Rest], {Key, Message} = Elem, Acc) ->
    %% FIXME: here we make an implicit assumption that keys form a
    %% monotonic sequence. This should be up to the backend to decide.
    Val =
        case Matcher(Key, Message) of
            true -> 1;
            false -> 0
        end,
    mk_mask(Rest, Elem, <<Acc/bitstring, Val:1>>).

filter_candidates(Reqs, Batch) ->
    filter_candidates(Reqs, Batch, {[], []}).

filter_candidates([], _, Acc) ->
    Acc;
filter_candidates(
    [Req = #poll_req{msg_matcher = Matcher} | Rest], Messages, {MatchAcc, NoMatchAcc}
) ->
    case lists:any(fun({MsgKey, Msg}) -> Matcher(MsgKey, Msg) end, Messages) of
        true ->
            filter_candidates(
                Rest,
                Messages,
                {[Req | MatchAcc], NoMatchAcc}
            );
        false ->
            filter_candidates(
                Rest,
                Messages,
                {MatchAcc, [Req | NoMatchAcc]}
            )
    end.

send_out(Node, Beam) ->
    ?tp(debug, beamformer_out, #{
        dest_node => Node,
        beam => Beam
    }),
    %% FIXME:
    %% emqx_ds_beamsplitter_proto_v1:dispatch(Node, Beam)
    do_dispatch(Beam),
    ok.

shard_metrics_id({DB, Shard}) ->
    emqx_ds_builtin_metrics:shard_metric_id(DB, Shard).

-compile({nowarn_unused_function, debug_pending/1}).
debug_pending(#s{shard = Shard, pending_queue = PendingTab}) ->
    case ets:tab2list(PendingTab) of
        [] -> ok;
        L -> logger:warning("Fulfill pending ~p: ~p", [Shard, L])
    end.

-compile({nowarn_unused_function, debug_waiting/2}).
debug_waiting(Stream, #s{shard = Shard, wait_queue = WaitingTab}) ->
    case ets:tab2list(WaitingTab) of
        [] -> ok;
        L -> logger:warning("Fulfill waiting ~p/~p: ~p", [Shard, Stream, L])
    end.

%%================================================================================
%% Tests
%%================================================================================

-ifdef(TEST).

is_member_test_() ->
    [
        ?_assert(is_member(0, <<1:1>>)),
        ?_assertNot(is_member(0, <<0:1>>)),
        ?_assertNot(is_member(5, <<0:10>>)),

        ?_assert(is_member(0, <<255:8>>)),
        ?_assert(is_member(7, <<255:8>>)),

        ?_assertNot(is_member(7, <<0:8, 1:1, 0:10>>)),
        ?_assert(is_member(8, <<0:8, 1:1, 0:10>>)),
        ?_assertNot(is_member(9, <<0:8, 1:1, 0:10>>))
    ].

mk_mask_test_() ->
    Always = fun(_It, _Msg) -> true end,
    Even = fun(It, _Msg) -> It rem 2 =:= 0 end,
    [
        ?_assertMatch(<<>>, mk_mask(Always, fake_message, [])),
        ?_assertMatch(<<1:1, 1:1, 1:1>>, mk_mask(Always, fake_message, [1, 2, 3])),
        ?_assertMatch(<<0:1, 1:1, 0:1, 1:1>>, mk_mask(Even, fake_message, [1, 2, 3, 4]))
    ].

%% pack_test_() ->
%%     Always = fun(_It, _Msg) -> true end,
%%     M1 = {<<"1">>, #message{id = <<"1">>}},
%%     M2 = {<<"2">>, #message{id = <<"2">>}},
%%     Its = [{<<"it1">>, it1}, {<<"it2">>, it2}],
%%     [
%%         ?_assertMatch(
%%             #beam{
%%                 iterators = [],
%%                 pack = [
%%                     {<<"1">>, <<>>, #message{id = <<"1">>}},
%%                     {<<"2">>, <<>>, #message{id = <<"2">>}}
%%                 ]
%%             },
%%             pack(Always, [], {ok, [M1, M2]})
%%         ),
%%         ?_assertMatch(
%%             #beam{
%%                 iterators = Its,
%%                 pack = [
%%                     {<<"1">>, <<1:1, 1:1>>, #message{id = <<"1">>}},
%%                     {<<"2">>, <<1:1, 1:1>>, #message{id = <<"2">>}}
%%                 ]
%%             },
%%             pack(Always, Its, {ok, [M1, M2]})
%%         )
%%     ].

split_test_() ->
    Always = fun(_It, _Msg) -> true end,
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
