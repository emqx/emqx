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

-behaviour(gen_server).

%% API:
-export([poll/5, do_dispatch/1]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([start_link/3]).

-export_type([beam/2, beam/0, return_addr/1]).

-include_lib("emqx_utils/include/emqx_message.hrl").

-include("emqx_ds.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

%% Request:

-type return_addr(ItKey) :: {reference(), ItKey}.

-record(poll_req, {
    %% Key can be anything, optimized for whatever lookups are needed.
    key,
    %% Node from where the poll request originates:
    node,
    %% Information about the process that created the request:
    return_addr,
    %% Iterator:
    it,
    opts,
    deadline
}).

-type poll_req(ItKey, Iterator) ::
    #poll_req{
        key :: term(),
        node :: node(),
        return_addr :: return_addr(ItKey),
        it :: Iterator,
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

-type match_messagef() :: fun((emqx_types:message()) -> boolean()).

-type get_iterators_for_keyf(ItKey, Iterator) :: fun(
    (emqx_ds:message_key()) -> [{node(), return_addr(ItKey), Iterator}]
).

-type beam() :: beam(_ItKey, _Iterator).

-type stream_scan_return() ::
    {ok, emqx_ds:message_key(), [{emqx_ds:message_key(), emqx_types:message()}]}
    | {ok, end_of_stream}
    | emqx_ds:error().

-record(s, {
    module :: module(),
    shard,
    name,
    new_requests :: ets:tid(),
    waiting_requests :: ets:tid()
}).

-type s() :: #s{}.

%%================================================================================
%% Callbacks
%%================================================================================

-callback unpack_iterator(_Shard, _Iterator) ->
    {_Stream, emqx_ds:message_key(), emqx_ds:timestamp_us()} | undefined.

%% -callback update_iterator(_Shard, Iterator, emqx_ds:message_key()) ->
%%     emqx_ds:make_iterator_result(Iterator).

-callback scan_stream(_Shard, _Stream, emqx_ds:message_key(), non_neg_integer()) ->
    stream_scan_return().

-callback message_matcher(_Shard, _Itarator) -> match_messagef().

%%================================================================================
%% API functions
%%================================================================================

-spec poll(node(), return_addr(_ItKey), _Shard, _Iterator, emqx_ds:poll_opts()) ->
    ok.
poll(Node, ReturnAddr, Shard, Iterator, Opts = #{timeout := Timeout}) ->
    CBM = emqx_ds_beamformer_sup:cbm(Shard),
    {Stream, DSKey, Timestamp} = CBM:unpack_iterator(Shard, Iterator),
    %% Try to maximize likelyhood of sending similar iterators to the
    %% same worker:
    Worker = gproc_pool:pick_worker(
        emqx_ds_beamformer_sup:pool(Shard),
        {Stream, Timestamp div 10_000_000}
    ),
    %% Make request:
    Req = #poll_req{
        key = {Stream, DSKey},
        node = Node,
        return_addr = ReturnAddr,
        it = Iterator,
        opts = Opts,
        deadline = erlang:monotonic_time(millisecond) + Timeout
    },
    gen_server:call(Worker, Req, Timeout).

%%================================================================================
%% behavior callbacks
%%================================================================================

init([CBM, ShardId, Name]) ->
    process_flag(trap_exit, true),
    Pool = emqx_ds_beamformer_sup:pool(ShardId),
    gproc_pool:add_worker(Pool, Name),
    gproc_pool:connect_worker(Pool, Name),
    NewTab = ets:new(pending_polls, [duplicate_bag, private, {keypos, #poll_req.key}]),
    WaitingTab = ets:new(pending_polls, [duplicate_bag, private, {keypos, #poll_req.key}]),
    S = #s{
        module = CBM,
        shard = ShardId,
        name = Name,
        new_requests = NewTab,
        waiting_requests = WaitingTab
    },
    %% FIXME:
    timer:send_interval(50, doit),
    {ok, S}.

handle_call(Req = #poll_req{}, _From, S) ->
    %% TODO: drop requests past deadline immediately.
    ets:insert(S#s.new_requests, Req),
    {reply, ok, S};
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(doit, S) ->
    fulfill_pending(S),
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

-spec start_link(module(), _Shard, integer()) -> {ok, pid()}.
start_link(Mod, ShardId, Name) ->
    gen_server:start_link(?MODULE, [Mod, ShardId, Name], []).

%% @doc RPC target: split the beam and dispatch replies to local
%% consumers.
-spec do_dispatch(beam()) -> ok.
do_dispatch(Beam = #beam{}) ->
    %% TODO: optimize by avoiding the intermediate list. Split may be
    %% organized in a fold-like fashion.
    lists:foreach(
        fun({{Alias, ItKey}, Result}) ->
            logger:warning("Beam to ~p:~n~p -> ~p~n", [Alias, ItKey, Result]),
            Alias ! #poll_reply{ref = Alias, userdata = ItKey, payload = Result}
        end,
        split(Beam)
    ).

%%================================================================================
%% Internal functions
%%================================================================================

fulfill_pending(S = #s{new_requests = PendingTab}) ->
    case ets:first(PendingTab) of
        '$end_of_table' ->
            ok;
        {Stream, MsgKey} ->
            logger:warning("Fulfilling ~p ~p", [Stream, MsgKey]),
            %% The function MUST destructively consume all requests
            %% matching stream and MsgKey to avoid infinite loop:
            do_fulfill_pending(S, Stream, MsgKey),
            fulfill_pending(S)
    end.

do_fulfill_pending(S = #s{shard = Shard, module = CBM}, Stream, StartKey) ->
    case CBM:scan_stream(Shard, Stream, StartKey, 100) of
        {ok, _EndKey, []} ->
            move_to_waiting(S, Stream, StartKey);
        {ok, EndKey, Batch} ->
            Beams = form_beams(S, Stream, StartKey, EndKey, Batch),
            logger:warning("Beams: ~p~n", [Beams]),
            maps:foreach(
                fun(_Node, Beam) ->
                    do_dispatch(Beam)
                %% emqx_ds_beamsplitter_proto_v1:dispatch(Node, Beam)
                end,
                Beams
            )
    end.

req_getter(PendingTab, Stream) ->
    fun(MsgKey) ->
        Pendings = ets:take(PendingTab, {Stream, MsgKey}),
        logger:warning("Pending for ~p/~p:~n  ~p~n~p", [
            Stream, MsgKey, Pendings, ets:tab2list(PendingTab)
        ]),
        Pendings
    end.

mk_message_matcher(#s{module = CBM, shard = Shard}, Iterator) ->
    CBM:message_matcher(Shard, Iterator).

-spec split(beam(ItKey, Iterator)) -> [{ItKey, emqx_ds:next_result(Iterator)}].
split(#beam{iterators = Its, pack = end_of_stream}) ->
    [{ItKey, {ok, end_of_stream}} || {ItKey, _Iter} <- Its];
split(#beam{iterators = Its, pack = {error, _, _} = Err}) ->
    [{ItKey, Err} || {ItKey, _Iter} <- Its];
split(#beam{iterators = Its, pack = Pack}) ->
    split(Its, Pack, 0, []).

-spec form_beams(s(), _Stream, emqx_ds:message_key(), emqx_ds:message_key(), [
    {emqx_ds:message_key(), emqx_types:message()}
]) ->
    #{node() => beam(_ItKey, _Iterator)}.
form_beams(S = #s{new_requests = PendingTab}, Stream, StartKey, EndKey, Messages) ->
    GetF = req_getter(PendingTab, Stream),
    %% Find iterators that match the first message of the batch:
    InitialAcc = update_consumers(GetF, StartKey, #{}),
    %% Assert, or else we get infinite loop:
    true = maps:size(InitialAcc) > 0,
    %% Find additional iterators that can be packed in a beam:
    ItGroups = lists:foldl(
        fun({Key, _Msg}, Acc) ->
            update_consumers(GetF, Key, Acc)
        end,
        InitialAcc,
        Messages
    ),
    maps:map(
        fun(_Node, Its) ->
            pack(S, EndKey, Its, Messages)
        end,
        ItGroups
    ).

-spec pack(
    s(),
    emqx_ds:message_key(),
    [{ItKey, Iterator}],
    stream_scan_return()
) -> beam(ItKey, Iterator).
pack(S, NextKey, Iterators, Messages) ->
    #s{module = CBM, shard = Shard} = S,
    Matchers = [mk_message_matcher(S, It) || {_, It} <- Iterators],
    Pack = [{Key, mk_mask(Matchers, Msg), Msg} || {Key, Msg} <- Messages],
    UpdatedIterators =
        lists:map(
            fun({ItKey, It0}) ->
                {ok, It} = CBM:update_iterator(Shard, It0, NextKey),
                {ItKey, It}
            end,
            Iterators
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
    Acc = [{ItKey, {ok, It, Msgs}} | Acc0],
    split(Rest, Pack, N + 1, Acc).

-spec is_member(non_neg_integer(), bitstring()) -> boolean().
is_member(N, Mask) ->
    <<_:N, Val:1, _/bitstring>> = Mask,
    Val =:= 1.

mk_mask(Matchers, Message) ->
    mk_mask(Matchers, Message, <<>>).

mk_mask([], _Message, Acc) ->
    Acc;
mk_mask([Matcher | Rest], Message, Acc) ->
    Val =
        case Matcher(Message) of
            true -> 1;
            false -> 0
        end,
    mk_mask(Rest, Message, <<Acc/bitstring, Val:1>>).

-spec move_to_waiting(s(), _Stream, emqx_ds:message_key()) -> ok.
move_to_waiting(#s{new_requests = PendingTab, waiting_requests = WaitingTab}, Stream, StartKey) ->
    GetF = req_getter(PendingTab, Stream),
    %% GetF is destructive, it will automatically removes elements
    %% from the pending tab:
    ToMove = GetF(StartKey),
    logger:warning("Moving to waiting ~p/~p~n  ~p", [Stream, StartKey, ToMove]),
    %% For the waiting polls we want fast lookup on stream:
    Waiting = lists:map(
        fun(Req = #poll_req{}) ->
            Req#poll_req{key = Stream}
        end,
        ToMove
    ),
    ets:insert(WaitingTab, Waiting).

-spec update_consumers(get_iterators_for_keyf(ItKey, Iterator), emqx_ds:message_key(), Acc) ->
    Acc
when
    Acc :: #{node() => [poll_req(ItKey, Iterator)]}.
update_consumers(GetF, MsgKey, Acc0) ->
    L = GetF(MsgKey),
    lists:foldl(
        fun(#poll_req{node = Node, return_addr = ReturnAddr, it = It}, Acc) ->
            Item = {ReturnAddr, It},
            maps:update_with(
                Node,
                fun(Old) -> [Item | Old] end,
                [Item],
                Acc
            )
        end,
        Acc0,
        L
    ).

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
