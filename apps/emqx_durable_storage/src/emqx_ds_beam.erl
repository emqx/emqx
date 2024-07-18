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
-module(emqx_ds_beam).

%% API:
-export([split/1, form/3, pack/3, do_dispatch/1]).

-export_type([
    beam/2, beam/0,
    get_iterators_for_keyf/2,
    return_addr/1
]).

-include_lib("emqx_utils/include/emqx_message.hrl").

-include("emqx_ds.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-type return_addr(ItKey) :: {reference(), ItKey}.

-type get_iterators_for_keyf(ItKey, Iterator) :: fun(
    (emqx_ds:message_key()) -> [{node(), return_addr(ItKey), Iterator}]
).

-type match_messagef(Iterator) :: fun((Iterator, emqx_types:message()) -> boolean()).

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

%%================================================================================
%% API functions
%%================================================================================

%% @doc RPC target: split the beam and dispatch replies to local
%% consumers.
-spec do_dispatch(beam()) -> ok.
do_dispatch(Beam) ->
    %% TODO: optimize by avoiding the intermediate list. Split may be
    %% organized in a fold-like fashion.
    lists:foreach(
        fun({Addr, Result}) ->
            {Alias, ItKey} = Addr,
            Alias ! #poll_reply{ref = Alias, userdata = ItKey, payload = Result}
        end,
        split(Beam)
    ).

-spec split(beam(ItKey, Iterator)) -> [{ItKey, emqx_ds:next_result(Iterator)}].
split(#beam{iterators = Its, pack = end_of_stream}) ->
    [{ItKey, {ok, end_of_stream}} || {ItKey, _Iter} <- Its];
split(#beam{iterators = Its, pack = {error, _, _} = Err}) ->
    [{ItKey, Err} || {ItKey, _Iter} <- Its];
split(#beam{iterators = Its, pack = Pack}) ->
    split(Its, Pack, 0, []).

-spec form(
    get_iterators_for_keyf(ItKey, Iterator),
    match_messagef(Iterator),
    [{emqx_ds:message_key(), emqx_types:message()}]
) ->
    #{node() => beam(ItKey, Iterator)}.
form(GetF, MatchF, Messages) ->
    %% Find iterators that can be packed in a beam:
    Its = lists:foldl(
        fun({Key, _Msg}, Acc) ->
            update_consumers(GetF, Key, Acc)
        end,
        #{},
        Messages
    ),
    pack(MatchF, Its, Messages).

-spec pack(
    match_messagef(Iterator),
    [{ItKey, Iterator}],
    [{emqx_ds:message_key(), emqx_types:message()}] | end_of_stream | emqx_ds:error()
) -> beam(ItKey, Iterator).
pack(_MatchF, Iterators, {ok, end_of_stream}) ->
    #beam{
        iterators = Iterators,
        pack = end_of_stream,
        misc = #{}
    };
pack(_MatchF, Iterators, {error, unrecoverable, _} = Err) ->
    #beam{
        iterators = Iterators,
        pack = Err,
        misc = #{}
    };
pack(MatchF, Iterators, {ok, Messages}) ->
    Its = [I || {_, I} <- Iterators],
    Pack = [{Key, mk_mask(MatchF, Msg, Its), Msg} || {Key, Msg} <- Messages],
    #beam{
        iterators = Iterators,
        pack = Pack,
        misc = #{}
    }.

%%================================================================================
%% Internal functions
%%================================================================================

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

mk_mask(MatchF, Message, Iterators) ->
    mk_mask(MatchF, Message, Iterators, <<>>).

mk_mask(_MatchF, _Message, [], Acc) ->
    Acc;
mk_mask(MatchF, Message, [It | Rest], Acc) ->
    Val =
        case MatchF(It, Message) of
            true -> 1;
            false -> 0
        end,
    mk_mask(MatchF, Message, Rest, <<Acc/bitstring, Val:1>>).

-spec update_consumers(get_iterators_for_keyf(ItKey, Iterator), emqx_ds:message_key(), Acc) ->
    Acc
when
    Acc :: #{node() => [{return_addr(ItKey), Iterator}]}.
update_consumers(GetF, MsgKey, Acc0) ->
    L = GetF(MsgKey),
    lists:foldl(
        fun({Node, ReturnAddr, It}, Acc) ->
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

pack_test_() ->
    Always = fun(_It, _Msg) -> true end,
    M1 = {<<"1">>, #message{id = <<"1">>}},
    M2 = {<<"2">>, #message{id = <<"2">>}},
    Its = [{<<"it1">>, it1}, {<<"it2">>, it2}],
    [
        ?_assertMatch(
            #beam{
                iterators = [],
                pack = [
                    {<<"1">>, <<>>, #message{id = <<"1">>}},
                    {<<"2">>, <<>>, #message{id = <<"2">>}}
                ]
            },
            pack(Always, [], [M1, M2])
        ),
        ?_assertMatch(
            #beam{
                iterators = Its,
                pack = [
                    {<<"1">>, <<1:1, 1:1>>, #message{id = <<"1">>}},
                    {<<"2">>, <<1:1, 1:1>>, #message{id = <<"2">>}}
                ]
            },
            pack(Always, Its, [M1, M2])
        )
    ].

split_test() ->
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
    ?assertMatch(
        {ok, it1, [M1, M3]},
        Result1
    ),
    ?assertMatch(
        {ok, it2, [M2, M3]},
        Result2
    ).

-endif.
