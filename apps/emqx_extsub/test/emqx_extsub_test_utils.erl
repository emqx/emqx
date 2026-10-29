%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_test_utils).

-export([
    emqtt_connect/1,
    emqtt_subscribe/2,
    emqtt_drain/0,
    emqtt_drain/1,
    emqtt_drain/2,
    emqtt_ack/1,
    assert_no_retained_functions/1
]).

-include_lib("../src/emqx_extsub_internal.hrl").
-include_lib("eunit/include/eunit.hrl").

assert_no_retained_functions(ChanPid) ->
    ?assertMatch(
        {ok, #{registry := #{by_ref := [_ | _]}}},
        emqx_extsub:inspect(ChanPid)
    ),
    {dictionary, Dict} = process_info(ChanPid, dictionary),
    ExtSubState = proplists:get_value(extsub_st, Dict),
    ?assertNotEqual(undefined, ExtSubState),
    ?assertNot(contains_function(ExtSubState)),
    ?assertNot(contains_function(proplists:get_value(extsub_channel_info, Dict))).

contains_function(Term) when is_function(Term) ->
    true;
contains_function(Term) when is_map(Term) ->
    contains_function(maps:to_list(Term));
contains_function(Term) when is_tuple(Term) ->
    contains_function(tuple_to_list(Term));
contains_function(Term) when is_list(Term) ->
    lists:any(fun contains_function/1, Term);
contains_function(_) ->
    false.

emqtt_connect(Opts) ->
    BaseOpts = [{proto_ver, v5}],
    {ok, C} = emqtt:start_link(BaseOpts ++ Opts),
    {ok, _} = emqtt:connect(C),
    C.

emqtt_subscribe(Client, Topic) ->
    {ok, _, _} = emqtt:subscribe(Client, {Topic, 1}),
    ok.

emqtt_drain() ->
    emqtt_drain(0, 0).

emqtt_drain(MinMsg) when is_integer(MinMsg) ->
    emqtt_drain(MinMsg, 0).

emqtt_drain(MinMsg, Timeout) when is_integer(MinMsg) andalso is_integer(Timeout) ->
    emqtt_drain(MinMsg, Timeout, [], 0).

emqtt_drain(MinMsg, Timeout, AccMsgs, AccNReceived) ->
    receive
        {publish, Msg} ->
            emqtt_drain(MinMsg, Timeout, [Msg | AccMsgs], AccNReceived + 1)
    after Timeout ->
        case AccNReceived >= MinMsg of
            true ->
                {ok, lists:reverse(AccMsgs)};
            false ->
                {error, {not_enough_messages, {received, AccNReceived}, {min, MinMsg}}}
        end
    end.

emqtt_ack(Msgs) ->
    ok = lists:foreach(
        fun(#{client_pid := Pid, packet_id := PacketId}) ->
            emqtt:puback(Pid, PacketId)
        end,
        Msgs
    ).
