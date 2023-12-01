%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_takeover_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(TOPIC, <<"t">>).
-define(CNT, 100).
-define(SLEEP, 10).

%%--------------------------------------------------------------------
%% Initial funcs

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    ok = emqx_cth_suite:stop(Apps),
    ok.
%%--------------------------------------------------------------------
%% Testcases

t_takeover(_) ->
    process_flag(trap_exit, true),
    ClientId = <<"clientid">>,
    Middle = ?CNT div 2,
    Client1Msgs = messages(0, Middle),
    Client2Msgs = messages(Middle, ?CNT div 2),
    AllMsgs = Client1Msgs ++ Client2Msgs,

    meck:new(emqx_cm, [non_strict, passthrough]),
    meck:expect(emqx_cm, takeover_session_end, fun(Arg) ->
        ok = timer:sleep(?SLEEP * 2),
        meck:passthrough([Arg])
    end),

    Commands =
        [{fun start_client/4, [ClientId, <<"t">>, ?QOS_1]}] ++
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs] ++
            [{fun start_client/4, [ClientId, <<"t">>, ?QOS_1]}] ++
            [{fun publish_msg/2, [Msg]} || Msg <- Client2Msgs] ++
            [{fun stop_client/1, []}],

    FCtx = lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        #{},
        Commands
    ),

    #{client := [CPid2, CPid1]} = FCtx,
    ?assertReceive({'EXIT', CPid1, {disconnected, ?RC_SESSION_TAKEN_OVER, _}}),
    ?assertReceive({'EXIT', CPid2, normal}),

    Received = [Msg || {publish, Msg} <- ?drainMailbox(?SLEEP)],
    ct:pal("middle: ~p", [Middle]),
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    assert_messages_missed(AllMsgs, Received),
    assert_messages_order(AllMsgs, Received),

    meck:unload(emqx_cm),
    ok.

t_takover_in_cluster(_) ->
    todo.

%%--------------------------------------------------------------------
%% Commands

start_client(Ctx, ClientId, Topic, Qos) ->
    {ok, CPid} = emqtt:start_link([
        {clientid, ClientId},
        {proto_ver, v5},
        {clean_start, false}
    ]),
    _ = erlang:spawn_link(fun() ->
        {ok, _} = emqtt:connect(CPid),
        ct:pal("CLIENT: connected ~p", [CPid]),
        {ok, _, [Qos]} = emqtt:subscribe(CPid, Topic, Qos)
    end),
    Ctx#{client => [CPid | maps:get(client, Ctx, [])]}.

publish_msg(Ctx, Msg) ->
    ok = timer:sleep(rand:uniform(?SLEEP)),
    case emqx:publish(Msg) of
        [] -> publish_msg(Ctx, Msg);
        [_ | _] -> Ctx
    end.

stop_client(Ctx = #{client := [CPid | _]}) ->
    ok = timer:sleep(?SLEEP),
    ok = emqtt:stop(CPid),
    Ctx.

%%--------------------------------------------------------------------
%% Helpers

assert_messages_missed(Ls1, Ls2) ->
    Missed = lists:filtermap(
        fun(Msg) ->
            No = emqx_message:payload(Msg),
            case lists:any(fun(#{payload := No1}) -> No1 == No end, Ls2) of
                true -> false;
                false -> {true, No}
            end
        end,
        Ls1
    ),
    case Missed of
        [] ->
            ok;
        _ ->
            ct:fail("Miss messages: ~p", [Missed]),
            error
    end.

assert_messages_order([], []) ->
    ok;
assert_messages_order([Msg | Expected], Received) ->
    %% Account for duplicate messages:
    case lists:splitwith(fun(#{payload := P}) -> emqx_message:payload(Msg) == P end, Received) of
        {[], [#{payload := Mismatch} | _]} ->
            ct:fail("Message order is not correct, expected: ~p, received: ~p", [
                emqx_message:payload(Msg), Mismatch
            ]),
            error;
        {_Matching, Rest} ->
            assert_messages_order(Expected, Rest)
    end.

messages(Offset, Cnt) ->
    [emqx_message:make(ct, ?QOS_1, ?TOPIC, payload(Offset + I)) || I <- lists:seq(1, Cnt)].

payload(I) ->
    % NOTE
    % Introduce randomness so that natural order is not the same as arrival order.
    iolist_to_binary(
        io_lib:format("~4.16.0B [~B] [~s]", [
            rand:uniform(16#10000) - 1,
            I,
            emqx_utils_calendar:now_to_rfc3339(millisecond)
        ])
    ).
