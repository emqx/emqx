%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(TOPIC, <<"t">>).
-define(CNT, 100).

%%--------------------------------------------------------------------
%% Inital funcs

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]),
    ok.
%%--------------------------------------------------------------------
%% Testcases

t_takeover(_) ->
    process_flag(trap_exit, true),
    AllMsgs = messages(?CNT),
    Pos = rand:uniform(?CNT),

    ClientId = <<"clientid">>,
    {ok, C1} = emqtt:start_link([{clientid, ClientId}, {clean_start, false}]),
    {ok, _} = emqtt:connect(C1),
    emqtt:subscribe(C1, <<"t">>, 1),

    spawn(fun() ->
            [begin
                emqx:publish(lists:nth(I, AllMsgs)),
                timer:sleep(rand:uniform(10))
             end || I <- lists:seq(1, Pos)]
          end),
    emqtt:pause(C1),
    timer:sleep(?CNT*10),

    load_meck(ClientId),
    spawn(fun() ->
            [begin
                emqx:publish(lists:nth(I, AllMsgs)),
                timer:sleep(rand:uniform(10))
             end || I <- lists:seq(Pos+1, ?CNT)]
          end),
    {ok, C2} = emqtt:start_link([{clientid, ClientId}, {clean_start, false}]),
    {ok, _} = emqtt:connect(C2),

    Received = all_received_publishs(),
    ct:pal("middle: ~p, received: ~p", [Pos, [P || {publish, #{payload := P}} <- Received]]),
    assert_messages_missed(AllMsgs, Received),
    assert_messages_order(AllMsgs, Received),

    emqtt:disconnect(C2),
    unload_meck(ClientId).

t_takover_in_cluster(_) ->
    todo.

%%--------------------------------------------------------------------
%% Helpers

load_meck(ClientId) ->
    meck:new(fake_conn_mod, [non_strict]),
    HookTakeover = fun(Pid, Msg = {takeover, 'begin'}) ->
                           emqx_connection:call(Pid, Msg);
                      (Pid, Msg = {takeover, 'end'}) ->
                           timer:sleep(?CNT*10),
                           emqx_connection:call(Pid, Msg);
                      (Pid, Msg) ->
                           emqx_connection:call(Pid, Msg)
                   end,
    meck:expect(fake_conn_mod, call, HookTakeover),
    [ChanPid] = emqx_cm:lookup_channels(ClientId),
    ChanInfo = #{conninfo := ConnInfo} = emqx_cm:get_chan_info(ClientId),
    NChanInfo = ChanInfo#{conninfo := ConnInfo#{conn_mod := fake_conn_mod}},
    true = ets:update_element(emqx_channel_info, {ClientId, ChanPid}, {2, NChanInfo}).

unload_meck(_ClientId) ->
    meck:unload(fake_conn_mod).

all_received_publishs() ->
    all_received_publishs([]).

all_received_publishs(Ls) ->
    receive
        M = {publish, _Pub} -> all_received_publishs([M|Ls]);
        _ -> all_received_publishs(Ls)
    after 100 ->
        lists:reverse(Ls)
    end.

assert_messages_missed(Ls1, Ls2) ->
    Missed = lists:filtermap(fun(Msg) ->
                 No = emqx_message:payload(Msg),
                 case lists:any(fun({publish, #{payload := No1}}) ->  No1 == No end, Ls2) of
                     true -> false;
                     false -> {true, No}
                 end
             end, Ls1),
    case Missed of
        [] -> ok;
        _ ->
            ct:fail("Miss messages: ~p", [Missed]), error
    end.

assert_messages_order([], []) ->
    ok;
assert_messages_order([Msg|Ls1], [{publish, #{payload := No}}|Ls2]) ->
    case emqx_message:payload(Msg) == No of
        false ->
            ct:fail("Message order is not correct, expected: ~p, received: ~p", [emqx_message:payload(Msg), No]),
            error;
        true -> assert_messages_order(Ls1, Ls2)
    end.

messages(Cnt) ->
    [emqx_message:make(ct, 1, ?TOPIC, integer_to_binary(I)) || I <- lists:seq(1, Cnt)].

