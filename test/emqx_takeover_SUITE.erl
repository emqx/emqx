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

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(TOPIC, <<"t">>).
-define(CNT, 100).

%%--------------------------------------------------------------------
%% Inital funcs

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]),
    ok.

init_per_testcase(Case, Config) ->
    ?MODULE:Case({'init', Config}).

end_per_testcase(Case, Config) ->
    ?MODULE:Case({'end', Config}).

%%--------------------------------------------------------------------
%% Testcases

t_takeover({init, Config}) when is_list(Config) ->
    Config;
t_takeover({'end', Config}) when is_list(Config) ->
    ok;
t_takeover(Config) when is_list(Config) ->
    AllMsgs = messages(?CNT),
    Pos = rand:uniform(?CNT),
    ClientId = random_clientid(),
    ClientOpts = [{clientid, ClientId},
                  {clean_start, false},
                  {host, "127.0.0.1"},
                  {port, 1883}
                 ],
    C1 =
        with_retry(
          fun() ->
              {ok, C} = emqtt:start_link(ClientOpts),
              {ok, _} = emqtt:connect(C),
              C
          end, 5),
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
    try
        spawn(fun() ->
                [begin
                    emqx:publish(lists:nth(I, AllMsgs)),
                    timer:sleep(rand:uniform(10))
                end || I <- lists:seq(Pos+1, ?CNT)]
            end),
        {ok, C2} = emqtt:start_link(ClientOpts),
        %% C1 is going down, unlink it so the test can continue to run
        _ = monitor(process, C1),
        ?assert(erlang:is_process_alive(C1)),
        unlink(C1),
        {ok, _} = emqtt:connect(C2),
        receive
            {'DOWN', _, process, C1, _} ->
                ok
        after 1000 ->
                  ct:fail("timedout_waiting_for_old_connection_shutdown")
        end,
        Received = all_received_publishs(),
        ct:pal("middle: ~p, received: ~p", [Pos, [P || {publish, #{payload := P}} <- Received]]),
        assert_messages_missed(AllMsgs, Received),
        assert_messages_order(AllMsgs, Received),
        kill_process(C2, fun emqtt:stop/1)
    after
        unload_meck(ClientId)
    end.

%%--------------------------------------------------------------------
%% Helpers

random_clientid() ->
    iolist_to_binary(["clientid", "-", integer_to_list(erlang:system_time())]).

kill_process(Pid, WithFun) ->
    _ = unlink(Pid),
    _ = monitor(process, Pid),
    try WithFun(Pid)
    catch _:_ -> ok
    end,
    receive
        {'DOWN', _, process, Pid, _} ->
            ok
    after 10_000 ->
              exit(Pid, kill),
              error(timeout)
    end.

with_retry(Fun, 1) -> Fun();
with_retry(Fun, N) when N > 1 ->
    try
        Fun()
    catch
        _ : _ ->
            ct:sleep(1000),
            with_retry(Fun, N - 1)
    end.

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

