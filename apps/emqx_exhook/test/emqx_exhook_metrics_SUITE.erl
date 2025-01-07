%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exhook_metrics_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_exhook/include/emqx_exhook.hrl").
-define(SvrFun(SvrName, FuncName), {SvrName, FuncName}).

-define(TARGET_HOOK, 'message.publish').

-define(CONF, <<
    "exhook {\n"
    "  servers = [\n"
    "              { name = succed,\n"
    "                url = \"http://127.0.0.1:9000\"\n"
    "              },\n"
    "              { name = failed,\n"
    "                failed_action = ignore,\n"
    "                url = \"http://127.0.0.1:9001\"\n"
    "              },\n"
    "            ]\n"
    "}\n"
>>).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Cfg) ->
    meck:new(emqx_exhook_mgr, [non_strict, passthrough, no_link]),
    meck:new(emqx_exhook_demo_svr, [non_strict, passthrough, no_link]),
    meck:expect(emqx_exhook_mgr, refresh_tick, fun() -> ok end),
    init_injections(hook_injects()),
    _ = emqx_exhook_demo_svr:start(),
    _ = emqx_exhook_demo_svr:start(failed, 9001),
    Apps = emqx_cth_suite:start(
        [emqx, {emqx_exhook, ?CONF}],
        #{work_dir => emqx_cth_suite:work_dir(Cfg)}
    ),
    [{suite_apps, Apps} | Cfg].

end_per_suite(Cfg) ->
    meck:unload(emqx_exhook_demo_svr),
    meck:unload(emqx_exhook_mgr),
    emqx_exhook_demo_svr:stop(),
    emqx_exhook_demo_svr:stop(failed),
    ok = emqx_cth_suite:stop(?config(suite_apps, Cfg)).

init_per_testcase(_, Config) ->
    clear_metrics(),
    Config.

end_per_testcase(_, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------
t_servers_metrics(_Cfg) ->
    Test = fun(C) ->
        Repeat = fun() ->
            emqtt:publish(C, <<"/exhook/metrics">>, <<>>, qos0)
        end,
        repeat(Repeat, 10)
    end,
    with_connection(Test),

    timer:sleep(200),
    SM = emqx_exhook_metrics:server_metrics(<<"succed">>),
    ?assertMatch(#{failed := 0, succeed := 10}, SM),

    FM = emqx_exhook_metrics:server_metrics(<<"failed">>),
    ?assertMatch(#{failed := 10, succeed := 0}, FM),

    SvrsM = emqx_exhook_metrics:servers_metrics(),
    ?assertEqual(SM, maps:get(<<"succed">>, SvrsM)),
    ?assertEqual(FM, maps:get(<<"failed">>, SvrsM)),
    ok.

t_rate(_) ->
    Test = fun(C) ->
        Repeat = fun() ->
            emqtt:publish(C, <<"/exhook/metrics">>, <<>>, qos0)
        end,

        repeat(Repeat, 5),
        timer:sleep(200),
        emqx_exhook_metrics:update(timer:seconds(1)),
        SM = emqx_exhook_metrics:server_metrics(<<"succed">>),
        ?assertMatch(#{rate := 5, max_rate := 5}, SM),

        repeat(Repeat, 6),
        timer:sleep(200),
        emqx_exhook_metrics:update(timer:seconds(1)),
        SM2 = emqx_exhook_metrics:server_metrics(<<"succed">>),
        ?assertMatch(#{rate := 6, max_rate := 6}, SM2),

        repeat(Repeat, 3),
        timer:sleep(200),
        emqx_exhook_metrics:update(timer:seconds(1)),
        SM3 = emqx_exhook_metrics:server_metrics(<<"succed">>),
        ?assertMatch(#{rate := 3, max_rate := 6}, SM3)
    end,
    with_connection(Test),
    ok.

t_hooks_metrics(_) ->
    Test = fun(C) ->
        Repeat = fun() ->
            emqtt:publish(C, <<"/exhook/metrics">>, <<>>, qos0)
        end,

        repeat(Repeat, 5),
        timer:sleep(200),
        HM = emqx_exhook_metrics:hooks_metrics(<<"succed">>),
        ?assertMatch(
            #{
                'message.publish' :=
                    #{failed := 0, succeed := 5}
            },
            HM
        )
    end,
    with_connection(Test),
    ok.

t_on_server_deleted(_) ->
    Test = fun(C) ->
        Repeat = fun() ->
            emqtt:publish(C, <<"/exhook/metrics">>, <<>>, qos0)
        end,
        repeat(Repeat, 10)
    end,
    with_connection(Test),

    timer:sleep(200),
    SM = emqx_exhook_metrics:server_metrics(<<"succed">>),
    ?assertMatch(#{failed := 0, succeed := 10}, SM),

    emqx_exhook_metrics:on_server_deleted(<<"succed">>),
    SM2 = emqx_exhook_metrics:server_metrics(<<"succed">>),
    ?assertMatch(#{failed := 0, succeed := 0}, SM2),
    ok.

%%--------------------------------------------------------------------
%% Utils
%%--------------------------------------------------------------------
clear_metrics() ->
    ets:delete_all_objects(?HOOKS_METRICS).

init_injections(Injects) ->
    lists:map(
        fun({Name, _}) ->
            Str = erlang:atom_to_list(Name),
            case lists:prefix("on_", Str) of
                true ->
                    Action = fun(Req, #{<<"channel">> := SvrName} = Md) ->
                        case maps:get(?SvrFun(SvrName, Name), Injects, undefined) of
                            undefined ->
                                meck:passthrough([Req, Md]);
                            Injection ->
                                Injection(Req, Md)
                        end
                    end,

                    meck:expect(emqx_exhook_demo_svr, Name, Action);
                _ ->
                    false
            end
        end,
        emqx_exhook_demo_svr:module_info(exports)
    ).

hook_injects() ->
    #{
        ?SvrFun(<<"failed">>, emqx_exhook_server:hk2func(?TARGET_HOOK)) =>
            fun(_Req, _Md) ->
                {error, "Error due to test"}
            end,
        ?SvrFun(<<"failed">>, on_provider_loaded) =>
            fun(_Req, Md) ->
                {ok, #{hooks => [#{name => <<"message.publish">>}]}, Md}
            end,
        ?SvrFun(<<"succed">>, on_provider_loaded) =>
            fun(_Req, Md) ->
                {ok, #{hooks => [#{name => <<"message.publish">>}]}, Md}
            end
    }.

with_connection(Fun) ->
    {ok, C} = emqtt:start_link([
        {host, "localhost"},
        {port, 1883},
        {username, <<"admin">>},
        {clientid, <<"exhook_tester">>}
    ]),
    {ok, _} = emqtt:connect(C),
    try
        Fun(C)
    catch
        Type:Error:Trace ->
            emqtt:stop(C),
            erlang:raise(Type, Error, Trace)
    end.

repeat(_Fun, 0) ->
    ok;
repeat(Fun, N) ->
    Fun(),
    repeat(Fun, N - 1).
