%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% @doc Test suite verifies that MQTT retain and qos parameters
%% correctly reach the authorization.
%%--------------------------------------------------------------------

-module(emqx_authz_rich_actions_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_testcase(TestCase, Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf, "authorization.no_match = deny, authorization.cache.enable = false"},
            emqx_auth
        ],
        #{work_dir => filename:join(?config(priv_dir, Config), TestCase)}
    ),
    [{tc_apps, Apps} | Config].

end_per_testcase(_TestCase, Config) ->
    emqx_cth_suite:stop(?config(tc_apps, Config)),
    _ = emqx_authz:set_feature_available(rich_actions, true).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_rich_actions_subscribe(_Config) ->
    ok = setup_config(#{
        <<"type">> => <<"file">>,
        <<"enable">> => true,
        <<"rules">> =>
            <<
                "{allow, {user, \"username\"}, {subscribe, [{qos, 1}]}, [\"t1\"]}."
                "\n{allow, {user, \"username\"}, {subscribe, [{qos, 2}]}, [\"t2\"]}."
            >>
    }),

    {ok, C} = emqtt:start_link([{username, <<"username">>}]),
    {ok, _} = emqtt:connect(C),

    ?assertMatch(
        {ok, _, [1]},
        emqtt:subscribe(C, <<"t1">>, 1)
    ),

    ?assertMatch(
        {ok, _, [1, 2]},
        emqtt:subscribe(C, #{}, [{<<"t1">>, [{qos, 1}]}, {<<"t2">>, [{qos, 2}]}])
    ),

    ?assertMatch(
        {ok, _, [128, 128]},
        emqtt:subscribe(C, #{}, [{<<"t1">>, [{qos, 2}]}, {<<"t2">>, [{qos, 1}]}])
    ),

    ok = emqtt:stop(C).

t_rich_actions_publish(_Config) ->
    ok = setup_config(#{
        <<"type">> => <<"file">>,
        <<"enable">> => true,
        <<"rules">> =>
            <<
                "{allow, {user, \"publisher\"}, {publish, [{qos, 0}]}, [\"t0\"]}."
                "\n{allow, {user, \"publisher\"}, {publish, [{qos, 1}, {retain, true}]}, [\"t1\"]}."
                "\n{allow, {user, \"subscriber\"}, subscribe, [\"#\"]}."
            >>
    }),

    {ok, PC} = emqtt:start_link([{username, <<"publisher">>}]),
    {ok, _} = emqtt:connect(PC),

    {ok, SC} = emqtt:start_link([{username, <<"subscriber">>}]),
    {ok, _} = emqtt:connect(SC),
    {ok, _, _} = emqtt:subscribe(SC, <<"#">>, 1),

    _ = emqtt:publish(PC, <<"t0">>, <<"qos0">>, [{qos, 0}]),
    _ = emqtt:publish(PC, <<"t1">>, <<"qos1-retain">>, [{qos, 1}, {retain, true}]),

    _ = emqtt:publish(PC, <<"t0">>, <<"qos1">>, [{qos, 1}]),
    _ = emqtt:publish(PC, <<"t1">>, <<"qos1-noretain">>, [{qos, 1}, {retain, false}]),

    ?assertReceive(
        {publish, #{topic := <<"t0">>, payload := <<"qos0">>}}
    ),

    ?assertReceive(
        {publish, #{topic := <<"t1">>, payload := <<"qos1-retain">>}}
    ),

    ?assertNotReceive(
        {publish, #{topic := <<"t0">>, payload := <<"qos1">>}}
    ),

    ?assertNotReceive(
        {publish, #{topic := <<"t1">>, payload := <<"qos1-noretain">>}}
    ),

    ok = emqtt:stop(PC),
    ok = emqtt:stop(SC).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

setup_config(Params) ->
    emqx_authz_test_lib:setup_config(
        Params,
        #{}
    ).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
