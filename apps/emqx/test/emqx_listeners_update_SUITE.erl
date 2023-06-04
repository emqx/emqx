%%--------------------------------------------------------------------
%% Copyright (c) 2017-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_listeners_update_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_schema.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-import(emqx_listeners, [current_conns/2, is_running/1]).

-define(LISTENERS, [listeners]).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:boot_modules(all),
    emqx_common_test_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([]).

init_per_testcase(_TestCase, Config) ->
    Init = emqx:get_raw_config(?LISTENERS),
    [{init_conf, Init} | Config].

end_per_testcase(_TestCase, Config) ->
    Conf = ?config(init_conf, Config),
    {ok, _} = emqx:update_config(?LISTENERS, Conf),
    ok.

t_default_conf(_Config) ->
    ?assertMatch(
        #{
            <<"tcp">> := #{<<"default">> := #{<<"bind">> := <<"0.0.0.0:1883">>}},
            <<"ssl">> := #{<<"default">> := #{<<"bind">> := <<"0.0.0.0:8883">>}},
            <<"ws">> := #{<<"default">> := #{<<"bind">> := <<"0.0.0.0:8083">>}},
            <<"wss">> := #{<<"default">> := #{<<"bind">> := <<"0.0.0.0:8084">>}}
        },
        emqx:get_raw_config(?LISTENERS)
    ),
    ?assertMatch(
        #{
            tcp := #{default := #{bind := {{0, 0, 0, 0}, 1883}}},
            ssl := #{default := #{bind := {{0, 0, 0, 0}, 8883}}},
            ws := #{default := #{bind := {{0, 0, 0, 0}, 8083}}},
            wss := #{default := #{bind := {{0, 0, 0, 0}, 8084}}}
        },
        emqx:get_config(?LISTENERS)
    ),
    ok.

t_update_conf(_Conf) ->
    Raw = emqx:get_raw_config(?LISTENERS),
    Raw1 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"bind">>], Raw, <<"127.0.0.1:1883">>
    ),
    Raw2 = emqx_utils_maps:deep_put(
        [<<"ssl">>, <<"default">>, <<"bind">>], Raw1, <<"127.0.0.1:8883">>
    ),
    Raw3 = emqx_utils_maps:deep_put(
        [<<"ws">>, <<"default">>, <<"bind">>], Raw2, <<"0.0.0.0:8083">>
    ),
    Raw4 = emqx_utils_maps:deep_put(
        [<<"wss">>, <<"default">>, <<"bind">>], Raw3, <<"127.0.0.1:8084">>
    ),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw4)),
    ?assertMatch(
        #{
            <<"tcp">> := #{<<"default">> := #{<<"bind">> := <<"127.0.0.1:1883">>}},
            <<"ssl">> := #{<<"default">> := #{<<"bind">> := <<"127.0.0.1:8883">>}},
            <<"ws">> := #{<<"default">> := #{<<"bind">> := <<"0.0.0.0:8083">>}},
            <<"wss">> := #{<<"default">> := #{<<"bind">> := <<"127.0.0.1:8084">>}}
        },
        emqx:get_raw_config(?LISTENERS)
    ),
    BindTcp = {{127, 0, 0, 1}, 1883},
    BindSsl = {{127, 0, 0, 1}, 8883},
    BindWs = {{0, 0, 0, 0}, 8083},
    BindWss = {{127, 0, 0, 1}, 8084},
    ?assertMatch(
        #{
            tcp := #{default := #{bind := BindTcp}},
            ssl := #{default := #{bind := BindSsl}},
            ws := #{default := #{bind := BindWs}},
            wss := #{default := #{bind := BindWss}}
        },
        emqx:get_config(?LISTENERS)
    ),
    ?assertError(not_found, current_conns(<<"tcp:default">>, {{0, 0, 0, 0}, 1883})),
    ?assertError(not_found, current_conns(<<"ssl:default">>, {{0, 0, 0, 0}, 8883})),

    ?assertEqual(0, current_conns(<<"tcp:default">>, BindTcp)),
    ?assertEqual(0, current_conns(<<"ssl:default">>, BindSsl)),

    ?assertEqual({0, 0, 0, 0}, proplists:get_value(ip, ranch:info('ws:default'))),
    ?assertEqual({127, 0, 0, 1}, proplists:get_value(ip, ranch:info('wss:default'))),
    ?assert(is_running('ws:default')),
    ?assert(is_running('wss:default')),
    ok.

t_add_delete_conf(_Conf) ->
    Raw = emqx:get_raw_config(?LISTENERS),
    %% add
    #{<<"tcp">> := #{<<"default">> := Tcp}} = Raw,
    NewBind = <<"127.0.0.1:1987">>,
    Raw1 = emqx_utils_maps:deep_put([<<"tcp">>, <<"new">>], Raw, Tcp#{<<"bind">> => NewBind}),
    Raw2 = emqx_utils_maps:deep_put([<<"ssl">>, <<"default">>], Raw1, ?TOMBSTONE_VALUE),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw2)),
    ?assertEqual(0, current_conns(<<"tcp:new">>, {{127, 0, 0, 1}, 1987})),
    ?assertError(not_found, current_conns(<<"ssl:default">>, {{0, 0, 0, 0}, 8883})),
    %% deleted
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw)),
    ?assertError(not_found, current_conns(<<"tcp:new">>, {{127, 0, 0, 1}, 1987})),
    ?assertEqual(0, current_conns(<<"ssl:default">>, {{0, 0, 0, 0}, 8883})),
    ok.

t_delete_default_conf(_Conf) ->
    Raw = emqx:get_raw_config(?LISTENERS),
    %% delete default listeners
    Raw1 = emqx_utils_maps:deep_put([<<"tcp">>, <<"default">>], Raw, ?TOMBSTONE_VALUE),
    Raw2 = emqx_utils_maps:deep_put([<<"ssl">>, <<"default">>], Raw1, ?TOMBSTONE_VALUE),
    Raw3 = emqx_utils_maps:deep_put([<<"ws">>, <<"default">>], Raw2, ?TOMBSTONE_VALUE),
    Raw4 = emqx_utils_maps:deep_put([<<"wss">>, <<"default">>], Raw3, ?TOMBSTONE_VALUE),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw4)),
    ?assertError(not_found, current_conns(<<"tcp:default">>, {{0, 0, 0, 0}, 1883})),
    ?assertError(not_found, current_conns(<<"ssl:default">>, {{0, 0, 0, 0}, 8883})),
    ?assertMatch({error, not_found}, is_running('ws:default')),
    ?assertMatch({error, not_found}, is_running('wss:default')),

    %% reset
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw)),
    ?assertEqual(0, current_conns(<<"tcp:default">>, {{0, 0, 0, 0}, 1883})),
    ?assertEqual(0, current_conns(<<"ssl:default">>, {{0, 0, 0, 0}, 8883})),
    ?assert(is_running('ws:default')),
    ?assert(is_running('wss:default')),
    ok.
