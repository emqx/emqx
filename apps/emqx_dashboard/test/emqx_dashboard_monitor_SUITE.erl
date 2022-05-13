%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_dashboard_monitor_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include("emqx_dashboard.hrl").

-define(SERVER, "http://127.0.0.1:18083").
-define(BASE_PATH, "/api/v5").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    mria:start(),
    emqx_common_test_helpers:start_apps([emqx_dashboard], fun set_special_configs/1),
    Config.

end_per_suite(Config) ->
    emqx_common_test_helpers:stop_apps([emqx_dashboard]),
    Config.

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config(),
    ok;
set_special_configs(_) ->
    ok.

t_monitor_samplers_all(_Config) ->
    timer:sleep(?DEFAULT_SAMPLE_INTERVAL * 2 * 1000 + 20),
    Size = mnesia:table_info(emqx_dashboard_monitor, size),
    All = emqx_dashboard_monitor:samplers(all, infinity),
    All2 = emqx_dashboard_monitor:samplers(),
    ?assert(erlang:length(All) == Size),
    ?assert(erlang:length(All2) == Size),
    ok.

t_monitor_samplers_latest(_Config) ->
    timer:sleep(?DEFAULT_SAMPLE_INTERVAL * 2 * 1000 + 20),
    Samplers = emqx_dashboard_monitor:samplers(node(), 2),
    Latest = emqx_dashboard_monitor:samplers(node(), 1),
    ?assert(erlang:length(Samplers) == 2),
    ?assert(erlang:length(Latest) == 1),
    ?assert(hd(Latest) == lists:nth(2, Samplers)),
    ok.

t_monitor_sampler_format(_Config) ->
    timer:sleep(?DEFAULT_SAMPLE_INTERVAL * 2 * 1000 + 20),
    Latest = hd(emqx_dashboard_monitor:samplers(node(), 1)),
    SamplerKeys = maps:keys(Latest),
    [?assert(lists:member(SamplerName, SamplerKeys)) || SamplerName <- ?SAMPLER_LIST],
    ok.

t_monitor_api(_) ->
    timer:sleep(?DEFAULT_SAMPLE_INTERVAL * 2 * 1000 + 20),
    {ok, Samplers} = request(["monitor"], "latest=20"),
    ?assert(erlang:length(Samplers) >= 2),
    Fun =
        fun(Sampler) ->
            Keys = [binary_to_atom(Key, utf8) || Key <- maps:keys(Sampler)],
            [?assert(lists:member(SamplerName, Keys)) || SamplerName <- ?SAMPLER_LIST]
        end,
    [Fun(Sampler) || Sampler <- Samplers],
    {ok, NodeSamplers} = request(["monitor", "nodes", node()]),
    [Fun(NodeSampler) || NodeSampler <- NodeSamplers],
    ok.

t_monitor_current_api(_) ->
    timer:sleep(?DEFAULT_SAMPLE_INTERVAL * 2 * 1000 + 20),
    {ok, Rate} = request(["monitor_current"]),
    [
        ?assert(maps:is_key(atom_to_binary(Key, utf8), Rate))
     || Key <- maps:values(?DELTA_SAMPLER_RATE_MAP) ++ ?GAUGE_SAMPLER_LIST
    ],
    {ok, NodeRate} = request(["monitor_current", "nodes", node()]),
    [
        ?assert(maps:is_key(atom_to_binary(Key, utf8), NodeRate))
     || Key <- maps:values(?DELTA_SAMPLER_RATE_MAP) ++ ?GAUGE_SAMPLER_LIST
    ],
    ok.

t_monitor_reset(_) ->
    restart_monitor(),
    {ok, Rate} = request(["monitor_current"]),
    [
        ?assert(maps:is_key(atom_to_binary(Key, utf8), Rate))
     || Key <- maps:values(?DELTA_SAMPLER_RATE_MAP) ++ ?GAUGE_SAMPLER_LIST
    ],
    timer:sleep(?DEFAULT_SAMPLE_INTERVAL * 2 * 1000 + 20),
    {ok, Samplers} = request(["monitor"], "latest=1"),
    ?assertEqual(1, erlang:length(Samplers)),
    ok.

t_monitor_api_error(_) ->
    {error, {400, #{<<"code">> := <<"BAD_RPC">>}}} =
        request(["monitor", "nodes", 'emqx@127.0.0.2']),
    {error, {400, #{<<"code">> := <<"BAD_RPC">>}}} =
        request(["monitor_current", "nodes", 'emqx@127.0.0.2']),
    {error, {400, #{<<"code">> := <<"BAD_REQUEST">>}}} =
        request(["monitor"], "latest=0"),
    {error, {400, #{<<"code">> := <<"BAD_REQUEST">>}}} =
        request(["monitor"], "latest=-1"),
    ok.

request(Path) ->
    request(Path, "").

request(Path, QS) ->
    Url = url(Path, QS),
    do_request_api(get, {Url, [auth_header_()]}).

url(Parts, QS) ->
    case QS of
        "" ->
            ?SERVER ++ filename:join([?BASE_PATH | Parts]);
        _ ->
            ?SERVER ++ filename:join([?BASE_PATH | Parts]) ++ "?" ++ QS
    end.

do_request_api(Method, Request) ->
    ct:pal("Req ~p ~p~n", [Method, Request]),
    case httpc:request(Method, Request, [], []) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _, Return}} when
            Code >= 200 andalso Code =< 299
        ->
            ct:pal("Resp ~p ~p~n", [Code, Return]),
            {ok, emqx_json:decode(Return, [return_maps])};
        {ok, {{"HTTP/1.1", Code, _}, _, Return}} ->
            ct:pal("Resp ~p ~p~n", [Code, Return]),
            {error, {Code, emqx_json:decode(Return, [return_maps])}};
        {error, Reason} ->
            {error, Reason}
    end.

auth_header_() ->
    Basic = binary_to_list(base64:encode(<<"admin:public">>)),
    {"Authorization", "Basic " ++ Basic}.

restart_monitor() ->
    OldMonitor = erlang:whereis(emqx_dashboard_monitor),
    erlang:exit(OldMonitor, kill),
    ?assertEqual(ok, wait_new_monitor(OldMonitor, 10)).

wait_new_monitor(_OldMonitor, Count) when Count =< 0 -> timeout;
wait_new_monitor(OldMonitor, Count) ->
    NewMonitor = erlang:whereis(emqx_dashboard_monitor),
    case is_pid(NewMonitor) andalso NewMonitor =/= OldMonitor of
        true ->
            ok;
        false ->
            timer:sleep(100),
            wait_new_monitor(OldMonitor, Count - 1)
    end.
