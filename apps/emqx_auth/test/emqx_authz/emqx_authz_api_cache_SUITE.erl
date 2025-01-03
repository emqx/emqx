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
%%--------------------------------------------------------------------

-module(emqx_authz_api_cache_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_mgmt_api_test_util, [request/2, request/3, uri/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-define(SOURCE_HTTP, #{
    <<"type">> => <<"http">>,
    <<"enable">> => true,
    <<"url">> => <<"https://127.0.0.1:443/acl?username=", ?PH_USERNAME/binary>>,
    <<"ssl">> => #{<<"enable">> => true},
    <<"headers">> => #{},
    <<"method">> => <<"get">>,
    <<"request_timeout">> => <<"5s">>
}).

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, #{
                config => #{
                    authorization =>
                        #{
                            cache => #{enable => true},
                            no_match => deny,
                            sources => []
                        }
                }
            }},
            emqx_auth,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(t_node_cache, _Config) ->
    {ok, 204, _} = request(
        delete,
        uri(["authorization", "sources", "http"]),
        []
    ),
    ok = emqx_authz_source_registry:unregister(http);
end_per_testcase(_, _Config) ->
    ok.

t_clean_cache(_) ->
    {ok, C} = emqtt:start_link([{clientid, <<"emqx0">>}, {username, <<"emqx0">>}]),
    {ok, _} = emqtt:connect(C),
    {ok, _, _} = emqtt:subscribe(C, <<"a/b/c">>, 0),
    ok = emqtt:publish(C, <<"a/b/c">>, <<"{\"x\":1,\"y\":1}">>, 0),

    {ok, 200, Result3} = request(get, uri(["clients", "emqx0", "authorization", "cache"])),
    ?assertEqual(2, length(emqx_utils_json:decode(Result3))),

    request(delete, uri(["authorization", "cache"])),

    {ok, 200, Result4} = request(get, uri(["clients", "emqx0", "authorization", "cache"])),
    ?assertEqual(0, length(emqx_utils_json:decode(Result4))),

    ok.

t_node_cache(_) ->
    {ok, 200, CacheData0} = request(
        get,
        uri(["authorization", "node_cache"])
    ),
    ?assertMatch(
        #{<<"enable">> := false},
        emqx_utils_json:decode(CacheData0, [return_maps])
    ),
    {ok, 200, MetricsData0} = request(
        get,
        uri(["authorization", "node_cache", "status"])
    ),
    ?assertMatch(
        #{<<"metrics">> := #{<<"count">> := 0}},
        emqx_utils_json:decode(MetricsData0, [return_maps])
    ),
    {ok, 204, _} = request(
        put,
        uri(["authorization", "node_cache"]),
        #{
            <<"enable">> => true
        }
    ),
    {ok, 200, CacheData1} = request(
        get,
        uri(["authorization", "node_cache"])
    ),
    ok = emqx_authz_source_registry:register(http, emqx_authz_http),
    ?assertMatch(
        #{<<"enable">> := true},
        emqx_utils_json:decode(CacheData1, [return_maps])
    ),
    {ok, 204, _} = request(post, uri(["authorization", "sources"]), ?SOURCE_HTTP),

    %% We enabled authz cache, let's create client and make a subscription
    %% to touch the cache
    {ok, Client} = emqtt:start_link([
        {username, <<"user">>},
        {password, <<"pass">>}
    ]),
    ?assertMatch(
        {ok, _},
        emqtt:connect(Client)
    ),
    {ok, _, _} = emqtt:subscribe(Client, <<"test/topic">>, 1),
    ok = emqtt:disconnect(Client),

    %% Now check the metrics, the cache should have been populated
    {ok, 200, MetricsData2} = request(
        get,
        uri(["authorization", "node_cache", "status"])
    ),
    ?assertMatch(
        #{<<"metrics">> := #{<<"misses">> := #{<<"value">> := 1}}},
        emqx_utils_json:decode(MetricsData2, [return_maps])
    ),
    ok.

t_node_cache_reset(_) ->
    {ok, 204, _} = request(
        post,
        uri(["authorization", "node_cache", "reset"])
    ).
