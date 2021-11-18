%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_st_statistics_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_management/include/emqx_mgmt.hrl").
-include_lib("emqx_plugin_libs/include/emqx_st_statistics.hrl").

-define(CONTENT_TYPE, "application/x-www-form-urlencoded").

-define(HOST, "http://127.0.0.1:18083/").

-define(API_VERSION, "v4").

-define(BASE_PATH, "api").

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    application:load(emqx_plugin_libs),
    emqx_ct_helpers:start_apps([emqx_modules, emqx_management, emqx_dashboard]),
    Config.

end_per_suite(Config) ->
    emqx_ct_helpers:stop_apps([emqx_management]),
    Config.

init_per_testcase(_, Config) ->
    emqx_mod_st_statistics:load(emqx_st_statistics_SUITE:base_conf()),
    Config.

end_per_testcase(_, Config) ->
    emqx_mod_st_statistics:unload(undefined),
    Config.

t_get_history(_) ->
    ets:insert(?TOPK_TAB, #top_k{rank = 1,
                                 topic = <<"test">>,
                                 average_count = 12,
                                 average_elapsed = 1500}),

    {ok, Data} = request_api(get, api_path(["slow_topic"]), "_page=1&_limit=10",
                             auth_header_()),

    ShouldRet = #{meta => #{page => 1,
                            limit => 10,
                            hasnext => false,
                            count => 1},
                   data => [#{topic => <<"test">>,
                              rank => 1,
                              elapsed => 1500,
                              count => 12}],
                   code => 0},

    Ret = decode(Data),

    ?assertEqual(ShouldRet, Ret).

t_rank_range(_) ->
    Insert = fun(Rank) ->
                     ets:insert(?TOPK_TAB,
                                #top_k{rank = Rank,
                                       topic = <<"test">>,
                                       average_count = 12,
                                       average_elapsed = 1500})
             end,
    lists:foreach(Insert, lists:seq(1, 15)),

    timer:sleep(100),

    {ok, Data} = request_api(get, api_path(["slow_topic"]), "_page=1&_limit=10",
                             auth_header_()),

    Meta1 = #{page => 1, limit => 10, hasnext => true, count => 10},
    Ret1 = decode(Data),
    ?assertEqual(Meta1, maps:get(meta, Ret1)),

    %% End > Size
    {ok, Data2} = request_api(get, api_path(["slow_topic"]), "_page=2&_limit=10",
                              auth_header_()),

    Meta2 = #{page => 2, limit => 10, hasnext => false, count => 5},
    Ret2 = decode(Data2),
    ?assertEqual(Meta2, maps:get(meta, Ret2)),

    %% Start > Size
    {ok, Data3} = request_api(get, api_path(["slow_topic"]), "_page=3&_limit=10",
                              auth_header_()),

    Meta3 = #{page => 3, limit => 10, hasnext => false, count => 0},
    Ret3 = decode(Data3),
    ?assertEqual(Meta3, maps:get(meta, Ret3)).

t_clear(_) ->
    ets:insert(?TOPK_TAB, #top_k{rank = 1,
                                 topic = <<"test">>,
                                 average_count = 12,
                                 average_elapsed = 1500}),

    {ok, _} = request_api(delete, api_path(["slow_topic"]), [],
                          auth_header_()),

    ?assertEqual(0, ets:info(?TOPK_TAB, size)).

decode(Data) ->
    Pairs = emqx_json:decode(Data),
    to_maps(Pairs).

to_maps([H | _] = List) when is_tuple(H) ->
    to_maps(List, #{});

to_maps([_ | _] = List) ->
    [to_maps(X) || X <- List];

to_maps(V) -> V.

to_maps([{K, V} | T], Map) ->
    AtomKey = erlang:binary_to_atom(K),
    to_maps(T, Map#{AtomKey => to_maps(V)});

to_maps([], Map) ->
    Map.

request_api(Method, Url, Auth) ->
    request_api(Method, Url, [], Auth, []).

request_api(Method, Url, QueryParams, Auth) ->
    request_api(Method, Url, QueryParams, Auth, []).

request_api(Method, Url, QueryParams, Auth, []) ->
    NewUrl = case QueryParams of
                 "" -> Url;
                 _ -> Url ++ "?" ++ QueryParams
             end,
    do_request_api(Method, {NewUrl, [Auth]});
request_api(Method, Url, QueryParams, Auth, Body) ->
    NewUrl = case QueryParams of
                 "" -> Url;
                 _ -> Url ++ "?" ++ QueryParams
             end,
    do_request_api(Method, {NewUrl, [Auth], "application/json", emqx_json:encode(Body)}).

do_request_api(Method, Request)->
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], []) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _, Return} }
            when Code =:= 200 orelse Code =:= 201 ->
            {ok, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

auth_header_() ->
    AppId = <<"admin">>,
    AppSecret = <<"public">>,
    auth_header_(binary_to_list(AppId), binary_to_list(AppSecret)).

auth_header_(User, Pass) ->
    Encoded = base64:encode_to_string(lists:append([User,":",Pass])),
    {"Authorization","Basic " ++ Encoded}.

api_path(Parts)->
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION] ++ Parts).
