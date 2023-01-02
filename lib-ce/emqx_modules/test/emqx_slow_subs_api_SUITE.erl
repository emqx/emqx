%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%%--------------------------------------------------------------------n

-module(emqx_slow_subs_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_management/include/emqx_mgmt.hrl").
-include_lib("emqx_plugin_libs/include/emqx_slow_subs.hrl").

-define(CONTENT_TYPE, "application/x-www-form-urlencoded").

-define(HOST, "http://127.0.0.1:18083/").

-define(API_VERSION, "v4").

-define(BASE_PATH, "api").
-define(NOW, erlang:system_time(millisecond)).

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    ok = meck:new([emqx_modules], [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_modules, find_module, fun(_) -> [{true, true}] end),
    emqx_ct_helpers:boot_modules(all),
    application:load(emqx_plugin_libs),
    emqx_ct_helpers:start_apps([emqx_modules, emqx_management, emqx_dashboard]),
    Config.

end_per_suite(Config) ->
    emqx_ct_helpers:stop_apps([emqx_management]),
    ok = meck:unload(emqx_modules),
    Config.

init_per_testcase(_, Config) ->
    emqx_mod_slow_subs:load(base_conf()),
    Config.

end_per_testcase(_, Config) ->
    emqx_mod_slow_subs:unload([]),
    Config.

base_conf() ->
    [ {threshold, 500}
    , {top_k_num, 5}
    , {expire_interval, timer:seconds(60)}
    , {notice_interval, 0}
    , {notice_qos, 0}
    , {notice_batch_size, 3}
    ].

t_get_history(_) ->
    Now = ?NOW,
    Each = fun(I) ->
                   ClientId = erlang:list_to_binary(io_lib:format("test_~p", [I])),
                   Topic = erlang:list_to_binary(io_lib:format("topic/~p", [I])),
                   ets:insert(?TOPK_TAB, #top_k{index = ?TOPK_INDEX(I, ?ID(ClientId, Topic)),
                                                last_update_time = Now})
           end,

    lists:foreach(Each, lists:seq(1, 5)),

    {ok, Data} = request_api(get, api_path(["slow_subscriptions"]), "",
                             auth_header_()),
    #{data := [First | _]} = decode(Data),

    RFirst = #{clientid => <<"test_5">>,
               topic => <<"topic/5">>,
               timespan => 5,
               node => erlang:atom_to_binary(node()),
               last_update_time => Now},

    ?assertEqual(RFirst, First).

t_clear(_) ->
    ets:insert(?TOPK_TAB, #top_k{index = ?TOPK_INDEX(1, ?ID(<<"test">>, <<"test">>)),
                                 last_update_time = ?NOW}),

    {ok, _} = request_api(delete, api_path(["slow_subscriptions"]), [],
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
