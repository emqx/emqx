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
-module(emqx_auto_subscribe_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(APP, emqx_auto_subscribe).

-define(TOPIC_C, <<"/c/${clientid}">>).
-define(TOPIC_U, <<"/u/${username}">>).
-define(TOPIC_H, <<"/h/${host}">>).
-define(TOPIC_P, <<"/p/${port}">>).
-define(TOPIC_A, <<"/client/${clientid}/username/${username}/host/${host}/port/${port}">>).
-define(TOPIC_S, <<"/topic/simple">>).

-define(TOPICS, [?TOPIC_C, ?TOPIC_U, ?TOPIC_H, ?TOPIC_P, ?TOPIC_A, ?TOPIC_S]).

-define(ENSURE_TOPICS , [<<"/c/auto_sub_c">>
                        , <<"/u/auto_sub_u">>
                        , ?TOPIC_S]).

-define(CLIENT_ID, <<"auto_sub_c">>).
-define(CLIENT_USERNAME, <<"auto_sub_u">>).

all() ->
    [t_auto_subscribe, t_update].

init_per_suite(Config) ->
    mria:start(),
    application:stop(?APP),
    meck:new(emqx_schema, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_schema, fields, fun("auto_subscribe") ->
                                             meck:passthrough(["auto_subscribe"]) ++
                                             emqx_auto_subscribe_schema:fields("auto_subscribe");
                                        (F) -> meck:passthrough([F])
                                     end),

    meck:new(emqx_resource, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_resource, create, fun(_, _, _) -> {ok, meck_data} end),
    meck:expect(emqx_resource, update, fun(_, _, _, _) -> {ok, meck_data} end),
    meck:expect(emqx_resource, remove, fun(_) -> ok end ),

    application:load(emqx_dashboard),
    application:load(?APP),
    ok = emqx_common_test_helpers:load_config(emqx_auto_subscribe_schema,
        <<"auto_subscribe {
            topics = [
                {
                    topic = \"/c/${clientid}\"
                },
                {
                    topic = \"/u/${username}\"
                },
                {
                    topic = \"/h/${host}\"
                },
                {
                    topic = \"/p/${port}\"
                },
                {
                    topic = \"/client/${clientid}/username/${username}/host/${host}/port/${port}\"
                },
                {
                    topic = \"/topic/simple\"
                    qos   = 1
                    rh    = 0
                    rap   = 0
                    nl    = 0
                }
            ]
        }">>),
    emqx_common_test_helpers:start_apps([emqx_conf, emqx_dashboard, ?APP],
                                        fun set_special_configs/1),
    Config.

set_special_configs(emqx_dashboard) ->
    Config = #{
        default_username => <<"admin">>,
        default_password => <<"public">>,
        listeners => [#{
                        protocol => http,
                        port => 18083
                       }]
       },
    emqx_config:put([emqx_dashboard], Config),
    ok;
set_special_configs(_) ->
    ok.

topic_config(T) ->
    #{
        topic => T,
        qos   => 0,
        rh    => 0,
        rap   => 0,
        nl    => 0
    }.

end_per_suite(_) ->
    application:unload(emqx_management),
    application:unload(emqx_conf),
    application:unload(?APP),
    meck:unload(emqx_resource),
    meck:unload(emqx_schema),
    emqx_common_test_helpers:stop_apps([emqx_dashboard, emqx_conf, ?APP]).

t_auto_subscribe(_) ->
    emqx_auto_subscribe:update([#{<<"topic">> => Topic} || Topic <- ?TOPICS]),
    {ok, Client} = emqtt:start_link(#{username => ?CLIENT_USERNAME, clientid => ?CLIENT_ID}),
    {ok, _} = emqtt:connect(Client),
    timer:sleep(200),
    ?assertEqual(check_subs(length(?TOPICS)), ok),
    emqtt:disconnect(Client),
    ok.

t_update(_) ->
    Path = emqx_mgmt_api_test_util:api_path(["mqtt", "auto_subscribe"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Body = [#{topic => ?TOPIC_S}],
    {ok, Response} = emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, Body),
    ResponseMap = emqx_json:decode(Response, [return_maps]),
    ?assertEqual(1, erlang:length(ResponseMap)),

    {ok, Client} = emqtt:start_link(#{username => ?CLIENT_USERNAME, clientid => ?CLIENT_ID}),
    {ok, _} = emqtt:connect(Client),
    timer:sleep(100),
    ?assertEqual(check_subs(ets:tab2list(emqx_suboption), [?TOPIC_S]), ok),
    emqtt:disconnect(Client),

    {ok, GETResponse} = emqx_mgmt_api_test_util:request_api(get, Path),
    GETResponseMap = emqx_json:decode(GETResponse, [return_maps]),
    ?assertEqual(1, erlang:length(GETResponseMap)),
    ok.


check_subs(Count) ->
    Subs = ets:tab2list(emqx_suboption),
    ct:pal("--->  ~p ~p ~n", [Subs, Count]),
    ?assert(length(Subs) >= Count),
    check_subs((Subs), ?ENSURE_TOPICS).

check_subs([], []) ->
    ok;
check_subs([{{_, Topic}, #{subid := ?CLIENT_ID}} | Subs], List) ->
    check_subs(Subs, lists:delete(Topic, List));
check_subs([_ | Subs], List) ->
    check_subs(Subs, List).
