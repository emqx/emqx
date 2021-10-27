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
-include_lib("emqx_st_statistics/include/emqx_st_statistics.hrl").

-define(CONTENT_TYPE, "application/x-www-form-urlencoded").

-define(HOST, "http://127.0.0.1:18083/").

-define(API_VERSION, "v5").

-define(BASE_PATH, "api").

-define(CONF_DEFAULT, <<"""
emqx_st_statistics {
  threshold_time = 10s
  top_k_num = 500
  notice_batch_size = 500
                    time_window = 5m
                    max_log_num = 500
                    ignore_before_create = true
                   }
""">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ok = emqx_config:init_load(emqx_st_statistics_schema, ?CONF_DEFAULT),
    emqx_mgmt_api_test_util:init_suite([emqx_st_statistics]),
    {ok, _} = application:ensure_all_started(emqx_authn),
    Config.

end_per_suite(Config) ->
    application:stop(emqx_authn),
    emqx_mgmt_api_test_util:end_suite([emqx_st_statistics]),
    Config.

init_per_testcase(_, Config) ->
    application:ensure_all_started(emqx_st_statistics),
    timer:sleep(500),
    Config.

end_per_testcase(_, Config) ->
    application:stop(emqx_st_statistics),
    Config.

t_get_history(_) ->

    ets:insert(?TOPK_TAB, #top_k{rank = 1,
                                 topic = <<"test">>,
                                 average_count = 12,
                                 average_elapsed = 1500}),

    {ok, Data} = request_api(get, api_path(["slow_topic"]), "page=1&limit=10",
                             auth_header_()),

    ShouldReturn = #{data => [#{topic => <<"test">>,
                                rank => 1,
                                elapsed => 1500,
                                count => 12}]},

    Return = emqx_map_lib:unsafe_atom_key_map(
               emqx_json:decode(Data, [return_maps])),

    ?assertEqual(ShouldReturn, Return).

t_clear(_) ->
    ets:insert(?TOPK_TAB, #top_k{rank = 1,
                                 topic = <<"test">>,
                                 average_count = 12,
                                 average_elapsed = 1500}),

    {ok, _} = request_api(delete, api_path(["slow_topic"]), [],
                          auth_header_()),

    ?assertEqual(0, ets:info(?TOPK_TAB, size)).

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
    case httpc:request(Method, Request, [], [{body_format, binary}]) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _, Return} }
          when Code =:= 200 orelse Code =:= 204 ->
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
