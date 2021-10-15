%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_subscription_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(CLIENTID, <<"api_clientid">>).
-define(USERNAME, <<"api_username">>).

%% notice: integer topic for sort response
-define(TOPIC1, <<"/t/0000">>).
-define(TOPIC2, <<"/t/0001">>).

-define(TOPIC_SORT, #{?TOPIC1 => 1, ?TOPIC2 => 2}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_mgmt_api_test_util:init_suite(),
    Config.

end_per_suite(_) ->
    emqx_mgmt_api_test_util:end_suite().

t_subscription_api(_) ->
    {ok, Client} = emqtt:start_link(#{username => ?USERNAME, clientid => ?CLIENTID}),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, ?TOPIC1),
    {ok, _, _} = emqtt:subscribe(Client, ?TOPIC2),
    Path = emqx_mgmt_api_test_util:api_path(["subscriptions"]),
    {ok, Response} = emqx_mgmt_api_test_util:request_api(get, Path),
    Data = emqx_json:decode(Response, [return_maps]),
    Meta = maps:get(<<"meta">>, Data),
    ?assertEqual(1, maps:get(<<"page">>, Meta)),
    ?assertEqual(emqx_mgmt:max_row_limit(), maps:get(<<"limit">>, Meta)),
    ?assertEqual(2, maps:get(<<"count">>, Meta)),
    Subscriptions = maps:get(<<"data">>, Data),
    ?assertEqual(length(Subscriptions), 2),
    Sort =
        fun(#{<<"topic">> := T1}, #{<<"topic">> := T2}) ->
            maps:get(T1, ?TOPIC_SORT) =< maps:get(T2, ?TOPIC_SORT)
        end,
    [Subscriptions1, Subscriptions2]  = lists:sort(Sort, Subscriptions),
    ?assertEqual(maps:get(<<"topic">>, Subscriptions1), ?TOPIC1),
    ?assertEqual(maps:get(<<"topic">>, Subscriptions2), ?TOPIC2),
    ?assertEqual(maps:get(<<"clientid">>, Subscriptions1), ?CLIENTID),
    ?assertEqual(maps:get(<<"clientid">>, Subscriptions2), ?CLIENTID),

    QsTopic = "topic=" ++ <<"%2Ft%2F0001">>,
    Headers = emqx_mgmt_api_test_util:auth_header_(),
    {ok, ResponseTopic1} = emqx_mgmt_api_test_util:request_api(get, Path, QsTopic, Headers),
    DataTopic1 = emqx_json:decode(ResponseTopic1, [return_maps]),
    Meta1 = maps:get(<<"meta">>, DataTopic1),
    ?assertEqual(1, maps:get(<<"page">>, Meta1)),
    ?assertEqual(emqx_mgmt:max_row_limit(), maps:get(<<"limit">>, Meta1)),
    ?assertEqual(1, maps:get(<<"count">>, Meta1)),
    Subscriptions_qs1 = maps:get(<<"data">>, DataTopic1),
    ?assertEqual(length(Subscriptions_qs1), 1),


    emqtt:disconnect(Client).
