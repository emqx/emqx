
%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_session_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").

all() -> [t_session_all].

init_per_suite(Config) ->
    emqx_ct_broker_helpers:run_setup_steps(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_broker_helpers:run_teardown_steps().

t_session_all(_) ->
    ClientId = <<"ClientId">>,
    {ok, ConnPid} = emqx_mock_client:start_link(ClientId),
    {ok, SPid} = emqx_mock_client:open_session(ConnPid, ClientId, internal),
    Message1 = emqx_message:make(<<"ClientId">>, 2, <<"topic">>, <<"hello">>),
    emqx_session:subscribe(SPid, [{<<"topic">>, #{qos => 2}}]),
    emqx_session:subscribe(SPid, [{<<"topic">>, #{qos => 1}}]),
    timer:sleep(200),
    [{<<"topic">>, _}] = emqx:subscriptions({SPid, <<"ClientId">>}),
    emqx_session:publish(SPid, 1, Message1),
    timer:sleep(200),
    {publish, 1, _} = emqx_mock_client:get_last_message(ConnPid),
    emqx_session:puback(SPid, 2),
    emqx_session:puback(SPid, 3, reasoncode),
    emqx_session:pubrec(SPid, 4),
    emqx_session:pubrec(SPid, 5, reasoncode),
    emqx_session:pubrel(SPid, 6, reasoncode),
    emqx_session:pubcomp(SPid, 7, reasoncode),
    timer:sleep(200),
    2 = emqx_metrics:val('packets/puback/missed'),
    2 = emqx_metrics:val('packets/pubrec/missed'),
    1 = emqx_metrics:val('packets/pubrel/missed'),
    1 = emqx_metrics:val('packets/pubcomp/missed'),
    Attrs = emqx_session:attrs(SPid),
    Info = emqx_session:info(SPid),
    Stats = emqx_session:stats(SPid),
    ClientId = proplists:get_value(client_id, Attrs),
    ClientId = proplists:get_value(client_id, Info),
    1 = proplists:get_value(subscriptions_count, Stats),
    emqx_session:unsubscribe(SPid, [<<"topic">>]),
    timer:sleep(200),
    [] = emqx:subscriptions({SPid, <<"clientId">>}),
    emqx_mock_client:close_session(ConnPid, SPid).
