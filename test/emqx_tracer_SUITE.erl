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

-module(emqx_tracer_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").

all() -> [start_traces].

init_per_suite(Config) ->
    emqx_ct_broker_helpers:run_setup_steps(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_broker_helpers:run_teardown_steps().

start_traces(_Config) ->
    {ok, T} = emqx_client:start_link([{host, "localhost"},
                                      {client_id, <<"client">>},
                                      {username, <<"testuser">>},
                                      {password, <<"pass">>}]),
    emqx_client:connect(T),
    emqx_client:subscribe(T, <<"a/b/c">>),
    ok = emqx_tracer:start_trace({client_id, <<"client">>}, all, "test/emqx_SUITE_data/clientid_trace.log"),
    ok = emqx_tracer:start_trace({topic, <<"topic">>}, all, "test/emqx_SUITE_data/topic_trace.log"),
    {ok, _} = file:read_file("test/emqx_SUITE_data/clientid_trace.log"),
    {ok, _} = file:read_file("test/emqx_SUITE_data/topic_trace.log"),
    Result = emqx_tracer:lookup_traces(),
    ?assertEqual([{{client_id,<<"client">>},{all,"test/emqx_SUITE_data/clientid_trace.log"}},{{topic,<<"topic">>},{all,"test/emqx_SUITE_data/topic_trace.log"}}], Result),
    ok = emqx_tracer:stop_trace({client_id, <<"client">>}),
    ok = emqx_tracer:stop_trace({topic, <<"topic">>}).
