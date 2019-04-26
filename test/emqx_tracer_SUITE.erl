%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

start_traces(_Config) ->
    {ok, T} = emqx_client:start_link([{host, "localhost"},
                                      {client_id, <<"client">>},
                                      {username, <<"testuser">>},
                                      {password, <<"pass">>}]),
    emqx_client:connect(T),

    %% Start tracing
    emqx_logger:set_log_level(error),
    {error, _} = emqx_tracer:start_trace({client_id, <<"client">>}, debug, "tmp/client.log"),
    emqx_logger:set_log_level(debug),
    ok = emqx_tracer:start_trace({client_id, <<"client">>}, debug, "tmp/client.log"),
    ok = emqx_tracer:start_trace({client_id, <<"client2">>}, all, "tmp/client2.log"),
    {error, invalid_log_level} = emqx_tracer:start_trace({client_id, <<"client3">>}, bad_level, "tmp/client3.log"),
    ok = emqx_tracer:start_trace({topic, <<"a/#">>}, all, "tmp/topic_trace.log"),
    ct:sleep(100),

    %% Verify the tracing file exits
    ?assert(filelib:is_regular("tmp/client.log")),
    ?assert(filelib:is_regular("tmp/client2.log")),
    ?assert(filelib:is_regular("tmp/topic_trace.log")),

    %% Get current traces
    ?assertEqual([{{client_id,<<"client">>},{debug,"tmp/client.log"}},
                  {{client_id,<<"client2">>},{all,"tmp/client2.log"}},
                  {{topic,<<"a/#">>},{all,"tmp/topic_trace.log"}}], emqx_tracer:lookup_traces()),

    %% set the overall log level to debug
    emqx_logger:set_log_level(debug),

    %% Client with clientid = "client" publishes a "hi" message to "a/b/c".
    emqx_client:publish(T, <<"a/b/c">>, <<"hi">>),
    ct:sleep(200),

    %% Verify messages are logged to "tmp/client.log" and "tmp/topic_trace.log", but not "tmp/client2.log".
    ?assert(filelib:file_size("tmp/client.log") > 0),
    ?assert(filelib:file_size("tmp/topic_trace.log") > 0),
    ?assert(filelib:file_size("tmp/client2.log") == 0),

    %% Stop tracing
    ok = emqx_tracer:stop_trace({client_id, <<"client">>}),
    ok = emqx_tracer:stop_trace({client_id, <<"client2">>}),
    ok = emqx_tracer:stop_trace({topic, <<"a/#">>}),
    emqx_client:disconnect(T),

    emqx_logger:set_log_level(error).
