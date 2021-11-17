%%--------------------------------------------------------------------
%% Copyright (c) 2019-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_tracer_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").
-define(CLIENT, [{host, "localhost"},
                 {clientid, <<"client">>},
                 {username, <<"testuser">>},
                 {password, <<"pass">>}
                ]).

all() -> [t_trace_clientid, t_trace_topic, t_is_match].

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

t_trace_clientid(_Config) ->
    {ok, T} = emqtt:start_link(?CLIENT),
    emqtt:connect(T),

    %% Start tracing
    emqx_logger:set_log_level(error),
    {error, _} = emqx_tracer:start_trace(clientid, <<"client">>, debug, "tmp/client.log"),
    emqx_logger:set_log_level(debug),
    %% add list clientid
    ok = emqx_tracer:start_trace(clientid, "client", debug, "tmp/client.log"),
    ok = emqx_tracer:start_trace(clientid, <<"client2">>, all, "tmp/client2.log"),
    ok = emqx_tracer:start_trace(clientid, <<"client3">>, all, "tmp/client3.log"),
    {error, {invalid_log_level, bad_level}} =
        emqx_tracer:start_trace(clientid, <<"client4">>, bad_level, "tmp/client4.log"),
    {error, {handler_not_added, {file_error, ".", eisdir}}} =
        emqx_tracer:start_trace(clientid, <<"client5">>, debug, "."),
    ct:sleep(100),

    %% Verify the tracing file exits
    ?assert(filelib:is_regular("tmp/client.log")),
    ?assert(filelib:is_regular("tmp/client2.log")),
    ?assert(filelib:is_regular("tmp/client3.log")),

    %% Get current traces
    ?assertEqual([#{type => clientid, clientid => <<"client">>,
        name => <<"client">>, level => debug, dst => "tmp/client.log"},
        #{type => clientid, clientid => <<"client2">>,
            name => <<"client2">>, level => debug, dst => "tmp/client2.log"},
        #{type => clientid, clientid => <<"client3">>,
            name => <<"client3">>, level => debug, dst => "tmp/client3.log"}
    ], emqx_tracer:lookup_traces()),

    %% set the overall log level to debug
    emqx_logger:set_log_level(debug),

    %% Client with clientid = "client" publishes a "hi" message to "a/b/c".
    emqtt:publish(T, <<"a/b/c">>, <<"hi">>),
    ct:sleep(200),

    %% Verify messages are logged to "tmp/client.log" but not "tmp/client2.log".
    ?assert(filelib:file_size("tmp/client.log") > 0),
    ?assert(filelib:file_size("tmp/client2.log") == 0),

    %% Stop tracing
    ok = emqx_tracer:stop_trace(clientid, <<"client">>),
    ok = emqx_tracer:stop_trace(clientid, <<"client2">>),
    ok = emqx_tracer:stop_trace(clientid, <<"client3">>),
    emqtt:disconnect(T),

    emqx_logger:set_log_level(warning).

t_trace_topic(_Config) ->
    {ok, T} = emqtt:start_link(?CLIENT),
    emqtt:connect(T),

    %% Start tracing
    emqx_logger:set_log_level(debug),
    ok = emqx_tracer:start_trace(topic, <<"x/#">>, all, "tmp/topic_trace_x.log"),
    ok = emqx_tracer:start_trace(topic, <<"y/#">>, all, "tmp/topic_trace_y.log"),
    ct:sleep(100),

    %% Verify the tracing file exits
    ?assert(filelib:is_regular("tmp/topic_trace_x.log")),
    ?assert(filelib:is_regular("tmp/topic_trace_y.log")),

    %% Get current traces
    ?assertEqual([#{type => topic, topic => <<"x/#">>, name => <<"x/#">>,
        level => debug, dst => "tmp/topic_trace_x.log"},
        #{type => topic, topic => <<"y/#">>, name => <<"y/#">>,
            level => debug, dst => "tmp/topic_trace_y.log"}],
        emqx_tracer:lookup_traces()),

    %% set the overall log level to debug
    emqx_logger:set_log_level(debug),

    %% Client with clientid = "client" publishes a "hi" message to "x/y/z".
    emqtt:publish(T, <<"x/y/z">>, <<"hi1">>),
    emqtt:publish(T, <<"x/y/z">>, <<"hi2">>),
    ct:sleep(200),

    ?assert(filelib:file_size("tmp/topic_trace_x.log") > 0),
    ?assert(filelib:file_size("tmp/topic_trace_y.log") =:= 0),

    %% Stop tracing
    ok = emqx_tracer:stop_trace(topic, <<"x/#">>),
    ok = emqx_tracer:stop_trace(topic, <<"y/#">>),
    {error, _Reason} = emqx_tracer:stop_trace(topic, <<"z/#">>),
    emqtt:disconnect(T),

    emqx_logger:set_log_level(warning).

t_is_match(_Config) ->
    ClientId = <<"test">>,
    ?assert(emqx_tracer:is_match(#{clientid => ClientId, type => clientid, labels => []},
        #{clientid => ClientId}, warning)),
    ?assert(emqx_tracer:is_match(#{clientid => ClientId, type => clientid, labels => []},
        #{clientid => ClientId}, debug)),
    ?assertNot(emqx_tracer:is_match(#{clientid => ClientId, type => clientid,
        labels => ['PUBLISH']}, #{clientid => ClientId}, debug)),
    ?assert(emqx_tracer:is_match(#{clientid => ClientId, type => clientid, labels => []},
        #{clientid => ClientId, trace_label => 'PUBLISH'}, debug)),
    ?assert(emqx_tracer:is_match(#{clientid => ClientId, type => clientid, labels => ['PUBLISH']},
        #{clientid => ClientId, trace_label => 'PUBLISH'}, debug)),
    ?assertNot(emqx_tracer:is_match(#{clientid => ClientId, type => clientid, labels => ['SUBACK']},
        #{clientid => ClientId, trace_label => 'PUBLISH'}, debug)),
    ?assert(emqx_tracer:is_match(#{clientid => ClientId, type => clientid, labels => ['SUBACK']},
        #{clientid => ClientId, trace_label => 'ALL'}, debug)),
    ?assertNot(emqx_tracer:is_match(#{clientid => ClientId, type => clientid, labels => []},
        #{clientid => <<"Bad">>}, warning)),
    ?assertNot(emqx_tracer:is_match(#{clientid => ClientId, type => clientid, labels => []},
        #{clientid => <<"Bad">>, trace_label => 'PUBLISH'}, debug)),

    Topic = <<"/test/#">>,
    ?assert(emqx_tracer:is_match(#{type => topic, topic => Topic, labels => []},
        #{topic => <<"/test/1">>}, warning)),
    ?assert(emqx_tracer:is_match(#{type => topic, topic => Topic, labels => []},
        #{topic => <<"/test/1/2">>}, debug)),
    ?assertNot(emqx_tracer:is_match(#{type => topic, topic => Topic, labels => ['SUBSCRIBE']},
        #{topic => <<"/test/1/2">>}, debug)),
    ?assert(emqx_tracer:is_match(#{type => topic, topic => Topic, labels => []},
        #{topic => <<"/test/3">>, trace_label => 'PUBLISH'}, debug)),
    ?assert(emqx_tracer:is_match(#{type => topic, topic => Topic, labels => ['PUBLISH']},
        #{topic => <<"/test/398/">>, trace_label => 'PUBLISH'}, debug)),
    ?assertNot(emqx_tracer:is_match(#{type => topic, topic => Topic, labels => ['SUBACK']},
        #{topic => <<"/test/1/xy/y">>, trace_label => 'PUBLISH'}, debug)),

    ?assertNot(emqx_tracer:is_match(#{type => topic, topic => Topic, labels => ['PUBLISH']},
        #{topic => <<"/t1est/398/">>, trace_label => 'PUBLISH'}, debug)),
    ?assertNot(emqx_tracer:is_match(#{type => topic, topic => Topic, labels => []},
        #{topic => <<"/t1est/1/xy/y">>, trace_label => 'PUBLISH'}, debug)),
    ok.
