%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_limiter_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("asserts.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

-define(EMQX_CONFIG, #{
    <<"listeners">> => #{
        <<"tcp">> => #{
            <<"default">> => #{
                <<"acceptors">> => 1
            }
        }
    }
}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.
end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    erlang:process_flag(trap_exit, true),
    Apps = emqx_cth_suite:start([{emqx, ?EMQX_CONFIG}], #{
        work_dir => emqx_cth_suite:work_dir(TestCase, Config)
    }),
    snabbkaffe:start_trace(),
    [{apps, Apps} | Config].

end_per_testcase(_TestCase, Config) ->
    snabbkaffe:stop(),
    emqx_cth_suite:stop(?config(apps, Config)).

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_max_conn_listener(_Config) ->
    ?assertWaitEvent(
        begin
            set_limiter_for_listener(max_conn_rate, <<"2/500ms">>),
            ct:sleep(550),
            spawn_connector()
        end,
        #{?snk_kind := esockd_limiter_consume_pause},
        10_000
    ).

t_max_conn_zone(_Config) ->
    ?assertWaitEvent(
        begin
            set_limiter_for_zone(max_conn_rate, <<"2/500ms">>),
            ct:sleep(550),
            spawn_connector()
        end,
        #{?snk_kind := esockd_limiter_consume_pause},
        10_000
    ).

t_max_message_rate_listener(_Config) ->
    ?assertWaitEvent(
        begin
            set_limiter_for_listener(messages_rate, <<"2/500ms">>),
            ct:sleep(550),
            spawn_publisher(100)
        end,
        #{?snk_kind := limiter_exclusive_try_consume, result := false},
        10_000
    ).

t_max_message_rate_zone(_Config) ->
    ?assertWaitEvent(
        begin
            set_limiter_for_zone(messages_rate, <<"2/500ms">>),
            ct:sleep(550),
            spawn_publisher(100)
        end,
        #{?snk_kind := limiter_shared_try_consume, result := false},
        10_000
    ).

t_bytes_rate_listener(_Config) ->
    ?assertWaitEvent(
        begin
            set_limiter_for_listener(bytes_rate, <<"5kb/1m">>),
            ct:sleep(550),
            spawn_publisher(100)
        end,
        #{?snk_kind := limiter_exclusive_try_consume, result := false},
        10_000
    ).

t_bytes_rate_zone(_Config) ->
    ?assertWaitEvent(
        begin
            set_limiter_for_zone(bytes_rate, <<"5kb/1m">>),
            ct:sleep(550),
            spawn_publisher(100)
        end,
        #{?snk_kind := limiter_shared_try_consume, result := false},
        10_000
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

set_limiter_for_zone(Key, Value) ->
    KeyBin = atom_to_binary(Key, utf8),
    MqttConf0 = emqx_config:fill_defaults(#{<<"mqtt">> => emqx:get_raw_config([<<"mqtt">>])}),
    MqttConf1 = emqx_utils_maps:deep_put([<<"mqtt">>, <<"limiter">>, KeyBin], MqttConf0, Value),
    {ok, _} = emqx:update_config([mqtt], maps:get(<<"mqtt">>, MqttConf1)),
    ok = emqx_limiter:update_zone_limiters().

set_limiter_for_listener(Key, Value) ->
    KeyBin = atom_to_binary(Key, utf8),
    emqx:update_config(
        [listeners, tcp, default],
        {update, #{
            KeyBin => Value
        }}
    ),
    ok.

spawn_connector() ->
    spawn_link(fun() ->
        run_connector()
    end).

run_connector() ->
    {ok, C} = emqtt:start_link([{host, "127.0.0.1"}, {port, 1883}]),
    case emqtt:connect(C) of
        {ok, _} ->
            {ok, _} = emqtt:publish(C, <<"test">>, <<"a">>, 1),
            ok = emqtt:stop(C);
        {error, _Reason} ->
            ok
    end,
    ct:sleep(10),
    run_connector().

spawn_publisher(PayloadSize) ->
    spawn_link(fun() ->
        {ok, C} = emqtt:start_link([{host, "127.0.0.1"}, {port, 1883}]),
        {ok, _} = emqtt:connect(C),
        run_publisher(C, PayloadSize)
    end).

run_publisher(C, PayloadSize) ->
    _ = emqtt:publish(C, <<"test">>, binary:copy(<<"a">>, PayloadSize), 1),
    ct:sleep(10),
    run_publisher(C, PayloadSize).
