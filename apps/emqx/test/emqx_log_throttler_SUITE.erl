%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_log_throttler_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% Have to use real msgs, as the schema is guarded by enum.
-define(THROTTLE_MSG, authorization_permission_denied).
-define(THROTTLE_MSG1, cannot_publish_to_topic_due_to_not_authorized).
-define(THROTTLE_UNRECOVERABLE_MSG, unrecoverable_resource_error).
-define(TIME_WINDOW, <<"1s">>).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    %% This test suite can't be run in standalone tests (without emqx_conf)
    case module_exists(emqx_conf) of
        true ->
            Apps = emqx_cth_suite:start(
                [
                    {emqx_conf, #{
                        config =>
                            #{
                                log => #{
                                    throttling => #{
                                        time_window => ?TIME_WINDOW, msgs => [?THROTTLE_MSG]
                                    }
                                }
                            }
                    }},
                    emqx
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            [{suite_apps, Apps} | Config];
        false ->
            {skip, standalone_not_supported}
    end.

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    emqx_config:delete_override_conf_files().

init_per_testcase(t_throttle_recoverable_msg, Config) ->
    ok = snabbkaffe:start_trace(),
    [?THROTTLE_MSG] = Conf = emqx:get_config([log, throttling, msgs]),
    {ok, _} = emqx_conf:update([log, throttling, msgs], [?THROTTLE_UNRECOVERABLE_MSG | Conf], #{}),
    Config;
init_per_testcase(t_throttle_add_new_msg, Config) ->
    ok = snabbkaffe:start_trace(),
    [?THROTTLE_MSG] = Conf = emqx:get_config([log, throttling, msgs]),
    {ok, _} = emqx_conf:update([log, throttling, msgs], [?THROTTLE_MSG1 | Conf], #{}),
    Config;
init_per_testcase(t_throttle_debug_primary_level, Config) ->
    ok = snabbkaffe:start_trace(),
    Level = emqx_logger:get_primary_log_level(),
    [{prev_log_level, Level} | Config];
init_per_testcase(_TC, Config) ->
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(t_throttle_recoverable_msg, _Config) ->
    ok = snabbkaffe:stop(),
    {ok, _} = emqx_conf:update([log, throttling, msgs], [?THROTTLE_MSG], #{}),
    ok;
end_per_testcase(t_throttle_add_new_msg, _Config) ->
    ok = snabbkaffe:stop(),
    {ok, _} = emqx_conf:update([log, throttling, msgs], [?THROTTLE_MSG], #{}),
    ok;
end_per_testcase(t_update_time_window, _Config) ->
    ok = snabbkaffe:stop(),
    {ok, _} = emqx_conf:update([log, throttling, time_window], ?TIME_WINDOW, #{}),
    ok;
end_per_testcase(t_throttle_debug_primary_level, Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_logger:set_primary_log_level(?config(prev_log_level, Config));
end_per_testcase(_TC, _Config) ->
    ok = snabbkaffe:stop().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_throttle(_Config) ->
    ?check_trace(
        begin
            %% Warm-up and block to increase the probability that next events
            %% will be in the same throttling time window.
            {_, {ok, _}} = ?wait_async_action(
                events(?THROTTLE_MSG),
                #{?snk_kind := log_throttler_dropped, throttled_msg := ?THROTTLE_MSG},
                5000
            ),

            ?assert(emqx_log_throttler:allow(?THROTTLE_MSG, undefined)),
            ?assertNot(emqx_log_throttler:allow(?THROTTLE_MSG, undefined)),
            {ok, _} = ?block_until(
                #{
                    ?snk_kind := log_throttler_dropped,
                    throttled_msg := ?THROTTLE_MSG,
                    dropped_count := 1
                },
                3000
            )
        end,
        []
    ).

t_throttle_recoverable_msg(_Config) ->
    ResourceId = <<"resource_id">>,
    ThrottledMsg = iolist_to_binary([atom_to_list(?THROTTLE_UNRECOVERABLE_MSG), ":", ResourceId]),
    ?check_trace(
        begin
            %% Warm-up and block to increase the probability that next events
            %% will be in the same throttling time window.
            {ok, _} = ?block_until(
                #{?snk_kind := log_throttler_new_msg, throttled_msg := ?THROTTLE_UNRECOVERABLE_MSG},
                5000
            ),
            {_, {ok, _}} = ?wait_async_action(
                events(?THROTTLE_UNRECOVERABLE_MSG, ResourceId),
                #{
                    ?snk_kind := log_throttler_dropped,
                    throttled_msg := ThrottledMsg
                },
                5000
            ),

            ?assert(emqx_log_throttler:allow(?THROTTLE_UNRECOVERABLE_MSG, ResourceId)),
            ?assertNot(emqx_log_throttler:allow(?THROTTLE_UNRECOVERABLE_MSG, ResourceId)),
            {ok, _} = ?block_until(
                #{
                    ?snk_kind := log_throttler_dropped,
                    throttled_msg := ThrottledMsg,
                    dropped_count := 1
                },
                3000
            )
        end,
        []
    ).

t_throttle_add_new_msg(_Config) ->
    ?check_trace(
        begin
            {ok, _} = ?block_until(
                #{?snk_kind := log_throttler_new_msg, throttled_msg := ?THROTTLE_MSG1}, 5000
            ),
            ?assert(emqx_log_throttler:allow(?THROTTLE_MSG1, undefined)),
            ?assertNot(emqx_log_throttler:allow(?THROTTLE_MSG1, undefined)),
            {ok, _} = ?block_until(
                #{
                    ?snk_kind := log_throttler_dropped,
                    throttled_msg := ?THROTTLE_MSG1,
                    dropped_count := 1
                },
                3000
            )
        end,
        []
    ).

t_throttle_no_msg(_Config) ->
    %% Must simply pass with no crashes
    Pid = erlang:whereis(emqx_log_throttler),
    ?assert(emqx_log_throttler:allow(no_test_throttle_msg, undefined)),
    ?assert(emqx_log_throttler:allow(no_test_throttle_msg, undefined)),
    %% assert process is not restarted
    ?assertEqual(Pid, erlang:whereis(emqx_log_throttler)),
    %% make a gen_call to ensure the process is alive
    %% note: this call result in an 'unexpected_call' error log.
    ?assertEqual(ignored, gen_server:call(Pid, probe)),
    ok.

t_update_time_window(_Config) ->
    ?check_trace(
        begin
            {_, {ok, _}} = ?wait_async_action(
                emqx_conf:update([log, throttling, time_window], <<"2s">>, #{}),
                #{?snk_kind := log_throttler_sched_refresh, new_period_ms := 2000},
                5000
            ),
            timer:sleep(10),
            ?assert(erlang:is_process_alive(erlang:whereis(emqx_log_throttler)))
        end,
        []
    ).

t_throttle_debug_primary_level(_Config) ->
    ?check_trace(
        begin
            ok = emqx_logger:set_primary_log_level(debug),
            ?assert(lists:all(fun(Allow) -> Allow =:= true end, events(?THROTTLE_MSG))),

            ok = emqx_logger:set_primary_log_level(warning),
            {_, {ok, _}} = ?wait_async_action(
                events(?THROTTLE_MSG),
                #{?snk_kind := log_throttler_dropped, throttled_msg := ?THROTTLE_MSG},
                5000
            ),
            ?assert(emqx_log_throttler:allow(?THROTTLE_MSG, undefined)),
            ?assertNot(emqx_log_throttler:allow(?THROTTLE_MSG, undefined)),
            {ok, _} = ?block_until(
                #{
                    ?snk_kind := log_throttler_dropped,
                    throttled_msg := ?THROTTLE_MSG,
                    dropped_count := 1
                },
                3000
            )
        end,
        []
    ).

%%--------------------------------------------------------------------
%% internal functions
%%--------------------------------------------------------------------

events(Msg) ->
    events(100, Msg, undefined).

events(Msg, Id) ->
    events(100, Msg, Id).

events(N, Msg, Id) ->
    [emqx_log_throttler:allow(Msg, Id) || _ <- lists:seq(1, N)].

module_exists(Mod) ->
    case erlang:module_loaded(Mod) of
        true ->
            true;
        false ->
            case code:ensure_loaded(Mod) of
                ok -> true;
                {module, Mod} -> true;
                _ -> false
            end
    end.
