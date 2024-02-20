%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

init_per_testcase(t_throttle_add_new_msg, Config) ->
    ok = snabbkaffe:start_trace(),
    [?THROTTLE_MSG] = Conf = emqx:get_config([log, throttling, msgs]),
    {ok, _} = emqx_conf:update([log, throttling, msgs], [?THROTTLE_MSG1 | Conf], #{}),
    Config;
init_per_testcase(_TC, Config) ->
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(t_throttle_add_new_msg, _Config) ->
    ok = snabbkaffe:stop(),
    {ok, _} = emqx_conf:update([log, throttling, msgs], [?THROTTLE_MSG], #{}),
    ok;
end_per_testcase(t_update_time_window, _Config) ->
    ok = snabbkaffe:stop(),
    {ok, _} = emqx_conf:update([log, throttling, time_window], ?TIME_WINDOW, #{}),
    ok;
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
            lists:foreach(
                fun(_) -> emqx_log_throttler:allow(warning, ?THROTTLE_MSG) end,
                lists:seq(1, 100)
            ),
            {ok, _} = ?block_until(
                #{?snk_kind := log_throttler_dropped, throttled_msg := ?THROTTLE_MSG}, 5000
            ),

            ?assert(emqx_log_throttler:allow(warning, ?THROTTLE_MSG)),
            ?assertNot(emqx_log_throttler:allow(warning, ?THROTTLE_MSG)),
            %% Debug is always allowed
            ?assert(emqx_log_throttler:allow(debug, ?THROTTLE_MSG)),
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

t_throttle_add_new_msg(_Config) ->
    ?check_trace(
        begin
            ?block_until(
                #{?snk_kind := log_throttler_new_msg, throttled_msg := ?THROTTLE_MSG1}, 5000
            ),
            ?assert(emqx_log_throttler:allow(warning, ?THROTTLE_MSG1)),
            ?assertNot(emqx_log_throttler:allow(warning, ?THROTTLE_MSG1)),
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
    ?assert(emqx_log_throttler:allow(warning, no_test_throttle_msg)),
    ?assert(emqx_log_throttler:allow(warning, no_test_throttle_msg)),
    timer:sleep(10),
    ?assert(erlang:is_process_alive(erlang:whereis(emqx_log_throttler))).

t_update_time_window(_Config) ->
    ?check_trace(
        begin
            ?wait_async_action(
                emqx_conf:update([log, throttling, time_window], <<"2s">>, #{}),
                #{?snk_kind := log_throttler_sched_refresh, new_period_ms := 2000},
                5000
            ),
            timer:sleep(10),
            ?assert(erlang:is_process_alive(erlang:whereis(emqx_log_throttler)))
        end,
        []
    ).

%%--------------------------------------------------------------------
%% internal functions
%%--------------------------------------------------------------------

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
