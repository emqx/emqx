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
-module(emqx_broker_mon_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(mnesia_tm_mailbox_size_alarm_threshold, 50).
-define(broker_pool_max_threshold, 100).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    persistent_term:put({emqx_broker_mon, update_interval}, 100),
    Apps = emqx_cth_suite:start(
        [
            {emqx, #{
                config =>
                    #{
                        <<"sysmon">> => #{
                            <<"mnesia_tm_mailbox_size_alarm_threshold">> => ?mnesia_tm_mailbox_size_alarm_threshold,
                            <<"broker_pool_mailbox_size_alarm_threshold">> => ?broker_pool_max_threshold
                        }
                    }
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    persistent_term:erase({emqx_broker_mon, update_interval}),
    Apps = ?config(apps, Config),
    ok = emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TestCase, Config) ->
    ok = emqx_alarm:delete_all_deactivated_alarms(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

send_messages(Pid, N) ->
    lists:foreach(
        fun(M) ->
            Pid ! M
        end,
        lists:seq(1, N)
    ).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_mnesia_tm_overload(_Config) ->
    ?assertEqual(0, emqx_broker_mon:get_mnesia_tm_mailbox_size()),
    ?assertEqual([], emqx_alarm:get_alarms()),
    on_exit(fun() -> sys:resume(mnesia_tm) end),
    ct:pal("suspending mnesia_tm"),
    ok = sys:suspend(mnesia_tm),
    send_messages(mnesia_tm, ?mnesia_tm_mailbox_size_alarm_threshold),
    %% Note: we expect the sent messages _or more_ here because any other process that
    %% tries to interact with mnesia concurrently will also make more messages appear in
    %% `mnesia_tm''s mailbox...
    ?retry(
        100,
        5,
        ?assert(
            ?mnesia_tm_mailbox_size_alarm_threshold =< emqx_broker_mon:get_mnesia_tm_mailbox_size(),
            #{mailbox => process_info(whereis(mnesia_tm), messages)}
        )
    ),
    %% We can't directly get the alarms right now because that involves transactions...
    %% We'll have to rely on the history after we unblock `mnesia_tm'.
    send_messages(mnesia_tm, 1),
    ?retry(
        100,
        5,
        ?assert(
            ?mnesia_tm_mailbox_size_alarm_threshold < emqx_broker_mon:get_mnesia_tm_mailbox_size(),
            #{mailbox => process_info(whereis(mnesia_tm), messages)}
        )
    ),
    ct:pal("resuming mnesia_tm"),
    ok = sys:resume(mnesia_tm),
    ?retry(
        100,
        10,
        ?assertMatch(
            [
                #{
                    message := <<"mnesia overloaded; mailbox size: ", _/binary>>,
                    name := <<"mnesia_transaction_manager_overload">>,
                    details := #{mailbox_size := _}
                }
            ],
            emqx_alarm:get_alarms(deactivated)
        )
    ),
    ?retry(
        100,
        5,
        ?assertEqual(0, emqx_broker_mon:get_mnesia_tm_mailbox_size())
    ),
    ?assertEqual([], emqx_alarm:get_alarms(activated)),
    ok.

t_broker_pool_overload(_Config) ->
    ?assertEqual(0, emqx_broker_mon:get_broker_pool_max_mailbox_size()),
    ?assertEqual([], emqx_alarm:get_alarms()),
    WorkerPids = emqx_broker_sup:get_broker_pool_workers(),
    on_exit(fun() -> lists:foreach(fun sys:resume/1, WorkerPids) end),
    ct:pal("suspending broker workers"),
    lists:foreach(fun sys:suspend/1, WorkerPids),
    lists:foreach(
        fun({N, WorkerPid}) ->
            send_messages(WorkerPid, N)
        end,
        lists:enumerate(WorkerPids)
    ),
    ?retry(
        100,
        5,
        ?assertEqual(length(WorkerPids), emqx_broker_mon:get_broker_pool_max_mailbox_size())
    ),
    MessagesUntilHighWatermark = ?broker_pool_max_threshold - length(WorkerPids),
    case MessagesUntilHighWatermark > 0 of
        false ->
            ok;
        true ->
            send_messages(lists:last(WorkerPids), MessagesUntilHighWatermark + 1)
    end,
    ?retry(
        100,
        5,
        ?assertEqual(
            ?broker_pool_max_threshold + 1,
            emqx_broker_mon:get_broker_pool_max_mailbox_size()
        )
    ),
    ?retry(
        100,
        5,
        ?assertMatch(
            [
                #{
                    message := <<"broker pool overloaded; mailbox size: ", _/binary>>,
                    name := <<"broker_pool_overload">>,
                    details := #{mailbox_size := _}
                }
            ],
            emqx_alarm:get_alarms(activated)
        )
    ),
    ct:pal("resuming broker workers"),
    lists:foreach(fun sys:resume/1, WorkerPids),
    ?retry(
        100,
        5,
        ?assertEqual(0, emqx_broker_mon:get_broker_pool_max_mailbox_size())
    ),
    ?retry(100, 5, ?assertEqual([], emqx_alarm:get_alarms(activated))),
    ok.
