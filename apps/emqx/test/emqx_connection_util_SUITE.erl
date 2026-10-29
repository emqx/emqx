%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connection_util_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

-doc "Check the OOM policy against the calling process's own mailbox size.".
t_check_oom(_) ->
    Policy = #{
        max_mailbox_size => 10,
        max_heap_size => 1024 * 1024 * 8,
        enable => true
    },
    [self() ! {msg, I} || I <- lists:seq(1, 5)],
    ?assertEqual(ok, emqx_connection_util:check_oom(Policy)),
    [self() ! {msg, I} || I <- lists:seq(1, 6)],
    ?assertMatch(
        {shutdown, #{
            shutdown_count := mailbox_overflow,
            reason := mailbox_overflow,
            value := 11,
            max := 10
        }},
        emqx_connection_util:check_oom(Policy)
    ).

-doc """
The shutdown reason returned by `check_oom/2` carries the checked
process's label under the `label` key.
""".
t_check_oom_with_label(_) ->
    Policy = #{
        max_mailbox_size => 10,
        max_heap_size => 1024 * 1024 * 8,
        enable => true
    },
    Parent = self(),
    Pid = spawn_link(fun() ->
        proc_lib:set_label({clientid, <<"oom-client">>}),
        Parent ! {ready, self()},
        receive
            stop -> ok
        end
    end),
    receive
        {ready, Pid} -> ok
    end,
    [Pid ! {msg, I} || I <- lists:seq(1, 11)],
    ?assertEqual(
        {shutdown, #{
            shutdown_count => mailbox_overflow,
            reason => mailbox_overflow,
            value => 11,
            max => 10,
            label => {clientid, <<"oom-client">>}
        }},
        emqx_connection_util:check_oom(Pid, Policy)
    ),
    Pid ! stop.

-doc """
When the checked process has no label, the shutdown reason contains no
`label` key and keeps the `shutdown_count`, `reason`, `value` and `max` keys
unchanged.
""".
t_check_oom_without_label(_) ->
    Policy = #{
        max_mailbox_size => 10,
        max_heap_size => 1024 * 1024 * 8,
        enable => true
    },
    Pid = spawn_link(fun() ->
        receive
            stop -> ok
        end
    end),
    [Pid ! {msg, I} || I <- lists:seq(1, 11)],
    ?assertEqual(
        {shutdown, #{
            shutdown_count => mailbox_overflow,
            reason => mailbox_overflow,
            value => 11,
            max => 10
        }},
        emqx_connection_util:check_oom(Pid, Policy)
    ),
    Pid ! stop.

-doc """
Each OOM shutdown reason carries `shutdown_count` set to the exceeded limit, so
esockd counts the exit under that name instead of reporting an unidentified
shutdown at error level.
""".
t_check_oom_shutdown_count(_) ->
    Pid = spawn_link(fun() ->
        receive
            stop -> ok
        end
    end),
    [Pid ! {msg, I} || I <- lists:seq(1, 11)],
    ?assertMatch(
        {shutdown, #{shutdown_count := mailbox_overflow, reason := mailbox_overflow}},
        emqx_connection_util:check_oom(Pid, #{
            enable => true, max_mailbox_size => 10, max_heap_size => 1024 * 1024 * 8
        })
    ),
    ?assertMatch(
        {shutdown, #{shutdown_count := proc_heap_too_large, reason := proc_heap_too_large}},
        emqx_connection_util:check_oom(Pid, #{
            enable => true, max_mailbox_size => 1000, max_heap_size => 1
        })
    ),
    Pid ! stop.
