%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_eviction_agent_channel_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_channel.hrl").

-define(CLIENT_ID, iolist_to_binary(["client_with_session-", atom_to_list(?FUNCTION_NAME)])).

-import(
    emqx_eviction_agent_test_helpers,
    [emqtt_connect/2]
).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx,
            emqx_eviction_agent
        ],
        #{
            work_dir => emqx_cth_suite:work_dir(Config)
        }
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_start_no_session(_Config) ->
    Opts = #{
        clientinfo => #{
            clientid => ?CLIENT_ID,
            zone => default
        },
        conninfo => #{
            clientid => ?CLIENT_ID,
            receive_maximum => 32,
            expiry_interval => 10000
        }
    },
    ?assertMatch(
        {error, {no_session, _}},
        emqx_eviction_agent_channel:start_supervised(Opts)
    ).

t_start_no_expire(_Config) ->
    erlang:process_flag(trap_exit, true),

    _ = emqtt_connect(?CLIENT_ID, false),

    Opts = #{
        clientinfo => #{
            clientid => ?CLIENT_ID,
            zone => default
        },
        conninfo => #{
            clientid => ?CLIENT_ID,
            receive_maximum => 32,
            expiry_interval => 0
        }
    },
    ?assertMatch(
        {error, {should_be_expired, _}},
        emqx_eviction_agent_channel:start_supervised(Opts)
    ).

t_start_infinite_expire(_Config) ->
    erlang:process_flag(trap_exit, true),

    _ = emqtt_connect(?CLIENT_ID, false),

    Opts = #{
        clientinfo => #{
            clientid => ?CLIENT_ID,
            zone => default
        },
        conninfo => #{
            clientid => ?CLIENT_ID,
            receive_maximum => 32,
            expiry_interval => ?EXPIRE_INTERVAL_INFINITE
        }
    },
    ?assertMatch(
        {ok, _},
        emqx_eviction_agent_channel:start_supervised(Opts)
    ).

-doc """
A kicked channel stops with a `shutdown` reason, so that OTP writes no error
report for it. The `session.terminated` hook still gets the reason `kicked`.
""".
t_kick(_Config) ->
    ?assertEqual(
        #{
            stop_reason => {shutdown, kicked},
            session_hook => {'session.terminated', kicked},
            otp_reports => []
        },
        stepdown(?CLIENT_ID, kick)
    ).

-doc """
A discarded channel stops with a `shutdown` reason, so that OTP writes no error
report for it. The `session.discarded` hook still runs.
""".
t_discard(_Config) ->
    ?assertEqual(
        #{
            stop_reason => {shutdown, discarded},
            session_hook => 'session.discarded',
            otp_reports => []
        },
        stepdown(?CLIENT_ID, discard)
    ).

t_stop(_Config) ->
    erlang:process_flag(trap_exit, true),

    _ = emqtt_connect(?CLIENT_ID, false),
    Opts = evict_session_opts(?CLIENT_ID),

    {ok, Pid} = emqx_eviction_agent_channel:start_supervised(Opts),

    ?assertEqual(
        ok,
        emqx_eviction_agent_channel:stop(Pid)
    ).

t_ignored_calls(_Config) ->
    erlang:process_flag(trap_exit, true),

    _ = emqtt_connect(?CLIENT_ID, false),
    Opts = evict_session_opts(?CLIENT_ID),

    {ok, Pid} = emqx_eviction_agent_channel:start_supervised(Opts),

    ok = emqx_eviction_agent_channel:cast(Pid, unknown),
    Pid ! unknown,

    ?assertEqual(
        [],
        emqx_eviction_agent_channel:call(Pid, list_acl_cache)
    ),

    ?assertEqual(
        ok,
        emqx_eviction_agent_channel:call(Pid, {quota, quota})
    ),

    ?assertEqual(
        ignored,
        emqx_eviction_agent_channel:call(Pid, unknown)
    ).

t_expire(_Config) ->
    erlang:process_flag(trap_exit, true),

    _ = emqtt_connect(?CLIENT_ID, false),
    #{conninfo := ConnInfo} = Opts0 = evict_session_opts(?CLIENT_ID),
    Opts1 = Opts0#{conninfo => ConnInfo#{expiry_interval => 1000}},

    {ok, Pid} = emqx_eviction_agent_channel:start_supervised(Opts1),

    MRef = monitor(process, Pid),
    receive
        {'DOWN', MRef, process, Pid, Reason} ->
            ?assertEqual({shutdown, expired}, Reason)
    after 5000 ->
        ct:fail(session_did_not_expire)
    end.

t_get_connected_client_count(_Config) ->
    erlang:process_flag(trap_exit, true),

    _ = emqtt_connect(?CLIENT_ID, false),

    ?assertEqual(
        1,
        emqx_cm:get_connected_client_count()
    ),

    Opts = evict_session_opts(?CLIENT_ID),

    {ok, _} = emqx_eviction_agent_channel:start_supervised(Opts),

    ?assertEqual(
        0,
        emqx_cm:get_connected_client_count()
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

stepdown(ClientId, Req) ->
    %% The linked client exits when the channel takes its session over.
    erlang:process_flag(trap_exit, true),
    _ = emqtt_connect(ClientId, false),
    {ok, Pid} = emqx_eviction_agent_channel:start_supervised(evict_session_opts(ClientId)),
    MRef = monitor(process, Pid),
    TestPid = self(),
    ok = emqx_hooks:add('session.terminated', {?MODULE, on_session_terminated, [TestPid]}, 0),
    ok = emqx_hooks:add('session.discarded', {?MODULE, on_session_discarded, [TestPid]}, 0),
    try
        Reports = emqx_cth_log_capture:capture(error, fun() ->
            ?assertEqual(ok, emqx_eviction_agent_channel:call(Pid, Req)),
            receive
                {'DOWN', MRef, process, Pid, StopReason} ->
                    TestPid ! {stop_reason, Pid, StopReason}
            after 5000 ->
                ct:fail(channel_did_not_stop)
            end
        end),
        #{
            stop_reason => received(stop_reason, Pid),
            session_hook => received(session_hook, Pid),
            %% Reports from `gen_server`, `proc_lib` and `supervisor`.
            otp_reports => [R || #{label := _} = R <- Reports]
        }
    after
        emqx_hooks:del('session.terminated', {?MODULE, on_session_terminated}),
        emqx_hooks:del('session.discarded', {?MODULE, on_session_discarded})
    end.

%% The hooks run in the process which terminates the session.
on_session_terminated(_ClientInfo, Reason, _SessionInfo, TestPid) ->
    TestPid ! {session_hook, self(), {'session.terminated', Reason}},
    ok.

on_session_discarded(_ClientInfo, _SessionInfo, TestPid) ->
    TestPid ! {session_hook, self(), 'session.discarded'},
    ok.

received(Tag, Pid) ->
    receive
        {Tag, Pid, Value} -> Value
    after 0 ->
        ct:fail({not_received, Tag})
    end.

evict_session_opts(ClientId) ->
    maps:with(
        [conninfo, clientinfo],
        emqx_cm:get_chan_info(ClientId)
    ).
