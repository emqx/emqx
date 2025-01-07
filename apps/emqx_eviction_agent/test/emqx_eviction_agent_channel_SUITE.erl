%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_eviction_agent_channel_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_channel.hrl").

-define(CLIENT_ID, <<"client_with_session">>).

-import(
    emqx_eviction_agent_test_helpers,
    [emqtt_connect/0, emqtt_connect/2]
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

t_kick(_Config) ->
    erlang:process_flag(trap_exit, true),

    _ = emqtt_connect(?CLIENT_ID, false),
    Opts = evict_session_opts(?CLIENT_ID),

    {ok, Pid} = emqx_eviction_agent_channel:start_supervised(Opts),

    ?assertEqual(
        ok,
        emqx_eviction_agent_channel:call(Pid, kick)
    ).

t_discard(_Config) ->
    erlang:process_flag(trap_exit, true),

    _ = emqtt_connect(?CLIENT_ID, false),
    Opts = evict_session_opts(?CLIENT_ID),

    {ok, Pid} = emqx_eviction_agent_channel:start_supervised(Opts),

    ?assertEqual(
        ok,
        emqx_eviction_agent_channel:call(Pid, discard)
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
    Opts1 = Opts0#{conninfo => ConnInfo#{expiry_interval => 1}},

    {ok, Pid} = emqx_eviction_agent_channel:start_supervised(Opts1),

    ct:sleep(1500),

    ?assertNot(is_process_alive(Pid)).

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

evict_session_opts(ClientId) ->
    maps:with(
        [conninfo, clientinfo],
        emqx_cm:get_chan_info(ClientId)
    ).
