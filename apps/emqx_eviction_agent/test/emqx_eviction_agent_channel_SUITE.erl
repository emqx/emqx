%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_eviction_agent_channel_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_channel.hrl").

-define(CLIENT_ID, <<"client_with_session">>).

-import(
    emqx_eviction_agent_test_helpers,
    [emqtt_connect/0, emqtt_connect/2]
).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:start_apps([emqx_conf, emqx_eviction_agent]),
    {ok, _} = emqx:update_config([rpc, port_discovery], manual),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([emqx_eviction_agent, emqx_conf]).

init_per_testcase(t_persistence, Config) ->
    emqx_config:put([persistent_session_store, enabled], true),
    {ok, _} = emqx_persistent_session_sup:start_link(),
    emqx_persistent_session:init_db_backend(),
    ?assert(emqx_persistent_session:is_store_enabled()),
    Config;
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(t_persistence, Config) ->
    emqx_config:put([persistent_session_store, enabled], false),
    emqx_persistent_session:init_db_backend(),
    ?assertNot(emqx_persistent_session:is_store_enabled()),
    Config;
end_per_testcase(_TestCase, _Config) ->
    ok.

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

t_persistence(_Config) ->
    erlang:process_flag(trap_exit, true),

    Topic = <<"t1">>,
    Message = <<"message_to_persist">>,

    {ok, C0} = emqtt_connect(?CLIENT_ID, false),
    {ok, _, _} = emqtt:subscribe(C0, Topic, 0),

    Opts = evict_session_opts(?CLIENT_ID),
    {ok, Pid} = emqx_eviction_agent_channel:start_supervised(Opts),

    {ok, C1} = emqtt_connect(),
    {ok, _} = emqtt:publish(C1, Topic, Message, 1),
    ok = emqtt:disconnect(C1),

    %% Kill channel so that the session is only persisted
    ok = emqx_eviction_agent_channel:call(Pid, kick),

    %% Should restore session from persistents storage and receive messages
    {ok, C2} = emqtt_connect(?CLIENT_ID, false),

    receive
        {publish, #{
            payload := Message,
            topic := Topic
        }} ->
            ok
    after 1000 ->
        ct:fail("message not received")
    end,

    ok = emqtt:disconnect(C2).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

evict_session_opts(ClientId) ->
    maps:with(
        [conninfo, clientinfo],
        emqx_cm:get_chan_info(ClientId)
    ).
