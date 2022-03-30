%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway_cm_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(GWNAME, mqttsn).
-define(CLIENTID, <<"client1">>).

-define(CONF_DEFAULT, <<"gateway {}">>).

%%--------------------------------------------------------------------
%% setups
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Conf) ->
    emqx_config:erase(gateway),
    emqx_common_test_helpers:load_config(emqx_gateway_schema, ?CONF_DEFAULT),
    emqx_common_test_helpers:start_apps([]),

    ok = meck:new(emqx_gateway_metrics, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_gateway_metrics, inc, fun(_, _) -> ok end),
    Conf.

end_per_suite(_Conf) ->
    meck:unload(emqx_gateway_metrics),
    emqx_common_test_helpers:stop_apps([]),
    emqx_config:delete_override_conf_files().

init_per_testcase(_TestCase, Conf) ->
    process_flag(trap_exit, true),
    {ok, CMPid} = emqx_gateway_cm:start_link([{gwname, ?GWNAME}]),
    [{cm, CMPid} | Conf].

end_per_testcase(_TestCase, Conf) ->
    CMPid = proplists:get_value(cm, Conf),
    gen_server:stop(CMPid),
    process_flag(trap_exit, false),
    Conf.

%%--------------------------------------------------------------------
%% cases
%%--------------------------------------------------------------------

t_open_session(_) ->
    {ok, #{
        present := false,
        session := #{}
    }} = emqx_gateway_cm:open_session(
        ?GWNAME,
        false,
        clientinfo(),
        conninfo(),
        fun(_, _) -> #{} end
    ),

    {ok, SessionRes} = emqx_gateway_cm:open_session(
        ?GWNAME,
        true,
        clientinfo(),
        conninfo(),
        fun(_, _) -> #{no => 1} end
    ),
    ?assertEqual(
        #{
            present => false,
            session => #{no => 1}
        },
        SessionRes
    ),

    %% assert1. check channel infos in ets table
    Chann = {?CLIENTID, self()},
    ?assertEqual(
        [Chann],
        ets:tab2list(emqx_gateway_cm:tabname(chan, ?GWNAME))
    ),
    ?assertEqual(
        [{Chann, ?MODULE}],
        ets:tab2list(emqx_gateway_cm:tabname(conn, ?GWNAME))
    ),

    %% assert2. discard the presented session

    {ok, SessionRes2} = emqx_gateway_cm:open_session(
        ?GWNAME,
        true,
        clientinfo(),
        conninfo(),
        fun(_, _) -> #{no => 2} end
    ),
    ?assertEqual(
        #{
            present => false,
            session => #{no => 2}
        },
        SessionRes2
    ),

    emqx_gateway_cm:insert_channel_info(
        ?GWNAME,
        ?CLIENTID,
        #{clientinfo => clientinfo(), conninfo => conninfo()},
        []
    ),
    ?assertEqual(
        1,
        ets:info(emqx_gateway_cm:tabname(info, ?GWNAME), size)
    ),

    receive
        discard ->
            emqx_gateway_cm:connection_closed(?GWNAME, ?CLIENTID),
            emqx_gateway_cm:unregister_channel(?GWNAME, ?CLIENTID)
    after 100 ->
        ?assert(false, "waiting discard msg timeout")
    end,

    %% assert3. no channel infos in ets table
    ?assertEqual(
        [],
        ets:tab2list(emqx_gateway_cm:tabname(chan, ?GWNAME))
    ),
    ?assertEqual(
        [],
        ets:tab2list(emqx_gateway_cm:tabname(conn, ?GWNAME))
    ),
    ?assertEqual(
        [],
        ets:tab2list(emqx_gateway_cm:tabname(info, ?GWNAME))
    ).

t_get_set_chan_info_stats(_) ->
    {ok, SessionRes} = emqx_gateway_cm:open_session(
        ?GWNAME,
        true,
        clientinfo(),
        conninfo(),
        fun(_, _) -> #{no => 1} end
    ),
    ?assertEqual(
        #{
            present => false,
            session => #{no => 1}
        },
        SessionRes
    ),
    emqx_gateway_cm:insert_channel_info(
        ?GWNAME,
        ?CLIENTID,
        #{clientinfo => clientinfo(), conninfo => conninfo()},
        []
    ),

    %% Info: get/set
    NInfo = #{newinfo => true, node => node()},
    emqx_gateway_cm:set_chan_info(?GWNAME, ?CLIENTID, NInfo),
    ?assertEqual(
        NInfo,
        emqx_gateway_cm:get_chan_info(?GWNAME, ?CLIENTID)
    ),
    ?assertEqual(
        NInfo,
        emqx_gateway_cm:get_chan_info(?GWNAME, ?CLIENTID, self())
    ),
    %% Stats: get/set
    NStats = [{newstats, true}],
    emqx_gateway_cm:set_chan_stats(?GWNAME, ?CLIENTID, NStats),
    ?assertEqual(
        NStats,
        emqx_gateway_cm:get_chan_stats(?GWNAME, ?CLIENTID)
    ),
    ?assertEqual(
        NStats,
        emqx_gateway_cm:get_chan_stats(?GWNAME, ?CLIENTID, self())
    ),

    emqx_gateway_cm:connection_closed(?GWNAME, ?CLIENTID),
    emqx_gateway_cm:unregister_channel(?GWNAME, ?CLIENTID).

t_handle_process_down(Conf) ->
    Pid = proplists:get_value(cm, Conf),

    {ok, SessionRes} = emqx_gateway_cm:open_session(
        ?GWNAME,
        true,
        clientinfo(),
        conninfo(),
        fun(_, _) -> #{no => 1} end
    ),
    ?assertEqual(
        #{
            present => false,
            session => #{no => 1}
        },
        SessionRes
    ),
    emqx_gateway_cm:insert_channel_info(
        ?GWNAME,
        ?CLIENTID,
        #{clientinfo => clientinfo(), conninfo => conninfo()},
        []
    ),

    _ = Pid ! {'DOWN', mref, process, self(), normal},

    %% wait the async clear task
    timer:sleep(200),
    ?assertEqual(
        [],
        ets:tab2list(emqx_gateway_cm:tabname(chan, ?GWNAME))
    ),
    ?assertEqual(
        [],
        ets:tab2list(emqx_gateway_cm:tabname(conn, ?GWNAME))
    ),
    ?assertEqual(
        [],
        ets:tab2list(emqx_gateway_cm:tabname(info, ?GWNAME))
    ).

t_kick_session(_) ->
    %% session1
    {ok, _} = emqx_gateway_cm:open_session(
        ?GWNAME,
        true,
        clientinfo(),
        conninfo(),
        fun(_, _) -> #{no => 1} end
    ),
    emqx_gateway_cm:insert_channel_info(
        ?GWNAME,
        ?CLIENTID,
        #{clientinfo => clientinfo(), conninfo => conninfo()},
        []
    ),

    %% meck `lookup_channels`
    Self = self(),
    ok = meck:new(
        emqx_gateway_cm_registry,
        [passthrough, no_history, no_link]
    ),
    ok = meck:expect(
        emqx_gateway_cm_registry,
        lookup_channels,
        fun(_, ?CLIENTID) -> [Self, Self] end
    ),

    ok = emqx_gateway_cm:kick_session(?GWNAME, ?CLIENTID),

    receive
        kick -> ok
    after 100 -> ?assert(false, "waiting discard msg timeout")
    end,
    receive
        kick ->
            emqx_gateway_cm:connection_closed(?GWNAME, ?CLIENTID),
            emqx_gateway_cm:unregister_channel(?GWNAME, ?CLIENTID)
    after 100 ->
        ?assert(false, "waiting kick msg timeout")
    end,
    ?assertMatch({error, not_found}, emqx_gateway_http:kickout_client(?GWNAME, <<"i-dont-exist">>)),
    meck:unload(emqx_gateway_cm_registry).

t_unexpected_handle(Conf) ->
    Pid = proplists:get_value(cm, Conf),
    _ = Pid ! unexpected_info,
    ok = gen_server:call(Pid, unexpected_call),
    ok = gen_server:cast(Pid, unexpected_cast).

%%--------------------------------------------------------------------
%% helpers

clientinfo() ->
    #{
        clientid => ?CLIENTID,
        is_bridge => false,
        is_superuser => false,
        listener => 'mqttsn:udp:default',
        mountpoint => <<"mqttsn/">>,
        peerhost => {127, 0, 0, 1},
        protocol => 'mqtt-sn',
        sockport => 1884,
        username => undefined,
        zone => default
    }.

conninfo() ->
    #{
        clean_start => true,
        clientid => ?CLIENTID,
        conn_mod => ?MODULE,
        connected_at => 1641805544652,
        expiry_interval => 0,
        keepalive => 10,
        peercert => nossl,
        peername => {{127, 0, 0, 1}, 64810},
        proto_name => <<"MQTT-SN">>,
        proto_ver => <<"1.2">>,
        sockname => {{0, 0, 0, 0}, 1884},
        socktype => udp
    }.

%%--------------------------------------------------------------------
%% connection module mock

call(ConnPid, discard, _) ->
    ConnPid ! discard,
    ok;
call(ConnPid, kick, _) ->
    ConnPid ! kick,
    ok.
