%%--------------------------------------------------------------------
%% Copyright (c) 2019-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_flapping_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            %% NOTE:
            %% Ban time should be > 1s as it's second-level precision. Otherwise
            %% test cases will be flaky.
            {emqx, """
                flapping_detect {
                    enable = true
                    max_count = 3
                    window_time = 100ms
                    ban_time = 2s
                }
            """}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

t_detect_check(_) ->
    ClientInfo = #{
        zone => default,
        listener => 'tcp:default',
        clientid => <<"client007">>,
        peerhost => {127, 0, 0, 1}
    },
    false = emqx_flapping:detect(ClientInfo),
    false = emqx_banned:check(ClientInfo),
    false = emqx_flapping:detect(ClientInfo),
    false = emqx_banned:check(ClientInfo),
    true = emqx_flapping:detect(ClientInfo),
    timer:sleep(50),
    ct:pal("the table emqx_banned: ~p, nowsec: ~p", [
        ets:tab2list(emqx_banned),
        erlang:system_time(second)
    ]),
    true = emqx_banned:check(ClientInfo),
    timer:sleep(2500),
    false = emqx_banned:check(ClientInfo).

t_detect_subsequent(_) ->
    ClientInfo = #{
        zone => default,
        listener => 'tcp:default',
        clientid => atom_to_binary(?FUNCTION_NAME),
        peerhost => {127, 0, 0, 1}
    },
    [Pid] = [P || {emqx_flapping, P, _, _} <- supervisor:which_children(emqx_cm_sup)],
    false = emqx_banned:check(ClientInfo),
    %% First time:
    false = emqx_flapping:detect(ClientInfo),
    false = emqx_flapping:detect(ClientInfo),
    true = emqx_flapping:detect(ClientInfo),
    %% Has been banned:
    timer:sleep(50),
    true = emqx_banned:check(ClientInfo),
    %% Second time:
    false = emqx_flapping:detect(ClientInfo),
    false = emqx_flapping:detect(ClientInfo),
    true = emqx_flapping:detect(ClientInfo),
    %% Still banned:
    timer:sleep(50),
    true = emqx_banned:check(ClientInfo),
    %% Process is fine:
    _ = sys:get_state(Pid).

t_no_detect_rare(_) ->
    ClientInfo = #{
        zone => default,
        listener => 'tcp:default',
        clientid => atom_to_binary(?FUNCTION_NAME),
        peerhost => {127, 0, 0, 1}
    },
    false = emqx_banned:check(ClientInfo),
    N = emqx_utils:foldl_while(
        fun(I, _) ->
            case emqx_flapping:detect(ClientInfo) of
                false ->
                    timer:sleep(60),
                    {cont, I};
                true ->
                    {halt, I}
            end
        end,
        0,
        lists:seq(1, 20)
    ),
    N < 20 orelse ct:comment("flapping was not observed"),
    %% Still not banned:
    timer:sleep(50),
    false = emqx_banned:check(ClientInfo).

t_rogue_messages(_) ->
    [Pid] = [P || {emqx_flapping, P, _, _} <- supervisor:which_children(emqx_cm_sup)],
    gen_server:call(Pid, unexpected_msg),
    gen_server:cast(Pid, unexpected_msg),
    Pid ! test,
    timer:sleep(50),
    ?assertEqual(
        [Pid],
        [P || {emqx_flapping, P, _, _} <- supervisor:which_children(emqx_cm_sup)]
    ).

t_expired_detecting(_) ->
    ClientInfo = #{
        zone => default,
        listener => 'tcp:default',
        clientid => <<"client008">>,
        peerhost => {127, 0, 0, 1}
    },
    false = emqx_flapping:detect(ClientInfo),
    ?assertMatch(
        [_],
        [X || X = {flapping, <<"client008">>, _, _, _} <- ets:tab2list(emqx_flapping)]
    ),
    timer:sleep(200),
    ?assertMatch(
        [],
        [X || X = {flapping, <<"client008">>, _, _, _} <- ets:tab2list(emqx_flapping)]
    ).

-doc """
Flapping detection keyed on username: clients with distinct client IDs and
source IPs sharing one username get the username banned once the threshold
is exceeded, without banning any of the client IDs; the ban expires after
the configured ban_time.
""".
t_detect_by_username(_) ->
    Zone = ?FUNCTION_NAME,
    Username = <<"flap_user">>,
    ok = emqx_config:put_zone_conf(Zone, [flapping_detect], #{
        enable => false,
        by_username => #{max_count => 3, window_time => 10000, ban_time => 2000},
        by_peerhost => none
    }),
    Detect = fun(N) ->
        emqx_flapping:detect(#{
            zone => Zone,
            listener => 'tcp:default',
            clientid => <<"username_dim_client_", (integer_to_binary(N))/binary>>,
            username => Username,
            peerhost => {10, 0, 0, N}
        })
    end,
    Detected0 = emqx_metrics:val_global('flapping.detected.username'),
    false = Detect(1),
    false = Detect(2),
    true = Detect(3),
    timer:sleep(50),
    %% The username is banned, regardless of client ID and source IP:
    true = emqx_banned:check(#{
        clientid => <<"a_fresh_clientid">>, username => Username, peerhost => {10, 0, 0, 99}
    }),
    %% Other usernames are not affected:
    false = emqx_banned:check(#{
        clientid => <<"a_fresh_clientid">>, username => <<"other_user">>, peerhost => {10, 0, 0, 99}
    }),
    %% None of the client IDs got banned:
    ?assertEqual([], emqx_banned:look_up({clientid, <<"username_dim_client_3">>})),
    ?assertEqual(Detected0 + 1, emqx_metrics:val_global('flapping.detected.username')),
    %% The ban expires:
    timer:sleep(2500),
    false = emqx_banned:check(#{
        clientid => <<"a_fresh_clientid">>, username => Username, peerhost => {10, 0, 0, 99}
    }).

-doc """
Flapping detection keyed on source IP address: clients with distinct client
IDs and usernames connecting from one IP address get the IP banned once the
threshold is exceeded; other IP addresses are not affected.
""".
t_detect_by_peerhost(_) ->
    Zone = ?FUNCTION_NAME,
    PeerHost = {10, 1, 2, 3},
    ok = emqx_config:put_zone_conf(Zone, [flapping_detect], #{
        enable => false,
        by_username => none,
        by_peerhost => #{max_count => 3, window_time => 10000, ban_time => 2000}
    }),
    Detect = fun(N) ->
        emqx_flapping:detect(#{
            zone => Zone,
            listener => 'tcp:default',
            clientid => <<"peerhost_dim_client_", (integer_to_binary(N))/binary>>,
            username => <<"peerhost_dim_user_", (integer_to_binary(N))/binary>>,
            peerhost => PeerHost
        })
    end,
    Detected0 = emqx_metrics:val_global('flapping.detected.peerhost'),
    false = Detect(1),
    false = Detect(2),
    true = Detect(3),
    timer:sleep(50),
    %% The source IP is banned, regardless of client ID and username:
    true = emqx_banned:check(#{
        clientid => <<"a_fresh_clientid">>, username => <<"a_fresh_user">>, peerhost => PeerHost
    }),
    %% Other source IPs are not affected:
    false = emqx_banned:check(#{
        clientid => <<"a_fresh_clientid">>,
        username => <<"a_fresh_user">>,
        peerhost => {10, 1, 2, 4}
    }),
    ?assertEqual([], emqx_banned:look_up({clientid, <<"peerhost_dim_client_3">>})),
    ?assertEqual(Detected0 + 1, emqx_metrics:val_global('flapping.detected.peerhost')),
    ok = emqx_banned:delete({peerhost, PeerHost}).

-doc """
Each dimension counts against its own threshold: with a lower client ID
threshold and a higher username threshold in the same zone, the client ID
gets banned first while the username is banned only after its own
threshold is exceeded.
""".
t_independent_dimension_policies(_) ->
    Zone = ?FUNCTION_NAME,
    ClientId = <<"indep_client">>,
    Username = <<"indep_user">>,
    ok = emqx_config:put_zone_conf(Zone, [flapping_detect], #{
        enable => true,
        max_count => 3,
        window_time => 10000,
        ban_time => 2000,
        by_username => #{max_count => 6, window_time => 10000, ban_time => 2000},
        by_peerhost => none
    }),
    ClientInfo = #{
        zone => Zone,
        listener => 'tcp:default',
        clientid => ClientId,
        username => Username,
        peerhost => {10, 2, 0, 1}
    },
    false = emqx_flapping:detect(ClientInfo),
    false = emqx_flapping:detect(ClientInfo),
    true = emqx_flapping:detect(ClientInfo),
    timer:sleep(50),
    %% Client ID hit its threshold (3), username (3 of 6) did not:
    ?assertMatch([_], emqx_banned:look_up({clientid, ClientId})),
    ?assertEqual([], emqx_banned:look_up({username, Username})),
    %% Three more connect events trip the username threshold too:
    false = emqx_flapping:detect(ClientInfo),
    false = emqx_flapping:detect(ClientInfo),
    true = emqx_flapping:detect(ClientInfo),
    timer:sleep(50),
    ?assertMatch([_], emqx_banned:look_up({username, Username})),
    ok = emqx_banned:delete({clientid, ClientId}),
    ok = emqx_banned:delete({username, Username}).

-doc """
Connections without a username are not counted towards the username
dimension, so they never produce a username ban entry.
""".
t_no_username_no_detect(_) ->
    Zone = ?FUNCTION_NAME,
    ok = emqx_config:put_zone_conf(Zone, [flapping_detect], #{
        enable => false,
        by_username => #{max_count => 2, window_time => 10000, ban_time => 2000},
        by_peerhost => none
    }),
    ClientInfo = #{
        zone => Zone,
        listener => 'tcp:default',
        clientid => <<"anon_client">>,
        username => undefined,
        peerhost => {10, 3, 0, 1}
    },
    false = emqx_flapping:detect(ClientInfo),
    false = emqx_flapping:detect(ClientInfo),
    false = emqx_flapping:detect(ClientInfo),
    ?assertEqual(
        [],
        [X || X = {flapping, {username, _}, _, _, _} <- ets:tab2list(emqx_flapping)]
    ).

-doc """
A username ban created by the flapping detector only rejects new connection
attempts (before authentication); clients already connected with that
username stay connected.
""".
t_existing_sessions_not_disconnected(_) ->
    erlang:process_flag(trap_exit, true),
    Zone = ?FUNCTION_NAME,
    Username = <<"storm_user">>,
    ok = emqx_config:put_zone_conf(Zone, [flapping_detect], #{
        enable => false,
        by_username => #{max_count => 3, window_time => 10000, ban_time => 10000},
        by_peerhost => none
    }),
    %% A client is already connected with the username:
    {ok, C1} = emqtt:start_link([
        {clientid, <<"storm_client_0">>}, {username, Username}, {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(C1),
    %% The username trips the flapping threshold:
    lists:foreach(
        fun(N) ->
            _ = emqx_flapping:detect(#{
                zone => Zone,
                listener => 'tcp:default',
                clientid => <<"storm_client_", (integer_to_binary(N))/binary>>,
                username => Username,
                peerhost => {10, 4, 0, N}
            })
        end,
        lists:seq(1, 3)
    ),
    timer:sleep(50),
    ?assertMatch([_], emqx_banned:look_up({username, Username})),
    %% The connected client is not disconnected:
    ?assertEqual(pong, emqtt:ping(C1)),
    %% New connection attempts with the banned username are rejected:
    Banned0 = emqx_metrics:val_global('client.banned'),
    {ok, C2} = emqtt:start_link([
        {clientid, <<"storm_client_new">>}, {username, Username}, {proto_ver, v5}
    ]),
    ?assertMatch({error, {banned, _}}, emqtt:connect(C2)),
    ?assertEqual(Banned0 + 1, emqx_metrics:val_global('client.banned')),
    %% Manual removal of the ban unblocks new connections:
    ok = emqx_banned:delete({username, Username}),
    {ok, C3} = emqtt:start_link([
        {clientid, <<"storm_client_new">>}, {username, Username}, {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(C3),
    ok = emqtt:disconnect(C3),
    ok = emqtt:disconnect(C1).

t_conf_update(_) ->
    Global = emqx_config:get([flapping_detect]),
    #{
        ban_time := _BanTime,
        enable := _Enable,
        max_count := _MaxCount,
        window_time := _WindowTime
    } = Global,

    emqx_config:put_zone_conf(new_zone, [flapping_detect], #{}),
    ?assertEqual(Global, get_policy(new_zone)),

    emqx_config:put_zone_conf(zone_1, [flapping_detect], #{window_time => 100}),
    ?assertEqual(Global#{window_time := 100}, emqx_flapping:get_policy(zone_1)),

    Zones = #{
        <<"zone_1">> => #{<<"flapping_detect">> => #{<<"window_time">> => <<"123s">>}},
        <<"zone_2">> => #{<<"flapping_detect">> => #{<<"window_time">> => <<"456s">>}}
    },
    ?assertMatch({ok, _}, emqx:update_config([zones], Zones)),
    %% new_zone is already deleted
    ?assertError({config_not_found, _}, get_policy(new_zone)),
    %% update zone(zone_1) has default.
    ?assertEqual(Global#{window_time := 123000}, emqx_flapping:get_policy(zone_1)),
    %% create zone(zone_2) has default
    ?assertEqual(Global#{window_time := 456000}, emqx_flapping:get_policy(zone_2)),
    %% reset to default(empty) andalso get default from global
    ?assertMatch({ok, _}, emqx:update_config([zones], #{})),
    ?assertEqual(Global, emqx:get_config([zones, default, flapping_detect])),
    ?assertError({config_not_found, _}, get_policy(zone_1)),
    ?assertError({config_not_found, _}, get_policy(zone_2)),
    ok.

t_conf_update_timer(_Config) ->
    %% delete all zones
    ?assertMatch({ok, _}, emqx:update_config([zones], #{})),
    emqx_cm_sup:restart_flapping(),
    validate_timer([{default, true}]),
    %% change zones
    {ok, _} =
        emqx:update_config([zones], #{
            <<"timer_1">> => #{<<"flapping_detect">> => #{<<"enable">> => true}},
            <<"timer_2">> => #{<<"flapping_detect">> => #{<<"enable">> => true}},
            <<"timer_3">> => #{<<"flapping_detect">> => #{<<"enable">> => false}}
        }),
    validate_timer([{timer_1, true}, {timer_2, true}, {timer_3, false}, {default, true}]),
    %% change global flapping_detect
    Global = emqx:get_raw_config([flapping_detect]),
    {ok, _} = emqx:update_config([flapping_detect], Global#{<<"enable">> => false}),
    validate_timer([{timer_1, true}, {timer_2, true}, {timer_3, false}, {default, false}]),
    %% reset
    {ok, _} = emqx:update_config([flapping_detect], Global#{<<"enable">> => true}),
    validate_timer([{timer_1, true}, {timer_2, true}, {timer_3, false}, {default, true}]),
    ok.

validate_timer(Lists) ->
    {Names, _} = lists:unzip(Lists),
    Zones = emqx:get_config([zones]),
    ?assertEqual(lists:sort(Names), lists:sort(maps:keys(Zones))),
    Timers = sys:get_state(emqx_flapping),
    maps:foreach(
        fun(Name, #{flapping_detect := #{enable := Enable}}) ->
            ?assertEqual(lists:keyfind(Name, 1, Lists), {Name, Enable}),
            ?assertEqual(Enable, is_reference(maps:get(Name, Timers)), Timers)
        end,
        Zones
    ),
    ?assertEqual(maps:keys(Zones), maps:keys(Timers)),
    ok.

t_window_compatibility_check(_Conf) ->
    Flapping = emqx:get_raw_config([flapping_detect]),
    ok = emqx_config:init_load(emqx_schema, <<"flapping_detect {window_time = disable}">>),
    ?assertMatch(#{window_time := 60000, enable := false}, emqx:get_config([flapping_detect])),
    %% reset
    FlappingBin = iolist_to_binary(["flapping_detect {", hocon_pp:do(Flapping, #{}), "}"]),
    ok = emqx_config:init_load(emqx_schema, FlappingBin),
    ?assertEqual(Flapping, emqx:get_raw_config([flapping_detect])),
    ok.

-doc """
The by_username and by_peerhost dimensions default to `none` (disabled)
and accept a policy object with defaults filled in for omitted fields.
""".
t_dimension_config_check(_Conf) ->
    Flapping = emqx:get_raw_config([flapping_detect]),
    ok = emqx_config:init_load(emqx_schema, <<"flapping_detect {by_username {max_count = 7}}">>),
    ?assertMatch(
        #{
            by_username := #{max_count := 7, window_time := 60000, ban_time := 300000},
            by_peerhost := none
        },
        emqx:get_config([flapping_detect])
    ),
    %% reset
    FlappingBin = iolist_to_binary(["flapping_detect {", hocon_pp:do(Flapping, #{}), "}"]),
    ok = emqx_config:init_load(emqx_schema, FlappingBin),
    ?assertEqual(Flapping, emqx:get_raw_config([flapping_detect])),
    ok.

get_policy(Zone) ->
    emqx_config:get_zone_conf(Zone, [flapping_detect]).
