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

get_policy(Zone) ->
    emqx_config:get_zone_conf(Zone, [flapping_detect]).
