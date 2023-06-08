%%--------------------------------------------------------------------
%% Copyright (c) 2019-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_flapping_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:boot_modules(all),
    emqx_common_test_helpers:start_apps([]),
    %% update global default config
    {ok, _} = emqx:update_config(
        [flapping_detect],
        #{
            <<"enable">> => true,
            <<"max_count">> => 3,
            % 0.1s
            <<"window_time">> => 100,
            %% 2s
            <<"ban_time">> => "2s"
        }
    ),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([]),
    %% Clean emqx_banned table
    mria_mnesia:delete_schema(),
    ok.

t_detect_check(_) ->
    ClientInfo = #{
        zone => default,
        listener => {tcp, default},
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
    timer:sleep(3000),
    false = emqx_banned:check(ClientInfo),
    Children = supervisor:which_children(emqx_cm_sup),
    {emqx_flapping, Pid, _, _} = lists:keyfind(emqx_flapping, 1, Children),
    gen_server:call(Pid, unexpected_msg),
    gen_server:cast(Pid, unexpected_msg),
    Pid ! test,
    ok = emqx_flapping:stop().

t_expired_detecting(_) ->
    ClientInfo = #{
        zone => default,
        listener => {tcp, default},
        clientid => <<"client008">>,
        peerhost => {127, 0, 0, 1}
    },
    false = emqx_flapping:detect(ClientInfo),
    ?assertEqual(
        true,
        lists:any(
            fun
                ({flapping, <<"client008">>, _, _, _}) -> true;
                (_) -> false
            end,
            ets:tab2list(emqx_flapping)
        )
    ),
    timer:sleep(200),
    ?assertEqual(
        true,
        lists:all(
            fun
                ({flapping, <<"client008">>, _, _, _}) -> false;
                (_) -> true
            end,
            ets:tab2list(emqx_flapping)
        )
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
        <<"zone_1">> => #{<<"flapping_detect">> => #{<<"window_time">> => 123}},
        <<"zone_2">> => #{<<"flapping_detect">> => #{<<"window_time">> => 456}}
    },
    ?assertMatch({ok, _}, emqx:update_config([zones], Zones)),
    %% new_zone is already deleted
    ?assertError({config_not_found, _}, get_policy(new_zone)),
    %% update zone(zone_1) has default.
    ?assertEqual(Global#{window_time := 123}, emqx_flapping:get_policy(zone_1)),
    %% create zone(zone_2) has default
    ?assertEqual(Global#{window_time := 456}, emqx_flapping:get_policy(zone_2)),
    %% reset to default(empty) andalso get default from global
    ?assertMatch({ok, _}, emqx:update_config([zones], #{})),
    ?assertEqual(Global, emqx:get_config([zones, default, flapping_detect])),
    ?assertError({config_not_found, _}, get_policy(zone_1)),
    ?assertError({config_not_found, _}, get_policy(zone_2)),
    ok.

t_conf_update_timer(_Config) ->
    _ = emqx_flapping:start_link(),
    validate_timer([default]),
    {ok, _} =
        emqx:update_config([zones], #{
            <<"timer_1">> => #{<<"flapping_detect">> => #{<<"enable">> => true}},
            <<"timer_2">> => #{<<"flapping_detect">> => #{<<"enable">> => true}},
            <<"timer_3">> => #{<<"flapping_detect">> => #{<<"enable">> => false}}
        }),
    validate_timer([timer_1, timer_2, timer_3, default]),
    ok.

validate_timer(Names) ->
    Zones = emqx:get_config([zones]),
    ?assertEqual(lists:sort(Names), lists:sort(maps:keys(Zones))),
    Timers = sys:get_state(emqx_flapping),
    maps:foreach(
        fun(Name, #{flapping_detect := #{enable := Enable}}) ->
            ?assertEqual(Enable, is_reference(maps:get(Name, Timers)), Timers)
        end,
        Zones
    ),
    ?assertEqual(maps:keys(Zones), maps:keys(Timers)),
    ok.

t_window_compatibility_check(_Conf) ->
    Flapping = emqx:get_config([flapping_detect]),
    ok = emqx_config:init_load(emqx_schema, <<"flapping_detect {window_time = disable}">>),
    ?assertMatch(#{window_time := 60000, enable := false}, emqx:get_config([flapping_detect])),
    %% reset
    FlappingBin = iolist_to_binary(["flapping_detect {", hocon_pp:do(Flapping, #{}), "}"]),
    ok = emqx_config:init_load(emqx_schema, FlappingBin),
    ?assertEqual(Flapping, emqx:get_config([flapping_detect])),
    ok.

get_policy(Zone) ->
    emqx_config:get_zone_conf(Zone, [flapping_detect]).
