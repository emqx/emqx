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
    emqx_config:put_zone_conf(
        default,
        [flapping_detect],
        #{
            max_count => 3,
            % 0.1s
            window_time => 100,
            %% 2s
            ban_time => 2000
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

t_conf_without_window_time(_) ->
    %% enable is deprecated, so we need to make sure it won't be used.
    Global = emqx_config:get([flapping_detect]),
    ?assertNot(maps:is_key(enable, Global)),
    %% zones don't have default value, so we need to make sure fallback to global conf.
    %% this new_zone will fallback to global conf.
    emqx_config:put_zone_conf(new_zone, [flapping_detect], #{}),
    ?assertEqual(Global, get_policy(new_zone)),

    emqx_config:put_zone_conf(new_zone_1, [flapping_detect], #{window_time => 100}),
    ?assertEqual(100, emqx_flapping:get_policy(window_time, new_zone_1)),
    ?assertEqual(maps:get(ban_time, Global), emqx_flapping:get_policy(ban_time, new_zone_1)),
    ?assertEqual(maps:get(max_count, Global), emqx_flapping:get_policy(max_count, new_zone_1)),
    ok.

get_policy(Zone) ->
    emqx_flapping:get_policy([window_time, ban_time, max_count], Zone).
