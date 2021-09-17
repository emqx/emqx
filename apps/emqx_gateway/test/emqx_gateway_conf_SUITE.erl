%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway_conf_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Conf) ->
    emqx_ct_helpers:start_apps([emqx_gateway]),
    Conf.

end_per_suite(_Conf) ->
    emqx_ct_helpers:stop_apps([emqx_gateway]).

init_per_testcase(_CaseName, Conf) ->
    emqx_gateway_conf:unload(),
    emqx_config:put([gateway], #{}),
    emqx_config:put_raw([gateway], #{}),
    emqx_config:init_load(emqx_gateway_schema, <<"gateway {}">>),
    emqx_gateway_conf:load(),
    Conf.

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

-define(CONF_STOMP1, #{listeners => #{tcp => #{default => #{bind => 61613}}}}).
-define(CONF_STOMP2, #{listeners => #{tcp => #{default => #{bind => 61614}}}}).

t_load_remove_gateway(_) ->
    ok = emqx_gateway_conf:load_gateway(stomp, ?CONF_STOMP1),
    {error, {pre_config_update, emqx_gateway_conf, already_exist}} =
        emqx_gateway_conf:load_gateway(stomp, ?CONF_STOMP1),
    assert_confs(?CONF_STOMP1, emqx:get_config([gateway, stomp])),

    ok = emqx_gateway_conf:update_gateway(stomp, ?CONF_STOMP2),
    assert_confs(?CONF_STOMP2, emqx:get_config([gateway, stomp])),

    ok = emqx_gateway_conf:remove_gateway(stomp),
    ok = emqx_gateway_conf:remove_gateway(stomp),

    {error, {pre_config_update, emqx_gateway_conf, not_found}} =
        emqx_gateway_conf:update_gateway(stomp, ?CONF_STOMP2),

    ?assertException(error, {config_not_found, [gateway,stomp]},
                     emqx:get_config([gateway, stomp])),
    ok.

%%--------------------------------------------------------------------
%% Utils

assert_confs(Expected, Effected) ->
    case do_assert_confs(Expected, Effected) of
        false ->
            io:format(standard_error, "Expected config: ~p,\n"
                                      "Effected config: ~p",
                                      [Expected, Effected]),
            exit(conf_not_match);
        true ->
            ok
    end.

do_assert_confs(Expected, Effected) when is_map(Expected),
                                      is_map(Effected) ->
    Ks1 = maps:keys(Expected),
    lists:all(fun(K) ->
        do_assert_confs(maps:get(K, Expected),
                        maps:get(K, Effected, undefined))
    end, Ks1);
do_assert_confs(Expected, Effected) ->
    Expected =:= Effected.
