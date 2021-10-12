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
    %% FIXME: Magic line. for saving gateway schema name for emqx_config
    emqx_config:init_load(emqx_gateway_schema, <<"gateway {}">>),
    emqx_ct_helpers:start_apps([emqx_gateway]),
    Conf.

end_per_suite(_Conf) ->
    emqx_ct_helpers:stop_apps([emqx_gateway]).

init_per_testcase(_CaseName, Conf) ->
    _ = emqx_gateway_conf:unload_gateway(stomp),
    Conf.

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

-define(CONF_STOMP_BAISC_1,
        #{ <<"idle_timeout">> => <<"10s">>,
           <<"mountpoint">> => <<"t/">>,
           <<"frame">> =>
           #{ <<"max_headers">> => 20,
              <<"max_headers_length">> => 2000,
              <<"max_body_length">> => 2000
            }
         }).
-define(CONF_STOMP_BAISC_2,
        #{ <<"idle_timeout">> => <<"20s">>,
           <<"mountpoint">> => <<"t2/">>,
           <<"frame">> =>
           #{ <<"max_headers">> => 30,
              <<"max_headers_length">> => 3000,
              <<"max_body_length">> => 3000
            }
         }).
-define(CONF_STOMP_LISTENER_1,
        #{ <<"bind">> => <<"61613">>
         }).
-define(CONF_STOMP_LISTENER_2,
        #{ <<"bind">> => <<"61614">>
         }).
-define(CONF_STOMP_AUTHN_1,
        #{ <<"mechanism">> => <<"password-based">>,
           <<"backend">> => <<"built-in-database">>,
           <<"user_id_type">> => <<"clientid">>
         }).
-define(CONF_STOMP_AUTHN_2,
        #{ <<"mechanism">> => <<"password-based">>,
           <<"backend">> => <<"built-in-database">>,
           <<"user_id_type">> => <<"username">>
         }).

t_load_unload_gateway(_) ->
    StompConf1 = compose(?CONF_STOMP_BAISC_1,
                         ?CONF_STOMP_AUTHN_1,
                         ?CONF_STOMP_LISTENER_1
                        ),
    StompConf2 = compose(?CONF_STOMP_BAISC_2,
                         ?CONF_STOMP_AUTHN_1,
                         ?CONF_STOMP_LISTENER_1),

    ok = emqx_gateway_conf:load_gateway(stomp, StompConf1),
    {error, already_exist} =
        emqx_gateway_conf:load_gateway(stomp, StompConf1),
    assert_confs(StompConf1, emqx:get_raw_config([gateway, stomp])),

    ok = emqx_gateway_conf:update_gateway(stomp, StompConf2),
    assert_confs(StompConf2, emqx:get_raw_config([gateway, stomp])),

    ok = emqx_gateway_conf:unload_gateway(stomp),
    ok = emqx_gateway_conf:unload_gateway(stomp),

    {error, not_found} =
        emqx_gateway_conf:update_gateway(stomp, StompConf2),

    ?assertException(error, {config_not_found, [gateway, stomp]},
                     emqx:get_raw_config([gateway, stomp])),
    ok.

t_load_remove_authn(_) ->
    StompConf = compose_listener(?CONF_STOMP_BAISC_1, ?CONF_STOMP_LISTENER_1),

    ok = emqx_gateway_conf:load_gateway(<<"stomp">>, StompConf),
    assert_confs(StompConf, emqx:get_raw_config([gateway, stomp])),

    ok = emqx_gateway_conf:add_authn(<<"stomp">>, ?CONF_STOMP_AUTHN_1),
    assert_confs(
      maps:put(<<"authentication">>, ?CONF_STOMP_AUTHN_1, StompConf),
      emqx:get_raw_config([gateway, stomp])),

    ok = emqx_gateway_conf:update_authn(<<"stomp">>, ?CONF_STOMP_AUTHN_2),
    assert_confs(
      maps:put(<<"authentication">>, ?CONF_STOMP_AUTHN_2, StompConf),
      emqx:get_raw_config([gateway, stomp])),

    ok = emqx_gateway_conf:remove_authn(<<"stomp">>),

    {error, not_found} =
        emqx_gateway_conf:update_authn(<<"stomp">>, ?CONF_STOMP_AUTHN_2),

    ?assertException(
       error, {config_not_found, [gateway, stomp, authentication]},
       emqx:get_raw_config([gateway, stomp, authentication])
      ),
    ok.

t_load_remove_listeners(_) ->
    StompConf = compose_authn(?CONF_STOMP_BAISC_1, ?CONF_STOMP_AUTHN_1),

    ok = emqx_gateway_conf:load_gateway(<<"stomp">>, StompConf),
    assert_confs(StompConf, emqx:get_raw_config([gateway, stomp])),

    ok = emqx_gateway_conf:add_listener(
           <<"stomp">>, {<<"tcp">>, <<"default">>}, ?CONF_STOMP_LISTENER_1),
    assert_confs(
      maps:merge(StompConf, listener(?CONF_STOMP_LISTENER_1)),
      emqx:get_raw_config([gateway, stomp])),

    ok = emqx_gateway_conf:update_listener(
           <<"stomp">>, {<<"tcp">>, <<"default">>}, ?CONF_STOMP_LISTENER_2),
    assert_confs(
      maps:merge(StompConf, listener(?CONF_STOMP_LISTENER_2)),
      emqx:get_raw_config([gateway, stomp])),

    ok = emqx_gateway_conf:remove_listener(
           <<"stomp">>, {<<"tcp">>, <<"default">>}),

    {error, not_found} =
        emqx_gateway_conf:update_listener(
          <<"stomp">>, {<<"tcp">>, <<"default">>}, ?CONF_STOMP_LISTENER_2),

    ?assertException(
       error, {config_not_found, [gateway, stomp, listeners, tcp, default]},
       emqx:get_raw_config([gateway, stomp, listeners, tcp, default])
      ),
    ok.

t_load_remove_listener_authn(_) ->
    StompConf  = compose_listener(
                   ?CONF_STOMP_BAISC_1,
                   ?CONF_STOMP_LISTENER_1
                  ),
    StompConf1 = compose_listener_authn(
                   ?CONF_STOMP_BAISC_1,
                   ?CONF_STOMP_LISTENER_1,
                   ?CONF_STOMP_AUTHN_1
                  ),
    StompConf2 = compose_listener_authn(
                   ?CONF_STOMP_BAISC_1,
                   ?CONF_STOMP_LISTENER_1,
                   ?CONF_STOMP_AUTHN_2
                 ),

    ok = emqx_gateway_conf:load_gateway(<<"stomp">>, StompConf),
    assert_confs(StompConf, emqx:get_raw_config([gateway, stomp])),

    ok = emqx_gateway_conf:add_authn(
           <<"stomp">>, {<<"tcp">>, <<"default">>}, ?CONF_STOMP_AUTHN_1),
    assert_confs(StompConf1, emqx:get_raw_config([gateway, stomp])),

    ok = emqx_gateway_conf:update_authn(
           <<"stomp">>, {<<"tcp">>, <<"default">>}, ?CONF_STOMP_AUTHN_2),
    assert_confs(StompConf2, emqx:get_raw_config([gateway, stomp])),

    ok = emqx_gateway_conf:remove_authn(
           <<"stomp">>, {<<"tcp">>, <<"default">>}),

    {error, not_found} =
        emqx_gateway_conf:update_authn(
          <<"stomp">>, {<<"tcp">>, <<"default">>}, ?CONF_STOMP_AUTHN_2),

    Path = [gateway, stomp, listeners, tcp, default, authentication],
    ?assertException(
       error, {config_not_found, Path},
       emqx:get_raw_config(Path)
      ),
    ok.

%%--------------------------------------------------------------------
%% Utils

compose(Basic, Authn, Listener) ->
    maps:merge(
      maps:merge(Basic, #{<<"authentication">> => Authn}),
      listener(Listener)).

compose_listener(Basic, Listener) ->
    maps:merge(Basic, listener(Listener)).

compose_authn(Basic, Authn) ->
    maps:merge(Basic, #{<<"authentication">> => Authn}).

compose_listener_authn(Basic, Listener, Authn) ->
    maps:merge(
      Basic,
      listener(maps:put(<<"authentication">>, Authn, Listener))).

listener(L) ->
    #{<<"listeners">> => [L#{<<"type">> => <<"tcp">>,
                             <<"name">> => <<"default">>}]}.

assert_confs(Expected0, Effected) ->
    Expected = maybe_unconvert_listeners(Expected0),
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

maybe_unconvert_listeners(Conf) ->
    case maps:take(<<"listeners">>, Conf) of
        error -> Conf;
        {Ls, Conf1} ->
            Conf1#{<<"listeners">> =>
                   emqx_gateway_conf:unconvert_listeners(Ls)}
    end.
