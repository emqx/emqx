%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

%%=================================================================================
%% CT boilerplate
%%=================================================================================

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_auth
        ],
        #{work_dir => emqx_cth_suite:work_dir(Case, Config)}
    ),
    ?MODULE:Case({init, [{apps, Apps} | Config]}).

end_per_testcase(Case, Config) ->
    ?MODULE:Case({'end', Config}),
    emqx_cth_suite:stop(?config(apps, Config)).

%%=================================================================================
%% Helpers fns
%%=================================================================================

%%=================================================================================
%% Testcases
%%=================================================================================

t_fill_defaults({init, Config}) ->
    Config;
t_fill_defaults({'end', _Config}) ->
    ok;
t_fill_defaults(Config) when is_list(Config) ->
    Conf0 = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"mysql">>,
        <<"query">> => <<"SELECT 1">>,
        <<"server">> => <<"mysql:3306">>,
        <<"database">> => <<"mqtt">>
    },

    %% Missing defaults are filled
    ?assertMatch(
        #{<<"query_timeout">> := _},
        emqx_authn:fill_defaults(Conf0)
    ),

    Conf1 = Conf0#{<<"mechanism">> => <<"unknown-xx">>},
    %% fill_defaults (check_config formerly) is actually never called on unvalidated config
    %% so it will not meet validation errors
    %% However, we still test it here
    ?assertThrow(
        #{reason := "unknown_mechanism"}, emqx_authn:fill_defaults(Conf1)
    ).

t_will_message_connection_denied({init, Config}) ->
    emqx_authn_test_lib:register_fake_providers([{password_based, built_in_database}]),
    AuthnConfig = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"built_in_database">>,
        <<"user_id_type">> => <<"clientid">>
    },
    Chain = 'mqtt:global',
    emqx:update_config(
        [authentication],
        {create_authenticator, Chain, AuthnConfig}
    ),
    User = #{user_id => <<"subscriber">>, password => <<"p">>},
    AuthenticatorID = <<"password_based:built_in_database">>,
    {ok, _} = emqx_authn_chains:add_user(
        Chain,
        AuthenticatorID,
        User
    ),
    Config;
t_will_message_connection_denied({'end', _Config}) ->
    emqx:update_config(
        [authentication],
        {delete_authenticator, 'mqtt:global', <<"password_based:built_in_database">>}
    ),
    ok;
t_will_message_connection_denied(Config) when is_list(Config) ->
    process_flag(trap_exit, true),

    {ok, Subscriber} = emqtt:start_link([
        {clientid, <<"subscriber">>},
        {password, <<"p">>}
    ]),
    {ok, _} = emqtt:connect(Subscriber),
    {ok, _, [?RC_SUCCESS]} = emqtt:subscribe(Subscriber, <<"lwt">>),

    {ok, Publisher} = emqtt:start_link([
        {clientid, <<"publisher">>},
        {will_topic, <<"lwt">>},
        {will_payload, <<"should not be published">>}
    ]),
    Ref = monitor(process, Publisher),
    _ = unlink(Publisher),
    {error, _} = emqtt:connect(Publisher),
    receive
        {'DOWN', Ref, process, Publisher, Reason} ->
            ?assertEqual({shutdown, malformed_username_or_password}, Reason)
    after 2000 ->
        error(timeout)
    end,
    receive
        {publish, #{
            topic := <<"lwt">>,
            payload := <<"should not be published">>
        }} ->
            ct:fail("should not publish will message")
    after 1000 ->
        ok
    end,
    ok.

%% With auth enabled, send CONNECT without password field,
%% expect CONNACK with reason_code=5 and socket close
t_password_undefined({init, Config}) ->
    emqx_authn_test_lib:register_fake_providers([
        {password_based, built_in_database}
    ]),
    AuthnConfig = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"built_in_database">>,
        <<"user_id_type">> => <<"clientid">>
    },
    Chain = 'mqtt:global',
    emqx:update_config(
        [authentication],
        {create_authenticator, Chain, AuthnConfig}
    ),
    Config;
t_password_undefined({'end', _Config}) ->
    emqx:update_config(
        [authentication],
        {delete_authenticator, 'mqtt:global', <<"password_based:built_in_database">>}
    ),
    ok;
t_password_undefined(Config) when is_list(Config) ->
    Payload = <<16, 19, 0, 4, 77, 81, 84, 84, 4, 130, 0, 60, 0, 2, 97, 49, 0, 3, 97, 97, 97>>,
    {ok, Sock} = gen_tcp:connect("localhost", 1883, [binary, {active, true}]),
    gen_tcp:send(Sock, Payload),
    receive
        {tcp, Sock, Bytes} ->
            Resp = parse(iolist_to_binary(Bytes)),
            ?assertMatch(
                #mqtt_packet{
                    header = #mqtt_packet_header{type = ?CONNACK},
                    variable = #mqtt_packet_connack{
                        ack_flags = 0,
                        reason_code = ?CONNACK_CREDENTIALS
                    },
                    payload = undefined
                },
                Resp
            )
    after 2000 ->
        error(timeout)
    end,
    receive
        {tcp_closed, Sock} ->
            ok
    after 2000 ->
        error(timeout)
    end,
    ok.

t_update_conf({init, Config}) ->
    emqx_authn_test_lib:register_fake_providers([
        {password_based, built_in_database},
        {password_based, http},
        jwt
    ]),
    {ok, _} = emqx:update_config([authentication], []),
    Config;
t_update_conf({'end', _Config}) ->
    {ok, _} = emqx:update_config([authentication], []),
    ok;
t_update_conf(Config) when is_list(Config) ->
    Authn1 = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"built_in_database">>,
        <<"user_id_type">> => <<"clientid">>,
        <<"enable">> => true
    },
    Authn2 = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"http">>,
        <<"method">> => <<"post">>,
        <<"url">> => <<"http://127.0.0.1:18083">>,
        <<"headers">> => #{
            <<"content-type">> => <<"application/json">>
        },
        <<"enable">> => true
    },
    Authn3 = #{
        <<"mechanism">> => <<"jwt">>,
        <<"use_jwks">> => false,
        <<"algorithm">> => <<"hmac-based">>,
        <<"secret">> => <<"mysecret">>,
        <<"secret_base64_encoded">> => false,
        <<"verify_claims">> => #{<<"username">> => <<"${username}">>},
        <<"enable">> => true
    },
    Chain = 'mqtt:global',
    {ok, _} = emqx:update_config([authentication], [Authn1]),
    ?assertMatch(
        {ok, #{
            authenticators := [
                #{
                    enable := true,
                    id := <<"password_based:built_in_database">>,
                    provider := emqx_authn_fake_provider
                }
            ]
        }},
        emqx_authn_chains:lookup_chain(Chain)
    ),

    {ok, _} = emqx:update_config([authentication], [Authn1, Authn2, Authn3]),
    ?assertMatch(
        {ok, #{
            authenticators := [
                #{
                    enable := true,
                    id := <<"password_based:built_in_database">>,
                    provider := emqx_authn_fake_provider
                },
                #{
                    enable := true,
                    id := <<"password_based:http">>,
                    provider := emqx_authn_fake_provider
                },
                #{
                    enable := true,
                    id := <<"jwt">>,
                    provider := emqx_authn_fake_provider
                }
            ]
        }},
        emqx_authn_chains:lookup_chain(Chain)
    ),
    {ok, _} = emqx:update_config([authentication], [Authn2, Authn1]),
    ?assertMatch(
        {ok, #{
            authenticators := [
                #{
                    enable := true,
                    id := <<"password_based:http">>,
                    provider := emqx_authn_fake_provider
                },
                #{
                    enable := true,
                    id := <<"password_based:built_in_database">>,
                    provider := emqx_authn_fake_provider
                }
            ]
        }},
        emqx_authn_chains:lookup_chain(Chain)
    ),

    {ok, _} = emqx:update_config([authentication], [Authn3, Authn2, Authn1]),
    ?assertMatch(
        {ok, #{
            authenticators := [
                #{
                    enable := true,
                    id := <<"jwt">>,
                    provider := emqx_authn_fake_provider
                },
                #{
                    enable := true,
                    id := <<"password_based:http">>,
                    provider := emqx_authn_fake_provider
                },
                #{
                    enable := true,
                    id := <<"password_based:built_in_database">>,
                    provider := emqx_authn_fake_provider
                }
            ]
        }},
        emqx_authn_chains:lookup_chain(Chain)
    ),
    {ok, _} = emqx:update_config([authentication], []),
    ?assertMatch(
        {error, {not_found, {chain, Chain}}},
        emqx_authn_chains:lookup_chain(Chain)
    ),
    ok.

parse(Bytes) ->
    {Frame, <<>>, _} = emqx_frame:parse(Bytes),
    Frame.

authenticate_return_quota_exceeded_hook(_, _) ->
    {stop, {error, quota_exceeded}}.

t_authenticate_return_quota_exceeded({init, Config}) ->
    Priority = 0,
    ok = emqx_hooks:put(
        'client.authenticate', {?MODULE, authenticate_return_quota_exceeded_hook, []}, Priority
    ),
    Config;
t_authenticate_return_quota_exceeded({'end', _Config}) ->
    ok = emqx_hooks:del('client.authenticate', {?MODULE, authenticate_return_quota_exceeded_hook});
t_authenticate_return_quota_exceeded(Config) when is_list(Config) ->
    {ok, Client} = emqtt:start_link([
        {clientid, <<"tests-cli1">>},
        {password, <<"pass">>},
        {proto_ver, v5}
    ]),
    _ = monitor(process, Client),
    _ = unlink(Client),
    ?assertMatch({error, {quota_exceeded, _}}, emqtt:connect(Client)),
    receive
        {'DOWN', _, process, Client, Reason} ->
            ?assertEqual({shutdown, quota_exceeded}, Reason)
    end.
