%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_jwt_enhanced_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(PATH, [authentication]).
-define(METHOD, <<"JWT">>).
-define(SECRET, <<"secret">>).
-define(USERNAME, <<"myuser">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    %% Both the missing-token and the backend-failure outcomes are chosen by the
    %% security profile, so pin it to keep the reason codes deterministic.
    emqx_common_test_helpers:set_security_profile(hardened),
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf,
                emqx_authn_test_lib:emqx_appspec(#{
                    config =>
                        "authorization.no_match = deny, authorization.cache.enable = false"
                })},
            emqx_auth,
            emqx_auth_jwt
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)),
    emqx_common_test_helpers:clear_security_profile().

init_per_testcase(_, Config) ->
    _ = emqx_authn_test_lib:delete_authenticators(?PATH, ?GLOBAL),
    {ok, _} = emqx:update_config(?PATH, {create_authenticator, ?GLOBAL, auth_config()}),
    {ok, [#{provider := emqx_authn_jwt}]} = emqx_authn_chains:list_authenticators(?GLOBAL),
    ok = emqx_config:put([mqtt, idle_timeout], 5000),
    Config.

end_per_testcase(_, _Config) ->
    _ = emqx_authn_test_lib:delete_authenticators(?PATH, ?GLOBAL),
    emqx_common_test_helpers:call_janitor().

%%--------------------------------------------------------------------
%% CT cases
%%--------------------------------------------------------------------

%% The token is validated in one round: the CONNECT carrying it is answered
%% with a CONNACK, never with an AUTH continue.
t_connect_is_single_round(_Config) ->
    _ = connect(<<"c_single">>, jws(#{<<"exp">> => exp(60)})),
    ?assertMatch(?CONNACK_PACKET(?RC_SUCCESS, _, _), receive_packet()).

t_connect_rejects_bad_token(_Config) ->
    _ = connect(<<"c_bad">>, <<"not.a.jwt">>),
    ?assertMatch(?CONNACK_PACKET(?RC_NOT_AUTHORIZED, _), receive_packet()).

%% A missing Authentication Data under the JWT method follows the configured
%% `on_missing_jwt' policy, pinned to `deny' by this suite.
t_connect_without_token(_Config) ->
    {ok, Pid} = emqx_mqtt_test_client:start_link("127.0.0.1", 1883),
    ok = emqx_mqtt_test_client:send(
        Pid,
        ?CONNECT_PACKET(#mqtt_packet_connect{
            proto_ver = ?MQTT_PROTO_V5,
            clientid = <<"c_no_token">>,
            username = ?USERNAME,
            properties = #{'Authentication-Method' => ?METHOD}
        })
    ),
    ?assertMatch(?CONNACK_PACKET(?RC_BAD_USER_NAME_OR_PASSWORD, _), receive_packet()).

t_reauthenticate_preserves_session(_Config) ->
    ClientId = <<"c_reauth">>,
    _ = connect(ClientId, jws(#{<<"exp">> => exp(60)})),
    ?assertMatch(?CONNACK_PACKET(?RC_SUCCESS, _, _), receive_packet()),
    [ChanPid] = emqx_cm:lookup_channels(ClientId),

    ok = reauthenticate(jws(#{<<"exp">> => exp(120)})),
    ?assertMatch(?AUTH_PACKET(?RC_SUCCESS, _), receive_packet()),

    %% Same channel, same identity: the session was not taken over or replaced.
    ?assertEqual([ChanPid], emqx_cm:lookup_channels(ClientId)),
    ?assert(is_process_alive(ChanPid)),
    ?assertMatch(
        #{clientinfo := #{clientid := ClientId, username := ?USERNAME}},
        emqx_cm:get_chan_info(ClientId)
    ).

%% A fresh token must push the disconnect deadline out. Without re-arming the
%% timer the client is dropped at the original `exp'.
t_reauthenticate_extends_deadline(_Config) ->
    _ = connect(<<"c_extend">>, jws(#{<<"exp">> => exp(3)})),
    ?assertMatch(?CONNACK_PACKET(?RC_SUCCESS, _, _), receive_packet()),

    ok = reauthenticate(jws(#{<<"exp">> => exp(120)})),
    ?assertMatch(?AUTH_PACKET(?RC_SUCCESS, _), receive_packet()),

    %% Past the first deadline, still connected.
    ?assertEqual(timeout, receive_nothing(6000)).

t_reauthenticate_rejects_expired_token(_Config) ->
    _ = connect(<<"c_expired">>, jws(#{<<"exp">> => exp(60)})),
    ?assertMatch(?CONNACK_PACKET(?RC_SUCCESS, _, _), receive_packet()),

    ok = reauthenticate(jws(#{<<"exp">> => exp(-60)})),
    ?assertMatch(?DISCONNECT_PACKET(?RC_BAD_USER_NAME_OR_PASSWORD), receive_packet()).

t_reauthenticate_rejects_bad_method(_Config) ->
    _ = connect(<<"c_method">>, jws(#{<<"exp">> => exp(60)})),
    ?assertMatch(?CONNACK_PACKET(?RC_SUCCESS, _, _), receive_packet()),

    %% [MQTT-4.12.0-1]: the method is fixed at CONNECT and cannot be switched.
    ok = emqx_mqtt_test_client:send(
        get_client(),
        ?AUTH_PACKET(?RC_RE_AUTHENTICATE, #{
            'Authentication-Method' => <<"SCRAM-SHA-512">>,
            'Authentication-Data' => jws(#{<<"exp">> => exp(60)})
        })
    ),
    ?assertMatch(?DISCONNECT_PACKET(?RC_BAD_AUTHENTICATION_METHOD), receive_packet()).

%% Client attributes are derived by the CONNECT enrichment pipeline and stay
%% frozen for the session; a re-authentication must not redefine them.
t_reauthenticate_freezes_client_attrs(_Config) ->
    ClientId = <<"c_attrs">>,
    _ = connect(
        ClientId,
        jws(#{<<"exp">> => exp(60), <<"client_attrs">> => #{<<"tier">> => <<"gold">>}})
    ),
    ?assertMatch(?CONNACK_PACKET(?RC_SUCCESS, _, _), receive_packet()),
    ?assertMatch(#{<<"tier">> := <<"gold">>}, client_attrs(ClientId)),

    ok = reauthenticate(
        jws(#{<<"exp">> => exp(120), <<"client_attrs">> => #{<<"tier">> => <<"bronze">>}})
    ),
    ?assertMatch(?AUTH_PACKET(?RC_SUCCESS, _), receive_packet()),
    ?assertMatch(#{<<"tier">> := <<"gold">>}, client_attrs(ClientId)).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

connect(ClientId, JWT) ->
    {ok, Pid} = emqx_mqtt_test_client:start_link("127.0.0.1", 1883),
    put(client, Pid),
    ok = emqx_mqtt_test_client:send(
        Pid,
        ?CONNECT_PACKET(#mqtt_packet_connect{
            proto_ver = ?MQTT_PROTO_V5,
            clientid = ClientId,
            username = ?USERNAME,
            properties = #{
                'Authentication-Method' => ?METHOD,
                'Authentication-Data' => JWT
            }
        })
    ),
    Pid.

reauthenticate(JWT) ->
    emqx_mqtt_test_client:send(
        get_client(),
        ?AUTH_PACKET(?RC_RE_AUTHENTICATE, #{
            'Authentication-Method' => ?METHOD,
            'Authentication-Data' => JWT
        })
    ).

get_client() ->
    get(client).

client_attrs(ClientId) ->
    #{clientinfo := #{client_attrs := Attrs}} = emqx_cm:get_chan_info(ClientId),
    Attrs.

receive_packet() ->
    receive
        {packet, Packet} ->
            ct:pal("Delivered packet: ~p", [Packet]),
            Packet
    after 5000 ->
        ct:fail("Deliver timeout")
    end.

receive_nothing(Timeout) ->
    receive
        {packet, Packet} -> ct:fail("Unexpected packet: ~p", [Packet])
    after Timeout ->
        timeout
    end.

exp(SecondsFromNow) ->
    erlang:system_time(second) + SecondsFromNow.

jws(Claims) ->
    emqx_authn_jwt_SUITE:generate_jws(
        'hmac-based', Claims#{<<"username">> => ?USERNAME}, ?SECRET
    ).

auth_config() ->
    #{
        <<"use_jwks">> => false,
        <<"algorithm">> => <<"hmac-based">>,
        <<"acl_claim_name">> => <<"acl">>,
        <<"secret">> => ?SECRET,
        <<"mechanism">> => <<"jwt">>,
        <<"on_missing_jwt">> => <<"deny">>,
        <<"verify_claims">> => #{<<"username">> => <<"${username}">>}
    }.
