%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_jwt_enhanced_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_access_control.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
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

-doc """
Tests that the `JWT` method without Authentication Data is denied, also when
`on_missing_jwt = ignore` is set and a later authentication hook would accept the client.
""".
t_connect_without_token(_Config) ->
    _ = emqx_authn_test_lib:delete_authenticators(?PATH, ?GLOBAL),
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, (auth_config())#{<<"on_missing_jwt">> => <<"ignore">>}}
    ),
    ok = emqx_hooks:add('client.authenticate', {?MODULE, allow_all, []}, ?HP_LOWEST),
    emqx_common_test_helpers:on_exit(fun() ->
        emqx_hooks:del('client.authenticate', {?MODULE, allow_all})
    end),
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
        emqx_connection:info(ChanPid)
    ).

%% A fresh token pushes the disconnect deadline out: the connection outlives the
%% `exp' of the token it connected with.
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

-doc """
Tests that a re-authentication token whose `clientid_override` differs from the client ID
fails the re-authentication, and that no channel is registered under the other client ID.
""".
t_reauthenticate_rejects_clientid_override(_Config) ->
    ClientId = <<"c_keep_id">>,
    _ = connect(ClientId, jws(#{<<"exp">> => exp(60)})),
    ?assertMatch(?CONNACK_PACKET(?RC_SUCCESS, _, _), receive_packet()),
    [_ChanPid] = emqx_cm:lookup_channels(ClientId),

    ok = reauthenticate(
        jws(#{<<"exp">> => exp(120), <<"clientid_override">> => <<"c_keep_id_other">>})
    ),
    ?assertMatch(?DISCONNECT_PACKET(?RC_NOT_AUTHORIZED), receive_packet()),
    ?assertEqual([], emqx_cm:lookup_channels(<<"c_keep_id_other">>)).

-doc """
Tests that an override in the CONNECT token applies for the session, and that a
re-authentication token with the same override succeeds and keeps the registration.
""".
t_reauthenticate_accepts_same_clientid_override(_Config) ->
    ClientId = <<"c_override_connect">>,
    _ = connect(
        <<"c_override_orig">>,
        jws(#{<<"exp">> => exp(60), <<"clientid_override">> => ClientId})
    ),
    ?assertMatch(?CONNACK_PACKET(?RC_SUCCESS, _, _), receive_packet()),
    [ChanPid] = emqx_cm:lookup_channels(ClientId),
    ?assertEqual(ClientId, clientid(ChanPid)),

    ok = reauthenticate(
        jws(#{<<"exp">> => exp(120), <<"clientid_override">> => ClientId})
    ),
    ?assertMatch(?AUTH_PACKET(?RC_SUCCESS, _), receive_packet()),
    ?assertEqual(ClientId, clientid(ChanPid)),
    ?assertEqual([ChanPid], emqx_cm:lookup_channels(ClientId)),
    ?assertEqual([], emqx_cm:lookup_channels(<<"c_override_orig">>)).

-doc """
Tests that a re-authentication token without an `acl` claim removes the ACL of the previous
token: the client-info authorizer no longer matches, so the publish falls to `no_match = deny`.
""".
t_reauthenticate_without_acl_revokes_acl(_Config) ->
    ClientId = <<"c_acl_revoke">>,
    _ = connect(
        ClientId,
        jws(#{<<"exp">> => exp(60), <<"acl">> => #{<<"pub">> => [<<"t/1">>]}})
    ),
    ?assertMatch(?CONNACK_PACKET(?RC_SUCCESS, _, _), receive_packet()),
    ?assertEqual(?RC_NO_MATCHING_SUBSCRIBERS, publish(<<"t/1">>, 1)),

    ok = reauthenticate(jws(#{<<"exp">> => exp(3600)})),
    ?assertMatch(?AUTH_PACKET(?RC_SUCCESS, _), receive_packet()),
    ClientInfo = clientinfo(ClientId),
    ?assertNot(maps:is_key(acl, ClientInfo)),
    ?assertEqual(
        ignore, emqx_authz_client_info:authorize(ClientInfo, ?AUTHZ_PUBLISH(1), <<"t/1">>, #{})
    ),
    ?assertEqual(?RC_NOT_AUTHORIZED, publish(<<"t/1">>, 2)).

%% With the authorization cache enabled, a re-authentication that narrows the
%% `acl' claim applies to the next publish.
t_reauthenticate_empties_authz_cache(_Config) ->
    ok = emqx_config:put([authorization, cache, enable], true),
    emqx_common_test_helpers:on_exit(fun() ->
        emqx_config:put([authorization, cache, enable], false)
    end),
    _ = connect(
        <<"c_authz_cache">>,
        jws(#{<<"exp">> => exp(60), <<"acl">> => #{<<"pub">> => [<<"t/1">>]}})
    ),
    ?assertMatch(?CONNACK_PACKET(?RC_SUCCESS, _, _), receive_packet()),
    ?assertEqual(?RC_NO_MATCHING_SUBSCRIBERS, publish(<<"t/1">>, 1)),

    ok = reauthenticate(
        jws(#{<<"exp">> => exp(120), <<"acl">> => #{<<"pub">> => [<<"t/2">>]}})
    ),
    ?assertMatch(?AUTH_PACKET(?RC_SUCCESS, _), receive_packet()),
    ?assertEqual(?RC_NOT_AUTHORIZED, publish(<<"t/1">>, 2)),
    ?assertEqual(?RC_NO_MATCHING_SUBSCRIBERS, publish(<<"t/2">>, 3)).

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

%% Re-authentication does not rewrite the `emqx_cm' channel info row, so ask the
%% channel process.
client_attrs(ClientId) ->
    #{client_attrs := Attrs} = clientinfo(ClientId),
    Attrs.

clientinfo(ClientId) ->
    [ChanPid] = emqx_cm:lookup_channels(ClientId),
    #{clientinfo := ClientInfo} = emqx_connection:info(ChanPid),
    ClientInfo.

allow_all(_Credential, _Acc) ->
    {stop, {ok, #{is_superuser => false}}}.

clientid(ChanPid) ->
    #{clientinfo := #{clientid := ClientId}} = emqx_connection:info(ChanPid),
    ClientId.

publish(Topic, PacketId) ->
    ok = emqx_mqtt_test_client:send(
        get_client(), ?PUBLISH_PACKET(?QOS_1, Topic, PacketId, <<"payload">>)
    ),
    ?PUBACK_PACKET(PacketId, ReasonCode) = receive_packet(),
    ReasonCode.

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
