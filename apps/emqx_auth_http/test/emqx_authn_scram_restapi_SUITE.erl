%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_scram_restapi_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").

-define(PATH, [authentication]).

%% Keep below the ephemeral port range (net.ipv4.ip_local_port_range, 32768-60999
%% on CI): a port inside that range can be picked as the source port of an
%% outbound connection, making this listener fail to bind with eaddrinuse.
-define(HTTP_PORT, 32336).
-define(HTTP_PATH, "/user/[...]").
-define(ALGORITHM, sha512).
-define(ALGORITHM_STR, <<"sha512">>).
-define(ITERATION_COUNT, 4096).

-define(T_ACL_USERNAME, <<"username">>).
-define(T_ACL_PASSWORD, <<"password">>).

-include_lib("emqx/include/emqx_placeholder.hrl").

all() ->
    case emqx_release:edition() of
        ce -> [];
        ee -> emqx_common_test_helpers:all(?MODULE)
    end.

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([cowboy, emqx, emqx_conf, emqx_auth, emqx_auth_http], #{
        work_dir => ?config(priv_dir, Config)
    }),

    IdleTimeout = emqx_config:get([mqtt, idle_timeout]),
    [{apps, Apps}, {idle_timeout, IdleTimeout} | Config].

end_per_suite(Config) ->
    ok = emqx_config:put([mqtt, idle_timeout], ?config(idle_timeout, Config)),
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(_Case, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    {ok, _} = emqx_authn_scram_restapi_test_server:start_link(?HTTP_PORT, ?HTTP_PATH),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = emqx_authn_scram_restapi_test_server:stop().

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_create(_Config) ->
    AuthConfig = raw_config(),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    {ok, [#{provider := emqx_authn_scram_restapi}]} = emqx_authn_chains:list_authenticators(
        ?GLOBAL
    ).

t_create_invalid(_Config) ->
    AuthConfig = raw_config(),

    InvalidConfigs =
        [
            AuthConfig#{<<"headers">> => []},
            AuthConfig#{<<"method">> => <<"delete">>},
            AuthConfig#{<<"url">> => <<"localhost">>},
            AuthConfig#{<<"url">> => <<"http://foo.com/xxx#fragment">>},
            AuthConfig#{<<"url">> => <<"http://${foo}.com/xxx">>},
            AuthConfig#{<<"url">> => <<"//foo.com/xxx">>},
            AuthConfig#{<<"algorithm">> => <<"sha128">>}
        ],

    lists:foreach(
        fun(Config) ->
            ct:pal("creating authenticator with invalid config: ~p", [Config]),
            {error, _} =
                try
                    emqx:update_config(
                        ?PATH,
                        {create_authenticator, ?GLOBAL, Config}
                    )
                catch
                    throw:Error ->
                        {error, Error}
                end,
            ?assertEqual(
                {error, {not_found, {chain, ?GLOBAL}}},
                emqx_authn_chains:list_authenticators(?GLOBAL)
            )
        end,
        InvalidConfigs
    ).

t_oauth2_client_credentials(_Config) ->
    lists:foreach(
        fun(Method) ->
            test_oauth2_client_credentials(Method)
        end,
        [get, post]
    ).

t_create_oauth2_authorization_header_conflict(_Config) ->
    AuthConfig = (raw_config())#{
        <<"headers">> => #{<<"authorization">> => <<"Basic dXNlcjpwYXNz">>},
        <<"oauth2">> => emqx_authn_http_SUITE:oauth2_config(token_endpoint_url())
    },
    Result =
        try
            emqx:update_config(?PATH, {create_authenticator, ?GLOBAL, AuthConfig})
        catch
            throw:Error -> {error, Error}
        end,
    %% OAuth2 injects the `authorization' header, so a manually configured one
    %% must be rejected by the runtime validation, as for authn_http. The
    %% reason is nested inside the config handler wrapper, so search for it
    %% instead of matching a brittle wrapper shape.
    ?assertMatch({error, {post_config_update, emqx_authn_config, _}}, Result),
    ?assert(contains_reason(invalid_headers, Result)),
    ?assertEqual(
        {error, {not_found, {chain, ?GLOBAL}}},
        emqx_authn_chains:list_authenticators(?GLOBAL)
    ).

test_oauth2_client_credentials(Method) ->
    Username = <<"oauth2-", (atom_to_binary(Method, utf8))/binary, "-user">>,
    Password = <<"p">>,
    Token = <<"scram-oauth2-token">>,
    TestPid = self(),
    ok = emqx_authn_scram_restapi_test_server:set_handler(
        oauth2_handler(Method, Username, Password, Token, TestPid)
    ),
    AuthConfig = (raw_config())#{
        <<"method">> => atom_to_binary(Method, utf8),
        <<"oauth2">> => emqx_authn_http_SUITE:oauth2_config(token_endpoint_url())
    },
    ok = emqx_auth_cache:reset(?AUTHN_CACHE),
    emqx_authn_test_lib:delete_authenticators([authentication], ?GLOBAL),
    _State = init_auth(AuthConfig),
    {ok, Pid} = create_connection(Username, Password),
    ok = emqx_mqtt_test_client:stop(Pid),
    receive
        {oauth2_authorization, Method, Authorization} ->
            ?assertEqual(<<"Bearer ", Token/binary>>, Authorization)
    after 1000 ->
        ct:fail("scram_restapi request did not carry the oauth2 token")
    end.

t_authenticate(_Config) ->
    Username = <<"u">>,
    Password = <<"p">>,

    set_user_handler(Username, Password),
    init_auth(),

    ok = emqx_config:put([mqtt, idle_timeout], 500),

    {ok, Pid} = create_connection(Username, Password),
    emqx_mqtt_test_client:stop(Pid).

t_authenticate_bad_props(_Config) ->
    Username = <<"u">>,
    Password = <<"p">>,

    set_user_handler(Username, Password),
    init_auth(),

    {ok, Pid} = emqx_mqtt_test_client:start_link("127.0.0.1", 1883),

    ConnectPacket = ?CONNECT_PACKET(
        #mqtt_packet_connect{
            proto_ver = ?MQTT_PROTO_V5,
            properties = #{
                'Authentication-Method' => <<"SCRAM-SHA-512">>
            }
        }
    ),

    ok = emqx_mqtt_test_client:send(Pid, ConnectPacket),

    ?CONNACK_PACKET(?RC_NOT_AUTHORIZED) = receive_packet().

t_authenticate_bad_username(_Config) ->
    Username = <<"u">>,
    Password = <<"p">>,

    set_user_handler(Username, Password),
    init_auth(),

    {ok, Pid} = emqx_mqtt_test_client:start_link("127.0.0.1", 1883),

    ClientFirstMessage = sasl_auth_scram:client_first_message(<<"badusername">>),

    ConnectPacket = ?CONNECT_PACKET(
        #mqtt_packet_connect{
            proto_ver = ?MQTT_PROTO_V5,
            properties = #{
                'Authentication-Method' => <<"SCRAM-SHA-512">>,
                'Authentication-Data' => ClientFirstMessage
            }
        }
    ),

    ok = emqx_mqtt_test_client:send(Pid, ConnectPacket),

    ?CONNACK_PACKET(?RC_NOT_AUTHORIZED) = receive_packet().

t_authenticate_bad_password(_Config) ->
    Username = <<"u">>,
    Password = <<"p">>,

    set_user_handler(Username, Password),
    init_auth(),

    {ok, Pid} = emqx_mqtt_test_client:start_link("127.0.0.1", 1883),

    ClientFirstMessage = sasl_auth_scram:client_first_message(Username),

    ConnectPacket = ?CONNECT_PACKET(
        #mqtt_packet_connect{
            proto_ver = ?MQTT_PROTO_V5,
            properties = #{
                'Authentication-Method' => <<"SCRAM-SHA-512">>,
                'Authentication-Data' => ClientFirstMessage
            }
        }
    ),

    ok = emqx_mqtt_test_client:send(Pid, ConnectPacket),

    ?AUTH_PACKET(
        ?RC_CONTINUE_AUTHENTICATION,
        #{'Authentication-Data' := ServerFirstMessage}
    ) = receive_packet(),

    {continue, ClientFinalMessage, _ClientCache} =
        sasl_auth_scram:check_server_first_message(
            ServerFirstMessage,
            #{
                client_first_message => ClientFirstMessage,
                password => <<"badpassword">>,
                algorithm => ?ALGORITHM
            }
        ),

    AuthContinuePacket = ?AUTH_PACKET(
        ?RC_CONTINUE_AUTHENTICATION,
        #{
            'Authentication-Method' => <<"SCRAM-SHA-512">>,
            'Authentication-Data' => ClientFinalMessage
        }
    ),

    ok = emqx_mqtt_test_client:send(Pid, AuthContinuePacket),

    ?CONNACK_PACKET(?RC_NOT_AUTHORIZED) = receive_packet().

t_destroy(_Config) ->
    Username = <<"u">>,
    Password = <<"p">>,

    set_user_handler(Username, Password),
    init_auth(),

    ok = emqx_config:put([mqtt, idle_timeout], 500),

    {ok, Pid} = emqx_mqtt_test_client:start_link("127.0.0.1", 1883),

    ConnectPacket = ?CONNECT_PACKET(
        #mqtt_packet_connect{
            proto_ver = ?MQTT_PROTO_V5,
            properties = #{
                'Authentication-Method' => <<"SCRAM-SHA-512">>
            }
        }
    ),

    ok = emqx_mqtt_test_client:send(Pid, ConnectPacket),

    ok = ct:sleep(1000),

    ?CONNACK_PACKET(?RC_NOT_AUTHORIZED) = receive_packet(),

    %% emqx_mqtt_test_client:stop(Pid),

    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),

    {ok, Pid2} = emqx_mqtt_test_client:start_link("127.0.0.1", 1883),

    ok = emqx_mqtt_test_client:send(Pid2, ConnectPacket),

    ok = ct:sleep(1000),

    ?CONNACK_PACKET(
        ?RC_SUCCESS,
        _,
        _
    ) = receive_packet().

t_acl(_Config) ->
    init_auth(),

    ACL = emqx_authn_http_SUITE:acl_rules(),
    set_user_handler(?T_ACL_USERNAME, ?T_ACL_PASSWORD, #{acl => ACL}),
    {ok, Pid} = create_connection(?T_ACL_USERNAME, ?T_ACL_PASSWORD),

    Cases = [
        {allow, <<"http-authn-acl/#">>},
        {deny, <<"http-authn-acl/1">>},
        {deny, <<"t/#">>}
    ],

    try
        lists:foreach(
            fun(Case) ->
                test_acl(Case, Pid)
            end,
            Cases
        )
    after
        ok = emqx_mqtt_test_client:stop(Pid)
    end.

t_auth_expire(_Config) ->
    init_auth(),

    ExpireSec = 3,
    WaitTime = timer:seconds(ExpireSec + 1),
    ACL = emqx_authn_http_SUITE:acl_rules(),

    set_user_handler(?T_ACL_USERNAME, ?T_ACL_PASSWORD, #{
        acl => ACL,
        expire_at =>
            erlang:system_time(second) + ExpireSec
    }),
    {ok, Pid} = create_connection(?T_ACL_USERNAME, ?T_ACL_PASSWORD),

    timer:sleep(WaitTime),
    ?assertEqual(false, erlang:is_process_alive(Pid)).

t_is_superuser() ->
    State = init_auth(),
    ok = test_is_superuser(State, false),
    ok = test_is_superuser(State, true),
    ok = test_is_superuser(State, false).

test_is_superuser(State, ExpectedIsSuperuser) ->
    Username = <<"u">>,
    Password = <<"p">>,

    set_user_handler(Username, Password, #{is_superuser => ExpectedIsSuperuser}),

    ClientFirstMessage = sasl_auth_scram:client_first_message(Username),

    {continue, ServerFirstMessage, ServerCache} =
        emqx_authn_scram_restapi:authenticate(
            #{
                auth_method => <<"SCRAM-SHA-512">>,
                auth_data => ClientFirstMessage,
                auth_cache => #{}
            },
            State
        ),

    {continue, ClientFinalMessage, ClientCache} =
        sasl_auth_scram:check_server_first_message(
            ServerFirstMessage,
            #{
                client_first_message => ClientFirstMessage,
                password => Password,
                algorithm => ?ALGORITHM
            }
        ),

    {ok, UserInfo1, ServerFinalMessage} =
        emqx_authn_scram_restapi:authenticate(
            #{
                auth_method => <<"SCRAM-SHA-512">>,
                auth_data => ClientFinalMessage,
                auth_cache => ServerCache
            },
            State
        ),

    ok = sasl_auth_scram:check_server_final_message(
        ServerFinalMessage, ClientCache#{algorithm => ?ALGORITHM}
    ),

    ?assertMatch(#{is_superuser := ExpectedIsSuperuser}, UserInfo1).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

raw_config() ->
    #{
        <<"mechanism">> => <<"scram">>,
        <<"backend">> => <<"http">>,
        <<"enable">> => <<"true">>,
        <<"max_inactive">> => <<"10s">>,
        <<"method">> => <<"get">>,
        <<"url">> => <<"http://127.0.0.1:32336/user">>,
        <<"body">> => #{<<"username">> => ?PH_USERNAME},
        <<"headers">> => #{<<"X-Test-Header">> => <<"Test Value">>},
        <<"algorithm">> => ?ALGORITHM_STR,
        <<"iteration_count">> => ?ITERATION_COUNT
    }.

set_user_handler(Username, Password) ->
    set_user_handler(Username, Password, #{is_superuser => false}).
set_user_handler(Username, Password, Extra0) ->
    %% HTTP Server
    Handler = fun(Req0, State) ->
        #{
            username := Username
        } = cowboy_req:match_qs([username], Req0),

        UserInfo = make_user_info(Password, ?ALGORITHM, ?ITERATION_COUNT),
        Extra = maps:merge(#{is_superuser => false}, Extra0),
        Req = cowboy_req:reply(
            200,
            #{<<"content-type">> => <<"application/json">>},
            emqx_utils_json:encode(maps:merge(Extra, UserInfo)),
            Req0
        ),
        {ok, Req, State}
    end,
    ok = emqx_authn_scram_restapi_test_server:set_handler(Handler).

%% Handler for the OAuth2 test cases: it serves both the token endpoint and
%% the user lookup endpoint on the same test server, and reports the
%% `authorization' header seen on the user lookup request back to the test.
oauth2_handler(Method, Username, Password, Token, TestPid) ->
    Oauth2Path = emqx_authn_scram_restapi_test_server:oauth2_token_path(),
    fun(Req0, State) ->
        case cowboy_req:path(Req0) of
            Oauth2Path ->
                {ok, Body, Req1} = cowboy_req:read_body(Req0),
                Form = uri_string:dissect_query(Body),
                ?assert(lists:member({<<"grant_type">>, <<"client_credentials">>}, Form)),
                Req = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    emqx_utils_json:encode(#{
                        access_token => Token,
                        expires_in => 3600,
                        token_type => <<"Bearer">>
                    }),
                    Req1
                ),
                {ok, Req, State};
            _ ->
                TestPid !
                    {oauth2_authorization, Method,
                        maps:get(<<"authorization">>, cowboy_req:headers(Req0), undefined)},
                {Username0, Req1} = request_username(Method, Req0),
                ?assertEqual(Username, Username0),
                UserInfo = make_user_info(Password, ?ALGORITHM, ?ITERATION_COUNT),
                Req = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    emqx_utils_json:encode(maps:merge(#{is_superuser => false}, UserInfo)),
                    Req1
                ),
                {ok, Req, State}
        end
    end.

request_username(get, Req) ->
    #{username := Username} = cowboy_req:match_qs([username], Req),
    {Username, Req};
request_username(post, Req) ->
    {ok, Body, Req1} = cowboy_req:read_body(Req),
    #{<<"username">> := Username} = emqx_utils_json:decode(Body),
    {Username, Req1}.

token_endpoint_url() ->
    TokenPath = emqx_authn_scram_restapi_test_server:oauth2_token_path(),
    <<"http://127.0.0.1:", (integer_to_binary(?HTTP_PORT))/binary, TokenPath/binary>>.

%% True if `Reason' appears as the first element of any tuple nested in `Term'.
contains_reason(Reason, Term) when is_tuple(Term) ->
    element(1, Term) =:= Reason orelse
        lists:any(fun(Sub) -> contains_reason(Reason, Sub) end, tuple_to_list(Term));
contains_reason(Reason, [Head | Tail]) ->
    contains_reason(Reason, Head) orelse contains_reason(Reason, Tail);
contains_reason(_Reason, _Term) ->
    false.

init_auth() ->
    init_auth(raw_config()).

init_auth(Config) ->
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, Config}
    ),

    {ok, [#{state := State}]} = emqx_authn_chains:list_authenticators(?GLOBAL),
    State.

make_user_info(Password, Algorithm, IterationCount) ->
    {StoredKey, ServerKey, Salt} = sasl_auth_scram:generate_authentication_info(
        Password,
        #{
            algorithm => Algorithm,
            iteration_count => IterationCount
        }
    ),
    #{
        stored_key => binary:encode_hex(StoredKey),
        server_key => binary:encode_hex(ServerKey),
        salt => binary:encode_hex(Salt)
    }.

receive_packet() ->
    receive
        {packet, Packet} ->
            ct:pal("Delivered packet: ~p", [Packet]),
            Packet
    after 1000 ->
        ct:fail("Deliver timeout")
    end.

create_connection(Username, Password) ->
    {ok, Pid} = emqx_mqtt_test_client:start_link("127.0.0.1", 1883),

    ClientFirstMessage = sasl_auth_scram:client_first_message(Username),

    ConnectPacket = ?CONNECT_PACKET(
        #mqtt_packet_connect{
            proto_ver = ?MQTT_PROTO_V5,
            properties = #{
                'Authentication-Method' => <<"SCRAM-SHA-512">>,
                'Authentication-Data' => ClientFirstMessage
            }
        }
    ),

    ok = emqx_mqtt_test_client:send(Pid, ConnectPacket),

    %% Intentional sleep to trigger idle timeout for the connection not yet authenticated
    ok = ct:sleep(1000),

    ?AUTH_PACKET(
        ?RC_CONTINUE_AUTHENTICATION,
        #{'Authentication-Data' := ServerFirstMessage}
    ) = receive_packet(),

    {continue, ClientFinalMessage, ClientCache} =
        sasl_auth_scram:check_server_first_message(
            ServerFirstMessage,
            #{
                client_first_message => ClientFirstMessage,
                password => Password,
                algorithm => ?ALGORITHM
            }
        ),

    AuthContinuePacket = ?AUTH_PACKET(
        ?RC_CONTINUE_AUTHENTICATION,
        #{
            'Authentication-Method' => <<"SCRAM-SHA-512">>,
            'Authentication-Data' => ClientFinalMessage
        }
    ),

    ok = emqx_mqtt_test_client:send(Pid, AuthContinuePacket),

    ?CONNACK_PACKET(
        ?RC_SUCCESS,
        _,
        #{'Authentication-Data' := ServerFinalMessage}
    ) = receive_packet(),

    ok = sasl_auth_scram:check_server_final_message(
        ServerFinalMessage, ClientCache#{algorithm => ?ALGORITHM}
    ),
    {ok, Pid}.

test_acl({allow, Topic}, C) ->
    ?assertMatch(
        [0],
        send_subscribe(C, Topic)
    );
test_acl({deny, Topic}, C) ->
    ?assertMatch(
        [?RC_NOT_AUTHORIZED],
        send_subscribe(C, Topic)
    ).

send_subscribe(Client, Topic) ->
    TopicOpts = #{nl => 0, rap => 0, rh => 0, qos => 0},
    Packet = ?SUBSCRIBE_PACKET(1, [{Topic, TopicOpts}]),
    emqx_mqtt_test_client:send(Client, Packet),
    timer:sleep(200),

    ?SUBACK_PACKET(1, ReasonCode) = receive_packet(),
    ReasonCode.
