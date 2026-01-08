%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_scram_mnesia_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-define(PATH, [authentication]).

-define(USER_MAP, #{
    user_id := _,
    is_superuser := _
}).

-define(NS, <<"some_ns">>).
-define(OTHER_NS, <<"some_other_ns">>).

-define(global, global).
-define(ns, ns).

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf, "mqtt.client_attrs_init = [{expression = username, set_as_attr = tns}]"},
            emqx_auth,
            emqx_auth_mnesia,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{
            work_dir => ?config(priv_dir, Config)
        }
    ),
    IdleTimeout = emqx_config:get([mqtt, idle_timeout]),
    [{apps, Apps}, {idle_timeout, IdleTimeout} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_group(?global, Config) ->
    [{ns, ?global_ns} | Config];
init_per_group(?ns, Config) ->
    [{ns, ?NS} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    mria:clear_table(emqx_authn_scram_mnesia),
    mria:clear_table(emqx_authn_scram_mnesia_ns),
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    Config.

end_per_testcase(_Case, Config) ->
    Config.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_create(_Config) ->
    ValidConfig = #{
        <<"mechanism">> => <<"scram">>,
        <<"backend">> => <<"built_in_database">>,
        <<"algorithm">> => <<"sha512">>,
        <<"iteration_count">> => <<"4096">>
    },

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, ValidConfig}
    ),

    {ok, [#{provider := emqx_authn_scram_mnesia}]} =
        emqx_authn_chains:list_authenticators(?GLOBAL).

t_create_invalid(_Config) ->
    InvalidConfig = #{
        <<"mechanism">> => <<"scram">>,
        <<"backend">> => <<"built_in_database">>,
        <<"algorithm">> => <<"sha271828">>,
        <<"iteration_count">> => <<"4096">>
    },

    {error, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, InvalidConfig}
    ),

    ?assertEqual(
        {error, {not_found, {chain, ?GLOBAL}}},
        emqx_authn_chains:list_authenticators(?GLOBAL)
    ).

t_authenticate() ->
    [{matrix, true}].
t_authenticate(matrix) ->
    [[?global], [?ns]];
t_authenticate(TCConfig) ->
    Algorithm = sha512,
    Username = <<"u">>,
    Password = <<"p">>,

    init_auth(Username, Password, Algorithm, TCConfig),

    ok = emqx_config:put([mqtt, idle_timeout], 500),

    {ok, Pid} = emqx_mqtt_test_client:start_link("127.0.0.1", 1883),

    ClientFirstMessage = sasl_auth_scram:client_first_message(Username),

    ConnectPacket = ?CONNECT_PACKET(
        maybe_add_ns_connect_packet(
            #mqtt_packet_connect{
                proto_ver = ?MQTT_PROTO_V5,
                properties = #{
                    'Authentication-Method' => <<"SCRAM-SHA-512">>,
                    'Authentication-Data' => ClientFirstMessage
                }
            },
            TCConfig
        )
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
                algorithm => Algorithm
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
        ServerFinalMessage, ClientCache#{algorithm => Algorithm}
    ),

    ?assertMatch(
        {200, #{
            <<"metrics">> := #{<<"total">> := 1, <<"success">> := 1}
        }},
        get_authenticator_status()
    ),

    %% Namespace mismatch
    {ok, Pid2} = emqx_mqtt_test_client:start_link("127.0.0.1", 1883),
    ConnectPacket2 = ?CONNECT_PACKET(
        add_ns_connect_packet(
            #mqtt_packet_connect{
                proto_ver = ?MQTT_PROTO_V5,
                properties = #{
                    'Authentication-Method' => <<"SCRAM-SHA-512">>,
                    'Authentication-Data' => ClientFirstMessage
                }
            },
            ?OTHER_NS
        )
    ),
    ok = emqx_mqtt_test_client:send(Pid2, ConnectPacket2),
    %% Intentional sleep to trigger idle timeout for the connection not yet authenticated
    ok = ct:sleep(1000),
    ?CONNACK_PACKET(?RC_NOT_AUTHORIZED, _) = receive_packet(),

    ?assertMatch(
        {200, #{
            <<"metrics">> := #{
                <<"total">> := 2,
                <<"success">> := 1,
                <<"failed">> := 0,
                <<"nomatch">> := 1
            }
        }},
        get_authenticator_status()
    ),

    ok.

t_authenticate_bad_props() ->
    [{matrix, true}].
t_authenticate_bad_props(matrix) ->
    [[?global], [?ns]];
t_authenticate_bad_props(TCConfig) ->
    Algorithm = sha512,
    Username = <<"u">>,
    Password = <<"p">>,

    init_auth(Username, Password, Algorithm, TCConfig),

    {ok, Pid} = emqx_mqtt_test_client:start_link("127.0.0.1", 1883),

    ConnectPacket = ?CONNECT_PACKET(
        maybe_add_ns_connect_packet(
            #mqtt_packet_connect{
                proto_ver = ?MQTT_PROTO_V5,
                properties = #{
                    'Authentication-Method' => <<"SCRAM-SHA-512">>
                }
            },
            TCConfig
        )
    ),

    ok = emqx_mqtt_test_client:send(Pid, ConnectPacket),

    ?CONNACK_PACKET(?RC_NOT_AUTHORIZED) = receive_packet().

t_authenticate_bad_username() ->
    [{matrix, true}].
t_authenticate_bad_username(matrix) ->
    [[?global], [?ns]];
t_authenticate_bad_username(TCConfig) ->
    Algorithm = sha512,
    Username = <<"u">>,
    Password = <<"p">>,

    init_auth(Username, Password, Algorithm, TCConfig),

    {ok, Pid} = emqx_mqtt_test_client:start_link("127.0.0.1", 1883),

    ClientFirstMessage = sasl_auth_scram:client_first_message(<<"badusername">>),

    ConnectPacket = ?CONNECT_PACKET(
        maybe_add_ns_connect_packet(
            #mqtt_packet_connect{
                proto_ver = ?MQTT_PROTO_V5,
                properties = #{
                    'Authentication-Method' => <<"SCRAM-SHA-512">>,
                    'Authentication-Data' => ClientFirstMessage
                }
            },
            TCConfig
        )
    ),

    ok = emqx_mqtt_test_client:send(Pid, ConnectPacket),

    ?CONNACK_PACKET(?RC_NOT_AUTHORIZED) = receive_packet().

t_authenticate_bad_password() ->
    [{matrix, true}].
t_authenticate_bad_password(matrix) ->
    [[?global], [?ns]];
t_authenticate_bad_password(TCConfig) ->
    Algorithm = sha512,
    Username = <<"u">>,
    Password = <<"p">>,

    init_auth(Username, Password, Algorithm, TCConfig),

    {ok, Pid} = emqx_mqtt_test_client:start_link("127.0.0.1", 1883),

    ClientFirstMessage = sasl_auth_scram:client_first_message(Username),

    ConnectPacket = ?CONNECT_PACKET(
        maybe_add_ns_connect_packet(
            #mqtt_packet_connect{
                proto_ver = ?MQTT_PROTO_V5,
                properties = #{
                    'Authentication-Method' => <<"SCRAM-SHA-512">>,
                    'Authentication-Data' => ClientFirstMessage
                }
            },
            TCConfig
        )
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
                algorithm => Algorithm
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

t_destroy() ->
    [{matrix, true}].
t_destroy(matrix) ->
    [[?global], [?ns]];
t_destroy(TCConfig) ->
    Namespace = ns(TCConfig),
    Config = config(),
    OtherId = list_to_binary([<<"id-other">>]),
    {ok, State0} = emqx_authn_scram_mnesia:create(<<"id">>, Config),
    {ok, StateOther} = emqx_authn_scram_mnesia:create(OtherId, Config),

    User = maybe_add_ns(#{user_id => <<"u">>, password => <<"p">>}, TCConfig),
    User2 = add_ns(#{user_id => <<"u">>, password => <<"p">>}, ?OTHER_NS),

    {ok, _} = emqx_authn_scram_mnesia:add_user(User, State0),
    {ok, _} = emqx_authn_scram_mnesia:add_user(User2, State0),
    {ok, _} = emqx_authn_scram_mnesia:add_user(User, StateOther),
    {ok, _} = emqx_authn_scram_mnesia:add_user(User2, StateOther),

    {ok, _} = lookup_user(Namespace, <<"u">>, State0),
    {ok, _} = lookup_user(?OTHER_NS, <<"u">>, State0),
    {ok, _} = lookup_user(Namespace, <<"u">>, StateOther),
    {ok, _} = lookup_user(?OTHER_NS, <<"u">>, StateOther),

    ok = emqx_authn_scram_mnesia:destroy(State0),

    {ok, State1} = emqx_authn_scram_mnesia:create(<<"id">>, Config),
    {error, not_found} = lookup_user(Namespace, <<"u">>, State1),
    {error, not_found} = lookup_user(?OTHER_NS, <<"u">>, State1),
    {ok, _} = lookup_user(Namespace, <<"u">>, StateOther),
    {ok, _} = lookup_user(?OTHER_NS, <<"u">>, StateOther),

    ok.

t_add_user() ->
    [{matrix, true}].
t_add_user(matrix) ->
    [[?global], [?ns]];
t_add_user(TCConfig) ->
    Config = config(),
    {ok, State} = emqx_authn_scram_mnesia:create(<<"authn-id">>, Config),

    User = maybe_add_ns(#{user_id => <<"u">>, password => <<"p">>}, TCConfig),
    {ok, _} = emqx_authn_scram_mnesia:add_user(User, State),
    {error, already_exist} = emqx_authn_scram_mnesia:add_user(User, State),

    OtherUser = add_ns(User, ?OTHER_NS),
    {ok, _} = emqx_authn_scram_mnesia:add_user(OtherUser, State),

    ok.

t_delete_user() ->
    [{matrix, true}].
t_delete_user(matrix) ->
    [[?global], [?ns]];
t_delete_user(TCConfig) ->
    Namespace = ns(TCConfig),
    Config = config(),
    {ok, State} = emqx_authn_scram_mnesia:create(<<"id">>, Config),

    {error, not_found} = delete_user(Namespace, <<"u">>, State),
    User = maybe_add_ns(#{user_id => <<"u">>, password => <<"p">>}, TCConfig),
    {ok, _} = emqx_authn_scram_mnesia:add_user(User, State),

    {error, not_found} = delete_user(?OTHER_NS, <<"u">>, State),
    ok = delete_user(Namespace, <<"u">>, State),
    {error, not_found} = delete_user(Namespace, <<"u">>, State).

t_update_user() ->
    [{matrix, true}].
t_update_user(matrix) ->
    [[?global], [?ns]];
t_update_user(TCConfig) ->
    Namespace = ns(TCConfig),
    Config = config(),
    {ok, State} = emqx_authn_scram_mnesia:create(<<"id">>, Config),

    User = maybe_add_ns(#{user_id => <<"u">>, password => <<"p">>}, TCConfig),
    {ok, _} = emqx_authn_scram_mnesia:add_user(User, State),
    {ok, #{is_superuser := false}} = lookup_user(Namespace, <<"u">>, State),

    {error, not_found} = update_user(?OTHER_NS, <<"u">>, #{password => <<"p1">>}, State),
    {error, not_found} = update_user(Namespace, <<"u1">>, #{password => <<"p1">>}, State),

    {ok, #{
        user_id := <<"u">>,
        is_superuser := true
    }} = update_user(
        Namespace,
        <<"u">>,
        #{password => <<"p1">>, is_superuser => true},
        State
    ),

    {ok, #{is_superuser := true}} = lookup_user(Namespace, <<"u">>, State).

t_update_user_keys() ->
    [{matrix, true}].
t_update_user_keys(matrix) ->
    [[?global], [?ns]];
t_update_user_keys(TCConfig) ->
    Namespace = ns(TCConfig),
    Algorithm = sha512,
    Username = <<"u">>,
    Password = <<"p">>,

    init_auth(Username, <<"badpass">>, Algorithm, TCConfig),

    {ok, [#{state := State}]} = emqx_authn_chains:list_authenticators(?GLOBAL),

    update_user(
        Namespace,
        Username,
        #{password => Password},
        State
    ),

    ok = emqx_config:put([mqtt, idle_timeout], 500),

    {ok, Pid} = emqx_mqtt_test_client:start_link("127.0.0.1", 1883),

    ClientFirstMessage = sasl_auth_scram:client_first_message(Username),

    ConnectPacket = ?CONNECT_PACKET(
        maybe_add_ns_connect_packet(
            #mqtt_packet_connect{
                proto_ver = ?MQTT_PROTO_V5,
                properties = #{
                    'Authentication-Method' => <<"SCRAM-SHA-512">>,
                    'Authentication-Data' => ClientFirstMessage
                }
            },
            TCConfig
        )
    ),

    ok = emqx_mqtt_test_client:send(Pid, ConnectPacket),

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
                algorithm => Algorithm
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
        ServerFinalMessage, ClientCache#{algorithm => Algorithm}
    ).

t_list_users() ->
    [{matrix, true}].
t_list_users(matrix) ->
    [[?global], [?ns]];
t_list_users(TCConfig) ->
    Config = config(),
    {ok, State} = emqx_authn_scram_mnesia:create(<<"id">>, Config),

    Users0 = [
        #{user_id => <<"u1">>, password => <<"p">>},
        #{user_id => <<"u2">>, password => <<"p">>},
        #{user_id => <<"u3">>, password => <<"p">>}
    ],
    Users = lists:map(fun(U) -> maybe_add_ns(U, TCConfig) end, Users0),

    lists:foreach(
        fun(U) -> {ok, _} = emqx_authn_scram_mnesia:add_user(U, State) end,
        Users
    ),
    OtherUser = #{user_id => <<"u4">>, password => <<"p">>},
    {ok, _} = emqx_authn_scram_mnesia:add_user(add_ns(OtherUser, ?OTHER_NS), State),

    Namespace = ns(TCConfig),
    NSQS = fun
        (QS) when Namespace == ?global_ns ->
            QS;
        (QS) ->
            QS#{<<"ns">> => Namespace}
    end,

    #{
        data := [?USER_MAP, ?USER_MAP],
        meta := #{page := 1, limit := 2, count := 3, hasnext := true}
    } = emqx_authn_scram_mnesia:list_users(
        NSQS(#{<<"page">> => 1, <<"limit">> => 2}),
        State
    ),
    #{
        data := [?USER_MAP],
        meta := #{page := 2, limit := 2, count := 3, hasnext := false}
    } = emqx_authn_scram_mnesia:list_users(
        NSQS(#{<<"page">> => 2, <<"limit">> => 2}),
        State
    ),
    #{
        data := [
            #{
                user_id := <<"u1">>,
                is_superuser := _
            }
        ],
        meta := #{page := 1, limit := 3, hasnext := false}
    } = emqx_authn_scram_mnesia:list_users(
        NSQS(#{
            <<"page">> => 1,
            <<"limit">> => 3,
            <<"like_user_id">> => <<"1">>
        }),
        State
    ),

    #{
        data := [#{is_superuser := false, user_id := <<"u4">>}],
        meta := #{page := 1, limit := 20, hasnext := false}
    } = emqx_authn_scram_mnesia:list_users(
        #{
            <<"page">> => 1,
            <<"limit">> => 20,
            <<"ns">> => ?OTHER_NS
        },
        State
    ),
    ok.

t_is_superuser(_Config) ->
    ok = test_is_superuser(#{is_superuser => false}, false),
    ok = test_is_superuser(#{is_superuser => true}, true),
    ok = test_is_superuser(#{}, false).

test_is_superuser(UserInfo, ExpectedIsSuperuser) ->
    Config = config(),
    {ok, State} = emqx_authn_scram_mnesia:create(<<"id">>, Config),

    Username = <<"u">>,
    Password = <<"p">>,

    UserInfo0 = UserInfo#{
        user_id => Username,
        password => Password
    },

    {ok, _} = emqx_authn_scram_mnesia:add_user(UserInfo0, State),

    ClientFirstMessage = sasl_auth_scram:client_first_message(Username),

    {continue, ServerFirstMessage, ServerCache} =
        emqx_authn_scram_mnesia:authenticate(
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
                algorithm => sha512
            }
        ),

    {ok, UserInfo1, ServerFinalMessage} =
        emqx_authn_scram_mnesia:authenticate(
            #{
                auth_method => <<"SCRAM-SHA-512">>,
                auth_data => ClientFinalMessage,
                auth_cache => ServerCache
            },
            State
        ),

    ok = sasl_auth_scram:check_server_final_message(
        ServerFinalMessage, ClientCache#{algorithm => sha512}
    ),

    ?assertMatch(#{is_superuser := ExpectedIsSuperuser}, UserInfo1),

    ok = emqx_authn_scram_mnesia:destroy(State).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

config() ->
    #{
        mechanism => <<"scram">>,
        backend => <<"built_in_database">>,
        algorithm => sha512,
        iteration_count => 4096
    }.

raw_config(Algorithm) ->
    #{
        <<"mechanism">> => <<"scram">>,
        <<"backend">> => <<"built_in_database">>,
        <<"algorithm">> => atom_to_binary(Algorithm),
        <<"iteration_count">> => <<"4096">>
    }.

init_auth(Username, Password, Algorithm, TCConfig) ->
    Config = raw_config(Algorithm),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, Config}
    ),

    {ok, [#{state := State}]} = emqx_authn_chains:list_authenticators(?GLOBAL),

    emqx_authn_scram_mnesia:add_user(
        maybe_add_ns(#{user_id => Username, password => Password}, TCConfig),
        State
    ).

receive_packet() ->
    receive
        {packet, Packet} ->
            ct:pal("Delivered packet: ~p", [Packet]),
            Packet
    after 1000 ->
        ct:fail("Deliver timeout")
    end.

lookup_user(Namespace, UserId, State) ->
    emqx_authn_scram_mnesia:lookup_user(Namespace, UserId, State).

update_user(Namespace, UserId, UserInfo, State) ->
    emqx_authn_scram_mnesia:update_user(Namespace, UserId, UserInfo, State).

delete_user(Namespace, UserId, State) ->
    emqx_authn_scram_mnesia:delete_user(Namespace, UserId, State).

maybe_add_ns(UserInfo, TCConfig) ->
    case ns(TCConfig) of
        ?global_ns ->
            UserInfo;
        Namespace when is_binary(Namespace) ->
            add_ns(UserInfo, Namespace)
    end.

add_ns(UserInfo, Namespace) when is_binary(Namespace) ->
    UserInfo#{namespace => Namespace}.

maybe_add_ns_clientinfo(ClientInfo, TCConfig) ->
    case ns(TCConfig) of
        ?global_ns ->
            ClientInfo;
        Namespace when is_binary(Namespace) ->
            add_ns_clientinfo(ClientInfo, Namespace)
    end.

maybe_add_ns_connect_packet(ConnectPacket, TCConfig) ->
    case ns(TCConfig) of
        ?global_ns ->
            ConnectPacket;
        Namespace when is_binary(Namespace) ->
            add_ns_connect_packet(ConnectPacket, Namespace)
    end.

add_ns_connect_packet(ConnectPacket, Namespace) ->
    ConnectPacket#mqtt_packet_connect{username = Namespace}.

add_ns_clientinfo(ClientInfo, Namespace) when is_binary(Namespace) ->
    ClientInfo#{client_attrs => #{?CLIENT_ATTR_NAME_TNS => Namespace}}.

ns(TCConfig) ->
    ?config(ns, TCConfig).

get_authenticator_status() ->
    AuthenticatorId = <<"scram:built_in_database">>,
    emqx_bridge_v2_testlib:simple_request(#{
        method => get,
        url => emqx_mgmt_api_test_util:api_path(["authentication", AuthenticatorId, "status"])
    }).
