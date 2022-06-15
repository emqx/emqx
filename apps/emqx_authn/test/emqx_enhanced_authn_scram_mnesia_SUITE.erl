%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_enhanced_authn_scram_mnesia_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-include_lib("emqx/include/emqx_mqtt.hrl").
-include("emqx_authn.hrl").

-define(PATH, [authentication]).

-define(USER_MAP, #{
    user_id := _,
    is_superuser := _
}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    ok = emqx_common_test_helpers:start_apps([emqx_authn]),
    Config.

end_per_suite(_Config) ->
    ok = emqx_common_test_helpers:stop_apps([emqx_authn]).

init_per_testcase(_Case, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    mria:clear_table(emqx_enhanced_authn_scram_mnesia),
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

    {ok, [#{provider := emqx_enhanced_authn_scram_mnesia}]} =
        emqx_authentication:list_authenticators(?GLOBAL).

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
        emqx_authentication:list_authenticators(?GLOBAL)
    ).

t_authenticate(_Config) ->
    Algorithm = sha512,
    Username = <<"u">>,
    Password = <<"p">>,

    init_auth(Username, Password, Algorithm),

    {ok, Pid} = emqx_authn_mqtt_test_client:start_link("127.0.0.1", 1883),

    ClientFirstMessage = esasl_scram:client_first_message(Username),

    ConnectPacket = ?CONNECT_PACKET(
        #mqtt_packet_connect{
            proto_ver = ?MQTT_PROTO_V5,
            properties = #{
                'Authentication-Method' => <<"SCRAM-SHA-512">>,
                'Authentication-Data' => ClientFirstMessage
            }
        }
    ),

    ok = emqx_authn_mqtt_test_client:send(Pid, ConnectPacket),

    ?AUTH_PACKET(
        ?RC_CONTINUE_AUTHENTICATION,
        #{'Authentication-Data' := ServerFirstMessage}
    ) = receive_packet(),

    {continue, ClientFinalMessage, ClientCache} =
        esasl_scram:check_server_first_message(
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

    ok = emqx_authn_mqtt_test_client:send(Pid, AuthContinuePacket),

    ?CONNACK_PACKET(
        ?RC_SUCCESS,
        _,
        #{'Authentication-Data' := ServerFinalMessage}
    ) = receive_packet(),

    ok = esasl_scram:check_server_final_message(
        ServerFinalMessage, ClientCache#{algorithm => Algorithm}
    ).

t_authenticate_bad_username(_Config) ->
    Algorithm = sha512,
    Username = <<"u">>,
    Password = <<"p">>,

    init_auth(Username, Password, Algorithm),

    {ok, Pid} = emqx_authn_mqtt_test_client:start_link("127.0.0.1", 1883),

    ClientFirstMessage = esasl_scram:client_first_message(<<"badusername">>),

    ConnectPacket = ?CONNECT_PACKET(
        #mqtt_packet_connect{
            proto_ver = ?MQTT_PROTO_V5,
            properties = #{
                'Authentication-Method' => <<"SCRAM-SHA-512">>,
                'Authentication-Data' => ClientFirstMessage
            }
        }
    ),

    ok = emqx_authn_mqtt_test_client:send(Pid, ConnectPacket),

    ?CONNACK_PACKET(?RC_NOT_AUTHORIZED) = receive_packet().

t_authenticate_bad_password(_Config) ->
    Algorithm = sha512,
    Username = <<"u">>,
    Password = <<"p">>,

    init_auth(Username, Password, Algorithm),

    {ok, Pid} = emqx_authn_mqtt_test_client:start_link("127.0.0.1", 1883),

    ClientFirstMessage = esasl_scram:client_first_message(Username),

    ConnectPacket = ?CONNECT_PACKET(
        #mqtt_packet_connect{
            proto_ver = ?MQTT_PROTO_V5,
            properties = #{
                'Authentication-Method' => <<"SCRAM-SHA-512">>,
                'Authentication-Data' => ClientFirstMessage
            }
        }
    ),

    ok = emqx_authn_mqtt_test_client:send(Pid, ConnectPacket),

    ?AUTH_PACKET(
        ?RC_CONTINUE_AUTHENTICATION,
        #{'Authentication-Data' := ServerFirstMessage}
    ) = receive_packet(),

    {continue, ClientFinalMessage, _ClientCache} =
        esasl_scram:check_server_first_message(
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

    ok = emqx_authn_mqtt_test_client:send(Pid, AuthContinuePacket),

    ?CONNACK_PACKET(?RC_NOT_AUTHORIZED) = receive_packet().

t_destroy(_) ->
    Config = config(),
    OtherId = list_to_binary([<<"id-other">>]),
    {ok, State0} = emqx_enhanced_authn_scram_mnesia:create(<<"id">>, Config),
    {ok, StateOther} = emqx_enhanced_authn_scram_mnesia:create(OtherId, Config),

    User = #{user_id => <<"u">>, password => <<"p">>},

    {ok, _} = emqx_enhanced_authn_scram_mnesia:add_user(User, State0),
    {ok, _} = emqx_enhanced_authn_scram_mnesia:add_user(User, StateOther),

    {ok, _} = emqx_enhanced_authn_scram_mnesia:lookup_user(<<"u">>, State0),
    {ok, _} = emqx_enhanced_authn_scram_mnesia:lookup_user(<<"u">>, StateOther),

    ok = emqx_enhanced_authn_scram_mnesia:destroy(State0),

    {ok, State1} = emqx_enhanced_authn_scram_mnesia:create(<<"id">>, Config),
    {error, not_found} = emqx_enhanced_authn_scram_mnesia:lookup_user(<<"u">>, State1),
    {ok, _} = emqx_enhanced_authn_scram_mnesia:lookup_user(<<"u">>, StateOther).

t_add_user(_) ->
    Config = config(),
    {ok, State} = emqx_enhanced_authn_scram_mnesia:create(<<"id">>, Config),

    User = #{user_id => <<"u">>, password => <<"p">>},
    {ok, _} = emqx_enhanced_authn_scram_mnesia:add_user(User, State),
    {error, already_exist} = emqx_enhanced_authn_scram_mnesia:add_user(User, State).

t_delete_user(_) ->
    Config = config(),
    {ok, State} = emqx_enhanced_authn_scram_mnesia:create(<<"id">>, Config),

    {error, not_found} = emqx_enhanced_authn_scram_mnesia:delete_user(<<"u">>, State),
    User = #{user_id => <<"u">>, password => <<"p">>},
    {ok, _} = emqx_enhanced_authn_scram_mnesia:add_user(User, State),

    ok = emqx_enhanced_authn_scram_mnesia:delete_user(<<"u">>, State),
    {error, not_found} = emqx_enhanced_authn_scram_mnesia:delete_user(<<"u">>, State).

t_update_user(_) ->
    Config = config(),
    {ok, State} = emqx_enhanced_authn_scram_mnesia:create(<<"id">>, Config),

    User = #{user_id => <<"u">>, password => <<"p">>},
    {ok, _} = emqx_enhanced_authn_scram_mnesia:add_user(User, State),
    {ok, #{is_superuser := false}} = emqx_enhanced_authn_scram_mnesia:lookup_user(<<"u">>, State),

    {ok, #{
        user_id := <<"u">>,
        is_superuser := true
    }} = emqx_enhanced_authn_scram_mnesia:update_user(
        <<"u">>,
        #{password => <<"p1">>, is_superuser => true},
        State
    ),

    {ok, #{is_superuser := true}} = emqx_enhanced_authn_scram_mnesia:lookup_user(<<"u">>, State).

t_list_users(_) ->
    Config = config(),
    {ok, State} = emqx_enhanced_authn_scram_mnesia:create(<<"id">>, Config),

    Users = [
        #{user_id => <<"u1">>, password => <<"p">>},
        #{user_id => <<"u2">>, password => <<"p">>},
        #{user_id => <<"u3">>, password => <<"p">>}
    ],

    lists:foreach(
        fun(U) -> {ok, _} = emqx_enhanced_authn_scram_mnesia:add_user(U, State) end,
        Users
    ),

    #{
        data := [?USER_MAP, ?USER_MAP],
        meta := #{page := 1, limit := 2, count := 3}
    } = emqx_enhanced_authn_scram_mnesia:list_users(
        #{<<"page">> => 1, <<"limit">> => 2},
        State
    ),
    #{
        data := [?USER_MAP],
        meta := #{page := 2, limit := 2, count := 3}
    } = emqx_enhanced_authn_scram_mnesia:list_users(
        #{<<"page">> => 2, <<"limit">> => 2},
        State
    ),
    #{
        data := [
            #{
                user_id := <<"u1">>,
                is_superuser := _
            }
        ],
        meta := #{page := 1, limit := 3, count := 1}
    } = emqx_enhanced_authn_scram_mnesia:list_users(
        #{
            <<"page">> => 1,
            <<"limit">> => 3,
            <<"like_user_id">> => <<"1">>
        },
        State
    ).

t_is_superuser(_Config) ->
    ok = test_is_superuser(#{is_superuser => false}, false),
    ok = test_is_superuser(#{is_superuser => true}, true),
    ok = test_is_superuser(#{}, false).

test_is_superuser(UserInfo, ExpectedIsSuperuser) ->
    Config = config(),
    {ok, State} = emqx_enhanced_authn_scram_mnesia:create(<<"id">>, Config),

    Username = <<"u">>,
    Password = <<"p">>,

    UserInfo0 = UserInfo#{
        user_id => Username,
        password => Password
    },

    {ok, _} = emqx_enhanced_authn_scram_mnesia:add_user(UserInfo0, State),

    ClientFirstMessage = esasl_scram:client_first_message(Username),

    {continue, ServerFirstMessage, ServerCache} =
        emqx_enhanced_authn_scram_mnesia:authenticate(
            #{
                auth_method => <<"SCRAM-SHA-512">>,
                auth_data => ClientFirstMessage,
                auth_cache => #{}
            },
            State
        ),

    {continue, ClientFinalMessage, ClientCache} =
        esasl_scram:check_server_first_message(
            ServerFirstMessage,
            #{
                client_first_message => ClientFirstMessage,
                password => Password,
                algorithm => sha512
            }
        ),

    {ok, UserInfo1, ServerFinalMessage} =
        emqx_enhanced_authn_scram_mnesia:authenticate(
            #{
                auth_method => <<"SCRAM-SHA-512">>,
                auth_data => ClientFinalMessage,
                auth_cache => ServerCache
            },
            State
        ),

    ok = esasl_scram:check_server_final_message(
        ServerFinalMessage, ClientCache#{algorithm => sha512}
    ),

    ?assertMatch(#{is_superuser := ExpectedIsSuperuser}, UserInfo1),

    ok = emqx_enhanced_authn_scram_mnesia:destroy(State).

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

init_auth(Username, Password, Algorithm) ->
    Config = raw_config(Algorithm),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, Config}
    ),

    {ok, [#{state := State}]} = emqx_authentication:list_authenticators(?GLOBAL),

    emqx_enhanced_authn_scram_mnesia:add_user(
        #{user_id => Username, password => Password},
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
