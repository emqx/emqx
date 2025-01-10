%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_listeners_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_authn.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_auth], #{
        work_dir => ?config(priv_dir, Config)
    }),
    ok = emqx_authn_test_lib:register_fake_providers([
        {password_based, built_in_database},
        {password_based, redis}
    ]),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(_Case, Config) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    [{port, Port} | Config].

end_per_testcase(_Case, _Config) ->
    ok.

t_create_update_delete(Config) ->
    ListenerConf = listener_mqtt_tcp_conf(Config),
    AuthnConfig0 = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"built_in_database">>,
        <<"user_id_type">> => <<"clientid">>
    },
    %% Create
    {ok, _} = emqx_conf:update(
        [listeners],
        #{
            <<"tcp">> => #{
                <<"listener0">> => ListenerConf#{
                    ?CONF_NS_BINARY => AuthnConfig0
                }
            }
        },
        #{}
    ),
    ?assertMatch(
        {ok, [
            #{
                authenticators := [
                    #{
                        id := <<"password_based:built_in_database">>,
                        state := #{
                            config := #{user_id_type := clientid}
                        }
                    }
                ],
                name := 'tcp:listener0'
            }
        ]},
        emqx_authn_chains:list_chains()
    ),

    %% Drop old, create new
    {ok, _} = emqx_conf:update(
        [listeners],
        #{
            <<"tcp">> => #{
                <<"listener1">> => ListenerConf#{
                    ?CONF_NS_BINARY => AuthnConfig0
                }
            }
        },
        #{}
    ),
    ?assertMatch(
        {ok, [
            #{
                authenticators := [
                    #{
                        id := <<"password_based:built_in_database">>,
                        state := #{
                            config := #{user_id_type := clientid}
                        }
                    }
                ],
                name := 'tcp:listener1'
            }
        ]},
        emqx_authn_chains:list_chains()
    ),

    %% Update
    {ok, _} = emqx_conf:update(
        [listeners],
        #{
            <<"tcp">> => #{
                <<"listener1">> => ListenerConf#{
                    ?CONF_NS_BINARY => AuthnConfig0#{<<"user_id_type">> => <<"username">>}
                }
            }
        },
        #{}
    ),
    ?assertMatch(
        {ok, [
            #{
                authenticators := [
                    #{
                        id := <<"password_based:built_in_database">>,
                        state := #{
                            config := #{user_id_type := username}
                        }
                    }
                ],
                name := 'tcp:listener1'
            }
        ]},
        emqx_authn_chains:list_chains()
    ),

    %% Update by listener path
    {ok, _} = emqx_conf:update(
        [listeners, tcp, listener1],
        {update, ListenerConf#{
            ?CONF_NS_BINARY => AuthnConfig0#{<<"user_id_type">> => <<"clientid">>}
        }},
        #{}
    ),
    ?assertMatch(
        {ok, [
            #{
                authenticators := [
                    #{
                        id := <<"password_based:built_in_database">>,
                        state := #{
                            config := #{user_id_type := clientid}
                        }
                    }
                ],
                name := 'tcp:listener1'
            }
        ]},
        emqx_authn_chains:list_chains()
    ),

    %% Delete
    {ok, _} = emqx_conf:tombstone(
        [listeners, tcp, listener1],
        #{}
    ),
    ?assertMatch(
        {ok, []},
        emqx_authn_chains:list_chains()
    ).

t_convert_certs(Config) ->
    ListenerConf = listener_mqtt_tcp_conf(Config),
    AuthnConfig0 = #{
        <<"mechanism">> => <<"password_based">>,
        <<"password_hash_algorithm">> => #{
            <<"name">> => <<"plain">>,
            <<"salt_position">> => <<"suffix">>
        },
        <<"enable">> => <<"true">>,

        <<"backend">> => <<"redis">>,
        <<"cmd">> => <<"HMGET mqtt_user:${username} password_hash salt is_superuser">>,
        <<"database">> => <<"1">>,
        <<"password">> => <<"public">>,
        <<"server">> => <<"127.0.0.1:55555">>,
        <<"redis_type">> => <<"single">>,
        <<"ssl">> => #{
            <<"enable">> => true,
            <<"cacertfile">> => cacert_pem(),
            <<"certfile">> => cert_pem(),
            <<"keyfile">> => private_key_pem()
        }
    },
    {ok, _} = emqx_conf:update(
        [listeners],
        #{
            <<"tcp">> => #{
                <<"listener0">> => ListenerConf#{
                    ?CONF_NS_BINARY => AuthnConfig0
                }
            }
        },
        #{}
    ),
    lists:foreach(
        fun(Key) ->
            [#{ssl := #{Key := FilePath}}] = emqx_config:get([
                listeners, tcp, listener0, authentication
            ]),
            ?assert(filelib:is_regular(FilePath))
        end,
        [cacertfile, certfile, keyfile]
    ).

%%--------------------------------------------------------------------
%% Helper Functions
%%--------------------------------------------------------------------

listener_mqtt_tcp_conf(Config) ->
    Port = ?config(port, Config),
    PortS = integer_to_binary(Port),
    #{
        <<"acceptors">> => 16,
        <<"access_rules">> => [<<"allow all">>],
        <<"bind">> => <<"0.0.0.0:", PortS/binary>>,
        <<"max_connections">> => 1024000,
        <<"mountpoint">> => <<>>,
        <<"proxy_protocol">> => false,
        <<"proxy_protocol_timeout">> => <<"3s">>,
        <<"enable_authn">> => true
    }.

private_key_pem() ->
    pem("client-key.pem").

cert_pem() ->
    pem("client-cert.pem").

cacert_pem() ->
    pem("cacert.pem").

pem(Name) ->
    Path = filename:join([code:lib_dir(emqx), etc, certs, Name]),
    {ok, Pem} = file:read_file(Path),
    Pem.
