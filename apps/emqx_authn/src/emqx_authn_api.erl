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

-module(emqx_authn_api).

-behavior(minirest_api).

-include("emqx_authn.hrl").

-export([ api_spec/0
        , authenticators/2
        , authenticators2/2
        , position/2
        , import_users/2
        , users/2
        , users2/2
        ]).

-define(EXAMPLE_1, #{name => <<"example 1">>,
                     mechanism => <<"password-based">>,
                     server_type => <<"built-in-example">>,
                     user_id_type => <<"username">>,
                     password_hash_algorithm => #{
                         name => <<"sha256">>    
                     }}).

-define(EXAMPLE_2, #{name => <<"example 2">>,
                     mechanism => <<"password-based">>,
                     server_type => <<"http-server">>,
                     method => <<"post">>,
                     url => <<"http://localhost:80/login">>,
                     headers => #{
                        <<"content-type">> => <<"application/json">>
                     },
                     form_data => #{
                        <<"username">> => <<"${mqtt-username}">>,
                        <<"password">> => <<"${mqtt-password}">>
                     }}).

-define(EXAMPLE_3, #{name => <<"example 3">>,
                     mechanism => <<"jwt">>,
                     use_jwks => false,
                     algorithm => <<"hmac-based">>,
                     secret => <<"mysecret">>,
                     secret_base64_encoded => false,
                     verify_claims => #{
                         <<"username">> => <<"${mqtt-username}">>
                     }}).

-define(ERR_RESPONSE(Desc), #{description => Desc,
                              content => #{
                                  'application/json' => #{
                                      schema => minirest:ref(<<"error">>),
                                      examples => #{
                                        example1 => #{
                                            summary => <<"Not Found">>,
                                            value => #{code => <<"NOT_FOUND">>, message => <<"Authenticator '67e4c9d3' does not exist">>}
                                        },
                                        example2 => #{
                                            summary => <<"Conflict">>,
                                            value => #{code => <<"ALREADY_EXISTS">>, message => <<"Name has be used">>}
                                        },
                                        example3 => #{
                                            summary => <<"Bad Request 1">>,
                                            value => #{code => <<"OUT_OF_RANGE">>, message => <<"Out of range">>}
                                        }
                                  }}}}).

api_spec() ->
    {[ authenticators_api()
     , authenticators_api2()
     , position_api()
     , import_users_api()
     , users_api()
     , users2_api()
     ], definitions()}.

authenticators_api() ->
    Metadata = #{
        post => #{
            description => "Create authenticator",
            requestBody => #{
                content => #{
                    'application/json' => #{
                        schema => minirest:ref(<<"authenticator">>),
                        examples => #{
                            default => #{
                                summary => <<"Default">>,
                                value => emqx_json:encode(?EXAMPLE_1)
                            },
                            http => #{
                                summary => <<"Authentication provided by HTTP Server">>,
                                value => emqx_json:encode(?EXAMPLE_2)
                            },
                            jwt => #{
                                summary => <<"JWT Authentication">>,
                                value => emqx_json:encode(?EXAMPLE_3)
                            }
                        }
                    }
                }
            },
            responses => #{
                <<"201">> => #{
                    description => <<"Created">>,
                    content => #{
                        'application/json' => #{
                            schema => minirest:ref(<<"returned_authenticator">>),
                            examples => #{
                                example1 => #{
                                    summary => <<"Example 1">>,
                                    value => emqx_json:encode(maps:put(id, <<"example 1">>, ?EXAMPLE_1))
                                },
                                example2 => #{
                                    summary => <<"Example 2">>,
                                    value => emqx_json:encode(maps:put(id, <<"example 2">>, ?EXAMPLE_2))
                                },
                                example3 => #{
                                    summary => <<"Example 3">>,
                                    value => emqx_json:encode(maps:put(id, <<"example 3">>, ?EXAMPLE_3))
                                }
                            }
                        }
                    }
                },
                <<"400">> => ?ERR_RESPONSE(<<"Bad Request">>),
                <<"409">> => ?ERR_RESPONSE(<<"Conflict">>)
            }
        },
        get => #{
            description => "List authenticators",
            responses => #{
                <<"200">> => #{
                    description => <<"OK">>,
                    content => #{
                        'application/json' => #{
                            schema => #{
                                type => array,
                                items => minirest:ref(<<"returned_authenticator">>)
                            },
                            examples => #{
                                example1 => #{
                                    summary => <<"Example 1">>,
                                    value => emqx_json:encode([ maps:put(id, <<"example 1">>, ?EXAMPLE_1)
                                                              , maps:put(id, <<"example 2">>, ?EXAMPLE_2)
                                                              , maps:put(id, <<"example 3">>, ?EXAMPLE_3)
                                                              ])
                                }
                            }
                        }
                    }
                }
            }
        }
    },
    {"/authentication/authenticators", Metadata, authenticators}.

authenticators_api2() ->
    Metadata = #{
        get => #{
            description => "Get authenicator by id",
            parameters => [
                #{
                    name => id,
                    in => path,
                    schema => #{
                        type => string
                    },
                    required => true
                }
            ],
            responses => #{
                <<"200">> => #{
                    description => <<"OK">>,
                    content => #{
                        'application/json' => #{
                            schema => minirest:ref(<<"returned_authenticator">>),
                            examples => #{
                                example1 => #{
                                    summary => <<"Example 1">>,
                                    value => emqx_json:encode(maps:put(id, <<"example 1">>, ?EXAMPLE_1))
                                },
                                example2 => #{
                                    summary => <<"Example 2">>,
                                    value => emqx_json:encode(maps:put(id, <<"example 2">>, ?EXAMPLE_2))
                                },
                                example3 => #{
                                    summary => <<"Example 3">>,
                                    value => emqx_json:encode(maps:put(id, <<"example 3">>, ?EXAMPLE_3))
                                }
                            }
                        }
                    }
                },
                <<"404">> => ?ERR_RESPONSE(<<"Not Found">>)
            }
        },
        put => #{
            description => "Update authenticator",
            parameters => [
                #{
                    name => id,
                    in => path,
                    schema => #{
                       type => string
                    },
                    required => true
                }
            ],
            requestBody => #{
                content => #{
                    'application/json' => #{
                        schema => #{
                            oneOf => [ minirest:ref(<<"password_based">>)
                                     , minirest:ref(<<"jwt">>)
                                     , minirest:ref(<<"scram">>)
                                     ]   
                        },
                        examples => #{
                            example1 => #{
                                summary => <<"Example 1">>,
                                value => emqx_json:encode(?EXAMPLE_1)
                            },
                            example2 => #{
                                summary => <<"Example 2">>,
                                value => emqx_json:encode(?EXAMPLE_2)
                            }
                        }
                    }
                }
            },
            responses => #{
                <<"200">> => #{
                    description => <<"OK">>,
                    content => #{
                        'application/json' => #{
                            schema => minirest:ref(<<"returned_authenticator">>),
                            examples => #{
                                example1 => #{
                                    summary => <<"Example 1">>,
                                    value => emqx_json:encode(maps:put(id, <<"example 1">>, ?EXAMPLE_1))
                                },
                                example2 => #{
                                    summary => <<"Example 2">>,
                                    value => emqx_json:encode(maps:put(id, <<"example 2">>, ?EXAMPLE_2))
                                },
                                example3 => #{
                                    summary => <<"Example 3">>,
                                    value => emqx_json:encode(maps:put(id, <<"example 3">>, ?EXAMPLE_3))
                                }
                            }
                        }
                    }
                },
                <<"400">> => ?ERR_RESPONSE(<<"Bad Request">>),
                <<"404">> => ?ERR_RESPONSE(<<"Not Found">>),
                <<"409">> => ?ERR_RESPONSE(<<"Conflict">>)
            }
        },
        delete => #{
            description => "Delete authenticator",
            parameters => [
                #{
                    name => id,
                    in => path,
                    schema => #{
                       type => string
                    },
                    required => true
                }
            ],
            responses => #{
                <<"204">> => #{
                    description => <<"No Content">>
                },
                <<"404">> => ?ERR_RESPONSE(<<"Not Found">>)
            }
        }
    },
    {"/authentication/authenticators/:id", Metadata, authenticators2}.

position_api() ->
    Metadata = #{
        post => #{
            description => "Change the order of authenticators",
            parameters => [
                #{
                    name => id,
                    in => path,
                    schema => #{
                        type => string
                    },
                    required => true
                }
            ],
            requestBody => #{
                content => #{
                    'application/json' => #{
                        schema => #{
                            type => object,
                            required => [position],
                            properties => #{
                                position => #{
                                    type => integer,
                                    example => 1
                                }
                            }
                        }
                    }
                }
            },
            responses => #{
                <<"204">> => #{
                    description => <<"No Content">>
                },
                <<"400">> => ?ERR_RESPONSE(<<"Bad Request">>),
                <<"404">> => ?ERR_RESPONSE(<<"Not Found">>)
            }
        }
    },
    {"/authentication/authenticators/:id/position", Metadata, position}.

import_users_api() ->
    Metadata = #{
        post => #{
            description => "Import users from json/csv file",
            parameters => [
                #{
                    name => id,
                    in => path,
                    schema => #{
                        type => string
                    },
                    required => true
                }
            ],
            requestBody => #{
                content => #{
                    'application/json' => #{
                        schema => #{
                            type => object,
                            required => [filename],
                            properties => #{
                                filename => #{
                                    type => string
                                }
                            }
                        }
                    }
                }
            },
            responses => #{
                <<"204">> => #{
                    description => <<"No Content">>
                },
                <<"400">> => ?ERR_RESPONSE(<<"Bad Request">>),
                <<"404">> => ?ERR_RESPONSE(<<"Not Found">>)
            }
        }
    },
    {"/authentication/authenticators/:id/import-users", Metadata, import_users}.

users_api() ->
    Metadata = #{
        post => #{
            description => "Add user",
            parameters => [
                #{
                    name => id,
                    in => path,
                    schema => #{
                        type => string
                    },
                    required => true
                }
            ],
            requestBody => #{
                content => #{
                    'application/json' => #{
                        schema => #{
                            type => object,
                            required => [user_id, password],
                            properties => #{
                                user_id => #{
                                    type => string
                                },
                                password => #{
                                    type => string
                                }
                            }
                        }
                    }
                }
            },
            responses => #{
                <<"201">> => #{
                    description => <<"Created">>,
                    content => #{
                        'application/json' => #{
                            schema => #{
                                type => object,
                                required => [user_id],
                                properties => #{
                                    user_id => #{
                                        type => string
                                    }
                                }
                            }
                        }
                    }
                },
                <<"400">> => ?ERR_RESPONSE(<<"Bad Request">>),
                <<"404">> => ?ERR_RESPONSE(<<"Not Found">>)
            }
        },
        get => #{
            description => "List users",
            parameters => [
                #{
                    name => id,
                    in => path,
                    schema => #{
                        type => string
                    },
                    required => true
                }
            ],
            responses => #{
                <<"200">> => #{
                    description => <<"OK">>,
                    content => #{
                        'application/json' => #{
                            schema => #{
                                type => array,
                                items => #{
                                    type => object,
                                    required => [user_id],
                                    properties => #{
                                        user_id => #{
                                            type => string
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                <<"404">> => ?ERR_RESPONSE(<<"Not Found">>)
            }
        }
    },
    {"/authentication/authenticators/:id/users", Metadata, users}.

users2_api() ->
    Metadata = #{
        patch => #{
            description => "Update user",
            parameters => [
                #{
                    name => id,
                    in => path,
                    schema => #{
                        type => string
                    },
                    required => true
                },
                #{
                    name => user_id,
                    in => path,
                    schema => #{
                        type => string
                    },
                    required => true
                }
            ],
            requestBody => #{
                content => #{
                    'application/json' => #{
                        schema => #{
                            type => object,
                            required => [password],
                            properties => #{
                                password => #{
                                    type => string
                                }
                            }
                        }
                    }
                }
            },
            responses => #{
                <<"200">> => #{
                    description => <<"OK">>,
                    content => #{
                        'application/json' => #{
                            schema => #{
                                type => array,
                                items => #{
                                    type => object,
                                    required => [user_id],
                                    properties => #{
                                        user_id => #{
                                            type => string
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                <<"400">> => ?ERR_RESPONSE(<<"Bad Request">>),
                <<"404">> => ?ERR_RESPONSE(<<"Not Found">>)
            }
        },
        get => #{
            description => "Get user info",
            parameters => [
                #{
                    name => id,
                    in => path,
                    schema => #{
                        type => string
                    },
                    required => true
                },
                #{
                    name => user_id,
                    in => path,
                    schema => #{
                        type => string
                    },
                    required => true
                }
            ],
            responses => #{
                <<"200">> => #{
                    description => <<"OK">>,
                    content => #{
                        'application/json' => #{
                            schema => #{
                                type => array,
                                items => #{
                                    type => object,
                                    required => [user_id],
                                    properties => #{
                                        user_id => #{
                                            type => string
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                <<"404">> => ?ERR_RESPONSE(<<"Not Found">>)
            }
        },
        delete => #{
            description => "Delete user",
            parameters => [
                #{
                    name => id,
                    in => path,
                    schema => #{
                        type => string
                    },
                    required => true
                },
                #{
                    name => user_id,
                    in => path,
                    schema => #{
                        type => string
                    },
                    required => true
                }
            ],
            responses => #{
                <<"204">> => #{
                    description => <<"No Content">>
                },
                <<"404">> => ?ERR_RESPONSE(<<"Not Found">>)
            }
        }
    },
    {"/authentication/authenticators/:id/users/:user_id", Metadata, users2}.


definitions() ->
    AuthenticatorDef = #{
        oneOf => [ minirest:ref(<<"password_based">>)
                 , minirest:ref(<<"jwt">>)
                 , minirest:ref(<<"scram">>)
                 ]   
    },

    ReturnedAuthenticatorDef = #{
        allOf => [
            #{
                type => object,
                properties => #{
                    id => #{
                        type => string
                    }
                }
            },
            #{
                oneOf => [ minirest:ref(<<"password_based">>)
                         , minirest:ref(<<"jwt">>)
                         , minirest:ref(<<"scram">>)
                         ]    
            }
        ]
    },

    PasswordBasedDef = #{
        allOf => [
            #{
                type => object,
                required => [name, mechanism],
                properties => #{
                    name => #{
                        type => string,
                        example => "exmaple"
                    },
                    mechanism => #{
                        type => string,
                        enum => [<<"password-based">>],
                        example => <<"password-based">>
                    }
                }
            },
            #{
                oneOf => [ minirest:ref(<<"password_based_built_in_database">>)
                         , minirest:ref(<<"password_based_mysql">>)
                         , minirest:ref(<<"password_based_pgsql">>)
                         , minirest:ref(<<"password_based_http_server">>)
                         ]    
            }
        ]
    },

    JWTDef = #{
        type => object,
        required => [name, mechanism],
        properties => #{
            name => #{
                type => string,
                example => "exmaple"
            },
            mechanism => #{
                type => string,
                enum => [<<"jwt">>],
                example => <<"jwt">>
            },
            use_jwks => #{
                type => boolean,
                default => false,
                example => false
            },
            algorithm => #{
                type => string,
                enum => [<<"hmac-based">>, <<"public-key">>],
                default => <<"hmac-based">>,
                example => <<"hmac-based">>
            },
            secret => #{
                type => string
            },
            secret_base64_encoded => #{
                type => boolean,
                default => false
            },
            certificate => #{
                type => string
            },
            verify_claims => #{
                type => object,
                additionalProperties => #{
                    type => string
                }
            },
            ssl => minirest:ref(<<"ssl">>)
        }
    },
    
    SCRAMDef = #{
        type => object,
        required => [name, mechanism],
        properties => #{
            name => #{
                type => string,
                example => "exmaple"
            },
            mechanism => #{
                type => string,
                enum => [<<"scram">>],
                example => <<"scram">>
            },
            server_type => #{
                type => string,
                enum => [<<"built-in-database">>],
                default => <<"built-in-database">>
            },
            algorithm => #{
                type => string,
                enum => [<<"sha256">>, <<"sha512">>],
                default => <<"sha256">>
            },
            iteration_count => #{
                type => integer,
                default => 4096
            }
        }
    },

    PasswordBasedBuiltInDatabaseDef = #{
        type => object,
        properties => #{
            server_type => #{
                type => string,
                enum => [<<"built-in-database">>],
                example => <<"built-in-database">>
            },
            user_id_type => #{
                type => string,
                enum => [<<"username">>, <<"clientid">>],
                default => <<"username">>,
                example => <<"username">>
            },
            password_hash_algorithm => minirest:ref(<<"password_hash_algorithm">>)
        }
    },

    PasswordBasedMySQLDef = #{
        type => object,
        properties => #{
            server_type => #{
                type => string,
                enum => [<<"mysql">>],
                example => <<"mysql">>
            },
            server => #{
                type => string,
                example => <<"localhost:3306">>
            },
            database => #{
                type => string
            },
            pool_size => #{
                type => integer,
                default => 8
            },
            username => #{
                type => string
            },
            password => #{
                type => string
            },
            auto_reconnect => #{
                type => boolean,
                default => true
            },
            ssl => minirest:ref(<<"ssl">>),
            password_hash_algorithm => #{
                type => string,
                enum => [<<"plain">>, <<"md5">>, <<"sha">>, <<"sha256">>, <<"sha512">>, <<"bcrypt">>],
                default => <<"sha256">>
            },
            salt_position => #{
                type => string,
                enum => [<<"prefix">>, <<"suffix">>],
                default => <<"prefix">>
            },
            query => #{
                type => string,
                example => <<"SELECT password_hash FROM mqtt_user WHERE username = ${mqtt-username}">>
            },
            query_timeout => #{
                type => integer,
                description => <<"Query timeout, Unit: Milliseconds">>,
                default => 5000
            }
        }
    },

    PasswordBasedPgSQLDef = #{
        type => object,
        properties => #{
            server_type => #{
                type => string,
                enum => [<<"pgsql">>],
                example => <<"pgsql">>
            },
            server => #{
                type => string,
                example => <<"localhost:5432">>
            },
            database => #{
                type => string
            },
            pool_size => #{
                type => integer,
                default => 8
            },
            username => #{
                type => string
            },
            password => #{
                type => string
            },
            auto_reconnect => #{
                type => boolean,
                default => true
            },
            password_hash_algorithm => #{
                type => string,
                enum => [<<"plain">>, <<"md5">>, <<"sha">>, <<"sha256">>, <<"sha512">>, <<"bcrypt">>],
                default => <<"sha256">>
            },
            salt_position => #{
                type => string,
                enum => [<<"prefix">>, <<"suffix">>],
                default => <<"prefix">>
            },
            query => #{
                type => string,
                example => <<"SELECT password_hash FROM mqtt_user WHERE username = ${mqtt-username}">>
            }
        }
    },

    PasswordBasedHTTPServerDef = #{
        type => object,
        properties => #{
            server_type => #{
                type => string,
                enum => [<<"http-server">>],
                example => <<"http-server">>
            },
            method => #{
                type => string,
                enum => [<<"get">>, <<"post">>],
                default => <<"post">>
            },
            url => #{
                type => string,
                example => <<"http://localhost:80/login">>
            },
            headers => #{
                type => object,
                additionalProperties => #{
                    type => string
                }
            },
            format_data => #{
                type => string
            },
            connect_timeout => #{
                type => integer,
                default => 5000
            },
            max_retries => #{
                type => integer,
                default => 5
            },
            retry_interval => #{
                type => integer,
                default => 1000
            },
            request_timout => #{
                type => integer,
                default => 5000
            },
            pool_size =>  #{
                type => integer,
                default => 8
            },
            enable_pipelining => #{
                type => boolean,
                default => true
            }
        }  
    },

    PasswordHashAlgorithmDef = #{
        type => object,
        required => [name],
        properties => #{
            name => #{
                type => string,
                enum => [<<"plain">>, <<"md5">>, <<"sha">>, <<"sha256">>, <<"sha512">>, <<"bcrypt">>],
                default => <<"sha256">>
            },
                salt_rounds => #{
                type => integer,
                default => 10
            }
        }
    },

    SSLDef = #{
        type => object,
        properties => #{
            enable => #{
                type => boolean,
                default => false    
            },
            certfile => #{
                type => string
            },
            keyfile => #{
                type => string
            },
            cacertfile => #{
                type => string
            },
            verify => #{
                type => boolean,
                default => true
            },
            server_name_indication => #{
                type => object,
                properties => #{
                    enable => #{
                        type => boolean,
                        default => false
                    },
                    hostname => #{
                        type => string
                    }
                }
            }
        }
    },

    ErrorDef = #{
        type => object,
        properties => #{
            code => #{
                type => string,
                enum => [<<"NOT_FOUND">>],
                example => <<"NOT_FOUND">>
            },
            message => #{
                type => string
            }
        }
    },

    [ #{<<"authenticator">> => AuthenticatorDef}
    , #{<<"returned_authenticator">> => ReturnedAuthenticatorDef}
    , #{<<"password_based">> => PasswordBasedDef}
    , #{<<"jwt">> => JWTDef}
    , #{<<"scram">> => SCRAMDef}
    , #{<<"password_based_built_in_database">> => PasswordBasedBuiltInDatabaseDef}
    , #{<<"password_based_mysql">> => PasswordBasedMySQLDef}
    , #{<<"password_based_pgsql">> => PasswordBasedPgSQLDef}
    , #{<<"password_based_http_server">> => PasswordBasedHTTPServerDef}
    , #{<<"password_hash_algorithm">> => PasswordHashAlgorithmDef}
    , #{<<"ssl">> => SSLDef}
    , #{<<"error">> => ErrorDef}
    ].

authenticators(post, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    AuthenticatorConfig = emqx_json:decode(Body, [return_maps]),
    Config = #{<<"emqx_authn">> => #{
                   <<"authenticators">> => [AuthenticatorConfig]
               }},
    NConfig = hocon_schema:check_plain(emqx_authn_schema, Config,
                                       #{nullable => true}),
    #{emqx_authn := #{authenticators := [NAuthenticatorConfig]}} = emqx_map_lib:unsafe_atom_key_map(NConfig),
    case emqx_authn:create_authenticator(?CHAIN, NAuthenticatorConfig) of
        {ok, Authenticator2} ->
            {201, Authenticator2};
        {error, Reason} ->
            serialize_error(Reason)
    end;
authenticators(get, _Request) ->
    {ok, Authenticators} = emqx_authn:list_authenticators(?CHAIN),
    {200, Authenticators}.

authenticators2(get, Request) ->
    AuthenticatorID = cowboy_req:binding(id, Request),
    case emqx_authn:lookup_authenticator(?CHAIN, AuthenticatorID) of
        {ok, Authenticator} ->
           {200, Authenticator};
        {error, Reason} ->
            serialize_error(Reason)
    end;
authenticators2(put, Request) ->
    AuthenticatorID = cowboy_req:binding(id, Request),
    {ok, Body, _} = cowboy_req:read_body(Request),
    AuthenticatorConfig = emqx_json:decode(Body, [return_maps]),
    Config = #{<<"emqx_authn">> => #{
                   <<"authenticators">> => [AuthenticatorConfig]
               }},
    NConfig = hocon_schema:check_plain(emqx_authn_schema, Config,
                                       #{nullable => true}),
    #{emqx_authn := #{authenticators := [NAuthenticatorConfig]}} = emqx_map_lib:unsafe_atom_key_map(NConfig),
    case emqx_authn:update_or_create_authenticator(?CHAIN, AuthenticatorID, NAuthenticatorConfig) of
        {ok, Authenticator} ->
            {200, Authenticator};
        {error, Reason} ->
            serialize_error(Reason)
    end;
authenticators2(delete, Request) ->
    AuthenticatorID = cowboy_req:binding(id, Request),
    case emqx_authn:delete_authenticator(?CHAIN, AuthenticatorID) of
        ok ->
            {204};
        {error, Reason} ->
            serialize_error(Reason)
    end.

position(post, Request) ->
    AuthenticatorID = cowboy_req:binding(id, Request),
    {ok, Body, _} = cowboy_req:read_body(Request),
    NBody = emqx_json:decode(Body, [return_maps]),
    Config = hocon_schema:check_plain(emqx_authn_other_schema, #{<<"position">> => NBody},
                                      #{nullable => true}, ["position"]),
    #{position := #{position := Position}} = emqx_map_lib:unsafe_atom_key_map(Config),
    case emqx_authn:move_authenticator_to_the_nth(?CHAIN, AuthenticatorID, Position) of
        ok ->
            {204};
        {error, Reason} ->
            serialize_error(Reason)
    end.

import_users(post, Request) ->
    AuthenticatorID = cowboy_req:binding(id, Request),
    {ok, Body, _} = cowboy_req:read_body(Request),
    NBody = emqx_json:decode(Body, [return_maps]),
    Config = hocon_schema:check_plain(emqx_authn_other_schema, #{<<"filename">> => NBody},
                                      #{nullable => true}, ["filename"]),
    #{filename := #{filename := Filename}} = emqx_map_lib:unsafe_atom_key_map(Config),
    case emqx_authn:import_users(?CHAIN, AuthenticatorID, Filename) of
        ok ->
            {204};
        {error, Reason} ->
            serialize_error(Reason)
    end.

users(post, Request) ->
    AuthenticatorID = cowboy_req:binding(id, Request),
    {ok, Body, _} = cowboy_req:read_body(Request),
    NBody = emqx_json:decode(Body, [return_maps]),
    Config = hocon_schema:check_plain(emqx_authn_other_schema, #{<<"user_info">> => NBody},
                                      #{nullable => true}, ["user_info"]),
    #{user_info := UserInfo} = emqx_map_lib:unsafe_atom_key_map(Config),
    case emqx_authn:add_user(?CHAIN, AuthenticatorID, UserInfo) of
        {ok, User} ->
            {201, User};
        {error, Reason} ->
            serialize_error(Reason)
    end;
users(get, Request) ->
    AuthenticatorID = cowboy_req:binding(id, Request),
    case emqx_authn:list_users(?CHAIN, AuthenticatorID) of
        {ok, Users} ->
            {200, Users};
        {error, Reason} ->
            serialize_error(Reason)
    end.

users2(patch, Request) ->
    AuthenticatorID = cowboy_req:binding(id, Request),
    UserID = cowboy_req:binding(user_id, Request),
    {ok, Body, _} = cowboy_req:read_body(Request),
    NBody = emqx_json:decode(Body, [return_maps]),
    Config = hocon_schema:check_plain(emqx_authn_other_schema, #{<<"new_user_info">> => NBody},
                                      #{nullable => true}, ["new_user_info"]),
    #{new_user_info := NewUserInfo} = emqx_map_lib:unsafe_atom_key_map(Config),
    case emqx_authn:update_user(?CHAIN, AuthenticatorID, UserID, NewUserInfo) of
        {ok, User} ->
            {200, User};
        {error, Reason} ->
            serialize_error(Reason)
    end;
users2(get, Request) ->
    AuthenticatorID = cowboy_req:binding(id, Request),
    UserID = cowboy_req:binding(user_id, Request),
    case emqx_authn:lookup_user(?CHAIN, AuthenticatorID, UserID) of
        {ok, User} ->
            {200, User};
        {error, Reason} ->
            serialize_error(Reason)
    end;
users2(delete, Request) ->
    AuthenticatorID = cowboy_req:binding(id, Request),
    UserID = cowboy_req:binding(user_id, Request),
    case emqx_authn:delete_user(?CHAIN, AuthenticatorID, UserID) of
        ok ->
            {204};
        {error, Reason} ->
            serialize_error(Reason)
    end.

serialize_error({not_found, {authenticator, ID}}) ->
    {404, #{code => <<"NOT_FOUND">>,
            message => list_to_binary(io_lib:format("Authenticator '~s' does not exist", [ID]))}};
serialize_error(name_has_be_used) ->
    {409, #{code => <<"ALREADY_EXISTS">>,
            message => <<"Name has be used">>}};
serialize_error(out_of_range) ->
    {400, #{code => <<"OUT_OF_RANGE">>,
            message => <<"Out of range">>}};
serialize_error({missing_parameter, Name}) ->
    {400, #{code => <<"MISSING_PARAMETER">>,
            message => list_to_binary(
                io_lib:format("The input parameter '~p' that is mandatory for processing this request is not supplied", [Name])
            )}};
serialize_error(Reason) ->
    {400, #{code => <<"BAD_REQUEST">>,
            message => list_to_binary(io_lib:format("Todo: ~p", [Reason]))}}.