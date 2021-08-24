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
        , authentication/3
        , authenticators/3
        , authenticators2/3
        , move/3
        , import_users/3
        , users/3
        , users2/3
        ]).

-define(EXAMPLE_1, #{name => <<"example 1">>,
                     mechanism => <<"password-based">>,
                     server_type => <<"built-in-database">>,
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

-define(EXAMPLE_4, #{name => <<"example 4">>,
                     mechanism => <<"password-based">>,
                     server_type => <<"mongodb">>,
                     server => <<"127.0.0.1:27017">>,
                     database => example,
                     collection => users,
                     selector => #{
                         username => <<"${mqtt-username}">>
                     },
                     password_hash_field => <<"password_hash">>,
                     salt_field => <<"salt">>,
                     password_hash_algorithm => <<"sha256">>,
                     salt_position => <<"prefix">>
                    }).

-define(EXAMPLE_5, #{name => <<"example 5">>,
                     mechanism => <<"password-based">>,
                     server_type => <<"redis">>,
                     server => <<"127.0.0.1:6379">>,
                     database => 0,
                     query => <<"HMGET ${mqtt-username} password_hash salt">>,
                     password_hash_algorithm => <<"sha256">>,
                     salt_position => <<"prefix">>
                    }).

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
    {[ authentication_api()
     , authenticators_api()
     , authenticators_api2()
     , move_api()
     , import_users_api()
     , users_api()
     , users2_api()
     ], definitions()}.

authentication_api() ->
    Metadata = #{
        post => #{
            description => "Enable or disbale authentication",
            requestBody => #{
                content => #{
                    'application/json' => #{
                        schema => #{
                            type => object,
                            required => [enable],
                            properties => #{
                                enable => #{
                                    type => boolean,
                                    example => true
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
                <<"400">> => ?ERR_RESPONSE(<<"Bad Request">>)
            }
        },
        get => #{
            description => "Get status of authentication",
            responses => #{
                <<"200">> => #{
                    description => <<"OK">>,
                    content => #{
                        'application/json' => #{
                            schema => #{
                                type => object,
                                properties => #{
                                    enabled => #{
                                        type => boolean,
                                        example => true
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    },
    {"/authentication", Metadata, authentication}.

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
                            },
                            mongodb => #{
                                summary => <<"Authentication with MongoDB">>,
                                value => emqx_json:encode(?EXAMPLE_4)
                            },
                            redis => #{
                                summary => <<"Authentication with Redis">>,
                                value => emqx_json:encode(?EXAMPLE_5)
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
                                %% TODO: return full content
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
                                },
                                example4 => #{
                                    summary => <<"Example 4">>,
                                    value => emqx_json:encode(maps:put(id, <<"example 4">>, ?EXAMPLE_4))
                                },
                                example5 => #{
                                    summary => <<"Example 4">>,
                                    value => emqx_json:encode(maps:put(id, <<"example 5">>, ?EXAMPLE_5))
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
                                                              , maps:put(id, <<"example 4">>, ?EXAMPLE_4)
                                                              , maps:put(id, <<"example 5">>, ?EXAMPLE_5)
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
                                },
                                example4 => #{
                                    summary => <<"Example 4">>,
                                    value => emqx_json:encode(maps:put(id, <<"example 4">>, ?EXAMPLE_4))
                                },
                                example5 => #{
                                    summary => <<"Example 5">>,
                                    value => emqx_json:encode(maps:put(id, <<"example 5">>, ?EXAMPLE_5))
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
                                },
                                example4 => #{
                                    summary => <<"Example 4">>,
                                    value => emqx_json:encode(maps:put(id, <<"example 4">>, ?EXAMPLE_4))
                                },
                                example5 => #{
                                    summary => <<"Example 5">>,
                                    value => emqx_json:encode(maps:put(id, <<"example 5">>, ?EXAMPLE_5))
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

move_api() ->
    Metadata = #{
        post => #{
            description => "Move authenticator",
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
                            oneOf => [
                                #{
                                    type => object,
                                    required => [position],
                                    properties => #{
                                        position => #{
                                            type => string,
                                            enum => [<<"top">>, <<"bottom">>],
                                            example => <<"top">>
                                        }
                                    }
                                },
                                #{
                                    type => object,
                                    required => [position],
                                    properties => #{
                                        position => #{
                                            type => string,
                                            description => <<"before:<authenticator_id>">>,
                                            example => <<"before:67e4c9d3">>
                                        }
                                    }
                                }
                            ]
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
    {"/authentication/authenticators/:id/move", Metadata, move}.

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
                                },
                                superuser => #{
                                    type => boolean,
                                    default => false
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
                                properties => #{
                                    user_id => #{
                                        type => string
                                    },
                                    superuser => #{
                                        type => boolean
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
                                    properties => #{
                                        user_id => #{
                                            type => string
                                        },
                                        superuser => #{
                                            type => boolean
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
                            properties => #{
                                password => #{
                                    type => string
                                },
                                superuser => #{
                                    type => boolean
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
                                    properties => #{
                                        user_id => #{
                                            type => string
                                        },
                                        superuser => #{
                                            type => boolean
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
                                    properties => #{
                                        user_id => #{
                                            type => string
                                        },
                                        superuser => #{
                                            type => boolean
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
                         , minirest:ref(<<"password_based_mongodb">>)
                         , minirest:ref(<<"password_based_redis">>)
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
        required => [name, mechanism, server_type],
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
        required => [server_type],
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
        required => [ server_type
                    , server
                    , database
                    , username
                    , password
                    , query],
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
        required => [ server_type
                    , server
                    , database
                    , username
                    , password
                    , query],
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

    PasswordBasedMongoDBDef = #{
        type => object,
        required => [ server_type
                    , server
                    , servers
                    , replica_set_name
                    , database
                    , username
                    , password
                    , collection
                    , selector
                    , password_hash_field
                    ],
        properties => #{
            server_type => #{
                type => string,
                enum => [<<"mongodb">>],
                example => [<<"mongodb">>]
            },
            server => #{
                description => <<"Mutually exclusive with the 'servers' field, only valid in standalone mode">>,
                type => string,
                example => <<"127.0.0.1:27017">>
            },
            servers => #{
                description => <<"Mutually exclusive with the 'server' field, only valid in replica set and sharded mode">>,
                type => array,
                items => #{
                    type => string
                },
                example => [<<"127.0.0.1:27017">>]
            },
            replica_set_name => #{
                description => <<"Only valid in replica set mode">>,
                type => string
            },
            database => #{
                type => string
            },
            username => #{
                type => string
            },
            password => #{
                type => string
            },
            auth_source => #{
                type => string,
                default => <<"admin">>
            },
            pool_size => #{
                type => integer,
                default => 8
            },
            collection => #{
                type => string
            },
            selector => #{
                type => object,
                additionalProperties => true,
                example => <<"{\"username\":\"${mqtt-username}\"}">>
            },
            password_hash_field => #{
                type => string,
                example => <<"password_hash">>
            },
            salt_field => #{
                type => string,
                example => <<"salt">>
            },
            password_hash_algorithm => #{
                type => string,
                enum => [<<"plain">>, <<"md5">>, <<"sha">>, <<"sha256">>, <<"sha512">>, <<"bcrypt">>],
                default => <<"sha256">>,
                example => <<"sha256">>
            },
            salt_position => #{
                description => <<"Only valid when the 'salt_field' field is specified">>,
                type => string,
                enum => [<<"prefix">>, <<"suffix">>],
                default => <<"prefix">>,
                example => <<"prefix">>
            }
        }
    },

    PasswordBasedRedisDef = #{
        type => object,
        required => [ server_type
                    , server
                    , servers
                    , password
                    , database
                    , query
                    ],
        properties => #{
            server_type => #{
                type => string,
                enum => [<<"redis">>],
                example => [<<"redis">>]
            },
            server => #{
                description => <<"Mutually exclusive with the 'servers' field, only valid in standalone mode">>,
                type => string,
                example => <<"127.0.0.1:27017">>
            },
            servers => #{
                description => <<"Mutually exclusive with the 'server' field, only valid in cluster and sentinel mode">>,
                type => array,
                items => #{
                    type => string
                },
                example => [<<"127.0.0.1:27017">>]
            },
            sentinel => #{
                description => <<"Only valid in sentinel mode">>,
                type => string
            },
            password => #{
                type => string
            },
            database => #{
                type => integer,
                example => 0
            },
            query => #{
                type => string,
                example => <<"HMGET ${mqtt-username} password_hash salt">>
            },
            password_hash_algorithm => #{
                type => string,
                enum => [<<"plain">>, <<"md5">>, <<"sha">>, <<"sha256">>, <<"sha512">>, <<"bcrypt">>],
                default => <<"sha256">>,
                example => <<"sha256">>
            },
            salt_position => #{
                type => string,
                enum => [<<"prefix">>, <<"suffix">>],
                default => <<"prefix">>,
                example => <<"prefix">>
            },
            pool_size => #{
                type => integer,
                default => 8
            },
            auto_reconnect => #{
                type => boolean,
                default => true
            }
        }
    },

    PasswordBasedHTTPServerDef = #{
        type => object,
        required => [ server_type
                    , url
                    , form_data
                    ],
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
            form_data => #{
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
    , #{<<"password_based_mongodb">> => PasswordBasedMongoDBDef}
    , #{<<"password_based_redis">> => PasswordBasedRedisDef}
    , #{<<"password_based_http_server">> => PasswordBasedHTTPServerDef}
    , #{<<"password_hash_algorithm">> => PasswordHashAlgorithmDef}
    , #{<<"ssl">> => SSLDef}
    , #{<<"error">> => ErrorDef}
    ].

authentication(post, _Params, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    case emqx_json:decode(Body, [return_maps]) of
        #{<<"enable">> := Enable} ->
            {ok, _} = emqx_authn:update_config([authentication, enable], {enable, Enable}),
            {204};
        _ ->
            serialize_error({missing_parameter, enable})
    end;
authentication(get, _Params, _Request) ->
    Enabled = emqx_authn:is_enabled(),
    {200, #{enabled => Enabled}}.

authenticators(post, _Params, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Config = emqx_json:decode(Body, [return_maps]),
    case emqx_authn:update_config([authentication, authenticators], {create_authenticator, Config}) of
        {ok, #{post_config_update := #{emqx_authn := #{id := ID, name := Name}},
               raw_config := RawConfig}} ->
            [RawConfig1] = [RC || #{<<"name">> := N} = RC <- RawConfig, N =:= Name],
            {200, RawConfig1#{id => ID}};
        {error, {_, _, Reason}} ->
            serialize_error(Reason)
    end;
authenticators(get, _Params, _Request) ->
    RawConfig = get_raw_config([authentication, authenticators]),
    {ok, Authenticators} = emqx_authn:list_authenticators(?CHAIN),
    NAuthenticators = lists:zipwith(fun(#{<<"name">> := Name} = Config, #{id := ID, name := Name}) ->
                                        Config#{id => ID}
                                    end, RawConfig, Authenticators),
    {200, NAuthenticators}.

authenticators2(get, _Params, Request) ->
    AuthenticatorID = cowboy_req:binding(id, Request),
    case emqx_authn:lookup_authenticator(?CHAIN, AuthenticatorID) of
        {ok, #{id := ID, name := Name}} ->
            RawConfig = get_raw_config([authentication, authenticators]),
            [RawConfig1] = [RC || #{<<"name">> := N} = RC <- RawConfig, N =:= Name],
            {200, RawConfig1#{id => ID}};
        {error, Reason} ->
            serialize_error(Reason)
    end;
authenticators2(put, _Params, Request) ->
    AuthenticatorID = cowboy_req:binding(id, Request),
    {ok, Body, _} = cowboy_req:read_body(Request),
    Config = emqx_json:decode(Body, [return_maps]),
    case emqx_authn:update_config([authentication, authenticators],
                                  {update_or_create_authenticator, AuthenticatorID, Config}) of
        {ok, #{post_config_update := #{emqx_authn := #{id := ID, name := Name}},
               raw_config := RawConfig}} ->
            [RawConfig0] = [RC || #{<<"name">> := N} = RC <- RawConfig, N =:= Name],
            {200, RawConfig0#{id => ID}};
        {error, {_, _, Reason}} ->
            serialize_error(Reason)
    end;
authenticators2(delete, _Params, Request) ->
    AuthenticatorID = cowboy_req:binding(id, Request),
    case emqx_authn:update_config([authentication, authenticators], {delete_authenticator, AuthenticatorID}) of
        {ok, _} ->
            {204};
        {error, {_, _, Reason}} ->
            serialize_error(Reason)
    end.

move(post, _Params, Request) ->
    AuthenticatorID = cowboy_req:binding(id, Request),
    {ok, Body, _} = cowboy_req:read_body(Request),
    case emqx_json:decode(Body, [return_maps]) of
        #{<<"position">> := Position} ->
            case emqx_authn:update_config([authentication, authenticators], {move_authenticator, AuthenticatorID, Position}) of
                {ok, _} -> {204};
                {error, {_, _, Reason}} -> serialize_error(Reason)
            end;
        _ ->
            serialize_error({missing_parameter, position})
    end.

import_users(post, _Params, Request) ->
    AuthenticatorID = cowboy_req:binding(id, Request),
    {ok, Body, _} = cowboy_req:read_body(Request),
    case emqx_json:decode(Body, [return_maps]) of
        #{<<"filename">> := Filename} ->
            case emqx_authn:import_users(?CHAIN, AuthenticatorID, Filename) of
                ok -> {204};
                {error, Reason} -> serialize_error(Reason)
            end;
        _ ->
            serialize_error({missing_parameter, filename})
    end.

users(post, _Params, Request) ->
    AuthenticatorID = cowboy_req:binding(id, Request),
    {ok, Body, _} = cowboy_req:read_body(Request),
    case emqx_json:decode(Body, [return_maps]) of
        #{ <<"user_id">> := UserID
         , <<"password">> := Password} = UserInfo ->
            Superuser = maps:get(<<"superuser">>, UserInfo, false),
            case emqx_authn:add_user(?CHAIN, AuthenticatorID, #{ user_id => UserID
                                                               , password => Password
                                                               , superuser => Superuser}) of
                {ok, User} ->
                    {201, User};
                {error, Reason} ->
                    serialize_error(Reason)
            end;
        #{<<"user_id">> := _} ->
            serialize_error({missing_parameter, password});
        _ ->
            serialize_error({missing_parameter, user_id})
    end;
users(get, _Params, Request) ->
    AuthenticatorID = cowboy_req:binding(id, Request),
    case emqx_authn:list_users(?CHAIN, AuthenticatorID) of
        {ok, Users} ->
            {200, Users};
        {error, Reason} ->
            serialize_error(Reason)
    end.

users2(patch, _Params, Request) ->
    AuthenticatorID = cowboy_req:binding(id, Request),
    UserID = cowboy_req:binding(user_id, Request),
    {ok, Body, _} = cowboy_req:read_body(Request),
    UserInfo = emqx_json:decode(Body, [return_maps]),
    NUserInfo = maps:with([<<"password">>, <<"superuser">>], UserInfo),
    case NUserInfo =:= #{} of
        true ->
            serialize_error({missing_parameter, password});
        false ->
            case emqx_authn:update_user(?CHAIN, AuthenticatorID, UserID, UserInfo) of
                {ok, User} ->
                    {200, User};
                {error, Reason} ->
                    serialize_error(Reason)
            end
    end;
users2(get, _Params, Request) ->
    AuthenticatorID = cowboy_req:binding(id, Request),
    UserID = cowboy_req:binding(user_id, Request),
    case emqx_authn:lookup_user(?CHAIN, AuthenticatorID, UserID) of
        {ok, User} ->
            {200, User};
        {error, Reason} ->
            serialize_error(Reason)
    end;
users2(delete, _Params, Request) ->
    AuthenticatorID = cowboy_req:binding(id, Request),
    UserID = cowboy_req:binding(user_id, Request),
    case emqx_authn:delete_user(?CHAIN, AuthenticatorID, UserID) of
        ok ->
            {204};
        {error, Reason} ->
            serialize_error(Reason)
    end.

get_raw_config(ConfKeyPath) ->
    %% TODO: call emqx_config:get_raw(ConfKeyPath) directly
    NConfKeyPath = [atom_to_binary(Key, utf8) || Key <- ConfKeyPath],
    emqx_map_lib:deep_get(NConfKeyPath, emqx_config:fill_defaults(emqx_config:get_raw([]))).

serialize_error({not_found, {authenticator, ID}}) ->
    {404, #{code => <<"NOT_FOUND">>,
            message => list_to_binary(io_lib:format("Authenticator '~s' does not exist", [ID]))}};
serialize_error(name_has_be_used) ->
    {409, #{code => <<"ALREADY_EXISTS">>,
            message => <<"Name has be used">>}};
serialize_error({missing_parameter, Name}) ->
    {400, #{code => <<"MISSING_PARAMETER">>,
            message => list_to_binary(
                io_lib:format("The input parameter '~p' that is mandatory for processing this request is not supplied", [Name])
            )}};
serialize_error({invalid_parameter, Name}) ->
    {400, #{code => <<"INVALID_PARAMETER">>,
            message => list_to_binary(
                io_lib:format("The value of input parameter '~p' is invalid", [Name])
            )}};
serialize_error(Reason) ->
    {400, #{code => <<"BAD_REQUEST">>,
            message => list_to_binary(io_lib:format("Todo: ~p", [Reason]))}}.
