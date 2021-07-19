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

-export([ api_spec/0 ]).

api_spec() ->
    {[authenticator_api()], definitions()}.

authenticator_api() ->
    Example1 = #{name => <<"example">>,
                 mechanism => <<"password-based">>,
                 config => #{
                     server_type => <<"built-in-example">>,
                     user_id_type => <<"username">>,
                     password_hash_algorithm => #{
                         name => <<"sha256">>    
                     }
                 }},
    Example2 = #{name => <<"example">>,
                 mechanism => <<"password-based">>,
                 config => #{
                     server_type => <<"http-server">>,
                     method => <<"post">>,
                     url => <<"http://localhost:80/login">>,
                     headers => #{
                        <<"content-type">> => <<"application/json">>
                     },
                     form_data => #{
                        <<"username">> => <<"${mqtt-username}">>,
                        <<"password">> => <<"${mqtt-password}">>
                     }
                 }},
    Example3 = #{name => <<"example">>,
                 mechanism => <<"jwt">>,
                 config => #{
                     use_jwks => false,
                     algorithm => <<"hmac-based">>,
                     secret => <<"mysecret">>,
                     secret_base64_encoded => false,
                     verify_claims => #{
                         <<"username">> => <<"${mqtt-username}">>
                     }
                 }},
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
                                value => emqx_json:encode(Example1)
                            },
                            http => #{
                                summary => <<"Authentication provided by HTTP Server">>,
                                value => emqx_json:encode(Example2)
                            },
                            jwt => #{
                                summary => <<"JWT Authentication">>,
                                value => emqx_json:encode(Example3)
                            }
                        }
                    }
                }
            },
            responses => #{
                <<"201">> => #{
                    description => <<"Created successfully">>,
                    content => #{}
                }
            }
        }
    },
    {"/authentication/authenticators", Metadata, clients}.

definitions() ->
    AuthenticatorDef = #{
        allOf => [
            #{
                type => object,
                required => [name],
                properties => #{
                    name => #{
                        type => string,
                        example => "exmaple"
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
        type => object,
        properties => #{
            mechanism => #{
                type => string,
                enum => [<<"password-based">>],
                example => <<"password-based">>
            },
            config => #{
                oneOf => [ minirest:ref(<<"password_based_built_in_database">>)
                         , minirest:ref(<<"password_based_mysql">>)
                         , minirest:ref(<<"password_based_pgsql">>)
                         , minirest:ref(<<"password_based_http_server">>)
                         ]
            }
        }
    },

    JWTDef = #{
        type => object,
        properties => #{
            mechanism => #{
                type => string,
                enum => [<<"jwt">>],
                example => <<"jwt">>
            },
            config => #{
                type => object,
                properties => #{
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
            }
        }
    },
    
    SCRAMDef = #{
        type => object,
        properties => #{
            mechanism => #{
                type => string,
                enum => [<<"scram">>],
                example => <<"scram">>
            },
            config => #{
                type => object,
                properties => #{
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
            password_hash_algorithm => minirest:ref(<<"password_hash_algorithm">>),
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
            password_hash_algorithm => minirest:ref(<<"password_hash_algorithm">>),
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

    [ #{<<"authenticator">> => AuthenticatorDef}
    , #{<<"password_based">> => PasswordBasedDef}
    , #{<<"jwt">> => JWTDef}
    , #{<<"scram">> => SCRAMDef}
    , #{<<"password_based_built_in_database">> => PasswordBasedBuiltInDatabaseDef}
    , #{<<"password_based_mysql">> => PasswordBasedMySQLDef}
    , #{<<"password_based_pgsql">> => PasswordBasedPgSQLDef}
    , #{<<"password_based_http_server">> => PasswordBasedHTTPServerDef}
    , #{<<"password_hash_algorithm">> => PasswordHashAlgorithmDef}
    , #{<<"ssl">> => SSLDef}
    ].