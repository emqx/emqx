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
        , authentication/2
        , authentication2/2
        , authentication3/2
        , authentication4/2
        , move/2
        , move2/2
        , import_users/2
        , import_users2/2
        , users/2
        , users2/2
        , users3/2
        , users4/2
        ]).

-define(EXAMPLE_1, #{mechanism => <<"password-based">>,
                     backend => <<"built-in-database">>,
                     user_id_type => <<"username">>,
                     password_hash_algorithm => #{
                         name => <<"sha256">>
                     }}).

-define(EXAMPLE_2, #{mechanism => <<"password-based">>,
                     backend => <<"http-server">>,
                     method => <<"post">>,
                     url => <<"http://localhost:80/login">>,
                     headers => #{
                        <<"content-type">> => <<"application/json">>
                     },
                     body => #{
                        <<"username">> => <<"${mqtt-username}">>,
                        <<"password">> => <<"${mqtt-password}">>
                     }}).

-define(EXAMPLE_3, #{mechanism => <<"jwt">>,
                     use_jwks => false,
                     algorithm => <<"hmac-based">>,
                     secret => <<"mysecret">>,
                     secret_base64_encoded => false,
                     verify_claims => #{
                         <<"username">> => <<"${mqtt-username}">>
                     }}).

-define(EXAMPLE_4, #{mechanism => <<"password-based">>,
                     backend => <<"mongodb">>,
                     server => <<"127.0.0.1:27017">>,
                     database => example,
                     collection => users,
                     selector => #{
                         username => <<"${mqtt-username}">>
                     },
                     password_hash_field => <<"password_hash">>,
                     salt_field => <<"salt">>,
                     is_superuser_field => <<"is_superuser">>,
                     password_hash_algorithm => <<"sha256">>,
                     salt_position => <<"prefix">>
                    }).

-define(EXAMPLE_5, #{mechanism => <<"password-based">>,
                     backend => <<"redis">>,
                     server => <<"127.0.0.1:6379">>,
                     database => 0,
                     query => <<"HMGET ${mqtt-username} password_hash salt">>,
                     password_hash_algorithm => <<"sha256">>,
                     salt_position => <<"prefix">>
                    }).

-define(INSTANCE_EXAMPLE_1, maps:merge(?EXAMPLE_1, #{id => <<"password-based:built-in-database">>,
                                                     enable => true})).

-define(INSTANCE_EXAMPLE_2, maps:merge(?EXAMPLE_2, #{id => <<"password-based:http-server">>,
                                                     connect_timeout => 5000,
                                                     enable_pipelining => true,
                                                     headers => #{
                                                         <<"accept">> => <<"application/json">>,
                                                         <<"cache-control">> => <<"no-cache">>,
                                                         <<"connection">> => <<"keepalive">>,
                                                         <<"content-type">> => <<"application/json">>,
                                                         <<"keep-alive">> => <<"timeout=5">>
                                                     },
                                                     max_retries => 5,
                                                     pool_size => 8,
                                                     request_timeout => 5000,
                                                     retry_interval => 1000,
                                                     enable => true})).

-define(INSTANCE_EXAMPLE_3, maps:merge(?EXAMPLE_3, #{id => <<"jwt">>,
                                                     enable => true})).

-define(INSTANCE_EXAMPLE_4, maps:merge(?EXAMPLE_4, #{id => <<"password-based:mongodb">>,
                                                     mongo_type => <<"single">>,
                                                     pool_size => 8,
                                                     ssl => #{
                                                         enable => false
                                                     },
                                                     topology => #{
                                                         max_overflow => 8,
                                                         pool_size => 8 
                                                     },
                                                     enable => true})).

-define(INSTANCE_EXAMPLE_5, maps:merge(?EXAMPLE_5, #{id => <<"password-based:redis">>,
                                                     auto_reconnect => true,
                                                     redis_type => single,
                                                     pool_size => 8,
                                                     ssl => #{
                                                         enable => false
                                                     },
                                                     enable => true})).

-define(ERR_RESPONSE(Desc), #{description => Desc,
                              content => #{
                                  'application/json' => #{
                                      schema => minirest:ref(<<"Error">>),
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
     , authentication_api2()
     , move_api()
     , authentication_api3()
     , authentication_api4()
     , move_api2()
     , import_users_api()
     , import_users_api2()
     , users_api()
     , users2_api()
     , users3_api()
     , users4_api()
     ], definitions()}.

authentication_api() ->
    Metadata = #{
        post => create_authenticator_api_spec(),
        get => list_authenticators_api_spec()
    },
    {"/authentication", Metadata, authentication}.

authentication_api2() ->
    Metadata = #{
        get => find_authenticator_api_spec(),
        put => update_authenticator_api_spec(),
        delete => delete_authenticator_api_spec()
    },
    {"/authentication/:id", Metadata, authentication2}.

authentication_api3() ->
    Metadata = #{
        post => create_authenticator_api_spec2(),
        get => list_authenticators_api_spec2()
    },
    {"/listeners/:listener_id/authentication", Metadata, authentication3}.

authentication_api4() ->
    Metadata = #{
        get => find_authenticator_api_spec2(),
        put => update_authenticator_api_spec2(),
        delete => delete_authenticator_api_spec2()
    },
    {"/listeners/:listener_id/authentication/:id", Metadata, authentication4}.

move_api() ->
    Metadata = #{
        post => move_authenticator_api_spec()
    },
    {"/authentication/:id/move", Metadata, move}.

move_api2() ->
    Metadata = #{
        post => move_authenticator_api_spec2()
    },
    {"/listeners/:listener_id/authentication/:id/move", Metadata, move2}.

import_users_api() ->
    Metadata = #{
        post => import_users_api_spec()
    },
    {"/authentication/:id/import_users", Metadata, import_users}.

import_users_api2() ->
    Metadata = #{
        post => import_users_api_spec2()
    },
    {"/listeners/:listener_id/authentication/:id/import_users", Metadata, import_users2}.

users_api() ->
    Metadata = #{
        post => create_user_api_spec(),
        get => list_users_api_spec()
    },
    {"/authentication/:id/users", Metadata, users}.

users2_api() ->
    Metadata = #{
        put => update_user_api_spec(),
        get => find_user_api_spec(),
        delete => delete_user_api_spec()
    },
    {"/authentication/:id/users/:user_id", Metadata, users2}.

users3_api() ->
    Metadata = #{
        post => create_user_api_spec2(),
        get => list_users_api_spec2()
    },
    {"/listeners/:listener_id/authentication/:id/users", Metadata, users3}.

users4_api() ->
    Metadata = #{
        put => update_user_api_spec2(),
        get => find_user_api_spec2(),
        delete => delete_user_api_spec2()
    },
    {"/listeners/:listener_id/authentication/:id/users/:user_id", Metadata, users4}.

create_authenticator_api_spec() ->
    #{
        description => "Create a authenticator for global authentication",
        requestBody => #{
            content => #{
                'application/json' => #{
                    schema => minirest:ref(<<"AuthenticatorConfig">>),
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
                        schema => minirest:ref(<<"AuthenticatorInstance">>),
                        examples => #{
                            example1 => #{
                                summary => <<"Example 1">>,
                                value => emqx_json:encode(?INSTANCE_EXAMPLE_1)
                            },
                            example2 => #{
                                summary => <<"Example 2">>,
                                value => emqx_json:encode(?INSTANCE_EXAMPLE_2)
                            },
                            example3 => #{
                                summary => <<"Example 3">>,
                                value => emqx_json:encode(?INSTANCE_EXAMPLE_3)
                            },
                            example4 => #{
                                summary => <<"Example 4">>,
                                value => emqx_json:encode(?INSTANCE_EXAMPLE_4)
                            },
                            example5 => #{
                                summary => <<"Example 5">>,
                                value => emqx_json:encode(?INSTANCE_EXAMPLE_5)
                            }
                        }
                    }
                }
            },
            <<"400">> => ?ERR_RESPONSE(<<"Bad Request">>),
            <<"409">> => ?ERR_RESPONSE(<<"Conflict">>)
        }    
    }.

create_authenticator_api_spec2() ->
    Spec = create_authenticator_api_spec(),
    Spec#{
        description => "Create a authenticator for listener",
        parameters => [
            #{
                name => listener_id,
                in => path,
                schema => #{
                    type => string
                },
                required => true
            }
        ]
    }.

list_authenticators_api_spec() ->
    #{
        description => "List authenticators for global authentication",
        responses => #{
            <<"200">> => #{
                description => <<"OK">>,
                content => #{
                    'application/json' => #{
                        schema => #{
                            type => array,
                            items => minirest:ref(<<"AuthenticatorInstance">>)
                        },
                        examples => #{
                            example => #{
                                summary => <<"Example">>,
                                value => emqx_json:encode([ ?INSTANCE_EXAMPLE_1
                                                          , ?INSTANCE_EXAMPLE_2
                                                          , ?INSTANCE_EXAMPLE_3
                                                          , ?INSTANCE_EXAMPLE_4
                                                          , ?INSTANCE_EXAMPLE_5
                                                          ])}}}}}}}.

list_authenticators_api_spec2() ->
    Spec = list_authenticators_api_spec(),
    Spec#{
        description => "List authenticators for listener",
        parameters => [
            #{
                name => listener_id,
                in => path,
                schema => #{
                    type => string
                },
                required => true
            }
        ]
    }.

find_authenticator_api_spec() ->
    #{
        description => "Get authenticator by id",
        parameters => [
            #{
                name => id,
                in => path,
                description => "Authenticator id",
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
                        schema => minirest:ref(<<"AuthenticatorInstance">>),
                        examples => #{
                            example1 => #{
                                summary => <<"Example 1">>,
                                value => emqx_json:encode(?INSTANCE_EXAMPLE_1)
                            },
                            example2 => #{
                                summary => <<"Example 2">>,
                                value => emqx_json:encode(?INSTANCE_EXAMPLE_2)
                            },
                            example3 => #{
                                summary => <<"Example 3">>,
                                value => emqx_json:encode(?INSTANCE_EXAMPLE_3)
                            },
                            example4 => #{
                                summary => <<"Example 4">>,
                                value => emqx_json:encode(?INSTANCE_EXAMPLE_4)
                            },
                            example5 => #{
                                summary => <<"Example 5">>,
                                value => emqx_json:encode(?INSTANCE_EXAMPLE_5)
                            }
                        }
                    }
                }
            },
            <<"404">> => ?ERR_RESPONSE(<<"Not Found">>)
        }
    }.

find_authenticator_api_spec2() ->
    Spec = find_authenticator_api_spec(),
    Spec#{
        parameters => [
            #{
                name => listener_id,
                in => path,
                description => "Listener id",
                schema => #{
                    type => string
                },
                required => true
            },
            #{
                name => id,
                in => path,
                description => "Authenticator id",
                schema => #{
                    type => string
                },
                required => true
            }
        ]
    }.

update_authenticator_api_spec() ->
    #{
        description => "Update authenticator",
        parameters => [
            #{
                name => id,
                in => path,
                description => "Authenticator id",
                schema => #{
                    type => string
                },
                required => true
            }
        ],
        requestBody => #{
            content => #{
                'application/json' => #{
                    schema => minirest:ref(<<"AuthenticatorConfig">>),
                    examples => #{
                        example1 => #{
                            summary => <<"Example 1">>,
                            value => emqx_json:encode(?EXAMPLE_1)
                        },
                        example2 => #{
                            summary => <<"Example 2">>,
                            value => emqx_json:encode(?EXAMPLE_2)
                        },
                        example3 => #{
                            summary => <<"Example 3">>,
                            value => emqx_json:encode(?EXAMPLE_3)
                        },
                        example4 => #{
                            summary => <<"Example 4">>,
                            value => emqx_json:encode(?EXAMPLE_4)
                        },
                        example5 => #{
                            summary => <<"Example 5">>,
                            value => emqx_json:encode(?EXAMPLE_5)
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
                        schema => minirest:ref(<<"AuthenticatorInstance">>),
                        examples => #{
                            example1 => #{
                                summary => <<"Example 1">>,
                                value => emqx_json:encode(?INSTANCE_EXAMPLE_1)
                            },
                            example2 => #{
                                summary => <<"Example 2">>,
                                value => emqx_json:encode(?INSTANCE_EXAMPLE_2)
                            },
                            example3 => #{
                                summary => <<"Example 3">>,
                                value => emqx_json:encode(?INSTANCE_EXAMPLE_3)
                            },
                            example4 => #{
                                summary => <<"Example 4">>,
                                value => emqx_json:encode(?INSTANCE_EXAMPLE_4)
                            },
                            example5 => #{
                                summary => <<"Example 5">>,
                                value => emqx_json:encode(?INSTANCE_EXAMPLE_5)
                            }
                        }
                    }
                }
            },
            <<"400">> => ?ERR_RESPONSE(<<"Bad Request">>),
            <<"404">> => ?ERR_RESPONSE(<<"Not Found">>),
            <<"409">> => ?ERR_RESPONSE(<<"Conflict">>)
        }
    }.

update_authenticator_api_spec2() ->
    Spec = update_authenticator_api_spec(),
    Spec#{
        parameters => [
            #{
                name => listener_id,
                in => path,
                description => "Listener id",
                schema => #{
                    type => string
                },
                required => true
            },
            #{
                name => id,
                in => path,
                description => "Authenticator id",
                schema => #{
                    type => string
                },
                required => true
            }
        ]
    }.

delete_authenticator_api_spec() ->
    #{
        description => "Delete authenticator",
        parameters => [
            #{
                name => id,
                in => path,
                description => "Authenticator id",
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
    }.

delete_authenticator_api_spec2() ->
    Spec = delete_authenticator_api_spec(),
    Spec#{
        parameters => [
            #{
                name => listener_id,
                in => path,
                description => "Listener id",
                schema => #{
                    type => string
                },
                required => true
            },
            #{
                name => id,
                in => path,
                description => "Authenticator id",
                schema => #{
                    type => string
                },
                required => true
            }
        ]
    }.

move_authenticator_api_spec() ->
    #{
        description => "Move authenticator",
        parameters => [
            #{
                name => id,
                in => path,
                description => "Authenticator id",
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
                                        example => <<"before:password-based:mysql">>
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
    }.

move_authenticator_api_spec2() ->
    Spec = move_authenticator_api_spec(),
    Spec#{
        parameters => [
            #{
                name => listener_id,
                in => path,
                description => "Listener id",
                schema => #{
                    type => string
                },
                required => true
            },
            #{
                name => id,
                in => path,
                description => "Authenticator id",
                schema => #{
                    type => string
                },
                required => true
            }
        ]
    }.

import_users_api_spec() ->
    #{
        description => "Import users from json/csv file",
        parameters => [
            #{
                name => id,
                in => path,
                description => "Authenticator id",
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
    }.

import_users_api_spec2() ->
    Spec = import_users_api_spec(),
    Spec#{
        parameters => [
            #{
                name => listener_id,
                in => path,
                description => "Listener id",
                schema => #{
                    type => string
                },
                required => true
            },
            #{
                name => id,
                in => path,
                description => "Authenticator id",
                schema => #{
                    type => string
                },
                required => true
            }
        ]
    }.

create_user_api_spec() ->
    #{
        description => "Add user",
        parameters => [
            #{
                name => id,
                in => path,
                description => "Authenticator id",
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
                            is_superuser => #{
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
                                is_superuser => #{
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
    }.

create_user_api_spec2() ->
    Spec = create_user_api_spec(),
    Spec#{
        parameters => [
            #{
                name => listener_id,
                in => path,
                description => "Listener id",
                schema => #{
                    type => string
                },
                required => true
            },
            #{
                name => id,
                in => path,
                description => "Authenticator id",
                schema => #{
                    type => string
                },
                required => true
            }
        ]
    }.

list_users_api_spec() ->
    #{
        description => "List users",
        parameters => [
            #{
                name => id,
                in => path,
                description => "Authenticator id",
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
                                    is_superuser => #{
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
    }.

list_users_api_spec2() ->
    Spec = list_users_api_spec(),
    Spec#{
        parameters => [
            #{
                name => listener_id,
                in => path,
                description => "Listener id",
                schema => #{
                    type => string
                },
                required => true
            },
            #{
                name => id,
                in => path,
                description => "Authenticator id",
                schema => #{
                    type => string
                },
                required => true
            }
        ]
    }.

update_user_api_spec() ->
    #{
        description => "Update user",
        parameters => [
            #{
                name => id,
                in => path,
                description => "Authenticator id",
                schema => #{
                    type => string
                },
                required => true
            },
            #{
                name => user_id,
                in => path,
                description => "User id",
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
                            is_superuser => #{
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
                                    is_superuser => #{
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
    }.

update_user_api_spec2() ->
    Spec = update_user_api_spec(),
    Spec#{
        parameters => [
            #{
                name => listener_id,
                in => path,
                description => "Listener id",
                schema => #{
                    type => string
                },
                required => true
            },
            #{
                name => id,
                in => path,
                description => "Authenticator id",
                schema => #{
                    type => string
                },
                required => true
            },
            #{
                name => user_id,
                in => path,
                description => "User id",
                schema => #{
                    type => string
                },
                required => true
            }
        ]
    }.

find_user_api_spec() ->
    #{
        description => "Get user info",
        parameters => [
            #{
                name => id,
                in => path,
                description => "Authenticator id",
                schema => #{
                    type => string
                },
                required => true
            },
            #{
                name => user_id,
                in => path,
                description => "User id",
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
                                    is_superuser => #{
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
    }.

find_user_api_spec2() ->
    Spec = find_user_api_spec(),
    Spec#{
        parameters => [
            #{
                name => listener_id,
                in => path,
                description => "Listener id",
                schema => #{
                    type => string
                },
                required => true
            },
            #{
                name => id,
                in => path,
                description => "Authenticator id",
                schema => #{
                    type => string
                },
                required => true
            },
            #{
                name => user_id,
                in => path,
                description => "User id",
                schema => #{
                    type => string
                },
                required => true
            }
        ]
    }.

delete_user_api_spec() ->
    #{
        description => "Delete user",
        parameters => [
            #{
                name => id,
                in => path,
                description => "Authenticator id",
                schema => #{
                    type => string
                },
                required => true
            },
            #{
                name => user_id,
                in => path,
                description => "User id",
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
    }.

delete_user_api_spec2() ->
    Spec = delete_user_api_spec(),
    Spec#{
        parameters => [
            #{
                name => listener_id,
                in => path,
                description => "Listener id",
                schema => #{
                    type => string
                },
                required => true
            },
            #{
                name => id,
                in => path,
                description => "Authenticator id",
                schema => #{
                    type => string
                },
                required => true
            },
            #{
                name => user_id,
                in => path,
                description => "User id",
                schema => #{
                    type => string
                },
                required => true
            }
        ]
    }.


definitions() ->
    AuthenticatorConfigDef = #{
        allOf => [
            #{
                type => object,
                properties => #{
                    enable => #{
                        type => boolean,
                        default => true,
                        example => true
                    }
                }
            },
            #{
                oneOf => [ minirest:ref(<<"PasswordBasedBuiltInDatabase">>)
                         , minirest:ref(<<"PasswordBasedMySQL">>)
                         , minirest:ref(<<"PasswordBasedPostgreSQL">>)
                         , minirest:ref(<<"PasswordBasedMongoDB">>)
                         , minirest:ref(<<"PasswordBasedRedis">>)
                         , minirest:ref(<<"PasswordBasedHTTPServer">>)
                         , minirest:ref(<<"JWT">>)
                         , minirest:ref(<<"SCRAMBuiltInDatabase">>)
                         ]
            }
        ]
    },

    AuthenticatorInstanceDef = #{
        allOf => [
            #{
                type => object,
                properties => #{
                    id => #{
                        type => string
                    }
                }
            }
        ] ++ maps:get(allOf, AuthenticatorConfigDef)
    },

    PasswordBasedBuiltInDatabaseDef = #{
        type => object,
        required => [mechanism, backend],
        properties => #{
            mechanism => #{
                type => string,
                enum => [<<"password-based">>],
                example => <<"password-based">>
            },
            backend => #{
                type => string,
                enum => [<<"built-in-database">>],
                example => <<"built-in-database">>
            },
            user_id_type => #{
                type => string,
                enum => [<<"username">>, <<"clientid">>],
                example => <<"username">>
            },
            password_hash_algorithm => minirest:ref(<<"PasswordHashAlgorithm">>)
        }
    },

    PasswordBasedMySQLDef = #{
        type => object,
        required => [ mechanism
                    , backend
                    , server
                    , database
                    , username
                    , password
                    , query],
        properties => #{
            mechanism => #{
                type => string,
                enum => [<<"password-based">>],
                example => <<"password-based">>
            },
            backend => #{
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
            ssl => minirest:ref(<<"SSL">>),
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

    PasswordBasedPostgreSQLDef = #{
        type => object,
        required => [ mechanism
                    , backend
                    , server
                    , database
                    , username
                    , password
                    , query],
        properties => #{
            mechanism => #{
                type => string,
                enum => [<<"password-based">>],
                example => <<"password-based">>
            },
            backend => #{
                type => string,
                enum => [<<"postgresql">>],
                example => <<"postgresql">>
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
        required => [ mechanism
                    , backend
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
            mechanism => #{
                type => string,
                enum => [<<"password-based">>],
                example => <<"password-based">>
            },
            backend => #{
                type => string,
                enum => [<<"mongodb">>],
                example => <<"mongodb">>
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
            is_superuser_field => #{
                type => string,
                example => <<"is_superuser">>
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
        required => [ mechanism
                    , backend
                    , server
                    , servers
                    , password
                    , database
                    , query
                    ],
        properties => #{
            mechanism => #{
                type => string,
                enum => [<<"password-based">>],
                example => <<"password-based">>
            },
            backend => #{
                type => string,
                enum => [<<"redis">>],
                example => <<"redis">>
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
        required => [ mechanism
                    , backend
                    , url
                    , body
                    ],
        properties => #{
            mechanism => #{
                type => string,
                enum => [<<"password-based">>],
                example => <<"password-based">>
            },
            backend => #{
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
            body => #{
                type => object
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
            },
            ssl => minirest:ref(<<"SSL">>)
        }
    },

    JWTDef = #{
        type => object,
        required => [mechanism],
        properties => #{
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
            endpoint => #{
                type => string,
                example => <<"http://localhost:80">>
            },
            verify_claims => #{
                type => object,
                additionalProperties => #{
                    type => string
                }
            },
            ssl => minirest:ref(<<"SSL">>)
        }
    },

    SCRAMBuiltInDatabaseDef = #{
        type => object,
        required => [mechanism, backend],
        properties => #{
            mechanism => #{
                type => string,
                enum => [<<"scram">>],
                example => <<"scram">>
            },
            backend => #{
                type => string,
                enum => [<<"built-in-database">>],
                example => <<"built-in-database">>
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
                description => <<"Only valid when the name field is set to bcrypt">>,
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

    [ #{<<"AuthenticatorConfig">> => AuthenticatorConfigDef}
    , #{<<"AuthenticatorInstance">> => AuthenticatorInstanceDef}
    , #{<<"PasswordBasedBuiltInDatabase">> => PasswordBasedBuiltInDatabaseDef}
    , #{<<"PasswordBasedMySQL">> => PasswordBasedMySQLDef}
    , #{<<"PasswordBasedPostgreSQL">> => PasswordBasedPostgreSQLDef}
    , #{<<"PasswordBasedMongoDB">> => PasswordBasedMongoDBDef}
    , #{<<"PasswordBasedRedis">> => PasswordBasedRedisDef}
    , #{<<"PasswordBasedHTTPServer">> => PasswordBasedHTTPServerDef}
    , #{<<"JWT">> => JWTDef}
    , #{<<"SCRAMBuiltInDatabase">> => SCRAMBuiltInDatabaseDef}
    , #{<<"PasswordHashAlgorithm">> => PasswordHashAlgorithmDef}
    , #{<<"SSL">> => SSLDef}
    , #{<<"Error">> => ErrorDef}
    ].

authentication(post, #{body := Config}) ->
    create_authenticator([authentication], ?GLOBAL, Config);

authentication(get, _Params) ->
    list_authenticators([authentication]).

authentication2(get, #{bindings := #{id := AuthenticatorID}}) ->
    list_authenticator([authentication], AuthenticatorID);

authentication2(put, #{bindings := #{id := AuthenticatorID}, body := Config}) ->
    update_authenticator([authentication], ?GLOBAL, AuthenticatorID, Config);

authentication2(delete, #{bindings := #{id := AuthenticatorID}}) ->
    delete_authenticator([authentication], ?GLOBAL, AuthenticatorID).

authentication3(post, #{bindings := #{listener_id := ListenerID}, body := Config}) ->
    case find_listener(ListenerID) of
        {ok, {Type, Name}} ->
            create_authenticator([listeners, Type, Name, authentication], ListenerID, Config);
        {error, Reason} ->
            serialize_error(Reason)
    end;
authentication3(get, #{bindings := #{listener_id := ListenerID}}) ->
    case find_listener(ListenerID) of
        {ok, {Type, Name}} ->
            list_authenticators([listeners, Type, Name, authentication]);
        {error, Reason} ->
            serialize_error(Reason)
    end.

authentication4(get, #{bindings := #{listener_id := ListenerID, id := AuthenticatorID}}) ->
    case find_listener(ListenerID) of
        {ok, {Type, Name}} ->
            list_authenticator([listeners, Type, Name, authentication], AuthenticatorID);
        {error, Reason} ->
            serialize_error(Reason)
    end;
authentication4(put, #{bindings := #{listener_id := ListenerID, id := AuthenticatorID}, body := Config}) ->
    case find_listener(ListenerID) of
        {ok, {Type, Name}} ->
            update_authenticator([listeners, Type, Name, authentication], ListenerID, AuthenticatorID, Config);
        {error, Reason} ->
            serialize_error(Reason)
    end;
authentication4(delete, #{bindings := #{listener_id := ListenerID, id := AuthenticatorID}}) ->
    case find_listener(ListenerID) of
        {ok, {Type, Name}} ->
            delete_authenticator([listeners, Type, Name, authentication], ListenerID, AuthenticatorID);
        {error, Reason} ->
            serialize_error(Reason)
    end.

move(post, #{bindings := #{id := AuthenticatorID}, body := #{<<"position">> := Position}}) ->
    move_authenitcator([authentication], ?GLOBAL, AuthenticatorID, Position);
move(post, #{bindings := #{id := _}, body := _}) ->
    serialize_error({missing_parameter, position}).

move2(post, #{bindings := #{listener_id := ListenerID, id := AuthenticatorID}, body := #{<<"position">> := Position}}) ->
    case find_listener(ListenerID) of
        {ok, {Type, Name}} ->
            move_authenitcator([listeners, Type, Name, authentication], ListenerID, AuthenticatorID, Position);
        {error, Reason} ->
            serialize_error(Reason)
    end;
move2(post, #{bindings := #{listener_id := _, id := _}, body := _}) ->
    serialize_error({missing_parameter, position}).

import_users(post, #{bindings := #{id := AuthenticatorID}, body := #{<<"filename">> := Filename}}) ->
    case ?AUTHN:import_users(?GLOBAL, AuthenticatorID, Filename) of
        ok -> {204};
        {error, Reason} -> serialize_error(Reason)
    end;
import_users(post, #{bindings := #{id := _}, body := _}) ->
    serialize_error({missing_parameter, filename}).

import_users2(post, #{bindings := #{listener_id := ListenerID, id := AuthenticatorID}, body := #{<<"filename">> := Filename}}) ->
    case ?AUTHN:import_users(ListenerID, AuthenticatorID, Filename) of
        ok -> {204};
        {error, Reason} -> serialize_error(Reason)
    end;
import_users2(post, #{bindings := #{listener_id := _, id := _}, body := _}) ->
    serialize_error({missing_parameter, filename}).

users(post, #{bindings := #{id := AuthenticatorID}, body := UserInfo}) ->
    add_user(?GLOBAL, AuthenticatorID, UserInfo);
users(get, #{bindings := #{id := AuthenticatorID}}) ->
    list_users(?GLOBAL, AuthenticatorID).

users2(put, #{bindings := #{id := AuthenticatorID,
                            user_id := UserID}, body := UserInfo}) ->
    update_user(?GLOBAL, AuthenticatorID, UserID, UserInfo);
users2(get, #{bindings := #{id := AuthenticatorID, user_id := UserID}}) ->
    find_user(?GLOBAL, AuthenticatorID, UserID);
users2(delete, #{bindings := #{id := AuthenticatorID, user_id := UserID}}) ->
    delete_user(?GLOBAL, AuthenticatorID, UserID).

users3(post, #{bindings := #{listener_id := ListenerID,
                             id := AuthenticatorID}, body := UserInfo}) ->
    add_user(ListenerID, AuthenticatorID, UserInfo);
users3(get, #{bindings := #{listener_id := ListenerID,
                            id := AuthenticatorID}}) ->
    list_users(ListenerID, AuthenticatorID).

users4(put, #{bindings := #{listener_id := ListenerID,
                            id := AuthenticatorID,
                            user_id := UserID}, body := UserInfo}) ->
    update_user(ListenerID, AuthenticatorID, UserID, UserInfo);
users4(get, #{bindings := #{listener_id := ListenerID,
                            id := AuthenticatorID,
                            user_id := UserID}}) ->
    find_user(ListenerID, AuthenticatorID, UserID);
users4(delete, #{bindings := #{listener_id := ListenerID,
                               id := AuthenticatorID,
                               user_id := UserID}}) ->
    delete_user(ListenerID, AuthenticatorID, UserID).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

find_listener(ListenerID) ->
    {Type, Name} = emqx_listeners:parse_listener_id(ListenerID),
    case emqx_config:find([listeners, Type, Name]) of
        {not_found, _, _} ->
            {error, {not_found, {listener, ListenerID}}};
        {ok, _} ->
            {ok, {Type, Name}}
    end.

create_authenticator(ConfKeyPath, ChainName0, Config) ->
    ChainName = to_atom(ChainName0),
    case update_config(ConfKeyPath, {create_authenticator, ChainName, Config}) of
        {ok, #{post_config_update := #{?AUTHN := #{id := ID}},
               raw_config := AuthenticatorsConfig}} ->
            {ok, AuthenticatorConfig} = find_config(ID, AuthenticatorsConfig),
            {200, maps:put(id, ID, convert_certs(fill_defaults(AuthenticatorConfig)))};
        {error, {_, _, Reason}} ->
            serialize_error(Reason)
    end.

list_authenticators(ConfKeyPath) ->
    AuthenticatorsConfig = get_raw_config_with_defaults(ConfKeyPath),
    NAuthenticators = [maps:put(id, ?AUTHN:generate_id(AuthenticatorConfig), convert_certs(AuthenticatorConfig))
                        || AuthenticatorConfig <- AuthenticatorsConfig],
    {200, NAuthenticators}.

list_authenticator(ConfKeyPath, AuthenticatorID) ->
    AuthenticatorsConfig = get_raw_config_with_defaults(ConfKeyPath),
    case find_config(AuthenticatorID, AuthenticatorsConfig) of
        {ok, AuthenticatorConfig} ->
            {200, maps:put(id, AuthenticatorID, convert_certs(AuthenticatorConfig))};
        {error, Reason} ->
            serialize_error(Reason)
    end.

update_authenticator(ConfKeyPath, ChainName0, AuthenticatorID, Config) ->
    ChainName = to_atom(ChainName0),
    case update_config(ConfKeyPath, {update_authenticator, ChainName, AuthenticatorID, Config}) of
        {ok, #{post_config_update := #{?AUTHN := #{id := ID}},
               raw_config := AuthenticatorsConfig}} ->
            {ok, AuthenticatorConfig} = find_config(ID, AuthenticatorsConfig),
            {200, maps:put(id, ID, convert_certs(fill_defaults(AuthenticatorConfig)))};
        {error, {_, _, Reason}} ->
            serialize_error(Reason)
    end.

delete_authenticator(ConfKeyPath, ChainName0, AuthenticatorID) ->
    ChainName = to_atom(ChainName0),
    case update_config(ConfKeyPath, {delete_authenticator, ChainName, AuthenticatorID}) of
        {ok, _} ->
            {204};
        {error, {_, _, Reason}} ->
            serialize_error(Reason)
    end.

move_authenitcator(ConfKeyPath, ChainName0, AuthenticatorID, Position) ->
    ChainName = to_atom(ChainName0),
    case parse_position(Position) of
        {ok, NPosition} ->
            case update_config(ConfKeyPath, {move_authenticator, ChainName, AuthenticatorID, NPosition}) of
                {ok, _} ->
                    {204};
                {error, {_, _, Reason}} ->
                    serialize_error(Reason)
            end;
        {error, Reason} ->
            serialize_error(Reason)
    end.

add_user(ChainName0, AuthenticatorID, #{<<"user_id">> := UserID, <<"password">> := Password} = UserInfo) ->
    ChainName = to_atom(ChainName0),
    IsSuperuser = maps:get(<<"is_superuser">>, UserInfo, false),
    case ?AUTHN:add_user(ChainName, AuthenticatorID, #{ user_id => UserID
                                                      , password => Password
                                                      , is_superuser => IsSuperuser}) of
        {ok, User} ->
            {201, User};
        {error, Reason} ->
            serialize_error(Reason)
    end;
add_user(_, _, #{<<"user_id">> := _}) ->
    serialize_error({missing_parameter, password});
add_user(_, _, _) ->
    serialize_error({missing_parameter, user_id}).

update_user(ChainName0, AuthenticatorID, UserID, UserInfo) ->
    ChainName = to_atom(ChainName0),
    case maps:with([<<"password">>, <<"is_superuser">>], UserInfo) =:= #{} of
        true ->
            serialize_error({missing_parameter, password});
        false ->
            case ?AUTHN:update_user(ChainName, AuthenticatorID, UserID, UserInfo) of
                {ok, User} ->
                    {200, User};
                {error, Reason} ->
                    serialize_error(Reason)
            end
    end.

find_user(ChainName0, AuthenticatorID, UserID) ->
    ChainName = to_atom(ChainName0),
    case ?AUTHN:lookup_user(ChainName, AuthenticatorID, UserID) of
        {ok, User} ->
            {200, User};
        {error, Reason} ->
            serialize_error(Reason)
    end.

delete_user(ChainName0, AuthenticatorID, UserID) ->
    ChainName = to_atom(ChainName0),
    case ?AUTHN:delete_user(ChainName, AuthenticatorID, UserID) of
        ok ->
            {204};
        {error, Reason} ->
            serialize_error(Reason)
    end.

list_users(ChainName0, AuthenticatorID) ->
    ChainName = to_atom(ChainName0),
    case ?AUTHN:list_users(ChainName, AuthenticatorID) of
        {ok, Users} ->
            {200, Users};
        {error, Reason} ->
            serialize_error(Reason)
    end.

update_config(Path, ConfigRequest) ->
    emqx:update_config(Path, ConfigRequest, #{rawconf_with_defaults => true}).

get_raw_config_with_defaults(ConfKeyPath) ->
    NConfKeyPath = [atom_to_binary(Key, utf8) || Key <- ConfKeyPath],
    RawConfig = emqx_map_lib:deep_get(NConfKeyPath, emqx_config:get_raw([]), []),
    to_list(fill_defaults(RawConfig)).

find_config(AuthenticatorID, AuthenticatorsConfig) ->
    case [AC || AC <- to_list(AuthenticatorsConfig), AuthenticatorID =:= ?AUTHN:generate_id(AC)] of
        [] -> {error, {not_found, {authenticator, AuthenticatorID}}};
        [AuthenticatorConfig] -> {ok, AuthenticatorConfig}
    end.

fill_defaults(Config) ->
    #{<<"authentication">> := CheckedConfig} = hocon_schema:check_plain(
        ?AUTHN, #{<<"authentication">> => Config}, #{nullable => true, no_conversion => true}),
    CheckedConfig.

convert_certs(#{<<"ssl">> := SSLOpts} = Config) ->
    NSSLOpts = lists:foldl(fun(K, Acc) ->
                               case maps:get(K, Acc, undefined) of
                                   undefined -> Acc;
                                   Filename ->
                                       {ok, Bin} = file:read_file(Filename),
                                       Acc#{K => Bin}
                               end
                           end, SSLOpts, [<<"certfile">>, <<"keyfile">>, <<"cacertfile">>]),
    Config#{<<"ssl">> => NSSLOpts};
convert_certs(Config) ->
    Config.

serialize_error({not_found, {authenticator, ID}}) ->
    {404, #{code => <<"NOT_FOUND">>,
            message => list_to_binary(
                io_lib:format("Authenticator '~s' does not exist", [ID])
            )}};

serialize_error({not_found, {listener, ID}}) ->
    {404, #{code => <<"NOT_FOUND">>,
            message => list_to_binary(
                io_lib:format("Listener '~s' does not exist", [ID])
            )}};

serialize_error({not_found, {chain, ?GLOBAL}}) ->
    {500, #{code => <<"INTERNAL_SERVER_ERROR">>,
            message => <<"Authentication status is abnormal">>}};

serialize_error({not_found, {chain, Name}}) ->
    {400, #{code => <<"BAD_REQUEST">>,
            message => list_to_binary(
                io_lib:format("No authentication has been create for listener '~s'", [Name])
            )}};

serialize_error({already_exists, {authenticator, ID}}) ->
    {409, #{code => <<"ALREADY_EXISTS">>,
            message => list_to_binary(
                io_lib:format("Authenticator '~s' already exist", [ID])
            )}};

serialize_error(no_available_provider) ->
    {400, #{code => <<"BAD_REQUEST">>,
            message => <<"Unsupported authentication type">>}};

serialize_error(change_of_authentication_type_is_not_allowed) ->
    {400, #{code => <<"BAD_REQUEST">>,
            message => <<"Change of authentication type is not allowed">>}};

serialize_error(unsupported_operation) ->
    {400, #{code => <<"BAD_REQUEST">>,
            message => <<"Operation not supported in this authentication type">>}};

serialize_error({save_cert_to_file, invalid_certificate}) ->
    {400, #{code => <<"BAD_REQUEST">>,
            message => <<"Invalid certificate">>}};

serialize_error({save_cert_to_file, {_, Reason}}) ->
    {500, #{code => <<"INTERNAL_SERVER_ERROR">>,
            message => list_to_binary(
                io_lib:format("Cannot save certificate to file due to '~p'", [Reason])
            )}};

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
            message => list_to_binary(io_lib:format("~p", [Reason]))}}.

parse_position(<<"top">>) ->
    {ok, top};
parse_position(<<"bottom">>) ->
    {ok, bottom};
parse_position(<<"before:", Before/binary>>) ->
    {ok, {before, Before}};
parse_position(_) ->
    {error, {invalid_parameter, position}}.

to_list(M) when is_map(M) ->
    [M];
to_list(L) when is_list(L) ->
    L.

to_atom(B) when is_binary(B) ->
    binary_to_atom(B);
to_atom(A) when is_atom(A) ->
    A.