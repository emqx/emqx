%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_api_schema).

-export([definitions/0]).

definitions() ->
    RetruenedSources = #{
        allOf => [ #{type => object,
                     properties => #{
                        annotations => #{
                            type => object,
                            required => [status],
                            properties => #{
                                id => #{
                                    type => string
                                },
                                status => #{
                                    type => string,
                                    example => <<"healthy">>
                                }
                            }
                        }
                     }
                   }
                 , minirest:ref(<<"sources">>)
                 ]
    },
    Sources = #{
        oneOf => [ minirest:ref(<<"http">>)
                 , minirest:ref(<<"mongo_single">>)
                 , minirest:ref(<<"mongo_rs">>)
                 , minirest:ref(<<"mongo_sharded">>)
                 , minirest:ref(<<"mysql">>)
                 , minirest:ref(<<"pgsql">>)
                 , minirest:ref(<<"redis_single">>)
                 , minirest:ref(<<"redis_sentinel">>)
                 , minirest:ref(<<"redis_cluster">>)
                 , minirest:ref(<<"file">>)
                 ]
    },
    SSL = #{
      type => object,
      required => [enable],
      properties => #{
         enable => #{type => boolean, example => true},
         cacertfile => #{type => string},
         keyfile => #{type => string},
         certfile => #{type => string},
         verify => #{type => boolean, example => false}
      }
    },
    HTTP = #{
        type => object,
        required => [ type
                    , enable
                    , method
                    , headers
                    , request_timeout
                    , connect_timeout
                    , max_retries
                    , retry_interval
                    , pool_type
                    , pool_size
                    , enable_pipelining
                    , ssl
                    ],
        properties => #{
            type => #{
                type => string,
                enum => [<<"http">>],
                example => <<"http">>
            },
            enable => #{
                type => boolean,
                example => true
            },
            url => #{
                type => string,
                example => <<"https://emqx.com">>
            },
            method => #{
                type => string,
                enum => [<<"get">>, <<"post">>, <<"put">>],
                example => <<"get">>
            },
            headers => #{type => object},
            body => #{type => object},
            connect_timeout => #{type => integer},
            max_retries => #{type => integer},
            retry_interval => #{type => integer},
            pool_type => #{
                type => string,
                enum => [<<"random">>, <<"hash">>],
                example => <<"random">>
            },
            pool_size => #{type => integer},
            enable_pipelining => #{type => boolean},
            ssl => minirest:ref(<<"ssl">>)
        }
    },
    MongoSingle= #{
        type => object,
        required => [ type
                    , enable
                    , collection
                    , find
                    , mongo_type
                    , server
                    , pool_size
                    , username
                    , password
                    , auth_source
                    , database
                    , topology
                    , ssl
                    ],
        properties => #{
            type => #{
                type => string,
                enum => [<<"mongo">>],
                example => <<"mongo">>
            },
            enable => #{
                type => boolean,
                example => true
            },
            collection => #{type => string},
            find => #{type => object},
            mongo_type => #{type => string,
                            enum => [<<"single">>],
                            example => <<"single">>},
            server => #{type => string, example => <<"127.0.0.1:27017">>},
            pool_size => #{type => integer},
            username => #{type => string},
            password => #{type => string},
            auth_source => #{type => string},
            database => #{type => string},
            topology => #{type => object,
                          properties => #{
                            pool_size => #{type => integer},
                            max_overflow => #{type => integer},
                            overflow_ttl => #{type => integer},
                            overflow_check_period => #{type => integer},
                            local_threshold_ms => #{type => integer},
                            connect_timeout_ms => #{type => integer},
                            socket_timeout_ms => #{type => integer},
                            server_selection_timeout_ms => #{type => integer},
                            wait_queue_timeout_ms => #{type => integer},
                            heartbeat_frequency_ms => #{type => integer},
                            min_heartbeat_frequency_ms => #{type => integer}
                          }
                         },
            ssl => minirest:ref(<<"ssl">>)
        }
    },
    MongoRs= #{
        type => object,
        required => [ type
                    , enable
                    , collection
                    , find
                    , mongo_type
                    , servers
                    , replica_set_name
                    , pool_size
                    , username
                    , password
                    , auth_source
                    , database
                    , topology
                    , ssl
                    ],
        properties => #{
            type => #{
                type => string,
                enum => [<<"mongo">>],
                example => <<"mongo">>
            },
            enable => #{
                type => boolean,
                example => true
            },
            collection => #{type => string},
            find => #{type => object},
            mongo_type => #{type => string,
                            enum => [<<"rs">>],
                            example => <<"rs">>},
            servers => #{type => array,
                         items => #{type => string,example => <<"127.0.0.1:27017">>}},
            replica_set_name => #{type => string},
            pool_size => #{type => integer},
            username => #{type => string},
            password => #{type => string},
            auth_source => #{type => string},
            database => #{type => string},
            topology => #{type => object,
                          properties => #{
                            pool_size => #{type => integer},
                            max_overflow => #{type => integer},
                            overflow_ttl => #{type => integer},
                            overflow_check_period => #{type => integer},
                            local_threshold_ms => #{type => integer},
                            connect_timeout_ms => #{type => integer},
                            socket_timeout_ms => #{type => integer},
                            server_selection_timeout_ms => #{type => integer},
                            wait_queue_timeout_ms => #{type => integer},
                            heartbeat_frequency_ms => #{type => integer},
                            min_heartbeat_frequency_ms => #{type => integer}
                          }
                         },
            ssl => minirest:ref(<<"ssl">>)
        }
    },
    MongoSharded = #{
        type => object,
        required => [ type
                    , enable
                    , collection
                    , find
                    , mongo_type
                    , servers
                    , pool_size
                    , username
                    , password
                    , auth_source
                    , database
                    , topology
                    , ssl
                    ],
        properties => #{
            type => #{
                type => string,
                enum => [<<"mongo">>],
                example => <<"mongo">>
            },
            enable => #{
                type => boolean,
                example => true
            },
            collection => #{type => string},
            find => #{type => object},
            mongo_type => #{type => string,
                            enum => [<<"sharded">>],
                            example => <<"sharded">>},
            servers => #{type => array,
                         items => #{type => string,example => <<"127.0.0.1:27017">>}},
            pool_size => #{type => integer},
            username => #{type => string},
            password => #{type => string},
            auth_source => #{type => string},
            database => #{type => string},
            topology => #{type => object,
                          properties => #{
                            pool_size => #{type => integer},
                            max_overflow => #{type => integer},
                            overflow_ttl => #{type => integer},
                            overflow_check_period => #{type => integer},
                            local_threshold_ms => #{type => integer},
                            connect_timeout_ms => #{type => integer},
                            socket_timeout_ms => #{type => integer},
                            server_selection_timeout_ms => #{type => integer},
                            wait_queue_timeout_ms => #{type => integer},
                            heartbeat_frequency_ms => #{type => integer},
                            min_heartbeat_frequency_ms => #{type => integer}
                          }
                         },
            ssl => minirest:ref(<<"ssl">>)
        }
    },
    Mysql = #{
        type => object,
        required => [ type
                    , enable
                    , sql
                    , server
                    , database
                    , pool_size
                    , username
                    , password
                    , auto_reconnect
                    , ssl
                    ],
        properties => #{
            type => #{
                type => string,
                enum => [<<"mysql">>],
                example => <<"mysql">>
            },
            enable => #{
                type => boolean,
                example => true
            },
            sql => #{type => string},
            server => #{type => string,
                        example => <<"127.0.0.1:3306">>
                       },
            database => #{type => string},
            pool_size => #{type => integer},
            username => #{type => string},
            password => #{type => string},
            auto_reconnect => #{type => boolean,
                                example => true
                               },
            ssl => minirest:ref(<<"ssl">>)
        }
    },
    Pgsql = #{
        type => object,
        required => [ type
                    , enable
                    , sql
                    , server
                    , database
                    , pool_size
                    , username
                    , password
                    , auto_reconnect
                    , ssl
                    ],
        properties => #{
            type => #{
                type => string,
                enum => [<<"pgsql">>],
                example => <<"pgsql">>
            },
            enable => #{
                type => boolean,
                example => true
            },
            sql => #{type => string},
            server => #{type => string,
                        example => <<"127.0.0.1:5432">>
                       },
            database => #{type => string},
            pool_size => #{type => integer},
            username => #{type => string},
            password => #{type => string},
            auto_reconnect => #{type => boolean,
                                example => true
                               },
            ssl => minirest:ref(<<"ssl">>)
        }
    },
    RedisSingle = #{
        type => object,
        required => [ type
                    , enable
                    , cmd
                    , server
                    , redis_type
                    , pool_size
                    , auto_reconnect
                    , ssl
                    ],
        properties => #{
            type => #{
                type => string,
                enum => [<<"redis">>],
                example => <<"redis">>
            },
            enable => #{
                type => boolean,
                example => true
            },
            cmd => #{
                type => string,
                example => <<"HGETALL mqtt_authz">>
            },
            server => #{type => string, example => <<"127.0.0.1:3306">>},
            redis_type => #{type => string,
                            enum => [<<"single">>],
                            example => <<"single">>},
            pool_size => #{type => integer},
            auto_reconnect => #{type => boolean, example => true},
            password => #{type => string},
            database => #{type => integer},
            ssl => minirest:ref(<<"ssl">>)
        }
    },
    RedisSentinel= #{
        type => object,
        required => [ type
                    , enable
                    , cmd
                    , servers
                    , redis_type
                    , sentinel
                    , pool_size
                    , auto_reconnect
                    , ssl
                    ],
        properties => #{
            type => #{
                type => string,
                enum => [<<"redis">>],
                example => <<"redis">>
            },
            enable => #{
                type => boolean,
                example => true
            },
            cmd => #{
                type => string,
                example => <<"HGETALL mqtt_authz">>
            },
            servers => #{type => array,
                         items => #{type => string,example => <<"127.0.0.1:3306">>}},
            redis_type => #{type => string,
                            enum => [<<"sentinel">>],
                            example => <<"sentinel">>},
            sentinel => #{type => string},
            pool_size => #{type => integer},
            auto_reconnect => #{type => boolean, example => true},
            password => #{type => string},
            database => #{type => integer},
            ssl => minirest:ref(<<"ssl">>)
        }
    },
    RedisCluster= #{
        type => object,
        required => [ type
                    , enable
                    , cmd
                    , servers
                    , redis_type
                    , pool_size
                    , auto_reconnect
                    , ssl],
        properties => #{
            type => #{
                type => string,
                enum => [<<"redis">>],
                example => <<"redis">>
            },
            enable => #{
                type => boolean,
                example => true
            },
            cmd => #{
                type => string,
                example => <<"HGETALL mqtt_authz">>
            },
            servers => #{type => array,
                         items => #{type => string, example => <<"127.0.0.1:3306">>}},
            redis_type => #{type => string,
                            enum => [<<"cluster">>],
                            example => <<"cluster">>},
            pool_size => #{type => integer},
            auto_reconnect => #{type => boolean, example => true},
            password => #{type => string},
            database => #{type => integer},
            ssl => minirest:ref(<<"ssl">>)
        }
    },
    File = #{
        type => object,
        required => [type, enable, rules],
        properties => #{
            type => #{
                type => string,
                enum => [<<"redis">>],
                example => <<"redis">>
            },
            enable => #{
                type => boolean,
                example => true
            },
            rules => #{
                type => array,
                items => #{
                  type => string,
                  example => <<"{allow,{username,\"^dashboard?\"},subscribe,[\"$SYS/#\"]}.\n{allow,{ipaddr,\"127.0.0.1\"},all,[\"$SYS/#\",\"#\"]}.">>
                }
            },
            path => #{
                type => string,
                example => <<"/path/to/authorizaiton_rules.conf">>
            }
        }
    },
    [ #{<<"returned_sources">> => RetruenedSources}
    , #{<<"sources">> => Sources}
    , #{<<"ssl">> => SSL}
    , #{<<"http">> => HTTP}
    , #{<<"mongo_single">> => MongoSingle}
    , #{<<"mongo_rs">> => MongoRs}
    , #{<<"mongo_sharded">> => MongoSharded}
    , #{<<"mysql">> => Mysql}
    , #{<<"pgsql">> => Pgsql}
    , #{<<"redis_single">> => RedisSingle}
    , #{<<"redis_sentinel">> => RedisSentinel}
    , #{<<"redis_cluster">> => RedisCluster}
    , #{<<"file">> => File}
    ].
