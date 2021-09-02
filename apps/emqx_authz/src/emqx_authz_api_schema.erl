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
        oneOf => [ minirest:ref(<<"redis">>)
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
    Redis = #{
        type => object,
        required => [type, enable, config, cmd],
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
            config => #{
                oneOf => [ #{type => object,
                             required => [server, redis_type, pool_size, auto_reconnect],
                             properties => #{
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
                            }
                         , #{type => object,
                             required => [servers, redis_type, sentinel, pool_size, auto_reconnect],
                             properties => #{
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
                            }
                         , #{type => object,
                             required => [servers, redis_type, pool_size, auto_reconnect],
                             properties => #{
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
                            }
                         ],
                type => object
            },
            cmd => #{
                type => string,
                example => <<"HGETALL mqtt_authz">>
            }
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
                  example => <<"{allow,{username,\"^dashboard?\"},subscribe,[\"$SYS/#\"]}.">>
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
    , #{<<"redis">> => Redis}
    , #{<<"file">> => File}
    ].
