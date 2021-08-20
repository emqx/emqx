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
%%
-module(emqx_gateway_api).

-behaviour(minirest_api).

-compile(nowarn_unused_function).

%% minirest behaviour callbacks
-export([api_spec/0]).

%% http handlers
-export([ gateway/2
        , gateway_insta/2
        ]).

-define(EXAMPLE_GATEWAY_LIST,
        [ #{ name => <<"lwm2m">>
           , status => <<"running">>
           , started_at => <<"2021-08-19T11:45:56.006373+08:00">>
           , max_connection => 1024000
           , current_connection => 12
           , listeners => [
                #{name => <<"lw-udp-1">>, status => <<"activing">>},
                #{name => <<"lw-udp-2">>, status => <<"inactived">>}
             ]
           }
        ]).

-define(EXAMPLE_STOMP_GATEWAY_CONF, #{
            frame => #{
                max_headers => 10,
                max_headers_length => 1024,
                max_body_length => 8192
            },
            listener => #{
                tcp => #{<<"default-stomp-listener">> => #{
                            bind => <<"61613">>
                       }}
            }
        }).

-define(EXAMPLE_MQTTSN_GATEWAY_CONF, #{
        }).

-define(EXAMPLE_GATEWAY_STATS, #{
            max_connection => 10240000,
            current_connection => 1000,
            messages_in => 100.24,
            messages_out => 32.5
        }).

%%--------------------------------------------------------------------
%% minirest behaviour callbacks
%%--------------------------------------------------------------------

api_spec() ->
    {apis(), schemas()}.

apis() ->
    [ {"/gateway", metadata(gateway), gateway}  
    , {"/gateway/:name", metadata(gateway_insta), gateway_insta}
    , {"/gateway/:name/stats", metadata(gateway_insta_stats), gateway_insta_stats}
    ].

metadata(gateway) ->
    #{get => #{
        description => <<"Get gateway list">>,
        parameters => [
            #{name => status,
              in => query,
              schema => #{type => string},
              required => false
             }
        ],
        responses => #{
            <<"200">> => #{
                description => <<"OK">>,
                content => #{
                    'application/json' => #{
                        schema => minirest:ref(<<"gateway_overrview">>),
                        examples => #{
                            simple => #{
                                summary => <<"Gateway List Example">>,
                                value => emqx_json:encode(?EXAMPLE_GATEWAY_LIST)
                            }
                        }
                    }
                }         
            }
        }
     }};

metadata(gateway_insta) ->
    UriNameParamDef = #{name => name,
                        in => path,
                        schema => #{type => string},
                        required => true
                       },
    NameNotFoundRespDef =
        #{description => <<"Not Found">>,
          content => #{
            'application/json' => #{
                schema => minirest:ref(<<"error">>),
                examples => #{
                    simple => #{
                        summary => <<"Not Found">>,
                        value => #{
                            code => <<"NOT_FOUND">>,
                            message => <<"gateway xxx not found">>
                        }
                    }
                }
            }
         }},
    #{delete => #{
        description => <<"Delete/Unload the gateway">>,
        parameters => [UriNameParamDef],
        responses => #{
            <<"404">> => NameNotFoundRespDef,
            <<"204">> => #{description => <<"No Content">>}
        }
      },
      get => #{
        description => <<"Get the gateway configurations">>,
        parameters => [UriNameParamDef],
        responses => #{
            <<"404">> => NameNotFoundRespDef,
            <<"200">> => #{
                description => <<"OK">>,
                content => #{
                    'application/json' => #{
                        schema => minirest:ref(<<"gateway_conf">>),
                        examples => #{
                            simple1 => #{
                                summary => <<"Stomp Gateway">>,
                                value => emqx_json:encode(?EXAMPLE_STOMP_GATEWAY_CONF)
                            },
                            simple2 => #{
                                summary => <<"MQTT-SN Gateway">>,
                                value => emqx_json:encode(?EXAMPLE_MQTTSN_GATEWAY_CONF)
                            }
                        }
                    }
                }
            }
        }
      },
      put => #{
        description => <<"Update the gateway configurations/status">>,
        parameters => [UriNameParamDef],
        requestBody => #{
            content => #{
                'application/json' => #{
                    schema => minirest:ref(<<"gateway_conf">>),
                    examples => #{
                        simple1 => #{
                            summary => <<"Stom Gateway">>,
                            value => emqx_json:encode(?EXAMPLE_STOMP_GATEWAY_CONF)
                        },
                        simple2 => #{
                            summary => <<"MQTT-SN Gateway">>,
                            value => emqx_json:encode(?EXAMPLE_MQTTSN_GATEWAY_CONF)
                        }
                    }
                }
            }
        },
        responses => #{
            <<"404">> => NameNotFoundRespDef,
            <<"204">> => #{description => <<"Created">>}
        }
      }
     };

metadata(gateway_insta_stats) ->
    #{get => #{
        description => <<"Get gateway Statistic">>,
        responses => #{
            <<"200">> => #{
                description => <<"OK">>,
                content => #{
                    'application/json' => #{
                        schema => minirest:ref(<<"gateway_stats">>),
                        examples => #{
                            simple => #{
                                summary => <<"Gateway Statistic">>,
                                value => emqx_json:encode(?EXAMPLE_GATEWAY_STATS)
                            }
                        }
                    }
                }         
            }
        }
     }}.

schemas() ->
    [ #{<<"gateway_overrview">> => schema_for_gateway_overrview()}
    , #{<<"gateway_conf">> => schema_for_gateway_conf()}
    , #{<<"gateway_stats">> => schema_for_gateway_stats()}
    ].

schema_for_gateway_overrview() ->
    #{type => array,
      items => #{
        type => object,
        properties => #{
            name => #{
                type => string,
                example => <<"lwm2m">>
            },
            status => #{
                type => string,
                enum => [<<"running">>, <<"stopped">>, <<"unloaded">>],
                example => <<"running">>
            },
            started_at => #{
                type => string,
                example => <<"2021-08-19T11:45:56.006373+08:00">>
            },
            max_connection => #{
                type => integer,
                example => 1024000
            },
            current_connection => #{
                type => integer,
                example => 1000
            },
            listeners => #{
                type => array,
                items => #{
                    type => object,
                    properties => #{
                        name => #{
                            type => string,
                            example => <<"lw-udp">>
                        },
                        status => #{
                            type => string,
                            enum => [<<"activing">>, <<"inactived">>]
                        }
                    }
                }
            }
        }
      }
     }.

schema_for_gateway_conf() ->
   #{oneOf =>
     [ schema_for_gateway_conf_stomp()
     , schema_for_gateway_conf_mqttsn()
     , schema_for_gateway_conf_coap()
     , schema_for_gateway_conf_lwm2m()
     , schema_for_gateway_conf_exproto()
     ]}.

schema_for_clientinfo_override() ->
    #{type => object,
      properties => #{
        clientid => #{type => string},
        username => #{type => string},
        password => #{type => string}
     }}.

schema_for_authenticator() ->
    %% TODO.
    #{type => object, properties => #{
        a_key => #{type => string}
     }}.

schema_for_tcp_listener() ->
    %% TODO.
    #{type => object, properties => #{
        a_key => #{type => string}
     }}.

schema_for_udp_listener() ->
    %% TODO.
    #{type => object, properties => #{
        a_key => #{type => string}
     }}.

%% It should be generated by _schema.erl module
%% and emqx_gateway.conf
schema_for_gateway_conf_stomp() ->
    #{type => object,
      properties => #{
        frame => #{
            type => object,
            properties => #{
                max_headers => #{type => integer},
                max_headers_length => #{type => integer},
                max_body_length => #{type => integer}
            }
        },
        clientinfo_override => schema_for_clientinfo_override(),
        authenticator => schema_for_authenticator(),
        listener => schema_for_tcp_listener()
      }
     }.

schema_for_gateway_conf_mqttsn() ->
    #{type => object,
      properties => #{
        gateway_id => #{type => integer},
        broadcast => #{type => boolean},
        enable_stats => #{type => boolean},
        enable_qos3 => #{type => boolean},
        idle_timeout => #{type => integer},
        predefined => #{
            type => array,
            items => #{
                type => object,
                properties => #{
                    id => #{type => integer},
                    topic => #{type => string}
                }
            }
        },
        clientinfo_override => schema_for_clientinfo_override(),
        authenticator => schema_for_authenticator(),
        listener => schema_for_udp_listener()
     }}.


schema_for_gateway_conf_coap() ->
    #{type => object,
      properties => #{
        clientinfo_override => schema_for_clientinfo_override(),
        authenticator => schema_for_authenticator(),
        listener => schema_for_udp_listener()
     }}.

schema_for_gateway_conf_lwm2m() ->
    #{type => object,
      properties => #{
        clientinfo_override => schema_for_clientinfo_override(),
        authenticator => schema_for_authenticator(),
        listener => schema_for_udp_listener()
     }}.

schema_for_gateway_conf_exproto() ->
    #{type => object,
      properties => #{
        clientinfo_override => schema_for_clientinfo_override(),
        authenticator => schema_for_authenticator(),
        listener => #{oneOf => [schema_for_tcp_listener(),
                                schema_for_udp_listener()
                               ]
                     }
     }}.

schema_for_gateway_stats() ->
    #{type => object,
      properties => #{
        a_key => #{type => string}
     }}.

%%--------------------------------------------------------------------
%% http handlers

gateway(get, _Request) ->
    {200, ok}.

gateway_insta(delete, _Request) ->
    {200, ok};
gateway_insta(get, _Request) ->
    {200, ok};
gateway_insta(put, _Request) ->
    {200, ok}.

gateway_insta_stats(get, _Req) ->
    {401, <<"Implement it later (maybe 5.1)">>}.

