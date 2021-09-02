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

-import(emqx_mgmt_util, [ schema/1
                        ]).

%% minirest behaviour callbacks
-export([api_spec/0]).

%% http handlers
-export([ gateway/2
        , gateway_insta/2
        , gateway_insta_stats/2
        ]).

-define(EXAMPLE_GATEWAY_LIST,
        [ #{ name => <<"lwm2m">>
           , status => <<"running">>
           , started_at => <<"2021-08-19T11:45:56.006373+08:00">>
           , max_connection => 1024000
           , current_connection => 1000
           , listeners => [
                #{name => <<"lw-udp-1">>, status => <<"activing">>},
                #{name => <<"lw-udp-2">>, status => <<"inactived">>}
             ]
           }
        ]).

%% XXX: This is whole confs for all type gateways. It is used to fill the
%% default configurations and generate the swagger-schema
%%
%% NOTE: It is a temporary measure to generate swagger-schema
-define(COAP_GATEWAY_CONFS,
#{<<"authentication">> =>
      #{<<"mechanism">> => <<"password-based">>,
        <<"name">> => <<"authenticator1">>,
        <<"server_type">> => <<"built-in-database">>,
        <<"user_id_type">> => <<"clientid">>},
  <<"enable">> => true,
  <<"enable_stats">> => true,<<"heartbeat">> => <<"30s">>,
  <<"idle_timeout">> => <<"30s">>,
  <<"listeners">> =>
      #{<<"udp">> => #{<<"default">> => #{<<"bind">> => 5683}}},
  <<"mountpoint">> => <<>>,<<"notify_type">> => <<"qos">>,
  <<"publish_qos">> => <<"qos1">>,
  <<"subscribe_qos">> => <<"qos0">>}
).

-define(EXPROTO_GATEWAY_CONFS,
#{<<"enable">> => true,
  <<"enable_stats">> => true,
  <<"handler">> =>
      #{<<"address">> => <<"http://127.0.0.1:9001">>},
  <<"idle_timeout">> => <<"30s">>,
  <<"listeners">> =>
      #{<<"tcp">> =>
            #{<<"default">> =>
                  #{<<"acceptors">> => 8,<<"bind">> => 7993,
                    <<"max_conn_rate">> => 1000,
                    <<"max_connections">> => 10240}}},
  <<"mountpoint">> => <<>>,
  <<"server">> => #{<<"bind">> => 9100}}
).

-define(LWM2M_GATEWAY_CONFS,
#{<<"auto_observe">> => false,
  <<"enable">> => true,
  <<"enable_stats">> => true,
  <<"idle_timeout">> => <<"30s">>,
  <<"lifetime_max">> => <<"86400s">>,
  <<"lifetime_min">> => <<"1s">>,
  <<"listeners">> =>
      #{<<"udp">> => #{<<"default">> => #{<<"bind">> => 5783}}},
  <<"mountpoint">> => <<"lwm2m/%e/">>,
  <<"qmode_time_windonw">> => 22,
  <<"translators">> =>
      #{<<"command">> => <<"dn/#">>,<<"notify">> => <<"up/notify">>,
        <<"register">> => <<"up/resp">>,
        <<"response">> => <<"up/resp">>,
        <<"update">> => <<"up/resp">>},
  <<"update_msg_publish_condition">> =>
      <<"contains_object_list">>,
  <<"xml_dir">> => <<"etc/lwm2m_xml">>}
).

-define(MQTTSN_GATEWAY_CONFS,
#{<<"broadcast">> => true,
  <<"clientinfo_override">> =>
      #{<<"password">> => <<"abc">>,
        <<"username">> => <<"mqtt_sn_user">>},
  <<"enable">> => true,
  <<"enable_qos3">> => true,<<"enable_stats">> => true,
  <<"gateway_id">> => 1,<<"idle_timeout">> => <<"30s">>,
  <<"listeners">> =>
      #{<<"udp">> =>
            #{<<"default">> =>
                  #{<<"bind">> => 1884,<<"max_conn_rate">> => 1000,
                    <<"max_connections">> => 10240000}}},
  <<"mountpoint">> => <<>>,
  <<"predefined">> =>
      [#{<<"id">> => 1,
         <<"topic">> => <<"/predefined/topic/name/hello">>},
       #{<<"id">> => 2,
         <<"topic">> => <<"/predefined/topic/name/nice">>}]}
).

-define(STOMP_GATEWAY_CONFS,
#{<<"authentication">> =>
      #{<<"mechanism">> => <<"password-based">>,
        <<"name">> => <<"authenticator1">>,
        <<"server_type">> => <<"built-in-database">>,
        <<"user_id_type">> => <<"clientid">>},
  <<"clientinfo_override">> =>
      #{<<"password">> => <<"${Packet.headers.passcode}">>,
        <<"username">> => <<"${Packet.headers.login}">>},
  <<"enable">> => true,
  <<"enable_stats">> => true,
  <<"frame">> =>
      #{<<"max_body_length">> => 8192,<<"max_headers">> => 10,
        <<"max_headers_length">> => 1024},
  <<"idle_timeout">> => <<"30s">>,
  <<"listeners">> =>
      #{<<"tcp">> =>
            #{<<"default">> =>
                  #{<<"acceptors">> => 16,<<"active_n">> => 100,
                    <<"bind">> => 61613,<<"max_conn_rate">> => 1000,
                    <<"max_connections">> => 1024000}}},
  <<"mountpoint">> => <<>>}
).

%% --- END

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
                            message => <<"The gateway not found">>
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
            <<"200">> => schema(schema_for_gateway_conf())
        }
      },
      put => #{
        description => <<"Update the gateway configurations/status">>,
        parameters => [UriNameParamDef],
        requestBody => schema(schema_for_gateway_conf()),
        responses => #{
            <<"404">> => NameNotFoundRespDef,
            <<"200">> => #{description => <<"Changed">>}
        }
      }
     };

metadata(gateway_insta_stats) ->
    UriNameParamDef = #{name => name,
                        in => path,
                        schema => #{type => string},
                        required => true
                       },
    #{get => #{
        description => <<"Get gateway Statistic">>,
        parameters => [UriNameParamDef],
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
     [ emqx_mgmt_api_configs:gen_schema(?STOMP_GATEWAY_CONFS)
     , emqx_mgmt_api_configs:gen_schema(?MQTTSN_GATEWAY_CONFS)
     , emqx_mgmt_api_configs:gen_schema(?COAP_GATEWAY_CONFS)
     , emqx_mgmt_api_configs:gen_schema(?LWM2M_GATEWAY_CONFS)
     , emqx_mgmt_api_configs:gen_schema(?EXPROTO_GATEWAY_CONFS)
     ]}.

schema_for_gateway_stats() ->
    #{type => object,
      properties => #{
        a_key => #{type => string}
     }}.

%%--------------------------------------------------------------------
%% http handlers

gateway(get, Request) ->
    Params = maps:get(query_string, Request, #{}),
    Status = case maps:get(<<"status">>, Params, undefined) of
                 undefined -> all;
                 S0 -> binary_to_existing_atom(S0, utf8)
             end,
    {200, emqx_gateway_intr:gateways(Status)}.

gateway_insta(delete, #{bindings := #{name := Name0}}) ->
    Name = binary_to_existing_atom(Name0),
    case emqx_gateway:unload(Name) of
        ok ->
            {200, ok};
        {error, not_found} ->
            {404, <<"Not Found">>}
    end;
gateway_insta(get, #{bindings := #{name := Name0}}) ->
    Name = binary_to_existing_atom(Name0),
    case emqx_gateway:lookup(Name) of
        #{config := _Config} ->
            %% FIXME: Got the parsed config, but we should return rawconfig to
            %% frontend
            RawConf = emqx_config:fill_defaults(
                        emqx_config:get_root_raw([<<"gateway">>])
                       ),
            {200, emqx_map_lib:deep_get([<<"gateway">>, Name0], RawConf)};
        undefined ->
            {404, <<"Not Found">>}
    end;
gateway_insta(put, #{body := RawConfsIn,
                     bindings := #{name := Name}
                    }) ->
    %% FIXME: Cluster Consistence ??
    case emqx_gateway:update_rawconf(Name, RawConfsIn) of
        ok ->
            {200, <<"Changed">>};
        {error, not_found} ->
            {404, <<"Not Found">>};
        {error, Reason} ->
            {500, emqx_gateway_utils:stringfy(Reason)}
    end.

gateway_insta_stats(get, _Req) ->
    {401, <<"Implement it later (maybe 5.1)">>}.
