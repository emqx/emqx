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

-module(emqx_authz_api_sources).

-behavior(minirest_api).

-include("emqx_authz.hrl").
-include_lib("emqx/include/logger.hrl").

-define(EXAMPLE_REDIS,
        #{type=> redis,
          config => #{server => <<"127.0.0.1:3306">>,
                      redis_type => single,
                      pool_size => 1,
                      auto_reconnect => true
                     },
          cmd => <<"HGETALL mqtt_authz">>}).
-define(EXAMPLE_RETURNED_REDIS,
        maps:put(annotations, #{status => healthy}, ?EXAMPLE_REDIS)
        ).

-define(EXAMPLE_RETURNED,
        #{sources => [?EXAMPLE_RETURNED_REDIS
                     ]
        }).

-export([ api_spec/0
        , sources/2
        , source/2
        , move_source/2
        ]).

api_spec() ->
    {[ sources_api()
     , source_api()
     , move_source_api()
     ], definitions()}.

definitions() -> emqx_authz_api_schema:definitions().

sources_api() ->
    Metadata = #{
        get => #{
            description => "List authorization sources",
            responses => #{
                <<"200">> => #{
                    description => <<"OK">>,
                    content => #{
                        'application/json' => #{
                            schema => #{
                                type => object,
                                required => [sources],
                                properties => #{sources => #{
                                                  type => array,
                                                  items => minirest:ref(<<"returned_sources">>)
                                                 }
                                               }
                            },
                            examples => #{
                                sources => #{
                                    summary => <<"Sources">>,
                                    value => jsx:encode(?EXAMPLE_RETURNED)
                                }
                            }
                         }
                    }
                }
            }
        },
        post => #{
            description => "Add new source",
            requestBody => #{
                content => #{
                    'application/json' => #{
                        schema => minirest:ref(<<"sources">>),
                        examples => #{
                            redis => #{
                                summary => <<"Redis">>,
                                value => jsx:encode(?EXAMPLE_REDIS)
                            }
                       }
                    }
                }
            },
            responses => #{
                <<"204">> => #{description => <<"Created">>},
                <<"400">> => emqx_mgmt_util:bad_request()
            }
        },
        put => #{
            description => "Update all sources",
            requestBody => #{
                content => #{
                    'application/json' => #{
                        schema => #{
                            type => array,
                            items => minirest:ref(<<"returned_sources">>)
                        },
                        examples => #{
                            redis => #{
                                summary => <<"Redis">>,
                                value => jsx:encode([?EXAMPLE_REDIS])
                            }
                        }
                    }
                }
            },
            responses => #{
                <<"204">> => #{description => <<"Created">>},
                <<"400">> => emqx_mgmt_util:bad_request()
            }
        }
    },
    {"/authorization/sources", Metadata, sources}.

source_api() ->
    Metadata = #{
        get => #{
            description => "List authorization sources",
            parameters => [
                #{
                    name => type,
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
                            schema => minirest:ref(<<"returned_sources">>),
                            examples => #{
                                sources => #{
                                    summary => <<"Sources">>,
                                    value => jsx:encode(?EXAMPLE_RETURNED_REDIS)
                                }
                            }
                         }
                    }
                },
                <<"404">> => emqx_mgmt_util:bad_request(<<"Not Found">>)
            }
        },
        put => #{
            description => "Update source",
            parameters => [
                #{
                    name => type,
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
                        schema => minirest:ref(<<"sources">>),
                        examples => #{
                            redis => #{
                                summary => <<"Redis">>,
                                value => jsx:encode(?EXAMPLE_REDIS)
                            }
                       }
                    }
                }
            },
            responses => #{
                <<"204">> => #{description => <<"No Content">>},
                <<"404">> => emqx_mgmt_util:bad_request(<<"Not Found">>),
                <<"400">> => emqx_mgmt_util:bad_request()
            }
        },
        delete => #{
            description => "Delete source",
            parameters => [
                #{
                    name => type,
                    in => path,
                    schema => #{
                       type => string
                    },
                    required => true
                }
            ],
            responses => #{
                <<"204">> => #{description => <<"No Content">>},
                <<"400">> => emqx_mgmt_util:bad_request()
            }
        }
    },
    {"/authorization/sources/:type", Metadata, source}.

move_source_api() ->
    Metadata = #{
        post => #{
            description => "Change the order of sources",
            parameters => [
                #{
                    name => type,
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
                                    oneOf => [
                                        #{type => string,
                                          enum => [<<"top">>, <<"bottom">>]
                                        },
                                        #{type => object,
                                          required => ['after'],
                                          properties => #{
                                            'after' => #{
                                              type => string
                                             }
                                           }
                                        },
                                        #{type => object,
                                          required => ['before'],
                                          properties => #{
                                            'before' => #{
                                              type => string
                                             }
                                           }
                                        }
                                    ]
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
                <<"404">> => emqx_mgmt_util:bad_request(<<"Not Found">>),
                <<"400">> => emqx_mgmt_util:bad_request()
            }
        }
    },
    {"/authorization/sources/:type/move", Metadata, move_source}.

sources(get, _) ->
    Sources = lists:foldl(fun (#{type := _Type, enable := true, config := Config, annotations := #{id := Id}} = Source, AccIn) ->
                                  NSource0 = case maps:get(server, Config, undefined) of
                                                 undefined -> Source;
                                                 Server ->
                                                     Source#{config => Config#{server => emqx_connector_schema_lib:ip_port_to_string(Server)}}
                                             end,
                                  NSource1 = case maps:get(servers, Config, undefined) of
                                                 undefined -> NSource0;
                                                 Servers ->
                                                     NSource0#{config => Config#{servers => [emqx_connector_schema_lib:ip_port_to_string(Server) || Server <- Servers]}}
                                             end,
                                  NSource2 = case emqx_resource:health_check(Id) of
                                                 ok ->
                                                     NSource1#{annotations => #{status => healthy}};
                                                 _ ->
                                                     NSource1#{annotations => #{status => unhealthy}}
                                             end,
                                  lists:append(AccIn, [NSource2]);
                              (Source, AccIn) ->
                                  lists:append(AccIn, [Source#{annotations => #{status => healthy}}])
                        end, [], emqx_authz:lookup()),
    {200, #{sources => Sources}};
sources(post, #{body := Body}) ->
    case emqx_authz:update(head, [save_cert(Body)]) of
        {ok, _} -> {204};
        {error, Reason} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    messgae => atom_to_binary(Reason)}}
    end;
sources(put, #{body := Body}) ->
    case emqx_authz:update(replace, save_cert(Body)) of
        {ok, _} -> {204};
        {error, Reason} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    messgae => atom_to_binary(Reason)}}
    end.

source(get, #{bindings := #{type := Type}}) ->
    case emqx_authz:lookup(Type) of
        {error, Reason} -> {404, #{messgae => atom_to_binary(Reason)}};
        #{enable := false} = Source -> {200, Source};
        #{type := file} = Source -> {200, Source};
        #{config := Config, annotations := #{id := Id}} = Source ->
            NSource0 = case maps:get(server, Config, undefined) of
                           undefined -> Source;
                           Server ->
                               Source#{config => Config#{server => emqx_connector_schema_lib:ip_port_to_string(Server)}}
                       end,
            NSource1 = case maps:get(servers, Config, undefined) of
                           undefined -> NSource0;
                           Servers ->
                               NSource0#{config => Config#{servers => [emqx_connector_schema_lib:ip_port_to_string(Server) || Server <- Servers]}}
                       end,
            NSource2 = case emqx_resource:health_check(Id) of
                ok ->
                    NSource1#{annotations => #{status => healthy}};
                _ ->
                    NSource1#{annotations => #{status => unhealthy}}
            end,
            {200, NSource2}
    end;
source(put, #{bindings := #{type := Type}, body := Body}) ->

    case emqx_authz:update({replace_once, Type}, save_cert(Body)) of
        {ok, _} -> {204};
        {error, not_found_source} ->
            {404, #{code => <<"NOT_FOUND">>,
                    messgae => <<"source ", Type/binary, " not found">>}};
        {error, Reason} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    messgae => atom_to_binary(Reason)}}
    end;
source(delete, #{bindings := #{type := Type}}) ->
    case emqx_authz:update({delete_once, Type}, #{}) of
        {ok, _} -> {204};
        {error, Reason} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    messgae => atom_to_binary(Reason)}}
    end.
move_source(post, #{bindings := #{type := Type}, body := #{<<"position">> := Position}}) ->
    case emqx_authz:move(Type, Position) of
        {ok, _} -> {204};
        {error, not_found_source} ->
            {404, #{code => <<"NOT_FOUND">>,
                    messgae => <<"source ", Type/binary, " not found">>}};
        {error, Reason} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    messgae => atom_to_binary(Reason)}}
    end.

save_cert(#{<<"config">> := #{<<"ssl">> := #{<<"enable">> := true} = SSL} = Config} = Body) ->
    CertPath = filename:join([emqx:get_config([node, data_dir]), "certs"]),
    CaCert = case maps:is_key(<<"cacertfile">>, SSL) of
                 true ->
                     write_file(filename:join([CertPath, "cacert-" ++ emqx_rule_id:gen() ++".pem"]),
                                maps:get(<<"cacertfile">>, SSL));
                 false -> ""
             end,
    Cert = case maps:is_key(<<"certfile">>, SSL) of
                 true ->
                     write_file(filename:join([CertPath, "cert-" ++ emqx_rule_id:gen() ++".pem"]),
                                maps:get(<<"certfile">>, SSL));
                 false -> ""
             end,
    Key = case maps:is_key(<<"keyfile">>, SSL) of
                 true ->
                     write_file(filename:join([CertPath, "key-" ++ emqx_rule_id:gen() ++".pem"]),
                                maps:get(<<"keyfile">>, SSL));
                 false -> ""
             end,
    Body#{<<"config">> := Config#{<<"ssl">> => SSL#{<<"cacertfile">> => CaCert,
                                                    <<"certfile">> => Cert,
                                                    <<"keyfile">> => Key}
                           }
         };
save_cert(Body) -> Body.

write_file(Filename, Bytes) ->
    ok = filelib:ensure_dir(Filename),
    case file:write_file(Filename, Bytes) of
       ok -> Filename;
       {error, Reason} ->
           ?LOG(error, "Write File ~p Error: ~p", [Filename, Reason]),
           error(Reason)
    end.
