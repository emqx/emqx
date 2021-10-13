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
          enable => true,
          server => <<"127.0.0.1:3306">>,
          redis_type => single,
          pool_size => 1,
          auto_reconnect => true,
          cmd => <<"HGETALL mqtt_authz">>}).
-define(EXAMPLE_FILE,
        #{type=> file,
          enable => true,
          rules => <<"{allow,{username,\"^dashboard?\"},subscribe,[\"$SYS/#\"]}.\n{allow,{ipaddr,\"127.0.0.1\"},all,[\"$SYS/#\",\"#\"]}.">>
                   }).

-define(EXAMPLE_RETURNED,
        #{sources => [ ?EXAMPLE_REDIS
                     , ?EXAMPLE_FILE
                     ]
        }).

-export([ get_raw_sources/0
        , get_raw_source/1
        ]).

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
                                                  items => minirest:ref(<<"sources">>)
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
                            },
                            file => #{
                                summary => <<"File">>,
                                value => jsx:encode(?EXAMPLE_FILE)
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
                            items => minirest:ref(<<"sources">>)
                        },
                        examples => #{
                            redis => #{
                                summary => <<"Redis">>,
                                value => jsx:encode(?EXAMPLE_REDIS)
                            },
                            file => #{
                                summary => <<"File">>,
                                value => jsx:encode(?EXAMPLE_FILE)
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
                       type => string,
                        enum => [ <<"file">>
                                , <<"http">>
                                , <<"mongodb">>
                                , <<"mysql">>
                                , <<"postgresql">>
                                , <<"redis">>
                                , <<"built-in-database">>
                                ]
                    },
                    required => true
                }
            ],
            responses => #{
                <<"200">> => #{
                    description => <<"OK">>,
                    content => #{
                        'application/json' => #{
                            schema => minirest:ref(<<"sources">>),
                            examples => #{
                                redis => #{
                                    summary => <<"Redis">>,
                                    value => jsx:encode(?EXAMPLE_REDIS)
                                },
                                file => #{
                                    summary => <<"File">>,
                                    value => jsx:encode(?EXAMPLE_FILE)
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
                       type => string,
                        enum => [ <<"file">>
                                , <<"http">>
                                , <<"mongodb">>
                                , <<"mysql">>
                                , <<"postgresql">>
                                , <<"redis">>
                                , <<"built-in-database">>
                                ]
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
                            },
                            file => #{
                                summary => <<"File">>,
                                value => jsx:encode(?EXAMPLE_FILE)
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
                       type => string,
                        enum => [ <<"file">>
                                , <<"http">>
                                , <<"mongodb">>
                                , <<"mysql">>
                                , <<"postgresql">>
                                , <<"redis">>
                                , <<"built-in-database">>
                                ]
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
                        type => string,
                        enum => [ <<"file">>
                                , <<"http">>
                                , <<"mongodb">>
                                , <<"mysql">>
                                , <<"postgresql">>
                                , <<"redis">>
                                , <<"built-in-database">>
                                ]
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
    Sources = lists:foldl(fun (#{<<"type">> := <<"file">>, <<"enable">> := Enable, <<"path">> := Path}, AccIn) ->
                                  case file:read_file(Path) of
                                      {ok, Rules} ->
                                          lists:append(AccIn, [#{type => file,
                                                                 enable => Enable,
                                                                 rules => Rules
                                                                }]);
                                      {error, _} ->
                                          lists:append(AccIn, [#{type => file,
                                                                 enable => Enable,
                                                                 rules => <<"">>
                                                                }])
                                  end;
                              (Source, AccIn) ->
                                  lists:append(AccIn, [read_cert(Source)])
                          end, [], get_raw_sources()),
    {200, #{sources => Sources}};
sources(post, #{body := #{<<"type">> := <<"file">>, <<"rules">> := Rules}}) ->
    {ok, Filename} = write_file(filename:join([emqx:get_config([node, data_dir]), "acl.conf"]), Rules),
    update_config(?CMD_PREPEND, [#{<<"type">> => <<"file">>, <<"enable">> => true, <<"path">> => Filename}]);
sources(post, #{body := Body}) when is_map(Body) ->
    update_config(?CMD_PREPEND, [write_cert(Body)]);
sources(put, #{body := Body}) when is_list(Body) ->
    NBody = [ begin
                case Source of
                    #{<<"type">> := <<"file">>, <<"rules">> := Rules, <<"enable">> := Enable} ->
                        {ok, Filename} = write_file(filename:join([emqx:get_config([node, data_dir]), "acl.conf"]), Rules),
                        #{<<"type">> => <<"file">>, <<"enable">> => Enable, <<"path">> => Filename};
                    _ -> write_cert(Source)
                end
              end || Source <- Body],
    update_config(?CMD_REPLCAE, NBody).

source(get, #{bindings := #{type := Type}}) ->
    case get_raw_source(Type) of
        [] -> {404, #{message => <<"Not found ", Type/binary>>}};
        [#{<<"type">> := <<"file">>, <<"enable">> := Enable, <<"path">> := Path}] ->
            case file:read_file(Path) of
                {ok, Rules} ->
                    {200, #{type => file,
                            enable => Enable,
                            rules => Rules
                           }
                    };
                {error, Reason} ->
                    {400, #{code => <<"BAD_REQUEST">>,
                            message => bin(Reason)}}
            end;
        [Source] ->
            {200, read_cert(Source)}
    end;
source(put, #{bindings := #{type := <<"file">>}, body := #{<<"type">> := <<"file">>, <<"rules">> := Rules, <<"enable">> := Enable}}) ->
    {ok, Filename} = write_file(maps:get(path, emqx_authz:lookup(file), ""), Rules),
    case emqx_authz:update({?CMD_REPLCAE, <<"file">>}, #{<<"type">> => <<"file">>, <<"enable">> => Enable, <<"path">> => Filename}) of
        {ok, _} -> {204};
        {error, Reason} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    message => bin(Reason)}}
    end;
source(put, #{bindings := #{type := Type}, body := Body}) when is_map(Body) ->
    update_config({?CMD_REPLCAE, Type}, write_cert(Body));
source(delete, #{bindings := #{type := Type}}) ->
    update_config({?CMD_DELETE, Type}, #{}).

move_source(post, #{bindings := #{type := Type}, body := #{<<"position">> := Position}}) ->
    case emqx_authz:move(Type, Position) of
        {ok, _} -> {204};
        {error, not_found_source} ->
            {404, #{code => <<"NOT_FOUND">>,
                    message => <<"source ", Type/binary, " not found">>}};
        {error, Reason} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    message => bin(Reason)}}
    end.

get_raw_sources() ->
    RawSources = emqx:get_raw_config([authorization, sources]),
    Schema = #{roots => emqx_authz_schema:fields("authorization"), fields => #{}},
    Conf = #{<<"sources">> => RawSources},
    #{<<"sources">> := Sources} = hocon_schema:check_plain(Schema, Conf, #{only_fill_defaults => true}),
    Sources.

get_raw_source(Type) ->
    lists:filter(fun (#{<<"type">> := T}) ->
                         T =:= Type
                 end, get_raw_sources()).

update_config(Cmd, Sources) ->
    case emqx_authz:update(Cmd, Sources) of
        {ok, _} -> {204};
        {error, {pre_config_update, emqx_authz, Reason}} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    message => bin(Reason)}};
        {error, {post_config_update, emqx_authz, Reason}} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    message => bin(Reason)}};
        {error, Reason} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    message => bin(Reason)}}
    end.

read_cert(#{<<"ssl">> := #{<<"enable">> := true} = SSL} = Source) ->
    CaCert = case file:read_file(maps:get(<<"cacertfile">>, SSL, "")) of
                 {ok, CaCert0} -> CaCert0;
                 _ -> ""
             end,
    Cert =   case file:read_file(maps:get(<<"certfile">>, SSL, "")) of
                 {ok, Cert0} -> Cert0;
                 _ -> ""
             end,
    Key =   case file:read_file(maps:get(<<"keyfile">>, SSL, "")) of
                 {ok, Key0} -> Key0;
                 _ -> ""
             end,
    Source#{<<"ssl">> => SSL#{<<"cacertfile">> => CaCert,
                              <<"certfile">> => Cert,
                              <<"keyfile">> => Key
                             }
           };
read_cert(Source) -> Source.

write_cert(#{<<"ssl">> := #{<<"enable">> := true} = SSL} = Source) ->
    CertPath = filename:join([emqx:get_config([node, data_dir]), "certs"]),
    CaCert = case maps:is_key(<<"cacertfile">>, SSL) of
                 true ->
                     {ok, CaCertFile} = write_file(filename:join([CertPath, "cacert-" ++ emqx_misc:gen_id() ++".pem"]),
                                                 maps:get(<<"cacertfile">>, SSL)),
                     CaCertFile;
                 false -> ""
             end,
    Cert =   case maps:is_key(<<"certfile">>, SSL) of
                 true ->
                     {ok, CertFile} = write_file(filename:join([CertPath, "cert-" ++ emqx_misc:gen_id() ++".pem"]),
                                                 maps:get(<<"certfile">>, SSL)),
                     CertFile;
                 false -> ""
             end,
    Key =    case maps:is_key(<<"keyfile">>, SSL) of
                 true ->
                     {ok, KeyFile}  = write_file(filename:join([CertPath, "key-" ++ emqx_misc:gen_id() ++".pem"]),
                                                 maps:get(<<"keyfile">>, SSL)),
                     KeyFile;
                 false -> ""
             end,
    Source#{<<"ssl">> => SSL#{<<"cacertfile">> => CaCert,
                              <<"certfile">> => Cert,
                              <<"keyfile">> => Key
                             }
           };
write_cert(Source) -> Source.

write_file(Filename, Bytes0) ->
    ok = filelib:ensure_dir(Filename),
    case file:read_file(Filename) of
        {ok, Bytes1} ->
            case crypto:hash(md5, Bytes1) =:= crypto:hash(md5, Bytes0) of
                true -> {ok,iolist_to_binary(Filename)};
                false -> do_write_file(Filename, Bytes0)
            end;
        _ -> do_write_file(Filename, Bytes0)
    end.

do_write_file(Filename, Bytes) ->
    case file:write_file(Filename, Bytes) of
       ok -> {ok, iolist_to_binary(Filename)};
       {error, Reason} ->
           ?LOG(error, "Write File ~p Error: ~p", [Filename, Reason]),
           error(Reason)
    end.

bin(Term) ->
   erlang:iolist_to_binary(io_lib:format("~p", [Term])).
