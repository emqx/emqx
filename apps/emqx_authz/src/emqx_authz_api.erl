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

-module(emqx_authz_api).

-behavior(minirest_api).

-include("emqx_authz.hrl").

-define(EXAMPLE_RETURNED_RULE1,
        #{principal => <<"all">>,
          permission => <<"allow">>,
          action => <<"all">>,
          topics => [<<"#">>],
          annotations => #{id => 1}
         }).


-define(EXAMPLE_RETURNED_RULES,
        #{sources => [?EXAMPLE_RETURNED_RULE1
                   ]
        }).

-define(EXAMPLE_RULE1, #{principal => <<"all">>,
                         permission => <<"allow">>,
                         action => <<"all">>,
                         topics => [<<"#">>]}).

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
            parameters => [
                #{
                    name => page,
                    in => query,
                    schema => #{
                       type => integer
                    },
                    required => false
                },
                #{
                    name => limit,
                    in => query,
                    schema => #{
                       type => integer
                    },
                    required => false
                }
            ],
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
                                    value => jsx:encode(?EXAMPLE_RETURNED_RULES)
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
                            simple_source => #{
                                summary => <<"Sources">>,
                                value => jsx:encode(?EXAMPLE_RULE1)
                            }
                       }
                    }
                }
            },
            responses => #{
                <<"204">> => #{description => <<"Created">>},
                <<"400">> => #{
                    description => <<"Bad Request">>,
                    content => #{
                        'application/json' => #{
                            schema => minirest:ref(<<"error">>),
                            examples => #{
                                example1 => #{
                                    summary => <<"Bad Request">>,
                                    value => #{
                                        code => <<"BAD_REQUEST">>,
                                        message => <<"Bad Request">>
                                    }
                                }
                            }
                        }
                    }
                }
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
                            sources => #{
                                summary => <<"Sources">>,
                                value => jsx:encode([?EXAMPLE_RULE1])
                            }
                        }
                    }
                }
            },
            responses => #{
                <<"204">> => #{description => <<"Created">>},
                <<"400">> => #{
                    description => <<"Bad Request">>,
                    content => #{
                        'application/json' => #{
                            schema => minirest:ref(<<"error">>),
                            examples => #{
                                example1 => #{
                                    summary => <<"Bad Request">>,
                                    value => #{
                                        code => <<"BAD_REQUEST">>,
                                        message => <<"Bad Request">>
                                    }
                                }
                            }
                        }
                    }
                }
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
                                    value => jsx:encode(?EXAMPLE_RETURNED_RULE1)
                                }
                            }
                         }
                    }
                },
                <<"404">> => #{
                    description => <<"Bad Request">>,
                    content => #{
                        'application/json' => #{
                            schema => minirest:ref(<<"error">>),
                            examples => #{
                                example1 => #{
                                    summary => <<"Not Found">>,
                                    value => #{
                                        code => <<"NOT_FOUND">>,
                                        message => <<"source xxx not found">>
                                    }
                                }
                            }
                        }
                    }
                }
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
                            simple_source => #{
                                summary => <<"Sources">>,
                                value => jsx:encode(?EXAMPLE_RULE1)
                            }
                       }
                    }
                }
            },
            responses => #{
                <<"204">> => #{description => <<"No Content">>},
                <<"404">> => #{
                    description => <<"Bad Request">>,
                    content => #{
                        'application/json' => #{
                            schema => minirest:ref(<<"error">>),
                            examples => #{
                                example1 => #{
                                    summary => <<"Not Found">>,
                                    value => #{
                                        code => <<"NOT_FOUND">>,
                                        message => <<"source xxx not found">>
                                    }
                                }
                            }
                        }
                    }
                },
                <<"400">> => #{
                    description => <<"Bad Request">>,
                    content => #{
                        'application/json' => #{
                            schema => minirest:ref(<<"error">>),
                            examples => #{
                                example1 => #{
                                    summary => <<"Bad Request">>,
                                    value => #{
                                        code => <<"BAD_REQUEST">>,
                                        message => <<"Bad Request">>
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        delete => #{
            description => "Delete source",
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
                <<"204">> => #{description => <<"No Content">>},
                <<"400">> => #{
                    description => <<"Bad Request">>,
                    content => #{
                        'application/json' => #{
                            schema => minirest:ref(<<"error">>),
                            examples => #{
                                example1 => #{
                                    summary => <<"Bad Request">>,
                                    value => #{
                                        code => <<"BAD_REQUEST">>,
                                        message => <<"Bad Request">>
                                    }
                                }
                            }
                        }
                    }
                }
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
                <<"404">> => #{
                    description => <<"Bad Request">>,
                    content => #{ 'application/json' => #{ schema => minirest:ref(<<"error">>),
                            examples => #{
                                example1 => #{
                                    summary => <<"Not Found">>,
                                    value => #{
                                        code => <<"NOT_FOUND">>,
                                        message => <<"source xxx not found">>
                                    }
                                }
                            }
                        }
                    }
                },
                <<"400">> => #{
                    description => <<"Bad Request">>,
                    content => #{
                        'application/json' => #{
                            schema => minirest:ref(<<"error">>),
                            examples => #{
                                example1 => #{
                                    summary => <<"Bad Request">>,
                                    value => #{
                                        code => <<"BAD_REQUEST">>,
                                        message => <<"Bad Request">>
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    },
    {"/authorization/sources/:type/move", Metadata, move_source}.

sources(get, #{query_string := Query}) ->
    Sources = lists:foldl(fun (#{type := _Type, enable := true, config := #{server := Server} = Config, annotations := #{id := Id}} = Source, AccIn) ->
                                NSource = case emqx_resource:health_check(Id) of
                                    ok ->
                                        Source#{config => Config#{server => emqx_connector_schema_lib:ip_port_to_string(Server)},
                                                annotations => #{id => Id,
                                                                 status => healthy}};
                                    _ ->
                                        Source#{config => Config#{server => emqx_connector_schema_lib:ip_port_to_string(Server)},
                                                annotations => #{id => Id,
                                                                 status => unhealthy}}
                                end,
                                lists:append(AccIn, [NSource]);
                            (#{type := _Type, enable := true, annotations := #{id := Id}} = Source, AccIn) ->
                                NSource = case emqx_resource:health_check(Id) of
                                    ok ->
                                        Source#{annotations => #{status => healthy}};
                                    _ ->
                                        Source#{annotations => #{status => unhealthy}}
                                end,
                                lists:append(AccIn, [NSource]);
                            (Source, AccIn) ->
                                lists:append(AccIn, [Source])
                        end, [], emqx_authz:lookup()),
    case maps:is_key(<<"page">>, Query) andalso maps:is_key(<<"limit">>, Query) of
        true ->
            Page = maps:get(<<"page">>, Query),
            Limit = maps:get(<<"limit">>, Query),
            Index = (binary_to_integer(Page) - 1) * binary_to_integer(Limit),
            {_, Sources1} = lists:split(Index, Sources),
            case binary_to_integer(Limit) < length(Sources1) of
                true ->
                    {Sources2, _} = lists:split(binary_to_integer(Limit), Sources1),
                    {200, #{sources => Sources2}};
                false -> {200, #{sources => Sources1}}
            end;
        false -> {200, #{sources => Sources}}
    end;
sources(post, #{body := RawConfig}) ->
    case emqx_authz:update(head, [RawConfig]) of
        {ok, _} -> {204};
        {error, Reason} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    messgae => atom_to_binary(Reason)}}
    end;
sources(put, #{body := RawConfig}) ->
    case emqx_authz:update(replace, RawConfig) of
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
        #{config := #{server := Server,
                      annotations := #{id := Id}
                     } = Config} = Source ->
            case emqx_resource:health_check(Id) of
                ok ->
                    {200, Source#{config => Config#{server => emqx_connector_schema_lib:ip_port_to_string(Server)},
                                  annotations => #{status => healthy}}};
                _ ->
                    {200, Source#{config => Config#{server => emqx_connector_schema_lib:ip_port_to_string(Server)},
                                  annotations => #{status => unhealthy}}}
            end;
        #{config := #{annotations := #{id := Id}}} = Source ->
            case emqx_resource:health_check(Id) of
                ok ->
                    {200, Source#{annotations => #{status => healthy}}};
                _ ->
                    {200, Source#{annotations => #{status => unhealthy}}}
            end
    end;
source(put, #{bindings := #{type := Type}, body := RawConfig}) ->
    case emqx_authz:update({replace_once, Type}, RawConfig) of
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



