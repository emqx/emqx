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
        #{rules => [?EXAMPLE_RETURNED_RULE1
                   ]
        }).

-define(EXAMPLE_RULE1, #{principal => <<"all">>,
                         permission => <<"allow">>,
                         action => <<"all">>,
                         topics => [<<"#">>]}).

-export([ api_spec/0
        , rules/2
        , rule/2
        , move_rule/2
        ]).

api_spec() ->
    {[ rules_api()
     , rule_api()
     , move_rule_api()
     ], definitions()}.

definitions() -> emqx_authz_api_schema:definitions().

rules_api() ->
    Metadata = #{
        get => #{
            description => "List authorization rules",
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
                                required => [rules],
                                properties => #{rules => #{
                                                  type => array,
                                                  items => minirest:ref(<<"returned_rules">>)
                                                 }
                                               }
                            },
                            examples => #{
                                rules => #{
                                    summary => <<"Rules">>,
                                    value => jsx:encode(?EXAMPLE_RETURNED_RULES)
                                }
                            }
                         }
                    }
                }
            }
        },
        post => #{
            description => "Add new rule",
            requestBody => #{
                content => #{
                    'application/json' => #{
                        schema => minirest:ref(<<"rules">>),
                        examples => #{
                            simple_rule => #{
                                summary => <<"Rules">>,
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

            description => "Update all rules",
            requestBody => #{
                content => #{
                    'application/json' => #{
                        schema => #{
                            type => array,
                            items => minirest:ref(<<"returned_rules">>)
                        },
                        examples => #{
                            rules => #{
                                summary => <<"Rules">>,
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
    {"/authorization", Metadata, rules}.

rule_api() ->
    Metadata = #{
        get => #{
            description => "List authorization rules",
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
                            schema => minirest:ref(<<"returned_rules">>),
                            examples => #{
                                rules => #{
                                    summary => <<"Rules">>,
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
                                        message => <<"rule xxx not found">>
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        put => #{
            description => "Update rule",
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
                        schema => minirest:ref(<<"rules">>),
                        examples => #{
                            simple_rule => #{
                                summary => <<"Rules">>,
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
                                        message => <<"rule xxx not found">>
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
            description => "Delete rule",
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
    {"/authorization/:id", Metadata, rule}.

move_rule_api() ->
    Metadata = #{
        post => #{
            description => "Change the order of rules",
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
                    content => #{
                        'application/json' => #{
                            schema => minirest:ref(<<"error">>),
                            examples => #{
                                example1 => #{
                                    summary => <<"Not Found">>,
                                    value => #{
                                        code => <<"NOT_FOUND">>,
                                        message => <<"rule xxx not found">>
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
    {"/authorization/:id/move", Metadata, move_rule}.

rules(get, #{query_string := Query}) ->
    Rules = lists:foldl(fun (#{type := _Type, enable := true, config := #{server := Server} = Config, annotations := #{id := Id}} = Rule, AccIn) ->
                                NRule = case emqx_resource:health_check(Id) of
                                    ok ->
                                        Rule#{config => Config#{server => emqx_connector_schema_lib:ip_port_to_string(Server)},
                                              annotations => #{id => Id,
                                                               status => healthy}};
                                    _ ->
                                        Rule#{config => Config#{server => emqx_connector_schema_lib:ip_port_to_string(Server)},
                                              annotations => #{id => Id,
                                                               status => unhealthy}}
                                end,
                                lists:append(AccIn, [NRule]);
                            (#{type := _Type, enable := true, annotations := #{id := Id}} = Rule, AccIn) ->
                                NRule = case emqx_resource:health_check(Id) of
                                    ok ->
                                        Rule#{annotations => #{id => Id,
                                                               status => healthy}};
                                    _ ->
                                        Rule#{annotations => #{id => Id,
                                                               status => unhealthy}}
                                end,
                                lists:append(AccIn, [NRule]);
                            (Rule, AccIn) ->
                                lists:append(AccIn, [Rule])
                        end, [], emqx_authz:lookup()),
    case maps:is_key(<<"page">>, Query) andalso maps:is_key(<<"limit">>, Query) of
        true ->
            Page = maps:get(<<"page">>, Query),
            Limit = maps:get(<<"limit">>, Query),
            Index = (binary_to_integer(Page) - 1) * binary_to_integer(Limit),
            {_, Rules1} = lists:split(Index, Rules),
            case binary_to_integer(Limit) < length(Rules1) of
                true ->
                    {Rules2, _} = lists:split(binary_to_integer(Limit), Rules1),
                    {200, #{rules => Rules2}};
                false -> {200, #{rules => Rules1}}
            end;
        false -> {200, #{rules => Rules}}
    end;
rules(post, #{body := RawConfig}) ->
    case emqx_authz:update(head, [RawConfig]) of
        {ok, _} -> {204};
        {error, Reason} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    messgae => atom_to_binary(Reason)}}
    end;
rules(put, #{body := RawConfig}) ->
    case emqx_authz:update(replace, RawConfig) of
        {ok, _} -> {204};
        {error, Reason} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    messgae => atom_to_binary(Reason)}}
    end.

rule(get, #{bindings := #{id := Id}}) ->
    case emqx_authz:lookup(Id) of
        {error, Reason} -> {404, #{messgae => atom_to_binary(Reason)}};
        #{type := file} = Rule -> {200, Rule};
        #{config := #{server := Server} = Config} = Rule ->
            case emqx_resource:health_check(Id) of
                ok ->
                    {200, Rule#{config => Config#{server => emqx_connector_schema_lib:ip_port_to_string(Server)},
                                annotations => #{id => Id,
                                                 status => healthy}}};
                _ ->
                    {200, Rule#{config => Config#{server => emqx_connector_schema_lib:ip_port_to_string(Server)},
                                annotations => #{id => Id,
                                                 status => unhealthy}}}
            end;
        Rule ->
            case emqx_resource:health_check(Id) of
                ok ->
                    {200, Rule#{annotations => #{id => Id,
                                                 status => healthy}}};
                _ ->
                    {200, Rule#{annotations => #{id => Id,
                                                 status => unhealthy}}}
            end
    end;
rule(put, #{bindings := #{id := RuleId}, body := RawConfig}) ->
    case emqx_authz:update({replace_once, RuleId}, RawConfig) of
        {ok, _} -> {204};
        {error, not_found_rule} ->
            {404, #{code => <<"NOT_FOUND">>,
                    messgae => <<"rule ", RuleId/binary, " not found">>}};
        {error, Reason} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    messgae => atom_to_binary(Reason)}}
    end;
rule(delete, #{bindings := #{id := RuleId}}) ->
    case emqx_authz:update({replace_once, RuleId}, #{}) of
        {ok, _} -> {204};
        {error, Reason} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    messgae => atom_to_binary(Reason)}}
    end.
move_rule(post, #{bindings := #{id := RuleId}, body := Body}) ->
    #{<<"position">> := Position} = Body,
    case emqx_authz:move(RuleId, Position) of
        {ok, _} -> {204};
        {error, not_found_rule} ->
            {404, #{code => <<"NOT_FOUND">>,
                    messgae => <<"rule ", RuleId/binary, " not found">>}};
        {error, Reason} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    messgae => atom_to_binary(Reason)}}
    end.
