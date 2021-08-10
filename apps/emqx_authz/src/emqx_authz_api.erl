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
        , authorization/2
        , authorization_once/2
        ]).

api_spec() ->
    {[ authorization_api(),
       authorization_api2()
     ], definitions()}.

definitions() -> emqx_authz_api_schema:definitions().

authorization_api() ->
    Metadata = #{
        get => #{
            description => "List authorization rules",
            parameters => [],
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
                },
                <<"404">> => #{description => <<"Not Found">>}
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
                <<"201">> => #{description => <<"Created">>},
                <<"400">> => #{description => <<"Bad Request">>}
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
                <<"201">> => #{description => <<"Created">>},
                <<"400">> => #{description => <<"Bad Request">>}
            }
        }
    },
    {"/authorization", Metadata, authorization}.

authorization_api2() ->
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
                <<"404">> => #{description => <<"Not Found">>}
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
                <<"400">> => #{description => <<"Bad Request">>}
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
                <<"400">> => #{description => <<"Bad Request">>}
            }
        }
    },
    {"/authorization/:id", Metadata, authorization_once}.

authorization(get, _Request) ->
    Rules = lists:foldl(fun (#{type := _Type, enable := true, annotations := #{id := Id} = Annotations} = Rule, AccIn) ->
                                NRule = case emqx_resource:health_check(Id) of
                                    ok ->
                                        Rule#{annotations => Annotations#{status => healthy}};
                                    _ ->
                                        Rule#{annotations => Annotations#{status => unhealthy}}
                                end,
                                lists:append(AccIn, [NRule]);
                            (Rule, AccIn) ->
                                lists:append(AccIn, [Rule])
                        end, [], emqx_authz:lookup()),
    {200, #{rules => Rules}};
authorization(post, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    RawConfig = jsx:decode(Body, [return_maps]),
    case emqx_authz:update(head, [RawConfig]) of
        ok -> {201};
        {error, Reason} -> {400, #{messgae => atom_to_binary(Reason)}}
    end;
authorization(put, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    RawConfig = jsx:decode(Body, [return_maps]),
    case emqx_authz:update(replace, RawConfig) of
        ok -> {204};
        {error, Reason} -> {400, #{messgae => atom_to_binary(Reason)}}
    end.

authorization_once(get, Request) ->
    Id = cowboy_req:binding(id, Request),
    case emqx_authz:lookup(Id) of
        {error, Reason} -> {404, #{messgae => atom_to_binary(Reason)}};
        Rule ->
            case maps:get(type, Rule, undefined) of
                undefined -> {200, Rule};
                _ ->
                    case emqx_resource:health_check(Id) of
                        ok ->
                            {200, Rule#{annotations => #{status => healthy}}};
                        _ ->
                            {200, Rule#{annotations => #{status => unhealthy}}}
                    end

            end
    end;
authorization_once(put, Request) ->
    RuleId = cowboy_req:binding(id, Request),
    {ok, Body, _} = cowboy_req:read_body(Request),
    RawConfig = jsx:decode(Body, [return_maps]),
    case emqx_authz:update({replace_once, RuleId}, RawConfig) of
        ok -> {204};
        {error, Reason} -> {400, #{messgae => atom_to_binary(Reason)}}
    end;
authorization_once(delete, Request) ->
    RuleId = cowboy_req:binding(id, Request),
    case emqx_authz:update({replace_once, RuleId}, #{}) of
        ok -> {204};
        {error, Reason} -> {400, #{messgae => atom_to_binary(Reason)}}
    end.
