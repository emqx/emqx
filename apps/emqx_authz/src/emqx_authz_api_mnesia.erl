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

-module(emqx_authz_api_mnesia).

-behavior(minirest_api).

-include("emqx_authz.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-define(EXAMPLE_USERNAME, #{type => username,
                            key => user1,
                            rules => [ #{topic => <<"test/toopic/1">>,
                                         permission => <<"allow">>,
                                         action => <<"publish">>
                                        }
                                     , #{topic => <<"test/toopic/2">>,
                                         permission => <<"allow">>,
                                         action => <<"subscribe">>
                                        }
                                     , #{topic => <<"eq test/#">>,
                                         permission => <<"deny">>,
                                         action => <<"all">>
                                        }
                                     ]
                           }).
-define(EXAMPLE_CLIENTID, #{type => clientid,
                            key => client1,
                            rules => [ #{topic => <<"test/toopic/1">>,
                                         permission => <<"allow">>,
                                         action => <<"publish">>
                                        }
                                     , #{topic => <<"test/toopic/2">>,
                                         permission => <<"allow">>,
                                         action => <<"subscribe">>
                                        }
                                     , #{topic => <<"eq test/#">>,
                                         permission => <<"deny">>,
                                         action => <<"all">>
                                        }
                                     ]
                           }).
-define(EXAMPLE_ALL ,     #{type => all,
                            rules => [ #{topic => <<"test/toopic/1">>,
                                         permission => <<"allow">>,
                                         action => <<"publish">>
                                        }
                                     , #{topic => <<"test/toopic/2">>,
                                         permission => <<"allow">>,
                                         action => <<"subscribe">>
                                        }
                                     , #{topic => <<"eq test/#">>,
                                         permission => <<"deny">>,
                                         action => <<"all">>
                                        }
                                     ]
                           }).

-export([ api_spec/0
        , purge/2
        , records/2
        , record/2
        ]).

api_spec() ->
    {[ purge_api()
     , records_api()
     , record_api()
     ], definitions()}.

definitions() ->
    Rules = #{
        type => array,
        items => #{
            type => object,
            required => [topic, permission, action],
            properties => #{
                topic => #{
                    type => string,
                    example => <<"test/topic/1">>
                },
                permission => #{
                    type => string,
                    enum => [<<"allow">>, <<"deny">>],
                    example => <<"allow">>
                },
                action => #{
                    type => string,
                    enum => [<<"publish">>, <<"subscribe">>, <<"all">>],
                    example => <<"publish">>
                }
            }
        }
    },
    Record = #{
        oneOf => [ #{type => object,
                     required => [username, rules],
                     properties => #{
                        username => #{
                            type => string,
                            example => <<"username">>
                        },
                        rules => minirest:ref(<<"rules">>)
                     }
                   }
                 , #{type => object,
                     required => [clientid, rules],
                     properties => #{
                        username => #{
                            type => string,
                            example => <<"clientid">>
                        },
                        rules => minirest:ref(<<"rules">>)
                     }
                   }
                 , #{type => object,
                     required => [rules],
                     properties => #{
                        rules => minirest:ref(<<"rules">>)
                     }
                   }
                 ]
    },
    [ #{<<"rules">> => Rules}
    , #{<<"record">> => Record}
    ].

purge_api() ->
    Metadata = #{
        delete => #{
            description => "Purge all records",
            responses => #{
                <<"204">> => #{description => <<"No Content">>},
                <<"400">> => emqx_mgmt_util:bad_request()
            }
        }
     },
    {"/authorization/sources/built-in-database/purge-all", Metadata, purge}.

records_api() ->
    Metadata = #{
        get => #{
            description => "List records",
            parameters => [
                #{
                    name => type,
                    in => path,
                    schema => #{
                       type => string,
                       enum => [<<"username">>, <<"clientid">>, <<"all">>]
                    },
                    required => true
                },
                #{
                    name => page,
                    in => query,
                    required => false,
                    description => <<"Page Index">>,
                    schema => #{type => integer}
                },
                #{
                    name => limit,
                    in => query,
                    required => false,
                    description => <<"Page limit">>,
                    schema => #{type => integer}
                }
            ],
            responses => #{
                <<"200">> => #{
                    description => <<"OK">>,
                    content => #{
                        'application/json' => #{
                            schema => #{
                                type => array,
                                items => minirest:ref(<<"record">>)
                            },
                            examples => #{
                                username => #{
                                    summary => <<"Username">>,
                                    value => jsx:encode([?EXAMPLE_USERNAME])
                                },
                                clientid => #{
                                    summary => <<"Clientid">>,
                                    value => jsx:encode([?EXAMPLE_CLIENTID])
                                },
                                all => #{
                                    summary => <<"All">>,
                                    value => jsx:encode([?EXAMPLE_ALL])
                                }
                           }
                        }
                    }
                }
            }
        },
        post => #{
            description => "Add new records",
            parameters => [
                #{
                    name => type,
                    in => path,
                    schema => #{
                       type => string,
                       enum => [<<"username">>, <<"clientid">>]
                    },
                    required => true
                }
            ],
            requestBody => #{
                content => #{
                    'application/json' => #{
                        schema => #{
                            type => array,
                            items => minirest:ref(<<"record">>)
                        },
                        examples => #{
                            username => #{
                                summary => <<"Username">>,
                                value => jsx:encode([?EXAMPLE_USERNAME])
                            },
                            clientid => #{
                                summary => <<"Clientid">>,
                                value => jsx:encode([?EXAMPLE_CLIENTID])
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
            description => "Set the list of rules for all",
            parameters => [
                #{
                    name => type,
                    in => path,
                    schema => #{
                       type => string,
                       enum => [<<"all">>]
                    },
                    required => true
                }
            ],
            requestBody => #{
                content => #{
                    'application/json' => #{
                        schema => minirest:ref(<<"record">>),
                        examples => #{
                            all => #{
                                summary => <<"All">>,
                                value => jsx:encode(?EXAMPLE_ALL)
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
    {"/authorization/sources/built-in-database/:type", Metadata, records}.

record_api() ->
    Metadata = #{
        get => #{
            description => "Get record info",
            parameters => [
                #{
                    name => type,
                    in => path,
                    schema => #{
                       type => string,
                       enum => [<<"username">>, <<"clientid">>]
                    },
                    required => true
                },
                #{
                    name => key,
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
                            schema => minirest:ref(<<"record">>),
                            examples => #{
                                username => #{
                                    summary => <<"Username">>,
                                    value => jsx:encode(?EXAMPLE_USERNAME)
                                },
                                clientid => #{
                                    summary => <<"Clientid">>,
                                    value => jsx:encode(?EXAMPLE_CLIENTID)
                                },
                                all => #{
                                    summary => <<"All">>,
                                    value => jsx:encode(?EXAMPLE_ALL)
                                }
                            }
                        }
                    }
                },
                <<"404">> => emqx_mgmt_util:bad_request(<<"Not Found">>)
            }
        },
        put => #{
            description => "Update one record",
            parameters => [
                #{
                    name => type,
                    in => path,
                    schema => #{
                       type => string,
                       enum => [<<"username">>, <<"clientid">>]
                    },
                    required => true
                },
                #{
                    name => key,
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
                        schema => minirest:ref(<<"record">>),
                        examples => #{
                            username => #{
                                summary => <<"Username">>,
                                value => jsx:encode(?EXAMPLE_USERNAME)
                            },
                            clientid => #{
                                summary => <<"Clientid">>,
                                value => jsx:encode(?EXAMPLE_CLIENTID)
                            }
                        }
                    }
                }
            },
            responses => #{
                <<"204">> => #{description => <<"Updated">>},
                <<"400">> => emqx_mgmt_util:bad_request()
            }
        },
        delete => #{
            description => "Delete one record",
            parameters => [
                #{
                    name => type,
                    in => path,
                    schema => #{
                       type => string,
                       enum => [<<"username">>, <<"clientid">>]
                    },
                    required => true
                },
                #{
                    name => key,
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
    {"/authorization/sources/built-in-database/:type/:key", Metadata, record}.

purge(delete, _) ->
    ok = lists:foreach(fun(Key) ->
                           ok = ekka_mnesia:dirty_delete(?ACL_TABLE, Key)
                       end, mnesia:dirty_all_keys(?ACL_TABLE)),
    {204}.

records(get, #{bindings := #{type := <<"username">>},
               query_string := Qs
              }) ->
    MatchSpec = ets:fun2ms(
                  fun({?ACL_TABLE, {?ACL_TABLE_USERNAME, Username}, Rules}) ->
                          [{username, Username}, {rules, Rules}]
                  end),
    Format = fun ([{username, Username}, {rules, Rules}]) ->
                #{username => Username,
                  rules => [ #{topic => Topic,
                               action => Action,
                               permission => Permission
                              } || {Permission, Action, Topic} <- Rules]
                 }
             end,
    case Qs of
        #{<<"limit">> := _, <<"page">> := _} = Page ->
            {200, emqx_mgmt_api:paginate(?ACL_TABLE, MatchSpec, Page, Format)};
        #{<<"limit">> := Limit} ->
            case ets:select(?ACL_TABLE, MatchSpec, binary_to_integer(Limit)) of
                {Rows, _Continuation} -> {200, [Format(Row) || Row <- Rows ]};
                '$end_of_table' -> {404, #{code => <<"NOT_FOUND">>, message => <<"Not Found">>}}
            end;
        _ ->
            {200, [Format(Row) || Row <- ets:select(?ACL_TABLE, MatchSpec)]}
    end;

records(get, #{bindings := #{type := <<"clientid">>},
               query_string := Qs
              }) ->
    MatchSpec = ets:fun2ms(
                  fun({?ACL_TABLE, {?ACL_TABLE_CLIENTID, Clientid}, Rules}) ->
                          [{clientid, Clientid}, {rules, Rules}]
                  end),
    Format = fun ([{clientid, Clientid}, {rules, Rules}]) ->
                #{clientid => Clientid,
                  rules => [ #{topic => Topic,
                               action => Action,
                               permission => Permission
                              } || {Permission, Action, Topic} <- Rules]
                 }
             end,
    case Qs of
        #{<<"limit">> := _, <<"page">> := _} = Page ->
            {200, emqx_mgmt_api:paginate(?ACL_TABLE, MatchSpec, Page, Format)};
        #{<<"limit">> := Limit} ->
            case ets:select(?ACL_TABLE, MatchSpec, binary_to_integer(Limit)) of
                {Rows, _Continuation} -> {200, [Format(Row) || Row <- Rows ]};
                '$end_of_table' -> {404, #{code => <<"NOT_FOUND">>, message => <<"Not Found">>}}
            end;
        _ ->
            {200, [Format(Row) || Row <- ets:select(?ACL_TABLE, MatchSpec)]}
    end;
records(get, #{bindings := #{type := <<"all">>}}) ->
    MatchSpec = ets:fun2ms(
                  fun({?ACL_TABLE, ?ACL_TABLE_ALL, Rules}) ->
                          [{rules, Rules}]
                  end),
    {200, [ #{rules => [ #{topic => Topic,
                           action => Action,
                           permission => Permission
                          } || {Permission, Action, Topic} <- Rules]
             } || [{rules, Rules}] <- ets:select(?ACL_TABLE, MatchSpec)]};
records(post, #{bindings := #{type := <<"username">>},
                body := Body}) when is_list(Body) ->
    lists:foreach(fun(#{<<"username">> := Username, <<"rules">> := Rules}) ->
                      ekka_mnesia:dirty_write(#emqx_acl{
                                                 who = {?ACL_TABLE_USERNAME, Username},
                                                 rules = format_rules(Rules)
                                                })
                  end, Body),
    {204};
records(post, #{bindings := #{type := <<"clientid">>},
                body := Body}) when is_list(Body) ->
    lists:foreach(fun(#{<<"clientid">> := Clientid, <<"rules">> := Rules}) ->
                      ekka_mnesia:dirty_write(#emqx_acl{
                                                 who = {?ACL_TABLE_CLIENTID, Clientid},
                                                 rules = format_rules(Rules)
                                                })
                  end, Body),
    {204};
records(put, #{bindings := #{type := <<"all">>},
               body := #{<<"rules">> := Rules}}) ->
    ekka_mnesia:dirty_write(#emqx_acl{
                               who = ?ACL_TABLE_ALL,
                               rules = format_rules(Rules)
                              }),
    {204}.

record(get, #{bindings := #{type := <<"username">>, key := Key}}) ->
    case mnesia:dirty_read(?ACL_TABLE, {?ACL_TABLE_USERNAME, Key}) of
        [] -> {404, #{code => <<"NOT_FOUND">>, message => <<"Not Found">>}};
        [#emqx_acl{who = {?ACL_TABLE_USERNAME, Username}, rules = Rules}] ->
            {200, #{username => Username,
                    rules => [ #{topic => Topic,
                                 action => Action,
                                 permission => Permission
                                } || {Permission, Action, Topic} <- Rules]}
            }
    end;
record(get, #{bindings := #{type := <<"clientid">>, key := Key}}) ->
    case mnesia:dirty_read(?ACL_TABLE, {?ACL_TABLE_CLIENTID, Key}) of
        [] -> {404, #{code => <<"NOT_FOUND">>, message => <<"Not Found">>}};
        [#emqx_acl{who = {?ACL_TABLE_CLIENTID, Clientid}, rules = Rules}] ->
            {200, #{clientid => Clientid,
                    rules => [ #{topic => Topic,
                                 action => Action,
                                 permission => Permission
                                } || {Permission, Action, Topic} <- Rules]}
            }
    end;
record(put, #{bindings := #{type := <<"username">>, key := Username},
              body := #{<<"username">> := Username, <<"rules">> := Rules}}) ->
    ekka_mnesia:dirty_write(#emqx_acl{
                               who = {?ACL_TABLE_USERNAME, Username},
                               rules = format_rules(Rules)
                              }),
    {204};
record(put, #{bindings := #{type := <<"clientid">>, key := Clientid},
              body := #{<<"clientid">> := Clientid, <<"rules">> := Rules}}) ->
    ekka_mnesia:dirty_write(#emqx_acl{
                               who = {?ACL_TABLE_CLIENTID, Clientid},
                               rules = format_rules(Rules)
                              }),
    {204};
record(delete, #{bindings := #{type := <<"username">>, key := Key}}) ->
    ekka_mnesia:dirty_delete({?ACL_TABLE, {?ACL_TABLE_USERNAME, Key}}),
    {204};
record(delete, #{bindings := #{type := <<"clientid">>, key := Key}}) ->
    ekka_mnesia:dirty_delete({?ACL_TABLE, {?ACL_TABLE_CLIENTID, Key}}),
    {204}.

format_rules(Rules) when is_list(Rules) ->
    lists:foldl(fun(#{<<"topic">> := Topic,
                      <<"action">> := Action,
                      <<"permission">> := Permission
                     }, AccIn) when ?PUBSUB(Action)
                            andalso ?ALLOW_DENY(Permission) ->
                   AccIn ++ [{ atom(Permission), atom(Action), Topic }]
                end, [], Rules).

atom(B) when is_binary(B) ->
    try binary_to_existing_atom(B, utf8)
    catch
        _ -> binary_to_atom(B)
    end;
atom(A) when is_atom(A) -> A.
