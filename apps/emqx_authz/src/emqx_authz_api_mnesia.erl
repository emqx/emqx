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

-behaviour(minirest_api).

-include("emqx_authz.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-define(EXAMPLE_USERNAME, #{username => user1,
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
-define(EXAMPLE_CLIENTID, #{clientid => client1,
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
-define(EXAMPLE_ALL ,     #{rules => [ #{topic => <<"test/toopic/1">>,
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
-define(FORMAT_USERNAME_FUN, {?MODULE, format_by_username}).
-define(FORMAT_CLIENTID_FUN, {?MODULE, format_by_clientid}).


-export([ api_spec/0
        , purge/2
        , users/2
        , user/2
        , clients/2
        , client/2
        , all/2
        ]).

-export([ format_by_username/1
        , format_by_clientid/1]).

api_spec() ->
    {[ purge_api()
     , users_api()
     , user_api()
     , clients_api()
     , client_api()
     , all_api()
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
    Username = #{
        type => object,
        required => [username, rules],
        properties => #{
           username => #{
               type => string,
               example => <<"username">>
           },
           rules => minirest:ref(<<"rules">>)
        }
    },
    Clientid = #{
        type => object,
        required => [clientid, rules],
        properties => #{
           clientid => #{
               type => string,
               example => <<"clientid">>
           },
           rules => minirest:ref(<<"rules">>)
        }
    },
    ALL = #{
      type => object,
      required => [rules],
      properties => #{
         rules => minirest:ref(<<"rules">>)
      }
    },
    [ #{<<"rules">> => Rules}
    , #{<<"username">> => Username}
    , #{<<"clientid">> => Clientid}
    , #{<<"all">> => ALL}
    ].

users_api() ->
    Metadata = #{
        get => #{
            description => "Show the list of record for username",
            parameters => [
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
                                items => minirest:ref(<<"username">>)
                            },
                            examples => #{
                                username => #{
                                    summary => <<"Username">>,
                                    value => jsx:encode([?EXAMPLE_USERNAME])
                                }
                           }
                        }
                    }
                }
            }
        },
        post => #{
            description => "Add new records for username",
            requestBody => #{
                content => #{
                    'application/json' => #{
                        schema => #{
                            type => array,
                            items => #{
                                oneOf => [ minirest:ref(<<"username">>)
                                         ]
                            }
                        },
                        examples => #{
                            username => #{
                                summary => <<"Username">>,
                                value => jsx:encode([?EXAMPLE_USERNAME])
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
    {"/authorization/sources/built-in-database/username", Metadata, users}.

clients_api() ->
    Metadata = #{
        get => #{
            description => "Show the list of record for clientid",
            parameters => [
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
                                items => minirest:ref(<<"clientid">>)
                            },
                            examples => #{
                                clientid => #{
                                    summary => <<"Clientid">>,
                                    value => jsx:encode([?EXAMPLE_CLIENTID])
                                }
                           }
                        }
                    }
                }
            }
        },
        post => #{
            description => "Add new records for clientid",
            requestBody => #{
                content => #{
                    'application/json' => #{
                        schema => #{
                            type => array,
                            items => #{
                                oneOf => [ minirest:ref(<<"clientid">>)
                                         ]
                            }
                        },
                        examples => #{
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
        }
    },
    {"/authorization/sources/built-in-database/clientid", Metadata, clients}.

user_api() ->
    Metadata = #{
        get => #{
            description => "Get record info for username",
            parameters => [
                #{
                    name => username,
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
                            schema => minirest:ref(<<"username">>),
                            examples => #{
                                username => #{
                                    summary => <<"Username">>,
                                    value => jsx:encode(?EXAMPLE_USERNAME)
                                }
                            }
                        }
                    }
                },
                <<"404">> => emqx_mgmt_util:bad_request(<<"Not Found">>)
            }
        },
        put => #{
            description => "Set record for username",
            parameters => [
                #{
                    name => username,
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
                        schema => minirest:ref(<<"username">>),
                        examples => #{
                            username => #{
                                summary => <<"Username">>,
                                value => jsx:encode(?EXAMPLE_USERNAME)
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
            description => "Delete one record for username",
            parameters => [
                #{
                    name => username,
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
    {"/authorization/sources/built-in-database/username/:username", Metadata, user}.

client_api() ->
    Metadata = #{
        get => #{
            description => "Get record info for clientid",
            parameters => [
                #{
                    name => clientid,
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
                            schema => minirest:ref(<<"clientid">>),
                            examples => #{
                                clientid => #{
                                    summary => <<"Clientid">>,
                                    value => jsx:encode(?EXAMPLE_CLIENTID)
                                }
                            }
                        }
                    }
                },
                <<"404">> => emqx_mgmt_util:bad_request(<<"Not Found">>)
            }
        },
        put => #{
            description => "Set record for clientid",
            parameters => [
                #{
                    name => clientid,
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
                        schema => minirest:ref(<<"clientid">>),
                        examples => #{
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
            description => "Delete one record for clientid",
            parameters => [
                #{
                    name => clientid,
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
    {"/authorization/sources/built-in-database/clientid/:clientid", Metadata, client}.

all_api() ->
    Metadata = #{
        get => #{
            description => "Show the list of rules for all",
            responses => #{
                <<"200">> => #{
                    description => <<"OK">>,
                    content => #{
                        'application/json' => #{
                            schema => minirest:ref(<<"clientid">>),
                            examples => #{
                                clientid => #{
                                    summary => <<"All">>,
                                    value => jsx:encode(?EXAMPLE_ALL)
                                }
                           }
                        }
                    }
                }
            }
        },
        put => #{
            description => "Set the list of rules for all",
            requestBody => #{
                content => #{
                    'application/json' => #{
                        schema => minirest:ref(<<"all">>),
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
    {"/authorization/sources/built-in-database/all", Metadata, all}.

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

users(get, #{query_string := PageParams}) ->
    MatchSpec = ets:fun2ms(
                  fun({?ACL_TABLE, {?ACL_TABLE_USERNAME, Username}, Rules}) ->
                          [{username, Username}, {rules, Rules}]
                  end),
    {200, emqx_mgmt_api:paginate(?ACL_TABLE, MatchSpec, PageParams, ?FORMAT_USERNAME_FUN)};
users(post, #{body := Body}) when is_list(Body) ->
    lists:foreach(fun(#{<<"username">> := Username, <<"rules">> := Rules}) ->
                      mria:dirty_write(#emqx_acl{
                                          who = {?ACL_TABLE_USERNAME, Username},
                                          rules = format_rules(Rules)
                                         })
                  end, Body),
    {204}.

clients(get, #{query_string := PageParams}) ->
    MatchSpec = ets:fun2ms(
                  fun({?ACL_TABLE, {?ACL_TABLE_CLIENTID, Clientid}, Rules}) ->
                          [{clientid, Clientid}, {rules, Rules}]
                  end),
    {200, emqx_mgmt_api:paginate(?ACL_TABLE, MatchSpec, PageParams, ?FORMAT_CLIENTID_FUN)};
clients(post, #{body := Body}) when is_list(Body) ->
    lists:foreach(fun(#{<<"clientid">> := Clientid, <<"rules">> := Rules}) ->
                      mria:dirty_write(#emqx_acl{
                                          who = {?ACL_TABLE_CLIENTID, Clientid},
                                          rules = format_rules(Rules)
                                         })
                  end, Body),
    {204}.

user(get, #{bindings := #{username := Username}}) ->
    case mnesia:dirty_read(?ACL_TABLE, {?ACL_TABLE_USERNAME, Username}) of
        [] -> {404, #{code => <<"NOT_FOUND">>, message => <<"Not Found">>}};
        [#emqx_acl{who = {?ACL_TABLE_USERNAME, Username}, rules = Rules}] ->
            {200, #{username => Username,
                    rules => [ #{topic => Topic,
                                 action => Action,
                                 permission => Permission
                                } || {Permission, Action, Topic} <- Rules]}
            }
    end;
user(put, #{bindings := #{username := Username},
              body := #{<<"username">> := Username, <<"rules">> := Rules}}) ->
    mria:dirty_write(#emqx_acl{
                        who = {?ACL_TABLE_USERNAME, Username},
                        rules = format_rules(Rules)
                       }),
    {204};
user(delete, #{bindings := #{username := Username}}) ->
    mria:dirty_delete({?ACL_TABLE, {?ACL_TABLE_USERNAME, Username}}),
    {204}.

client(get, #{bindings := #{clientid := Clientid}}) ->
    case mnesia:dirty_read(?ACL_TABLE, {?ACL_TABLE_CLIENTID, Clientid}) of
        [] -> {404, #{code => <<"NOT_FOUND">>, message => <<"Not Found">>}};
        [#emqx_acl{who = {?ACL_TABLE_CLIENTID, Clientid}, rules = Rules}] ->
            {200, #{clientid => Clientid,
                    rules => [ #{topic => Topic,
                                 action => Action,
                                 permission => Permission
                                } || {Permission, Action, Topic} <- Rules]}
            }
    end;
client(put, #{bindings := #{clientid := Clientid},
              body := #{<<"clientid">> := Clientid, <<"rules">> := Rules}}) ->
    mria:dirty_write(#emqx_acl{
                        who = {?ACL_TABLE_CLIENTID, Clientid},
                        rules = format_rules(Rules)
                       }),
    {204};
client(delete, #{bindings := #{clientid := Clientid}}) ->
    mria:dirty_delete({?ACL_TABLE, {?ACL_TABLE_CLIENTID, Clientid}}),
    {204}.

all(get, _) ->
    case mnesia:dirty_read(?ACL_TABLE, ?ACL_TABLE_ALL) of
        [] ->
            {200, #{rules => []}};
        [#emqx_acl{who = ?ACL_TABLE_ALL, rules = Rules}] ->
            {200, #{rules => [ #{topic => Topic,
                                 action => Action,
                                 permission => Permission
                                } || {Permission, Action, Topic} <- Rules]}
            }
    end;
all(put, #{body := #{<<"rules">> := Rules}}) ->
    mria:dirty_write(#emqx_acl{
                        who = ?ACL_TABLE_ALL,
                        rules = format_rules(Rules)
                       }),
    {204}.

purge(delete, _) ->
    case emqx_authz_api_sources:get_raw_source(<<"built-in-database">>) of
        [#{<<"enable">> := false}] ->
            ok = lists:foreach(fun(Key) ->
                                   ok = mria:dirty_delete(?ACL_TABLE, Key)
                               end, mnesia:dirty_all_keys(?ACL_TABLE)),
            {204};
        [#{<<"enable">> := true}] ->
            {400, #{code => <<"BAD_REQUEST">>,
                    message => <<"'built-in-database' type source must be disabled before purge.">>}};
        [] ->
            {404, #{code => <<"BAD_REQUEST">>,
                    message => <<"'built-in-database' type source is not found.">>
                   }}
    end.

format_rules(Rules) when is_list(Rules) ->
    lists:foldl(fun(#{<<"topic">> := Topic,
                      <<"action">> := Action,
                      <<"permission">> := Permission
                     }, AccIn) when ?PUBSUB(Action)
                            andalso ?ALLOW_DENY(Permission) ->
                   AccIn ++ [{ atom(Permission), atom(Action), Topic }]
                end, [], Rules).

format_by_username([{username, Username}, {rules, Rules}]) ->
    #{username => Username,
      rules => [ #{topic => Topic,
                   action => Action,
                   permission => Permission
                  } || {Permission, Action, Topic} <- Rules]
     }.
format_by_clientid([{clientid, Clientid}, {rules, Rules}]) ->
    #{clientid => Clientid,
      rules => [ #{topic => Topic,
                   action => Action,
                   permission => Permission
                  } || {Permission, Action, Topic} <- Rules]
     }.
atom(B) when is_binary(B) ->
    try binary_to_existing_atom(B, utf8)
    catch
        _ -> binary_to_atom(B)
    end;
atom(A) when is_atom(A) -> A.
