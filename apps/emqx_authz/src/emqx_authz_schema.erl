-module(emqx_authz_schema).

-include_lib("typerefl/include/types.hrl").

-reflect_type([ permission/0
              , action/0
              , url/0
              ]).

-typerefl_from_string({url/0, emqx_http_lib, uri_parse}).

-type action() :: publish | subscribe | all.
-type permission() :: allow | deny.
-type url() :: emqx_http_lib:uri_map().

-export([ structs/0
        , fields/1
        ]).

structs() -> ["emqx_authz"].

fields("emqx_authz") ->
    [ {rules, rules()}
    ];
fields(http) ->
    [ {principal, principal()}
    , {type, #{type => http}}
    , {config, #{type => hoconsc:union([ hoconsc:ref(?MODULE, http_get)
                                       , hoconsc:ref(?MODULE, http_post)
                                       ])}
      }
    ];
fields(http_get) ->
    [ {url, #{type => url()}}
    , {headers, #{type => map(),
                  default => #{ <<"accept">> => <<"application/json">>
                              , <<"cache-control">> => <<"no-cache">>
                              , <<"connection">> => <<"keep-alive">>
                              , <<"keep-alive">> => <<"timeout=5">>
                              },
                  converter => fun (Headers0) ->
                                    Headers1 = maps:fold(fun(K0, V, AccIn) ->
                                                           K1 = iolist_to_binary(string:to_lower(binary_to_list(K0))),
                                                           maps:put(K1, V, AccIn)
                                                        end, #{}, Headers0),
                                    maps:merge(#{ <<"accept">> => <<"application/json">>
                                                , <<"cache-control">> => <<"no-cache">>
                                                , <<"connection">> => <<"keep-alive">>
                                                , <<"keep-alive">> => <<"timeout=5">>
                                                }, Headers1)
                               end
                 }
      }
    , {method,  #{type => get, default => get }}
    , {request_timeout,  #{type => timeout(), default => 30000 }}
    ]  ++ proplists:delete(base_url, emqx_connector_http:fields(config));
fields(http_post) ->
    [ {url, #{type => url()}}
    , {headers, #{type => map(),
                  default => #{ <<"accept">> => <<"application/json">>
                              , <<"cache-control">> => <<"no-cache">>
                              , <<"connection">> => <<"keep-alive">>
                              , <<"content-type">> => <<"application/json">>
                              , <<"keep-alive">> => <<"timeout=5">>
                              },
                  converter => fun (Headers0) ->
                                    Headers1 = maps:fold(fun(K0, V, AccIn) ->
                                                           K1 = iolist_to_binary(string:to_lower(binary_to_list(K0))),
                                                           maps:put(K1, V, AccIn)
                                                        end, #{}, Headers0),
                                    maps:merge(#{ <<"accept">> => <<"application/json">>
                                                , <<"cache-control">> => <<"no-cache">>
                                                , <<"connection">> => <<"keep-alive">>
                                                , <<"content-type">> => <<"application/json">>
                                                , <<"keep-alive">> => <<"timeout=5">>
                                                }, Headers1)
                               end
                 }
      }
    , {method,  #{type => hoconsc:enum([post, put]),
                  default => get}}
    , {body, #{type => map(),
               nullable => true
              }
      }
    ]  ++ proplists:delete(base_url, emqx_connector_http:fields(config));
fields(mongo) ->
    connector_fields(mongo) ++
    [ {collection, #{type => atom()}}
    , {find, #{type => map()}}
    ];
fields(redis) ->
    connector_fields(redis) ++
    [ {cmd, query()} ];
fields(mysql) ->
    connector_fields(mysql) ++
    [ {sql, query()} ];
fields(pgsql) ->
    connector_fields(pgsql) ++
    [ {sql, query()} ];
fields(simple_rule) ->
    [ {permission,   #{type => permission()}}
    , {action,   #{type => action()}}
    , {topics,   #{type => union_array(
                             [ binary()
                             , hoconsc:ref(?MODULE, eq_topic)
                             ]
                            )}}
    , {principal, principal()}
    ];
fields(username) ->
    [{username, #{type => binary()}}];
fields(clientid) ->
    [{clientid, #{type => binary()}}];
fields(ipaddress) ->
    [{ipaddress, #{type => string()}}];
fields(andlist) ->
    [{'and', #{type => union_array(
                         [ hoconsc:ref(?MODULE, username)
                         , hoconsc:ref(?MODULE, clientid)
                         , hoconsc:ref(?MODULE, ipaddress)
                         ])
              }
     }
    ];
fields(orlist) ->
    [{'or', #{type => union_array(
                         [ hoconsc:ref(?MODULE, username)
                         , hoconsc:ref(?MODULE, clientid)
                         , hoconsc:ref(?MODULE, ipaddress)
                         ])
              }
     }
    ];
fields(eq_topic) ->
    [{eq, #{type => binary()}}].


%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

union_array(Item) when is_list(Item) ->
    hoconsc:array(hoconsc:union(Item)).

rules() ->
    #{type => union_array(
                [ hoconsc:ref(?MODULE, simple_rule)
                , hoconsc:ref(?MODULE, http)
                , hoconsc:ref(?MODULE, mysql)
                , hoconsc:ref(?MODULE, pgsql)
                , hoconsc:ref(?MODULE, redis)
                , hoconsc:ref(?MODULE, mongo)
                ])
    }.

principal() ->
    #{default => all,
      type => hoconsc:union(
                [ all
                , hoconsc:ref(?MODULE, username)
                , hoconsc:ref(?MODULE, clientid)
                , hoconsc:ref(?MODULE, ipaddress)
                , hoconsc:ref(?MODULE, andlist)
                , hoconsc:ref(?MODULE, orlist)
                ])
     }.

query() ->
    #{type => binary(),
      validator => fun(S) ->
                         case size(S) > 0 of
                             true -> ok;
                             _ -> {error, "Request query"}
                         end
                       end
     }.

connector_fields(DB) ->
    Mod0 = io_lib:format("~s_~s",[emqx_connector, DB]),
    Mod = try
              list_to_existing_atom(Mod0)
          catch
              error:badarg ->
                  list_to_atom(Mod0);
              Error ->
                  erlang:error(Error)
          end,
    [ {principal, principal()}
    , {type, #{type => DB}}
    ] ++ Mod:fields("").
