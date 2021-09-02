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

-export([ roots/0
        , fields/1
        ]).

roots() -> ["authorization"].

fields("authorization") ->
    [ {sources, #{type => union_array(
                    [ hoconsc:ref(?MODULE, file)
                    , hoconsc:ref(?MODULE, http_get)
                    , hoconsc:ref(?MODULE, http_post)
                    , hoconsc:ref(?MODULE, mongo_single)
                    , hoconsc:ref(?MODULE, mongo_rs)
                    , hoconsc:ref(?MODULE, mongo_sharded)
                    , hoconsc:ref(?MODULE, mysql)
                    , hoconsc:ref(?MODULE, pgsql)
                    , hoconsc:ref(?MODULE, redis_single)
                    , hoconsc:ref(?MODULE, redis_sentinel)
                    , hoconsc:ref(?MODULE, redis_cluster)
                    ])}
      }
    ];
fields(file) ->
    [ {type, #{type => file}}
    , {enable, #{type => boolean(),
                 default => true}}
    , {path, #{type => string(),
               validator => fun(S) -> case filelib:is_file(S) of
                                        true -> ok;
                                        _ -> {error, "File does not exist"}
                                      end
                            end
              }}
    ];
fields(http_get) ->
    [ {type, #{type => http}}
    , {enable, #{type => boolean(),
                 default => true}}
    , {url, #{type => url()}}
    , {method,  #{type => get, default => get }}
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
    , {request_timeout,  #{type => timeout(), default => 30000 }}
    ]  ++ proplists:delete(base_url, emqx_connector_http:fields(config));
fields(http_post) ->
    [ {type, #{type => http}}
    , {enable, #{type => boolean(),
                 default => true}}
    , {url, #{type => url()}}
    , {method,  #{type => hoconsc:enum([post, put]),
                  default => get}}
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
    , {request_timeout,  #{type => timeout(), default => 30000 }}
    , {body, #{type => map(),
               nullable => true
              }
      }
    ]  ++ proplists:delete(base_url, emqx_connector_http:fields(config));
fields(mongo_single) ->
    connector_fields(mongo, single) ++
    [ {collection, #{type => atom()}}
    , {find, #{type => map()}}
    ];
fields(mongo_rs) ->
    connector_fields(mongo, rs) ++
    [ {collection, #{type => atom()}}
    , {find, #{type => map()}}
    ];
fields(mongo_sharded) ->
    connector_fields(mongo, sharded) ++
    [ {collection, #{type => atom()}}
    , {find, #{type => map()}}
    ];
fields(mysql) ->
    connector_fields(mysql) ++
    [ {sql, query()} ];
fields(pgsql) ->
    connector_fields(pgsql) ++
    [ {sql, query()} ];
fields(redis_single) ->
    connector_fields(redis, single) ++
    [ {cmd, query()} ];
fields(redis_sentinel) ->
    connector_fields(redis, sentinel) ++
    [ {cmd, query()} ];
fields(redis_cluster) ->
    connector_fields(redis, cluster) ++
    [ {cmd, query()} ].

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

union_array(Item) when is_list(Item) ->
    hoconsc:array(hoconsc:union(Item)).

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
    connector_fields(DB, config).
connector_fields(DB, Fields) ->
    Mod0 = io_lib:format("~s_~s",[emqx_connector, DB]),
    Mod = try
              list_to_existing_atom(Mod0)
          catch
              error:badarg ->
                  list_to_atom(Mod0);
              Error ->
                  erlang:error(Error)
          end,
    [ {type, #{type => DB}}
    , {enable, #{type => boolean(),
                 default => true}}
    ] ++ Mod:fields(Fields).
