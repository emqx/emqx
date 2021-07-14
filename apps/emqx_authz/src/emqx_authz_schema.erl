-module(emqx_authz_schema).

-include_lib("typerefl/include/types.hrl").

-type action() :: publish | subscribe | all.
-type permission() :: allow | deny.

-reflect_type([ permission/0
              , action/0
              ]).

-export([structs/0, fields/1]).

structs() -> ["emqx_authz"].

fields("emqx_authz") ->
    [ {rules, rules()}
    ];
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
    Mod = list_to_existing_atom(io_lib:format("~s_~s",[emqx_connector, DB])),
    [ {principal, principal()}
    , {type, #{type => DB}}
    ] ++ Mod:fields("").
