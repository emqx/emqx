-module(emqx_authz_schema).

-include_lib("typerefl/include/types.hrl").

-type action() :: pub | sub | pubsub.
-type access() :: allow | deny.

-reflect_type([ access/0
              , action/0
              ]).

-export([structs/0, fields/1]).

structs() -> [authz].

fields(authz) ->
    [ {rules, rules()}
    ];
fields(redis_connector) ->
    [ {principal, principal()}
    , {type, #{type => hoconsc:enum([redis])}}
    , {config, #{type => map()}}
    , {cmd, query()}
    ];
fields(sql_connector) ->
    [ {principal, principal() }
    , {type, #{type => hoconsc:enum([mysql, pgsql])}}
    , {config, #{type => map()}}
    , {sql, query()}
    ];
fields(simple_rule) ->
    [ {access,   #{type => access()}}
    , {action,   #{type => action()}}
    , {topics,   #{type => union_array(
                             [ binary()
                             , hoconsc:ref(eq_topic) 
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
                         [ hoconsc:ref(username)
                         , hoconsc:ref(clientid)
                         , hoconsc:ref(ipaddress)
                         ])
              }
     }
    ];
fields(orlist) ->
    [{'or', #{type => union_array(
                         [ hoconsc:ref(username)
                         , hoconsc:ref(clientid)
                         , hoconsc:ref(ipaddress)
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
                [ hoconsc:ref(simple_rule)
                , hoconsc:ref(sql_connector)
                , hoconsc:ref(redis_connector)
                ])
    }.

principal() ->
    #{default => all,
      type => hoconsc:union(
                [ all
                , hoconsc:ref(username)
                , hoconsc:ref(clientid)
                , hoconsc:ref(ipaddress)
                , hoconsc:ref(andlist)
                , hoconsc:ref(orlist)
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
