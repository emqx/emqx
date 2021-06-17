-module(emqx_authz_schema).

-include_lib("typerefl/include/types.hrl").

-type action() :: pub | sub | pubsub.
-type access() :: allow | deny.

-reflect_type([ access/0
              , action/0
              ]).

-export([structs/0, fields/1]).

structs() -> ["authz"].

fields("authz") ->
    [ {rules, hoconsc:array({union,["simple_rule", "sql_connector", "redis_connector"]})
      }
    ];
fields("redis_connector") ->
    [ {principal, fun (default) -> all;
                      (type) -> {union,
                                 [all,
                                  "username",
                                  "clientid",
                                  "ipaddress",
                                  "andlist",
                                  "orlist"]
                                };
                      (_) -> undefined
                 end
      }
    , {type, fun (type) -> {enum, [redis]};
                 (_) -> undefined
             end}
    , {config, fun (type) -> map();
                   (_) -> undefined
               end}
    , {cmd, fun(type) -> binary();
               (validator) -> fun(S) ->
                                case size(S) > 0 of
                                    true -> ok;
                                    _ -> {error, "Request CMD"}
                                end
                              end;
               (_) -> undefined
            end}
    ];
fields("sql_connector") ->
    [ {principal, fun (default) -> all;
                      (type) -> {union,
                                 [all,
                                  "username",
                                  "clientid",
                                  "ipaddress",
                                  "andlist",
                                  "orlist"]
                                };
                      (_) -> undefined
                 end
      }
    , {type, fun (type) -> {enum, [mysql, pgsql]};
                 (_) -> undefined
             end}
    , {config, fun (type) -> map();
                    (_) -> undefined
               end}
    , {sql, fun(type) -> binary();
               (validator) -> fun(S) ->
                                case size(S) > 0 of
                                    true -> ok;
                                    _ -> {error, "Request SQL"}
                                end
                              end;
               (_) -> undefined
            end}
    ];
fields("simple_rule") ->
    [ {access,   fun access/1}
    , {action,   fun action/1}
    , {topics,   fun topics/1}
    , {principal, fun (default) -> all;
                      (type) -> {union,
                                 [all,
                                  "username",
                                  "clientid",
                                  "ipaddress",
                                  "andlist",
                                  "orlist"]
                                };
                      (_) -> undefined
                 end
      }
    ];
fields("username") ->
    [{username, fun username/1}];
fields("clientid") ->
    [{clientid, fun clientid/1}];
fields("ipaddress") ->
    [{ipaddress, fun ipaddress/1}];
fields("andlist") ->
    [{'and', fun principal/1}];
fields("orlist") ->
    [{'or', fun principal/1}];
fields("eq_topic") ->
    [{eq, fun(type) -> binary();
              (_) -> undefined
          end
     }
    ].

access(type) -> access();
access(_) -> undefined.

action(type) -> action();
action(_) -> undefined.

topics(type) -> {array, {union, [binary(), "eq_topic"]}};
topics(_) -> undefined.

username(type) -> binary();
username(_) -> undefined.

clientid(type) -> binary();
clientid(_) -> undefined.

ipaddress(type) -> string();
ipaddress(_) -> undefined.

principal(type) -> {array, {union, ["username", "clientid", "ipaddress"]}};
principal(_) -> undefined.
