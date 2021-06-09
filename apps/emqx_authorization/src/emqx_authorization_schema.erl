-module(emqx_authorization_schema).

-include_lib("typerefl/include/types.hrl").

-type action() :: pub | sub | pubsub.
-type access() :: allow | deny.

-reflect_type([ access/0
              , action/0
              ]).

-export([structs/0, fields/1]).

structs() -> ["rules"].

fields("rules") ->
    [ fun (type) -> {array, {union, ["base_rule", "connector_rule"]}};
          (default) -> [];
          (_) -> undefined
      end
    ];
fields("connector_rule") ->
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
    , {type, fun (type) -> {union, [mysql]};
                 (_) -> undefined
             end}
    , {config, fun (type) -> map();
                    (_) -> undefined
               end}
    ];
fields("base_rule") ->
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

fields(_) -> [].

access(type) -> access();
access(_) -> undefined.

action(type) -> action();
action(_) -> undefined.

topics(type) -> {array, {union, [binary(), {eq, binary()}]}};
topics(_) -> undefined.

username(type) -> binary();
username(_) -> undefined.

clientid(type) -> binary();
clientid(_) -> undefined.

ipaddress(type) -> string();
ipaddress(_) -> undefined.

principal(type) -> {array, {union, ["username", "clientid", "ipaddress"]}};
principal(_) -> undefined.
