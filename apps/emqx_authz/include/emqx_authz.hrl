-type(ipaddress() :: {ipaddr,  esockd_cidr:cidr_string()} |
                     {ipaddrs, list(esockd_cidr:cidr_string())}).

-type(username() :: {username, binary()}).

-type(clientid() :: {clientid, binary()}).

-type(who() :: ipaddress() | username() | clientid() |
               {'and', [ipaddress() | username() | clientid()]} |
               {'or',  [ipaddress() | username() | clientid()]} |
               all).

-type(action() :: subscribe | publish | all).

-type(permission() :: allow | deny).

-type(rule() :: {permission(), who(), action(), list(emqx_types:topic())}).
-type(rules() :: [rule()]).

-type(sources() :: [map()]).

-define(APP, emqx_authz).

-define(ALLOW_DENY(A), ((A =:= allow) orelse (A =:= <<"allow">>) orelse
                        (A =:= deny)  orelse (A =:= <<"deny">>)
                       )).
-define(PUBSUB(A), ((A =:= subscribe) orelse (A =:= <<"subscribe">>) orelse
                    (A =:= publish)   orelse (A =:= <<"publish">>) orelse
                    (A =:= all)       orelse (A =:= <<"all">>)
                   )).

-define(ACL_SHARDED, emqx_acl_sharded).

-define(ACL_TABLE, emqx_acl).

%% To save some space, use an integer for label, 0 for 'all', {1, Username} and {2, ClientId}.
-define(ACL_TABLE_ALL, 0).
-define(ACL_TABLE_USERNAME, 1).
-define(ACL_TABLE_CLIENTID, 2).

-record(emqx_acl, {
          who :: ?ACL_TABLE_ALL| {?ACL_TABLE_USERNAME, binary()} | {?ACL_TABLE_CLIENTID, binary()},
          rules :: [ {permission(), action(), emqx_topic:topic()} ]
         }).

-record(authz_metrics, {
        allow = 'client.authorize.allow',
        deny = 'client.authorize.deny',
        ignore = 'client.authorize.ignore'
    }).

-define(CMD_REPLACE, replace).
-define(CMD_DELETE, delete).
-define(CMD_PREPEND, prepend).
-define(CMD_APPEND, append).
-define(CMD_MOVE, move).

-define(CMD_MOVE_TOP, <<"top">>).
-define(CMD_MOVE_BOTTOM, <<"bottom">>).
-define(CMD_MOVE_BEFORE(Before), {<<"before">>, Before}).
-define(CMD_MOVE_AFTER(After), {<<"after">>, After}).

-define(METRICS(Type), tl(tuple_to_list(#Type{}))).
-define(METRICS(Type, K), #Type{}#Type.K).

-define(AUTHZ_METRICS, ?METRICS(authz_metrics)).
-define(AUTHZ_METRICS(K), ?METRICS(authz_metrics, K)).

-define(CONF_KEY_PATH, [authorization, sources]).

-define(RE_PLACEHOLDER, "\\$\\{[a-z0-9\\-]+\\}").

-define(USERNAME_RULES_EXAMPLE, #{username => user1,
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
-define(CLIENTID_RULES_EXAMPLE, #{clientid => client1,
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
-define(ALL_RULES_EXAMPLE,      #{rules => [ #{topic => <<"test/toopic/1">>,
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
-define(META_EXAMPLE,           #{ page => 1
                                 , limit => 100
                                 , count => 1
                                 }).
