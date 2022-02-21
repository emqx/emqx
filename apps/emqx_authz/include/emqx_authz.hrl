-define(APP, emqx_authz).

-define(ALLOW_DENY(A), ((A =:= allow) orelse (A =:= <<"allow">>) orelse
                        (A =:= deny)  orelse (A =:= <<"deny">>)
                       )).
-define(PUBSUB(A), ((A =:= subscribe) orelse (A =:= <<"subscribe">>) orelse
                    (A =:= publish)   orelse (A =:= <<"publish">>) orelse
                    (A =:= all)       orelse (A =:= <<"all">>)
                   )).

-define(CMD_REPLACE, replace).
-define(CMD_DELETE, delete).
-define(CMD_PREPEND, prepend).
-define(CMD_APPEND, append).
-define(CMD_MOVE, move).

-define(CMD_MOVE_TOP, <<"top">>).
-define(CMD_MOVE_BOTTOM, <<"bottom">>).
-define(CMD_MOVE_BEFORE(Before), {<<"before">>, Before}).
-define(CMD_MOVE_AFTER(After), {<<"after">>, After}).

-define(CONF_KEY_PATH, [authorization, sources]).

-define(RE_PLACEHOLDER, "\\$\\{[a-z0-9_]+\\}").

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

-define(RESOURCE_GROUP, <<"emqx_authz">>).
