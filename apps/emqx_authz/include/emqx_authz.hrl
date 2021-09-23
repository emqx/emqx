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

-define(METRICS(Type), tl(tuple_to_list(#Type{}))).
-define(METRICS(Type, K), #Type{}#Type.K).

-define(AUTHZ_METRICS, ?METRICS(authz_metrics)).
-define(AUTHZ_METRICS(K), ?METRICS(authz_metrics, K)).
