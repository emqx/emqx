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

-type(rule() :: {permission(), who(), action(), list(emqx_topic:topic())}).
-type(rules() :: [rule()]).

-define(APP, emqx_authz).

-define(ALLOW_DENY(A), ((A =:= allow) orelse (A =:= <<"allow">>) orelse
                        (A =:= deny)  orelse (A =:= <<"deny">>)
                       )).
-define(PUBSUB(A), ((A =:= subscribe) orelse (A =:= <<"subscribe">>) orelse
                    (A =:= publish)   orelse (A =:= <<"publish">>) orelse
                    (A =:= all)       orelse (A =:= <<"all">>)
                   )).

-record(authz_metrics, {
        allow = 'client.authorize.allow',
        deny = 'client.authorize.deny',
        ignore = 'client.authorize.ignore'
    }).

-define(METRICS(Type), tl(tuple_to_list(#Type{}))).
-define(METRICS(Type, K), #Type{}#Type.K).

-define(AUTHZ_METRICS, ?METRICS(authz_metrics)).
-define(AUTHZ_METRICS(K), ?METRICS(authz_metrics, K)).
