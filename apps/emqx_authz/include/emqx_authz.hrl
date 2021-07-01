-type(rule() :: #{atom() => any()}).
-type(rules() :: [rule()]).

-define(APP, emqx_authz).

-define(ALLOW_DENY(A), ((A =:= allow) orelse (A =:= deny))).
-define(PUBSUB(A), ((A =:= subscribe) orelse (A =:= publish) orelse (A =:= all))).

-record(acl_metrics, {
        allow = 'client.acl.allow',
        deny = 'client.acl.deny',
        ignore = 'client.acl.ignore'
    }).

-define(METRICS(Type), tl(tuple_to_list(#Type{}))).
-define(METRICS(Type, K), #Type{}#Type.K).

-define(ACL_METRICS, ?METRICS(acl_metrics)).
-define(ACL_METRICS(K), ?METRICS(acl_metrics, K)).
