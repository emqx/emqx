-type(rule() :: #{atom() => any()}).
-type(rules() :: [rule()]).

-define(APP, emqx_authz).

-define(ALLOW_DENY(A), ((A =:= allow) orelse (A =:= deny))).
-define(PUBSUB(A), ((A =:= subscribe) orelse (A =:= publish) orelse (A =:= all))).

-record(authz_metrics, {
        allow = 'client.authorize.allow',
        deny = 'client.authorize.deny',
        ignore = 'client.authorize.ignore'
    }).

-define(METRICS(Type), tl(tuple_to_list(#Type{}))).
-define(METRICS(Type, K), #Type{}#Type.K).

-define(AUTHZ_METRICS, ?METRICS(authz_metrics)).
-define(AUTHZ_METRICS(K), ?METRICS(authz_metrics, K)).
