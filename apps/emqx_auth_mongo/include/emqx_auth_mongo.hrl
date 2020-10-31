
-define(APP, emqx_auth_mongo).

-define(DEFAULT_SELECTORS, [{<<"username">>, <<"%u">>}]).

-record(superquery, {collection = <<"mqtt_user">>,
                     field      = <<"is_superuser">>,
                     selector   = {<<"username">>, <<"%u">>}}).

-record(authquery, {collection = <<"mqtt_user">>,
                    field      = <<"password">>,
                    hash       = sha256,
                    selector   = {<<"username">>, <<"%u">>}}).

-record(aclquery, {collection = <<"mqtt_acl">>,
                   selector   = {<<"username">>, <<"%u">>}}).

-record(auth_metrics, {
        success = 'client.auth.success',
        failure = 'client.auth.failure',
        ignore  = 'client.auth.ignore'
    }).

-record(acl_metrics, {
        allow = 'client.acl.allow',
        deny = 'client.acl.deny',
        ignore = 'client.acl.ignore'
    }).

-define(METRICS(Type), tl(tuple_to_list(#Type{}))).
-define(METRICS(Type, K), #Type{}#Type.K).

-define(AUTH_METRICS, ?METRICS(auth_metrics)).
-define(AUTH_METRICS(K), ?METRICS(auth_metrics, K)).

-define(ACL_METRICS, ?METRICS(acl_metrics)).
-define(ACL_METRICS(K), ?METRICS(acl_metrics, K)).
