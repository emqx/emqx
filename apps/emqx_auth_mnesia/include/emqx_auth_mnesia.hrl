-define(APP, emqx_auth_mnesia).

-type(login():: {clientid, binary()}
              | {username, binary()}).

-record(emqx_user, {
          login :: login(),
          password :: binary(),
          created_at :: integer()
        }).

-record(emqx_acl, {
          filter:: {login() | all, emqx_topic:topic()},
          action :: pub | sub | pubsub,
          access :: allow | deny,
          created_at :: integer()
         }).

-record(auth_metrics, {
        success = 'client.auth.success',
        failure = 'client.auth.failure',
        ignore = 'client.auth.ignore'
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
