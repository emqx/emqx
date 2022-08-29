-define(APP, emqx_auth_mnesia).

-type(login() :: {clientid, binary()}
              | {username, binary()}).

-type(acl_target() :: login() | all).

-type(acl_target_type() :: clientid | username | all).

-type(access():: allow | deny).
-type(action():: pub | sub).
-type(legacy_action():: action() | pubsub).
-type(created_at():: integer()).

-record(emqx_user, {
          login,
          password,
          created_at
        }).

-type(emqx_user() :: #emqx_user{
          login :: login(),
          password :: binary(),
          created_at :: created_at()
        }).

-define(ACL_TABLE, emqx_acl).

-define(MIGRATION_MARK_KEY, emqx_acl2_migration_started).

-record(?ACL_TABLE, {
          filter :: {acl_target(), emqx_topic:topic()} | ?MIGRATION_MARK_KEY,
          action :: legacy_action(),
          access :: access(),
          created_at :: created_at()
         }).

-define(MIGRATION_MARK_RECORD, #?ACL_TABLE{filter = ?MIGRATION_MARK_KEY, action = pub, access = deny, created_at = 0}).

-type(rule() :: {access(), action(), emqx_topic:topic(), created_at()}).

-define(ACL_TABLE2, emqx_acl2).

-record(?ACL_TABLE2, {
          who :: acl_target(),
          rules :: [ rule() ]
         }).

-type(acl_record() :: {acl_target(), emqx_topic:topic(), action(), access(), created_at()}).
