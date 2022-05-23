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
