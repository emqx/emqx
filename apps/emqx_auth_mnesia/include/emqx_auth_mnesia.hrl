%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_AUTH_MNESIA_HRL).
-define(EMQX_AUTH_MNESIA_HRL, true).

-define(AUTHN_SHARD, emqx_authn_shard).

-define(AUTHZ_TYPE, built_in_database).
-define(AUTHZ_TYPE_BIN, <<"built_in_database">>).

-define(AUTHN_MECHANISM_SIMPLE, password_based).
-define(AUTHN_MECHANISM_SIMPLE_BIN, <<"password_based">>).

-define(AUTHN_MECHANISM_SCRAM, scram).
-define(AUTHN_MECHANISM_SCRAM_BIN, <<"scram">>).

-define(AUTHN_BACKEND, built_in_database).
-define(AUTHN_BACKEND_BIN, <<"built_in_database">>).

-define(AUTHN_TYPE_SIMPLE, {?AUTHN_MECHANISM_SIMPLE, ?AUTHN_BACKEND}).
-define(AUTHN_TYPE_SCRAM, {?AUTHN_MECHANISM_SCRAM, ?AUTHN_BACKEND}).

-endif.
