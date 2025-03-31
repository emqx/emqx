%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_AUTH_POSTGRESQL_HRL).
-define(EMQX_AUTH_POSTGRESQL_HRL, true).

-define(AUTHZ_TYPE, postgresql).
-define(AUTHZ_TYPE_BIN, <<"postgresql">>).

-define(AUTHN_MECHANISM, password_based).
-define(AUTHN_MECHANISM_BIN, <<"password_based">>).

-define(AUTHN_BACKEND, postgresql).
-define(AUTHN_BACKEND_BIN, <<"postgresql">>).

-define(AUTHN_TYPE, {?AUTHN_MECHANISM, ?AUTHN_BACKEND}).

-endif.
