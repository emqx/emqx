%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_AUTH_HTTP_HRL).
-define(EMQX_AUTH_HTTP_HRL, true).

-define(AUTHZ_TYPE, http).
-define(AUTHZ_TYPE_BIN, <<"http">>).

-define(AUTHN_MECHANISM, password_based).
-define(AUTHN_MECHANISM_BIN, <<"password_based">>).

-define(AUTHN_MECHANISM_SCRAM, scram).
-define(AUTHN_MECHANISM_SCRAM_BIN, <<"scram">>).

-define(AUTHN_BACKEND, http).
-define(AUTHN_BACKEND_BIN, <<"http">>).
-define(AUTHN_TYPE, {?AUTHN_MECHANISM, ?AUTHN_BACKEND}).
-define(AUTHN_TYPE_SCRAM, {?AUTHN_MECHANISM_SCRAM, ?AUTHN_BACKEND}).

-define(AUTHN_DATA_FIELDS, [is_superuser, client_attrs, expire_at, acl]).

-endif.
