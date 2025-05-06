%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_AUTH_LDAP_HRL).
-define(EMQX_AUTH_LDAP_HRL, true).

-define(AUTHZ_TYPE, ldap).
-define(AUTHZ_TYPE_BIN, <<"ldap">>).

-define(AUTHN_MECHANISM, password_based).
-define(AUTHN_MECHANISM_BIN, <<"password_based">>).

-define(AUTHN_BACKEND, ldap).
-define(AUTHN_BACKEND_BIN, <<"ldap">>).

-define(AUTHN_TYPE, {?AUTHN_MECHANISM, ?AUTHN_BACKEND}).

-endif.
