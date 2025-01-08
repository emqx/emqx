%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_AUTH_KERBEROS_HRL).
-define(EMQX_AUTH_KERBEROS_HRL, true).

-define(AUTHN_MECHANISM_GSSAPI, gssapi).
-define(AUTHN_MECHANISM_GSSAPI_BIN, <<"gssapi">>).

-define(AUTHN_BACKEND, kerberos).
-define(AUTHN_BACKEND_BIN, <<"kerberos">>).

-define(AUTHN_TYPE_KERBEROS, {?AUTHN_MECHANISM_GSSAPI, ?AUTHN_BACKEND}).

-define(AUTHN_METHOD, <<"GSSAPI-KERBEROS">>).

-define(SERVICE, <<"mqtt">>).

-endif.
