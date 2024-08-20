%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_AUTH_KERBEROS_HRL).
-define(EMQX_AUTH_KERBEROS_HRL, true).

-define(AUTHN_MECHANISM_GSSAPI, gssapi).
-define(AUTHN_MECHANISM_GSSAPI_BIN, <<"gssapi">>).

-define(AUTHN_BACKEND, kerberos).
-define(AUTHN_BACKEND_BIN, <<"kerberos">>).

-define(AUTHN_TYPE_KERBEROS, {?AUTHN_MECHANISM_GSSAPI, ?AUTHN_BACKEND}).

%% Following non-normative example provided in spec
%% https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901256
-define(AUTHN_METHOD, <<"GS2-KRB5">>).

-define(SERVICE, <<"mqtt">>).

-endif.
