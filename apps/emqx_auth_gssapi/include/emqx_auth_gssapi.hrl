%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_AUTH_GSSAPI_HRL).
-define(EMQX_AUTH_GSSAPI_HRL, true).

-define(AUTHN_MECHANISM_GSSAPI, gssapi).
-define(AUTHN_MECHANISM_GSSAPI_BIN, <<"gssapi">>).

-define(AUTHN_BACKEND, gssapi).
-define(AUTHN_BACKEND_BIN, <<"gssapi">>).

-define(AUTHN_TYPE_GSSAPI, {?AUTHN_MECHANISM_GSSAPI, ?AUTHN_BACKEND}).

-endif.
