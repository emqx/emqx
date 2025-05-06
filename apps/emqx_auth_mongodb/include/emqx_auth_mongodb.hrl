%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_AUTH_MONGODB_HRL).
-define(EMQX_AUTH_MONGODB_HRL, true).

-define(AUTHZ_TYPE, mongodb).
-define(AUTHZ_TYPE_BIN, <<"mongodb">>).

-define(AUTHN_MECHANISM, password_based).
-define(AUTHN_MECHANISM_BIN, <<"password_based">>).

-define(AUTHN_BACKEND, mongodb).
-define(AUTHN_BACKEND_BIN, <<"mongodb">>).

-define(AUTHN_TYPE, {?AUTHN_MECHANISM, ?AUTHN_BACKEND}).

-endif.
