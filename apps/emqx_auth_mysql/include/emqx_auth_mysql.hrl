%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_AUTH_MYSQL_HRL).
-define(EMQX_AUTH_MYSQL_HRL, true).

-define(AUTHZ_TYPE, mysql).
-define(AUTHZ_TYPE_BIN, <<"mysql">>).

-define(AUTHN_MECHANISM, password_based).
-define(AUTHN_MECHANISM_BIN, <<"password_based">>).

-define(AUTHN_BACKEND, mysql).
-define(AUTHN_BACKEND_BIN, <<"mysql">>).

-define(AUTHN_TYPE, {?AUTHN_MECHANISM, ?AUTHN_BACKEND}).

-endif.
