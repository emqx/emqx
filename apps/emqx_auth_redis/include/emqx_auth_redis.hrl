%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_AUTH_REDIS_HRL).
-define(EMQX_AUTH_REDIS_HRL, true).

-define(AUTHZ_TYPE, redis).
-define(AUTHZ_TYPE_BIN, <<"redis">>).

-define(AUTHN_MECHANISM, password_based).
-define(AUTHN_MECHANISM_BIN, <<"password_based">>).

-define(AUTHN_BACKEND, redis).
-define(AUTHN_BACKEND_BIN, <<"redis">>).

-define(AUTHN_TYPE, {?AUTHN_MECHANISM, ?AUTHN_BACKEND}).

-endif.
