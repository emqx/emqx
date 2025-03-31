%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_AUTH_JWT_HRL).
-define(EMQX_AUTH_JWT_HRL, true).

-define(AUTHZ_TYPE, http).
-define(AUTHZ_TYPE_BIN, <<"http">>).

-define(AUTHN_MECHANISM, jwt).
-define(AUTHN_MECHANISM_BIN, <<"jwt">>).
-define(AUTHN_TYPE, ?AUTHN_MECHANISM).

-endif.
