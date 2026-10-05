%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_AUTH_JWT_HRL).
-define(EMQX_AUTH_JWT_HRL, true).

-define(AUTHN_MECHANISM, jwt).
-define(AUTHN_MECHANISM_BIN, <<"jwt">>).
-define(AUTHN_TYPE, ?AUTHN_MECHANISM).

%% Value of the MQTT 5.0 `Authentication Method' property that selects this
%% authenticator. The client sends it in CONNECT and, per [MQTT-4.12.0-1], must
%% repeat the same value in every AUTH packet of the session.
-define(MQTT_AUTHN_METHOD_BIN, <<"JWT">>).

-endif.
