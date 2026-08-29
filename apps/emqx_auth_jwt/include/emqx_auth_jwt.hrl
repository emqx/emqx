%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_AUTH_JWT_HRL).
-define(EMQX_AUTH_JWT_HRL, true).

-define(AUTHN_MECHANISM, jwt).
-define(AUTHN_MECHANISM_BIN, <<"jwt">>).
-define(AUTHN_TYPE, ?AUTHN_MECHANISM).

%% MQTT 5.0 `Authentication Method' this authenticator answers to. The value is
%% a contract between client and broker, fixed at CONNECT by [MQTT-4.12.0-1].
-define(MQTT_AUTHN_METHOD_BIN, <<"JWT">>).

-endif.
