%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% @doc EMQX License Management CLI.
%%--------------------------------------------------------------------

-ifndef(_EMQX_LICENSE_).
-define(_EMQX_LICENSE_, true).

-define(EVALUATION_LOG,
    "\n"
    "========================================================================\n"
    "Using an evaluation license limited to ~p concurrent sessions.\n"
    "This license is for evaluation purposes only and not permitted for production use.\n"
    "Visit https://emqx.com/apply-licenses/emqx?version=5 to apply a new license.\n"
    "Or contact EMQ customer services via email contact@emqx.io\n"
    "========================================================================\n"
).

-define(EXPIRY_LOG,
    "\n"
    "========================================================================\n"
    "License has been expired for ~p days.\n"
    "Visit https://emqx.com/apply-licenses/emqx?version=5 to apply a new license.\n"
    "Or contact EMQ customer services via email contact@emqx.io\n"
    "========================================================================\n"
).

-define(OFFICIAL, 1).
-define(TRIAL, 0).

-define(SMALL_CUSTOMER, 0).
-define(MEDIUM_CUSTOMER, 1).
-define(LARGE_CUSTOMER, 2).
-define(BUSINESS_CRITICAL_CUSTOMER, 3).
-define(BYOC_CUSTOMER, 4).
-define(EVALUATION_CUSTOMER, 10).

-define(EXPIRED_DAY, -90).

-define(ERR_EXPIRED, expired).
-endif.
