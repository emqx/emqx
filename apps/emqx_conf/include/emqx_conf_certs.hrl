%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_CONF_CERTS_HRL).
-define(EMQX_CONF_CERTS_HRL, true).

-define(FILE_KIND_KEY, key).
-define(FILE_KIND_CHAIN, chain).
-define(FILE_KIND_CA, ca).
%% ACME account key
-define(FILE_KIND_ACC_KEY, acc_key).
-define(FILE_KIND_KEY_PASSWORD, key_password).

-endif.
