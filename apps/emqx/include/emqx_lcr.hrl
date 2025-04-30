%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_LCR_HRL).
-define(EMQX_LCR_HRL, true).

-define(lcr_err_max_retries, lcr_err_max_retries).
-define(lcr_err_channel_outdated, lcr_err_channel_outdated).
-define(lcr_err_restart_takeover, lcr_err_restart_takeover).

-record(lcr_channel, {
    id :: lcr_channel_id(),
    pid :: pid() | undefined | '_',
    vsn :: integer() | undefined | '$2'
}).

-type lcr_channel_id() :: emqx_types:clientid().

%% EMQX_LCR_HRL
-endif.
