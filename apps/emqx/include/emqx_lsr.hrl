%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_LSR_HRL).
-define(EMQX_LSR_HRL, true).

-define(lsr_err_max_retries, lsr_err_max_retries).
-define(lsr_err_channel_outdated, lsr_err_channel_outdated).
-define(lsr_err_restart_takeover, lsr_err_restart_takeover).

-record(lsr_channel, {
    id :: emqx_types:clientid(),
    % '$1' for match spec
    pid :: pid() | '$1',
    % '$2' for match spec
    vsn :: integer() | undefined | '$2'
}).

%% EMQX_LSR_HRL
-endif.
