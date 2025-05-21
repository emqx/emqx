%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_LSR_HRL).
-define(EMQX_LSR_HRL, true).

-define(lsr_err_max_retries, lsr_err_max_retries).
-define(lsr_err_channel_outdated, lsr_err_channel_outdated).
-define(lsr_err_restart_takeover, lsr_err_restart_takeover).

-record(lsr_channel, {
    id :: lsr_session_id(),
    pid :: pid() | '$1',
    vsn :: integer() | undefined | '$2'
}).

-type lsr_session_id() :: emqx_types:clientid().

%% EMQX_LSR_HRL
-endif.
