%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-record(lcr_channel, {
    id :: lcr_channel_id(),
    pid :: pid() | undefined,
    vsn :: integer() | undefined
}).

-type lcr_channel_id() :: emqx_types:clientid().
