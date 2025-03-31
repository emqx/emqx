%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_RESOURCE_RUNTIME_HRL).
-define(EMQX_RESOURCE_RUNTIME_HRL, true).

-include("emqx_resource.hrl").

-define(NO_CHANNEL, no_channel).

%% status and error, cached in ets for each connector
-type st_err() :: #{
    status := resource_status(),
    error := term()
}.

%% the relatively stable part to be cached in persistent_term for each connector
-type cb() :: #{
    mod := module(),
    callback_mode := callback_mode(),
    query_mode := query_mode(),
    state := term()
}.

%% the rutime context to be used for each channel
-record(rt, {
    st_err :: st_err(),
    cb :: cb(),
    query_mode :: emqx_resource:resource_query_mode(),
    channel_status :: ?NO_CHANNEL | channel_status()
}).

-type runtime() :: #rt{}.

-endif.
