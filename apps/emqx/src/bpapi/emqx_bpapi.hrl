%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_BPAPI_HRL).
-define(EMQX_BPAPI_HRL, true).

-define(TAB, bpapi).

-define(multicall, multicall).

-record(?TAB, {
    %% {node() | ?multicall, emqx_bpapi:api()},
    key,
    %% emqx_bpapi:api_version()
    version
}).

-endif.
