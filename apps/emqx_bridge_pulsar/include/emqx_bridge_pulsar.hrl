%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_BRIDGE_PULSAR_HRL).
-define(EMQX_BRIDGE_PULSAR_HRL, true).

-define(PULSAR_HOST_OPTIONS, #{
    default_port => 6650,
    default_scheme => "pulsar",
    supported_schemes => ["pulsar", "pulsar+ssl"]
}).

-endif.
