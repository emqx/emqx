%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_BRIDGE_RESOURCE_HRL).
-define(EMQX_BRIDGE_RESOURCE_HRL, true).

-define(BRIDGE_HOOKPOINT(BridgeId), <<"$bridges/", BridgeId/binary>>).
-define(SOURCE_HOOKPOINT(BridgeId), <<"$sources/", BridgeId/binary>>).

-endif.
