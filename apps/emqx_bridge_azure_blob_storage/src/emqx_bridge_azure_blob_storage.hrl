%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(__EMQX_BRIDGE_ABS_HRL__).
-define(__EMQX_BRIDGE_ABS_HRL__, true).

-define(CONNECTOR_TYPE, azure_blob_storage).
-define(CONNECTOR_TYPE_BIN, <<"azure_blob_storage">>).

-define(ACTION_TYPE, azure_blob_storage).
-define(ACTION_TYPE_BIN, <<"azure_blob_storage">>).
-define(AGGREG_SUP, emqx_bridge_azure_blob_storage_sup).

%% END ifndef(__EMQX_BRIDGE_ABS_HRL__)
-endif.
