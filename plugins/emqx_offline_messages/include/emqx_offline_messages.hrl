%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-define(METRICS_WORKER, emqx_offline_messages_metrics_worker).
-define(PLUGIN_NAME, emqx_offline_messages).

%% Do not update version manually, use make bump-version-patch/minor/major instead
-define(PLUGIN_RELEASE_VERSION, "2.0.0").

-define(PLUGIN_NAME_VSN, <<"emqx_offline_messages-", ?PLUGIN_RELEASE_VERSION>>).
