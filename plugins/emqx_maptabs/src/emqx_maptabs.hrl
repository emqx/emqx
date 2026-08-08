%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% The registry maps table name to its current data-table generation:
%% `{Name, Tid, Meta}'. Owned by emqx_maptabs_server, read directly
%% (no gen_server call) by the emqx_maptabs lookup hot path.
-define(MAPTABS_REGISTRY, emqx_maptabs_registry).
