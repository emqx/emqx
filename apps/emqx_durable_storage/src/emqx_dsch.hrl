%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_DSCH_HRL).
-define(EMQX_DSCH_HRL, true).

-include("emqx_ds.hrl").

-define(empty_schema, no_schema).

-define(global_name(SITE), {emqx_dsch_global_name, SITE}).

%% Persistent terms:
-define(dsch_pt_schema, emqx_dsch_schema).
-define(dsch_pt_backends, emqx_dsch_backend_cbms).
-define(dsch_pt_db_runtime(DB), {emqx_dsch_db_runtime, DB}).

-define(with_dsch(DB, VAR, BODY),
    case persistent_term:get(?dsch_pt_db_runtime(DB), undefined) of
        undefined ->
            ?err_rec({database_is_not_open, DB});
        VAR ->
            BODY
    end
).

%% Tracepoints:
-define(tp_pending_spawn_fail, "Failed to start a pending schema task").

-endif.
