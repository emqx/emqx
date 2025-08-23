%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_DSCH_HRL).
-define(EMQX_DSCH_HRL, true).

-include("emqx_ds.hrl").

%% Persistent terms:
-define(dsch_pt_schema, emqx_dsch_schema).
-define(dsch_pt_db_runtime(DB), {emqx_dsch_db_runtime, DB}).

-define(with_dsch(DB, VAR, BODY),
    case persistent_term:get(?dsch_pt_db_runtime(DB), undefined) of
        undefined ->
            ?err_rec({database_is_not_open, DB});
        VAR ->
            BODY
    end
).

-endif.
