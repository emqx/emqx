%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_OMP_PG_HRL).
-define(EMQX_OMP_PG_HRL, true).

%% `{Namespace, TopicId, Offset}` of a message returned to ExtSub.
-define(OMP_POSITION, omp_position).

%% Live copy of a committed message, sent with `emqx_broker:dispatch_info/2`.
-define(OMP_LIVE(Namespace, TopicId, Offset, Msg), {omp_live, Namespace, TopicId, Offset, Msg}).

-endif.
