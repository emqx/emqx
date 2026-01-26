%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-include_lib("snabbkaffe/include/trace.hrl").

-define(tp_debug(MSG, DATA), ?tp_ignore_side_effects_in_prod(MSG, DATA)).

-define(tp_routerepl(MSG, DATA), ?tp("cluster_link_routerepl_" MSG, DATA)).
-define(tp_routerepl(LEVEL, MSG, DATA), ?tp(LEVEL, "cluster_link_routerepl_" MSG, DATA)).
