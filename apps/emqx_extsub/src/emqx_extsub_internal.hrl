%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_EXTSUB_INTERNAL_HRL).
-define(EMQX_EXTSUB_INTERNAL_HRL, true).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(tp_debug(KIND, EVENT), ?tp_ignore_side_effects_in_prod(KIND, EVENT)).

-record(info_to_extsub, {
    subscriber_ref :: emqx_extsub_types:subscriber_ref(),
    info :: term()
}).

-record(info_extsub_try_deliver, {}).

-define(EXTSUB_HEADER_INFO, extsub).

-define(EXTSUB_DELIVER_RETRY_INTERVAL, 100).
-define(EXTSUB_MAX_UNACKED, 10).

-define(MIN_SUB_DELIVERING, 100).

-endif.
