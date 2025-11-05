%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_STREAMS_INTERNAL_HRL).
-define(EMQX_STREAMS_INTERNAL_HRL, true).

-include_lib("snabbkaffe/include/trace.hrl").

-define(tp_debug(KIND, EVENT), ?tp_ignore_side_effects_in_prod(KIND, EVENT)).

-define(SCHEMA_ROOT, streams).

-define(DEFAULT_STREAM_LIMITS,
    (#{
        max_shard_message_count => infinity,
        max_shard_message_bytes => infinity
    })
).

-endif.
