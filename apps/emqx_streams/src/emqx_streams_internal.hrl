%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_STREAMS_INTERNAL_HRL).
-define(EMQX_STREAMS_INTERNAL_HRL, true).

-include_lib("emqx_utils/include/emqx_ds_dbs.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-define(tp_debug(KIND, EVENT), ?tp_ignore_side_effects_in_prod(KIND, EVENT)).

-define(SCHEMA_ROOT, streams).

-define(DEFAULT_STREAM_LIMITS,
    (#{
        max_shard_message_count => infinity,
        max_shard_message_bytes => infinity
    })
).

-define(QUOTA_INDEX_TS, 1).

-define(STREAMS_QUOTA_BUFFER, streams_quota_buffer).

-define(DEFAULT_QUOTA_BUFFER_MAX_SIZE, 100).
-define(DEFAULT_QUOTA_BUFFER_FLUSH_INTERVAL, 1000).
-define(DEFAULT_QUOTA_BUFFER_POOL_SIZE, 10).

-define(STREAMS_MESSAGE_DB_TOPIC(STREAM_TOPIC, STREAM_ID, KEY), [
    <<"topic">>, STREAM_TOPIC, STREAM_ID, <<"key">>, KEY
]).

-record(shard_dispatch_command, {group, c, context}).

-endif.
