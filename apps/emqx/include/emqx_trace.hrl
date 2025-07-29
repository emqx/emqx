%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_TRACE_HRL).
-define(EMQX_TRACE_HRL, true).

-define(TRACE, emqx_trace).

-include_lib("emqx/include/emqx_config.hrl").

-type trace_extra() :: #{
    formatter => text | json,
    payload_limit => integer(),
    any() => term()
}.

-record(?TRACE, {
    name :: binary() | undefined | '_',
    type :: clientid | topic | ip_address | ruleid | undefined | '_',
    filter ::
        emqx_types:topic()
        | emqx_types:clientid()
        | emqx_trace:ip_address()
        | {?global_ns | binary(), emqx_trace:ruleid()}
        | undefined
        | '_',
    enable = true :: boolean() | '_',
    payload_encode = text :: hex | text | hidden | '_',
    extra = #{formatter => text} :: trace_extra() | '_',
    start_at :: integer() | undefined | '_',
    end_at :: integer() | undefined | '_'
}).

-record(emqx_trace_format_func_data, {
    function :: fun((any()) -> any()),
    data :: any()
}).

-define(SHARD, ?COMMON_SHARD).
-define(MAX_SIZE, 30).

-define(EMQX_TRACE_STOP_ACTION(REASON),
    {unrecoverable_error, {action_stopped_after_template_rendering, REASON}}
).

-define(EMQX_TRACE_STOP_ACTION_MATCH, ?EMQX_TRACE_STOP_ACTION(_)).

-define(MAX_PAYLOAD_FORMAT_SIZE, 1024).
-define(TRUNCATED_PAYLOAD_SIZE, 100).

-define(FORMAT_META_KEY_PACKET, packet).
-define(FORMAT_META_KEY_PAYLOAD, payload).
-define(FORMAT_META_KEY_PAYLOAD_BIN, <<"payload">>).
-define(FORMAT_META_KEY_INPUT, input).
-define(FORMAT_META_KEY_RESULT, result).

%% Bridges SLOG tracing meta key
%% data (rabbitmq, tablestore)
%% sql (clickhouse)
%% query (dynamodb, rocketmq, sqlserver, syskeeper)
%% send_message (elasticsearch)
%% requests (gcp_pubsub)
%% points (greptimedb, influxdb)
%% request (kinesis)
%% commands (redis)
%% batch_data_list (tablestore)
-define(FORMAT_META_KEY_DATA, data).
-define(FORMAT_META_KEY_SQL, sql).
-define(FORMAT_META_KEY_QUERY, query).
-define(FORMAT_META_KEY_SEND_MESSAGE, send_message).
-define(FORMAT_META_KEY_REQUESTS, requests).
-define(FORMAT_META_KEY_POINTS, points).
-define(FORMAT_META_KEY_REQUEST, request).
-define(FORMAT_META_KEY_COMMANDS, commands).
-define(FORMAT_META_KEY_BATCH_DATA_LIST, batch_data_list).

-define(TRUNCATED_IOLIST(PART, TRUNCATEDBYTES), [
    PART, "...(", integer_to_list(TRUNCATEDBYTES), " bytes)"
]).

-endif.
