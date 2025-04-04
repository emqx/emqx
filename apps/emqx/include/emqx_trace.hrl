%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_TRACE_HRL).
-define(EMQX_TRACE_HRL, true).

-define(TRACE, emqx_trace).

-type trace_extra() :: #{formatter => text | json, payload_limit => integer()}.

-record(?TRACE, {
    name :: binary() | undefined | '_',
    type :: clientid | topic | ip_address | ruleid | undefined | '_',
    filter ::
        emqx_types:topic()
        | emqx_types:clientid()
        | emqx_trace:ip_address()
        | emqx_trace:ruleid()
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

-define(DEFAULT_PAYLOAD_LIMIT, 1024).

-endif.
