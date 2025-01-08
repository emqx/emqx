%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-record(buffer, {
    since :: emqx_connector_aggregator:timestamp(),
    until :: emqx_connector_aggregator:timestamp(),
    seq :: non_neg_integer(),
    filename :: file:filename(),
    fd :: file:io_device() | undefined,
    max_records :: pos_integer() | undefined,
    cnt_records :: {ets:tab(), _Counter} | undefined
}).

-type buffer() :: #buffer{}.

-type buffer_map() :: #{
    since := emqx_connector_aggregator:timestamp(),
    until := emqx_connector_aggregator:timestamp(),
    seq := non_neg_integer(),
    filename := file:filename(),
    max_records := pos_integer() | undefined
}.
