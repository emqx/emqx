%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_MQ_QUOTA_HRL).
-define(EMQX_MQ_QUOTA_HRL, true).

%% A message was written. Bytes and count are added to the index.
%% The legacy index places it by publication time.
%% The serial index places it by the DS serial of the write transaction.
-record(quota_add, {
    pub_ts_us :: non_neg_integer(),
    serial :: pos_integer(),
    bytes :: non_neg_integer(),
    count :: non_neg_integer()
}).

%% A message was replaced. Bytes and count are subtracted from the index.
%% The legacy index finds its bucket by publication time.
%% The serial index finds its bucket by the DS timestamp of the stored message.
-record(quota_del, {
    pub_ts_us :: non_neg_integer(),
    ds_ts_us :: non_neg_integer(),
    bytes :: non_neg_integer(),
    count :: non_neg_integer()
}).

-endif.
