%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_FT_STORAGE_FS_HRL).
-define(EMQX_FT_STORAGE_FS_HRL, true).

-record(gcstats, {
    started_at :: integer(),
    finished_at :: integer() | undefined,
    files = 0 :: non_neg_integer(),
    directories = 0 :: non_neg_integer(),
    space = 0 :: non_neg_integer(),
    errors = #{} :: #{_GCSubject => {error, _}}
}).

-endif.
