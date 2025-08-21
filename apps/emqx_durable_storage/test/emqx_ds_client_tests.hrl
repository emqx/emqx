%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_DS_CLIENT_TESTS_HRL).
-define(EMQX_DS_CLIENT_TESTS_HRL, true).

-define(sub_handle(REF), {h, REF}).
-define(sub_ref(HANDLE), {s, HANDLE}).

-define(err_get_streams, err_get_streams).
-define(err_make_iterator, err_make_iterator).
-define(err_subscribe, err_subscribe).
-define(err_publish, err_publish).

-record(fake_stream, {shard, gen, id}).
-record(fake_iter, {stream, time}).

-define(DB, test_db).

-endif.
