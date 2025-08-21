%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_MT_HRL_INTERNAL).
-define(EMQX_MT_HRL_INTERNAL, true).

%% mria tables
-define(RECORD_TAB, emqx_mt_record).
-define(COUNTER_TAB, emqx_mt_counter).
-define(NS_TAB, emqx_mt_ns).
-define(CONFIG_TAB, emqx_mt_config).
-define(TOMBSTONE_TAB, emqx_mt_tombstone).

-endif.
