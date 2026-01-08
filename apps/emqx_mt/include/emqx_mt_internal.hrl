%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_MT_HRL_INTERNAL).
-define(EMQX_MT_HRL_INTERNAL, true).

%% mria tables
-define(RECORD_TAB, emqx_mt_record).
%% Deprecated since 6.0.0
%% -define(COUNTER_TAB, emqx_mt_counter).
%% "OS" here stands for `ordered_set`: the old version of this table used `set`.
%% Introduced in 6.0.0
-define(COUNTER_TAB, emqx_mt_counter2).
-define(NS_TAB, emqx_mt_ns).
-define(CONFIG_TAB, emqx_mt_config).
-define(TOMBSTONE_TAB, emqx_mt_tombstone).

-endif.
