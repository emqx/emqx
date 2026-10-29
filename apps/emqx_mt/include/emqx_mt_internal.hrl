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

%% Mria table to store various configurations for explicitly created namespaces.
%% They is simply the namespace name (a binary).
%% Currently, we limit the maximum number of configurable namespaces.
-record(?CONFIG_TAB, {
    key :: emqx_mt:tns(),
    configs :: emqx_mt_config:root_config(),
    extra = #{} :: map()
}).

-endif.
