%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_SHARED_SUB_HRL).
-define(EMQX_SHARED_SUB_HRL, true).

%% Mnesia table for shared sub message routing
-define(SHARED_SUBSCRIPTION, emqx_shared_subscription).

%% ETS tables for Shared PubSub
-define(SHARED_SUBSCRIBER, emqx_shared_subscriber).
-define(SHARED_SUBS_ROUND_ROBIN_COUNTER, emqx_shared_subscriber_round_robin_counter).

-endif.
