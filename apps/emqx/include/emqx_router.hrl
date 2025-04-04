%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_ROUTER_HRL).
-define(EMQX_ROUTER_HRL, true).

%% ETS tables for message routing
-define(ROUTE_TAB, emqx_route).
-define(ROUTE_TAB_FILTERS, emqx_route_filters).

%% Mnesia table for message routing
-define(ROUTING_NODE, emqx_routing_node).

%% ETS tables for PubSub
-define(SUBOPTION, emqx_suboption).
-define(SUBSCRIBER, emqx_subscriber).
-define(SUBSCRIPTION, emqx_subscription).

-endif.
