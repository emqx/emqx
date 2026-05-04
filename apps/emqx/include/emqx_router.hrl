%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_ROUTER_HRL).
-define(EMQX_ROUTER_HRL, true).

%% Mria tables for message routing (v2, regular mria table)
-define(ROUTE_TAB_V2, emqx_route).
-define(ROUTE_TAB_FILTERS_V2, emqx_route_filters).
-define(ROUTING_NODE_V2, emqx_routing_node).
%% Mria tables for message routing (v3, merged mria table)
-define(ROUTE_TAB_V3, emqx_route_m).
-define(ROUTE_TAB_FILTERS_V3, emqx_route_filters_m).

%% ETS tables for PubSub
-define(SUBOPTION, emqx_suboption).
-define(SUBSCRIBER, emqx_subscriber).
-define(SUBSCRIPTION, emqx_subscription).

-endif.
