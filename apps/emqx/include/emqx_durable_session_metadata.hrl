%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This header contains definitions of durable session metadata
%% keys, that can be consumed by the external code.
-ifndef(EMQX_DURABLE_SESSION_META_HRL).
-define(EMQX_DURABLE_SESSION_META_HRL, true).

%% Session metadata keys:
-define(created_at, created_at).
-define(last_alive_at, last_alive_at).
-define(node_epoch_id, node_epoch_id).
-define(expiry_interval, expiry_interval).
%% Unique integer used to create unique identities:
-define(last_id, last_id).
%% Connection info (relevent for the dashboard):
-define(peername, peername).
-define(will_message, will_message).
-define(clientinfo, clientinfo).
-define(protocol, protocol).
-define(offline_info, offline_info).

-endif.
