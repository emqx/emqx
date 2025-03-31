%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_new_streams_proto_v1).

-behavior(emqx_bpapi).
-include_lib("emqx_utils/include/bpapi.hrl").

%% API:
-export([notify/3, set_dirty/2]).

%% behavior callbacks:
-export([introduced_in/0]).

%%================================================================================
%% API functions
%%================================================================================

-spec notify([node()], emqx_ds:db(), emqx_ds:topic_filter()) -> ok.
notify(Nodes, DB, TopicFilter) ->
    erpc:multicast(Nodes, emqx_ds_new_streams, local_notify_new_stream, [DB, TopicFilter]).

-spec set_dirty([node()], emqx_ds:db()) -> ok.
set_dirty(Nodes, DB) ->
    erpc:multicast(Nodes, emqx_ds_new_streams, local_set_dirty, [DB]).

%%================================================================================
%% behavior callbacks
%%================================================================================

introduced_in() ->
    "5.8.2".
