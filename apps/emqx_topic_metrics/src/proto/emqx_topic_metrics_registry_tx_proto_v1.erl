%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Transaction funs of emqx_topic_metrics_registry. A replicant passes
%% them to mria by name and a core node runs them, see
%% apps/emqx_bpapi/README.md.
-module(emqx_topic_metrics_registry_tx_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([introduced_in/0]).

-export([all_names/0]).

introduced_in() ->
    "6.3.3".

-spec all_names() -> [emqx_topic_metrics_registry:name()].
all_names() ->
    emqx_topic_metrics_registry:all_names_tx().
