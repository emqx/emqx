%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Transaction funs of emqx_streams_registry. A replicant passes them to
%% mria by name and a core node runs them, see apps/emqx_bpapi/README.md.
-module(emqx_streams_registry_tx_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([introduced_in/0]).

-export([
    create/4,
    update_index/3
]).

introduced_in() ->
    "6.3.3".

-spec create(binary(), binary(), binary(), tuple()) -> ok | {error, stream_exists}.
create(Name, TopicFilter, Id, Rec) ->
    emqx_streams_registry:create_tx(Name, TopicFilter, Id, Rec).

-spec update_index(term(), binary(), map()) -> {ok, map()} | not_found.
update_index(Key, Id, UpdateFields) ->
    emqx_streams_registry:update_index_tx(Key, Id, UpdateFields).
