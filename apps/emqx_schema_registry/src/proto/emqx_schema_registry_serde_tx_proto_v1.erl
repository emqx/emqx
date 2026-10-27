%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Transaction funs of emqx_schema_registry_serde. A replicant passes
%% them to mria by name and a core node runs them, see
%% apps/emqx_bpapi/README.md.
-module(emqx_schema_registry_serde_tx_proto_v1).

-behaviour(emqx_bpapi).

-include("emqx_schema_registry.hrl").
-include_lib("emqx/include/bpapi.hrl").

-export([introduced_in/0]).

-export([
    lazy_generate_protobuf_code/3,
    destroy_protobuf_code/1
]).

introduced_in() ->
    "6.3.3".

-spec lazy_generate_protobuf_code(schema_name(), module(), schema_source()) ->
    {ok, module(), binary()} | {error, #{error := term(), warnings := [term()]}}.
lazy_generate_protobuf_code(Name, SerdeMod0, Source) ->
    emqx_schema_registry_serde:lazy_generate_protobuf_code_trans(Name, SerdeMod0, Source).

-spec destroy_protobuf_code({schema_name(), binary()}) -> ok.
destroy_protobuf_code(CacheKey) ->
    emqx_schema_registry_serde:destroy_protobuf_code_trans(CacheKey).
