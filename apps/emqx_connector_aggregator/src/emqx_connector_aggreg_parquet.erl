%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_aggreg_parquet).

-moduledoc """
Parquet container implementation for `emqx_connector_aggreg_container`.
""".

-behaviour(emqx_connector_aggreg_container).

%% `emqx_connector_aggreg_container' API
-export([
    new/1,
    fill/2,
    close/1
]).

-export_type([container/0, options/0]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-record(parquet, {writer}).

-opaque container() :: #parquet{
    writer :: parquer_writer:t()
}.

-type options() :: #{
    schema := map(),
    writer_opts := map()
}.

%%------------------------------------------------------------------------------
%% `emqx_connector_aggreg_container' API
%%------------------------------------------------------------------------------

-spec new(options()) -> container().
new(#{schema := Schema, writer_opts := WriterOpts}) ->
    Writer = parquer_writer:new(Schema, WriterOpts),
    #parquet{writer = Writer}.

-spec fill([emqx_connector_aggregator:record()], container()) -> {iodata(), container()}.
fill(Records0, #parquet{writer = Writer0} = Container0) ->
    Records = lists:map(fun emqx_utils_maps:binary_key_map/1, Records0),
    {IOData, Writer1} = parquer_writer:write_many(Writer0, Records),
    Container = Container0#parquet{writer = Writer1},
    {IOData, Container}.

-spec close(container()) -> iodata().
close(#parquet{writer = Writer}) ->
    {IOData, _} = parquer_writer:close(Writer),
    IOData.
