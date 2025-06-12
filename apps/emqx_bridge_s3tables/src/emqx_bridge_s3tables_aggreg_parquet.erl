%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_s3tables_aggreg_parquet).

-moduledoc """
Parquet container implementation for `emqx_connector_aggregator` (almost).

Does not quite match the API of `emqx_connector_aggregator`, though, because we need to
keep track of extra metadata for each fill.
""".

-behaviour(emqx_bridge_s3tables_aggreg_unpartitioned).

%% Quasi-`emqx_connector_aggreg_container' API
-export([
    new/1,
    fill/2,
    close/1
]).

-export_type([container/0, options/0]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(num_records, num_records).

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

-spec fill([emqx_connector_aggregator:record()], container()) -> {iodata(), map(), container()}.
fill(Records, #parquet{writer = Writer0} = Container0) ->
    NumRecords = length(Records),
    {IOData, Writer1} = parquer_writer:write_many(Writer0, Records),
    WriteMetadata = #{?num_records => NumRecords},
    Container1 = Container0#parquet{writer = Writer1},
    {IOData, WriteMetadata, Container1}.

-spec close(container()) -> iodata().
close(#parquet{writer = Writer}) ->
    {IOData, _} = parquer_writer:close(Writer),
    IOData.
