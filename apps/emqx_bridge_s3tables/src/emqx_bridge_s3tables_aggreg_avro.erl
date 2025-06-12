%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_s3tables_aggreg_avro).

-moduledoc """
Avro OCF container implementation for `emqx_connector_aggregator` (almost).

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

-record(avro, {
    schema,
    header,
    encoder,
    root_type
}).

-opaque container() :: #avro{
    schema :: avro:avro_type(),
    header :: undefined | avro_ocf:header(),
    encoder :: avro:encode_fun(),
    root_type :: avro:type_or_name()
}.

-type options() :: #{
    schema := avro:avro_type(),
    root_type := binary()
}.

%%------------------------------------------------------------------------------
%% `emqx_connector_aggreg_container' API
%%------------------------------------------------------------------------------

-spec new(options()) -> container().
new(#{schema := Schema, root_type := RootType}) ->
    Encoder = avro:make_encoder(Schema, [{encoding, avro_binary}]),
    #avro{
        schema = Schema,
        header = undefined,
        encoder = Encoder,
        root_type = RootType
    }.

-spec fill([emqx_connector_aggregator:record()], container()) -> {iodata(), map(), container()}.
fill(Records, #avro{header = undefined} = Container0) ->
    #avro{
        schema = Schema,
        encoder = Encoder,
        root_type = RootType
    } = Container0,
    Header = avro_ocf:make_header(Schema),
    Objs = lists:map(fun(O) -> Encoder(RootType, O) end, Records),
    Block = avro_ocf:make_block(Header, Objs),
    Container = Container0#avro{header = Header},
    NumRecords = length(Records),
    WriteMetadata = #{?num_records => NumRecords},
    BinHeader = avro_ocf:encode_header(Header),
    {[BinHeader, Block], WriteMetadata, Container};
fill(Records, #avro{} = Container) ->
    #avro{
        header = Header,
        encoder = Encoder,
        root_type = RootType
    } = Container,
    Objs = lists:map(fun(O) -> Encoder(RootType, O) end, Records),
    Block = avro_ocf:make_block(Header, Objs),
    NumRecords = length(Records),
    WriteMetadata = #{?num_records => NumRecords},
    {Block, WriteMetadata, Container}.

-spec close(container()) -> iodata().
close(#avro{}) ->
    [].
