%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_s3tables_aggreg_unpartitioned).

-moduledoc """
Format-wrapper container implementation for `emqx_connector_aggregator` (almost).

Does not quite match the API of `emqx_connector_aggregator`, though, because we need to
keep track of extra metadata for each fill.
""".

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

-record(unpartitioned_aggreg, {
    module,
    state
}).

-opaque container() :: #unpartitioned_aggreg{
    module :: module(),
    state :: term()
}.

-type options() :: #{
    type := avro | parquet,
    writer_opts := map()
}.

-type internal_container() :: term().

%%------------------------------------------------------------------------------
%% Callbacks declarations
%%------------------------------------------------------------------------------

-callback new(map()) -> internal_container().

-callback fill([emqx_connector_aggregator:record()], internal_container()) ->
    {iodata(), map(), internal_container()}.

-callback close(internal_container()) -> iodata().

%%------------------------------------------------------------------------------
%% `emqx_connector_aggreg_container' API
%%------------------------------------------------------------------------------

-spec new(options()) -> container().
new(#{type := ContainerType, writer_opts := WriterOpts}) ->
    case ContainerType of
        avro ->
            Module = emqx_bridge_s3tables_aggreg_avro,
            State = Module:new(WriterOpts);
        parquet ->
            Module = emqx_bridge_s3tables_aggreg_parquet,
            State = Module:new(WriterOpts)
    end,
    #unpartitioned_aggreg{module = Module, state = State}.

-spec fill([emqx_connector_aggregator:record()], container()) -> {iodata(), map(), container()}.
fill(Records, #unpartitioned_aggreg{module = Module, state = State0} = Container0) ->
    {IOData, WriteMeta, State1} = Module:fill(Records, State0),
    Container1 = Container0#unpartitioned_aggreg{state = State1},
    {IOData, WriteMeta, Container1}.

-spec close(container()) -> iodata().
close(#unpartitioned_aggreg{module = Module, state = State}) ->
    Module:close(State).
