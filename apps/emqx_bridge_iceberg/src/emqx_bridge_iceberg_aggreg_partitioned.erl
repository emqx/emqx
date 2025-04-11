%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_iceberg_aggreg_partitioned).

-behaviour(emqx_connector_aggreg_container).

%% `emqx_connector_aggreg_container' API
-export([
    new/1,
    fill/2,
    close/1
]).

-export_type([container/0, options/0, partition_key/0]).

-include("emqx_bridge_iceberg.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-record(iceberg, {
    inner_container_opts,
    partition_fields,
    partitions = #{}
}).

-opaque container() :: #iceberg{
    inner_container_opts :: map(),
    partition_fields :: [partition_field()],
    partitions :: #{[partition_key()] => avro_container()}
}.

-type avro_container() :: emqx_connector_aggreg_avro:container().

-type options() :: #{
    inner_container_opts := inner_container_opts(),
    partition_fields := [partition_field()]
}.
-type partition_key() :: null | binary() | integer().
-type partition_field() :: emqx_bridge_iceberg_logic:partition_field().
-type partition_out() :: #{[partition_key()] => iodata()}.
-type write_metadata() :: #{[partition_key()] => #{?num_records := pos_integer()}}.

-type inner_container_opts() :: emqx_connector_aggreg_avro:options().

%%------------------------------------------------------------------------------
%% `emqx_connector_aggreg_container' API
%%------------------------------------------------------------------------------

-spec new(options()) -> container().
new(Opts) ->
    #{
        inner_container_opts := InnerContainerOpts,
        partition_fields := PartitionFields
    } = Opts,
    #iceberg{
        inner_container_opts = InnerContainerOpts,
        partition_fields = PartitionFields,
        partitions = #{}
    }.

-spec fill([emqx_connector_aggregator:record()], container()) ->
    {partition_out(), write_metadata(), container()}.
fill(Records, #iceberg{} = Container0) ->
    #iceberg{partition_fields = PartitionFields} = Container0,
    Partitions = maps:groups_from_list(
        fun(R) -> to_partition_keys(R, PartitionFields) end,
        Records
    ),
    fill_partitions(Partitions, Container0).

-spec close(container()) -> {partition_out(), write_metadata()}.
close(#iceberg{partitions = Partitions}) ->
    maps:fold(
        fun(PK, Avro, {AccIO0, AccMeta0}) ->
            {IOData, WriteMetadata} = emqx_connector_aggreg_avro:close(Avro),
            AccIO = AccIO0#{PK => IOData},
            AccMeta = AccMeta0#{PK => WriteMetadata},
            {AccIO, AccMeta}
        end,
        {#{}, #{}},
        Partitions
    ).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

to_partition_keys(Record, PartitionFields) ->
    case emqx_bridge_iceberg_logic:record_to_partition_keys(Record, PartitionFields) of
        {ok, PKs} ->
            PKs;
        {error, Reason} ->
            exit({upload_failed, {data, Reason}})
    end.

fill_partitions(Partitions, Container0) ->
    #iceberg{inner_container_opts = InnerContainerOpts} = Container0,
    maps:fold(
        fun
            (PK, Records, {AccIO0, AccMeta0, #iceberg{partitions = Ps0} = AccCont0}) when
                is_map_key(PK, Ps0)
            ->
                #{?num_records := N0} = WM0 = maps:get(PK, AccMeta0, #{?num_records => 0}),
                #iceberg{partitions = #{PK := Avro0}} = AccCont0,
                {IOData, #{?num_records := N1}, Avro} = emqx_connector_aggreg_avro:fill(
                    Records, Avro0
                ),
                AccIO = maps:update_with(PK, fun(IO0) -> [IO0 | IOData] end, IOData, AccIO0),
                AccMeta = AccMeta0#{PK => WM0#{?num_records := N0 + N1}},
                AccCont = AccCont0#iceberg{partitions = Ps0#{PK := Avro}},
                {AccIO, AccMeta, AccCont};
            (PK, Records, {AccIO0, AccMeta0, #iceberg{partitions = Ps0} = AccCont0}) ->
                %% New partition key
                Avro0 = emqx_connector_aggreg_avro:new(InnerContainerOpts),
                {IOData, #{?num_records := N1}, Avro} = emqx_connector_aggreg_avro:fill(
                    Records, Avro0
                ),
                AccIO = maps:put(PK, IOData, AccIO0),
                AccMeta = maps:put(PK, #{?num_records => N1}, AccMeta0),
                AccCont = AccCont0#iceberg{partitions = Ps0#{PK => Avro}},
                {AccIO, AccMeta, AccCont}
        end,
        {#{}, #{}, Container0},
        Partitions
    ).
