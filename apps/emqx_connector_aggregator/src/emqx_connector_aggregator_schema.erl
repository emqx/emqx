%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_aggregator_schema).

-behaviour(hocon_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% API
-export([
    container/0,
    container/1
]).

%% `hocon_schema' API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

container() ->
    container(_Opts = #{}).

container(Opts) ->
    AllTypes = [<<"csv">>, <<"json_lines">>, <<"parquet">>],
    SupportedTypes = maps:get(supported_types, Opts, AllTypes),
    %% Assert
    true = SupportedTypes =/= [],
    Default = maps:get(default, Opts, <<"csv">>),
    %% Assert
    true = lists:member(Default, SupportedTypes),
    Selector0 = #{
        <<"csv">> => ref(container_csv),
        <<"json_lines">> => ref(container_json_lines),
        <<"parquet">> => ref(container_parquet)
    },
    Selector = maps:with(SupportedTypes, Selector0),
    MetaOverrides = maps:get(meta, Opts, #{}),
    {container,
        hoconsc:mk(
            emqx_schema:mkunion(type, Selector, Default),
            emqx_utils_maps:deep_merge(
                #{
                    required => true,
                    default => #{<<"type">> => Default},
                    desc => ?DESC("container")
                },
                MetaOverrides
            )
        )}.

%%------------------------------------------------------------------------------
%% `hocon_schema' API
%%------------------------------------------------------------------------------

namespace() -> "connector_aggregator".

roots() -> [].

fields(container_csv) ->
    [
        {type,
            mk(
                csv,
                #{
                    required => true,
                    desc => ?DESC("container_csv")
                }
            )},
        {column_order,
            mk(
                hoconsc:array(string()),
                #{
                    required => false,
                    default => [],
                    desc => ?DESC("container_csv_column_order")
                }
            )}
    ];
fields(container_json_lines) ->
    [
        {type,
            mk(
                json_lines,
                #{
                    required => true,
                    desc => ?DESC("container_json_lines")
                }
            )}
    ];
fields(container_parquet) ->
    [
        {type,
            mk(
                parquet,
                #{
                    required => true,
                    desc => ?DESC("container_parquet")
                }
            )},
        {schema,
            mk(
                emqx_schema:mkunion(
                    type,
                    #{
                        <<"avro_inline">> => ref(parquet_schema_avro_inline),
                        <<"avro_ref">> => ref(parquet_schema_avro_ref)
                    }
                ),
                #{
                    required => true,
                    desc => ?DESC("parquet_schema")
                }
            )},
        {write_old_list_structure,
            mk(boolean(), #{default => false, importance => ?IMPORTANCE_HIDDEN})},
        {enable_dictionary, mk(boolean(), #{default => false, importance => ?IMPORTANCE_HIDDEN})},
        {default_compression,
            mk(
                hoconsc:enum([none, zstd, snappy]),
                #{default => snappy, desc => ?DESC("parquet_default_compression")}
            )},
        {data_page_header_version,
            mk(
                hoconsc:enum([1, 2]),
                #{default => 2, importance => ?IMPORTANCE_HIDDEN}
            )},
        {max_row_group_bytes,
            mk(
                emqx_schema:bytesize(),
                #{default => <<"128MB">>, desc => ?DESC("container_parquet_max_row_group_bytes")}
            )}
    ];
fields(parquet_schema_avro_inline) ->
    [
        {type,
            mk(avro_inline, #{required => true, desc => ?DESC("parquet_schema_avro_inline_type")})},
        {def,
            mk(binary(), #{
                required => true,
                validator => fun parquet_avro_schema_validator/1,
                desc => ?DESC("parquet_schema_avro_inline_def")
            })}
    ];
fields(parquet_schema_avro_ref) ->
    [
        {type, mk(avro_ref, #{required => true, desc => ?DESC("parquet_schema_avro_ref_type")})},
        {name, mk(binary(), #{required => true, desc => ?DESC("parquet_schema_avro_ref_name")})}
    ].

desc(Name) when
    Name == container_csv;
    Name == container_json_lines;
    Name == container_parquet;
    Name == parquet_schema_avro_ref;
    Name == parquet_schema_avro_inline
->
    ?DESC(Name);
desc(_Name) ->
    undefined.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Name) -> hoconsc:ref(?MODULE, Name).

parquet_avro_schema_validator(AvroScBin) ->
    case emqx_utils_json:safe_decode(AvroScBin) of
        {error, _} ->
            {error, <<"invalid avro schema json">>};
        {ok, AvroSc} ->
            try
                _ = parquer_schema_avro:from_avro(AvroSc),
                ok
            catch
                throw:Context ->
                    {error, Context}
            end
    end.
