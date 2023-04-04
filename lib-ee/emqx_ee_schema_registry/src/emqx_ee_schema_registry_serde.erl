%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_schema_registry_serde).

-include("emqx_ee_schema_registry.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    decode/2,
    decode/3,
    encode/2,
    encode/3,
    make_serde/3
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec decode(schema_name(), encoded_data()) -> decoded_data().
decode(SerdeName, RawData) ->
    decode(SerdeName, RawData, []).

-spec decode(schema_name(), encoded_data(), [term()]) -> decoded_data().
decode(SerdeName, RawData, VarArgs) when is_list(VarArgs) ->
    case emqx_ee_schema_registry:get_serde(SerdeName) of
        {error, not_found} ->
            error({serde_not_found, SerdeName});
        {ok, #{deserializer := Deserializer}} ->
            apply(Deserializer, [RawData | VarArgs])
    end.

-spec encode(schema_name(), decoded_data()) -> encoded_data().
encode(SerdeName, RawData) ->
    encode(SerdeName, RawData, []).

-spec encode(schema_name(), decoded_data(), [term()]) -> encoded_data().
encode(SerdeName, EncodedData, VarArgs) when is_list(VarArgs) ->
    case emqx_ee_schema_registry:get_serde(SerdeName) of
        {error, not_found} ->
            error({serde_not_found, SerdeName});
        {ok, #{serializer := Serializer}} ->
            apply(Serializer, [EncodedData | VarArgs])
    end.

-spec make_serde(serde_type(), schema_name(), schema_source()) ->
    {serializer(), deserializer(), destructor()}.
make_serde(avro, Name, Source0) ->
    Source = inject_avro_name(Name, Source0),
    Serializer = avro:make_simple_encoder(Source, _Opts = []),
    Deserializer = avro:make_simple_decoder(Source, [{map_type, map}, {record_type, map}]),
    Destructor = fun() ->
        ?tp(serde_destroyed, #{type => avro, name => Name}),
        ok
    end,
    {Serializer, Deserializer, Destructor}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

-spec inject_avro_name(schema_name(), schema_source()) -> schema_source().
inject_avro_name(Name, Source0) ->
    %% The schema checks that the source is a valid JSON when
    %% typechecking, so we shouldn't need to validate here.
    Schema0 = emqx_json:decode(Source0, [return_maps]),
    Schema = Schema0#{<<"name">> => Name},
    emqx_json:encode(Schema).
