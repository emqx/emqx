%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_serde).

-behaviour(emqx_rule_funcs).

-include("emqx_schema_registry.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-elvis([{elvis_style, invalid_dynamic_call, #{ignore => [emqx_schema_registry_serde]}}]).

%% API
-export([
    decode/2,
    decode/3,
    encode/2,
    encode/3,
    make_serde/3,
    handle_rule_function/2
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec handle_rule_function(atom(), list()) -> any() | {error, no_match_for_function}.
handle_rule_function(sparkplug_decode, [Data]) ->
    handle_rule_function(
        schema_decode,
        [?EMQX_SCHEMA_REGISTRY_SPARKPLUGB_SCHEMA_NAME, Data, <<"Payload">>]
    );
handle_rule_function(sparkplug_decode, [Data | MoreArgs]) ->
    handle_rule_function(
        schema_decode,
        [?EMQX_SCHEMA_REGISTRY_SPARKPLUGB_SCHEMA_NAME, Data | MoreArgs]
    );
handle_rule_function(schema_decode, [SchemaId, Data | MoreArgs]) ->
    decode(SchemaId, Data, MoreArgs);
handle_rule_function(schema_decode, Args) ->
    error({args_count_error, {schema_decode, Args}});
handle_rule_function(sparkplug_encode, [Term]) ->
    handle_rule_function(
        schema_encode,
        [?EMQX_SCHEMA_REGISTRY_SPARKPLUGB_SCHEMA_NAME, Term, <<"Payload">>]
    );
handle_rule_function(sparkplug_encode, [Term | MoreArgs]) ->
    handle_rule_function(
        schema_encode,
        [?EMQX_SCHEMA_REGISTRY_SPARKPLUGB_SCHEMA_NAME, Term | MoreArgs]
    );
handle_rule_function(schema_encode, [SchemaId, Term | MoreArgs]) ->
    %% encode outputs iolists, but when the rule actions process those
    %% it might wrongly encode them as JSON lists, so we force them to
    %% binaries here.
    IOList = encode(SchemaId, Term, MoreArgs),
    iolist_to_binary(IOList);
handle_rule_function(schema_encode, Args) ->
    error({args_count_error, {schema_encode, Args}});
handle_rule_function(_, _) ->
    {error, no_match_for_function}.

-spec decode(schema_name(), encoded_data()) -> decoded_data().
decode(SerdeName, RawData) ->
    decode(SerdeName, RawData, []).

-spec decode(schema_name(), encoded_data(), [term()]) -> decoded_data().
decode(SerdeName, RawData, VarArgs) when is_list(VarArgs) ->
    case emqx_schema_registry:get_serde(SerdeName) of
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
    case emqx_schema_registry:get_serde(SerdeName) of
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
    {Serializer, Deserializer, Destructor};
make_serde(protobuf, Name, Source) ->
    SerdeMod = make_protobuf_serde_mod(Name, Source),
    Serializer =
        fun(DecodedData0, MessageName0) ->
            DecodedData = emqx_utils_maps:safe_atom_key_map(DecodedData0),
            MessageName = binary_to_existing_atom(MessageName0, utf8),
            SerdeMod:encode_msg(DecodedData, MessageName)
        end,
    Deserializer =
        fun(EncodedData, MessageName0) ->
            MessageName = binary_to_existing_atom(MessageName0, utf8),
            Decoded = SerdeMod:decode_msg(EncodedData, MessageName),
            emqx_utils_maps:binary_key_map(Decoded)
        end,
    Destructor =
        fun() ->
            unload_code(SerdeMod),
            ?tp(serde_destroyed, #{type => protobuf, name => Name}),
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
    Schema0 = emqx_utils_json:decode(Source0, [return_maps]),
    Schema = Schema0#{<<"name">> => Name},
    emqx_utils_json:encode(Schema).

-spec make_protobuf_serde_mod(schema_name(), schema_source()) -> module().
make_protobuf_serde_mod(Name, Source) ->
    {SerdeMod0, SerdeModFileName} = protobuf_serde_mod_name(Name),
    case lazy_generate_protobuf_code(Name, SerdeMod0, Source) of
        {ok, SerdeMod, ModBinary} ->
            load_code(SerdeMod, SerdeModFileName, ModBinary),
            SerdeMod;
        {error, #{error := Error, warnings := Warnings}} ->
            ?SLOG(
                warning,
                #{
                    msg => "error_generating_protobuf_code",
                    error => Error,
                    warnings => Warnings
                }
            ),
            error({invalid_protobuf_schema, Error})
    end.

-spec protobuf_serde_mod_name(schema_name()) -> {module(), string()}.
protobuf_serde_mod_name(Name) ->
    %% must be a string (list)
    SerdeModName = "$schema_parser_" ++ binary_to_list(Name),
    SerdeMod = list_to_atom(SerdeModName),
    %% the "path" to the module, for `code:load_binary'.
    SerdeModFileName = SerdeModName ++ ".memory",
    {SerdeMod, SerdeModFileName}.

-spec lazy_generate_protobuf_code(schema_name(), module(), schema_source()) ->
    {ok, module(), binary()} | {error, #{error := term(), warnings := [term()]}}.
lazy_generate_protobuf_code(Name, SerdeMod0, Source) ->
    %% We run this inside a transaction with locks to avoid running
    %% the compile on all nodes; only one will get the lock, compile
    %% the schema, and other nodes will simply read the final result.
    {atomic, Res} = mria:transaction(
        ?SCHEMA_REGISTRY_SHARD,
        fun lazy_generate_protobuf_code_trans/3,
        [Name, SerdeMod0, Source]
    ),
    Res.

-spec lazy_generate_protobuf_code_trans(schema_name(), module(), schema_source()) ->
    {ok, module(), binary()} | {error, #{error := term(), warnings := [term()]}}.
lazy_generate_protobuf_code_trans(Name, SerdeMod0, Source) ->
    Fingerprint = erlang:md5(Source),
    _ = mnesia:lock({record, ?PROTOBUF_CACHE_TAB, Fingerprint}, write),
    case mnesia:read(?PROTOBUF_CACHE_TAB, Fingerprint) of
        [#protobuf_cache{module = SerdeMod, module_binary = ModBinary}] ->
            ?tp(schema_registry_protobuf_cache_hit, #{name => Name}),
            {ok, SerdeMod, ModBinary};
        [] ->
            ?tp(schema_registry_protobuf_cache_miss, #{name => Name}),
            case generate_protobuf_code(SerdeMod0, Source) of
                {ok, SerdeMod, ModBinary} ->
                    CacheEntry = #protobuf_cache{
                        fingerprint = Fingerprint,
                        module = SerdeMod,
                        module_binary = ModBinary
                    },
                    ok = mnesia:write(?PROTOBUF_CACHE_TAB, CacheEntry, write),
                    {ok, SerdeMod, ModBinary};
                {ok, SerdeMod, ModBinary, _Warnings} ->
                    CacheEntry = #protobuf_cache{
                        fingerprint = Fingerprint,
                        module = SerdeMod,
                        module_binary = ModBinary
                    },
                    ok = mnesia:write(?PROTOBUF_CACHE_TAB, CacheEntry, write),
                    {ok, SerdeMod, ModBinary};
                error ->
                    {error, #{error => undefined, warnings => []}};
                {error, Error} ->
                    {error, #{error => Error, warnings => []}};
                {error, Error, Warnings} ->
                    {error, #{error => Error, warnings => Warnings}}
            end
    end.

generate_protobuf_code(SerdeMod, Source) ->
    gpb_compile:string(
        SerdeMod,
        Source,
        [
            binary,
            strings_as_binaries,
            {maps, true},
            %% Fixme: currently, some bug in `gpb' prevents this
            %% option from working with `oneof' types...  We're then
            %% forced to use atom key maps.
            %% {maps_key_type, binary},
            {maps_oneof, flat},
            {verify, always},
            {maps_unset_optional, omitted}
        ]
    ).

-spec load_code(module(), string(), binary()) -> ok.
load_code(SerdeMod, SerdeModFileName, ModBinary) ->
    _ = code:purge(SerdeMod),
    {module, SerdeMod} = code:load_binary(SerdeMod, SerdeModFileName, ModBinary),
    ok.

-spec unload_code(module()) -> ok.
unload_code(SerdeMod) ->
    _ = code:purge(SerdeMod),
    _ = code:delete(SerdeMod),
    ok.
