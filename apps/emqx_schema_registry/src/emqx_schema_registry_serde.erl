%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_serde).

-feature(maybe_expr, enable).

-behaviour(emqx_rule_funcs).

-include("emqx_schema_registry.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    make_serde/3,
    handle_rule_function/2,
    schema_check/3,
    is_existing_type/1,
    is_existing_type/2,
    destroy/1
]).

%% Tests
-export([
    decode/2,
    decode/3,
    encode/2,
    encode/3,
    eval_decode/2,
    eval_encode/2
]).

%%------------------------------------------------------------------------------
%% Type definitions
%%------------------------------------------------------------------------------

-define(BOOL(SerdeName, EXPR),
    try
        _ = EXPR,
        true
    catch
        error:Reason ->
            ?SLOG(debug, #{msg => "schema_check_failed", schema => SerdeName, reason => Reason}),
            false
    end
).

-type eval_context() :: term().

-type fingerprint() :: binary().

-type otp_release() :: string().

-type protobuf_cache_key() :: {schema_name(), otp_release(), fingerprint()}.

-export_type([serde_type/0]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec is_existing_type(schema_name()) -> boolean().
is_existing_type(SchemaName) ->
    is_existing_type(SchemaName, []).

-spec is_existing_type(schema_name(), [binary()]) -> boolean().
is_existing_type(SchemaName, Path) ->
    maybe
        {ok, #serde{type = SerdeType, eval_context = EvalContext}} ?=
            emqx_schema_registry:get_serde(SchemaName),
        has_inner_type(SerdeType, EvalContext, Path)
    else
        _ -> false
    end.

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
handle_rule_function(schema_decode, [SchemaId, Data | MoreArgs]) ->
    decode(SchemaId, Data, MoreArgs);
handle_rule_function(schema_decode, Args) ->
    error({args_count_error, {schema_decode, Args}});
handle_rule_function(schema_encode, [SchemaId, Term | MoreArgs]) ->
    %% encode outputs iolists, but when the rule actions process those
    %% it might wrongly encode them as JSON lists, so we force them to
    %% binaries here.
    IOList = encode(SchemaId, Term, MoreArgs),
    iolist_to_binary(IOList);
handle_rule_function(schema_encode, Args) ->
    error({args_count_error, {schema_encode, Args}});
handle_rule_function(schema_check, [SchemaId, Data | MoreArgs]) ->
    schema_check(SchemaId, Data, MoreArgs);
handle_rule_function(avro_encode, [RegistryName, Data | Args]) ->
    case emqx_schema_registry_external:encode(RegistryName, Data, Args, #{tag => false}) of
        {ok, Encoded} ->
            Encoded;
        {error, Reason} ->
            error(Reason)
    end;
handle_rule_function(avro_decode, [RegistryName, Data | Args]) ->
    case emqx_schema_registry_external:decode(RegistryName, Data, Args, _Opts = #{}) of
        {ok, Decoded} ->
            Decoded;
        {error, Reason} ->
            error(Reason)
    end;
handle_rule_function(schema_encode_and_tag, [OurSchemaName, RegistryName, Data | Args]) ->
    case handle_schema_encode_and_tag(OurSchemaName, RegistryName, Data, Args) of
        {ok, Encoded} ->
            Encoded;
        {error, Reason} ->
            error(Reason)
    end;
handle_rule_function(schema_decode_tagged, [RegistryName, Data | Args]) ->
    case emqx_schema_registry_external:decode(RegistryName, Data, Args, _Opts = #{}) of
        {ok, Decoded} ->
            Decoded;
        {error, Reason} ->
            error(Reason)
    end;
handle_rule_function(_, _) ->
    {error, no_match_for_function}.

-spec schema_check(schema_name(), decoded_data() | encoded_data(), [term()]) -> decoded_data().
schema_check(SerdeName, Data, VarArgs) when is_list(VarArgs), is_binary(Data) ->
    with_serde(
        SerdeName,
        fun(Serde) ->
            ?BOOL(SerdeName, eval_decode(Serde, [Data | VarArgs]))
        end
    );
schema_check(SerdeName, Data, VarArgs) when is_list(VarArgs), is_map(Data) ->
    with_serde(
        SerdeName,
        fun(Serde) ->
            ?BOOL(SerdeName, eval_encode(Serde, [Data | VarArgs]))
        end
    ).

-spec decode(schema_name(), encoded_data()) -> decoded_data().
decode(SerdeName, RawData) ->
    decode(SerdeName, RawData, []).

-spec decode(schema_name(), encoded_data(), [term()]) -> decoded_data().
decode(SerdeName, RawData, VarArgs) when is_list(VarArgs) ->
    with_serde(SerdeName, fun(Serde) ->
        eval_decode(Serde, [RawData | VarArgs])
    end).

-spec encode(schema_name(), decoded_data()) -> encoded_data().
encode(SerdeName, RawData) ->
    encode(SerdeName, RawData, []).

-spec encode(schema_name(), decoded_data(), [term()]) -> encoded_data().
encode(SerdeName, Data, VarArgs) when is_list(VarArgs) ->
    with_serde(
        SerdeName,
        fun(Serde) ->
            eval_encode(Serde, [Data | VarArgs])
        end
    ).

with_serde(Name, F) ->
    case emqx_schema_registry:get_serde(Name) of
        {ok, Serde} ->
            Meta =
                case logger:get_process_metadata() of
                    undefined -> #{};
                    Meta0 -> Meta0
                end,
            logger:update_process_metadata(#{schema_name => Name}),
            try
                F(Serde)
            after
                logger:set_process_metadata(Meta)
            end;
        {error, not_found} ->
            error({serde_not_found, Name})
    end.

-spec make_serde(serde_type(), schema_name(), schema_source()) -> serde().
make_serde(avro, Name, Source) ->
    Store0 = avro_schema_store:new([map]),
    %% import the schema into the map store with an assigned name
    %% if it's a named schema (e.g. struct), then Name is added as alias
    Store = avro_schema_store:import_schema_json(Name, Source, Store0),
    #serde{
        name = Name,
        type = avro,
        eval_context = Store
    };
make_serde(protobuf, Name, Source) ->
    {CacheKey, SerdeMod} = make_protobuf_serde_mod(Name, Source),
    #serde{
        name = Name,
        type = protobuf,
        eval_context = SerdeMod,
        extra = #{cache_key => CacheKey}
    };
make_serde(json, Name, Source) ->
    case json_decode(Source) of
        SchemaObj when is_map(SchemaObj) ->
            %% jesse:add_schema adds any map() without further validation
            %% if it's not a map, then case_clause
            ok = jesse_add_schema(Name, SchemaObj),
            #serde{name = Name, type = json};
        _NotMap ->
            error({invalid_json_schema, bad_schema_object})
    end.

eval_decode(#serde{type = avro, name = Name, eval_context = Store}, [Data]) ->
    Opts = avro:make_decoder_options([{map_type, map}, {record_type, map}]),
    avro_binary_decoder:decode(Data, Name, Store, Opts);
eval_decode(#serde{type = protobuf}, [#{} = DecodedData, MessageType]) ->
    %% Already decoded, so it's an user error.
    throw(
        {schema_decode_error, #{
            error_type => decoding_failure,
            data => DecodedData,
            message_type => MessageType,
            explain =>
                <<
                    "Attempted to schema decode an already decoded message."
                    " Check your rules or transformation pipeline."
                >>
        }}
    );
eval_decode(#serde{type = protobuf, eval_context = SerdeMod}, [EncodedData, MessageType0]) ->
    MessageType = binary_to_existing_atom(MessageType0, utf8),
    try
        Decoded = apply(SerdeMod, decode_msg, [EncodedData, MessageType]),
        emqx_utils_maps:binary_key_map(Decoded)
    catch
        error:{gpb_error, {decoding_failure, {_Data, _Schema, {error, function_clause, _Stack}}}} ->
            #{schema_name := SchemaName} = logger:get_process_metadata(),
            throw(
                {schema_decode_error, #{
                    error_type => decoding_failure,
                    data => EncodedData,
                    message_type => MessageType,
                    schema_name => SchemaName,
                    explain =>
                        <<"The given data could not be decoded. Please check the input data and the schema.">>
                }}
            )
    end;
eval_decode(#serde{type = json, name = Name}, [Data]) ->
    true = is_binary(Data),
    Term = json_decode(Data),
    {ok, NewTerm} = jesse_validate(Name, Term),
    NewTerm.

eval_encode(#serde{type = avro, name = Name, eval_context = Store}, [Data]) ->
    avro_binary_encoder:encode(Store, Name, Data);
eval_encode(#serde{type = protobuf, eval_context = SerdeMod}, [DecodedData0, MessageName0]) ->
    DecodedData = emqx_utils_maps:safe_atom_key_map(DecodedData0),
    MessageName = binary_to_existing_atom(MessageName0, utf8),
    apply(SerdeMod, encode_msg, [DecodedData, MessageName]);
eval_encode(#serde{type = json, name = Name}, [Map]) ->
    %% The input Map may not be a valid JSON term for jesse
    Data = iolist_to_binary(emqx_utils_json:encode(Map)),
    NewMap = json_decode(Data),
    case jesse_validate(Name, NewMap) of
        {ok, _} ->
            Data;
        {error, Reason} ->
            error(Reason)
    end.

destroy(#serde{type = avro, name = _Name}) ->
    ?tp(serde_destroyed, #{type => avro, name => _Name}),
    ok;
destroy(#serde{type = protobuf, name = _Name, eval_context = SerdeMod} = Serde) ->
    unload_code(SerdeMod),
    destroy_protobuf_code(Serde),
    ?tp(serde_destroyed, #{type => protobuf, name => _Name}),
    ok;
destroy(#serde{type = json, name = Name}) ->
    ok = jesse_del_schema(Name),
    ?tp(serde_destroyed, #{type => json, name => Name}),
    ok.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

json_decode(Data) ->
    emqx_utils_json:decode(Data, [return_maps]).

jesse_add_schema(Name, Obj) ->
    jesse:add_schema(jesse_name(Name), Obj).

jesse_del_schema(Name) ->
    jesse:del_schema(jesse_name(Name)).

jesse_validate(Name, Map) ->
    jesse:validate(jesse_name(Name), Map, []).

jesse_name(Str) ->
    unicode:characters_to_list(Str).

-spec make_protobuf_serde_mod(schema_name(), schema_source()) -> {protobuf_cache_key(), module()}.
make_protobuf_serde_mod(Name, Source) ->
    {SerdeMod0, SerdeModFileName} = protobuf_serde_mod_name(Name),
    case lazy_generate_protobuf_code(Name, SerdeMod0, Source) of
        {ok, SerdeMod, ModBinary} ->
            load_code(SerdeMod, SerdeModFileName, ModBinary),
            CacheKey = protobuf_cache_key(Name, Source),
            {CacheKey, SerdeMod};
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

%% Fixme: we cannot uncomment the following typespec because Dialyzer complains that
%% `Source' should be `string()' due to `gpb_compile:string/3', but it does work fine with
%% binaries...
%% -spec protobuf_cache_key(schema_name(), schema_source()) -> {schema_name(), fingerprint()}.
protobuf_cache_key(Name, Source) ->
    {Name, erlang:system_info(otp_release), erlang:md5(Source)}.

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
    CacheKey = protobuf_cache_key(Name, Source),
    _ = mnesia:lock({record, ?PROTOBUF_CACHE_TAB, CacheKey}, write),
    case mnesia:read(?PROTOBUF_CACHE_TAB, CacheKey) of
        [#protobuf_cache{module = SerdeMod, module_binary = ModBinary}] ->
            ?tp(schema_registry_protobuf_cache_hit, #{name => Name}),
            {ok, SerdeMod, ModBinary};
        [] ->
            ?tp(schema_registry_protobuf_cache_miss, #{name => Name}),
            case generate_protobuf_code(SerdeMod0, Source) of
                {ok, SerdeMod, ModBinary} ->
                    CacheEntry = #protobuf_cache{
                        fingerprint = CacheKey,
                        module = SerdeMod,
                        module_binary = ModBinary
                    },
                    ok = mnesia:write(?PROTOBUF_CACHE_TAB, CacheEntry, write),
                    {ok, SerdeMod, ModBinary};
                {ok, SerdeMod, ModBinary, _Warnings} ->
                    CacheEntry = #protobuf_cache{
                        fingerprint = CacheKey,
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

-spec destroy_protobuf_code(serde()) -> ok.
destroy_protobuf_code(Serde) ->
    #serde{extra = #{cache_key := CacheKey}} = Serde,
    {atomic, Res} = mria:transaction(
        ?SCHEMA_REGISTRY_SHARD,
        fun destroy_protobuf_code_trans/1,
        [CacheKey]
    ),
    ?tp("schema_registry_protobuf_cache_destroyed", #{name => Serde#serde.name}),
    Res.

-spec destroy_protobuf_code_trans({schema_name(), fingerprint()}) -> ok.
destroy_protobuf_code_trans(CacheKey) ->
    mnesia:delete(?PROTOBUF_CACHE_TAB, CacheKey, write).

-spec has_inner_type(serde_type(), eval_context(), [binary()]) ->
    boolean().
has_inner_type(protobuf, _SerdeMod, [_, _ | _]) ->
    %% Protobuf only has one level of message types.
    false;
has_inner_type(protobuf, SerdeMod, [MessageTypeBin]) ->
    try apply(SerdeMod, get_msg_names, []) of
        Names ->
            lists:member(MessageTypeBin, [atom_to_binary(N, utf8) || N <- Names])
    catch
        _:_ ->
            false
    end;
has_inner_type(_SerdeType, _EvalContext, []) ->
    %% This function is only called if we already found a serde, so the root does exist.
    true;
has_inner_type(_SerdeType, _EvalContext, _Path) ->
    false.

handle_schema_encode_and_tag(OurSchemaName, RegistryName, Data, Args) ->
    maybe
        {ok, Schema} ?= emqx_schema_registry:get_schema(OurSchemaName),
        Source = maps:get(source, Schema),
        emqx_schema_registry_external:encode_with(
            RegistryName,
            OurSchemaName,
            Source,
            Data,
            Args,
            #{tag => true}
        )
    end.
