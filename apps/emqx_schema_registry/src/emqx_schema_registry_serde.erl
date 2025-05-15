%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_serde).

-feature(maybe_expr, enable).

-include("emqx_schema_registry.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    make_serde/3,
    schema_check/3,
    is_existing_type/1,
    is_existing_type/2,
    destroy/1
]).

%% Rule SQL functions
-export([
    rsf_sparkplug_decode/1,
    rsf_sparkplug_encode/1,
    rsf_schema_decode/1,
    rsf_schema_encode/1,
    rsf_schema_check/1,
    rsf_avro_encode/1,
    rsf_avro_decode/1,
    rsf_schema_encode_and_tag/1,
    rsf_schema_decode_tagged/1
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

%% Internal exports for `emqx_schema_registry_config`.
-export([protobuf_resolve_imports/1]).

-elvis([{elvis_style, no_match_in_condition, disable}]).

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
%% Rule SQL functions
%%------------------------------------------------------------------------------

rsf_sparkplug_decode([Data]) ->
    rsf_schema_decode(
        [?EMQX_SCHEMA_REGISTRY_SPARKPLUGB_SCHEMA_NAME, Data, <<"Payload">>]
    );
rsf_sparkplug_decode([Data | MoreArgs]) ->
    rsf_schema_decode(
        [?EMQX_SCHEMA_REGISTRY_SPARKPLUGB_SCHEMA_NAME, Data | MoreArgs]
    ).

rsf_sparkplug_encode([Term]) ->
    rsf_schema_encode(
        [?EMQX_SCHEMA_REGISTRY_SPARKPLUGB_SCHEMA_NAME, Term, <<"Payload">>]
    );
rsf_sparkplug_encode([Term | MoreArgs]) ->
    rsf_schema_encode(
        [?EMQX_SCHEMA_REGISTRY_SPARKPLUGB_SCHEMA_NAME, Term | MoreArgs]
    ).

rsf_schema_decode([SchemaId, Data | MoreArgs]) ->
    decode(SchemaId, Data, MoreArgs);
rsf_schema_decode(Args) ->
    error({args_count_error, {schema_decode, Args}}).

rsf_schema_encode([SchemaId, Term | MoreArgs]) ->
    %% encode outputs iolists, but when the rule actions process those
    %% it might wrongly encode them as JSON lists, so we force them to
    %% binaries here.
    IOList = encode(SchemaId, Term, MoreArgs),
    iolist_to_binary(IOList);
rsf_schema_encode(Args) ->
    error({args_count_error, {schema_encode, Args}}).

rsf_schema_check([SchemaId, Data | MoreArgs]) ->
    schema_check(SchemaId, Data, MoreArgs).

rsf_avro_encode([RegistryName, Data | Args]) ->
    case emqx_schema_registry_external:encode(RegistryName, Data, Args, #{tag => false}) of
        {ok, Encoded} ->
            Encoded;
        {error, Reason} ->
            error(Reason)
    end.

rsf_avro_decode([RegistryName, Data | Args]) ->
    case emqx_schema_registry_external:decode(RegistryName, Data, Args, #{}) of
        {ok, Decoded} ->
            Decoded;
        {error, Reason} ->
            error(Reason)
    end.

rsf_schema_encode_and_tag([OurSchemaName, RegistryName, Data | Args]) ->
    case handle_schema_encode_and_tag(OurSchemaName, RegistryName, Data, Args) of
        {ok, Encoded} ->
            Encoded;
        {error, Reason} ->
            error(Reason)
    end.

rsf_schema_decode_tagged([RegistryName, Data | Args]) ->
    case emqx_schema_registry_external:decode(RegistryName, Data, Args, #{}) of
        {ok, Decoded} ->
            Decoded;
        {error, Reason} ->
            error(Reason)
    end.

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
make_serde(?avro, Name, Source) ->
    Store0 = avro_schema_store:new([map]),
    %% import the schema into the map store with an assigned name
    %% if it's a named schema (e.g. struct), then Name is added as alias
    Store = avro_schema_store:import_schema_json(Name, Source, Store0),
    #serde{
        name = Name,
        type = ?avro,
        eval_context = Store
    };
make_serde(?protobuf, Name, Source) ->
    {CacheKey, SerdeMod} = make_protobuf_serde_mod(Name, Source),
    #serde{
        name = Name,
        type = ?protobuf,
        eval_context = SerdeMod,
        extra = #{cache_key => CacheKey}
    };
make_serde(?json, Name, Source) ->
    case json_decode(Source) of
        SchemaObj when is_map(SchemaObj) ->
            %% jesse:add_schema adds any map() without further validation
            %% if it's not a map, then case_clause
            ok = jesse_add_schema(Name, SchemaObj),
            #serde{name = Name, type = ?json};
        _NotMap ->
            error({invalid_json_schema, bad_schema_object})
    end;
make_serde(?external_http, Name, Params) ->
    Context = create_external_http_resource(Name, Params),
    #serde{
        name = Name,
        type = ?external_http,
        eval_context = Context,
        extra = #{}
    }.

eval_decode(#serde{type = ?avro, name = Name, eval_context = Store}, [Data]) ->
    Opts = avro:make_decoder_options([{map_type, map}, {record_type, map}]),
    avro_binary_decoder:decode(Data, Name, Store, Opts);
eval_decode(#serde{type = ?protobuf}, [#{} = DecodedData, MessageType]) ->
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
eval_decode(#serde{type = ?protobuf, eval_context = SerdeMod}, [EncodedData, MessageType0]) ->
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
eval_decode(#serde{type = ?json, name = Name}, [Data]) ->
    true = is_binary(Data),
    Term = json_decode(Data),
    {ok, NewTerm} = jesse_validate(Name, Term),
    NewTerm;
eval_decode(#serde{type = ?external_http, name = Name, eval_context = Context}, [Payload]) ->
    Request = generate_external_http_request(Payload, decode, Name, Context),
    exec_external_http_request(Request, Context).

eval_encode(#serde{type = ?avro, name = Name, eval_context = Store}, [Data]) ->
    avro_binary_encoder:encode(Store, Name, Data);
eval_encode(#serde{type = ?protobuf, eval_context = SerdeMod}, [DecodedData0, MessageName0]) ->
    DecodedData = emqx_utils_maps:safe_atom_key_map(DecodedData0),
    MessageName = binary_to_existing_atom(MessageName0, utf8),
    apply(SerdeMod, encode_msg, [DecodedData, MessageName]);
eval_encode(#serde{type = ?json, name = Name}, [Map]) ->
    %% The input Map may not be a valid JSON term for jesse
    Data = iolist_to_binary(emqx_utils_json:encode(Map)),
    NewMap = json_decode(Data),
    case jesse_validate(Name, NewMap) of
        {ok, _} ->
            Data;
        {error, Reason} ->
            error(Reason)
    end;
eval_encode(#serde{type = ?external_http, name = Name, eval_context = Context}, [Payload]) ->
    Request = generate_external_http_request(Payload, encode, Name, Context),
    exec_external_http_request(Request, Context).

destroy(#serde{type = ?avro = Type, name = _Name}) ->
    ?tp(serde_destroyed, #{type => Type, name => _Name}),
    ok;
destroy(#serde{type = ?protobuf = Type, name = _Name, eval_context = SerdeMod} = Serde) ->
    unload_code(SerdeMod),
    destroy_protobuf_code(Serde),
    ?tp(serde_destroyed, #{type => Type, name => _Name}),
    ok;
destroy(#serde{type = ?json = Type, name = Name}) ->
    ok = jesse_del_schema(Name),
    ?tp(serde_destroyed, #{type => Type, name => Name}),
    ok;
destroy(#serde{type = ?external_http = Type, name = _Name, eval_context = Context}) ->
    ok = destroy_external_http_resource(Context),
    ?tp(serde_destroyed, #{type => Type, name => _Name}),
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
protobuf_cache_key(Name, #{type := bundle, root_proto_path := RootPath}) ->
    #{valid := Sources0} = protobuf_resolve_imports({path, RootPath}),
    Sources1 = lists:map(
        fun(Path) ->
            {ok, Contents} = file:read_file(Path),
            Contents
        end,
        Sources0
    ),
    Sources = lists:sort(Sources1),
    {Name, erlang:system_info(otp_release), erlang:md5(Sources)};
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

generate_protobuf_code(SerdeMod, #{type := bundle, root_proto_path := RootPath0}) ->
    RootPath = str(RootPath0),
    maybe
        true ?= filelib:is_file(RootPath) orelse {error, {not_a_file, RootPath}},
        {ok, RootSource} ?= file:read_file(RootPath),
        BaseDir = filename:dirname(RootPath),
        ImportFetcherFn = fun(Path) -> protobuf_bundle_import_fetcher(Path, BaseDir) end,
        gpb_compile:string(
            SerdeMod,
            %% Binaries work too, but we convert to list to please dialyzer due to
            %% imprecise typespec...
            str(RootSource),
            [
                {i, BaseDir},
                {import_fetcher, ImportFetcherFn}
                | base_protobuf_opts()
            ]
        )
    end;
generate_protobuf_code(SerdeMod, Source) ->
    ImportFetcherFn = fun(_) -> {error, forbidden_single_file_import} end,
    gpb_compile:string(
        SerdeMod,
        Source,
        [
            {import_fetcher, ImportFetcherFn}
            | base_protobuf_opts()
        ]
    ).

base_protobuf_opts() ->
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
    ].

protobuf_bundle_import_fetcher(Path0, BaseDir) ->
    maybe
        ok ?= protobuf_validate_path(Path0, BaseDir),
        from_file
    end.

protobuf_validate_path(Path0, BaseDir0) ->
    BaseDir = str(BaseDir0),
    Path1 = str(Path0),
    Path2 = lists:flatten(string:replace(Path1, BaseDir ++ "/", "", leading)),
    maybe
        {ok, Path3} ?= gpb_compile:locate_import(Path2, [{i, BaseDir}]),
        Path = lists:flatten(string:replace(Path3, BaseDir ++ "/", "", leading)),
        Rel = filelib:safe_relative_path(Path, BaseDir),
        true ?= Rel /= unsafe orelse {error, {unsafe_import, Path}},
        ok
    else
        {error, {import_not_found, P, _}} ->
            {error, {import_not_found, P}};
        {error, Reason} ->
            {error, Reason}
    end.

-spec protobuf_resolve_imports({path, file:filename()} | {raw, binary()}) ->
    #{
        invalid := [file:filename()],
        valid := [file:filename()],
        missing := [file:filename()]
    }.
protobuf_resolve_imports(SourceOrRootPath) ->
    {BaseDir, Results} = protobuf_list_imports(SourceOrRootPath),
    Missing = proplists:get_value(missing, Results, []),
    Sources0 = proplists:get_value(sources, Results, []),
    Sources1 = lists:filter(fun(S) -> S /= from_input_string end, Sources0),
    {ValidSources, InvalidSources} =
        lists:partition(
            fun(Path) ->
                case protobuf_validate_path(Path, BaseDir) of
                    ok ->
                        true;
                    _ ->
                        false
                end
            end,
            Sources1
        ),
    #{
        invalid => InvalidSources,
        valid => ValidSources,
        missing => Missing
    }.

protobuf_list_imports({path, RootPath0}) ->
    RootPath = str(RootPath0),
    BaseDir0 = filename:dirname(RootPath),
    BaseDir = str(BaseDir0),
    {BaseDir, gpb_compile:list_io(RootPath, [{i, BaseDir}])};
protobuf_list_imports({raw, Source}) ->
    {"", gpb_compile:string_list_io(_Mod = undefined, str(Source), [])}.

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

create_external_http_resource(Name, Params) ->
    ResourceId = <<"schema_registry:external_http:", Name/binary>>,
    #{
        url := URL,
        max_retries := MaxRetries,
        connect_timeout := ConnectTimeout,
        request_timeout := RequestTimeout,
        headers := Headers,
        pool_type := PoolType,
        pool_size := PoolSize,
        external_params := ExternalParams
    } = Params,
    {ok, {Base, Path, QueryParams}} = emqx_schema_registry_schema:parse_url(URL),
    ConnectorConfig = #{
        request_base => Base,
        connect_timeout => ConnectTimeout,
        pool_type => PoolType,
        pool_size => PoolSize
    },
    AsyncStart = maps:get(async_start, Params, false),
    ResourceOpts = #{
        async_start => AsyncStart,
        start_after_created => true,
        spawn_buffer_workers => false,
        query_mode => simple_sync
    },
    {ok, _} = emqx_resource:create_local(
        ResourceId,
        ?SCHEMA_REGISTRY_RESOURCE_GROUP,
        emqx_bridge_http_connector,
        ConnectorConfig,
        ResourceOpts
    ),
    #{
        resource_id => ResourceId,
        headers => maps:to_list(Headers),
        path => Path,
        query_params => QueryParams,
        external_params => ExternalParams,
        request_timeout => RequestTimeout,
        max_retries => MaxRetries
    }.

destroy_external_http_resource(Context) ->
    #{resource_id := ResourceId} = Context,
    emqx_resource:remove_local(ResourceId).

generate_external_http_request(Payload, EncodeOrDecode, Name, Context) ->
    #{
        headers := Headers,
        path := Path,
        query_params := QueryParams,
        external_params := ExternalParams
    } = Context,
    PathWithQuery = append_query(Path, QueryParams),
    Body = #{
        <<"payload">> => base64:encode(Payload),
        <<"type">> => EncodeOrDecode,
        <<"schema_name">> => Name,
        <<"opts">> => ExternalParams
    },
    BodyBin = emqx_utils_json:encode(Body),
    {PathWithQuery, Headers, BodyBin}.

exec_external_http_request(Request, Context) ->
    #{
        resource_id := ResourceId,
        request_timeout := RequestTimeout,
        max_retries := MaxRetries
    } = Context,
    Query = {
        _ActionResId = undefined,
        _KeyOrNum = undefined,
        _Method = post,
        Request,
        RequestTimeout,
        MaxRetries
    },
    Result = emqx_resource:query(ResourceId, Query),
    handle_external_http_result(Result).

handle_external_http_result({ok, 200, _RespHeaders, RespEncoded}) ->
    try base64:decode(RespEncoded) of
        Resp ->
            Resp
    catch
        error:Reason ->
            error(#{
                msg => bad_external_http_response_format,
                hint => <<"server response is not a valid base64-encoded string">>,
                response => RespEncoded,
                reason => Reason
            })
    end;
handle_external_http_result({ok, StatusCode, _RespHeaders, RespBody}) ->
    error(#{
        msg => bad_external_http_response_status_code,
        response => RespBody,
        status_code => StatusCode
    });
handle_external_http_result({error, {unrecoverable_error, #{status_code := _} = Reason}}) ->
    error(#{
        msg => external_http_request_failed,
        reason => maps:with([status_code, body], Reason)
    });
handle_external_http_result({error, {recoverable_error, #{status_code := _} = Reason}}) ->
    error(#{
        msg => external_http_request_failed,
        reason => maps:with([status_code, body], Reason)
    });
handle_external_http_result({error, {unrecoverable_error, Reason}}) ->
    error(#{
        msg => external_http_request_failed,
        reason => Reason
    });
handle_external_http_result({error, {recoverable_error, Reason}}) ->
    error(#{
        msg => external_http_request_failed,
        reason => Reason
    });
handle_external_http_result({error, Reason}) ->
    error(#{
        msg => external_http_request_failed,
        reason => Reason
    }).

append_query(Path, <<"">>) ->
    Path;
append_query(Path, Query) ->
    [Path, $?, Query].

str(X) -> emqx_utils_conv:str(X).
