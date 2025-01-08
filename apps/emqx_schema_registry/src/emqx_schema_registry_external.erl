%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_external).

-feature(maybe_expr, enable).

-behaviour(gen_server).

-include("emqx_schema_registry.hrl").

%% API
-export([
    start_link/0,

    add/2,
    remove/1,

    encode/4,
    encode_with/6,
    decode/4
]).

%% `gen_server' API
-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(EXTERNAL_REGISTRY_TAB, emqx_schema_registry_external).

-define(NAME(RegistryName), {n, l, {?MODULE, RegistryName}}).
-define(REF(RegistryName), {via, gproc, ?NAME(RegistryName)}).

-define(bad_registry_credentials, bad_registry_credentials).
-define(schema_not_found, schema_not_found).
-define(bad_input, bad_input).
-define(bad_schema_source, bad_schema_source).
-define(external_registry_unavailable, external_registry_unavailable).

-type registry_name() :: atom() | binary().
-type encode_opts() :: #{
    %% Default: false
    tag => boolean()
}.
-type decode_opts() :: #{}.
-type data() :: term().
-type encoded() :: binary().
-type arg() :: term().
-type schema_source_hash() :: binary().

%% call/cast/info events
-record(ensure_registered, {
    name :: registry_name(),
    our_schema_name :: schema_name(),
    source :: schema_source(),
    hash :: schema_source_hash(),
    args :: [arg()]
}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

add(Name, Config) ->
    ChildSpec = worker_child_spec(Name, Config),
    ok = emqx_schema_registry_sup:ensure_external_registry_worker_started(ChildSpec),
    ok = initialize_registry_context(Name, Config),
    ok.

remove(Name) ->
    ok = emqx_schema_registry_sup:ensure_external_registry_worker_absent(Name),
    ok = deinitialize_registry_context(Name),
    ok.

-spec encode(registry_name(), data(), [arg()], encode_opts()) ->
    {ok, encoded()} | {error, term()}.
encode(Name, Data, Args, Opts) ->
    with_registry(Name, fun(Context) ->
        do_encode(Name, Context, Data, Args, Opts)
    end).

-spec encode_with(registry_name(), schema_name(), schema_source(), data(), [arg()], encode_opts()) ->
    {ok, encoded()} | {error, term()}.
encode_with(Name, OurSchemaName, Source, Data, Args, Opts) ->
    with_registry(Name, fun(Context) ->
        maybe
            {ok, Id} ?= ensure_registered(Name, Context, OurSchemaName, Source, Args),
            do_encode(Name, Context, Data, [Id | Args], Opts)
        end
    end).

-spec decode(registry_name(), encoded(), [arg()], decode_opts()) ->
    {ok, data()} | {error, term()}.
decode(Name, Data, Args, Opts) ->
    with_registry(Name, fun(Context) ->
        do_decode(Name, Context, Data, Args, Opts)
    end).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_) ->
    process_flag(trap_exit, true),
    create_tables(),
    State = #{},
    {ok, State}.

terminate(_Reason, _State) ->
    ets:foldr(
        fun
            ({_Id, #{on_remove := OnRemove}}, Acc) when is_function(OnRemove, 0) ->
                _ = OnRemove(),
                Acc;
            (_, Acc) ->
                Acc
        end,
        ok,
        ?EXTERNAL_REGISTRY_TAB
    ).

handle_call(#ensure_registered{} = Req, _From, State) ->
    handle_ensure_registered(Req, State);
handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns (`gen_server')
%%------------------------------------------------------------------------------

create_tables() ->
    ok = emqx_utils_ets:new(?EXTERNAL_REGISTRY_TAB, [set, public]),
    ok.

handle_ensure_registered(Req, State) ->
    #ensure_registered{
        name = Name,
        our_schema_name = OurSchemaName,
        source = Source,
        hash = SourceHash,
        args = Args
    } = Req,
    Res = with_registry(Name, fun(Context) ->
        %% Race: another call already registered it
        maybe
            {ok, _Id} ?= find_cached_schema_id(Context, OurSchemaName, SourceHash)
        else
            error ->
                do_register(Name, Context, OurSchemaName, Source, SourceHash, Args)
        end
    end),
    {reply, Res, State}.

do_register(Name, #{type := confluent} = Context, OurSchemaName, Source, SourceHash, [Subject]) ->
    maybe
        {ok, Id} ?= confluent_register_schema(Name, Subject, Source),
        NewContext = add_cached_schema_id(Context, OurSchemaName, SourceHash, Id),
        ets:insert(?EXTERNAL_REGISTRY_TAB, {id(Name), NewContext}),
        {ok, Id}
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

%%===========================
%% Confluent
%%===========================

avlizer_auth(#{auth := none}) ->
    undefined;
avlizer_auth(#{auth := #{} = Opts}) ->
    #{
        mechanism := Mechanism,
        username := Username,
        password := Password
    } = Opts,
    {Mechanism, emqx_utils_conv:str(Username), Password}.

confluent_make_encoder(Name, CacheTable, SchemaId) ->
    try
        {ok, avlizer_confluent:make_encoder2(?REF(id(Name)), CacheTable, SchemaId)}
    catch
        error:{bad_http_code, 404} ->
            {error, ?schema_not_found};
        error:{bad_http_code, 401} ->
            {error, ?bad_registry_credentials}
    end.

confluent_encode(Encoder, Data) ->
    try
        {ok, avlizer_confluent:encode(Encoder, Data)}
    catch
        _:_ ->
            {error, ?bad_input}
    end.

confluent_untag(Data) ->
    try
        {ok, avlizer_confluent:untag_data(Data)}
    catch
        _:_ ->
            {error, ?bad_input}
    end.

confluent_make_decoder(Name, CacheTable, SchemaId) ->
    try
        {ok, avlizer_confluent:make_decoder2(?REF(id(Name)), CacheTable, SchemaId)}
    catch
        error:{bad_http_code, 404} ->
            {error, ?schema_not_found};
        error:{bad_http_code, 401} ->
            {error, ?bad_registry_credentials}
    end.

confluent_decode(Decoder, Data) ->
    try
        {ok, avlizer_confluent:decode(Decoder, Data)}
    catch
        _:_ ->
            {error, bad_input}
    end.

confluent_register_schema(Name, Subject0, Source) ->
    Subject = emqx_utils_conv:str(Subject0),
    case avlizer_confluent:register_schema(?REF(id(Name)), Subject, Source) of
        {ok, Id} ->
            {ok, Id};
        {error, {bad_http_code, 401}} ->
            {error, ?bad_registry_credentials};
        {error, {bad_http_code, 409}} ->
            {error, ?bad_schema_source};
        {error, {bad_http_code, 422}} ->
            {error, ?bad_schema_source};
        {error, {bad_http_code, 500}} ->
            {error, ?external_registry_unavailable};
        {error, _} = Error ->
            Error
    end.

%%===========================
%% Misc
%%===========================

with_registry(Name, Fn) ->
    Id = id(Name),
    case ets:lookup(?EXTERNAL_REGISTRY_TAB, Id) of
        [{Id, Context}] ->
            Fn(Context);
        [] ->
            {error, registry_not_found}
    end.

worker_child_spec(Name, #{type := confluent} = Config) ->
    Id = id(Name),
    Opts = external_registry_worker_config(Config),
    #{
        id => Name,
        start => {avlizer_confluent, start_link, [?REF(Id), Opts]},
        restart => permanent,
        shutdown => 5_000,
        type => worker
    }.

external_registry_worker_config(#{type := confluent} = Config) ->
    #{url := URL} = Config,
    MAuth = avlizer_auth(Config),
    emqx_utils_maps:put_if(
        #{url => emqx_utils_conv:str(URL)},
        auth,
        MAuth,
        MAuth =/= undefined
    ).

id(Name) ->
    emqx_utils_conv:bin(Name).

initialize_registry_context(Name, #{type := confluent = Type}) ->
    Id = id(Name),
    CacheTable = avlizer_confluent:get_table(?REF(Id)),
    Context = #{type => Type, cache_table => CacheTable},
    Id = id(Name),
    true = ets:insert(?EXTERNAL_REGISTRY_TAB, {Id, Context}),
    ok.

deinitialize_registry_context(Name) ->
    Id = id(Name),
    case ets:take(?EXTERNAL_REGISTRY_TAB, Id) of
        [{Id, #{on_remove := OnRemove}}] when is_function(OnRemove, 0) ->
            OnRemove(),
            ok;
        _ ->
            ok
    end.

do_decode(Name, #{type := confluent} = Context, Data, [SchemaId], _Opts) ->
    %% Tag provided
    #{cache_table := CacheTable} = Context,
    maybe
        true ?= is_integer(SchemaId) orelse {error, {bad_schema_id, SchemaId}},
        {ok, Decoder} ?= confluent_make_decoder(Name, CacheTable, SchemaId),
        confluent_decode(Decoder, Data)
    end;
do_decode(Name, #{type := confluent} = Context, Data0, [], _Opts) ->
    %% Data is tagged.
    #{cache_table := CacheTable} = Context,
    maybe
        {ok, {SchemaId, Data}} ?= confluent_untag(Data0),
        {ok, Decoder} ?= confluent_make_decoder(Name, CacheTable, SchemaId),
        confluent_decode(Decoder, Data)
    end.

do_encode(Name, #{type := confluent} = Context, Data, [SchemaId | _MaybeSubject], Opts) ->
    %% assert
    true = is_integer(SchemaId),
    #{cache_table := CacheTable} = Context,
    ShouldTag = maps:get(tag, Opts, false),
    maybe
        true ?= is_integer(SchemaId) orelse {error, {bad_schema_id, SchemaId}},
        {ok, Encoder} ?= confluent_make_encoder(Name, CacheTable, SchemaId),
        {ok, Encoded} ?= confluent_encode(Encoder, Data),
        case ShouldTag of
            true ->
                {ok, avlizer_confluent:tag_data(SchemaId, Encoded)};
            false ->
                {ok, Encoded}
        end
    end.

hash_source(Source) when is_binary(Source) ->
    erlang:md5(Source).

find_cached_schema_id(Context, OurSchemaName, SourceHash) ->
    case Context of
        #{cache := #{OurSchemaName := #{SourceHash := Id}}} ->
            {ok, Id};
        _ ->
            error
    end.

add_cached_schema_id(Context, OurSchemaName, SourceHash, Id) ->
    %% Drops any stale source hash already in context
    Cached = #{SourceHash => Id},
    emqx_utils_maps:deep_put([cache, OurSchemaName], Context, Cached).

ensure_registered(Name, Context, OurSchemaName, Source, Args) ->
    SourceHash = hash_source(Source),
    maybe
        {ok, _Id} ?= find_cached_schema_id(Context, OurSchemaName, SourceHash)
    else
        error ->
            Req = #ensure_registered{
                name = Name,
                our_schema_name = OurSchemaName,
                source = Source,
                hash = SourceHash,
                args = Args
            },
            gen_server:call(?MODULE, Req, infinity)
    end.
