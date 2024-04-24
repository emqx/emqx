%%--------------------------------------------------------------------
%% Copyright (c) 2017-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_plugins_serde).

-include("emqx_plugins.hrl").
-include_lib("emqx/include/logger.hrl").

%% API
-export([
    start_link/0,
    lookup_serde/1,
    add_schema/2,
    get_schema/1,
    delete_schema/1
]).

%% `gen_server' API
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_continue/2,
    terminate/2
]).

-export([
    decode/2,
    encode/2
]).

%%-------------------------------------------------------------------------------------------------
%% API
%%-------------------------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec lookup_serde(schema_name()) -> {ok, plugin_schema_serde()} | {error, not_found}.
lookup_serde(SchemaName) ->
    case ets:lookup(?PLUGIN_SERDE_TAB, to_bin(SchemaName)) of
        [] ->
            {error, not_found};
        [Serde] ->
            {ok, Serde}
    end.

-spec add_schema(schema_name(), avsc()) -> ok | {error, term()}.
add_schema(Name, Avsc) ->
    case lookup_serde(Name) of
        {ok, _Serde} ->
            ?SLOG(warning, #{msg => "plugin_schema_already_exists", plugin => Name}),
            {error, already_exists};
        {error, not_found} ->
            case gen_server:call(?MODULE, {build_serdes, to_bin(Name), Avsc}) of
                ok ->
                    ?SLOG(debug, #{msg => "plugin_schema_added", plugin => Name}),
                    ok;
                {error, Reason} = E ->
                    ?SLOG(error, #{
                        msg => "plugin_schema_add_failed",
                        plugin => Name,
                        reason => emqx_utils:readable_error_msg(Reason)
                    }),
                    E
            end
    end.

get_schema(NameVsn) ->
    Path = emqx_plugins:avsc_file_path(NameVsn),
    case read_avsc_file(Path) of
        {ok, Avsc} ->
            {ok, Avsc};
        {error, Reason} ->
            ?SLOG(warning, Reason),
            {error, Reason}
    end.

-spec delete_schema(schema_name()) -> ok | {error, term()}.
delete_schema(NameVsn) ->
    case lookup_serde(NameVsn) of
        {ok, _Serde} ->
            async_delete_serdes([NameVsn]),
            ok;
        {error, not_found} ->
            {error, not_found}
    end.

-spec decode(schema_name(), encoded_data()) -> {ok, decoded_data()} | {error, any()}.
decode(SerdeName, RawData) ->
    with_serde(
        "decode_avro_binary",
        eval_serde_fun(?FUNCTION_NAME, "bad_avro_binary", SerdeName, [RawData])
    ).

-spec encode(schema_name(), decoded_data()) -> {ok, encoded_data()} | {error, any()}.
encode(SerdeName, Data) ->
    with_serde(
        "encode_avro_data",
        eval_serde_fun(?FUNCTION_NAME, "bad_avro_data", SerdeName, [Data])
    ).

%%-------------------------------------------------------------------------------------------------
%% `gen_server' API
%%-------------------------------------------------------------------------------------------------

init(_) ->
    process_flag(trap_exit, true),
    ok = emqx_utils_ets:new(?PLUGIN_SERDE_TAB, [
        public, ordered_set, {keypos, #plugin_schema_serde.name}
    ]),
    State = #{},
    SchemasMap = read_plugin_avsc(),
    {ok, State, {continue, {build_serdes, SchemasMap}}}.

handle_continue({build_serdes, SchemasMap}, State) ->
    _ = build_serdes(SchemasMap),
    {noreply, State}.

handle_call({build_serdes, NameVsn, Avsc}, _From, State) ->
    BuildRes = do_build_serde(NameVsn, Avsc),
    {reply, BuildRes, State};
handle_call(_Call, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({delete_serdes, Names}, State) ->
    lists:foreach(fun ensure_serde_absent/1, Names),
    {noreply, State};
handle_cast(_Cast, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%-------------------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------------------

read_plugin_avsc() ->
    Pattern = filename:join([emqx_plugins:install_dir(), "*", "config_schema.avsc"]),
    lists:foldl(
        fun(AvscPath, AccIn) ->
            case read_avsc_file(AvscPath) of
                {ok, Avsc} ->
                    [_, NameVsn | _] = lists:reverse(filename:split(AvscPath)),
                    AccIn#{to_bin(NameVsn) => Avsc};
                {error, Reason} ->
                    ?SLOG(warning, Reason),
                    AccIn
            end
        end,
        _Acc0 = #{},
        filelib:wildcard(Pattern)
    ).

build_serdes(Schemas) ->
    maps:foreach(fun do_build_serde/2, Schemas).

do_build_serde(NameVsn, Avsc) ->
    try
        Serde = make_serde(NameVsn, Avsc),
        true = ets:insert(?PLUGIN_SERDE_TAB, Serde),
        ok
    catch
        Kind:Error:Stacktrace ->
            ?SLOG(
                error,
                #{
                    msg => "error_building_plugin_schema_serde",
                    name => NameVsn,
                    kind => Kind,
                    error => Error,
                    stacktrace => Stacktrace
                }
            ),
            {error, Error}
    end.

make_serde(NameVsn, Avsc) ->
    Store0 = avro_schema_store:new([map]),
    %% import the schema into the map store with an assigned name
    %% if it's a named schema (e.g. struct), then Name is added as alias
    Store = avro_schema_store:import_schema_json(NameVsn, Avsc, Store0),
    #plugin_schema_serde{
        name = NameVsn,
        eval_context = Store
    }.

ensure_serde_absent(Name) when not is_binary(Name) ->
    ensure_serde_absent(to_bin(Name));
ensure_serde_absent(Name) ->
    case lookup_serde(Name) of
        {ok, _Serde} ->
            _ = ets:delete(?PLUGIN_SERDE_TAB, Name),
            ok;
        {error, not_found} ->
            ok
    end.

async_delete_serdes(Names) ->
    gen_server:cast(?MODULE, {delete_serdes, Names}).

with_serde(WhichOp, Fun) ->
    try
        Fun()
    catch
        throw:Reason ->
            ?SLOG(error, Reason#{
                which_op => WhichOp,
                reason => emqx_utils:readable_error_msg(Reason)
            }),
            {error, Reason};
        error:Reason:Stacktrace ->
            %% unexpected errors, log stacktrace
            ?SLOG(warning, #{
                msg => "plugin_schema_op_failed",
                which_op => WhichOp,
                exception => Reason,
                stacktrace => Stacktrace
            }),
            {error, #{
                which_op => WhichOp,
                reason => Reason
            }}
    end.

eval_serde_fun(Op, ErrMsg, SerdeName, Args) ->
    fun() ->
        case lookup_serde(SerdeName) of
            {ok, Serde} ->
                eval_serde(Op, Serde, Args);
            {error, not_found} ->
                throw(#{
                    error_msg => ErrMsg,
                    reason => plugin_serde_not_found,
                    serde_name => SerdeName
                })
        end
    end.

eval_serde(decode, #plugin_schema_serde{name = Name, eval_context = Store}, [Data]) ->
    Opts = avro:make_decoder_options([{map_type, map}, {record_type, map}]),
    {ok, avro_binary_decoder:decode(Data, Name, Store, Opts)};
eval_serde(encode, #plugin_schema_serde{name = Name, eval_context = Store}, [Data]) ->
    {ok, avro_binary_encoder:encode(Store, Name, Data)};
eval_serde(_, _, _) ->
    throw(#{error_msg => "unexpected_plugin_avro_op"}).

read_avsc_file(Path) ->
    case file:read_file(Path) of
        {ok, Bin} ->
            {ok, Bin};
        {error, _} ->
            {error, #{
                error => "failed_to_read_plugin_schema",
                path => Path
            }}
    end.

to_bin(A) when is_atom(A) -> atom_to_binary(A);
to_bin(L) when is_list(L) -> iolist_to_binary(L);
to_bin(B) when is_binary(B) -> B.
