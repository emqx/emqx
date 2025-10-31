%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% This module takes aggregated records from a buffer and delivers them to a blob storage
%% backend, wrapped in a configurable general-purpose container.
-module(emqx_connector_aggreg_delivery).

-feature(maybe_expr, enable).

-behaviour(gen_server).

-include_lib("snabbkaffe/include/trace.hrl").
-include("emqx_connector_aggregator.hrl").

-export([start_link/3]).
-export([validate_container_opts/1]).

%% `gen_server' API
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    terminate/2,
    format_status/1
]).

-export_type([buffer_map/0]).

%%------------------------------------------------------------------------------
%% Type definitions
%%------------------------------------------------------------------------------

-define(delivery, delivery).
-define(id, id).

%% Calls/casts/infos
-define(process_delivery, process_delivery).

-type container_and_mod() ::
    {emqx_connector_aggreg_csv, emqx_connector_aggreg_csv:container()}
    | {emqx_connector_aggreg_json_lines, emqx_connector_aggreg_json_lines:container()}.

-record(delivery, {
    id :: id(),
    callback_module :: module(),
    container :: container_and_mod(),
    reader :: emqx_connector_aggreg_buffer:reader(),
    transfer :: transfer_state(),
    empty :: boolean()
}).

-type id() :: term().

-type init_opts() :: #{
    callback_module := module(),
    any() => term()
}.

-type transfer_state() :: term().

-doc """
Initialize the transfer state, such as blob storage path, transfer options, client
credentials, etc. .  Also returns options to initialize container, if dynamic settings are
needed.
""".
-callback init_transfer_state_and_container_opts(buffer(), map()) ->
    {ok, transfer_state(), ContainerOpts} | {error, term()}
when
    ContainerOpts :: map().

-doc """
Append data to the transfer before sending.  Usually should not fail.
""".
-callback process_append(iodata() | term(), transfer_state()) ->
    transfer_state().

-doc """
Push appended transfer data to its destination (e.g.: upload a part of a multi-part
upload).  May fail.
""".
-callback process_write(transfer_state()) -> {ok, transfer_state()} | {error, term()}.

-doc """
Finalize the transfer and clean up any resources.  May return a term summarizing the
transfer.
""".
-callback process_complete(transfer_state()) -> {ok, term()}.

-doc """
Clean up any resources when the process finishes abnormally.  Result is ignored.
""".
-callback process_terminate(transfer_state()) -> any().

-doc """
When a delivery fails (or simply when `gen_server:format_status/1` is called on a delivery
process), this callback is used to format the internal transfer status.
""".
-callback process_format_status(transfer_state()) -> term().

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link(id(), buffer(), init_opts()) -> gen_server:start_ret().
start_link(Id, Buffer, Opts) ->
    InitOpts = #{id => Id, buffer => Buffer, opts => Opts},
    gen_server:start_link(?MODULE, InitOpts, []).

-spec validate_container_opts(map()) -> ok | {error, term()}.
validate_container_opts(ContainerOpts) ->
    case mk_container(ContainerOpts) of
        {ok, _} ->
            ok;
        {error, {container_opts_error, Details}} ->
            {error, Details}
    end.

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(InitOpts) ->
    #{
        id := Id,
        buffer := Buffer,
        opts := Opts
    } = InitOpts,
    ?tp(connector_aggreg_delivery_started, #{action => Id, buffer => Buffer}),
    Reader = open_buffer(Buffer),
    case init_delivery(Id, Reader, Buffer, Opts#{action => Id}) of
        {ok, Delivery} ->
            _ = erlang:process_flag(trap_exit, true),
            trigger_process_delivery(),
            State = #{?id => Id, ?delivery => Delivery},
            {ok, State};
        {error, Reason} ->
            ?tp("connector_aggreg_delivery_init_failure", #{id => Id, reason => Reason}),
            {error, {failed_to_initialize, Reason}}
    end.

terminate(Reason, State) ->
    %% Id is the same name as the parent process name.
    #{?id := Parent, ?delivery := Delivery} = State,
    emqx_connector_aggregator:delivery_exit(Parent, self(), Reason),
    case Reason of
        normal ->
            ok;
        {shutdown, _} ->
            ok;
        _ ->
            #delivery{callback_module = Mod, transfer = Transfer} = Delivery,
            _ = Mod:process_terminate(Transfer),
            ok
    end,
    ok.

handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(?process_delivery, State0) ->
    handle_process_delivery(State0);
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_process_delivery(#{?delivery := Delivery0 = #delivery{reader = Reader0}} = State0) ->
    case emqx_connector_aggreg_buffer:read(Reader0) of
        {Records = [#{} | _], Reader} ->
            Delivery1 = Delivery0#delivery{reader = Reader},
            Delivery2 = process_append_records(Records, Delivery1),
            Delivery = process_write(Delivery2),
            State = State0#{?delivery := Delivery},
            trigger_process_delivery(),
            {noreply, State};
        {[], Reader} ->
            Delivery = Delivery0#delivery{reader = Reader},
            State = State0#{?delivery := Delivery},
            trigger_process_delivery(),
            {noreply, State};
        eof ->
            process_complete(State0);
        {Unexpected, _Reader} ->
            {stop, {buffer_unexpected_record, Unexpected}, State0}
    end.

process_append_records(
    Records,
    Delivery = #delivery{
        callback_module = Mod,
        container = {ContainerMod, Container0},
        transfer = Transfer0
    }
) ->
    {Writes, Container} =
        emqx_connector_aggreg_container:fill(ContainerMod, Records, Container0),
    Transfer = Mod:process_append(Writes, Transfer0),
    Delivery#delivery{
        container = {ContainerMod, Container},
        transfer = Transfer,
        empty = false
    }.

process_write(Delivery = #delivery{callback_module = Mod, transfer = Transfer0}) ->
    case Mod:process_write(Transfer0) of
        {ok, Transfer} ->
            Delivery#delivery{transfer = Transfer};
        {error, Reason} ->
            %% Todo: handle more gracefully?  Retry?
            exit({upload_failed, Reason})
    end.

process_complete(#{?delivery := #delivery{id = Id, empty = true}} = State0) ->
    ?tp(connector_aggreg_delivery_completed, #{action => Id, transfer => empty}),
    {stop, {shutdown, {skipped, empty}}, State0};
process_complete(
    #{
        ?delivery := #delivery{
            id = Id,
            callback_module = Mod,
            container = {ContainerMod, Container},
            transfer = Transfer0
        }
    } = State0
) ->
    Trailer = emqx_connector_aggreg_container:close(ContainerMod, Container),
    Transfer = Mod:process_append(Trailer, Transfer0),
    case Mod:process_complete(Transfer) of
        {ok, Completed} ->
            ?tp(connector_aggreg_delivery_completed, #{action => Id, transfer => Completed}),
            {stop, normal, State0};
        {error, Error} ->
            {stop, {upload_failed, Error}, State0}
    end.

format_status(Status) ->
    maps:map(
        fun
            (state, #{?delivery := Delivery} = State) ->
                #delivery{callback_module = Mod} = Delivery,
                State#{
                    ?delivery := Delivery#delivery{
                        transfer = Mod:process_format_status(Delivery#delivery.transfer)
                    }
                };
            (_K, V) ->
                V
        end,
        Status
    ).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

trigger_process_delivery() ->
    gen_server:cast(self(), ?process_delivery).

init_delivery(
    Id,
    Reader,
    Buffer,
    Opts = #{callback_module := Mod}
) ->
    maybe
        {ok, Transfer, ContainerOpts} ?=
            Mod:init_transfer_state_and_container_opts(Buffer, Opts),
        {ok, Container} ?= mk_container(ContainerOpts),
        Delivery = #delivery{
            id = Id,
            callback_module = Mod,
            container = Container,
            reader = Reader,
            transfer = Transfer,
            empty = true
        },
        {ok, Delivery}
    end.

open_buffer(#buffer{filename = Filename}) ->
    case file:open(Filename, [read, binary, raw]) of
        {ok, FD} ->
            {_Meta, Reader} = emqx_connector_aggreg_buffer:new_reader(FD),
            Reader;
        {error, Reason} ->
            error(#{reason => buffer_open_failed, file => Filename, posix => Reason})
    end.

mk_container(ContainerOpts) ->
    try
        {ok, do_mk_container(ContainerOpts)}
    catch
        throw:Details ->
            {error, {container_opts_error, Details}}
    end.

do_mk_container(#{type := csv, column_order := OrderOpt}) ->
    %% TODO: Deduplicate?
    ColumnOrder = lists:map(fun emqx_utils_conv:bin/1, OrderOpt),
    {emqx_connector_aggreg_csv, emqx_connector_aggreg_csv:new(#{column_order => ColumnOrder})};
do_mk_container(#{type := json_lines}) ->
    Opts = #{},
    {emqx_connector_aggreg_json_lines, emqx_connector_aggreg_json_lines:new(Opts)};
do_mk_container(#{type := parquet} = ContainerOpts) ->
    #{schema := Schema} = ContainerOpts,
    AvroScBin = get_parquet_avro_schema(Schema),
    WriterOpts0 = maps:with(
        [
            write_old_list_structure,
            enable_dictionary,
            data_page_header_version,
            max_row_group_bytes
        ],
        ContainerOpts
    ),
    DefaultCompression = maps:get(default_compression, ContainerOpts, snappy),
    CompressionOpts = #{},
    WriterOpts = WriterOpts0#{default_compression => {DefaultCompression, CompressionOpts}},
    AvroSc = emqx_utils_json:decode(AvroScBin),
    ScOpts = maps:with([write_old_list_structure], ContainerOpts),
    ParquetSchema = parquer_schema_avro:from_avro(AvroSc, ScOpts),
    Opts = #{schema => ParquetSchema, writer_opts => WriterOpts},
    {emqx_connector_aggreg_parquet, emqx_connector_aggreg_parquet:new(Opts)};
do_mk_container(#{type := noop}) ->
    Opts = #{},
    {emqx_connector_aggreg_noop, emqx_connector_aggreg_noop:new(Opts)};
do_mk_container(#{type := custom, module := Mod, opts := Opts}) ->
    {Mod, Mod:new(Opts)}.

get_parquet_avro_schema(#{type := avro_inline, def := AvroScBin}) ->
    AvroScBin;
get_parquet_avro_schema(#{type := avro_ref, name := SerdeName}) ->
    %% fixme: calling an almost terminal application (`emqx_schema_registry`) from a more
    %% or less intial one (`emqx_connector_aggregator`)...
    %% how to avoid this?
    case emqx_schema_registry:get_schema(SerdeName) of
        {error, not_found} ->
            throw(#{
                reason => parquet_schema_not_found,
                schema_name => SerdeName
            });
        {ok, #{type := avro, source := AvroScBin}} ->
            AvroScBin;
        {ok, #{type := WrongType}} ->
            throw(#{
                reason => parquet_schema_wrong_type,
                expected_type => avro,
                schema_name => SerdeName,
                schema_type => WrongType
            })
    end.
