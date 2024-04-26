%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% This module takes aggregated records from a buffer and delivers them to S3,
%% wrapped in a configurable container (though currently there's only CSV).
-module(emqx_bridge_s3_aggreg_delivery).

-include_lib("snabbkaffe/include/trace.hrl").
-include("emqx_bridge_s3_aggregator.hrl").

-export([start_link/3]).

%% Internal exports
-export([
    init/4,
    loop/3
]).

-behaviour(emqx_template).
-export([lookup/2]).

%% Sys
-export([
    system_continue/3,
    system_terminate/4,
    format_status/2
]).

-record(delivery, {
    name :: _Name,
    container :: emqx_bridge_s3_aggreg_csv:container(),
    reader :: emqx_bridge_s3_aggreg_buffer:reader(),
    upload :: emqx_s3_upload:t(),
    empty :: boolean()
}).

-type state() :: #delivery{}.

%%

start_link(Name, Buffer, Opts) ->
    proc_lib:start_link(?MODULE, init, [self(), Name, Buffer, Opts]).

%%

-spec init(pid(), _Name, buffer(), _Opts :: map()) -> no_return().
init(Parent, Name, Buffer, Opts) ->
    ?tp(s3_aggreg_delivery_started, #{action => Name, buffer => Buffer}),
    Reader = open_buffer(Buffer),
    Delivery = init_delivery(Name, Reader, Buffer, Opts#{action => Name}),
    _ = erlang:process_flag(trap_exit, true),
    ok = proc_lib:init_ack({ok, self()}),
    loop(Delivery, Parent, []).

init_delivery(Name, Reader, Buffer, Opts = #{container := ContainerOpts}) ->
    #delivery{
        name = Name,
        container = mk_container(ContainerOpts),
        reader = Reader,
        upload = mk_upload(Buffer, Opts),
        empty = true
    }.

open_buffer(#buffer{filename = Filename}) ->
    case file:open(Filename, [read, binary, raw]) of
        {ok, FD} ->
            {_Meta, Reader} = emqx_bridge_s3_aggreg_buffer:new_reader(FD),
            Reader;
        {error, Reason} ->
            error({buffer_open_failed, Reason})
    end.

mk_container(#{type := csv, column_order := OrderOpt}) ->
    %% TODO: Deduplicate?
    ColumnOrder = lists:map(fun emqx_utils_conv:bin/1, OrderOpt),
    emqx_bridge_s3_aggreg_csv:new(#{column_order => ColumnOrder}).

mk_upload(
    Buffer,
    Opts = #{
        bucket := Bucket,
        upload_options := UploadOpts,
        client_config := Config,
        uploader_config := UploaderConfig
    }
) ->
    Client = emqx_s3_client:create(Bucket, Config),
    Key = mk_object_key(Buffer, Opts),
    emqx_s3_upload:new(Client, Key, UploadOpts, UploaderConfig).

mk_object_key(Buffer, #{action := Name, key := Template}) ->
    emqx_template:render_strict(Template, {?MODULE, {Name, Buffer}}).

%%

-spec loop(state(), pid(), [sys:debug_option()]) -> no_return().
loop(Delivery, Parent, Debug) ->
    %% NOTE: This function is mocked in tests.
    receive
        Msg -> handle_msg(Msg, Delivery, Parent, Debug)
    after 0 ->
        process_delivery(Delivery, Parent, Debug)
    end.

process_delivery(Delivery0 = #delivery{reader = Reader0}, Parent, Debug) ->
    case emqx_bridge_s3_aggreg_buffer:read(Reader0) of
        {Records = [#{} | _], Reader} ->
            Delivery1 = Delivery0#delivery{reader = Reader},
            Delivery2 = process_append_records(Records, Delivery1),
            Delivery = process_write(Delivery2),
            loop(Delivery, Parent, Debug);
        {[], Reader} ->
            Delivery = Delivery0#delivery{reader = Reader},
            loop(Delivery, Parent, Debug);
        eof ->
            process_complete(Delivery0);
        {Unexpected, _Reader} ->
            exit({buffer_unexpected_record, Unexpected})
    end.

process_append_records(Records, Delivery = #delivery{container = Container0, upload = Upload0}) ->
    {Writes, Container} = emqx_bridge_s3_aggreg_csv:fill(Records, Container0),
    {ok, Upload} = emqx_s3_upload:append(Writes, Upload0),
    Delivery#delivery{
        container = Container,
        upload = Upload,
        empty = false
    }.

process_write(Delivery = #delivery{upload = Upload0}) ->
    case emqx_s3_upload:write(Upload0) of
        {ok, Upload} ->
            Delivery#delivery{upload = Upload};
        {cont, Upload} ->
            process_write(Delivery#delivery{upload = Upload});
        {error, Reason} ->
            _ = emqx_s3_upload:abort(Upload0),
            exit({upload_failed, Reason})
    end.

process_complete(#delivery{name = Name, empty = true}) ->
    ?tp(s3_aggreg_delivery_completed, #{action => Name, upload => empty}),
    exit({shutdown, {skipped, empty}});
process_complete(#delivery{name = Name, container = Container, upload = Upload0}) ->
    Trailer = emqx_bridge_s3_aggreg_csv:close(Container),
    {ok, Upload} = emqx_s3_upload:append(Trailer, Upload0),
    case emqx_s3_upload:complete(Upload) of
        {ok, Completed} ->
            ?tp(s3_aggreg_delivery_completed, #{action => Name, upload => Completed}),
            ok;
        {error, Reason} ->
            _ = emqx_s3_upload:abort(Upload),
            exit({upload_failed, Reason})
    end.

%%

handle_msg({system, From, Msg}, Delivery, Parent, Debug) ->
    sys:handle_system_msg(Msg, From, Parent, ?MODULE, Debug, Delivery);
handle_msg({'EXIT', Parent, Reason}, Delivery, Parent, Debug) ->
    system_terminate(Reason, Parent, Debug, Delivery);
handle_msg(_Msg, Delivery, Parent, Debug) ->
    loop(Parent, Debug, Delivery).

-spec system_continue(pid(), [sys:debug_option()], state()) -> no_return().
system_continue(Parent, Debug, Delivery) ->
    loop(Delivery, Parent, Debug).

-spec system_terminate(_Reason, pid(), [sys:debug_option()], state()) -> _.
system_terminate(_Reason, _Parent, _Debug, #delivery{upload = Upload}) ->
    emqx_s3_upload:abort(Upload).

-spec format_status(normal, Args :: [term()]) -> _StateFormatted.
format_status(_Normal, [_PDict, _SysState, _Parent, _Debug, Delivery]) ->
    Delivery#delivery{
        upload = emqx_s3_upload:format(Delivery#delivery.upload)
    }.

%%

-spec lookup(emqx_template:accessor(), {_Name, buffer()}) ->
    {ok, integer() | string()} | {error, undefined}.
lookup([<<"action">>], {Name, _Buffer}) ->
    {ok, mk_fs_safe_string(Name)};
lookup(Accessor, {_Name, Buffer = #buffer{}}) ->
    lookup_buffer_var(Accessor, Buffer);
lookup(_Accessor, _Context) ->
    {error, undefined}.

lookup_buffer_var([<<"datetime">>, Format], #buffer{since = Since}) ->
    {ok, format_timestamp(Since, Format)};
lookup_buffer_var([<<"datetime_until">>, Format], #buffer{until = Until}) ->
    {ok, format_timestamp(Until, Format)};
lookup_buffer_var([<<"sequence">>], #buffer{seq = Seq}) ->
    {ok, Seq};
lookup_buffer_var([<<"node">>], #buffer{}) ->
    {ok, mk_fs_safe_string(atom_to_binary(erlang:node()))};
lookup_buffer_var(_Binding, _Context) ->
    {error, undefined}.

format_timestamp(Timestamp, <<"rfc3339utc">>) ->
    String = calendar:system_time_to_rfc3339(Timestamp, [{unit, second}, {offset, "Z"}]),
    mk_fs_safe_string(String);
format_timestamp(Timestamp, <<"rfc3339">>) ->
    String = calendar:system_time_to_rfc3339(Timestamp, [{unit, second}]),
    mk_fs_safe_string(String);
format_timestamp(Timestamp, <<"unix">>) ->
    Timestamp.

mk_fs_safe_string(String) ->
    unicode:characters_to_binary(string:replace(String, ":", "_", all)).
