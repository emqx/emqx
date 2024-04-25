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
-export([run_delivery/3]).

-behaviour(emqx_template).
-export([lookup/2]).

-record(delivery, {
    name :: _Name,
    container :: emqx_bridge_s3_aggreg_csv:container(),
    reader :: emqx_bridge_s3_aggreg_buffer:reader(),
    upload :: emqx_s3_upload:t(),
    empty :: boolean()
}).

%%

start_link(Name, Buffer, Opts) ->
    proc_lib:start_link(?MODULE, run_delivery, [Name, Buffer, Opts]).

%%

run_delivery(Name, Buffer, Opts) ->
    ?tp(s3_aggreg_delivery_started, #{action => Name, buffer => Buffer}),
    Reader = open_buffer(Buffer),
    Delivery = init_delivery(Name, Reader, Buffer, Opts#{action => Name}),
    ok = proc_lib:init_ack({ok, self()}),
    loop_deliver(Delivery).

init_delivery(Name, Reader, Buffer, Opts = #{container := ContainerOpts}) ->
    #delivery{
        name = Name,
        container = mk_container(ContainerOpts),
        reader = Reader,
        upload = mk_upload(Buffer, Opts),
        empty = true
    }.

loop_deliver(Delivery = #delivery{reader = Reader0}) ->
    case emqx_bridge_s3_aggreg_buffer:read(Reader0) of
        {Records = [#{} | _], Reader} ->
            loop_deliver_records(Records, Delivery#delivery{reader = Reader});
        {[], Reader} ->
            loop_deliver(Delivery#delivery{reader = Reader});
        eof ->
            complete_delivery(Delivery);
        {Unexpected, _Reader} ->
            exit({buffer_unexpected_record, Unexpected})
    end.

loop_deliver_records(Records, Delivery = #delivery{container = Container0, upload = Upload0}) ->
    {Writes, Container} = emqx_bridge_s3_aggreg_csv:fill(Records, Container0),
    {ok, Upload} = emqx_s3_upload:append(Writes, Upload0),
    loop_deliver_upload(Delivery#delivery{
        container = Container,
        upload = Upload,
        empty = false
    }).

loop_deliver_upload(Delivery = #delivery{upload = Upload0}) ->
    case emqx_s3_upload:write(Upload0) of
        {ok, Upload} ->
            loop_deliver(Delivery#delivery{upload = Upload});
        {cont, Upload} ->
            loop_deliver_upload(Delivery#delivery{upload = Upload});
        {error, Reason} ->
            %% TODO: retries
            _ = emqx_s3_upload:abort(Upload0),
            exit({upload_failed, Reason})
    end.

complete_delivery(#delivery{name = Name, empty = true}) ->
    ?tp(s3_aggreg_delivery_completed, #{action => Name, upload => empty}),
    exit({shutdown, {skipped, empty}});
complete_delivery(#delivery{name = Name, container = Container, upload = Upload0}) ->
    Trailer = emqx_bridge_s3_aggreg_csv:close(Container),
    {ok, Upload} = emqx_s3_upload:append(Trailer, Upload0),
    case emqx_s3_upload:complete(Upload) of
        {ok, Completed} ->
            ?tp(s3_aggreg_delivery_completed, #{action => Name, upload => Completed}),
            ok;
        {error, Reason} ->
            %% TODO: retries
            _ = emqx_s3_upload:abort(Upload),
            exit({upload_failed, Reason})
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

open_buffer(#buffer{filename = Filename}) ->
    case file:open(Filename, [read, binary, raw]) of
        {ok, FD} ->
            {_Meta, Reader} = emqx_bridge_s3_aggreg_buffer:new_reader(FD),
            Reader;
        {error, Reason} ->
            error({buffer_open_failed, Reason})
    end.

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
