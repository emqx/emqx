%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_upload).

-include_lib("emqx/include/types.hrl").

-export([
    new/4,
    append/2,
    write/1,
    complete/1,
    abort/1
]).

-export([format/1]).

-export_type([t/0, config/0]).

-type config() :: #{
    min_part_size => pos_integer(),
    max_part_size => pos_integer()
}.

-type t() :: #{
    started := boolean(),
    client := emqx_s3_client:client(),
    key := emqx_s3_client:key(),
    upload_opts := emqx_s3_client:upload_options(),
    buffer := iodata(),
    buffer_size := non_neg_integer(),
    min_part_size := pos_integer(),
    max_part_size := pos_integer(),
    upload_id := undefined | emqx_s3_client:upload_id(),
    etags := [emqx_s3_client:etag()],
    part_number := emqx_s3_client:part_number()
}.

%% 5MB
-define(DEFAULT_MIN_PART_SIZE, 5242880).
%% 5GB
-define(DEFAULT_MAX_PART_SIZE, 5368709120).

%%

-spec new(
    emqx_s3_client:client(),
    emqx_s3_client:key(),
    emqx_s3_client:upload_options(),
    config()
) ->
    t().
new(Client, Key, UploadOpts, Config) ->
    #{
        started => false,
        client => Client,
        key => Key,
        upload_opts => UploadOpts,
        buffer => [],
        buffer_size => 0,
        min_part_size => maps:get(min_part_size, Config, ?DEFAULT_MIN_PART_SIZE),
        max_part_size => maps:get(max_part_size, Config, ?DEFAULT_MAX_PART_SIZE),
        upload_id => undefined,
        etags => [],
        part_number => 1
    }.

-spec append(iodata(), t()) -> {ok, t()} | {error, term()}.
append(WriteData, #{buffer := Buffer, buffer_size := BufferSize} = Upload) ->
    case is_valid_part(WriteData, Upload) of
        true ->
            {ok, Upload#{
                buffer => [Buffer, WriteData],
                buffer_size => BufferSize + iolist_size(WriteData)
            }};
        false ->
            {error, {too_large, iolist_size(WriteData)}}
    end.

-spec write(t()) -> {ok, t()} | {cont, t()} | {error, term()}.
write(U0 = #{started := false}) ->
    case maybe_start_upload(U0) of
        not_started ->
            {ok, U0};
        {started, U1} ->
            {cont, U1#{started := true}};
        {error, _} = Error ->
            Error
    end;
write(U0 = #{started := true}) ->
    maybe_upload_part(U0).

-spec complete(t()) -> {ok, t()} | {error, term()}.
complete(
    #{
        started := true,
        client := Client,
        key := Key,
        upload_id := UploadId
    } = U0
) ->
    case upload_part(U0) of
        {ok, #{etags := ETagsRev} = U1} ->
            ETags = lists:reverse(ETagsRev),
            case emqx_s3_client:complete_multipart(Client, Key, UploadId, ETags) of
                ok ->
                    {ok, U1};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end;
complete(#{started := false} = Upload) ->
    put_object(Upload).

-spec abort(t()) -> ok_or_error(term()).
abort(#{
    started := true,
    client := Client,
    key := Key,
    upload_id := UploadId
}) ->
    case emqx_s3_client:abort_multipart(Client, Key, UploadId) of
        ok ->
            ok;
        {error, _} = Error ->
            Error
    end;
abort(#{started := false}) ->
    ok.

%%--------------------------------------------------------------------

-spec format(t()) -> map().
format(Upload = #{client := Client}) ->
    Upload#{
        client => emqx_s3_client:format(Client),
        buffer => [<<"...">>]
    }.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-spec maybe_start_upload(t()) -> not_started | {started, t()} | {error, term()}.
maybe_start_upload(#{buffer_size := BufferSize, min_part_size := MinPartSize} = Data) ->
    case BufferSize >= MinPartSize of
        true ->
            start_upload(Data);
        false ->
            not_started
    end.

-spec start_upload(t()) -> {started, t()} | {error, term()}.
start_upload(#{client := Client, key := Key, upload_opts := UploadOpts} = Data) ->
    case emqx_s3_client:start_multipart(Client, Key, UploadOpts) of
        {ok, UploadId} ->
            NewData = Data#{upload_id => UploadId},
            {started, NewData};
        {error, _} = Error ->
            Error
    end.

-spec maybe_upload_part(t()) -> ok_or_error(t(), term()).
maybe_upload_part(#{buffer_size := BufferSize, min_part_size := MinPartSize} = Data) ->
    case BufferSize >= MinPartSize of
        true ->
            upload_part(Data);
        false ->
            {ok, Data}
    end.

-spec upload_part(t()) -> ok_or_error(t(), term()).
upload_part(#{buffer_size := 0} = Upload) ->
    {ok, Upload};
upload_part(
    #{
        client := Client,
        key := Key,
        upload_id := UploadId,
        buffer := Buffer,
        part_number := PartNumber,
        etags := ETags
    } = Upload
) ->
    case emqx_s3_client:upload_part(Client, Key, UploadId, PartNumber, Buffer) of
        {ok, ETag} ->
            {ok, Upload#{
                buffer => [],
                buffer_size => 0,
                part_number => PartNumber + 1,
                etags => [{PartNumber, ETag} | ETags]
            }};
        {error, _} = Error ->
            Error
    end.

-spec put_object(t()) -> ok_or_error(t(), term()).
put_object(
    #{
        client := Client,
        key := Key,
        upload_opts := UploadOpts,
        buffer := Buffer
    } = Upload
) ->
    case emqx_s3_client:put_object(Client, Key, UploadOpts, Buffer) of
        ok ->
            {ok, Upload};
        {error, _} = Error ->
            Error
    end.

is_valid_part(WriteData, #{max_part_size := MaxPartSize, buffer_size := BufferSize}) ->
    BufferSize + iolist_size(WriteData) =< MaxPartSize.
