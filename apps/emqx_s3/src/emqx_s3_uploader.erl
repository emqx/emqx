%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_uploader).

-include_lib("emqx/include/types.hrl").

-behaviour(gen_statem).

-export([
    start_link/2,

    write/2,
    write/3,

    complete/1,
    complete/2,

    abort/1,
    abort/2,

    shutdown/1
]).

-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3,
    code_change/4,
    format_status/1,
    format_status/2
]).

-export_type([opts/0, config/0]).

-type config() :: #{
    min_part_size => pos_integer(),
    max_part_size => pos_integer()
}.

-type opts() :: #{
    key := string(),
    headers => emqx_s3_client:headers()
}.

-type data() :: #{
    profile_id := emqx_s3:profile_id(),
    client := emqx_s3_client:client(),
    key := emqx_s3_client:key(),
    buffer := iodata(),
    buffer_size := non_neg_integer(),
    min_part_size := pos_integer(),
    max_part_size := pos_integer(),
    upload_id := undefined | emqx_s3_client:upload_id(),
    etags := [emqx_s3_client:etag()],
    part_number := emqx_s3_client:part_number(),
    headers := emqx_s3_client:headers()
}.

%% 5MB
-define(DEFAULT_MIN_PART_SIZE, 5242880).
%% 5GB
-define(DEFAULT_MAX_PART_SIZE, 5368709120).

-define(DEFAULT_TIMEOUT, 30000).

-spec start_link(emqx_s3:profile_id(), opts()) -> gen_statem:start_ret().
start_link(ProfileId, #{key := Key} = Opts) when is_list(Key) ->
    gen_statem:start_link(?MODULE, [ProfileId, Opts], []).

-spec write(pid(), iodata()) -> ok_or_error(term()).
write(Pid, WriteData) ->
    write(Pid, WriteData, ?DEFAULT_TIMEOUT).

-spec write(pid(), iodata(), timeout()) -> ok_or_error(term()).
write(Pid, WriteData, Timeout) ->
    gen_statem:call(Pid, {write, wrap(WriteData)}, Timeout).

-spec complete(pid()) -> ok_or_error(term()).
complete(Pid) ->
    complete(Pid, ?DEFAULT_TIMEOUT).

-spec complete(pid(), timeout()) -> ok_or_error(term()).
complete(Pid, Timeout) ->
    gen_statem:call(Pid, complete, Timeout).

-spec abort(pid()) -> ok_or_error(term()).
abort(Pid) ->
    abort(Pid, ?DEFAULT_TIMEOUT).

-spec abort(pid(), timeout()) -> ok_or_error(term()).
abort(Pid, Timeout) ->
    gen_statem:call(Pid, abort, Timeout).

-spec shutdown(pid()) -> ok.
shutdown(Pid) ->
    _ = erlang:exit(Pid, shutdown),
    ok.

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

callback_mode() -> handle_event_function.

init([ProfileId, #{key := Key} = Opts]) ->
    process_flag(trap_exit, true),
    {ok, ClientConfig, UploaderConfig} = emqx_s3_profile_conf:checkout_config(ProfileId),
    Client = client(ClientConfig),
    {ok, upload_not_started, #{
        profile_id => ProfileId,
        client => Client,
        headers => maps:get(headers, Opts, #{}),
        key => Key,
        buffer => [],
        buffer_size => 0,
        min_part_size => maps:get(min_part_size, UploaderConfig, ?DEFAULT_MIN_PART_SIZE),
        max_part_size => maps:get(max_part_size, UploaderConfig, ?DEFAULT_MAX_PART_SIZE),
        upload_id => undefined,
        etags => [],
        part_number => 1
    }}.

handle_event({call, From}, {write, WriteDataWrapped}, State, Data0) ->
    WriteData = unwrap(WriteDataWrapped),
    case is_valid_part(WriteData, Data0) of
        true ->
            handle_write(State, From, WriteData, Data0);
        false ->
            {keep_state_and_data, {reply, From, {error, {too_large, iolist_size(WriteData)}}}}
    end;
handle_event({call, From}, complete, upload_not_started, Data0) ->
    case put_object(Data0) of
        ok ->
            {stop_and_reply, normal, {reply, From, ok}};
        {error, _} = Error ->
            {stop_and_reply, Error, {reply, From, Error}, Data0}
    end;
handle_event({call, From}, complete, upload_started, Data0) ->
    case complete_upload(Data0) of
        {ok, Data1} ->
            {stop_and_reply, normal, {reply, From, ok}, Data1};
        {error, _} = Error ->
            {stop_and_reply, Error, {reply, From, Error}, Data0}
    end;
handle_event({call, From}, abort, upload_not_started, _Data) ->
    {stop_and_reply, normal, {reply, From, ok}};
handle_event({call, From}, abort, upload_started, Data0) ->
    case abort_upload(Data0) of
        ok ->
            {stop_and_reply, normal, {reply, From, ok}};
        {error, _} = Error ->
            {stop_and_reply, Error, {reply, From, Error}, Data0}
    end.

handle_write(upload_not_started, From, WriteData, Data0) ->
    Data1 = append_buffer(Data0, WriteData),
    case maybe_start_upload(Data1) of
        not_started ->
            {keep_state, Data1, {reply, From, ok}};
        {started, Data2} ->
            case upload_part(Data2) of
                {ok, Data3} ->
                    {next_state, upload_started, Data3, {reply, From, ok}};
                {error, _} = Error ->
                    {stop_and_reply, Error, {reply, From, Error}, Data2}
            end;
        {error, _} = Error ->
            {stop_and_reply, Error, {reply, From, Error}, Data1}
    end;
handle_write(upload_started, From, WriteData, Data0) ->
    Data1 = append_buffer(Data0, WriteData),
    case maybe_upload_part(Data1) of
        {ok, Data2} ->
            {keep_state, Data2, {reply, From, ok}};
        {error, _} = Error ->
            {stop_and_reply, Error, {reply, From, Error}, Data1}
    end.

terminate(Reason, _State, #{client := Client, upload_id := UploadId, key := Key}) when
    (UploadId =/= undefined) andalso (Reason =/= normal)
->
    emqx_s3_client:abort_multipart(Client, Key, UploadId);
terminate(_Reason, _State, _Data) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

format_status(#{data := #{client := Client} = Data} = Status) ->
    Status#{
        data => Data#{
            client => emqx_s3_client:format(Client),
            buffer => [<<"...">>]
        }
    }.

format_status(_Opt, [PDict, State, #{client := Client} = Data]) ->
    #{
        data => Data#{
            client => emqx_s3_client:format(Client),
            buffer => [<<"...">>]
        },
        state => State,
        pdict => PDict
    }.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-spec maybe_start_upload(data()) -> not_started | {started, data()} | {error, term()}.
maybe_start_upload(#{buffer_size := BufferSize, min_part_size := MinPartSize} = Data) ->
    case BufferSize >= MinPartSize of
        true ->
            start_upload(Data);
        false ->
            not_started
    end.

-spec start_upload(data()) -> {started, data()} | {error, term()}.
start_upload(#{client := Client, key := Key, headers := Headers} = Data) ->
    case emqx_s3_client:start_multipart(Client, Headers, Key) of
        {ok, UploadId} ->
            NewData = Data#{upload_id => UploadId},
            {started, NewData};
        {error, _} = Error ->
            Error
    end.

-spec maybe_upload_part(data()) -> ok_or_error(data(), term()).
maybe_upload_part(#{buffer_size := BufferSize, min_part_size := MinPartSize} = Data) ->
    case BufferSize >= MinPartSize of
        true ->
            upload_part(Data);
        false ->
            {ok, Data}
    end.

-spec upload_part(data()) -> ok_or_error(data(), term()).
upload_part(#{buffer_size := 0} = Data) ->
    {ok, Data};
upload_part(
    #{
        client := Client,
        key := Key,
        upload_id := UploadId,
        buffer := Buffer,
        part_number := PartNumber,
        etags := ETags
    } = Data
) ->
    case emqx_s3_client:upload_part(Client, Key, UploadId, PartNumber, Buffer) of
        {ok, ETag} ->
            NewData = Data#{
                buffer => [],
                buffer_size => 0,
                part_number => PartNumber + 1,
                etags => [{PartNumber, ETag} | ETags]
            },
            {ok, NewData};
        {error, _} = Error ->
            Error
    end.

-spec complete_upload(data()) -> ok_or_error(data(), term()).
complete_upload(
    #{
        client := Client,
        key := Key,
        upload_id := UploadId
    } = Data0
) ->
    case upload_part(Data0) of
        {ok, #{etags := ETags} = Data1} ->
            case
                emqx_s3_client:complete_multipart(
                    Client, Key, UploadId, lists:reverse(ETags)
                )
            of
                ok ->
                    {ok, Data1};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

-spec abort_upload(data()) -> ok_or_error(term()).
abort_upload(
    #{
        client := Client,
        key := Key,
        upload_id := UploadId
    }
) ->
    case emqx_s3_client:abort_multipart(Client, Key, UploadId) of
        ok ->
            ok;
        {error, _} = Error ->
            Error
    end.

-spec put_object(data()) -> ok_or_error(term()).
put_object(
    #{
        client := Client,
        key := Key,
        buffer := Buffer,
        headers := Headers
    }
) ->
    case emqx_s3_client:put_object(Client, Headers, Key, Buffer) of
        ok ->
            ok;
        {error, _} = Error ->
            Error
    end.

-spec append_buffer(data(), iodata()) -> data().
append_buffer(#{buffer := Buffer, buffer_size := BufferSize} = Data, WriteData) ->
    Data#{
        buffer => [Buffer, WriteData],
        buffer_size => BufferSize + iolist_size(WriteData)
    }.

-compile({inline, [wrap/1, unwrap/1]}).
wrap(Data) ->
    fun() -> Data end.

unwrap(WrappedData) ->
    WrappedData().

is_valid_part(WriteData, #{max_part_size := MaxPartSize, buffer_size := BufferSize}) ->
    BufferSize + iolist_size(WriteData) =< MaxPartSize.

client(Config) ->
    emqx_s3_client:create(Config).
