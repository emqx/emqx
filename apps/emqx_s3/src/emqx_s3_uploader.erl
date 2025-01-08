%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_uploader).

-include_lib("emqx/include/types.hrl").

-behaviour(gen_server).

-export([
    start_link/3,

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
    handle_call/3,
    handle_cast/2,
    terminate/2,
    format_status/1
]).

-type data() :: #{
    profile_id => emqx_s3:profile_id(),
    upload := emqx_s3_upload:t() | aborted
}.

-define(DEFAULT_TIMEOUT, 30000).

-spec start_link(emqx_s3:profile_id(), emqx_s3_client:key(), emqx_s3_client:upload_options()) ->
    gen_server:start_ret().
start_link(ProfileId, Key, UploadOpts) when is_list(Key) ->
    gen_server:start_link(?MODULE, {profile, ProfileId, Key, UploadOpts}, []).

-spec write(pid(), iodata()) -> ok_or_error(term()).
write(Pid, WriteData) ->
    write(Pid, WriteData, ?DEFAULT_TIMEOUT).

-spec write(pid(), iodata(), timeout()) -> ok_or_error(term()).
write(Pid, WriteData, Timeout) ->
    gen_server:call(Pid, {write, wrap(WriteData)}, Timeout).

-spec complete(pid()) -> ok_or_error(term()).
complete(Pid) ->
    complete(Pid, ?DEFAULT_TIMEOUT).

-spec complete(pid(), timeout()) -> ok_or_error(term()).
complete(Pid, Timeout) ->
    gen_server:call(Pid, complete, Timeout).

-spec abort(pid()) -> ok_or_error(term()).
abort(Pid) ->
    abort(Pid, ?DEFAULT_TIMEOUT).

-spec abort(pid(), timeout()) -> ok_or_error(term()).
abort(Pid, Timeout) ->
    gen_server:call(Pid, abort, Timeout).

-spec shutdown(pid()) -> ok.
shutdown(Pid) ->
    _ = erlang:exit(Pid, shutdown),
    ok.

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

init({profile, ProfileId, Key, UploadOpts}) ->
    _ = process_flag(trap_exit, true),
    {Bucket, ClientConfig, BaseOpts, UploaderConfig} =
        emqx_s3_profile_conf:checkout_config(ProfileId),
    Upload = emqx_s3_upload:new(
        client(Bucket, ClientConfig),
        Key,
        maps:merge(BaseOpts, UploadOpts),
        UploaderConfig
    ),
    {ok, #{profile_id => ProfileId, upload => Upload}}.

-spec handle_call(_Call, gen_server:from(), data()) ->
    {reply, _Result, data()} | {stop, _Reason, _Result, data()}.
handle_call({write, WriteDataWrapped}, _From, St0 = #{upload := U0}) ->
    WriteData = unwrap(WriteDataWrapped),
    case emqx_s3_upload:append(WriteData, U0) of
        {ok, U1} ->
            handle_write(St0#{upload := U1});
        {error, _} = Error ->
            {reply, Error, St0}
    end;
handle_call(complete, _From, St0 = #{upload := U0}) ->
    case emqx_s3_upload:complete(U0) of
        {ok, U1} ->
            {stop, normal, ok, St0#{upload := U1}};
        {error, _} = Error ->
            {stop, Error, Error, St0}
    end;
handle_call(abort, _From, St = #{upload := Upload}) ->
    case emqx_s3_upload:abort(Upload) of
        ok ->
            {stop, normal, ok, St};
        {error, _} = Error ->
            {stop, Error, Error, St}
    end.

handle_write(St = #{upload := U0}) ->
    case emqx_s3_upload:write(U0) of
        {ok, U1} ->
            {reply, ok, St#{upload := U1}};
        {cont, U1} ->
            handle_write(St#{upload := U1});
        {error, _} = Error ->
            {stop, Error, Error, St}
    end.

-spec handle_cast(_Cast, data()) -> {noreply, data()}.
handle_cast(_Cast, St) ->
    {noreply, St}.

-spec terminate(_Reason, data()) -> ok.
terminate(normal, _St) ->
    ok;
terminate({shutdown, _}, _St) ->
    ok;
terminate(_Reason, #{upload := Upload}) ->
    emqx_s3_upload:abort(Upload).

format_status(#{state := State = #{upload := Upload}} = Status) ->
    StateRedacted = State#{upload := emqx_s3_upload:format(Upload)},
    Status#{state := StateRedacted}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-compile({inline, [wrap/1, unwrap/1]}).
wrap(Data) ->
    fun() -> Data end.

unwrap(WrappedData) ->
    WrappedData().

client(Bucket, Config) ->
    emqx_s3_client:create(Bucket, Config).
