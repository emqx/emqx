%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft_storage_exporter_s3).

-include_lib("emqx/include/logger.hrl").

%% Exporter API
-export([start_export/3]).
-export([write/2]).
-export([complete/2]).
-export([discard/1]).
-export([list/1]).

-export([
    start/1,
    stop/1,
    update/2
]).

-type options() :: emqx_s3:profile_config().
-type transfer() :: emqx_ft:transfer().
-type filemeta() :: emqx_ft:filemeta().
-type exportinfo() :: #{
    transfer := transfer(),
    name := file:name(),
    uri := uri_string:uri_string(),
    timestamp := emqx_datetime:epoch_second(),
    size := _Bytes :: non_neg_integer(),
    filemeta => filemeta()
}.

-type export_st() :: #{
    pid := pid(),
    filemeta := filemeta(),
    transfer := transfer()
}.

-define(S3_PROFILE_ID, ?MODULE).
-define(FILEMETA_VSN, <<"1">>).
-define(S3_LIST_LIMIT, 500).

%%--------------------------------------------------------------------
%% Exporter behaviour
%%--------------------------------------------------------------------

-spec start_export(options(), transfer(), filemeta()) ->
    {ok, export_st()} | {error, term()}.
start_export(_Options, Transfer, Filemeta) ->
    Options = #{
        key => s3_key(Transfer, Filemeta),
        headers => s3_headers(Transfer, Filemeta)
    },
    case emqx_s3:start_uploader(?S3_PROFILE_ID, Options) of
        {ok, Pid} ->
            true = erlang:link(Pid),
            {ok, #{filemeta => Filemeta, pid => Pid}};
        {error, _Reason} = Error ->
            Error
    end.

-spec write(export_st(), iodata()) ->
    {ok, export_st()} | {error, term()}.
write(#{pid := Pid} = ExportSt, IoData) ->
    case emqx_s3_uploader:write(Pid, IoData) of
        ok ->
            {ok, ExportSt};
        {error, _Reason} = Error ->
            Error
    end.

-spec complete(export_st(), emqx_ft:checksum()) ->
    ok | {error, term()}.
complete(#{pid := Pid} = _ExportSt, _Checksum) ->
    emqx_s3_uploader:complete(Pid).

-spec discard(export_st()) ->
    ok.
discard(#{pid := Pid} = _ExportSt) ->
    emqx_s3_uploader:abort(Pid).

-spec list(options()) ->
    {ok, [exportinfo()]} | {error, term()}.
list(Options) ->
    emqx_s3:with_client(?S3_PROFILE_ID, fun(Client) -> list(Client, Options) end).

%%--------------------------------------------------------------------
%% Exporter behaviour (lifecycle)
%%--------------------------------------------------------------------

-spec start(options()) -> ok | {error, term()}.
start(Options) ->
    emqx_s3:start_profile(?S3_PROFILE_ID, Options).

-spec stop(options()) -> ok.
stop(_Options) ->
    ok = emqx_s3:stop_profile(?S3_PROFILE_ID).

-spec update(options(), options()) -> ok.
update(_OldOptions, NewOptions) ->
    emqx_s3:update_profile(?S3_PROFILE_ID, NewOptions).

%%--------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

s3_key({ClientId, FileId} = _Transfer, #{name := Filename}) ->
    filename:join([
        emqx_ft_fs_util:escape_filename(ClientId),
        emqx_ft_fs_util:escape_filename(FileId),
        Filename
    ]).

s3_headers({ClientId, FileId}, Filemeta) ->
    #{
        %% The ClientID MUST be a UTF-8 Encoded String
        <<"x-amz-meta-clientid">> => ClientId,
        %% It [Topic Name] MUST be a UTF-8 Encoded String
        <<"x-amz-meta-fileid">> => FileId,
        <<"x-amz-meta-filemeta">> => emqx_json:encode(emqx_ft:encode_filemeta(Filemeta)),
        <<"x-amz-meta-filemeta-vsn">> => ?FILEMETA_VSN
    }.

list(Client, Options) ->
    case list_key_info(Client, Options) of
        {ok, KeyInfos} ->
            MaybeExportInfos = lists:map(
                fun(KeyInfo) -> key_info_to_exportinfo(Client, KeyInfo, Options) end, KeyInfos
            ),
            ExportInfos = [ExportInfo || {ok, ExportInfo} <- MaybeExportInfos],
            {ok, ExportInfos};
        {error, _Reason} = Error ->
            Error
    end.

list_key_info(Client, Options) ->
    list_key_info(Client, Options, _Marker = [], _Acc = []).

list_key_info(Client, Options, Marker, Acc) ->
    ListOptions = [{max_keys, ?S3_LIST_LIMIT}] ++ Marker,
    case emqx_s3_client:list(Client, ListOptions) of
        {ok, Result} ->
            ?SLOG(debug, #{msg => "list_key_info", result => Result}),
            KeyInfos = proplists:get_value(contents, Result, []),
            case proplists:get_value(is_truncated, Result, false) of
                true ->
                    NewMarker = next_marker(KeyInfos),
                    list_key_info(Client, Options, NewMarker, [KeyInfos | Acc]);
                false ->
                    {ok, lists:append(lists:reverse([KeyInfos | Acc]))}
            end;
        {error, _Reason} = Error ->
            Error
    end.

next_marker(KeyInfos) ->
    [{marker, proplists:get_value(key, lists:last(KeyInfos))}].

key_info_to_exportinfo(Client, KeyInfo, _Options) ->
    Key = proplists:get_value(key, KeyInfo),
    case parse_transfer_and_name(Key) of
        {ok, {Transfer, Name}} ->
            {ok, #{
                transfer => Transfer,
                name => unicode:characters_to_binary(Name),
                uri => emqx_s3_client:uri(Client, Key),
                timestamp => datetime_to_epoch_second(proplists:get_value(last_modified, KeyInfo)),
                size => proplists:get_value(size, KeyInfo)
            }};
        {error, _Reason} = Error ->
            Error
    end.

-define(EPOCH_START, 62167219200).

datetime_to_epoch_second(DateTime) ->
    calendar:datetime_to_gregorian_seconds(DateTime) - ?EPOCH_START.

parse_transfer_and_name(Key) ->
    case string:split(Key, "/", all) of
        [ClientId, FileId, Name] ->
            Transfer = {
                emqx_ft_fs_util:unescape_filename(ClientId),
                emqx_ft_fs_util:unescape_filename(FileId)
            },
            {ok, {Transfer, Name}};
        _ ->
            {error, invalid_key}
    end.
