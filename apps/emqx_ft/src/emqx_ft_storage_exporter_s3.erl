%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([list/2]).

-export([
    start/1,
    stop/1,
    update_config/2
]).

-export([
    pre_config_update/3,
    post_config_update/3
]).

-type options() :: emqx_s3:profile_config().
-type transfer() :: emqx_ft:transfer().
-type filemeta() :: emqx_ft:filemeta().
-type exportinfo() :: #{
    transfer := transfer(),
    name := file:name(),
    uri := uri_string:uri_string(),
    timestamp := emqx_utils_calendar:epoch_second(),
    size := _Bytes :: non_neg_integer(),
    filemeta => filemeta()
}.

-type query() :: emqx_ft_storage:query(cursor()).
-type page(T) :: emqx_ft_storage:page(T, cursor()).
-type cursor() :: iodata().

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
    Key = s3_key(Transfer, Filemeta),
    UploadOpts = #{headers => s3_headers(Transfer, Filemeta)},
    case emqx_s3:start_uploader(?S3_PROFILE_ID, Key, UploadOpts) of
        {ok, Pid} ->
            true = erlang:link(Pid),
            {ok, #{filemeta => Filemeta, pid => Pid}};
        {error, _Reason} = Error ->
            Error
    end.

-spec write(export_st(), iodata()) ->
    {ok, export_st()} | {error, term()}.
write(#{pid := Pid} = ExportSt, IoData) ->
    case emqx_s3_uploader:write(Pid, IoData, emqx_ft_conf:store_segment_timeout()) of
        ok ->
            {ok, ExportSt};
        {error, _Reason} = Error ->
            Error
    end.

-spec complete(export_st(), emqx_ft:checksum()) ->
    ok | {error, term()}.
complete(#{pid := Pid} = _ExportSt, _Checksum) ->
    emqx_s3_uploader:complete(Pid, emqx_ft_conf:assemble_timeout()).

-spec discard(export_st()) ->
    ok.
discard(#{pid := Pid} = _ExportSt) ->
    % NOTE: will abort upload asynchronously if needed
    emqx_s3_uploader:shutdown(Pid).

-spec list(options(), query()) ->
    {ok, page(exportinfo())} | {error, term()}.
list(Options, Query) ->
    emqx_s3:with_client(?S3_PROFILE_ID, fun(Client) -> list(Client, Options, Query) end).

%%--------------------------------------------------------------------
%% Exporter behaviour (lifecycle)
%%--------------------------------------------------------------------

-spec start(options()) -> ok | {error, term()}.
start(Options) ->
    emqx_s3:start_profile(?S3_PROFILE_ID, Options).

-spec stop(options()) -> ok.
stop(_Options) ->
    emqx_s3:stop_profile(?S3_PROFILE_ID).

-spec update_config(options(), options()) -> ok.
update_config(_OldOptions, NewOptions) ->
    emqx_s3:update_profile(?S3_PROFILE_ID, NewOptions).

%%--------------------------------------------------------------------
%% Config update hooks
%%--------------------------------------------------------------------

pre_config_update(_ConfKey, NewOptions, OldOptions) ->
    emqx_s3:pre_config_update(?S3_PROFILE_ID, NewOptions, OldOptions).

post_config_update(_ConfKey, NewOptions, OldOptions) ->
    emqx_s3:post_config_update(?S3_PROFILE_ID, NewOptions, OldOptions).

%%--------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

s3_key(Transfer, #{name := Filename}) ->
    s3_prefix(Transfer) ++ "/" ++ Filename.

s3_prefix({ClientId, FileId} = _Transfer) ->
    emqx_ft_fs_util:escape_filename(ClientId) ++ "/" ++ emqx_ft_fs_util:escape_filename(FileId).

s3_headers({ClientId, FileId}, Filemeta) ->
    #{
        %% The ClientID MUST be a UTF-8 Encoded String
        <<"x-amz-meta-clientid">> => ClientId,
        %% It [Topic Name] MUST be a UTF-8 Encoded String
        <<"x-amz-meta-fileid">> => FileId,
        <<"x-amz-meta-filemeta">> => s3_header_filemeta(Filemeta),
        <<"x-amz-meta-filemeta-vsn">> => ?FILEMETA_VSN
    }.

s3_header_filemeta(Filemeta) ->
    emqx_utils_json:encode(emqx_ft:encode_filemeta(Filemeta), [force_utf8, uescape]).

list(Client, _Options, #{transfer := Transfer}) ->
    case list_key_info(Client, [{prefix, s3_prefix(Transfer)}, {max_keys, ?S3_LIST_LIMIT}]) of
        {ok, {Exports, _Marker}} ->
            {ok, #{items => Exports}};
        {error, _Reason} = Error ->
            Error
    end;
list(Client, _Options, Query) ->
    Limit = maps:get(limit, Query, undefined),
    Marker = emqx_maybe:apply(fun decode_cursor/1, maps:get(following, Query, undefined)),
    case list_pages(Client, Marker, Limit, []) of
        {ok, {Exports, undefined}} ->
            {ok, #{items => Exports}};
        {ok, {Exports, NextMarker}} ->
            {ok, #{items => Exports, cursor => encode_cursor(NextMarker)}};
        {error, _Reason} = Error ->
            Error
    end.

list_pages(Client, Marker, Limit, Acc) ->
    MaxKeys = min(?S3_LIST_LIMIT, Limit),
    ListOptions = [{marker, Marker} || Marker =/= undefined],
    case list_key_info(Client, [{max_keys, MaxKeys} | ListOptions]) of
        {ok, {Exports, NextMarker}} ->
            Left = update_limit(Limit, Exports),
            NextAcc = [Exports | Acc],
            case NextMarker of
                undefined ->
                    {ok, {flatten_pages(NextAcc), undefined}};
                _ when Left =< 0 ->
                    {ok, {flatten_pages(NextAcc), NextMarker}};
                _ ->
                    list_pages(Client, NextMarker, Left, NextAcc)
            end;
        {error, _Reason} = Error ->
            Error
    end.

update_limit(undefined, _Exports) ->
    undefined;
update_limit(Limit, Exports) ->
    Limit - length(Exports).

flatten_pages(Pages) ->
    lists:append(lists:reverse(Pages)).

list_key_info(Client, ListOptions) ->
    case emqx_s3_client:list(Client, ListOptions) of
        {ok, Result} ->
            ?SLOG(debug, #{msg => "list_key_info", result => Result}),
            KeyInfos = proplists:get_value(contents, Result, []),
            Exports = lists:filtermap(
                fun(KeyInfo) -> key_info_to_exportinfo(Client, KeyInfo) end, KeyInfos
            ),
            Marker =
                case proplists:get_value(is_truncated, Result, false) of
                    true ->
                        next_marker(KeyInfos);
                    false ->
                        undefined
                end,
            {ok, {Exports, Marker}};
        {error, _Reason} = Error ->
            Error
    end.

encode_cursor(Key) ->
    unicode:characters_to_binary(Key).

decode_cursor(Cursor) ->
    case unicode:characters_to_list(Cursor) of
        Key when is_list(Key) ->
            Key;
        _ ->
            error({badarg, cursor})
    end.

next_marker(KeyInfos) ->
    proplists:get_value(key, lists:last(KeyInfos)).

key_info_to_exportinfo(Client, KeyInfo) ->
    Key = proplists:get_value(key, KeyInfo),
    case parse_transfer_and_name(Key) of
        {ok, {Transfer, Name}} ->
            {true, #{
                transfer => Transfer,
                name => unicode:characters_to_binary(Name),
                uri => emqx_s3_client:uri(Client, Key),
                timestamp => datetime_to_epoch_second(proplists:get_value(last_modified, KeyInfo)),
                size => proplists:get_value(size, KeyInfo)
            }};
        {error, _Reason} ->
            false
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
